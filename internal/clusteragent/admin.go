package clusteragent

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/viewertoken"
)

// Relay admission headers (RelayGuard::TICKET / ::AUTH) the downstream child's Go
// relay proxy sends when it pulls /admin/live from this node (relayproxy.go).
const (
	relayTicketHeader = "X-XCVM-Relay"
	relayAuthHeader   = "X-XCVM-Relay-Auth"
)

// admitRelay is the parent-side of RelayGuard's relay arm for /admin/<kind>: a
// downstream child pulls a stream sourced on this node with a MAIN-signed "rly"
// ticket (naming THIS node as parent_sid and the stream) plus a per-request
// node-key proof. It mirrors serveXFile's two-factor admission — the agent's
// child-side verifiedTicket checks child_sid==self; here the parent checks
// parent_sid==self and that the request is for the ticket's stream. nowMs is
// MAIN's clock as the node can vouch for it.
func (a *Agent) admitRelay(streamID int64, r *http.Request, nowMs int64) bool {
	wire := r.Header.Get(relayTicketHeader)
	auth := r.Header.Get(relayAuthHeader)
	if wire == "" || auth == "" || len(a.Client.State.PanelSignPub) != ed25519.PublicKeySize {
		return false
	}
	t, ok := cc.VerifyTicket(a.Client.State.PanelSignPub, "rly", wire, nowMs/1000)
	if !ok {
		return false
	}
	parent, okP := t.Int("parent_sid")
	sid, okS := t.Int("stream_id")
	childSid, okC := t.Int("child_sid")
	childGen, okG := t.Int("child_gen")
	if !okP || parent != a.Client.State.ServerID || !okS || sid != streamID || !okC || !okG {
		return false
	}
	childPub, ok := a.fetcherSignPub(childSid, childGen)
	if !ok {
		return false
	}
	// The proof is over the exact raw request-target the child signed; the front
	// forwards it unaltered.
	_, nonce, ok := cc.VerifyRelayAuth(childPub, auth, wire, http.MethodGet, r.RequestURI, nowMs)
	if !ok {
		return false
	}
	a.nonceOnce.Do(func() { a.nonces = newNonceCache() })
	if !a.nonces.fresh(hex.EncodeToString(nonce)) {
		return false
	}
	return true
}

// loopbackTokenRe matches LoopbackToken's wire: lb1-<exp>-<hex mac>.
var loopbackTokenRe = regexp.MustCompile(`^lb1-(\d{1,12})-([0-9a-f]{64})\z`)

// admitPassword is RelayGuard's legacy password arm for /admin: the shared
// live_streaming_pass, admitted only from an allowed server IP whose node has the
// DATAPLANE flow OFF (a legacy, pre-relay-ticket child; a data-plane child must
// present a relay ticket instead).
func (a *Agent) admitPassword(given, ip string) bool {
	return a.passMatches(a.secret("live_streaming_pass"), given) && a.passwordAllowed(ip)
}

// passwordAllowed ports RelayGuard::passwordAllowed: ip is in getAllowedIPs and
// no server whose server_ip/private_ip is ip has a present node with the
// DATAPLANE flow on (a null node, or a dataplane-off node, does not refuse).
func (a *Agent) passwordAllowed(ip string) bool {
	if ip == "" || !a.allowedIPs()[ip] || a.ReplicaDir == "" {
		return false
	}
	b, err := os.ReadFile(filepath.Join(a.ReplicaDir, "servers.json"))
	if err != nil {
		return false
	}
	var doc struct {
		Data struct {
			Servers []struct {
				ID        int64   `json:"id"`
				ServerIP  *string `json:"server_ip"`
				PrivateIP *string `json:"private_ip"`
			} `json:"servers"`
			Nodes []struct {
				Sid       int64 `json:"sid"`
				Dataplane bool  `json:"dataplane"`
			} `json:"nodes"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &doc) != nil {
		return false
	}
	dpOn := make(map[int64]bool, len(doc.Data.Nodes))
	for _, n := range doc.Data.Nodes {
		dpOn[n.Sid] = n.Dataplane
	}
	for _, s := range doc.Data.Servers {
		if (s.ServerIP != nil && strings.TrimSpace(*s.ServerIP) == ip) ||
			(s.PrivateIP != nil && strings.TrimSpace(*s.PrivateIP) == ip) {
			if dpOn[s.ID] {
				return false
			}
		}
	}
	return true
}

// loopbackMAC is LoopbackToken::mac: HMAC-SHA256(key, "xcvm-loopback-v1" ‖
// u64be(streamID) ‖ u64be(exp)), hex.
func loopbackMAC(key []byte, streamID, exp int64) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("xcvm-loopback-v1"))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(streamID))
	m.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(exp))
	m.Write(buf[:])
	return hex.EncodeToString(m.Sum(nil))
}

// verifyLoopbackToken is LoopbackToken::verify: a well-formed lb1-<exp>-<mac>
// that has not expired and whose MAC matches under the node's loopback key.
func verifyLoopbackToken(key []byte, streamID int64, tok string, now int64) bool {
	if streamID <= 0 || len(key) != 32 { // LoopbackToken::key requires exactly 32 bytes
		return false
	}
	m := loopbackTokenRe.FindStringSubmatch(tok)
	if m == nil {
		return false
	}
	exp, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || exp < now {
		return false
	}
	return hmac.Equal([]byte(loopbackMAC(key, streamID, exp)), []byte(m[2]))
}

// admitLoopback is RelayGuard's loopback arm: an lb1- token the node's own
// recorder presents over 127.0.0.1 only (a non-loopback peer is refused). The key
// is config/cluster/loopback.key (LoopbackKeyFile); with none, the arm is off.
func (a *Agent) admitLoopback(streamID int64, tok, clientIP string, now int64) bool {
	if ip := net.ParseIP(clientIP); ip == nil || !ip.IsLoopback() {
		return false
	}
	if a.LoopbackKeyFile == "" {
		return false
	}
	key, err := os.ReadFile(a.LoopbackKeyFile)
	if err != nil || len(key) == 0 {
		return false
	}
	return verifyLoopbackToken(key, streamID, tok, now)
}

// admitKind is how an /admin request was admitted.
type admitKind int

const (
	admitNone admitKind = iota
	admitUitoken
	admitRelayKind
	admitPasswordKind
	admitLoopbackKind
)

// adminAdmission is the outcome of admitAdmin.
type adminAdmission struct {
	kind     admitKind
	streamID int64     // the stream the caller is admitted for (the uitoken's, else the handler's param)
	tok      *adminTok // the decoded uitoken (kind admitUitoken), else nil
}

// adminPolicy is the subset of the gateway policy file the /admin handlers need:
// the token keys (uitoken decode), ip_subnet_match, the media paths, the
// per-server redirect base URLs (thumb's owner 302), and the vod throttle.
type adminPolicy struct {
	keys           viewertoken.Keys
	acceptLegacy   bool
	subnetMatch    bool
	streamsPath    string
	vodPath        string
	archivePath    string
	redirect       map[string][]string
	vodLimitPerc   int
	vodBitratePlus int
}

// adminPolicy reads and parses the gateway policy file; ok is false when it is
// unset or unreadable (the uitoken/serving config is then unavailable).
func (a *Agent) adminPolicy() (*adminPolicy, bool) {
	if a.GatewayPolicyFile == "" {
		return nil, false
	}
	b, err := os.ReadFile(a.GatewayPolicyFile)
	if err != nil {
		return nil, false
	}
	keys, acceptLegacy, subnet, ok := parseViewerKeys(b)
	if !ok {
		return nil, false
	}
	var p struct {
		Paths struct {
			Streams string `json:"streams"`
			Vod     string `json:"vod"`
			Archive string `json:"archive"`
		} `json:"paths"`
		Redirect       map[string][]string `json:"redirect"`
		VodLimitPerc   int                 `json:"vod_limit_perc"`
		VodBitratePlus int                 `json:"vod_bitrate_plus"`
	}
	_ = json.Unmarshal(b, &p) // keys already parsed; paths/redirect are best-effort
	return &adminPolicy{
		keys: keys, acceptLegacy: acceptLegacy, subnetMatch: subnet,
		streamsPath: p.Paths.Streams, vodPath: p.Paths.Vod, archivePath: p.Paths.Archive,
		redirect: p.Redirect, vodLimitPerc: p.VodLimitPerc, vodBitratePlus: p.VodBitratePlus,
	}, true
}

// viewerKeys reads the node's token keys (and ip_subnet_match / accept_legacy)
// from the gateway policy file, for uitoken decode; ok is false when unavailable.
func (a *Agent) viewerKeys() (keys viewertoken.Keys, acceptLegacy, subnetMatch, ok bool) {
	p, ok := a.adminPolicy()
	if !ok {
		return viewertoken.Keys{}, false, false, false
	}
	return p.keys, p.acceptLegacy, p.subnetMatch, true
}

// parsePositiveInt parses a decimal positive int64, or 0 when it is not one.
func parsePositiveInt(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// serveAdminThumb is Public/admin/thumb.php: a relay/uitoken-admitted thumbnail
// read. It serves STREAMS_PATH/<id>_.jpg (fresh, <60s) when this node owns the
// stream's vframes_server_id, else 302-redirects to the owning server; a
// non-live/direct stream or any refusal is a 404. No password/loopback arm
// (passwordOK=false). clientIP is the resolved address the front forwards.
func (a *Agent) serveAdminThumb(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	clientIP := r.Header.Get("X-XC-Client-IP")
	adm, ok := a.admitAdmin(parsePositiveInt(q.Get("stream")), r, clientIP, false, a.Client.MainNowMs())
	if !ok {
		http.NotFound(w, r)
		return
	}
	pol, ok := a.adminPolicy()
	if !ok {
		http.NotFound(w, r)
		return
	}
	m, ok := a.streamMeta(strconv.FormatInt(adm.streamID, 10))
	if !ok || m.TypeLive != 1 || m.DirectSource != 0 { // StreamSource::streamRow(id, live=true)
		http.NotFound(w, r)
		return
	}
	if m.VframesServerID == a.Client.State.ServerID {
		path := filepath.Join(pol.streamsPath, strconv.FormatInt(adm.streamID, 10)+"_.jpg")
		fi, err := os.Stat(path)
		if err != nil || fi.IsDir() || time.Since(fi.ModTime()) >= 60*time.Second {
			http.NotFound(w, r)
			return
		}
		b, err := os.ReadFile(path)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/jpg")
		w.Header().Set("Age", strconv.Itoa(int(time.Since(fi.ModTime()).Seconds())))
		_, _ = w.Write(b)
		return
	}
	bases := pol.redirect[strconv.FormatInt(m.VframesServerID, 10)]
	if len(bases) == 0 || bases[0] == "" {
		http.NotFound(w, r)
		return
	}
	loc := bases[0] + "admin/thumb?stream=" + strconv.FormatInt(adm.streamID, 10) +
		"&aid=" + strconv.FormatInt(parsePositiveInt(q.Get("aid")), 10) +
		"&uitoken=" + url.QueryEscape(q.Get("uitoken")) +
		"&expires=" + strconv.FormatInt(parsePositiveInt(q.Get("expires")), 10)
	http.Redirect(w, r, loc, http.StatusFound)
}

// vodExtRe is a safe VOD container extension (so <id>.<ext> cannot traverse).
var vodExtRe = regexp.MustCompile(`^[a-z0-9]{1,6}$`)

// containerMimeType is StreamUtils::containerMimeType: the VOD Content-Type from
// the stream's target_container, independent of the URL extension.
func containerMimeType(c string) string {
	switch c {
	case "mp4":
		return "video/mp4"
	case "mkv":
		return "video/x-matroska"
	case "avi":
		return "video/x-msvideo"
	case "3gp":
		return "video/3gpp"
	case "flv":
		return "video/x-flv"
	case "wmv":
		return "video/x-ms-wmv"
	case "mov":
		return "video/quicktime"
	case "ts":
		return "video/mp2t"
	default:
		return "application/octet-stream"
	}
}

// serveAdminVOD is Public/admin/vod.php: a relay/uitoken/password-admitted
// byte-range relay of VOD_PATH/<id>.<ext>. The id (and, for a uitoken, the
// container) come from the token; otherwise from the "{id}.{ext}" stream param.
// Content-Type is the stream's target_container, independent of the URL ext.
// ponytail: served at full speed — vod.php's vod_limit_perc/vod_bitrate_plus
// bandwidth throttle is a follow-up, not ported here.
func (a *Agent) serveAdminVOD(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	clientIP := r.Header.Get("X-XC-Client-IP")
	stream := q.Get("stream")
	adm, ok := a.admitAdmin(parsePositiveInt(vodIDPart(stream)), r, clientIP, true, a.Client.MainNowMs())
	if !ok {
		http.NotFound(w, r)
		return
	}
	ext := vodExtPart(stream)
	if adm.kind == admitUitoken {
		// PHP derives the vod ext solely from the token (stream = id.container);
		// never let the client's stream param influence the served path.
		ext = ""
		if adm.tok != nil {
			ext = adm.tok.Container
		}
	}
	if !vodExtRe.MatchString(ext) {
		http.NotFound(w, r)
		return
	}
	pol, ok := a.adminPolicy()
	if !ok {
		http.NotFound(w, r)
		return
	}
	m, ok := a.streamMeta(strconv.FormatInt(adm.streamID, 10))
	if !ok {
		http.NotFound(w, r)
		return
	}
	path := filepath.Join(pol.vodPath, strconv.FormatInt(adm.streamID, 10)+"."+ext)
	if !withinDir(pol.vodPath, path) {
		http.NotFound(w, r)
		return
	}
	f, err := os.Open(path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || fi.IsDir() {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", containerMimeType(m.TargetContainer))
	http.ServeContent(w, r, path, fi.ModTime(), f) // handles Range → 206/416
}

// vodIDPart / vodExtPart split a vod stream param "{id}.{ext}".
func vodIDPart(s string) string {
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		return s[:i]
	}
	return s
}

func vodExtPart(s string) string {
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		return s[i+1:]
	}
	return ""
}

// withinDir reports whether path stays inside dir once both are cleaned.
func withinDir(dir, path string) bool {
	d := filepath.Clean(dir)
	p := filepath.Clean(path)
	return p == d || strings.HasPrefix(p, d+string(filepath.Separator))
}

// admitAdmin runs the uitoken pre-check + RelayGuard::admit for an /admin handler,
// faithful to the order Public/admin/*.php use. paramStreamID is the handler's
// parse of its own stream param (the relay/password/loopback arms use it; the
// uitoken arm takes the stream from the token). clientIP is the resolved caller
// address the front forwards. passwordOK is false for thumb (relay or uitoken
// only). nowMs is MAIN's clock. ok=false is an opaque refusal (→ 404).
func (a *Agent) admitAdmin(paramStreamID int64, r *http.Request, clientIP string, passwordOK bool, nowMs int64) (adminAdmission, bool) {
	q := r.URL.Query()
	// A present uitoken is decided on its own — valid admits, invalid refuses; it
	// never falls back to the RelayGuard arms (the handlers check it first).
	if ui := q.Get("uitoken"); ui != "" {
		keys, acceptLegacy, subnet, ok := a.viewerKeys()
		if !ok {
			return adminAdmission{}, false
		}
		at, ok := decodeAdminToken(ui, keys, acceptLegacy, nowMs/1000)
		if !ok || !at.valid(subnet, clientIP, nowMs/1000) {
			return adminAdmission{}, false
		}
		return adminAdmission{kind: admitUitoken, streamID: int64(at.StreamID), tok: at}, true
	}
	// Relay credentials present → the relay arm alone (a bad ticket never degrades
	// to the password).
	if r.Header.Get(relayTicketHeader) != "" || r.Header.Get(relayAuthHeader) != "" {
		if paramStreamID > 0 && a.admitRelay(paramStreamID, r, nowMs) {
			return adminAdmission{kind: admitRelayKind, streamID: paramStreamID}, true
		}
		return adminAdmission{}, false
	}
	if !passwordOK {
		return adminAdmission{}, false
	}
	pw := q.Get("password")
	if strings.HasPrefix(pw, "lb1-") {
		if paramStreamID > 0 && a.admitLoopback(paramStreamID, pw, clientIP, nowMs/1000) {
			return adminAdmission{kind: admitLoopbackKind, streamID: paramStreamID}, true
		}
		return adminAdmission{}, false
	}
	if paramStreamID > 0 && a.admitPassword(pw, clientIP) {
		return adminAdmission{kind: admitPasswordKind, streamID: paramStreamID}, true
	}
	return adminAdmission{}, false
}

// streamMeta is the per-stream fields the /admin handlers read from the node's
// replica record (replica/streams/<id>.json), beyond streamOnDemand's on_demand:
// target_container (vod MIME), vframes_server_id (thumb owner), direct_source and
// type.live (the StreamSource row gates). Absent sub-blocks leave their fields 0/"".
type streamMeta struct {
	OnDemand        int64
	TargetContainer string
	VframesServerID int64
	DirectSource    int64
	TypeLive        int64
}

// streamMeta reads the stream's replica record; ok is false when it is
// missing/unreadable (→ 404, as StreamSource would with no row).
func (a *Agent) streamMeta(id string) (streamMeta, bool) {
	b, err := os.ReadFile(filepath.Join(streamsDir(a.ReplicaDir), id+".json"))
	if err != nil {
		return streamMeta{}, false
	}
	var w struct {
		Data struct {
			Stream *struct {
				TargetContainer string `json:"target_container"`
				VframesServerID int64  `json:"vframes_server_id"`
				DirectSource    int64  `json:"direct_source"`
			} `json:"stream"`
			Server *struct {
				OnDemand int64 `json:"on_demand"`
			} `json:"server"`
			Type *struct {
				Live int64 `json:"live"`
			} `json:"type"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &w) != nil {
		return streamMeta{}, false
	}
	var m streamMeta
	if w.Data.Server != nil {
		m.OnDemand = w.Data.Server.OnDemand
	}
	if w.Data.Stream != nil {
		m.TargetContainer = w.Data.Stream.TargetContainer
		m.VframesServerID = w.Data.Stream.VframesServerID
		m.DirectSource = w.Data.Stream.DirectSource
	}
	if w.Data.Type != nil {
		m.TypeLive = w.Data.Type.Live
	}
	return m, true
}
