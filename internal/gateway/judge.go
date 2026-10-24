package gateway

import (
	"crypto/rand"
	"encoding/json"
	"math/big"
	"net"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Action is what the gateway does with a request.
type Action string

const (
	// Serve: the gateway answers it (from step 12.2; in shadow, PHP does).
	Serve Action = "serve"
	// Redirect: another server minted the token; 302 to it, as segment.php.
	Redirect Action = "redirect"
	// Deny: 404, as PHP answers it.
	Deny Action = "deny"
	// PHP: not proven equivalent, so PHP takes it (X-Accel-Redirect to its location).
	PHP Action = "php"
)

// Verdict is the gateway's decision on one request, with why.
type Verdict struct {
	Action   Action
	Reason   string
	Stream   int
	Seq      int64
	UUID     string
	Codec    string
	Location string
	// Path and Offset: a catch-up minute's file, served from that byte.
	Path   string
	Offset int64
	// WebVTT: a subtitle the token asked for as WebVTT (subtitle.php's convertVTT).
	WebVTT bool
}

// Env is what judging a segment asks of the node.
type Env struct {
	// Cons: is the viewer's connection marker there (CONS_TMP_PATH/<uuid>)?
	Cons func(uuid string) bool
	// File: is this catch-up minute's file there?
	File func(path string) bool
	// Heard is ConnectionTracker::heartbeat through the agent: whether the
	// viewer's record says the session ended, and whether the agent answered.
	Heard func(uuid string) (ended, ok bool)
	// Mtime is a file's modification time (unix seconds) and whether it is
	// there: thumb.php serves a thumbnail only while it is under a minute old.
	Mtime func(path string) (modTime int64, ok bool)
}

var (
	uuidRe        = regexp.MustCompile(`^[0-9a-f]{32}$`)
	archiveNameRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}:\d{2}-\d{2}\.ts$`)
)

func verdict(a Action, reason string) Verdict { return Verdict{Action: a, Reason: reason} }

// serveOnlyRejects are the reasons PHP answers with its own 404 (generate404, or
// generateError with no code: TOKEN_EXPIRED, NO_TIMESTAMP, IP_MISMATCH,
// VOD_DOESNT_EXIST). On a serve-only node there is no PHP behind the front, so
// deferring them would be a 502: the gateway denies them itself. The node holds
// the authoritative keys, so a token that does not open is definitively bad.
var serveOnlyRejects = map[string]bool{
	"token": true, "token-form": true, "token-json": true, "timestamp": true,
	"stream-id": true, "missing": true, "ip": true, "lease": true,
	"expired": true, "not-on-air": true, "stream": true, "stale": true,
}

// reject is the verdict for a request PHP would 404: a Deny on a serve-only
// node, the unchanged PHP hand-over on any other.
func reject(p *Policy, why string) Verdict {
	if p != nil && p.ServeOnly && serveOnlyRejects[why] {
		return verdict(Deny, why)
	}
	return verdict(PHP, why)
}

// JudgeSegment is segment.php's decision on /hls/<token> as far as the gateway
// owns it: a live daemon segment ("<id>_d<seq>.ts") and a catch-up minute are
// served, a token of another server redirected, what PHP refuses denied, and
// anything else (a form PHP's lenient parsing might read otherwise, a node
// whose viewers are not in its agent) goes to PHP.
func JudgeSegment(p *Policy, token, clientIP string, now int64, env Env) Verdict {
	if p == nil {
		return verdict(PHP, "policy")
	}
	if p.ServeUntil != 0 && now > p.ServeUntil {
		return reject(p, "lease")
	}
	plain, v := p.Keys.Read(token, p.RawKeys.AcceptLegacyCBC, now)
	switch v {
	case Unsure:
		return reject(p, "token-form")
	case Refused:
		return verdict(Deny, "token")
	}
	f := strings.Split(string(plain), "/")
	if len(f) < 6 {
		return verdict(Deny, "fields")
	}
	// A catch-up link begins with TS and has its start, not a connection id, in the sixth field.
	archive := f[0] == "TS" && !uuidRe.MatchString(f[5])
	var sid string
	switch {
	case archive && len(f) != 9:
		return verdict(Deny, "fields")
	case archive:
		sid = f[8]
	case len(f) < 7:
		return verdict(PHP, "fields")
	default:
		sid = f[6]
	}
	server, ok := digits(sid)
	if !ok {
		return verdict(PHP, "server-id")
	}
	if server != p.ServerID {
		bases := p.Redirect[strconv.Itoa(server)]
		if len(bases) == 0 || sid != strconv.Itoa(server) {
			return verdict(PHP, "server-unknown")
		}
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(bases))))
		if err != nil {
			return verdict(PHP, "rand")
		}
		return Verdict{Action: Redirect, Reason: "owner", Location: bases[n.Int64()] + "/hls/" + token}
	}
	if archive {
		return judgeArchive(p, f, clientIP, env)
	}
	stream, ok := digits(f[3])
	if !ok {
		return reject(p, "stream-id")
	}
	seq, isDaemon := daemonSeq(f[4], stream)
	if !isDaemon {
		// With fanout on (the gateway's precondition), PHP serves no other live segment.
		return verdict(Deny, "not-daemon")
	}
	uuid := f[5]
	if !uuidRe.MatchString(uuid) {
		return verdict(PHP, "uuid")
	}
	if !env.Cons(uuid) {
		return verdict(Deny, "connection")
	}
	if p.RestrictSameIP && !ipMatch(f[2], clientIP, p.IPSubnetMatch) {
		return verdict(Deny, "ip")
	}
	codec := "h264"
	if len(f) > 7 && f[7] != "" {
		codec = f[7]
	}
	return Verdict{Action: Serve, Reason: "daemon", Stream: stream, Seq: seq, UUID: uuid, Codec: codec}
}

// judgeArchive is segment.php's catch-up branch, on this server's token of
// nine fields: TS/user/pass/ip/duration/start/<stream>_<minute>_<offset>/uuid/server.
func judgeArchive(p *Policy, f []string, clientIP string, env Env) Verdict {
	parts := strings.Split(f[6], "_")
	if len(parts) < 2 {
		return verdict(PHP, "archive-fields")
	}
	stream, ok := digits(parts[0])
	if !ok {
		return reject(p, "stream-id")
	}
	var offset int64
	if len(parts) > 2 {
		o, ok := digits(parts[2])
		if !ok {
			return verdict(PHP, "offset")
		}
		offset = int64(o)
	}
	uuid := f[7]
	// Only a recorded minute (the name ArchiveCommand gives it), for a viewer a connection id names.
	if !archiveNameRe.MatchString(parts[1]) || !uuidRe.MatchString(uuid) {
		return verdict(Deny, "archive-name")
	}
	path := p.Paths.Archive + strconv.Itoa(stream) + "/" + parts[1]
	if !env.File(path) {
		return verdict(Deny, "archive-file")
	}
	if !env.Cons(uuid) {
		return verdict(Deny, "connection")
	}
	// Before the viewer is heard: a request refused for its address touches nothing.
	if p.RestrictSameIP && !ipMatch(f[3], clientIP, p.IPSubnetMatch) {
		return verdict(Deny, "ip")
	}
	// The viewer is heard (or refused once its session ended) in its store:
	// the agent's here; Redis or MySQL are PHP's.
	if p.ConnStore != "agent" {
		return verdict(PHP, "conn-store")
	}
	ended, ok := env.Heard(uuid)
	if !ok {
		return verdict(PHP, "agent")
	}
	if ended {
		return verdict(Deny, "ended")
	}
	return Verdict{Action: Serve, Reason: "archive", Stream: stream, UUID: uuid, Path: path, Offset: offset}
}

// BootstrapRefuses is what StreamingRequestBootstrap refuses before any
// stream endpoint runs: an address with a flood block marker (403), and with
// verify_host a host outside the allowed list (INVALID_HOST). Its reason, or
// "": PHP answers what it refuses, as before.
func BootstrapRefuses(p *Policy, clientIP, httpHost string, exists func(path string) bool) string {
	if net.ParseIP(clientIP) != nil && exists(p.Paths.Flood+"block_"+clientIP) {
		return "blocked"
	}
	if p.VerifyHost && len(p.AllowedDomains) > 0 {
		host := phpHost(httpHost)
		if host != "xc_vm" && net.ParseIP(host) == nil && !slices.Contains(p.AllowedDomains, host) {
			return "host"
		}
	}
	return ""
}

// phpHost is the bootstrap's HOST: the Host header up to its first colon, trimmed as PHP's trim().
func phpHost(h string) string {
	if i := strings.IndexByte(h, ':'); i >= 0 {
		h = h[:i]
	}
	return strings.Trim(h, " \t\n\r\x00\x0b")
}

// JudgeKey is key.php's decision on /key/<token>: the stream's key for the
// address the token names, else 404.
func JudgeKey(p *Policy, token, clientIP string, now int64) Verdict {
	if p == nil {
		return verdict(PHP, "policy")
	}
	if p.ServeUntil != 0 && now > p.ServeUntil {
		return reject(p, "lease")
	}
	plain, v := p.Keys.Read(token, p.RawKeys.AcceptLegacyCBC, now)
	switch v {
	case Unsure:
		return reject(p, "token-form")
	case Refused:
		return verdict(Deny, "token")
	}
	f := strings.Split(string(plain), "/")
	if len(f) < 2 {
		return verdict(Deny, "fields")
	}
	if p.RestrictSameIP && !ipMatch(f[0], clientIP, p.IPSubnetMatch) {
		return verdict(Deny, "ip")
	}
	stream, ok := digits(f[1])
	if !ok {
		return reject(p, "stream-id")
	}
	return Verdict{Action: Serve, Reason: "key", Stream: stream}
}

// JudgeThumb is thumb.php's decision on /thumb?token=…: the stream's current
// thumbnail (STREAMS_PATH/<id>_.jpg, kept only while it is under a minute old)
// for a token that names when it expires. A token that does not open, is not
// the JSON a thumbnail link carries, has no expiry or has passed it, names no
// stream, or whose thumbnail is missing or stale is PHP's — it emits the exact
// TOKEN_EXPIRED / THUMBNAIL_DOESNT_EXIST the gateway does not. The expiry is
// read against the node's clock less its time_offset, as the endpoint does;
// the one-minute freshness is the raw clock, as filemtime is.
func JudgeThumb(p *Policy, token string, now int64, env Env) Verdict {
	if p == nil {
		return verdict(PHP, "policy")
	}
	if p.ServeUntil != 0 && now > p.ServeUntil {
		return reject(p, "lease")
	}
	if p.Headers.UniqueHeader {
		// thumb.php sets a fresh random send_unique_header cookie on every
		// response; the gateway does not reproduce it (nor does JudgeLive), so a
		// node with it set hands thumbnails to PHP, which sets the cookie.
		return verdict(PHP, "unique-header")
	}
	plain, v := p.Keys.Read(token, p.RawKeys.AcceptLegacyCBC, now)
	if v != Opened {
		// Refused or Unsure: it is not a 404, so PHP answers (TOKEN_EXPIRED).
		return reject(p, "token")
	}
	var td struct {
		Expires *int64 `json:"expires"`
		Stream  int    `json:"stream"`
	}
	// A thumbnail link names when it expires; one that does not (or is another
	// endpoint's slash-field token) is not this endpoint's.
	if json.Unmarshal(plain, &td) != nil || td.Expires == nil {
		return reject(p, "token-json")
	}
	if *td.Expires < now-p.TimeOffset {
		return reject(p, "expired")
	}
	if td.Stream <= 0 {
		return reject(p, "stream")
	}
	path := p.Paths.Streams + strconv.Itoa(td.Stream) + "_.jpg"
	mod, ok := env.Mtime(path)
	if !ok || now-mod >= 60 {
		return reject(p, "stale")
	}
	return Verdict{Action: Serve, Reason: "thumb", Stream: td.Stream, Path: path}
}

// JudgeSubtitle is subtitle.php's decision on /subtitle?token=…: the VOD
// subtitle file (VOD_PATH/<stream_id>_<sub_id>.srt, as WebVTT when the token
// asks) for a token that names when it expires. Unlike a thumbnail there is no
// freshness window. A token that does not open, is not the JSON a subtitle link
// carries, has no expiry or has passed it, names no stream, or whose file is
// missing is PHP's — it emits the exact TOKEN_EXPIRED / THUMBNAIL_DOESNT_EXIST
// the gateway does not. The expiry is read against the node's clock less its
// time_offset, as the endpoint does.
func JudgeSubtitle(p *Policy, token string, now int64, env Env) Verdict {
	if p == nil {
		return verdict(PHP, "policy")
	}
	if p.ServeUntil != 0 && now > p.ServeUntil {
		return reject(p, "lease")
	}
	if p.Headers.UniqueHeader {
		// subtitle.php, like thumb.php, writes a fresh random cookie per response.
		return verdict(PHP, "unique-header")
	}
	plain, v := p.Keys.Read(token, p.RawKeys.AcceptLegacyCBC, now)
	if v != Opened {
		return reject(p, "token")
	}
	var td struct {
		Expires  *int64 `json:"expires"`
		StreamID int    `json:"stream_id"`
		SubID    int    `json:"sub_id"`
		WebVTT   int    `json:"webvtt"`
	}
	// A subtitle link names when it expires; one that does not (or another
	// endpoint's slash-field token) is not this endpoint's.
	if json.Unmarshal(plain, &td) != nil || td.Expires == nil {
		return reject(p, "token-json")
	}
	if *td.Expires < now-p.TimeOffset {
		return reject(p, "expired")
	}
	if td.StreamID <= 0 {
		return reject(p, "stream")
	}
	path := p.Paths.Vod + strconv.Itoa(td.StreamID) + "_" + strconv.Itoa(td.SubID) + ".srt"
	if !env.File(path) {
		return reject(p, "missing")
	}
	return Verdict{Action: Serve, Reason: "subtitle", Stream: td.StreamID, Seq: int64(td.SubID), WebVTT: td.WebVTT != 0, Path: path}
}

// convertVTT is subtitle.php's SRT-to-WebVTT: a "WEBVTT" header, then comma
// decimals turned to dots on each cue's timestamp line — the line after a blank
// line (the SRT cue separator), or the second line of the file.
func convertVTT(srt []byte) []byte {
	lines := strings.Split(string(srt), "\n")
	for i := 1; i < len(lines); i++ {
		if i == 1 || strings.TrimSpace(lines[i-2]) == "" {
			lines[i] = strings.ReplaceAll(lines[i], ",", ".")
		}
	}
	return []byte("WEBVTT\n\n" + strings.Join(lines, "\n"))
}

// digits is a non-negative decimal written only in digits (what PHP's
// intval and == read the same way); anything else is PHP's to read.
func digits(s string) (int, bool) {
	if s == "" || len(s) > 9 {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(s)
	return n, err == nil
}

// daemonSeq matches segment.php's "/^<stream>_d(\d+)\.ts$/".
func daemonSeq(name string, stream int) (int64, bool) {
	prefix := strconv.Itoa(stream) + "_d"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".ts") {
		return 0, false
	}
	n := name[len(prefix) : len(name)-len(".ts")]
	if n == "" {
		return 0, false
	}
	for i := 0; i < len(n); i++ {
		if n[i] < '0' || n[i] > '9' {
			return 0, false
		}
	}
	seq, err := strconv.ParseInt(n, 10, 64)
	return seq, err == nil
}

// ipMatch is the panel's address check (NetworkUtils::ipMatches): the whole
// address, or with ip_subnet_match the same subnet, the /24 of an IPv4
// address and the /64 of an IPv6 one (an IPv4-mapped address is its IPv4).
// One of each, or what is not an address, matches only when equal. It used to
// drop what followed the last dot, as PHP did: an IPv6 address has none, so
// any two of them matched.
func ipMatch(tokenIP, clientIP string, subnet bool) bool {
	if tokenIP == clientIP {
		return true
	}
	if !subnet {
		return false
	}
	a, okA := subnetOf(tokenIP)
	b, okB := subnetOf(clientIP)
	return okA && okB && a == b
}

// subnetOf is an address's /24 (IPv4) or /64 (IPv6); false for what is not an address.
func subnetOf(ip string) (netip.Prefix, bool) {
	addr, err := netip.ParseAddr(ip)
	if err != nil || addr.Zone() != "" {
		return netip.Prefix{}, false
	}
	addr = addr.Unmap()
	bits := 64
	if addr.Is4() {
		bits = 24
	}
	prefix, err := addr.Prefix(bits)
	return prefix, err == nil
}
