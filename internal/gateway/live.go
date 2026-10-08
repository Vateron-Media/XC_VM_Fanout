package gateway

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// LiveEnv is what judging a playlist refresh asks of the node.
type LiveEnv struct {
	// StreamAlive is ProcessManager::isStreamAlive on the stream's producer.
	StreamAlive func(stream int, tokenPID json.RawMessage) bool
	// OnDisk: does the stream's own playlist (STREAMS_PATH/<id>_.m3u8) exist?
	OnDisk func(stream int) bool
	// Playlist is fanout's HLS playlist of a stream on air, "" otherwise.
	Playlist func(stream int) string
	// Record is the viewer's record in the agent: found, and whether the agent answered.
	Record func(uuid string) (rec map[string]json.RawMessage, found, ok bool)
	// AgentAlive: is the agent draining its spool (EventSpool::agentAlive)?
	AgentAlive func() bool
}

// Refresh is a playlist refresh the gateway answers: what serving it writes and builds.
type Refresh struct {
	Stream    int
	UUID      string
	Record    map[string]json.RawMessage
	ServerID  json.RawMessage
	Playlist  string
	UserAgent string // as live.php holds it: htmlentities(trim(UA))
	NoBuffer  bool   // use_buffer off: X-Accel-Buffering: no
	// The segment and key tokens' fields (tokenizeDaemonPlaylist).
	HMAC       string // "" for a line
	Identifier string
	Username   string
	Password   string
	Codec      string
	OnDemand   int
	// conn.limit's fields (StreamAuth::validateConnections); Limit false: no limit.
	Limit       bool
	UserID      int64
	MaxConns    int64
	IdentityKey string
}

var segLineRe = regexp.MustCompile(`(?m)^(\d+)\.ts$`)

// JudgeLive is live.php's answer to an HLS playlist refresh as far as the
// gateway owns it: a known viewer's next playlist on a node whose viewers
// are in its agent. Anything else goes to PHP before anything is written:
// the first request (admission, the connection's creation), a stream that is
// not running, a proxied or adaptive link, a line under the second-address
// rule, and any form PHP could read otherwise.
func JudgeLive(p *Policy, token, clientIP, rawUA string, now int64, env LiveEnv) (Verdict, *Refresh) {
	php := func(why string) (Verdict, *Refresh) { return verdict(PHP, why), nil }
	if p == nil {
		return php("policy")
	}
	if p.ConnStore != "agent" {
		return php("conn-store")
	}
	if p.ServeUntil != 0 && now > p.ServeUntil {
		return php("lease")
	}
	if p.Live.UniqueHeader {
		return php("unique-header")
	}
	plain, v := p.Keys.Read(token, p.RawKeys.AcceptLegacyCBC, now)
	if v != Opened {
		return php("token")
	}
	var t map[string]json.RawMessage
	if json.Unmarshal(plain, &t) != nil || t == nil {
		return php("token")
	}
	set := func(m map[string]json.RawMessage, k string) bool { r, ok := m[k]; return ok && !isNull(r) }
	if set(t, "video_path") || set(t, "off_air") || set(t, "adaptive") {
		return php("off-air")
	}
	if u, ok := t["uuid"]; ok {
		if s, isStr := jsonString(u); !isStr || !uuidRe.MatchString(s) {
			return php("token")
		}
	}
	if set(t, "expires") {
		exp, ok := jsonInt(t["expires"])
		if !ok || exp < now-p.TimeOffset {
			return php("expired")
		}
	}
	if ext, _ := jsonString(t["extension"]); ext != "m3u8" {
		return php("extension")
	}
	var ch, ui map[string]json.RawMessage
	if json.Unmarshal(t["channel_info"], &ch) != nil || len(ch) == 0 || json.Unmarshal(t["user_info"], &ui) != nil || ui == nil {
		return php("token")
	}
	if phpTruthy(ch["proxy"]) {
		return php("proxy")
	}
	stream, ok := jsonInt(t["stream_id"])
	if !ok || stream <= 0 || stream > math.MaxInt32 {
		return php("stream-id")
	}
	r := &Refresh{Stream: int(stream), NoBuffer: !p.Live.UseBuffer}

	// The serving server and the proxy in front of it (live.php's channel block).
	r.ServerID = json.RawMessage(strconv.Itoa(p.ServerID))
	var proxy json.RawMessage
	if phpTruthy(ch["originator_id"]) {
		r.ServerID, proxy = ch["originator_id"], ch["redirect_id"]
	} else if phpTruthy(ch["redirect_id"]) {
		r.ServerID = ch["redirect_id"]
	}
	if phpTruthy(proxy) {
		return php("proxy")
	}
	onDemand, ok := jsonInt(ch["on_demand"])
	if !ok && set(ch, "on_demand") {
		return php("token")
	}
	r.OnDemand = int(onDemand)
	if p.Live.InstantOff && onDemand == 1 {
		return php("instant-off")
	}
	if !env.StreamAlive(r.Stream, ch["pid"]) {
		return php("stream-down")
	}
	if !env.OnDisk(r.Stream) {
		return php("no-playlist")
	}
	if applies, sure := p.Live.secondIPRule(ui); applies || !sure {
		return php("second-ip")
	}

	// The viewer: a line, or an HMAC identity.
	if set(t, "hmac_id") {
		h, ok := jsonInt(t["hmac_id"])
		ident, identOK := phpString(t["identifier"])
		if !ok || !identOK {
			return php("token")
		}
		r.HMAC, r.Identifier = strconv.FormatInt(h, 10), ident
		r.IdentityKey = "h" + r.HMAC + "_" + ident
	} else {
		user, okU := phpString(t["username"])
		pass, okP := phpString(t["password"])
		id, okID := jsonInt(ui["id"])
		if !okU || !okP || (!okID && set(ui, "id")) {
			return php("token")
		}
		r.Username, r.Password, r.UserID = user, pass, id
		r.IdentityKey = "u" + strconv.FormatInt(id, 10)
	}
	codec, ok := phpString(t["video_codec"])
	if !ok {
		return php("token")
	}
	r.Codec = codec
	ua, ok := htmlentitiesASCII(rawUA)
	if !ok {
		return php("user-agent")
	}
	r.UserAgent = ua
	r.UUID = hlsConnectionKey(r.IdentityKey, r.Stream, clientIP, ua)

	// The connection: open, this viewer's, on this server and stream (lookupLive in the agent).
	rec, found, answered := env.Record(r.UUID)
	if !answered {
		return php("agent")
	}
	if !found || !liveMatches(rec, r) {
		return php("new-connection")
	}
	if !set(rec, "identity") || !set(rec, "uuid") {
		return php("record")
	}
	recIP, _ := phpString(rec["user_ip"])
	if p.RestrictSameIP && !ipMatch(recIP, clientIP, p.IPSubnetMatch) {
		return php("ip")
	}
	r.Record = rec

	// The line's limit is MAIN's, told through the agent's spool; a stopped agent means PHP's own limiter.
	if mc, ok := jsonInt(ui["max_connections"]); ok && mc != 0 {
		if !env.AgentAlive() {
			return php("spool")
		}
		r.Limit, r.MaxConns = true, mc
	} else if !ok && phpTruthy(ui["max_connections"]) {
		return php("token")
	}

	r.Playlist = env.Playlist(r.Stream)
	if r.Playlist == "" || !segLineRe.MatchString(r.Playlist) {
		return php("not-on-air")
	}
	return Verdict{Action: Serve, Reason: "playlist", Stream: r.Stream, UUID: r.UUID}, r
}

// liveMatches is ConnectionTracker::liveMatches for an open HLS record.
func liveMatches(rec map[string]json.RawMessage, r *Refresh) bool {
	if phpTruthy(rec["hls_end"]) {
		return false
	}
	if c, _ := phpString(rec["container"]); c != "hls" {
		return false
	}
	str := func(k string) string { s, _ := phpString(rec[k]); return s }
	server, _ := phpString(r.ServerID)
	if r.HMAC != "" {
		if str("hmac_id") != r.HMAC || str("hmac_identifier") != r.Identifier {
			return false
		}
	} else if str("user_id") != strconv.FormatInt(r.UserID, 10) {
		return false
	}
	return str("server_id") == server && str("stream_id") == strconv.Itoa(r.Stream)
}

// hlsConnectionKey is ConnectionTracker::hlsConnectionKey with live.php's
// identity ("u<user id>" or "h<hmac id>_<identifier>").
func hlsConnectionKey(identity string, stream int, ip, ua string) string {
	sum := md5.Sum([]byte("hls#" + identity + "#" + strconv.Itoa(stream) + "#" + ip + "#" + ua))
	return hex.EncodeToString(sum[:])
}

// htmlentitiesASCII is live.php's user agent, htmlentities(trim(UA)), for an
// ASCII one: false for any other byte, which PHP's entity table may turn into
// a name (the request is PHP's then).
func htmlentitiesASCII(ua string) (string, bool) {
	if ua == "" || ua == "0" { // PHP's empty()
		return "", true
	}
	ua = strings.Trim(ua, " \t\n\r\x00\x0b")
	var b strings.Builder
	for i := 0; i < len(ua); i++ {
		c := ua[i]
		switch {
		case c >= 0x80:
			return "", false
		case c == '&':
			b.WriteString("&amp;")
		case c == '"':
			b.WriteString("&quot;")
		case c == '\'':
			b.WriteString("&#039;")
		case c == '<':
			b.WriteString("&lt;")
		case c == '>':
			b.WriteString("&gt;")
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), true
}

// secondIPRule is live.php's disallow_2nd_ip_con condition for this line:
// whether it applies (PHP then checks the line's other addresses), and
// whether the values could be read the way PHP reads them.
func (l LivePolicy) secondIPRule(ui map[string]json.RawMessage) (applies, sure bool) {
	if !l.Disallow2ndIP {
		return false, true
	}
	if phpTruthy(ui["is_restreamer"]) {
		return false, true
	}
	mc, ok := jsonInt(ui["max_connections"])
	if !ok && phpTruthy(ui["max_connections"]) {
		return false, false
	}
	return (mc <= l.Disallow2ndIPMax && mc > 0) || l.Disallow2ndIPMax == 0, true
}

// isStreamAlive is ProcessManager::isStreamAlive: a process (pid > 1) whose
// command line names the stream.
func isStreamAlive(pid int64, stream int) bool {
	if pid <= 1 {
		return false
	}
	base := "/proc/" + strconv.FormatInt(pid, 10)
	if fi, err := os.Lstat(base + "/exe"); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		return false
	}
	cmd, err := os.ReadFile(base + "/cmdline")
	if err != nil {
		return false
	}
	return bytes.Contains(bytes.ToLower(bytes.ReplaceAll(cmd, []byte{0}, []byte{' '})), []byte(strconv.Itoa(stream)))
}

// phpIntval is intval() on a pid file's contents: leading space, then digits.
func phpIntval(b []byte) int64 {
	s := strings.TrimLeft(string(b), " \t\n\r\v\f")
	n := 0
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		n++
	}
	v, _ := strconv.ParseInt(s[:n], 10, 64)
	return v
}

// ── JSON as PHP's json_decode and casts read it ─────────────────────────

func isNull(r json.RawMessage) bool { return len(r) == 0 || string(r) == "null" }

// jsonString is a JSON string's value: the bytes between the quotes when
// nothing in it is escaped (most values), else json's reading of it.
func jsonString(r json.RawMessage) (string, bool) {
	if len(r) >= 2 && r[0] == '"' && r[len(r)-1] == '"' && bytes.IndexByte(r[1:len(r)-1], '\\') < 0 && bytes.IndexByte(r[1:len(r)-1], '"') < 0 {
		return string(r[1 : len(r)-1]), true
	}
	var s string
	return s, json.Unmarshal(r, &s) == nil
}

// jsonInt is an integer as a JSON integer or a string of digits; missing or
// null is 0. Anything else (a float, other text) is not read here.
func jsonInt(r json.RawMessage) (int64, bool) {
	if isNull(r) {
		return 0, true
	}
	if s, ok := jsonString(r); ok {
		n, ok := digits(s)
		return int64(n), ok
	}
	n, err := strconv.ParseInt(string(r), 10, 64)
	return n, err == nil
}

// phpString is (string) of a decoded JSON value: a string, an integer, a
// boolean ("1" or ""), null (""). Floats and containers are not read here.
func phpString(r json.RawMessage) (string, bool) {
	if isNull(r) {
		return "", true
	}
	if s, ok := jsonString(r); ok {
		return s, true
	}
	switch string(r) {
	case "true":
		return "1", true
	case "false":
		return "", true
	}
	if n, err := strconv.ParseInt(string(r), 10, 64); err == nil {
		return strconv.FormatInt(n, 10), true
	}
	return "", false
}

// phpTruthy is PHP's boolean value of a decoded JSON value.
func phpTruthy(r json.RawMessage) bool {
	return phpNonEmpty(r)
}
