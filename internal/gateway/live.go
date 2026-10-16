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
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/server"
)

// LiveEnv is what judging a playlist refresh asks of the node.
type LiveEnv struct {
	// StreamAlive is ProcessManager::isStreamAlive on the stream's producer.
	StreamAlive func(stream int, tokenPID json.RawMessage) bool
	// OnDisk: does the stream's own playlist (STREAMS_PATH/<id>_.m3u8) exist?
	OnDisk func(stream int) bool
	// Playlist is fanout's HLS playlist of a stream on air, "" otherwise.
	Playlist func(stream int) string
	// Fed is FanoutClient::isStreamFed: is fanout serving the stream (has_data)?
	Fed func(stream int) bool
	// Record is the viewer's record in the agent: found, and whether the agent answered.
	Record func(uuid string) (rec map[string]json.RawMessage, found, ok bool)
	// AgentAlive: is the agent draining its spool (EventSpool::agentAlive)?
	AgentAlive func() bool
	// File: does this path exist (vod.php's file_exists on the movie)?
	File func(path string) bool
	// FileSize is a file's size and whether it is there (timeshift's archive scan).
	FileSize func(path string) (int64, bool)
	// Offset reads a minute file's "<path>.offset" (timeshift's first partial minute), 0 when absent.
	Offset func(path string) int64
}

// Refresh is a playlist refresh (or a TS viewer's reconnect) the gateway
// answers: what serving it writes and builds.
type Refresh struct {
	// TS: an MPEG-TS viewer's reconnect, served from fanout's ring as
	// live.php's X-Accel-Redirect to /xc_fanout/<id> is; else a playlist.
	TS bool
	// Prebuffer is the seconds of history a TS viewer joins with (live.php's ?prebuffer=).
	Prebuffer int64
	Stream    int
	UUID      string
	Record    map[string]json.RawMessage
	// New: a TS viewer's first request. Record is then the record to store
	// (ConnectionTracker::createLive), and Admission the agent's
	// X-XCVM-Admission for it ("" for a plain register).
	New       bool
	Admission string
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
	// VOD's serve (vod.php handOverFile): the movie file the daemon serves, its
	// content type, the throttle (past LimitPerc of the response, at Rate bytes/s)
	// and a Range the token carried (used when the request has none).
	Path        string
	ContentType string
	LimitPerc   int
	Rate        int64
	Range       string
	// Parts is timeshift's run of archive minutes (the queue, the first from
	// its .offset), served as one response.
	Parts []server.FilePart
}

var segLineRe = regexp.MustCompile(`(?m)^(\d+)\.ts$`)

// JudgeLive is live.php's answer to an HLS playlist refresh or an MPEG-TS
// viewer as far as the gateway owns it: a known viewer's next playlist, or a
// TS viewer's stream from fanout (a new viewer's connection is created
// first, a known one's refreshed), on a node whose viewers are in its agent.
// Anything else goes to PHP before anything is written: an HLS viewer's first
// request, a stream that is not running, a proxied or adaptive link, a line
// under the second-address rule, and any form PHP could read otherwise.
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
	ext, _ := jsonString(t["extension"])
	if ext != "m3u8" && ext != "ts" {
		return php("extension")
	}
	if ext == "ts" && !p.Live.TS {
		// A panel from before the TS reconnect writes no prebuffer settings.
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
	r := &Refresh{Stream: int(stream), NoBuffer: !p.Live.UseBuffer, TS: ext == "ts"}

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
	container := "hls"
	if r.TS {
		// A TS connection keeps the uuid MAIN put in its token (live.php keys
		// only HLS on the viewer).
		u, isStr := jsonString(t["uuid"])
		if !isStr || !uuidRe.MatchString(u) {
			return php("token")
		}
		r.UUID, container = u, "ts"
	} else {
		r.UUID = hlsConnectionKey(r.IdentityKey, r.Stream, clientIP, ua)
	}

	// The connection: open, this viewer's, on this server and stream (lookupLive in the agent).
	rec, found, answered := env.Record(r.UUID)
	if !answered {
		return php("agent")
	}
	if !found && p.Live.CreateExpiration > 0 {
		// A viewer's first request: its connection is created (createLive). A TS
		// viewer is keyed on the token's uuid, an HLS one on its playlist key.
		var why string
		if r.TS {
			rec, why = newTSRecord(p, t, ch, ui, r, clientIP, now)
		} else {
			rec, why = newHLSRecord(p, t, ch, ui, r, clientIP, now)
		}
		if why != "" {
			return php(why)
		}
		r.New = true
	} else {
		// An ended connection is re-opened by PHP (its lookup is not open-only).
		if !found || !liveMatches(rec, r, container) {
			return php("new-connection")
		}
		if !set(rec, "identity") || !set(rec, "uuid") {
			return php("record")
		}
		if r.TS {
			// live.php kills a PHP worker still feeding this connection; the
			// gateway takes over only from fanout's own (pid 0).
			if pid, ok := jsonInt(rec["pid"]); !ok || pid != 0 {
				return php("worker")
			}
		}
		recIP, _ := phpString(rec["user_ip"])
		if p.RestrictSameIP && !ipMatch(recIP, clientIP, p.IPSubnetMatch) {
			return php("ip")
		}
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

	if r.New {
		adm, ok := admissionHeader(p, t, r, clientIP, now)
		if !ok {
			return php("token")
		}
		r.Admission = adm
	}

	if r.TS {
		if !env.Fed(r.Stream) {
			return php("not-on-air")
		}
		// live.php's ?prebuffer=: a restreamer's link asking for one gets a segment's time.
		r.Prebuffer = p.Live.ClientPrebuffer
		if phpTruthy(ui["is_restreamer"]) {
			r.Prebuffer = p.Live.RestreamerPrebuffer
			if phpTruthy(t["prebuffer"]) {
				r.Prebuffer = p.Live.SegTime
			}
		}
		reason := "ts"
		if r.New {
			reason = "ts-new"
		}
		return Verdict{Action: Serve, Reason: reason, Stream: r.Stream, UUID: r.UUID, Codec: r.Codec}, r
	}
	r.Playlist = env.Playlist(r.Stream)
	if r.Playlist == "" || !segLineRe.MatchString(r.Playlist) {
		return php("not-on-air")
	}
	reason := "playlist"
	if r.New {
		reason = "playlist-new"
	}
	return Verdict{Action: Serve, Reason: reason, Stream: r.Stream, UUID: r.UUID}, r
}

var mintProofRe = regexp.MustCompile(`^[0-9a-f]{32}$`)

// vodExtRe is a movie file's extension as it goes into VOD_PATH/<id>.<ext>:
// plain alphanumerics, so it names nothing but a file beside the others.
var vodExtRe = regexp.MustCompile(`^[A-Za-z0-9]+$`)

// vodContentType is vod.php's map from the channel's target_container to a MIME
// type, application/octet-stream for anything else.
func vodContentType(ch map[string]json.RawMessage) string {
	c, _ := phpString(ch["target_container"])
	switch c {
	case "mp4", "m4v":
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
	case "mpg", "mpeg":
		return "video/mpeg"
	}
	return "application/octet-stream"
}

// vodThrottle is vod.php's download pacing: limit_perc is passed through as the
// setting; the rate (bytes/s once past that share) is the movie's bitrate plus
// vod_bitrate_plus percent, but 0 (no throttle) without a limit or for a
// restreamer.
func vodThrottle(p *Policy, ch, ui map[string]json.RawMessage) (int, int64) {
	limitPerc := p.VodLimitPerc
	if limitPerc <= 0 || phpTruthy(ui["is_restreamer"]) {
		return limitPerc, 0
	}
	bitrate, _ := jsonInt(ch["bitrate"])
	download := float64(bitrate * 125)
	download += download * float64(p.VodBitratePlus) * 0.01
	return limitPerc, int64(download)
}

// JudgeVOD is vod.php's decision on /vauth/<token> as far as the gateway owns
// it: a local movie file (VOD_PATH/<id>.<ext>) the fanout daemon serves
// (handOverFile), the viewer tracked in the agent with pid 0, as on a modern
// fanout node. A direct-proxy movie, a connection a PHP worker still feeds, a
// node with send_unique_header, or any form PHP reads otherwise goes to PHP.
func JudgeVOD(p *Policy, token, clientIP, rawUA string, now int64, env LiveEnv) (Verdict, *Refresh) {
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
	if p.Headers.UniqueHeader {
		// vod.php's sendStreamHeaders writes a fresh send_unique_header cookie.
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
	// A VOD link names the file's extension; one that does not, or an odd one,
	// is another endpoint's or PHP's.
	ext, ok := phpString(t["extension"])
	if !ok || !vodExtRe.MatchString(ext) {
		return php("extension")
	}
	var ch, ui map[string]json.RawMessage
	if json.Unmarshal(t["channel_info"], &ch) != nil || len(ch) == 0 || json.Unmarshal(t["user_info"], &ui) != nil || ui == nil {
		return php("token")
	}
	if phpTruthy(ch["proxy"]) {
		// A direct-proxy movie: PHP relays its source.
		return php("proxy")
	}
	stream, ok := jsonInt(t["stream_id"])
	if !ok || stream <= 0 || stream > math.MaxInt32 {
		return php("stream-id")
	}
	r := &Refresh{Stream: int(stream)}
	// The serving server; the gateway owns only the simple, non-proxied case
	// (an originator/redirect proxy is PHP's, which tracks the proxied source).
	r.ServerID = json.RawMessage(strconv.Itoa(p.ServerID))
	if phpTruthy(ch["originator_id"]) {
		return php("proxy")
	} else if phpTruthy(ch["redirect_id"]) {
		r.ServerID = ch["redirect_id"]
	}
	// The viewer: a line, or an HMAC identity.
	if set(t, "hmac_id") {
		h, okH := jsonInt(t["hmac_id"])
		ident, identOK := phpString(t["identifier"])
		if !okH || !identOK {
			return php("token")
		}
		r.HMAC, r.Identifier = strconv.FormatInt(h, 10), ident
		r.IdentityKey = "h" + r.HMAC + "_" + ident
	} else {
		id, okID := jsonInt(ui["id"])
		if !okID && set(ui, "id") {
			return php("token")
		}
		r.UserID, r.IdentityKey = id, "u"+strconv.FormatInt(id, 10)
	}
	ua, ok := htmlentitiesASCII(rawUA)
	if !ok {
		return php("user-agent")
	}
	r.UserAgent = ua
	// VOD keeps the token's own uuid (not an HLS playlist key).
	u, isStr := jsonString(t["uuid"])
	if !isStr || !uuidRe.MatchString(u) {
		return php("token")
	}
	r.UUID = u
	// The movie on disk; missing (and not a direct proxy) is PHP's VOD_DOESNT_EXIST.
	path := p.Paths.Vod + strconv.Itoa(int(stream)) + "." + ext
	if !env.File(path) {
		return php("missing")
	}
	r.Path, r.ContentType = path, vodContentType(ch)
	r.Range, _ = phpString(t["http_range"])

	// The connection, under the token's uuid (findByUuid).
	rec, found, answered := env.Record(r.UUID)
	if !answered {
		return php("agent")
	}
	if !found {
		var why string
		if rec, why = newVODRecord(p, t, ch, ui, r, clientIP, now); why != "" {
			return php(why)
		}
		r.New = true
	} else {
		if !liveMatches(rec, r, "VOD") {
			return php("new-connection")
		}
		if !set(rec, "identity") || !set(rec, "uuid") {
			return php("record")
		}
		// The gateway takes over only a connection fanout already serves (pid 0);
		// one a PHP worker still feeds (vod.php may kill it) is PHP's.
		if pid, okP := jsonInt(rec["pid"]); !okP || pid != 0 {
			return php("worker")
		}
		recIP, _ := phpString(rec["user_ip"])
		if p.RestrictSameIP && !ipMatch(recIP, clientIP, p.IPSubnetMatch) {
			return php("ip")
		}
	}
	r.Record = rec
	// The line's limit via the agent's spool; a stopped agent is PHP's limiter.
	if mc, okM := jsonInt(ui["max_connections"]); okM && mc != 0 {
		if !env.AgentAlive() {
			return php("spool")
		}
		r.Limit, r.MaxConns = true, mc
	} else if !okM && phpTruthy(ui["max_connections"]) {
		return php("token")
	}
	if r.New {
		adm, okA := admissionHeader(p, t, r, clientIP, now)
		if !okA {
			return php("token")
		}
		r.Admission = adm
	}
	r.LimitPerc, r.Rate = vodThrottle(p, ch, ui)
	reason := "vod"
	if r.New {
		reason = "vod-new"
	}
	return Verdict{Action: Serve, Reason: reason, Stream: int(stream), UUID: r.UUID}, r
}

// tsStartRe is timeshift.php's accepted start: a Unix time, YYYYMMDD-HH or
// YYYY-MM-DD:HH-MM (with optional seconds, ignored).
var tsStartRe = regexp.MustCompile(`^(\d+|\d{8}-\d{1,2}|\d{4}-\d{1,2}-\d{1,2}:\d{1,2}-\d{1,2}(?:[:-]\d{1,2})?)$`)

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// parseTimeshiftStart reads a start as timeshift.php does: a Unix time (no
// timezone), or a YYYYMMDD-HH / YYYY-MM-DD:HH-MM date parsed in loc (mktime's
// timezone), to the minute it names. false for any other form (PHP's NO_TIMESTAMP).
func parseTimeshiftStart(start string, loc *time.Location) (int64, bool) {
	if !tsStartRe.MatchString(start) {
		return 0, false
	}
	if isAllDigits(start) {
		n, err := strconv.ParseInt(start, 10, 64)
		return n, err == nil
	}
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	var year, month, day, hour, min int
	if strings.Count(start, "-") == 1 {
		// YYYYMMDD-HH: PHP reads year/month/day by substring, the hour after the dash.
		d := strings.SplitN(start, "-", 2)
		if len(d[0]) != 8 {
			return 0, false
		}
		year, month, day, hour = atoi(d[0][0:4]), atoi(d[0][4:6]), atoi(d[0][6:8]), atoi(d[1])
	} else {
		// YYYY-MM-DD:HH-MM(:SS|-SS)?: PHP's explode(':') then explode('-'), first two of each.
		c := strings.Split(start, ":")
		dmy := strings.Split(c[0], "-")
		hm := strings.Split(c[1], "-")
		if len(dmy) != 3 || len(hm) < 2 {
			return 0, false
		}
		year, month, day, hour, min = atoi(dmy[0]), atoi(dmy[1]), atoi(dmy[2]), atoi(hm[0]), atoi(hm[1])
	}
	return time.Date(year, time.Month(month), day, hour, min, 0, 0, loc).Unix(), true
}

// gmdateMinute is timeshift.php's gmdate('Y-m-d:H-i') (UTC): the archive minute file's name.
func gmdateMinute(ts int64) string { return time.Unix(ts, 0).UTC().Format("2006-01-02:15-04") }

// JudgeTimeshift is timeshift.php's decision on /tsauth/<token> for the arm the
// gateway owns: the run of archive minute files the daemon serves (handOverFile),
// the viewer tracked in the agent with pid 0. The m3u8 playlist arm (it mints
// per-segment tokens), a proxy, a node with send_unique_header, a connection a
// PHP worker still feeds, and any form PHP reads otherwise go to PHP.
func JudgeTimeshift(p *Policy, token, clientIP, rawUA string, now int64, env LiveEnv) (Verdict, *Refresh) {
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
	if p.Headers.UniqueHeader {
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
	ext, ok := phpString(t["extension"])
	if !ok || ext == "m3u8" || !vodExtRe.MatchString(ext) {
		// m3u8 is the playlist arm (it mints per-segment tokens): PHP's.
		return php("extension")
	}
	var ui map[string]json.RawMessage
	if json.Unmarshal(t["user_info"], &ui) != nil || ui == nil {
		return php("token")
	}
	stream, ok := jsonInt(t["stream"])
	if !ok || stream <= 0 || stream > math.MaxInt32 {
		return php("stream-id")
	}
	duration, ok := jsonInt(t["duration"])
	if !ok || duration <= 0 {
		return php("timestamp")
	}
	// The serving server; the gateway owns only the simple, non-proxied case.
	r := &Refresh{Stream: int(stream)}
	r.ServerID = json.RawMessage(strconv.Itoa(p.ServerID))
	if phpTruthy(t["originator_id"]) {
		return php("proxy")
	} else if phpTruthy(t["redirect_id"]) {
		r.ServerID = t["redirect_id"]
	}
	// The start, to the archive minute it names (parsed in the node's timezone).
	start, _ := phpString(t["start"])
	var ts int64
	if isAllDigits(start) {
		if ts, _ = strconv.ParseInt(start, 10, 64); ts <= 0 {
			return php("timestamp")
		}
	} else {
		loc := time.UTC
		if p.DefaultTimezone != "" {
			l, err := time.LoadLocation(p.DefaultTimezone)
			if err != nil {
				return php("timezone")
			}
			loc = l
		}
		var okT bool
		if ts, okT = parseTimeshiftStart(start, loc); !okT || ts <= 0 {
			return php("timestamp")
		}
	}
	// The viewer: a line (timeshift has no HMAC identity).
	id, okID := jsonInt(ui["id"])
	if !okID && set(ui, "id") {
		return php("token")
	}
	r.UserID, r.IdentityKey = id, "u"+strconv.FormatInt(id, 10)
	ua, ok := htmlentitiesASCII(rawUA)
	if !ok {
		return php("user-agent")
	}
	r.UserAgent = ua
	u, isStr := jsonString(t["uuid"])
	if !isStr || !uuidRe.MatchString(u) {
		return php("token")
	}
	r.UUID = u

	// The archive: the first minute must be there, then the run of minutes up to
	// the duration, no later than the present (timeshift.php's scan).
	base := p.Paths.Archive + strconv.Itoa(int(stream)) + "/"
	first := base + gmdateMinute(ts) + ".ts"
	if _, present := env.FileSize(first); !present {
		return php("missing")
	}
	scan := duration
	if lim := now/60 - ts/60 + 1; lim < scan {
		scan = lim
	}
	offset := env.Offset(first)
	var parts []server.FilePart
	var total int64
	var count int
	for i := int64(0); i < scan; i++ {
		size, present := env.FileSize(base + gmdateMinute(ts+i*60) + ".ts")
		if !present {
			continue
		}
		total += size
		from := int64(0)
		if count == 0 {
			from = offset
		}
		count++
		if size > from {
			parts = append(parts, server.FilePart{Path: base + gmdateMinute(ts+i*60) + ".ts", Offset: from, Length: size - from})
		}
	}
	if count == 0 || len(parts) == 0 {
		return php("missing")
	}

	// The connection, under the token's uuid.
	rec, found, answered := env.Record(r.UUID)
	if !answered {
		return php("agent")
	}
	if !found {
		var why string
		if rec, why = newTimeshiftRecord(p, t, ui, r, clientIP, now, ext); why != "" {
			return php(why)
		}
		r.New = true
	} else {
		if !liveMatches(rec, r, ext) {
			return php("new-connection")
		}
		if !set(rec, "identity") || !set(rec, "uuid") {
			return php("record")
		}
		if pid, okP := jsonInt(rec["pid"]); !okP || pid != 0 {
			return php("worker")
		}
		recIP, _ := phpString(rec["user_ip"])
		if p.RestrictSameIP && !ipMatch(recIP, clientIP, p.IPSubnetMatch) {
			return php("ip")
		}
	}
	r.Record = rec
	if mc, okM := jsonInt(ui["max_connections"]); okM && mc != 0 {
		if !env.AgentAlive() {
			return php("spool")
		}
		r.Limit, r.MaxConns = true, mc
	} else if !okM && phpTruthy(ui["max_connections"]) {
		return php("token")
	}
	if r.New {
		adm, okA := admissionHeader(p, t, r, clientIP, now)
		if !okA {
			return php("token")
		}
		r.Admission = adm
	}
	// The recording's bitrate over the minutes it holds (timeshift.php), the
	// response served at it plus vod_bitrate_plus percent, past vod_limit_perc.
	rSize := total - offset
	download := float64(rSize) * 0.008 / float64(count*60) * 125
	download += download * float64(p.VodBitratePlus) * 0.01
	r.ContentType, r.Parts, r.Range = "video/mp2t", parts, ""
	r.LimitPerc = p.VodLimitPerc
	if p.VodLimitPerc > 0 && !phpTruthy(ui["is_restreamer"]) {
		r.Rate = int64(download)
	}
	if hr, okR := phpString(t["http_range"]); okR {
		r.Range = hr
	}
	reason := "timeshift"
	if r.New {
		reason = "timeshift-new"
	}
	return Verdict{Action: Serve, Reason: reason, Stream: int(stream), UUID: r.UUID}, r
}

// newTSRecord is the record createLive/openRecord store for a TS viewer fanout
// serves (pid 0): newLiveRecord's TS arm. A TS viewer is keyed on the token's
// own uuid (r.UUID), which is the reservation too.
func newTSRecord(p *Policy, t, ch, ui map[string]json.RawMessage, r *Refresh, clientIP string, now int64) (map[string]json.RawMessage, string) {
	return newLiveRecord(p, t, ch, ui, r, clientIP, now, "ts", json.RawMessage("0"), r.UUID, p.Live.CreateExpiration)
}

// newVODRecord is the record createLive/openRecord store for a VOD viewer the
// daemon serves (pid 0): newLiveRecord's VOD arm. Like a TS viewer, VOD keeps
// the token's own uuid (its reservation), so it stores no adm_uuid. vod.php
// fixes its create_expiration at 60 seconds (not the live setting).
func newVODRecord(p *Policy, t, ch, ui map[string]json.RawMessage, r *Refresh, clientIP string, now int64) (map[string]json.RawMessage, string) {
	rec, why := newLiveRecord(p, t, ch, ui, r, clientIP, now, "VOD", json.RawMessage("0"), r.UUID, 60)
	if why != "" {
		return nil, why
	}
	// vod.php's record hardcodes these (not from the token/channel like live.php).
	rec["external_device"], rec["on_demand"] = json.RawMessage(`""`), json.RawMessage("0")
	return rec, ""
}

// newTimeshiftRecord is the record timeshift.php's ts arm stores for a viewer
// the daemon serves (pid 0, container = the link's extension): newLiveRecord's
// fields, with external_device and on_demand hardcoded as timeshift.php does,
// and create_expiration fixed at 60. Like VOD/TS it keeps the token's own uuid.
func newTimeshiftRecord(p *Policy, t, ui map[string]json.RawMessage, r *Refresh, clientIP string, now int64, container string) (map[string]json.RawMessage, string) {
	rec, why := newLiveRecord(p, t, map[string]json.RawMessage{}, ui, r, clientIP, now, container, json.RawMessage("0"), r.UUID, 60)
	if why != "" {
		return nil, why
	}
	rec["external_device"], rec["on_demand"] = json.RawMessage(`""`), json.RawMessage("0")
	return rec, ""
}

// newHLSRecord is the record createLive/openRecord store for an HLS viewer's
// first request (its playlist served per request, pid null): newLiveRecord's
// HLS arm. live.php keys the record on a deterministic hlsConnectionKey
// (r.UUID) but reserves the viewer at mint under the token's own uuid, so that
// uuid is the reservation openRecord keeps as adm_uuid / names in mint.
func newHLSRecord(p *Policy, t, ch, ui map[string]json.RawMessage, r *Refresh, clientIP string, now int64) (map[string]json.RawMessage, string) {
	reserved, _ := jsonString(t["uuid"])
	return newLiveRecord(p, t, ch, ui, r, clientIP, now, "hls", json.RawMessage("null"), reserved, p.Live.CreateExpiration)
}

// newLiveRecord is the connection record ConnectionTracker::createLive and
// openRecord store for a new viewer, built from the token MAIN sealed as
// live.php builds it, with the arm's container and pid. The record is keyed on
// r.UUID; reserved is the uuid MAIN reserved at mint (r.UUID for a TS viewer,
// the token's own uuid for an HLS one, which openRecord keeps as adm_uuid when
// it differs from the record key and names in mint). "" or why PHP creates the
// connection ("expired": its TOKEN_EXPIRED, past the token's start plus
// create_expiration on MAIN's clock).
func newLiveRecord(p *Policy, t, ch, ui map[string]json.RawMessage, r *Refresh, clientIP string, now int64, container string, pid json.RawMessage, reserved string, createExp int64) (map[string]json.RawMessage, string) {
	set := func(m map[string]json.RawMessage, k string) bool { v, ok := m[k]; return ok && !isNull(v) }
	raw := func(v json.RawMessage) json.RawMessage {
		if len(v) == 0 {
			return json.RawMessage("null")
		}
		return v
	}
	str := func(s string) json.RawMessage { b, _ := json.Marshal(s); return b }
	start, ok := jsonInt(t["activity_start"])
	if !ok {
		return nil, "token"
	}
	if now-p.TimeOffset > start+createExp {
		return nil, "expired"
	}
	// The sealed token names no reservation of its own; live.php derives it from
	// the uuid. A token that carries adm_uuid is read by PHP.
	if set(t, "adm_uuid") {
		return nil, "token"
	}
	rec := map[string]json.RawMessage{
		"stream_id":          json.RawMessage(strconv.Itoa(r.Stream)),
		"server_id":          raw(r.ServerID),
		"proxy_id":           json.RawMessage("null"),
		"user_agent":         str(r.UserAgent),
		"user_ip":            str(clientIP),
		"container":          str(container),
		"pid":                pid,
		"date_start":         raw(t["activity_start"]),
		"geoip_country_code": raw(t["country_code"]),
		"isp":                raw(ui["con_isp_name"]),
		"external_device":    raw(t["external_device"]),
		"hls_end":            json.RawMessage("0"),
		"hls_last_read":      json.RawMessage(strconv.FormatInt(now-p.TimeOffset, 10)),
		"on_demand":          raw(ch["on_demand"]),
		"uuid":               str(r.UUID),
	}
	if r.HMAC != "" {
		// The identity is PHP's concatenation of the token's own values.
		h, _ := phpString(t["hmac_id"])
		if h != r.HMAC {
			return nil, "token"
		}
		rec["hmac_id"], rec["hmac_identifier"], rec["identity"] = raw(t["hmac_id"]), raw(t["identifier"]), str(h+"_"+r.Identifier)
	} else {
		if r.UserID <= 0 {
			return nil, "token"
		}
		rec["user_id"], rec["identity"] = ui["id"], ui["id"]
	}
	// openRecord keeps the reserved uuid as adm_uuid when the token carries an
	// admission claim and it differs from the record key (always for an HLS
	// viewer): MAIN releases the reservation by it when the viewer leaves
	// (ConnectionIngest). A TS viewer is keyed on the reservation itself, so none.
	if reserved != "" && reserved != r.UUID {
		var adm map[string]json.RawMessage
		if json.Unmarshal(t["adm"], &adm) == nil && adm != nil {
			rec["adm_uuid"] = str(reserved)
		}
	}
	// MAIN's proof that it minted the token (`prf`), kept as `mint` for the
	// record's life: <reserved uuid>.<iat>.<p>, stored only when the reservation
	// is a uuid.
	if uuidRe.MatchString(reserved) {
		var prf map[string]json.RawMessage
		if json.Unmarshal(t["prf"], &prf) == nil && prf != nil {
			iat, okI := strictInt(prf["iat"])
			proof, okP := jsonString(prf["p"])
			if okI && iat > 0 && okP && mintProofRe.MatchString(proof) {
				rec["mint"] = str(reserved + "." + strconv.FormatInt(iat, 10) + "." + proof)
			}
		}
	}
	return rec, ""
}

// admissionHeader is AgentConnections::admission for a new viewer's record:
// the agent's X-XCVM-Admission, "" when admission does not apply (no limit,
// or a node MAIN has not left active); false for a form PHP reads.
func admissionHeader(p *Policy, t map[string]json.RawMessage, r *Refresh, clientIP string, now int64) (string, bool) {
	if !r.Limit || r.MaxConns <= 0 || !p.Live.Admission {
		return "", true
	}
	server, ok := jsonInt(r.ServerID)
	if !ok {
		return "", false
	}
	out := map[string]any{"stream_id": r.Stream, "max_connections": r.MaxConns, "ip": clientIP, "ua": r.UserAgent}
	// MAIN's admission at mint, passed on only while it holds and only for this node.
	var adm map[string]json.RawMessage
	if json.Unmarshal(t["adm"], &adm) == nil && adm != nil {
		exp, okE := strictInt(adm["exp"])
		sid, okS := strictInt(adm["sid"])
		if okE && okS && exp >= now-p.TimeOffset && sid == server {
			out["adm"] = map[string]int64{"exp": exp, "sid": sid}
		}
	}
	if m, ok := r.Record["mint"]; ok {
		out["mint"] = m
	}
	if r.HMAC != "" {
		h, _ := jsonInt(json.RawMessage(r.HMAC))
		out["hmac_id"], out["identifier"] = h, r.Identifier
	} else {
		out["line_id"] = r.UserID
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if enc.Encode(out) != nil {
		return "", false
	}
	h := strings.TrimRight(b.String(), "\n")
	for i := 0; i < len(h); i++ {
		// A header of plain ASCII only: PHP escapes the rest its own way.
		if h[i] >= 0x80 {
			return "", false
		}
	}
	return h, true
}

// strictInt is PHP's is_int() on a decoded JSON value: an integer literal,
// not a string of digits.
func strictInt(r json.RawMessage) (int64, bool) {
	n, err := strconv.ParseInt(string(r), 10, 64)
	return n, err == nil
}

// liveMatches is ConnectionTracker::liveMatches for an open record of the container.
func liveMatches(rec map[string]json.RawMessage, r *Refresh, container string) bool {
	if phpTruthy(rec["hls_end"]) {
		return false
	}
	if c, _ := phpString(rec["container"]); c != container {
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
// identity ("u<user id>" or "h<hmac id>_<identifier>"). PHP's md5: the key
// names the viewer's record in the agent, which both sides must find.
func hlsConnectionKey(identity string, stream int, ip, ua string) string {
	// nosemgrep: go.lang.security.audit.crypto.use_of_weak_crypto.use-of-md5
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
