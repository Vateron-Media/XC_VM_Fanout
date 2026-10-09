package gateway

import (
	"crypto/rand"
	"math/big"
	"net"
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
}

var (
	uuidRe        = regexp.MustCompile(`^[0-9a-f]{32}$`)
	archiveNameRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}:\d{2}-\d{2}\.ts$`)
)

func verdict(a Action, reason string) Verdict { return Verdict{Action: a, Reason: reason} }

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
		return verdict(PHP, "lease")
	}
	plain, v := p.Keys.Read(token, p.RawKeys.AcceptLegacyCBC, now)
	switch v {
	case Unsure:
		return verdict(PHP, "token-form")
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
		return verdict(PHP, "stream-id")
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
		return verdict(PHP, "stream-id")
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
		return verdict(PHP, "lease")
	}
	plain, v := p.Keys.Read(token, p.RawKeys.AcceptLegacyCBC, now)
	switch v {
	case Unsure:
		return verdict(PHP, "token-form")
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
		return verdict(PHP, "stream-id")
	}
	return Verdict{Action: Serve, Reason: "key", Stream: stream}
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

// ipMatch is segment.php's address check: the whole address, or with
// ip_subnet_match every dot-separated part but the last (which for an
// address without dots, IPv6, compares nothing, as PHP's does).
func ipMatch(tokenIP, clientIP string, subnet bool) bool {
	if !subnet {
		return tokenIP == clientIP
	}
	return dropLast(tokenIP) == dropLast(clientIP)
}

func dropLast(ip string) string {
	if i := strings.LastIndexByte(ip, '.'); i >= 0 {
		return ip[:i]
	}
	return ""
}
