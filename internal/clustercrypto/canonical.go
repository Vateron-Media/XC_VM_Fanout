package clustercrypto

import (
	"crypto/hmac"
	"errors"
	"regexp"
	"sort"
	"strings"
)

// The canonical request/response contexts (panel-owned, ADR 0004):
//
//	req_ctx = "xcvm-req-v1" ‖ u32(proto) ‖ lp(agent) ‖ lp(METHOD) ‖ lp(path) ‖ lp(query)
//	          ‖ lp(content_type) ‖ lp(content_encoding) ‖ lp(node) ‖ u64(epoch) ‖ u64(ts_ms) ‖ nonce[16]
//	res_ctx = "xcvm-res-v1" ‖ SHA-256(req_ctx) ‖ u32(status) ‖ lp(content_type) ‖ u64(ts_ms) ‖ nonce[16]
//	mac     = HMAC-SHA256(K_mac_up | K_mac_down, "xcvm-mac-v1" ‖ lp(ctx) ‖ SHA-256(body))
const (
	PathPrefix = "/cluster/v1/"
	WindowMs   = 90000

	HProto    = "X-XCVM-Proto"
	HAgent    = "X-XCVM-Agent"
	HNode     = "X-XCVM-Node"
	HEpoch    = "X-XCVM-Epoch"
	HTs       = "X-XCVM-Ts"
	HNonce    = "X-XCVM-Nonce"
	HSig      = "X-XCVM-Sig"
	HNodeSig  = "X-XCVM-Node-Sig"
	HPanelSig = "X-XCVM-Panel-Sig"
)

// Request is everything the request context binds.
type Request struct {
	Proto           uint32
	Agent           string
	Method          string
	Path            string
	Query           string
	ContentType     string
	ContentEncoding string
	Node            string
	Epoch           uint64
	TsMs            uint64
	Nonce           []byte
}

var nodeRe = regexp.MustCompile(`^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|sid:[1-9][0-9]{0,9})$`)

// ValidNode reports whether s is a node id as X-XCVM-Node may carry it.
func ValidNode(s string) bool { return nodeRe.MatchString(s) }

// RequestContext serialises a request context.
func RequestContext(r Request) ([]byte, error) {
	if !strings.HasPrefix(r.Path, PathPrefix) || strings.Contains(r.Path, "?") {
		return nil, errors.New("clustercrypto: cluster path")
	}
	if len(r.Nonce) != 16 {
		return nil, errors.New("clustercrypto: nonce must be 16 bytes")
	}
	out := []byte("xcvm-req-v1")
	out = append(out, u32(r.Proto)...)
	out = append(out, lps(r.Agent)...)
	out = append(out, lps(strings.ToUpper(r.Method))...)
	out = append(out, lps(r.Path)...)
	out = append(out, lps(CanonicalQuery(r.Query))...)
	out = append(out, lps(lowerTrim(r.ContentType))...)
	out = append(out, lps(lowerTrim(r.ContentEncoding))...)
	out = append(out, lps(r.Node)...)
	out = append(out, u64(r.Epoch)...)
	out = append(out, u64(r.TsMs)...)
	return append(out, r.Nonce...), nil
}

// ResponseContext serialises a response context; it hashes the request in, so
// a reply cannot be moved onto another request.
func ResponseContext(reqCtx []byte, status uint32, contentType string, tsMs uint64, nonce []byte) ([]byte, error) {
	if len(nonce) != 16 {
		return nil, errors.New("clustercrypto: nonce must be 16 bytes")
	}
	out := append([]byte("xcvm-res-v1"), sum256(reqCtx)...)
	out = append(out, u32(status)...)
	out = append(out, lps(lowerTrim(contentType))...)
	out = append(out, u64(tsMs)...)
	return append(out, nonce...), nil
}

// MAC is the request or response MAC.
func MAC(key, context, body []byte) []byte {
	return hmac256(key, []byte("xcvm-mac-v1"), lp(context), sum256(body))
}

// VerifyMAC compares in constant time.
func VerifyMAC(key, context, body, mac []byte) bool {
	return len(mac) == 32 && hmac.Equal(MAC(key, context, body), mac)
}

// WithinWindow reports whether ts is within ±90 s of now.
func WithinWindow(tsMs, nowMs int64) bool {
	d := tsMs - nowMs
	if d < 0 {
		d = -d
	}
	return d <= WindowMs
}

// CanonicalQuery matches the panel's Canonical::query(): split on '&', skip
// empty parts, '+' is a space, percent-decode (PHP rawurldecode: a malformed
// escape stays literal), re-encode per RFC 3986 (PHP rawurlencode), and sort by
// key then value, bytewise.
func CanonicalQuery(raw string) string {
	if raw == "" {
		return ""
	}
	type pair struct{ k, v string }
	var pairs []pair
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		k, v, _ := strings.Cut(part, "=")
		pairs = append(pairs, pair{rawurlencode(rawurldecode(strings.ReplaceAll(k, "+", " "))), rawurlencode(rawurldecode(strings.ReplaceAll(v, "+", " ")))})
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func rawurldecode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			hi, ok1 := unhex(s[i+1])
			lo, ok2 := unhex(s[i+2])
			if ok1 && ok2 {
				b.WriteByte(hi<<4 | lo)
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func rawurlencode(s string) string {
	const hexUpper = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hexUpper[c>>4])
			b.WriteByte(hexUpper[c&15])
		}
	}
	return b.String()
}

// lowerTrim matches PHP strtolower(trim()): ASCII only, trimming " \t\n\r\0\x0B".
func lowerTrim(s string) string {
	s = strings.Trim(s, " \t\n\r\x00\x0b")
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}
