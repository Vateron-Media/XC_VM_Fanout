package clusteragent

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"strings"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/viewertoken"
)

// adminTok is AdminStreamToken's payload — the fields Public/admin/*.php reads
// from a decoded admin uitoken. stream_id, ip and expires are required
// (AdminStreamToken::decode returns null without them); container (vod), start
// and duration (timeshift) are optional.
type adminTok struct {
	StreamID  int
	IP        string
	Expires   int64
	Container string
	Start     string
	Duration  string
}

// decodeAdminToken is AdminStreamToken::decode: open the uitoken with the node's
// token keys (Encryption::readToken), then require the JSON to carry stream_id,
// ip and expires. acceptLegacy is !secure_stream_tokens.
func decodeAdminToken(token string, keys viewertoken.Keys, acceptLegacy bool, now int64) (*adminTok, bool) {
	plain, state := keys.Read(token, acceptLegacy, now)
	if state != viewertoken.Opened {
		return nil, false
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(plain, &raw); err != nil {
		return nil, false
	}
	for _, req := range []string{"stream_id", "ip", "expires"} {
		if _, ok := raw[req]; !ok {
			return nil, false
		}
	}
	return &adminTok{
		StreamID:  int(jsonInt64(raw["stream_id"])),
		IP:        jsonString(raw["ip"]),
		Expires:   jsonInt64(raw["expires"]),
		Container: jsonString(raw["container"]),
		Start:     jsonString(raw["start"]),
		Duration:  jsonString(raw["duration"]),
	}, true
}

// valid is AdminStreamToken::isValid: unexpired and the client's address matches
// the one the token is bound to (exact, or same subnet when ip_subnet_match is on).
func (t *adminTok) valid(subnetMatch bool, clientIP string, now int64) bool {
	return t.Expires >= now && ipMatches(subnetMatch, t.IP, clientIP)
}

// ipMatches is NetworkUtils::ipMatches: exact string match, or — when subnetMatch
// is on — the same IPv4 /24 (first three octets) or IPv6 /64 (first eight bytes),
// IPv4-mapped addresses compared as IPv4.
func ipMatches(subnetMatch bool, tokenIP, clientIP string) bool {
	if tokenIP == clientIP {
		return true
	}
	if !subnetMatch {
		return false
	}
	a, b := net.ParseIP(tokenIP), net.ParseIP(clientIP)
	if a == nil || b == nil {
		return false
	}
	a4, b4 := a.To4(), b.To4()
	if a4 != nil && b4 != nil {
		return a4[0] == b4[0] && a4[1] == b4[1] && a4[2] == b4[2]
	}
	if a4 != nil || b4 != nil {
		return false // one IPv4, one IPv6
	}
	return bytes.Equal(a.To16()[:8], b.To16()[:8])
}

// policyHexKey is one {hex, until} entry in the gateway policy file's keys.
type policyHexKey struct {
	Hex   string `json:"hex"`
	Until *int64 `json:"until"`
}

// parseViewerKeys reads the token-key subset the admin handlers need from the
// gateway policy file (the same file the gateway loads, written by cron:cache):
// the viewer/shared/context key lists (hex → bytes, with their windows), whether
// the legacy CBC format is accepted, and ip_subnet_match. ok is false when the
// JSON cannot be parsed.
func parseViewerKeys(policyJSON []byte) (keys viewertoken.Keys, acceptLegacy, subnetMatch, ok bool) {
	var p struct {
		IPSubnetMatch bool `json:"ip_subnet_match"`
		Keys          struct {
			Viewer          []policyHexKey `json:"viewer"`
			Shared          []policyHexKey `json:"shared"`
			Context         []policyHexKey `json:"context"`
			AcceptLegacyCBC bool           `json:"accept_legacy_cbc"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(policyJSON, &p); err != nil {
		return viewertoken.Keys{}, false, false, false
	}
	conv := func(list []policyHexKey) []viewertoken.Key {
		out := make([]viewertoken.Key, 0, len(list))
		for _, k := range list {
			v, err := hex.DecodeString(k.Hex)
			if err != nil {
				continue
			}
			var until int64
			if k.Until != nil {
				until = *k.Until
			}
			out = append(out, viewertoken.Key{Value: v, Until: until})
		}
		return out
	}
	keys = viewertoken.Keys{Viewer: conv(p.Keys.Viewer), Shared: conv(p.Keys.Shared), Context: conv(p.Keys.Context)}
	return keys, p.Keys.AcceptLegacyCBC, p.IPSubnetMatch, true
}

// jsonInt64 reads a JSON number or numeric string as PHP's intval would.
func jsonInt64(m json.RawMessage) int64 {
	if len(m) == 0 {
		return 0
	}
	var n json.Number
	if json.Unmarshal(m, &n) == nil {
		if i, err := n.Int64(); err == nil {
			return i
		}
	}
	var s string
	if json.Unmarshal(m, &s) == nil {
		if i, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return i
		}
	}
	return 0
}

// jsonString reads a JSON string, or the literal of a non-string scalar.
func jsonString(m json.RawMessage) string {
	if len(m) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(m, &s) == nil {
		return s
	}
	return string(bytes.Trim(m, `"`))
}
