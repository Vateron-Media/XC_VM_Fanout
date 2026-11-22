package clusteragent

import (
	"net/url"
	"strings"
)

// rtmpNotify is what nginx_rtmp names in its auth-notify query: who the client
// is (addr, clientid), what it asks (call), and which stream (name). The port
// of the panel's StreamAuth::notifyArguments.
type rtmpNotify struct {
	Addr, ClientID, Call, Name string
}

// parseRtmpNotify reads exactly addr/clientid/call/name from the raw query
// string, url-decoding each pair, and returns ok=false (→ the caller 404s) only
// when one of those four appears twice with DIFFERING values. Other keys are
// ignored (the creds and tcurl/app live in the play arm, as rtmp.php has them).
func parseRtmpNotify(rawQuery string) (*rtmpNotify, bool) {
	out := &rtmpNotify{}
	dst := map[string]*string{"addr": &out.Addr, "clientid": &out.ClientID, "call": &out.Call, "name": &out.Name}
	seen := map[string]bool{}
	for _, pair := range strings.Split(rawQuery, "&") {
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		key := urlDecode(kv[0])
		p, tracked := dst[key]
		if !tracked {
			continue
		}
		val := ""
		if len(kv) == 2 {
			val = urlDecode(kv[1])
		}
		if seen[key] && *p != val {
			return nil, false
		}
		*p, seen[key] = val, true
	}
	return out, true
}

// urlDecode mirrors PHP urldecode's leniency: an undecodable escape is left as
// the raw text rather than failing the whole parse.
func urlDecode(s string) string {
	if d, err := url.QueryUnescape(s); err == nil {
		return d
	}
	return s
}
