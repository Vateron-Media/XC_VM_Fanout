// Package redact keeps source credentials out of the daemon's logs.
//
// A source URL is the one string the daemon logs on every reconnect, and in an
// XC deployment it carries the provider account: either as userinfo
// (http://user:pass@host/live.ts) or, far more often, as path segments
// (http://host:8080/live/<username>/<password>/1234.ts — the shape XC panels
// hand out). A provider that answers 403 on every retry therefore wrote the
// account password into the operator log once per backoff cycle, forever, and
// those logs are read by support and shipped off the box.
//
// The rule here is deliberately narrow: mask what is known to be a secret and
// leave everything else legible, because a redacted log that no longer names
// the host or the stream is useless for the failure it was written for.
package redact

import (
	"net/url"
	"strings"
)

// mask replaces a secret. It matches net/url's own placeholder so a redacted
// userinfo reads the same whether it came from URL below or from u.Redacted().
const mask = "xxxxx"

// credPrefixes are the XC path shapes whose next two segments are the account:
// /live/<user>/<pass>/<id>.ts and its siblings for HLS, VOD and timeshift.
var credPrefixes = map[string]bool{
	"live":      true,
	"hls":       true,
	"movie":     true,
	"series":    true,
	"timeshift": true,
}

// secretParams are query keys whose value is a credential in the XC API.
var secretParams = map[string]bool{
	"username": true,
	"password": true,
	"token":    true,
	"auth":     true,
	"key":      true,
}

// URL returns raw with every credential it can recognise replaced, keeping the
// scheme, host, stream id and query structure intact so the line still says
// which source failed and why.
//
// A string that does not parse as a URL is returned with only its userinfo
// masked: it is still worth logging (it is usually a typo the operator has to
// see), and the one thing that must not survive is the password.
func URL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return maskUserinfo(raw)
	}
	if u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			u.User = url.UserPassword(u.User.Username(), mask)
		} else {
			u.User = url.User(mask)
		}
	}
	u.Path = maskPath(u.Path)
	if q := u.Query(); len(q) > 0 {
		changed := false
		for k, vs := range q {
			if !secretParams[strings.ToLower(k)] {
				continue
			}
			for i := range vs {
				vs[i] = mask
			}
			q[k] = vs
			changed = true
		}
		if changed {
			u.RawQuery = q.Encode()
		}
	}
	return u.String()
}

// URLs redacts a list of candidate sources for a single log line.
func URLs(raws []string) []string {
	out := make([]string, len(raws))
	for i, raw := range raws {
		out[i] = URL(raw)
	}
	return out
}

// maskPath blanks the two account segments of an XC-style path. Both the
// prefixed form (/live/<user>/<pass>/<id>.ts) and the bare one that older
// panels emit (/<user>/<pass>/<id>.ts) are covered; anything else — a plain
// /stream.ts, an HLS variant path — is left alone.
func maskPath(p string) string {
	if p == "" {
		return p
	}
	segs := strings.Split(strings.TrimPrefix(p, "/"), "/")
	i := -1
	switch {
	case len(segs) >= 4 && credPrefixes[strings.ToLower(segs[0])]:
		i = 1
	case len(segs) == 3 && segs[0] != "" && segs[1] != "" && segs[2] != "":
		// /<user>/<pass>/<id>[.ext]: only when the last segment is an id, so a
		// genuine three-deep content path is not mangled.
		if isStreamID(segs[2]) {
			i = 0
		}
	}
	if i < 0 {
		return p
	}
	segs[i], segs[i+1] = mask, mask
	return "/" + strings.Join(segs, "/")
}

// isStreamID reports whether seg is an XC stream id: digits, optionally with a
// container extension.
func isStreamID(seg string) bool {
	if dot := strings.IndexByte(seg, '.'); dot >= 0 {
		seg = seg[:dot]
	}
	if seg == "" {
		return false
	}
	for i := 0; i < len(seg); i++ {
		if seg[i] < '0' || seg[i] > '9' {
			return false
		}
	}
	return true
}

// maskUserinfo is the fallback for a string url.Parse would not take: blank
// whatever sits between "://" and the first "@" of the authority.
func maskUserinfo(raw string) string {
	i := strings.Index(raw, "://")
	if i < 0 {
		return raw
	}
	rest := raw[i+3:]
	end := strings.IndexAny(rest, "/?#")
	if end < 0 {
		end = len(rest)
	}
	at := strings.LastIndex(rest[:end], "@")
	if at < 0 {
		return raw
	}
	user := rest[:at]
	if c := strings.IndexByte(user, ':'); c >= 0 {
		user = user[:c]
	}
	return raw[:i+3] + user + ":" + mask + "@" + rest[at+1:]
}
