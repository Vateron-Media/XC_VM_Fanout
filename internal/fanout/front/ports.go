package front

import (
	"errors"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// listenRe captures the argument list of an nginx `listen …;` directive (one
// per line), ignoring comments and other directives. It reads the same
// ports/http.conf and ports/https.conf the node maintains for nginx (what
// ClusterNginxConfig::servedPorts parses on the PHP side).
var listenRe = regexp.MustCompile(`(?m)^\s*listen\s+([^;#]+);`)

// ParseListenSet turns the two nginx listen includes into the desired listen
// set. A directive is TLS iff it carries the `ssl` parameter (nginx's own
// marker); the port is the numeric tail of the first field (`80`, `[::]:443`,
// `1.2.3.4:443`). Ports keep http-then-https file order, deduped. No listen
// directive at all is an error, so a watcher keeps its last-good set.
func ParseListenSet(httpConf, httpsConf string) ([]ListenSpec, error) {
	var out []ListenSpec
	seen := map[int]bool{}
	for _, conf := range []string{httpConf, httpsConf} {
		for _, m := range listenRe.FindAllStringSubmatch(conf, -1) {
			fields := strings.Fields(m[1])
			if len(fields) == 0 {
				continue
			}
			port, err := parseListenPort(fields[0])
			if err != nil || seen[port] {
				continue
			}
			tls := false
			for _, f := range fields[1:] {
				if f == "ssl" {
					tls = true
				}
			}
			seen[port] = true
			out = append(out, ListenSpec{Port: port, TLS: tls})
		}
	}
	if len(out) == 0 {
		return nil, errors.New("front: no listen directives found")
	}
	return out, nil
}

// parseListenPort extracts the port from a listen first field, which may be a
// bare port or an address:port (IPv4 or bracketed IPv6).
func parseListenPort(field string) (int, error) {
	if i := strings.LastIndex(field, ":"); i >= 0 {
		field = field[i+1:]
	}
	return strconv.Atoi(field)
}

// WatchPorts applies the listen set parsed from httpPath+httpsPath immediately,
// then re-reads them every poll interval and calls apply again whenever the
// parsed set changes. A read or parse that yields no ports is dropped (the last
// good set stands). The returned stop cancels the watcher.
func WatchPorts(httpPath, httpsPath string, poll time.Duration, apply func([]ListenSpec)) (stop func()) {
	read := func() ([]ListenSpec, bool) {
		h, _ := os.ReadFile(httpPath)
		s, _ := os.ReadFile(httpsPath)
		specs, err := ParseListenSet(string(h), string(s))
		if err != nil {
			return nil, false
		}
		return specs, true
	}

	var last []ListenSpec
	if specs, ok := read(); ok {
		last = specs
		apply(specs)
	}

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(poll)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if specs, ok := read(); ok && !sameSet(specs, last) {
					last = specs
					apply(specs)
				}
			}
		}
	}()
	return func() { close(done) }
}

// sameSet reports whether two listen sets are equal regardless of order.
func sameSet(a, b []ListenSpec) bool {
	if len(a) != len(b) {
		return false
	}
	ca, cb := append([]ListenSpec(nil), a...), append([]ListenSpec(nil), b...)
	less := func(s []ListenSpec) func(i, j int) bool {
		return func(i, j int) bool { return s[i].Port < s[j].Port }
	}
	sort.Slice(ca, less(ca))
	sort.Slice(cb, less(cb))
	for i := range ca {
		if ca[i] != cb[i] {
			return false
		}
	}
	return true
}
