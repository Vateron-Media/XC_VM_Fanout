package front

import (
	"bufio"
	"net"
	"net/http"
	"os"
	"strings"
)

// TrustedProxies holds the CIDRs nginx trusts via set_real_ip_from (the
// realip_cdn / realip_cloudflare / realip_xc_vm.conf includes). The front reads
// the same files, so there is one source of truth kept fresh by the existing
// cron.
type TrustedProxies struct {
	nets []*net.IPNet
}

// LoadTrustedProxies reads set_real_ip_from CIDRs from the given nginx conf
// files. A missing file is skipped, exactly as an absent include is.
func LoadTrustedProxies(paths ...string) *TrustedProxies {
	t := &TrustedProxies{}
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			fields := strings.Fields(strings.TrimRight(line, ";"))
			if len(fields) == 2 && fields[0] == "set_real_ip_from" {
				t.add(fields[1])
			}
		}
		f.Close()
	}
	return t
}

func (t *TrustedProxies) add(cidr string) {
	if !strings.Contains(cidr, "/") { // a bare address is a single host
		if strings.Contains(cidr, ":") {
			cidr += "/128"
		} else {
			cidr += "/32"
		}
	}
	if _, n, err := net.ParseCIDR(cidr); err == nil {
		t.nets = append(t.nets, n)
	}
}

func (t *TrustedProxies) trusted(ip net.IP) bool {
	for _, n := range t.nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// RealIP returns the viewer's address as the LB's nginx resolves it
// (real_ip_header X-Forwarded-For, real_ip_recursive off): the TCP peer,
// unless it is a trusted proxy, in which case the last X-Forwarded-For entry.
// A trusted proxy appends the true peer rightmost, so a client that spoofs
// X-Forwarded-For only prepends to it and is overridden; an untrusted peer's
// header is ignored entirely.
func (t *TrustedProxies) RealIP(r *http.Request) string {
	peer := clientIP(r)
	ip := net.ParseIP(peer)
	if ip == nil || !t.trusted(ip) {
		return peer
	}
	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return peer
	}
	parts := strings.Split(xff, ",")
	if last := strings.TrimSpace(parts[len(parts)-1]); net.ParseIP(last) != nil {
		return last
	}
	return peer
}
