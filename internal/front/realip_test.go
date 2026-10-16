package front

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func reqFrom(peer, xff string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = peer
	if xff != "" {
		r.Header.Set("X-Forwarded-For", xff)
	}
	return r
}

func TestTrustedProxiesRealIP(t *testing.T) {
	tp := &TrustedProxies{}
	tp.add("10.0.0.0/8") // the trusted CDN range

	cases := []struct {
		name, peer, xff, want string
	}{
		{"untrusted peer ignores XFF", "203.0.113.7:9000", "1.1.1.1", "203.0.113.7"},
		{"trusted peer takes the single XFF", "10.0.0.1:9000", "1.1.1.1", "1.1.1.1"},
		{"trusted peer takes the last XFF (CDN appended)", "10.0.0.1:9000", "9.9.9.9, 2.2.2.2", "2.2.2.2"},
		{"trusted peer without XFF falls back to peer", "10.0.0.1:9000", "", "10.0.0.1"},
		{"trusted peer with garbage XFF falls back to peer", "10.0.0.1:9000", "notanip", "10.0.0.1"},
	}
	for _, c := range cases {
		if got := tp.RealIP(reqFrom(c.peer, c.xff)); got != c.want {
			t.Errorf("%s: RealIP = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestLoadTrustedProxies(t *testing.T) {
	p := filepath.Join(t.TempDir(), "realip.conf")
	conf := "# cloudflare\nset_real_ip_from 10.0.0.0/8;\nset_real_ip_from 192.168.1.5;\nset_real_ip_from 2606:4700::/32;\n\n#set_real_ip_from 8.8.8.8;\n"
	if err := os.WriteFile(p, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	tp := LoadTrustedProxies(p, filepath.Join(t.TempDir(), "absent.conf"))

	// A trusted peer yields the (distinct) XFF; an untrusted peer yields itself.
	const viewer = "7.7.7.7"
	for _, tc := range []struct {
		name, peer string
		trusted    bool
	}{
		{"inside the /8", "10.1.2.3", true},
		{"bare host becomes /32", "192.168.1.5", true},
		{"inside the v6 /32", "2606:4700::1", true},
		{"commented directive not trusted", "8.8.8.8", false},
		{"unlisted not trusted", "203.0.113.1", false},
	} {
		got := tp.RealIP(reqFrom("["+tc.peer+"]:1", viewer)) // brackets are harmless for v4
		want := tc.peer
		if tc.trusted {
			want = viewer
		}
		if got != want {
			t.Errorf("%s: RealIP(peer=%s) = %q, want %q", tc.name, tc.peer, got, want)
		}
	}
}

// Behind a trusted CDN, the gateway receives the viewer's real address, not the
// CDN's, as X-XC-Client-IP.
func TestFrontResolvesRealIPBehindCDN(t *testing.T) {
	var gotIP string
	gw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIP = r.Header.Get("X-XC-Client-IP")
		_, _ = io.WriteString(w, "ok")
	})
	tp := &TrustedProxies{}
	tp.add("10.0.0.0/8")
	front := NewServer(gw, newNginxStub(t).url, t.TempDir(), tp)

	req := httptest.NewRequest(http.MethodGet, "/auth/token", nil)
	req.RemoteAddr = "10.0.0.9:7000"                 // the CDN
	req.Header.Set("X-Forwarded-For", "88.88.88.88") // the viewer
	front.ServeHTTP(httptest.NewRecorder(), req)

	if gotIP != "88.88.88.88" {
		t.Fatalf("gateway got client IP %q, want the viewer 88.88.88.88", gotIP)
	}
}
