// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// withUniqueCookie turns send_unique_header on (cookie name "xcuniq") in a test policy.
func withUniqueCookie(doc map[string]any) {
	doc["headers"].(map[string]any)["unique_header_name"] = "xcuniq"
}

// wantCookies asserts the response carries exactly one Set-Cookie per prefix
// (none for nil), each with the anti-restream attributes.
func wantCookies(t *testing.T, rec *httptest.ResponseRecorder, prefixes []string) {
	t.Helper()
	got := rec.Header().Values("Set-Cookie")
	if len(got) != len(prefixes) {
		t.Fatalf("Set-Cookie: got %q, want %d cookie(s)", got, len(prefixes))
	}
	re := regexp.MustCompile(`^xcuniq=[A-Za-z0-9]{11}; Domain=[^;]*; Expires=[^;]+; Path=/; Secure; HttpOnly; SameSite=none$`)
	for i, c := range got {
		if !strings.HasPrefix(c, prefixes[i]) || !re.MatchString(c) {
			t.Fatalf("Set-Cookie %q: want prefix %q and the PHP attributes", c, prefixes[i])
		}
	}
}

func TestSetUniqueCookieDomain(t *testing.T) {
	for _, c := range []struct{ name, configured, host, want string }{
		{"hostname", "", "tv.example.com", "; Domain=.tv.example.com;"},
		{"hostname with port", "", "tv.example.com:8080", "; Domain=.tv.example.com;"},
		{"bare IPv4", "", "203.0.113.9:8080", "; Domain=;"},
		{"empty host", "", "", "; Domain=;"},
		{"configured domain verbatim", ".cdn.example", "tv.example.com", "; Domain=.cdn.example;"},
	} {
		p := &Policy{}
		p.Headers.UniqueName, p.Headers.UniqueDomain = "xcuniq", c.configured
		h := http.Header{}
		setUniqueCookie(h, p, c.host)
		got := h.Values("Set-Cookie")
		if len(got) != 1 || !strings.Contains(got[0], c.want) {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
	h := http.Header{}
	setUniqueCookie(h, &Policy{}, "tv.example.com")
	if len(h) != 0 {
		t.Errorf("name empty: wrote %v", h)
	}
}

// uniqueOn rewrites a written policy file with the cookie turned on.
func uniqueOn(t *testing.T, path string) {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(mustRead(t, path), &doc); err != nil {
		t.Fatal(err)
	}
	withUniqueCookie(doc)
	b, _ := json.Marshal(doc)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// Live: the cookie rides on a served warmup (200) and on the 503 a never-ready
// one answers, as it does on every live.php response; content is unchanged.
func TestUniqueCookieOnLive(t *testing.T) {
	withFastWarmupPlaylist(t)
	for _, ready := range []bool{true, false} {
		dir, path, _ := warmupPolicy(t, "segments+playlist", true)
		uniqueOn(t, path)
		startProducer(t, filepath.Join(dir, "streams"))
		calls := 0
		node := &fakeFiles{
			serveable:     map[string]bool{"12": true},
			ensureRunning: func(string) bool { return true },
			fedPlaylist: func(string) string {
				calls++
				if ready && calls > 1 {
					return "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:5\n"
				}
				return ""
			},
		}
		seg := &recordingHandler{}
		rec := warmupAsk(NewServer(path, seg, node))
		wantCode := http.StatusServiceUnavailable
		if ready {
			wantCode = 200
			// The ready warmup now tokenizes the playlist in-process (XC_VM_Fanout#128),
			// so it no longer re-dispatches to the daemon's raw HLS handler.
			if seg.hits != 0 {
				t.Fatalf("ready: handler hits %d; want 0 (tokenized in-process)", seg.hits)
			}
		}
		if rec.Code != wantCode {
			t.Fatalf("ready=%v: code %d; want %d", ready, rec.Code, wantCode)
		}
		wantCookies(t, rec, []string{"xcuniq="})
	}
}
