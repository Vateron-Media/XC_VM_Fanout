package redact

import (
	"strings"
	"testing"
)

func TestURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"userinfo", "http://bob:s3cr3t@host:8080/live.ts", "http://bob:xxxxx@host:8080/live.ts"},
		{"userinfo no password", "http://bob@host/live.ts", "http://xxxxx@host/live.ts"},
		{"xc live path", "http://host:8080/live/bob/s3cr3t/1234.ts", "http://host:8080/live/xxxxx/xxxxx/1234.ts"},
		{"xc hls path", "http://host/hls/bob/s3cr3t/1234/0.ts", "http://host/hls/xxxxx/xxxxx/1234/0.ts"},
		{"xc timeshift", "http://host/timeshift/bob/s3cr3t/60/2026-01-01:00-00/9.ts", "http://host/timeshift/xxxxx/xxxxx/60/2026-01-01:00-00/9.ts"},
		{"legacy bare path", "http://host:8080/bob/s3cr3t/1234.ts", "http://host:8080/xxxxx/xxxxx/1234.ts"},
		{"legacy bare path no ext", "http://host:8080/bob/s3cr3t/1234", "http://host:8080/xxxxx/xxxxx/1234"},
		{"query credentials", "http://host/get.php?username=bob&password=s3cr3t&type=m3u", "http://host/get.php?password=xxxxx&type=m3u&username=xxxxx"},
		{"both", "http://bob:s3cr3t@host/live/bob/s3cr3t/1.ts", "http://bob:xxxxx@host/live/xxxxx/xxxxx/1.ts"},
		{"plain source is untouched", "http://cdn.example.com/stream.ts", "http://cdn.example.com/stream.ts"},
		{"content path is untouched", "http://cdn.example.com/a/b/index.m3u8", "http://cdn.example.com/a/b/index.m3u8"},
		{"udp is untouched", "udp://239.0.0.1:1234", "udp://239.0.0.1:1234"},
		{"unparsable keeps host, loses secret", "http://bob:s3cr3t@host name/live.ts", "http://bob:xxxxx@host name/live.ts"},
		{"not a url at all", "/media/movie.ts", "/media/movie.ts"},
		{"empty", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := URL(c.in); got != c.want {
				t.Errorf("URL(%q)\n got %q\nwant %q", c.in, got, c.want)
			}
		})
	}
}

// The point of the package: no input that carries a secret may come back with
// that secret still in it.
func TestNoSecretSurvives(t *testing.T) {
	for _, raw := range []string{
		"http://bob:s3cr3t@host:8080/live.ts",
		"https://bob:s3cr3t@host/live/bob/s3cr3t/1234.ts",
		"http://host:8080/live/bob/s3cr3t/1234.ts",
		"http://host:8080/bob/s3cr3t/1234.ts",
		"http://host/get.php?username=bob&password=s3cr3t",
		"http://bob:s3cr3t@host name/live.ts",
		"http://bob:s3cr3t@host/live/bob/s3cr3t/1234.ts?token=s3cr3t",
	} {
		if got := URL(raw); strings.Contains(got, "s3cr3t") {
			t.Errorf("secret survived redaction: %q -> %q", raw, got)
		}
	}
}

func TestURLs(t *testing.T) {
	got := URLs([]string{"http://host/live/bob/s3cr3t/1.ts", "udp://239.0.0.1:1234"})
	want := []string{"http://host/live/xxxxx/xxxxx/1.ts", "udp://239.0.0.1:1234"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("URLs[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
