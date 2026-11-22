package gateway

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A request with no policy loaded names the cause once, not a bare 502.
func TestNoPolicyLogsOncePerMinute(t *testing.T) {
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)
	s := NewServer(filepath.Join(t.TempDir(), "missing.json"), http.NotFoundHandler(), nil)
	t0 := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return t0 }
	for i := 0; i < 3; i++ {
		r := httptest.NewRequest("GET", "/x", nil)
		r.Header.Set("X-XC-Original-URI", "/live/u/p/1.m3u8")
		s.serve(httptest.NewRecorder(), r)
	}
	if n := strings.Count(buf.String(), "no policy loaded"); n != 1 {
		t.Fatalf("want 1 warning, got %d: %q", n, buf.String())
	}
}
