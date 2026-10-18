package front

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func streamsServer(t *testing.T, dir string) *Server {
	t.Helper()
	return NewServer(okHandler(), mustURL(t, "http://127.0.0.1:1"), "", LoadTrustedProxies()).WithStreams(dir)
}

func TestXcHlsServesFileFromStreams(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "7_.ts"), []byte("SEGMENTDATA"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := streamsServer(t, dir)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/xc_hls/7_.ts", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d, want 200", rec.Code)
	}
	if body := rec.Body.String(); body != "SEGMENTDATA" {
		t.Fatalf("body=%q", body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "video/mp2t" {
		t.Fatalf("content-type=%q, want video/mp2t", ct)
	}
}

func TestXcHlsRange(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "seg.ts"), []byte("0123456789"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := streamsServer(t, dir)
	req := httptest.NewRequest(http.MethodGet, "/xc_hls/seg.ts", nil)
	req.Header.Set("Range", "bytes=2-5")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("code=%d, want 206", rec.Code)
	}
	if body := rec.Body.String(); body != "2345" {
		t.Fatalf("range body=%q, want 2345", body)
	}
}

func TestXcHlsMissing404(t *testing.T) {
	s := streamsServer(t, t.TempDir())
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/xc_hls/nope.ts", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code=%d, want 404", rec.Code)
	}
}

func TestXcHlsNoTraversal(t *testing.T) {
	root := t.TempDir()
	secret := filepath.Join(filepath.Dir(root), "secret.txt")
	if err := os.WriteFile(secret, []byte("TOPSECRET"), 0o644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(secret)
	s := streamsServer(t, root)
	rec := httptest.NewRecorder()
	// A raw escaping path; the request-target is normalised, but assert the secret never leaks regardless.
	req := httptest.NewRequest(http.MethodGet, "/xc_hls/..%2f..%2fsecret.txt", nil)
	req.URL.Path = "/xc_hls/../../secret.txt"
	s.ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), "TOPSECRET") {
		t.Fatalf("traversal leaked the file above the streams root: %q", rec.Body.String())
	}
	b, _ := io.ReadAll(rec.Body)
	_ = b
}

func portalServer(t *testing.T, dir string) *Server {
	t.Helper()
	return NewServer(okHandler(), mustURL(t, "http://127.0.0.1:1"), "", LoadTrustedProxies()).WithPortalAssets(dir)
}

func TestPortalAssetsServed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "app.css"), []byte("body{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	portalServer(t, dir).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/portal/assets/app.css", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d, want 200", rec.Code)
	}
	if rec.Body.String() != "body{}" {
		t.Fatalf("body=%q", rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/css") {
		t.Fatalf("content-type=%q, want text/css", ct)
	}
}

func TestPortalAssetsNoTraversal(t *testing.T) {
	root := t.TempDir()
	secret := filepath.Join(filepath.Dir(root), "psecret.txt")
	if err := os.WriteFile(secret, []byte("PTOP"), 0o644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(secret)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/portal/assets/x", nil)
	req.URL.Path = "/portal/assets/../../psecret.txt"
	portalServer(t, root).ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), "PTOP") {
		t.Fatalf("traversal leaked a file above the portal root: %q", rec.Body.String())
	}
}
