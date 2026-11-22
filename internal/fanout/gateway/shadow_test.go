package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestShadowBookPairsEitherWay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shadow.json")
	b := NewShadowBook(path)
	clock := time.Unix(testNow, 0)
	b.now = func() time.Time { return clock }

	b.Gateway("r1", "segment", Verdict{Action: Serve, Reason: "daemon", Stream: 12})
	b.PHP("r1", "segment", "serve")
	b.PHP("r2", "key", "deny")
	b.Gateway("r2", "key", Verdict{Action: Deny, Reason: "ip"})
	b.Gateway("r3", "segment", Verdict{Action: PHP, Reason: "archive"})
	b.PHP("r3", "segment", "serve")
	b.Gateway("r4", "segment", Verdict{Action: Deny, Reason: "connection", Stream: 9})
	b.PHP("r4", "segment", "serve")
	st := b.State()
	if st.Agree != 2 || st.Deferred != 1 || st.Disagree != 1 || st.Since != testNow || st.LastDisagree != testNow {
		t.Fatalf("state %+v", st)
	}
	if len(st.Samples) != 1 || st.Samples[0] != (Disagreement{At: testNow, Kind: "segment", Gateway: "deny connection", PHP: "serve", Stream: 9}) {
		t.Fatalf("sample %+v", st.Samples)
	}

	// A side whose other never comes is dropped after the window.
	b.Gateway("lonely", "segment", Verdict{Action: Serve})
	clock = clock.Add(shadowPairWindow + 2*time.Second)
	b.PHP("r5", "key", "serve")
	if st := b.State(); st.Unmatched != 1 {
		t.Fatalf("unmatched: %+v", st)
	}

	// The state survives a restart (it was saved at the disagreement).
	if again := NewShadowBook(path).State(); again.Disagree != 1 || again.Since != testNow || len(again.Samples) != 1 {
		t.Fatalf("reloaded: %+v", again)
	}
}

func TestShadowSamplesAreTheNewest(t *testing.T) {
	b := NewShadowBook("")
	for i := 0; i < shadowSamples+5; i++ {
		id := itoa(int64(i))
		b.Gateway(id, "key", Verdict{Action: Serve, Stream: i})
		b.PHP(id, "key", "deny")
	}
	st := b.State()
	if len(st.Samples) != shadowSamples || st.Samples[0].Stream != 5 || st.Disagree != shadowSamples+5 {
		t.Fatalf("samples %+v", st)
	}
}

func TestShadowEndpointsCompare(t *testing.T) {
	dir := t.TempDir()
	s := NewServer(writeServePolicy(t, dir, "shadow", nil), http.NotFoundHandler(), nil)
	ip := "198.51.100.7"
	mirror := httptest.NewRequest(http.MethodPost, "/shadow", nil)
	mirror.Header.Set("X-XC-Original-URI", "/key/"+tok(ip+"/12"))
	mirror.Header.Set("X-XC-Client-IP", ip)
	mirror.Header.Set("X-XC-Request-ID", "abc123")
	s.ServeHTTP(httptest.NewRecorder(), mirror)
	report := httptest.NewRequest(http.MethodPost, "/shadow/php", nil)
	report.Header.Set("X-XC-Request-ID", "abc123")
	report.Header.Set("X-XC-Kind", "key")
	report.Header.Set("X-XC-Outcome", "serve")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, report)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("report: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var stats struct {
		Shadow ShadowState `json:"shadow"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil || stats.Shadow.Agree != 1 {
		t.Fatalf("stats: %s", rec.Body.String())
	}
}
