package gateway

import (
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func decs(n int64) string { return strconv.FormatInt(n, 10) }

func thumbEnv(mod int64, exists bool) Env {
	return Env{Mtime: func(string) (int64, bool) { return mod, exists }}
}

func TestJudgeThumb(t *testing.T) {
	p := testPolicy()
	p.Paths.Streams = "/streams/"
	fresh := thumbEnv(testNow, true)

	// Happy path: an unexpired token and a thumbnail under a minute old.
	v := JudgeThumb(p, tok(`{"expires":`+decs(testNow+3600)+`,"stream":12}`), testNow, fresh)
	if v.Action != Serve || v.Stream != 12 || v.Path != "/streams/12_.jpg" {
		t.Fatalf("fresh valid: got %+v, want Serve stream=12 path=/streams/12_.jpg", v)
	}

	// Everything else is PHP's: it emits the exact TOKEN_EXPIRED /
	// THUMBNAIL_DOESNT_EXIST the gateway does not reproduce.
	cases := []struct {
		name  string
		token string
		env   Env
	}{
		{"expired", tok(`{"expires":` + decs(testNow-100) + `,"stream":12}`), fresh},
		{"no-expiry", tok(`{"stream":12}`), fresh},
		{"no-stream", tok(`{"expires":` + decs(testNow+3600) + `}`), fresh},
		{"zero-stream", tok(`{"expires":` + decs(testNow+3600) + `,"stream":0}`), fresh},
		{"stale", tok(`{"expires":` + decs(testNow+3600) + `,"stream":12}`), thumbEnv(testNow-120, true)},
		{"missing", tok(`{"expires":` + decs(testNow+3600) + `,"stream":12}`), thumbEnv(0, false)},
		{"slash-token", liveTok("3", "12_d5.ts", "1.2.3.4"), fresh},
		{"refused", Seal([]byte(`{"expires":`+decs(testNow+3600)+`,"stream":12}`), []byte("wrong-secret"), testContext, testNonce), fresh},
	}
	for _, c := range cases {
		if v := JudgeThumb(p, c.token, testNow, c.env); v.Action != PHP {
			t.Errorf("%s: got %+v, want PHP", c.name, v)
		}
	}

	// time_offset extends validity: a token just past the raw clock but within
	// the node's offset is still served.
	p.TimeOffset = 200
	if v := JudgeThumb(p, tok(`{"expires":`+decs(testNow-100)+`,"stream":12}`), testNow, fresh); v.Action != Serve {
		t.Errorf("within offset: got %+v, want Serve", v)
	}

	// The lease fence hands everything to PHP.
	p.TimeOffset = 0
	p.ServeUntil = testNow - 1
	if v := JudgeThumb(p, tok(`{"expires":`+decs(testNow+3600)+`,"stream":12}`), testNow, fresh); v.Action != PHP {
		t.Errorf("past lease: got %+v, want PHP", v)
	}

	// send_unique_header no longer defers: the gateway writes the cookie itself.
	p.ServeUntil = 0
	p.Headers.UniqueName = "xcuniq"
	if v := JudgeThumb(p, tok(`{"expires":`+decs(testNow+3600)+`,"stream":12}`), testNow, fresh); v.Action != Serve {
		t.Errorf("unique-header: got %+v, want Serve", v)
	}
}

// TestServeThumb: the gateway serves the thumbnail as thumb.php does under a
// mode that owns it, and hands it to PHP's @gw_thumb_php under one that does not.
func TestServeThumb(t *testing.T) {
	dir := t.TempDir()
	streams := filepath.Join(dir, "streams")
	if err := os.MkdirAll(streams, 0o755); err != nil {
		t.Fatal(err)
	}
	jpg := []byte("\xff\xd8\xff\xe0thumbnail-bytes")
	if err := os.WriteFile(filepath.Join(streams, "12_.jpg"), jpg, 0o644); err != nil {
		t.Fatal(err)
	}
	// The serve path judges on the wall clock (NewServer's now), so the token
	// expires against it, not testNow.
	token := tok(`{"expires":` + decs(time.Now().Unix()+3600) + `,"stream":12}`)
	withStreams := func(doc map[string]any) { doc["paths"].(map[string]any)["streams"] = streams + "/" }

	s := NewServer(writeServePolicy(t, dir, "segments+playlist+thumb", withStreams), http.NotFoundHandler(), nil)
	rec := serveReq(s, "/thauth/"+token, "198.51.100.7")
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "image/jpg" || rec.Body.String() != string(jpg) {
		t.Fatalf("served thumbnail: %d %v body=%q", rec.Code, rec.Header(), rec.Body.String())
	}
	wantCookies(t, rec, nil)
	un := NewServer(writeServePolicy(t, dir, "segments+playlist+thumb", func(d map[string]any) { withStreams(d); withUniqueCookie(d) }), http.NotFoundHandler(), nil)
	if r2 := serveReq(un, "/thauth/"+token, "198.51.100.7"); r2.Code != 200 || r2.Body.String() != string(jpg) {
		t.Fatalf("thumb with cookie: %d %q", r2.Code, r2.Body.String())
	} else {
		wantCookies(t, r2, []string{"xcuniq="})
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" || rec.Header().Get("Age") == "" {
		t.Fatalf("thumb headers: %v", rec.Header())
	}
	// thumb.php sets X-Content-Type-Options only with the protection headers.
	if rec.Header().Get("X-Content-Type-Options") != "" {
		t.Fatalf("nosniff must track send_protection_headers: %v", rec.Header())
	}

	// A mode that does not serve thumb hands it to PHP's @gw_thumb_php.
	off := NewServer(writeServePolicy(t, dir, "segments+playlist", withStreams), http.NotFoundHandler(), nil)
	rec = serveReq(off, "/thauth/"+token, "198.51.100.7")
	if rec.Header().Get("X-Accel-Redirect") != "@gw_thumb_php" {
		t.Fatalf("mode off thumb: want @gw_thumb_php, got %v", rec.Header())
	}
}
