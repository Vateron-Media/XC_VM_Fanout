package gateway

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func subEnv(exists bool) Env { return Env{File: func(string) bool { return exists }} }

func TestJudgeSubtitle(t *testing.T) {
	p := testPolicy()
	p.Paths.Vod = "/vod/"
	have := subEnv(true)

	// Happy path: an unexpired token, a file on disk, asked for as WebVTT.
	v := JudgeSubtitle(p, tok(`{"expires":`+decs(testNow+3600)+`,"stream_id":12,"sub_id":3,"webvtt":1}`), testNow, have)
	if v.Action != Serve || v.Stream != 12 || v.Seq != 3 || !v.WebVTT || v.Path != "/vod/12_3.srt" {
		t.Fatalf("valid webvtt: got %+v, want Serve stream=12 sub=3 webvtt path=/vod/12_3.srt", v)
	}
	// sub_id defaults to 0 and the file is served as .srt.
	v = JudgeSubtitle(p, tok(`{"expires":`+decs(testNow+3600)+`,"stream_id":7}`), testNow, have)
	if v.Action != Serve || v.Stream != 7 || v.Seq != 0 || v.WebVTT || v.Path != "/vod/7_0.srt" {
		t.Fatalf("valid srt: got %+v", v)
	}

	// Everything else is PHP's (TOKEN_EXPIRED / THUMBNAIL_DOESNT_EXIST).
	cases := []struct {
		name  string
		token string
		env   Env
	}{
		{"expired", tok(`{"expires":` + decs(testNow-100) + `,"stream_id":12}`), have},
		{"no-expiry", tok(`{"stream_id":12}`), have},
		{"no-stream", tok(`{"expires":` + decs(testNow+3600) + `}`), have},
		{"zero-stream", tok(`{"expires":` + decs(testNow+3600) + `,"stream_id":0}`), have},
		{"missing", tok(`{"expires":` + decs(testNow+3600) + `,"stream_id":12}`), subEnv(false)},
		{"slash-token", liveTok("3", "12_d5.ts", "1.2.3.4"), have},
		{"refused", Seal([]byte(`{"expires":`+decs(testNow+3600)+`,"stream_id":12}`), []byte("wrong-secret"), testContext, testNonce), have},
	}
	for _, c := range cases {
		if v := JudgeSubtitle(p, c.token, testNow, c.env); v.Action != PHP {
			t.Errorf("%s: got %+v, want PHP", c.name, v)
		}
	}

	// time_offset extends validity, as the endpoint reads it.
	p.TimeOffset = 200
	if v := JudgeSubtitle(p, tok(`{"expires":`+decs(testNow-100)+`,"stream_id":12}`), testNow, have); v.Action != Serve {
		t.Errorf("within offset: got %+v, want Serve", v)
	}
	// The lease fence and send_unique_header hand everything to PHP.
	p.TimeOffset = 0
	p.ServeUntil = testNow - 1
	if v := JudgeSubtitle(p, tok(`{"expires":`+decs(testNow+3600)+`,"stream_id":12}`), testNow, have); v.Action != PHP {
		t.Errorf("past lease: got %+v, want PHP", v)
	}
	p.ServeUntil = 0
	p.Headers.UniqueHeader = true
	if v := JudgeSubtitle(p, tok(`{"expires":`+decs(testNow+3600)+`,"stream_id":12}`), testNow, have); v.Action != PHP {
		t.Errorf("unique-header: got %+v, want PHP", v)
	}
}

// TestServeSubtitle: the gateway serves the VOD subtitle as subtitle.php does
// (plain .srt, or converted to WebVTT), and hands it to @gw_subtitle_php under a
// mode that does not own it.
func TestServeSubtitle(t *testing.T) {
	dir := t.TempDir()
	vod := filepath.Join(dir, "vod")
	if err := os.MkdirAll(vod, 0o755); err != nil {
		t.Fatal(err)
	}
	// A comma in the text line must survive; only the timestamp lines convert.
	srt := "1\n00:00:01,000 --> 00:00:04,000\nHello, world\n\n2\n00:00:05,500 --> 00:00:09,000\nBye\n"
	if err := os.WriteFile(filepath.Join(vod, "12_0.srt"), []byte(srt), 0o644); err != nil {
		t.Fatal(err)
	}
	withVod := func(doc map[string]any) { doc["paths"].(map[string]any)["vod"] = vod + "/" }
	s := NewServer(writeServePolicy(t, dir, "segments+playlist+thumb+subtitle", withVod), http.NotFoundHandler(), nil)

	// Plain SRT download.
	token := tok(`{"expires":` + decs(time.Now().Unix()+3600) + `,"stream_id":12,"sub_id":0}`)
	rec := serveReq(s, "/subauth/"+token, "198.51.100.7")
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/octet-stream" || rec.Body.String() != srt {
		t.Fatalf("served srt: %d %v body=%q", rec.Code, rec.Header(), rec.Body.String())
	}
	if rec.Header().Get("Content-Disposition") != `attachment; filename="12_0.srt"` {
		t.Fatalf("srt disposition: %v", rec.Header())
	}

	// WebVTT conversion: a WEBVTT header, dotted timestamps, the text comma kept.
	vtt := tok(`{"expires":` + decs(time.Now().Unix()+3600) + `,"stream_id":12,"sub_id":0,"webvtt":1}`)
	rec = serveReq(s, "/subauth/"+vtt, "198.51.100.7")
	body := rec.Body.String()
	if !strings.HasPrefix(body, "WEBVTT\n\n") || !strings.Contains(body, "00:00:01.000 --> 00:00:04.000") ||
		strings.Contains(body, "00:00:01,000") || !strings.Contains(body, "Hello, world") {
		t.Fatalf("served vtt: %q", body)
	}
	if rec.Header().Get("Content-Disposition") != `attachment; filename="12_0.vtt"` {
		t.Fatalf("vtt disposition: %v", rec.Header())
	}

	// A mode that does not serve subtitles hands it to PHP's @gw_subtitle_php.
	off := NewServer(writeServePolicy(t, dir, "segments+playlist+thumb", withVod), http.NotFoundHandler(), nil)
	rec = serveReq(off, "/subauth/"+token, "198.51.100.7")
	if rec.Header().Get("X-Accel-Redirect") != "@gw_subtitle_php" {
		t.Fatalf("mode off subtitle: want @gw_subtitle_php, got %v", rec.Header())
	}
}
