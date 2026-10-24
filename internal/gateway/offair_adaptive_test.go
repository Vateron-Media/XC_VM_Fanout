package gateway

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestJudgeLiveOffAirClip(t *testing.T) {
	dir := t.TempDir() + "/"
	clip := dir + "offline.ts"
	custom := filepath.Join(dir, "custom.ts")
	for _, f := range []string{clip, custom} {
		if err := os.WriteFile(f, []byte("CLIP"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ip := "198.51.100.7"
	judge := func(serveOnly bool, edit func(*Policy), tokEdit func(map[string]any)) Verdict {
		p := livePolicy()
		p.ServeOnly = serveOnly
		p.OffAir.VideoPath = dir
		if edit != nil {
			edit(p)
		}
		v, _ := JudgeLive(p, liveTokenJSON(tokEdit), ip, testUA, testNow, goodLiveEnv().env())
		return v
	}
	named := func(m map[string]any) { m["off_air"] = "not_on_air" }

	if v := judge(true, nil, named); v.Action != Serve || v.Reason != "off-air-clip" || v.Path != clip {
		t.Fatalf("VIDEO_PATH fallback: %+v", v)
	}
	if v := judge(true, func(p *Policy) { p.OffAir.Paths = map[string]string{"not_on_air_video_path": custom} }, named); v.Path != custom {
		t.Fatalf("configured path: %+v", v)
	}
	if v := judge(true, nil, func(m map[string]any) { m["video_path"] = custom }); v.Path != custom {
		t.Fatalf("legacy video_path: %+v", v)
	}
	if v := judge(true, nil, func(m map[string]any) { m["off_air"] = "banned" }); v.Action != Deny || v.Reason != "off-air-missing" {
		t.Fatalf("missing clip: %+v", v)
	}
	if v := judge(false, nil, named); v.Action != PHP || v.Reason != "off-air" {
		t.Fatalf("not serve-only: %+v", v)
	}

	rec := httptest.NewRecorder()
	if !(&Server{}).serveOffAir(rec, httptest.NewRequest("GET", "/", nil), clip) {
		t.Fatal("serveOffAir false")
	}
	if rec.Header().Get("Content-Type") != "video/mp2t" || rec.Body.String() != "CLIP" {
		t.Fatalf("got %q %q", rec.Header().Get("Content-Type"), rec.Body.String())
	}
}

func TestJudgeLiveAdaptiveSharesMasterConnection(t *testing.T) {
	ip := "198.51.100.7"
	master := hlsConnectionKey("u41", 99, ip, testUA)
	variant := func(stream int) string {
		return firstHLSToken(func(m map[string]any) { m["stream_id"], m["adaptive"] = stream, []int{3, 99} })
	}
	p := livePolicy()
	p.ServeOnly, p.Live.CreateExpiration, p.Live.Admission = true, 5, true

	// request 1: no record yet, created under the master's key
	st := goodLiveEnv()
	st.rec, st.recUUID = nil, master
	v, r := JudgeLive(p, variant(12), ip, testUA, testNow, st.env())
	if v.Action != Serve || r == nil || !r.New || r.UUID != master {
		t.Fatalf("first: %+v %+v", v, r)
	}
	// request 2: another variant finds that record (stream_id differs) and matches
	st = goodLiveEnv()
	st.recUUID = master
	st.rec["uuid"], st.rec["stream_id"], st.rec["server_id"] = master, 12, 7
	v, r = JudgeLive(p, variant(13), ip, testUA, testNow, st.env())
	if v.Action != Serve || r == nil || r.New || r.UUID != master || r.Stream != 13 {
		t.Fatalf("second: %+v %+v", v, r)
	}
	// without adaptive[1] the key falls back to the variant's own stream
	_, r = JudgeLive(p, firstHLSToken(func(m map[string]any) { m["adaptive"] = []int{3} }), ip, testUA, testNow, func() LiveEnv { s := goodLiveEnv(); s.rec = nil; s.recUUID = liveUUID(); return s.env() }())
	if r == nil || r.UUID != liveUUID() {
		t.Fatalf("fallback: %+v", r)
	}
	// a transition node keeps deferring
	p.ServeOnly = false
	if v, _ := JudgeLive(p, variant(12), ip, testUA, testNow, goodLiveEnv().env()); v.Action != PHP || v.Reason != "off-air" {
		t.Fatalf("not serve-only: %+v", v)
	}
}
