package gateway

import (
	"os"
	"testing"
)

// A serve-only node shows the connected clip (connected.ts) to a second-address
// viewer — live.php's USER_ALREADY_CONNECTED → showVideoServer
// show_connected_video — when the switch is on and the file is present, else the
// bare second-ip Deny (the front renders it). Mirrors the not_on_air fix for the
// disallow_2nd_ip_con path. A non-serve-only node is unchanged (it defers to PHP).
func TestJudgeLiveServeOnlyConnectedClip(t *testing.T) {
	dir := t.TempDir() + "/"
	clip := dir + "connected.ts"
	if err := os.WriteFile(clip, []byte("CONNECTED"), 0o644); err != nil {
		t.Fatal(err)
	}
	ip := "198.51.100.7"
	second := "203.0.113.9" // the oldest open connection, a different address

	judge := func(serveOnly, showConnected bool, videoPath string) Verdict {
		fa := &fakeAgent{records: map[string]map[string]any{}}
		fa.records["x"] = openRec(second, 100)
		ac := NewAgentConns(startAgent(t, fa))
		p := livePolicy()
		p.Live.Disallow2ndIP, p.ServeOnly = true, serveOnly
		p.OffAir.VideoPath = videoPath
		p.OffAir.Show = map[string]bool{"connected": showConnected}
		st := goodLiveEnv()
		env := st.env()
		env.Oldest = ac.Oldest
		v, _ := JudgeLive(p, liveTokenJSON(nil), ip, testUA, testNow, env)
		return v
	}

	// show_connected on + clip present → serve connected.ts
	if v := judge(true, true, dir); v.Action != Serve || v.Reason != "off-air-clip" || v.Path != clip {
		t.Fatalf("show_connected on: got %s/%s path=%q; want Serve/off-air-clip %q", v.Action, v.Reason, v.Path, clip)
	}
	// switch off → the second-ip Deny (unchanged)
	if v := judge(true, false, dir); v.Action != Deny || v.Reason != "second-ip" {
		t.Fatalf("show_connected off: got %s/%s; want Deny/second-ip", v.Action, v.Reason)
	}
	// no clip on the node → the second-ip Deny
	if v := judge(true, true, ""); v.Action != Deny || v.Reason != "second-ip" {
		t.Fatalf("no clip: got %s/%s; want Deny/second-ip", v.Action, v.Reason)
	}
	// non-serve-only is unchanged: it defers to PHP (which serves the clip itself)
	if v := judge(false, true, dir); v.Action != PHP || v.Reason != "second-ip" {
		t.Fatalf("non-serve-only: got %s/%s; want PHP/second-ip", v.Action, v.Reason)
	}
}
