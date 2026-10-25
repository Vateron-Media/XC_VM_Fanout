package gateway

import (
	"os"
	"testing"
)

// A serve-only node cannot bring a non-serveable stream on air (it holds no
// registered puller and the PHP-free daemon composes no encoder). live.php's
// showNotOnAir (OffAirHandler::showVideoServer with show_not_on_air_video /
// not_on_air_video_path) shows the not_on_air clip (offline.ts) for a
// non-restreamer with the switch on and the file present, else STREAM_OFFLINE.
// The gateway must do the same on serve-only rather than defer with
// php("stream-down")/php("no-playlist")/php("not-on-air") — a 502 for the first
// two, a bare 404 for the last. A non-serve-only node is unchanged (it defers to
// the PHP backend, which starts/serves the stream itself).
func TestJudgeLiveServeOnlyNotOnAirClip(t *testing.T) {
	dir := t.TempDir() + "/"
	clip := dir + "offline.ts"
	if err := os.WriteFile(clip, []byte("OFFLINE"), 0o644); err != nil {
		t.Fatal(err)
	}
	ip := "198.51.100.7"

	judge := func(serveOnly, ts bool, polEdit func(*Policy), envEdit func(*liveEnvState), tokEdit func(map[string]any)) Verdict {
		var p *Policy
		var tok string
		var st *liveEnvState
		if ts {
			p, tok, st = tsPolicy(), firstTSToken(tokEdit), goodTSEnv()
		} else {
			p, tok, st = livePolicy(), firstHLSToken(tokEdit), goodLiveEnv()
		}
		p.Live.CreateExpiration, p.Live.Admission, p.TimeOffset = 5, true, 0
		p.ServeOnly = serveOnly
		p.OffAir.VideoPath = dir
		p.OffAir.Show = map[string]bool{"not_on_air": true}
		if polEdit != nil {
			polEdit(p)
		}
		if envEdit != nil {
			envEdit(st)
		}
		v, _ := JudgeLive(p, tok, ip, testUA, testNow, st.env())
		return v
	}

	// downUnreg: the stream is idle and unregistered — not serveable (scenario A,
	// which short-circuited at php stream-down/no-playlist → a 502 on serve-only).
	downUnreg := func(st *liveEnvState) { st.alive, st.onDisk, st.fed, st.registered = false, false, false, false }
	// liveUnreg: the stream looks alive+on-disk but is not registered, so the TS
	// feed / HLS playlist gates find nothing (scenario B, which denied not-on-air).
	liveUnregTS := func(st *liveEnvState) { st.alive, st.onDisk, st.fed, st.registered = true, true, false, false }
	liveUnregHLS := func(st *liveEnvState) {
		st.alive, st.onDisk, st.registered, st.playlist = true, true, false, ""
	}
	restreamer := func(m map[string]any) { m["user_info"].(map[string]any)["is_restreamer"] = 1 }

	cases := []struct {
		name string
		ts   bool
		env  func(*liveEnvState)
	}{
		{"A/TS down+unregistered", true, downUnreg},
		{"A/HLS down+unregistered", false, downUnreg},
		{"B/TS alive-but-unregistered (not fed)", true, liveUnregTS},
		{"B/HLS alive-but-unregistered (no playlist)", false, liveUnregHLS},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// switch on + clip present → serve offline.ts
			if v := judge(true, c.ts, nil, c.env, nil); v.Action != Serve || v.Reason != "off-air-clip" || v.Path != clip {
				t.Fatalf("serve-only, show on: got %s/%s path=%q; want Serve/off-air-clip %q", v.Action, v.Reason, v.Path, clip)
			}
			// switch off → Deny not-on-air (STREAM_OFFLINE)
			if v := judge(true, c.ts, func(p *Policy) { p.OffAir.Show["not_on_air"] = false }, c.env, nil); v.Action != Deny || v.Reason != "not-on-air" {
				t.Fatalf("serve-only, show off: got %s/%s; want Deny/not-on-air", v.Action, v.Reason)
			}
			// restreamer → Deny even with the switch on
			if v := judge(true, c.ts, nil, c.env, restreamer); v.Action != Deny || v.Reason != "not-on-air" {
				t.Fatalf("serve-only, restreamer: got %s/%s; want Deny/not-on-air", v.Action, v.Reason)
			}
			// no clip on the node → Deny not-on-air
			if v := judge(true, c.ts, func(p *Policy) { p.OffAir.VideoPath = "" }, c.env, nil); v.Action != Deny || v.Reason != "not-on-air" {
				t.Fatalf("serve-only, no clip: got %s/%s; want Deny/not-on-air", v.Action, v.Reason)
			}
			// non-serve-only is unchanged: it defers to PHP (which serves the stream)
			if v := judge(false, c.ts, nil, c.env, nil); v.Action != PHP {
				t.Fatalf("non-serve-only: got %s/%s; want PHP (unchanged defer)", v.Action, v.Reason)
			}
		})
	}
}
