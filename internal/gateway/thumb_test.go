package gateway

import (
	"strconv"
	"testing"
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
}
