package gateway

import (
	"strings"
	"testing"
	"time"
)

// The gateway's decisions, one request each (Phase 12.5: auth adds ≤ 1 ms p99
// at 5,000 req/s on one core). `go test -bench . -benchmem ./internal/gateway`.

func BenchmarkJudgeSegment(b *testing.B) {
	p, token := testPolicy(), liveTok("3", "12_d345.ts", "198.51.100.7")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if JudgeSegment(p, token, "198.51.100.7", testNow, consAll).Action != Serve {
			b.Fatal("not served")
		}
	}
}

func BenchmarkJudgeKey(b *testing.B) {
	p, token := testPolicy(), tok("198.51.100.7/12")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if JudgeKey(p, token, "198.51.100.7", testNow).Action != Serve {
			b.Fatal("not served")
		}
	}
}

func BenchmarkJudgeLive(b *testing.B) {
	p, token, env := livePolicy(), liveTokenJSON(nil), goodLiveEnv().env()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if v, _ := JudgeLive(p, token, "198.51.100.7", testUA, testNow, env); v.Action != Serve {
			b.Fatal("not served: ", v.Reason)
		}
	}
}

// A refresh's playlist of six segments: six segment tokens minted.
func BenchmarkTokenize(b *testing.B) {
	p := livePolicy()
	var pl strings.Builder
	pl.WriteString("#EXTM3U\n#EXT-X-TARGETDURATION:4\n")
	for i := 100; i < 106; i++ {
		pl.WriteString("#EXTINF:4.0,\n" + itoa(int64(i)) + ".ts\n")
	}
	r := &Refresh{Stream: 12, UUID: testUUID, Username: "user1", Password: "pass1", Codec: "h264", Playlist: pl.String()}
	now := time.Unix(testNow, 0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Tokenize(p, r, "198.51.100.7", now); err != nil {
			b.Fatal(err)
		}
	}
}
