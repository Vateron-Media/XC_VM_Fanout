// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// drain keeps the pipe moving for the length of a test, the way ingest.Copy
// does, so the puller is never blocked on a write when we look at what it
// fetched.
func drain(rc io.ReadCloser) {
	go func() { _, _ = io.Copy(io.Discard, rc) }()
}

// TestTokenisedSegmentURIsAreNotReplayed: plenty of upstreams sign each segment
// URL per playlist response (seg.ts?token=<hmac(now)>). De-duping on the URI
// string, query included, made the whole live window look new on every poll, so
// the same content went into the ring again every target/2 — viewers watched the
// last thirty seconds on a loop, and the bitrate into the stream was several
// times the source's. ffmpeg de-dups on the media sequence and does not.
func TestTokenisedSegmentURIsAreNotReplayed(t *testing.T) {
	var polls, segGets atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, ".ts") {
			segGets.Add(1)
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(0))
			return
		}
		n := polls.Add(1) // a fresh token on every response, same window
		var b strings.Builder
		b.WriteString("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:100\n")
		for i := 0; i < 6; i++ {
			fmt.Fprintf(&b, "#EXTINF:1.0,\ns%d.ts?token=%d\n", i, n)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(b.String()))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	drain(rc)
	time.Sleep(3500 * time.Millisecond) // ~3 polls at the target/2 cadence
	rc.Close()

	if p := polls.Load(); p < 3 {
		t.Fatalf("only %d manifest polls: the test never exercised a re-poll", p)
	}
	if n := segGets.Load(); n > hlsLiveStartSegments {
		t.Fatalf("%d segment fetches over %d polls, want the %d joined at the live edge: "+
			"a re-signed URI is not new content, and replaying it puts the live window "+
			"on the wire again every poll", n, polls.Load(), hlsLiveStartSegments)
	}
}

// TestAnEncoderRestartIsNotSkipped: an encoder that restarts inside one window
// length — a flapping channel, an ffmpeg hls muxer restarting at ch_0.ts — lists
// the same segment names again with MEDIA-SEQUENCE back at zero. Keyed on the
// URI, every one of them was still in `seen`, so the new content was silently
// skipped and the channel sat there until the numbering passed the old window.
func TestAnEncoderRestartIsNotSkipped(t *testing.T) {
	var polls, segGets atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			segGets.Add(1)
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(0))
			return
		}
		seq := int64(100)
		if polls.Add(1) > 1 {
			seq = 0 // the encoder restarted, reusing the same names
		}
		var b strings.Builder
		fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:%d\n", seq)
		for _, n := range []string{"ch_0.ts", "ch_1.ts", "ch_2.ts"} {
			fmt.Fprintf(&b, "#EXTINF:1.0,\n%s\n", n)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(b.String()))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	drain(rc)
	time.Sleep(3500 * time.Millisecond)
	rc.Close()

	if n := segGets.Load(); n < 6 {
		t.Fatalf("%d segment fetches over %d polls, want the 3 before the restart and the 3 "+
			"after: a sequence that goes backwards is a new stream, not content already sent",
			n, polls.Load())
	}
}

// TestAFrozenMediaSequenceStillFollowsTheWindow: some encoders write a constant
// #EXT-X-MEDIA-SEQUENCE while rolling the window under it. Trusting the number
// there would freeze the channel, so the pull has to notice the window moving
// beneath a standing sequence and fall back to de-duping on the URI.
func TestAFrozenMediaSequenceStillFollowsTheWindow(t *testing.T) {
	var polls, segGets atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			segGets.Add(1)
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(0))
			return
		}
		start := int(polls.Add(1)) - 1 // the window rolls, the sequence does not
		var b strings.Builder
		b.WriteString("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n")
		for i := start; i < start+3; i++ {
			fmt.Fprintf(&b, "#EXTINF:1.0,\ns%d.ts\n", i)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(b.String()))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	drain(rc)
	time.Sleep(3500 * time.Millisecond)
	rc.Close()

	// 3 at the join, then one new segment per poll.
	if n := segGets.Load(); n < 5 {
		t.Fatalf("%d segment fetches over %d polls: the channel froze because the upstream's "+
			"media sequence never moves", n, polls.Load())
	}
}
