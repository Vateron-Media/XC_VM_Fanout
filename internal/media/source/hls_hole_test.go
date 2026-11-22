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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// holeServer serves a live window that rolls one segment per manifest poll, with
// exactly one segment permanently 404 — a segment the packager lost, which is
// the everyday shape of a hole in an otherwise healthy channel.
func holeServer(t *testing.T, window, dead int) (*httptest.Server, func(int) int) {
	t.Helper()
	var polls atomic.Int64
	var mu sync.Mutex
	got := map[int]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/s"), ".ts"))
			mu.Lock()
			got[n]++
			mu.Unlock()
			if n == dead {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(int64(n) * 90000))
			return
		}
		start := int(polls.Add(1)) - 1
		var b strings.Builder
		fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n", start)
		for i := start; i < start+window; i++ {
			fmt.Fprintf(&b, "#EXTINF:2.0,\ns%d.ts\n", i)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(b.String()))
	}))
	t.Cleanup(srv.Close)
	return srv, func(n int) int {
		mu.Lock()
		defer mu.Unlock()
		return got[n]
	}
}

// TestOneDeadSegmentDoesNotKillTheStream: ending the pass at a failed segment
// keeps the wire in order, but a segment that NEVER comes back then holds the
// whole channel behind it — nothing at all is streamed while it is the oldest
// unseen entry, so the consecutive-failure threshold trips on it and the pipe
// closes. One lost segment out of an otherwise healthy window took the channel
// off air (and, on reconnect, did it again while it was still in the window).
// The ordered retry has to be BOUNDED: hold the pass for a few polls so a
// transient failure is retried in place, then step over the hole in order and
// keep the channel on air.
func TestOneDeadSegmentDoesNotKillTheStream(t *testing.T) {
	srv, fetches := holeServer(t, 6, 5)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	// The join takes the last three of s0..s5: s3 and s4 stream, s5 is the hole.
	// Four whole segments means the pull got past it and kept going.
	one := len(tsSegment(0))
	want := []int{3, 4, 6, 7}
	buf := make([]byte, len(want)*one)
	if _, err := io.ReadFull(rc, buf); err != nil {
		t.Fatalf("read four segments: %v\nOne unrecoverable segment must not take the "+
			"channel off air: the ordered retry has to give up on it and stream what is "+
			"behind it", err)
	}
	for i, n := range want {
		if got := buf[i*one : (i+1)*one]; string(got) != string(tsSegment(int64(n)*90000)) {
			t.Fatalf("segment %d of the stream is not s%d: a skipped hole must not "+
				"reorder what follows it", i, n)
		}
	}
	if n := fetches(5); n < 2 {
		t.Fatalf("the dead segment was fetched %d time(s): it must be retried in place "+
			"before the pull gives up on it, or a transient 503 becomes a hole", n)
	}
	if n := fetches(5); n > 4 {
		t.Fatalf("the dead segment was fetched %d times: the hold on it is unbounded", n)
	}
}

// TestVODWithADeadSegmentStillEndsCleanly: the same bound on an ENDLIST
// playlist. Holding the pass at the dead entry meant the source never reached
// its last segment and ended with "5 consecutive segment failures" instead of
// delivering what it had.
func TestVODWithADeadSegmentStillEndsCleanly(t *testing.T) {
	pl := "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n" +
		"#EXTINF:2.0,\nv0.ts\n#EXTINF:2.0,\nv1.ts\n#EXTINF:2.0,\nv2.ts\n#EXT-X-ENDLIST\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "v1.ts"):
			http.NotFound(w, r) // the middle one is gone for good
		case strings.HasSuffix(r.URL.Path, ".ts"):
			w.Header().Set("Content-Type", "video/mp2t")
			pts := int64(0)
			if strings.HasSuffix(r.URL.Path, "v2.ts") {
				pts = 2 * 90000
			}
			_, _ = w.Write(tsSegment(pts))
		default:
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = io.WriteString(w, pl)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got, rerr := io.ReadAll(rc)
	if rerr != nil {
		t.Fatalf("VOD ended with %v, want a clean end after delivering what it had", rerr)
	}
	want := append(tsSegment(0), tsSegment(2*90000)...)
	if string(got) != string(want) {
		t.Fatalf("delivered %d bytes, want the %d of v0 and v2: a lost middle segment "+
			"must cost its own hole and nothing more", len(got), len(want))
	}
}
