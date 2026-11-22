// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestAFailedSegmentIsRetriedInOrder pins a time-reversal on the wire.
//
// The run loop skipped past a failed segment without marking it seen, but went
// on streaming the LATER segments of the same pass and marking those seen. The
// failed one was still unseen and still in the window at the next poll, so it
// went out AFTER its successor: viewers got a replayed older segment, PTS and
// PCR jumped backwards, and the ring and the HLS cutter saw time run backwards.
// One transient 503 on the middle segment of a three-segment join was enough,
// and against a flaky upstream it happened on every catch-up pass.
func TestAFailedSegmentIsRetriedInOrder(t *testing.T) {
	names := []string{"s0.ts", "s1.ts", "s2.ts"}
	segs := map[string][]byte{}
	byBody := map[string]string{}
	for i, n := range names {
		b := tsSegment(int64(i) * 90000)
		segs[n] = b
		byBody[string(b)] = n
	}
	var failedOnce atomic.Bool
	pl := "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n" +
		"#EXTINF:2.0,\ns0.ts\n#EXTINF:2.0,\ns1.ts\n#EXTINF:2.0,\ns2.ts\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		body, ok := segs[name]
		if !ok {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte(pl))
			return
		}
		// The middle segment has one transient hiccup, then recovers.
		if name == "s1.ts" && failedOnce.CompareAndSwap(false, true) {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	n := len(segs["s0.ts"])
	got := make([]byte, 3*n)
	if _, err := io.ReadFull(rc, got); err != nil {
		t.Fatalf("read three segments: %v", err)
	}
	var order []string
	for i := 0; i < 3; i++ {
		who, ok := byBody[string(got[i*n:(i+1)*n])]
		if !ok {
			t.Fatalf("chunk %d matched no segment: the stream is not segment-aligned", i)
		}
		order = append(order, who)
	}
	for i, want := range names {
		if order[i] != want {
			t.Fatalf("segments went out as %v, want %v: a retry must never put older "+
				"content behind newer content already on the wire", order, names)
		}
	}
}
