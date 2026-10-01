package nativesrc

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

// A VOD playlist is read in real time, less hlsVODLead, not at download speed:
// a channel made of a finite playlist reached its end in seconds and started
// over, and its viewers got not-on-air.
func TestHLSVODIsPacedToItsDurations(t *testing.T) {
	old := hlsVODLead
	hlsVODLead = 0.3
	t.Cleanup(func() { hlsVODLead = old })
	segs := map[string][]byte{}
	pl := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n"
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("s%d.ts", i)
		segs[name] = tsSegment(int64(i) * 27000)
		pl += "#EXTINF:0.3,\n" + name + "\n"
	}
	pl += "#EXT-X-ENDLIST\n"
	srv := newHLSServer(t, pl, segs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	elapsed := time.Since(start)
	want := 0
	for _, b := range segs {
		want += len(b)
	}
	if len(got) != want {
		t.Fatalf("read %d bytes, want all %d", len(got), want)
	}
	// The fifth segment goes out once 1.2 s of the playlist has played, less
	// the 0.3 s lead: 0.9 s at the least.
	if elapsed < 850*time.Millisecond {
		t.Fatalf("the playlist was read in %v: not paced", elapsed)
	}
}
