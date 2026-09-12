// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsmeta"
)

// TestRemuxEndToEndProducesPlayableSegments walks the whole path a real channel
// takes — source bytes in, HLS out — and checks that what comes out the far end
// is actually playable rather than merely non-empty.
//
// Run it with -v for a report of what was produced (segment count, durations,
// sizes, bitrate, detected codecs), which is the quickest way to answer "is the
// remux working" without a live node.
func TestRemuxEndToEndProducesPlayableSegments(t *testing.T) {
	const (
		streamID   = "900"
		segTargetS = 2
		keyframes  = 12 // 12 GOPs, 1s apart
		fillPerGOP = 40 // padding packets per GOP, so segments have real size
	)

	m := NewManager(1<<20, 30000, segTargetS, 6, time.Second)
	m.EnableSupervision()
	st := m.GetOrCreate(streamID)

	// Program structure first, exactly as a source opens: PAT, then a PMT
	// declaring H.264 video and AC-3 audio.
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(pmtWithAudio(0x100, 0x101, 0x102))

	// Then a keyframe-led GOP every second, with filler in between — the shape
	// the segmenter cuts on.
	published := 0
	for i := 0; i < keyframes; i++ {
		st.Publish(tsfixture.KeyframePCR(0x101, int64(i)*90000, int64(i)*90000))
		published++
		for j := 0; j < fillPerGOP; j++ {
			st.Publish(tsfixture.Fill(0x101))
			st.Publish(tsfixture.Fill(0x102)) // audio keeps flowing too
			published += 2
		}
	}
	t.Logf("fed %d TS packets (%d KB) as %d GOPs %ds apart",
		published, published*188/1024, keyframes, 1)

	// ── the playlist ──────────────────────────────────────────────────────
	ts := httptest.NewServer(m.ClientHandler())
	defer ts.Close()

	body, ctype, code := get(t, ts.URL+"/hls/"+streamID+"/index.m3u8")
	if code != http.StatusOK {
		t.Fatalf("playlist returned %d; the remux produced no segments at all", code)
	}
	if ctype != "application/vnd.apple.mpegurl" {
		t.Errorf("playlist content-type = %q", ctype)
	}
	pl := string(body)
	t.Logf("playlist:\n%s", strings.TrimSpace(pl))

	var seqs []int
	var durs []float64
	for _, line := range strings.Split(pl, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#EXTINF:") {
			d, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimPrefix(line, "#EXTINF:"), ","), 64)
			durs = append(durs, d)
		}
		if strings.HasSuffix(line, ".ts") {
			if n, err := strconv.Atoi(strings.TrimSuffix(line, ".ts")); err == nil {
				seqs = append(seqs, n)
			}
		}
	}
	if len(seqs) == 0 {
		t.Fatal("playlist lists no segments — the remux is not cutting")
	}
	if len(durs) != len(seqs) {
		t.Fatalf("%d #EXTINF lines for %d segments; a player will reject that", len(durs), len(seqs))
	}
	if !strings.Contains(pl, "#EXTM3U") || !strings.Contains(pl, "#EXT-X-TARGETDURATION:") {
		t.Error("playlist is missing the tags a player requires")
	}

	// Durations must be sane: the segmenter targets segTargetS, and a segment
	// far off that means the clock is being read wrong.
	for i, d := range durs {
		if d <= 0 {
			t.Errorf("segment %d has duration %.3f", seqs[i], d)
		}
		if d > float64(segTargetS)*3 {
			t.Errorf("segment %d is %.3fs against a %ds target — the segment clock looks wrong", seqs[i], d, segTargetS)
		}
	}

	// ── the segments ──────────────────────────────────────────────────────
	total := 0
	for i, seq := range seqs {
		seg, ctype, code := get(t, fmt.Sprintf("%s/hls/%s/%d.ts", ts.URL, streamID, seq))
		if code != http.StatusOK {
			t.Fatalf("segment %d listed in the playlist returned %d — a player would stall here", seq, code)
		}
		if ctype != "video/mp2t" {
			t.Errorf("segment %d content-type = %q", seq, ctype)
		}
		if len(seg) == 0 {
			t.Fatalf("segment %d is empty", seq)
		}
		if len(seg)%188 != 0 {
			t.Errorf("segment %d is %d bytes, not a whole number of TS packets", seq, len(seg))
		}
		// Every packet must carry the sync byte, or it is not decodable.
		for off := 0; off < len(seg); off += 188 {
			if seg[off] != 0x47 {
				t.Fatalf("segment %d lost TS alignment at byte %d", seq, off)
			}
		}
		// A segment must open with the program tables and contain a random-access
		// point, or a player joining on it has nothing to start from.
		if !hasPID(seg, 0) {
			t.Errorf("segment %d does not start with a PAT; a player cannot decode it standalone", seq)
		}
		if !hasPID(seg, 0x100) {
			t.Errorf("segment %d carries no PMT", seq)
		}
		if !hasKeyframe(seg) {
			t.Errorf("segment %d contains no random-access point", seq)
		}
		total += len(seg)
		t.Logf("segment %d: %6d bytes (%3d packets), %.3fs", seq, len(seg), len(seg)/188, durs[i])
	}
	t.Logf("=> %d segments, %d KB total, mean %.3fs", len(seqs), total/1024, mean(durs))

	// ── what the daemon derived about the stream ─────────────────────────
	meta, ok := m.StreamMetadata(streamID)
	if !ok {
		t.Fatal("no metadata derived from a stream that is clearly flowing")
	}
	t.Logf("derived: video=%q audio=%q %dx%d bitrate=%dkbps",
		meta.VideoCodec, meta.AudioCodec, meta.Width, meta.Height, meta.BitrateKbps)
	if meta.VideoCodec != "h264" {
		t.Errorf("VideoCodec = %q, want h264", meta.VideoCodec)
	}
	if meta.AudioCodec != "ac3" {
		t.Errorf("AudioCodec = %q, want ac3", meta.AudioCodec)
	}

	// And a live-TS viewer must get a clean entry point out of the same ring.
	// Read only the join burst and hang up: /live is an endless response, so
	// reading it to EOF would sit here until the viewer idle-timeout fired.
	snapBody := readJoinBurst(t, ts.URL+"/live/"+streamID+"?prebuffer=0")
	if len(snapBody) == 0 || len(snapBody)%188 != 0 {
		t.Errorf("live TS delivered %d bytes, not packet-aligned", len(snapBody))
	}
	if !hasKeyframe(snapBody) {
		t.Error("live TS join burst carries no random-access point; a player would show nothing")
	}
	t.Logf("live TS join burst: %d bytes (%d packets)", len(snapBody), len(snapBody)/188)
}

// readJoinBurst opens a live-TS viewer, reads what the daemon sends on connect,
// and disconnects. /live never ends by design, so a test must bound its own read
// rather than wait for the server to stop talking.
func readJoinBurst(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("live TS returned %d", resp.StatusCode)
	}
	// The burst arrives as several writes — the tables, then each GOP, straight
	// out of the ring — so read until the connection goes quiet rather than
	// trusting one Read to return all of it.
	var out []byte
	buf := make([]byte, 64<<10)
	for len(out) < 1<<20 {
		type res struct {
			n   int
			err error
		}
		ch := make(chan res, 1)
		go func() {
			n, err := resp.Body.Read(buf)
			ch <- res{n, err}
		}()
		select {
		case r := <-ch:
			out = append(out, buf[:r.n]...)
			if r.err != nil {
				return out
			}
		case <-time.After(200 * time.Millisecond):
			return out // quiet: the burst is done, the live tail has not started
		}
	}
	return out
}

// hasPID reports whether any packet in the buffer is on the given PID.
func hasPID(ts []byte, pid int) bool {
	for off := 0; off+188 <= len(ts); off += 188 {
		if (int(ts[off+1]&0x1f)<<8)|int(ts[off+2]) == pid {
			return true
		}
	}
	return false
}

// hasKeyframe reports whether any packet carries the random-access indicator,
// which is what a decoder needs to start from.
func hasKeyframe(ts []byte) bool {
	for off := 0; off+188 <= len(ts); off += 188 {
		pkt := ts[off : off+188]
		afc := (pkt[3] >> 4) & 0x3
		if (afc == 2 || afc == 3) && pkt[4] > 0 && pkt[5]&0x40 != 0 {
			return true
		}
	}
	return false
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

// TestRemuxSegmentsAreSelfContained: each segment must be decodable on its own,
// because an HLS player may join at any of them. This re-parses a segment in
// isolation and checks the daemon can still read the program out of it.
func TestRemuxSegmentsAreSelfContained(t *testing.T) {
	m := NewManager(1<<20, 30000, 2, 6, time.Second)
	st := m.GetOrCreate("901")
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(pmtWithAudio(0x100, 0x101, 0x102))
	for i := 0; i < 10; i++ {
		st.Publish(tsfixture.KeyframePCR(0x101, int64(i)*90000, int64(i)*90000))
		for j := 0; j < 20; j++ {
			st.Publish(tsfixture.Fill(0x101))
		}
	}

	pl := st.Hub.HLSPlaylist()
	if pl == "" {
		t.Fatal("no playlist produced")
	}
	seq := -1
	for _, line := range strings.Split(pl, "\n") {
		if line = strings.TrimSpace(line); strings.HasSuffix(line, ".ts") {
			seq, _ = strconv.Atoi(strings.TrimSuffix(line, ".ts"))
		}
	}
	if seq < 0 {
		t.Fatal("playlist lists no segment")
	}

	seg := st.Hub.HLSSegment(seq)
	if seg == nil {
		t.Fatalf("segment %d listed but not servable", seq)
	}
	// Parsed cold, with no prior context, exactly as a joining player sees it.
	info := tsmeta.Parse(seg)
	if info.VideoCodec != "h264" {
		t.Errorf("a segment read in isolation reports video=%q; it is not self-contained", info.VideoCodec)
	}
	if info.AudioCodec != "ac3" {
		t.Errorf("a segment read in isolation reports audio=%q", info.AudioCodec)
	}
}
