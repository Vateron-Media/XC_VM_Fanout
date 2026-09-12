// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsseg

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

const vpid = 0x101

// pmtWithPCR is the fixture PMT with its PCR_PID pointing at the video PID, so
// segment timing runs off the PCR like a real broadcast stream.
func pmtWithPCR() []byte {
	p := tsfixture.PMT(0x100, vpid)
	p[13] = byte(0xe0 | (vpid>>8)&0x1f) // section byte 8: PCR_PID
	p[14] = byte(vpid & 0xff)
	return p
}

type rig struct {
	t   *testing.T
	dir string
	s   *Segmenter
	now time.Time
}

func newRig(t *testing.T, cfg Config) *rig {
	t.Helper()
	r := &rig{t: t, dir: t.TempDir(), now: time.Unix(1_700_000_000, 0)}
	cfg.Playlist = filepath.Join(r.dir, "12_.m3u8")
	cfg.SegPattern = filepath.Join(r.dir, "12_%d.ts")
	cfg.Now = func() time.Time { return r.now }
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.s = s
	return r
}

func (r *rig) feed(pkts ...[]byte) {
	r.t.Helper()
	for _, p := range pkts {
		if err := r.s.Feed(p); err != nil {
			r.t.Fatalf("Feed: %v", err)
		}
	}
}

// gop feeds one GOP starting at t seconds: a flagged keyframe with PCR, then a
// few fill packets.
func (r *rig) gop(sec float64) {
	ticks := int64(sec * 90000)
	r.feed(tsfixture.KeyframePCR(vpid, ticks, ticks))
	for i := 0; i < 5; i++ {
		r.feed(tsfixture.Fill(vpid))
	}
}

func (r *rig) playlist() string {
	b, err := os.ReadFile(filepath.Join(r.dir, "12_.m3u8"))
	if err != nil {
		r.t.Fatalf("no playlist: %v", err)
	}
	return string(b)
}

func (r *rig) segFiles() []string {
	var out []string
	entries, _ := os.ReadDir(r.dir)
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

// TestSegmentsAtKeyframesInPanelLayout: segments are cut on keyframes once the
// target is reached, numbered from 0 under the panel's names, and the playlist
// has the shape the panel's own parsers read.
func TestSegmentsAtKeyframesInPanelLayout(t *testing.T) {
	r := newRig(t, Config{TargetSec: 4, ListSize: 3, KeepExtra: 1})
	r.feed(tsfixture.PAT(0x100), pmtWithPCR())
	for g := 0; g <= 12; g++ { // a keyframe every 2s → a segment every 4s
		r.gop(float64(g) * 2)
	}

	pl := r.playlist()
	if !strings.HasPrefix(pl, "#EXTM3U\n#EXT-X-VERSION:3\n") {
		t.Errorf("playlist header:\n%s", pl)
	}
	// Every #EXTINF is immediately followed by its segment's basename:
	// StreamUtils::getStreamBitrate reads the line after each EXTINF as the file.
	lines := strings.Split(strings.TrimSpace(pl), "\n")
	var listed []string
	for i, l := range lines {
		if strings.HasPrefix(l, "#EXTINF:") {
			if i+1 >= len(lines) || !regexp.MustCompile(`^12_\d+\.ts$`).MatchString(lines[i+1]) {
				t.Fatalf("EXTINF not followed by a segment basename:\n%s", pl)
			}
			if d, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimPrefix(l, "#EXTINF:"), ","), 64); d < 3.9 || d > 4.1 {
				t.Errorf("segment duration %s, want 4s from the PCR", l)
			}
			listed = append(listed, lines[i+1])
		}
	}
	if len(listed) != 3 {
		t.Fatalf("playlist lists %d segments, want hls_list_size=3:\n%s", len(listed), pl)
	}
	if !strings.Contains(pl, "#EXT-X-TARGETDURATION:4\n") {
		t.Errorf("TARGETDURATION should be the ceiling of the longest EXTINF:\n%s", pl)
	}
	first, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(listed[0], "12_"), ".ts"))
	if !strings.Contains(pl, fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", first)) {
		t.Errorf("MEDIA-SEQUENCE does not name the first listed segment:\n%s", pl)
	}

	// On disk: the listed segments plus hls_delete_threshold more, nothing else
	// left over, no temporary files.
	var ts []string
	for _, f := range r.segFiles() {
		if strings.HasSuffix(f, ".tmp") && f != "12_"+strconv.Itoa(r.s.seq)+".ts.tmp" {
			t.Errorf("leftover temporary file %s", f)
		}
		if strings.HasSuffix(f, ".ts") {
			ts = append(ts, f)
		}
	}
	if len(ts) != 4 {
		t.Errorf("%d segments on disk (%v), want list 3 + threshold 1", len(ts), ts)
	}
}

// TestFirstSegmentCutEarly: hls_init_time — a cold start lists a segment after
// the first keyframe past InitSec rather than a full target.
func TestFirstSegmentCutEarly(t *testing.T) {
	r := newRig(t, Config{TargetSec: 10, InitSec: 2, ListSize: 5})
	r.feed(tsfixture.PAT(0x100), pmtWithPCR())
	r.gop(0)
	r.gop(2)
	pl := r.playlist()
	if !strings.Contains(pl, "12_0.ts") || !strings.Contains(pl, "#EXTINF:2.000000,") {
		t.Fatalf("first segment was not cut at hls_init_time:\n%s", pl)
	}
	// After that, full targets.
	for g := 2; g <= 8; g++ {
		r.gop(float64(g) * 2)
	}
	if !strings.Contains(r.playlist(), "#EXTINF:10.000000,") {
		t.Errorf("later segments are not full-length:\n%s", r.playlist())
	}
}

// TestSegmentsStartWithTables: each file carries PAT and PMT ahead of its first
// keyframe, so it decodes on its own — the archive worker and any player can
// start from any segment.
func TestSegmentsStartWithTables(t *testing.T) {
	r := newRig(t, Config{TargetSec: 2, ListSize: 5})
	pat, pmt := tsfixture.PAT(0x100), pmtWithPCR()
	r.feed(pat, pmt)
	for g := 0; g <= 4; g++ {
		r.gop(float64(g) * 2)
	}
	for _, n := range []int{1, 2} {
		b, err := os.ReadFile(filepath.Join(r.dir, fmt.Sprintf("12_%d.ts", n)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b[:188], pat) || !bytes.Equal(b[188:376], pmt) {
			t.Errorf("segment %d does not open with PAT+PMT", n)
		}
		if !tspes.RAI(b[376:564]) {
			t.Errorf("segment %d's first media packet is not its keyframe", n)
		}
		if len(b)%188 != 0 {
			t.Errorf("segment %d is not packet-aligned", n)
		}
	}
}

// TestUnflaggedSourceStillSegments: the reason the keyframe scan was ported — a
// source that flags nothing still produces segments, cut on its IDR slices.
func TestUnflaggedSourceStillSegments(t *testing.T) {
	r := newRig(t, Config{TargetSec: 2, ListSize: 5})
	r.feed(tsfixture.PAT(0x100), tsfixture.PMT(0x100, vpid))
	for g := 0; g <= 4; g++ {
		base := int64(g) * 2 * 90000
		r.feed(tsfixture.PESStart(vpid, base, 0x65)) // IDR, no RAI
		r.feed(tsfixture.PESStart(vpid, base+45000, 0x41), tsfixture.Fill(vpid))
	}
	if n := strings.Count(r.playlist(), "#EXTINF:"); n < 3 {
		t.Fatalf("%d segments from an unflagged source:\n%s", n, r.playlist())
	}
}

// TestNoKeyframeGivesUp: a source with no detectable keyframe is not something
// this can segment; after the window it says so, which the remuxer turns into
// "use the fallback".
func TestNoKeyframeGivesUp(t *testing.T) {
	r := newRig(t, Config{TargetSec: 2})
	r.feed(tsfixture.PAT(0x100), tsfixture.PMT(0x100, vpid))
	var err error
	for i := 0; i < 100 && err == nil; i++ {
		r.now = r.now.Add(time.Second)
		err = r.s.Feed(tsfixture.PESStart(vpid, int64(i)*3600, 0x41))
	}
	if !errors.Is(err, ErrNoKeyframe) {
		t.Fatalf("err = %v, want ErrNoKeyframe", err)
	}
}

// TestTimelineJumpIsADiscontinuity: an upstream splice sends the clock
// backwards. That must become EXT-X-DISCONTINUITY and a sane duration, not a
// ~26h EXTINF that stalls every player's reload timer.
func TestTimelineJumpIsADiscontinuity(t *testing.T) {
	r := newRig(t, Config{TargetSec: 2, ListSize: 10})
	r.feed(tsfixture.PAT(0x100), pmtWithPCR())
	for g := 0; g <= 3; g++ {
		r.gop(100 + float64(g)*2)
	}
	for g := 0; g <= 3; g++ { // the source restarts its clock near zero
		r.gop(1 + float64(g)*2)
	}
	pl := r.playlist()
	if strings.Count(pl, "#EXT-X-DISCONTINUITY") < 2 { // discont_start + the splice
		t.Errorf("the splice was not marked:\n%s", pl)
	}
	for _, l := range strings.Split(pl, "\n") {
		if strings.HasPrefix(l, "#EXTINF:") {
			if d, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimPrefix(l, "#EXTINF:"), ","), 64); d > 8 {
				t.Errorf("absurd duration %s after a splice", l)
			}
		}
	}
}

// TestSweepClearsOnlyThisStream: a restart clears the previous run's files so a
// stale 12_0.ts cannot read as this run having started — and never touches
// another stream whose id shares the prefix.
func TestSweepClearsOnlyThisStream(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"12_0.ts", "12_7.ts", "12_3.ts.tmp", "12_.m3u8", "123_0.ts", "12_.pid", "12.errors"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := New(Config{Playlist: filepath.Join(dir, "12_.m3u8"), SegPattern: filepath.Join(dir, "12_%d.ts")}); err != nil {
		t.Fatal(err)
	}
	left, _ := os.ReadDir(dir)
	var names []string
	for _, e := range left {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if want := []string{"12.errors", "123_0.ts", "12_.pid"}; strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("after sweep: %v, want %v", names, want)
	}
}

func TestConfigRejectsUnusablePattern(t *testing.T) {
	for _, p := range []string{"/x/12.ts", "/x/12_%d_%d.ts", "/x/12_%s.ts"} {
		if _, err := New(Config{Playlist: "/x/12_.m3u8", SegPattern: p}); err == nil {
			t.Errorf("pattern %q accepted", p)
		}
	}
}

// TestCutsOnTheKeyframeClockNotThePCR: segments are timed on the keyframes' own
// PTS, as ffmpeg's hls muxer times them. The PCR — sampled from whatever packet
// last carried one before the keyframe — jitters around it, and with the GOP
// equal to the target the next keyframe read as just under target half the time,
// missed the cut and ran a whole GOP long: segments averaging ~3.5 s against
// ffmpeg's exact 2 s, and nearly twice the tmpfs for the same list size.
func TestCutsOnTheKeyframeClockNotThePCR(t *testing.T) {
	r := newRig(t, Config{TargetSec: 2, InitSec: 2, ListSize: 20, KeepExtra: 0, Logf: t.Logf})
	r.feed(tsfixture.PAT(0x100), pmtWithPCR())
	for g := 0; g < 12; g++ {
		pts := int64(g) * 2 * 90000 // a GOP exactly the target, 2 s
		// The PCR arrives on its own packet a little ahead of the keyframe, as
		// muxers place it: 40 ms early on every other GOP.
		early := int64(0)
		if g%2 == 1 {
			early = 3600
		}
		r.feed(pcrOnly(vpid, pts-early), tsfixture.Keyframe(vpid, pts))
		for i := 0; i < 5; i++ {
			r.feed(tsfixture.Fill(vpid))
		}
	}
	durs := regexp.MustCompile(`#EXTINF:([0-9.]+),`).FindAllStringSubmatch(r.playlist(), -1)
	if len(durs) < 10 {
		t.Fatalf("%d segments from 12 two-second GOPs, want one per GOP:\n%s", len(durs), r.playlist())
	}
	for _, d := range durs {
		if v, _ := strconv.ParseFloat(d[1], 64); v < 1.99 || v > 2.01 {
			t.Fatalf("segment of %ss, want every one exactly 2 s:\n%s", d[1], r.playlist())
		}
	}
}

// pcrOnly is an adaptation-only packet on pid carrying just a PCR.
func pcrOnly(pid int, pcr int64) []byte {
	p := make([]byte, tspes.PacketSize)
	p[0] = 0x47
	p[1], p[2] = byte(pid>>8)&0x1f, byte(pid)
	p[3] = 0x20
	p[4] = 183
	p[5] = 0x10 // PCR_flag
	p[6], p[7], p[8], p[9] = byte(pcr>>25), byte(pcr>>17), byte(pcr>>9), byte(pcr>>1)
	p[10] = byte(pcr&1) << 7
	return p
}
