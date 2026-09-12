// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package tsseg cuts a live MPEG-TS byte stream into HLS .ts segments on disk,
// copying the source packets verbatim — `ffmpeg -c copy -f hls
// -hls_segment_type mpegts`, in process.
//
// Ported from the P2PTV project's native passthrough segmenter (pkg/remux,
// tspassthrough.go — same author), which runs it in production for sources
// served with no ffmpeg in between. The segmenting rules are P2PTV's, and each
// one is there because a real source needed it:
//
//   - segments open only at a keyframe — random_access_indicator, or failing
//     that the elementary stream's own IDR/IRAP/sequence header
//     (tspes.StartsKeyframe) — so each one decodes on its own;
//   - the latest PAT, PMT and SDT are re-emitted at every segment head, so a
//     player (or the panel's tv_archive worker) can start from any file;
//   - durations come from the PCR, else the video PTS; a backwards step that is
//     not a 33-bit wrap is a source splice, which rebases the clock and marks
//     the next segment EXT-X-DISCONTINUITY instead of producing an absurd
//     EXTINF that would stall every player's reload timer;
//   - the first segment is cut early (InitSec) so a cold-started channel lists
//     a segment sooner;
//   - no keyframe for too long is ErrNoKeyframe: this source needs ffmpeg.
//
// What changed in the port is only the on-disk contract, which here is the one
// XC_VM's own ffmpeg command produces and the rest of the panel reads:
// `<id>_<n>.ts` numbered from 0, a `<id>_.m3u8` media playlist listing the last
// ListSize of them, and KeepExtra more left on disk past the window
// (hls_delete_threshold) for an archive worker reading behind the live edge.
// Writes are atomic — the archive treats "N+1 exists" as proof that N is
// complete — and a segment streams to its temporary file as it arrives rather
// than being held in memory, so the process stays small.
package tsseg

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// ErrNoKeyframe reports a source whose keyframes cannot be found: nothing
// flagged, and nothing recognisable in the elementary stream, for longer than
// Config.MaxNoKeyframe. Such a source cannot be cut into decodable segments
// without re-encoding or time-slicing, both of which ffmpeg can do.
var ErrNoKeyframe = errors.New("tsseg: no keyframe detected")

const (
	// maxSegBytes bounds one segment. A next keyframe that never arrives must
	// not grow a file without limit on a tmpfs shared with every other channel;
	// at this size something is wrong, so the segment is dropped and the
	// segmenter waits for the next keyframe.
	maxSegBytes = 64 << 20

	// pcrWrapFloor separates a 33-bit counter wrap from a source timeline jump:
	// a genuine wrap leaves the delta near -2^33, a splice or an upstream encoder
	// restart leaves it small and negative.
	pcrWrapFloor = -(int64(1) << 32)

	// fpsWindow is the wall-clock span over which frames are counted for the
	// reported frame rate. The stream is realtime, so frames over wall time is
	// the frame rate, with no PTS reordering or wrap to handle.
	fpsWindow = 5 * time.Second

	writeBuf = 64 << 10
)

// Config is where segments go and how many are kept, mirroring the ffmpeg hls
// muxer options the panel sets.
type Config struct {
	Playlist   string  // absolute path of the media playlist (<id>_.m3u8)
	SegPattern string  // absolute segment path with one %d (…/<id>_%d.ts)
	TargetSec  int     // hls_time
	InitSec    float64 // hls_init_time: the first segment's target; 0 = TargetSec
	ListSize   int     // hls_list_size: segments listed in the playlist
	KeepExtra  int     // hls_delete_threshold: segments kept on disk past the list
	// MaxNoKeyframe is how long the source may go without a detectable keyframe
	// before Feed returns ErrNoKeyframe. 0 = 3×TargetSec + 6s, P2PTV's value.
	MaxNoKeyframe time.Duration

	Logf func(format string, args ...any) // nil = silent
	Now  func() time.Time                 // nil = time.Now (tests)
}

func (c *Config) normalise() error {
	if c.Playlist == "" || c.SegPattern == "" {
		return errors.New("tsseg: playlist and segment pattern are required")
	}
	// A pattern with no numeric verb would send every segment to one file,
	// which looks like it works while destroying the recording.
	if strings.Count(c.SegPattern, "%d") != 1 || strings.Count(c.SegPattern, "%") != 1 {
		return fmt.Errorf("tsseg: segment pattern %q must contain exactly one %%d", c.SegPattern)
	}
	if c.TargetSec <= 0 {
		c.TargetSec = 10
	}
	if c.InitSec <= 0 || c.InitSec > float64(c.TargetSec) {
		c.InitSec = float64(c.TargetSec)
	}
	if c.ListSize <= 0 {
		c.ListSize = 6
	}
	if c.KeepExtra < 0 {
		c.KeepExtra = 0
	}
	if c.MaxNoKeyframe <= 0 {
		c.MaxNoKeyframe = time.Duration(3*c.TargetSec+6) * time.Second
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return nil
}

// segment is one written file.
type segment struct {
	seq  int
	dur  float64
	disc bool
}

// Stats is what the segmenter has seen, for the progress report.
type Stats struct {
	Frames   int64   // video access units (PES starts on the video PID)
	FPS      float64 // over the last fpsWindow
	Segments int     // segments written
	MediaSec float64 // stream time covered, from the video clock
}

// Segmenter is fed one packet at a time. Not safe for concurrent use.
type Segmenter struct {
	cfg Config

	pmtPID, videoPID, pcrPID uint16
	videoType                byte
	havePMT                  bool
	pat, pmt, sdt            []byte // latest raw tables, re-emitted per segment

	// open segment
	f        *os.File
	w        *bufio.Writer
	tmpPath  string
	segBytes int
	segOpen  bool
	startPCR int64
	startPTS int64
	seq      int

	lastPCR, lastPTS int64
	havePTS          bool
	firstDone        bool
	pendingDisc      bool

	written []segment // oldest→newest, still on disk

	lastKey    time.Time
	frames     int64
	fpsFrames  int64
	fpsStart   time.Time
	fps        float64
	hiPTS      int64 // highest video PTS seen, for MediaSec
	haveHi     bool
	mediaTicks int64
}

// New prepares a segmenter and clears whatever a previous run left under the
// same names — numbering restarts at 0, and a stale <id>_0.ts from the last
// life would read to the panel as this one having started.
func New(cfg Config) (*Segmenter, error) {
	if err := cfg.normalise(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Playlist), 0o755); err != nil {
		return nil, err
	}
	s := &Segmenter{
		cfg:      cfg,
		startPCR: -1, startPTS: -1, lastPCR: -1, lastPTS: -1,
		lastKey: cfg.Now(),
	}
	s.sweep()
	return s, nil
}

// Feed consumes one 188-byte packet.
func (s *Segmenter) Feed(pkt []byte) error {
	if len(pkt) != tspes.PacketSize || pkt[0] != 0x47 {
		return nil
	}
	pid := tspes.PID(pkt)
	pusi := tspes.PUSI(pkt)

	switch {
	case pid == 0x0000 && pusi:
		s.pat = append(s.pat[:0], pkt...)
		if !s.havePMT {
			if p := tspes.PMTPID(pkt); p != 0 {
				s.pmtPID = p
			}
		}
	case pid == 0x0011 && pusi: // SDT — carried for players that show the service name
		s.sdt = append(s.sdt[:0], pkt...)
	case s.pmtPID != 0 && pid == s.pmtPID && pusi:
		s.pmt = append(s.pmt[:0], pkt...)
		if !s.havePMT {
			if m, ok := tspes.ParsePMT(pkt); ok {
				s.videoPID, s.pcrPID, s.videoType, s.havePMT = m.VideoPID, m.PCRPID, m.VideoType, true
			}
		}
	}

	if s.havePMT && pid == s.pcrPID {
		if pcr, ok := tspes.PCR(pkt); ok {
			s.lastPCR = pcr
		}
	}

	key := false
	if s.havePMT && pid == s.videoPID && pusi {
		s.countFrame()
		if pts, ok := tspes.PTS(pkt); ok {
			s.notePTS(pts)
		}
		key = tspes.RAI(pkt) || tspes.StartsKeyframe(pkt, s.videoType)
	}

	now := s.cfg.Now()
	if key {
		s.lastKey = now
		cut := int64(float64(s.cfg.TargetSec) * 90000)
		if !s.firstDone {
			cut = int64(s.cfg.InitSec * 90000)
		}
		if s.segOpen && s.elapsed() >= cut {
			s.finalize()
		}
		if !s.segOpen {
			s.open()
		}
	} else if now.Sub(s.lastKey) > s.cfg.MaxNoKeyframe {
		return fmt.Errorf("%w for %s", ErrNoKeyframe, s.cfg.MaxNoKeyframe)
	}

	if s.segOpen {
		s.write(pkt)
	}
	return nil
}

// Close finishes the open segment (a clean stop) and releases the file.
func (s *Segmenter) Close() {
	s.finalize()
}

// Stats reports what has been seen so far.
func (s *Segmenter) Stats() Stats {
	return Stats{Frames: s.frames, FPS: s.fps, Segments: s.seq, MediaSec: float64(s.mediaTicks) / 90000}
}

func (s *Segmenter) countFrame() {
	s.frames++
	now := s.cfg.Now()
	if s.fpsStart.IsZero() {
		s.fpsStart = now
	}
	s.fpsFrames++
	if el := now.Sub(s.fpsStart); el >= fpsWindow {
		s.fps = float64(s.fpsFrames) / el.Seconds()
		s.fpsStart, s.fpsFrames = now, 0
	}
}

// notePTS tracks the video clock, and how much stream time it has covered for
// the progress report (out_time). Stream time advances with the HIGHEST PTS
// seen, because B-frames arrive with PTS below the frame before them; a jump of
// more than a few seconds either way is a splice, which moves the mark without
// adding to the total.
func (s *Segmenter) notePTS(pts int64) {
	s.lastPTS, s.havePTS = pts, true
	if !s.haveHi {
		s.hiPTS, s.haveHi = pts, true
		return
	}
	d := pts - s.hiPTS
	if d < pcrWrapFloor {
		d += 1 << 33 // 33-bit wrap
	}
	switch {
	case d > 0 && d < 10*90000:
		s.mediaTicks += d
		s.hiPTS = pts
	case d >= 10*90000 || d < -2*90000: // splice: re-anchor, add nothing
		s.hiPTS = pts
	}
}

// elapsed is the open segment's duration in 90 kHz ticks: on the video PTS, as
// ffmpeg's hls muxer measures it, else on the PCR.
//
// Every caller asks at a keyframe, when lastPTS is that keyframe's own
// presentation time and startPTS the one that opened the segment — so the answer
// is exactly the GOPs in between. The PCR, which this used first, is sampled from
// whatever packet last carried one before the keyframe, and jitters by tens of
// milliseconds around it: with the GOP equal to the target (a 2 s GOP under a
// 2 s, 4 s or 6 s hls_time) the next keyframe read as "1.96 s" about half the
// time, missed the cut, and the segment ran a whole GOP long — segments averaging
// 3.5 s against ffmpeg's exact 2 s, and nearly twice the tmpfs.
func (s *Segmenter) elapsed() int64 {
	if s.havePTS && s.startPTS >= 0 {
		return s.delta(s.lastPTS, s.startPTS)
	}
	if s.startPCR >= 0 && s.lastPCR >= 0 {
		return s.delta(s.lastPCR, s.startPCR)
	}
	return 0
}

// delta is last-start across a 33-bit wrap. A backwards step that is not a wrap
// is a timeline jump: rebase and flag a discontinuity rather than report ~26h.
func (s *Segmenter) delta(last, start int64) int64 {
	d := last - start
	if d < 0 {
		if d >= pcrWrapFloor {
			s.startPCR, s.startPTS = s.lastPCR, s.lastPTS
			s.pendingDisc = true
			return 0
		}
		d += 1 << 33
	}
	return d
}

func (s *Segmenter) open() {
	path := fmt.Sprintf(s.cfg.SegPattern, s.seq)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		s.cfg.Logf("segment %d: %v", s.seq, err)
		return
	}
	s.f, s.w, s.tmpPath = f, bufio.NewWriterSize(f, writeBuf), tmp
	s.segBytes, s.segOpen = 0, true
	s.startPCR, s.startPTS = s.lastPCR, s.lastPTS
	// PSI first, so the file decodes from its first byte.
	for _, t := range [][]byte{s.pat, s.pmt, s.sdt} {
		if len(t) == tspes.PacketSize {
			s.write(t)
		}
	}
}

func (s *Segmenter) write(pkt []byte) {
	if _, err := s.w.Write(pkt); err != nil {
		s.cfg.Logf("segment %d: write: %v", s.seq, err)
		s.abandon()
		return
	}
	s.segBytes += len(pkt)
	if s.segBytes > maxSegBytes {
		s.cfg.Logf("segment %d passed %d bytes without a keyframe; dropped", s.seq, maxSegBytes)
		s.abandon()
	}
}

// abandon discards the open segment; the next keyframe opens a fresh one.
func (s *Segmenter) abandon() {
	if s.f != nil {
		_ = s.f.Close()
		_ = os.Remove(s.tmpPath)
	}
	s.f, s.w, s.segOpen = nil, nil, false
}

// finalize closes the open segment, publishes it and rewrites the playlist.
func (s *Segmenter) finalize() {
	if !s.segOpen || s.f == nil {
		return
	}
	psi := 0
	for _, t := range [][]byte{s.pat, s.pmt, s.sdt} {
		if len(t) == tspes.PacketSize {
			psi += tspes.PacketSize
		}
	}
	if s.segBytes <= psi { // tables only, nothing to play
		s.abandon()
		return
	}

	dur := float64(s.elapsed()) / 90000
	if dur <= 0 {
		dur = float64(s.cfg.TargetSec)
	}
	// No keyframe-cut segment legitimately runs 4× the target, and the playlist
	// advertises the longest EXTINF as TARGETDURATION — one bogus value would set
	// every player's reload interval to it.
	if max := 4 * float64(s.cfg.TargetSec); dur > max {
		dur = max
	}

	err := s.w.Flush()
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	path := fmt.Sprintf(s.cfg.SegPattern, s.seq)
	if err == nil {
		err = os.Rename(s.tmpPath, path)
	}
	s.f, s.w, s.segOpen = nil, nil, false
	if err != nil {
		s.cfg.Logf("segment %d: %v", s.seq, err)
		_ = os.Remove(s.tmpPath)
		return
	}

	// discont_start: the first segment of a run is a discontinuity, as the
	// panel's ffmpeg marks it, so a player resets across a producer restart.
	disc := s.pendingDisc || len(s.written) == 0 && s.seq == 0
	s.pendingDisc = false
	s.written = append(s.written, segment{seq: s.seq, dur: dur, disc: disc})
	s.seq++
	s.firstDone = true

	if err := writeAtomic(s.cfg.Playlist, []byte(s.render())); err != nil {
		s.cfg.Logf("playlist: %v", err)
	}
	s.prune()
}

// render is the media playlist in the ffmpeg hls muxer's shape — what the
// panel's own parsers (StreamUtils::getPlaylistSegments, getStreamBitrate)
// expect: every #EXTINF immediately followed by its segment's basename.
func (s *Segmenter) render() string {
	list := s.written
	if n := len(list); n > s.cfg.ListSize {
		list = list[n-s.cfg.ListSize:]
	}
	longest := 0.0
	for _, sg := range list {
		longest = math.Max(longest, sg.dur)
	}
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(longest)))
	fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n", list[0].seq)
	for _, sg := range list {
		if sg.disc {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		fmt.Fprintf(&b, "#EXTINF:%.6f,\n%s\n", sg.dur, filepath.Base(fmt.Sprintf(s.cfg.SegPattern, sg.seq)))
	}
	return b.String()
}

// prune deletes segments that rolled out of the list more than KeepExtra ago.
func (s *Segmenter) prune() {
	keep := s.cfg.ListSize + s.cfg.KeepExtra
	for len(s.written) > keep {
		_ = os.Remove(fmt.Sprintf(s.cfg.SegPattern, s.written[0].seq))
		s.written = s.written[1:]
	}
}

// sweep removes a previous run's playlist and segments under these names. Only
// prefix+digits(+.tmp) names are touched, so a stream whose id is a prefix of
// another's (1 and 10) never deletes its neighbour's files.
func (s *Segmenter) sweep() {
	_ = os.Remove(s.cfg.Playlist)
	dir, base := filepath.Split(s.cfg.SegPattern)
	i := strings.Index(base, "%d")
	prefix, suffix := base[:i], base[i+2:]
	entries, err := os.ReadDir(filepath.Clean(dir))
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, prefix) {
			continue
		}
		mid := strings.TrimPrefix(name, prefix)
		mid = strings.TrimSuffix(mid, ".tmp")
		if !strings.HasSuffix(mid, suffix) {
			continue
		}
		mid = strings.TrimSuffix(mid, suffix)
		if mid == "" || strings.Trim(mid, "0123456789") != "" {
			continue
		}
		_ = os.Remove(filepath.Join(dir, name))
	}
}

// writeAtomic writes through a temporary file and renames it into place, so a
// reader never sees a half-written playlist — and never sees it missing, which
// the archive worker would read as the stream having stopped.
func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
