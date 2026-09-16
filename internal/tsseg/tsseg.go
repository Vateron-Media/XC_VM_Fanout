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
//   - durations come from the PCR, else the video PTS; a step between keyframes
//     that no wrap and no keyframe interval explains — backwards, or far enough
//     forward — is a source splice, which cuts the segment at that keyframe,
//     rebases the clock and marks the NEXT segment EXT-X-DISCONTINUITY — so the
//     jump falls on a segment boundary a player can reset at, rather than inside
//     a file where no tag can mark it, and no EXTINF is absurd enough to stall
//     every player's reload timer;
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

	// spliceForward is the floor under the forward step between two keyframes
	// that is read as a splice rather than as a long GOP. An encoder that
	// restarts ahead of itself lands minutes or hours away, while a keyframe
	// interval of half a minute is not something this can cut into hls_time
	// segments anyway. The floor matters because the bound is otherwise
	// 4×TargetSec — the span finalize refuses to believe — and under hls_time=2
	// that would be 8s, close enough to a real sparse-keyframe source to mark
	// every one of its segments discontinuous.
	spliceForward = 30 * 90000

	// fpsWindow is the wall-clock span over which frames are counted for the
	// reported frame rate. The stream is realtime, so frames over wall time is
	// the frame rate, with no PTS reordering or wrap to handle.
	fpsWindow = 5 * time.Second

	// deliveryStep bounds how much of one gap between packets counts towards
	// MaxNoKeyframe. A source is not obliged to deliver continuously: a native
	// HLS pull hands over a whole upstream segment at once and then says nothing
	// until the next one — with a ten-second upstream target duration, gaps of
	// 11-13s are healthy, and the reader's own idle bound (3×TD) treats them as
	// such. Charging the wall clock counted that silence as "this source shows
	// no keyframes", so under hls_time=2 (a 12s limit) the first packet of the
	// next burst tripped ErrNoKeyframe and the remuxer moved a perfectly
	// segmentable source to ffmpeg for good. One second per gap still lets a
	// source that is genuinely delivering video without keyframes reach the
	// limit, and costs a silent one almost nothing.
	deliveryStep = time.Second

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
	// A pattern with no numeric verb would send every segment to one file, which
	// looks like it works while destroying the recording. The verb has to be in
	// the file's own name: a %d in the directory part counts once here but names
	// one directory per segment, and every reader of these files — the sweep
	// below, the playlist, the panel's archive worker — looks for the number in
	// the base name.
	if strings.Count(c.SegPattern, "%") != 1 || strings.Count(filepath.Base(c.SegPattern), "%d") != 1 {
		return fmt.Errorf("tsseg: segment pattern %q must contain exactly one %%d, in its file name", c.SegPattern)
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

// maxPMTPackets bounds what a PMT PID may buffer before the segmenter gives up
// on the section: the PSI maximum is 1024 bytes, which is six payloads.
const maxPMTPackets = 8

// Segmenter is fed one packet at a time. Not safe for concurrent use.
type Segmenter struct {
	cfg Config

	pmtPID, videoPID, pcrPID uint16
	videoType                byte
	havePMT                  bool
	pat, pmt, sdt            []byte // latest raw tables, re-emitted per segment
	// pmtAsm folds a PMT section that spans several packets; pmtPkts holds
	// those packets verbatim until it does, so s.pmt re-emits the whole table
	// at a segment head and not just the packet that opened it.
	pmtAsm  tspes.SectionAssembler
	pmtPkts []byte

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
	keyPCR, keyPTS   int64 // the clock the previous keyframe carried; -1 = none yet
	havePTS          bool
	firstDone        bool
	pendingDisc      bool

	written []segment // oldest→newest, still on disk

	lastKey    time.Time
	lastFeed   time.Time // when the previous packet arrived; zero before the first
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
	// lastKey is deliberately NOT started here: the caller builds the segmenter
	// before it opens the source, and a dial, a TLS handshake and a first
	// manifest and segment fetch are not the stream failing to show a keyframe.
	// The limit starts at the first packet (see chargeGap).
	s := &Segmenter{
		cfg:      cfg,
		startPCR: -1, startPTS: -1, lastPCR: -1, lastPTS: -1, keyPCR: -1, keyPTS: -1,
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
	// transport_error_indicator: this packet arrived with at least one
	// uncorrectable bit error. Its bytes still go into the segment verbatim —
	// that is this segmenter's whole contract — but nothing is DECIDED from
	// them: a table whose PID bits were flipped would send the segmenter after
	// an elementary stream that does not exist.
	tei := pkt[1]&0x80 != 0

	switch {
	case pid == 0x0000 && pusi && !tei:
		s.pat = append(s.pat[:0], pkt...)
		if p := tspes.PMTPID(pkt); p != 0 && p != s.pmtPID {
			// The programme moved to another PMT PID. Drop the table cached
			// under the old one: re-emitting it at every segment head would pair
			// a live PAT with a PMT the PAT no longer points at.
			s.pmtPID, s.pmt = p, s.pmt[:0]
		}
	case pid == 0x0011 && pusi && !tei: // SDT — carried for players that show the service name
		s.sdt = append(s.sdt[:0], pkt...)
	case s.pmtPID != 0 && pid == s.pmtPID && !tei:
		// A PMT too long for one packet — a table with many streams, or with
		// descriptors, which is ordinary on DVB passthrough — continues in the
		// packets that follow, and those carry no PUSI. Reading only the packet
		// that STARTS the section meant a table whose video entry sits after the
		// descriptors was seen as a PMT that names no video: the segmenter never
		// found the video PID, never saw a keyframe, and after MaxNoKeyframe
		// declared the source unsegmentable, which moves the channel to the
		// ffmpeg fallback for good. Fold the section first, decide from the whole
		// of it, and re-emit exactly the packets it came in.
		if pusi {
			s.pmtPkts = append(s.pmtPkts[:0], pkt...)
		} else if len(s.pmtPkts) > 0 {
			s.pmtPkts = append(s.pmtPkts, pkt...)
		}
		if len(s.pmtPkts) > maxPMTPackets*tspes.PacketSize {
			s.pmtPkts = s.pmtPkts[:0] // a PID that never completes a section
		}
		if sec := s.pmtAsm.Feed(pkt); sec != nil {
			s.pmt = append(s.pmt[:0], s.pmtPkts...)
			s.pmtPkts = s.pmtPkts[:0]
			if m, ok := tspes.ParsePMTSection(sec); ok {
				s.adoptPMT(m)
			}
		}
	}

	// The clocks this packet carries are read but not committed yet: a keyframe
	// that splices has to close the segment that is open on the clock that
	// segment was measured against, before the new timeline replaces it.
	pcr, hasPCR := int64(0), false
	if s.havePMT && pid == s.pcrPID {
		pcr, hasPCR = tspes.PCR(pkt)
	}

	key := false
	pts, hasPTS := int64(0), false
	if s.havePMT && pid == s.videoPID && pusi {
		s.countFrame()
		pts, hasPTS = tspes.PTS(pkt)
		key = tspes.RAI(pkt) || tspes.StartsKeyframe(pkt, s.videoType)
	}

	if key && s.spliced(pts, hasPTS, pcr, hasPCR) {
		// Cut HERE. The jump then falls on a segment boundary; the segment just
		// closed is measured on the timeline it actually holds; and the tag goes
		// in front of the segment that starts the new one, which is where a
		// player has to reset. finalize is a no-op when nothing is open, so a
		// splice that lands after an abandoned segment is still marked.
		s.finalize()
		s.pendingDisc = true
	}
	if hasPCR {
		s.lastPCR = pcr
	}
	if hasPTS {
		s.notePTS(pts)
	}
	if key {
		s.keyPCR, s.keyPTS = s.lastPCR, s.lastPTS
	}

	now := s.cfg.Now()
	s.chargeGap(now)
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

// adoptPMT takes what a freshly parsed PMT says. Every PMT is read, not just
// the first: an upstream that restarts mid-connection can come back with its
// video on a different PID, and a segmenter still hunting keyframes on the old
// one finds none for MaxNoKeyframe and returns ErrNoKeyframe — which the remuxer
// reads as "this source needs ffmpeg", permanently, for a source that is
// perfectly segmentable. tsjoin has always re-read the table; only this did not.
func (s *Segmenter) adoptPMT(m tspes.PMT) {
	if s.havePMT && m.VideoPID == s.videoPID && m.PCRPID == s.pcrPID && m.VideoType == s.videoType {
		return
	}
	if s.havePMT {
		s.cfg.Logf("pmt changed: video pid 0x%04x→0x%04x type 0x%02x→0x%02x, pcr pid 0x%04x→0x%04x",
			s.videoPID, m.VideoPID, s.videoType, m.VideoType, s.pcrPID, m.PCRPID)
		// The open segment holds the old elementary stream and none of the new
		// one. Close it here, while its clock is still the one it was measured
		// against, and mark the segment that opens on the new stream: the PIDs
		// changing under a player is exactly what EXT-X-DISCONTINUITY is for.
		s.finalize()
		s.startPCR, s.startPTS = -1, -1
		s.lastPCR, s.lastPTS = -1, -1
		s.keyPCR, s.keyPTS = -1, -1
		s.havePTS, s.haveHi = false, false
		s.pendingDisc = true
	}
	s.videoPID, s.pcrPID, s.videoType, s.havePMT = m.VideoPID, m.PCRPID, m.VideoType, true
}

// chargeGap keeps the no-keyframe limit measured on stream that arrived rather
// than on wall time. Time in which nothing was delivered at all says nothing
// about whether this source has keyframes, so a gap between packets is charged
// at most deliveryStep and the rest of it moves the deadline along with it.
func (s *Segmenter) chargeGap(now time.Time) {
	if s.lastFeed.IsZero() {
		s.lastKey = now // the limit starts at the first packet, not at New
	} else if gap := now.Sub(s.lastFeed); gap > deliveryStep {
		s.lastKey = s.lastKey.Add(gap - deliveryStep)
	}
	s.lastFeed = now
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
// elapsed is how much media the open segment holds. A clock that has gone
// backwards by less than a wrap is a splice the keyframe-level check did not
// catch — the packet that carried it was not a keyframe, or carried neither
// clock — and it must not leave this pinned at 0: the segment would then never
// reach its target, so nothing would be finalised and the playlist would freeze
// at the live edge until the new clock climbed back past the old one, which is
// the source's whole previous uptime. Rebase on the clock the stream is running
// at now and mark the next segment discontinuous.
func (s *Segmenter) elapsed() int64 {
	if s.havePTS && s.startPTS >= 0 {
		d, ok := clockDelta(s.lastPTS, s.startPTS)
		if ok {
			return d
		}
		s.rebase()
		return 0
	}
	if s.startPCR >= 0 && s.lastPCR >= 0 {
		d, ok := clockDelta(s.lastPCR, s.startPCR)
		if ok {
			return d
		}
		s.rebase()
	}
	return 0
}

// rebase restarts the open segment's clocks where the stream now is, and marks
// the next segment EXT-X-DISCONTINUITY.
func (s *Segmenter) rebase() {
	s.startPCR, s.startPTS = s.lastPCR, s.lastPTS
	s.pendingDisc = true
}

// clockDelta is last-start across a 33-bit wrap. ok is false for a backwards
// step no wrap explains — a source timeline jump, whose "duration" would read as
// about 26 hours.
func clockDelta(last, start int64) (int64, bool) {
	d := last - start
	if d < 0 {
		if d >= pcrWrapFloor {
			return 0, false
		}
		d += 1 << 33
	}
	return d, true
}

// spliced reports whether the clock a keyframe carries jumps away from the
// previous keyframe's: an upstream splice, or an encoder that restarted and
// began counting again. Backwards by more than a 33-bit wrap explains, or
// forward by more than any keyframe interval explains — a restart is as likely
// to land an hour ahead as back at zero, and a forward jump left unrecognised
// reaches finalize as an elapsed no EXTINF can honestly carry. It is asked
// BEFORE the new clock is committed, so the caller can still close the open
// segment on the old one.
func (s *Segmenter) spliced(pts int64, hasPTS bool, pcr int64, hasPCR bool) bool {
	var d int64
	var ok bool
	switch {
	case hasPTS && s.keyPTS >= 0:
		d, ok = clockDelta(pts, s.keyPTS)
	case hasPCR && s.keyPCR >= 0:
		d, ok = clockDelta(pcr, s.keyPCR)
	case s.lastPCR >= 0 && s.keyPCR >= 0:
		// The keyframe packet itself carries neither clock. That is ordinary: a
		// stream whose PCR sits on its own PID (the PMT's PCR_PID need not be the
		// video PID) and whose video PES headers carry no PTS gives this packet
		// nothing to compare. Judge it on the clock the stream is RUNNING at
		// instead — lastPCR is still the value from before this keyframe, which
		// is exactly what elapsed() measures against. Without this the splice
		// check simply answered "no" for such a source, so a backwards splice was
		// neither marked nor recovered from.
		d, ok = clockDelta(s.lastPCR, s.keyPCR)
	default:
		return false
	}
	if !ok {
		return true // backwards, and no wrap explains it
	}
	fwd := int64(4*s.cfg.TargetSec) * 90000
	if fwd < spliceForward {
		fwd = spliceForward
	}
	return d > fwd
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
	if i < 0 { // normalise rejects such a pattern; never index at -1 if it ever slips
		return
	}
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
