// Package hlsseg turns a live MPEG-TS byte stream into an in-memory HLS
// presentation: a sliding window of segments cut at video keyframes, plus the
// media playlist. Nothing touches disk — segments live in RAM and are served
// straight to nginx. This is ADR 0002 phase P3 (HLS-A), replacing the ffmpeg
// `-f hls` output that wrote .ts/.m3u8 to tmpfs.
package hlsseg

import (
	"fmt"
	"math"
	"strings"
	"sync"
)

const packetSize = 188

// ptsClock is the 90 kHz MPEG-TS presentation clock.
const ptsClock = 90000

// ptsWrap is the 33-bit PTS modulus.
const ptsWrap = int64(1) << 33

// Segment is one finished HLS media segment held in memory.
type Segment struct {
	Seq  int
	Dur  float64 // seconds
	Data []byte
}

// Segmenter consumes packet-aligned MPEG-TS and maintains the HLS window.
// It is safe for concurrent Feed / Playlist / Segment calls.
type Segmenter struct {
	mu        sync.Mutex
	targetDur float64
	window    int
	maxSeg    int // hard cap on a single segment's bytes (safety)

	segs    []Segment
	nextSeq int

	// current-segment build state
	cur      []byte
	startPTS int64
	haveSeg  bool

	lastPAT  []byte
	lastPMT  []byte
	pmtPID   int
	videoPID int
}

// New returns a Segmenter targeting targetDur-second segments and keeping the
// last window segments.
func New(targetDur float64, window int) *Segmenter {
	if targetDur <= 0 {
		targetDur = 6
	}
	if window < 1 {
		window = 3
	}
	return &Segmenter{
		targetDur: targetDur,
		window:    window,
		maxSeg:    16 * 1024 * 1024,
		pmtPID:    -1,
		videoPID:  -1,
	}
}

// Feed folds a packet-aligned chunk into the current segment, cutting a new
// segment whenever a video keyframe lands at least targetDur past the last cut.
func (s *Segmenter) Feed(chunk []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for off := 0; off+packetSize <= len(chunk); off += packetSize {
		pkt := chunk[off : off+packetSize]
		if pkt[0] != 0x47 {
			continue
		}
		pid := (int(pkt[1]&0x1f) << 8) | int(pkt[2])

		switch {
		case pid == 0 && pkt[1]&0x40 != 0:
			s.lastPAT = clone(s.lastPAT, pkt)
			if p := parsePMTPID(pkt); p >= 0 {
				s.pmtPID = p
			}
			continue
		case s.pmtPID >= 0 && pid == s.pmtPID:
			s.lastPMT = clone(s.lastPMT, pkt)
			if v := parseVideoPID(pkt); v >= 0 {
				s.videoPID = v
			}
			continue
		}

		if s.videoPID >= 0 && pid == s.videoPID && isKeyframe(pkt) {
			if pts, ok := parsePTS(pkt); ok {
				s.onKeyframe(pts, pkt)
				continue
			}
		}

		if s.haveSeg {
			s.cur = append(s.cur, pkt...)
			if len(s.cur) >= s.maxSeg {
				s.finalize(s.targetDur) // safety cut without a keyframe
				s.startSeg(0, nil)      // will re-arm on next keyframe
			}
		}
	}
}

func (s *Segmenter) onKeyframe(pts int64, pkt []byte) {
	if !s.haveSeg {
		s.startSeg(pts, pkt)
		return
	}
	dur := ptsDiff(s.startPTS, pts)
	if dur >= s.targetDur {
		s.finalize(dur)
		s.startSeg(pts, pkt)
		return
	}
	s.cur = append(s.cur, pkt...)
}

// startSeg begins a fresh segment with PAT+PMT+the keyframe packet. Passing a
// nil packet arms the segmenter to start on the next keyframe.
func (s *Segmenter) startSeg(pts int64, keyframe []byte) {
	if keyframe == nil {
		s.haveSeg = false
		s.cur = nil
		return
	}
	buf := make([]byte, 0, 64*1024)
	buf = append(buf, s.lastPAT...)
	buf = append(buf, s.lastPMT...)
	buf = append(buf, keyframe...)
	s.cur = buf
	s.startPTS = pts
	s.haveSeg = true
}

func (s *Segmenter) finalize(dur float64) {
	if len(s.cur) == 0 {
		return
	}
	data := make([]byte, len(s.cur))
	copy(data, s.cur)
	s.segs = append(s.segs, Segment{Seq: s.nextSeq, Dur: dur, Data: data})
	s.nextSeq++
	if len(s.segs) > s.window {
		s.segs = s.segs[len(s.segs)-s.window:]
	}
}

// Playlist renders the current media playlist. Segment URIs are "<seq>.ts",
// resolved by the caller relative to the stream's HLS base path.
func (s *Segmenter) Playlist() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.segs) == 0 {
		return ""
	}
	maxDur := 0.0
	for _, seg := range s.segs {
		if seg.Dur > maxDur {
			maxDur = seg.Dur
		}
	}
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(maxDur)))
	fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n", s.segs[0].Seq)
	for _, seg := range s.segs {
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%d.ts\n", seg.Dur, seg.Seq)
	}
	return b.String()
}

// Segment returns the bytes of segment seq, or nil if it has rotated out.
func (s *Segmenter) Segment(seq int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seg := range s.segs {
		if seg.Seq == seq {
			return seg.Data
		}
	}
	return nil
}

func clone(dst, src []byte) []byte { return append(dst[:0], src...) }

// ptsDiff returns (b - a)/90000 seconds, accounting for 33-bit PTS wraparound.
func ptsDiff(a, b int64) float64 {
	d := b - a
	if d < 0 {
		d += ptsWrap
	}
	return float64(d) / ptsClock
}

func isKeyframe(pkt []byte) bool {
	afc := (pkt[3] >> 4) & 0x3
	if afc != 2 && afc != 3 {
		return false
	}
	return pkt[4] > 0 && pkt[5]&0x40 != 0 // adaptation_field_length>0 && random_access_indicator
}

func payloadStart(pkt []byte) int {
	switch (pkt[3] >> 4) & 0x3 {
	case 2:
		return -1
	case 3:
		return 5 + int(pkt[4])
	default:
		return 4
	}
}

func parsePMTPID(pkt []byte) int {
	ps := payloadStart(pkt)
	if ps < 0 || ps >= len(pkt) {
		return -1
	}
	p := ps + 1 + int(pkt[ps]) // skip pointer_field
	prog := p + 8
	for prog+4 <= len(pkt) {
		if (int(pkt[prog])<<8)|int(pkt[prog+1]) != 0 {
			return ((int(pkt[prog+2]) & 0x1f) << 8) | int(pkt[prog+3])
		}
		prog += 4
	}
	return -1
}

func parseVideoPID(pkt []byte) int {
	ps := payloadStart(pkt)
	if ps < 0 || ps >= len(pkt) {
		return -1
	}
	p := ps + 1 + int(pkt[ps])
	if p+12 > len(pkt) {
		return -1
	}
	pil := ((int(pkt[p+10]) & 0x0f) << 8) | int(pkt[p+11])
	es := p + 12 + pil
	for es+5 <= len(pkt) {
		streamType := pkt[es]
		epid := ((int(pkt[es+1]) & 0x1f) << 8) | int(pkt[es+2])
		if isVideoStreamType(streamType) {
			return epid
		}
		es += 5 + (((int(pkt[es+3]) & 0x0f) << 8) | int(pkt[es+4]))
	}
	return -1
}

func isVideoStreamType(t byte) bool {
	switch t {
	case 0x01, 0x02, 0x10, 0x1b, 0x24, 0xea: // MPEG1/2, MPEG4, H264, HEVC, VC-1
		return true
	}
	return false
}

// parsePTS extracts the 33-bit PTS from a PUSI video packet's PES header.
func parsePTS(pkt []byte) (int64, bool) {
	ps := payloadStart(pkt)
	if ps < 0 || ps+14 > len(pkt) {
		return 0, false
	}
	if pkt[ps] != 0 || pkt[ps+1] != 0 || pkt[ps+2] != 1 {
		return 0, false // no PES start code
	}
	if pkt[ps+7]&0x80 == 0 {
		return 0, false // PTS not present
	}
	p := ps + 9
	pts := (int64(pkt[p]>>1&0x07) << 30) |
		(int64(pkt[p+1]) << 22) |
		(int64(pkt[p+2]>>1&0x7f) << 15) |
		(int64(pkt[p+3]) << 7) |
		(int64(pkt[p+4] >> 1 & 0x7f))
	return pts, true
}
