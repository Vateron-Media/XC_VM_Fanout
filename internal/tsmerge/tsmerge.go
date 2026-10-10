// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package tsmerge joins the transport streams of one programme whose audio
// arrives apart from its video into a single MPEG-TS.
//
// That is an HLS variant whose audio is a separate rendition: the variant's
// segments carry video only, and each audio rendition is a playlist of its
// own. Each of them is already a transport stream by the time it gets here
// (internal/nativesrc pulls it), on the timeline the others are on — that is
// what makes them one programme — so nothing is decoded and no timestamp is
// rewritten. The video stream is passed through as it is and sets the pace;
// every audio frame is put in front of the first video frame that is not
// earlier than it, which is the order a muxer would have written them in.
//
// What does change:
//   - the audio streams' own PAT, PMT and null packets are dropped;
//   - an audio PID the video stream already uses is moved to a free one;
//   - the video stream's PMT is replaced, where it stood, by one that also
//     declares the audio streams (its version moves whenever its content
//     does, so a player reads it again).
//
// Video is held for audio that has not arrived yet, since only a later audio
// frame says that none is due; a rendition that stays silent for maxWait is
// no longer waited for until it speaks again. One that fails takes the
// programme down with it, so the source is opened afresh. One that finishes
// (its playlist ended) lets the video finish too, but not run on: video that
// goes on without sound past it ends the programme as a failure does.
package tsmerge

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsmux"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

const (
	pktSize = tspes.PacketSize

	// MaxAudio is how many audio streams one programme takes.
	MaxAudio = 8

	// queueUnits is how many frames of one input wait for the merge. A full
	// queue stops that input's reader, and through its pipe the pull behind it.
	queueUnits = 512

	// maxUnitBytes ends a run of packets no video frame has opened (a stream
	// whose PMT names no video): it is passed through as it came.
	maxUnitBytes = 4 << 20

	// farApart is where two timestamps stop being on one timeline (90 kHz).
	farApart = 30 * 90000

	// startTrim is how much audio from before the first video frame is kept.
	startTrim = 90000 / 2

	// soundLess is how far video may run past an audio stream that finished.
	soundLess = 10 * 90000
)

// maxWait is how long video is held for an audio stream that has gone quiet.
// A live HLS rendition arrives a segment at a time, each on its own playlist's
// schedule, so a few seconds are ordinary. A variable for the tests.
var maxWait = 12 * time.Second

// ErrAudioEnded is the programme ending because one of its audio streams did.
var ErrAudioEnded = errors.New("tsmerge: an audio stream ended")

// unit is one frame of an input: the packets from one PES start to the next.
type unit struct {
	pkts  []byte
	ts    int64 // DTS (video) or PTS (audio), 90 kHz
	hasTS bool
	pmtAt []int // offsets in pkts where the video stream's PMT stood
}

type input struct {
	r     io.ReadCloser
	units chan unit
	done  <-chan struct{}

	mu     sync.Mutex
	err    error  // why the stream ended; set before units is closed
	pmt    []byte // its last complete PMT section
	pmtPID uint16
	gen    int // counts the PMT's changes
}

func (in *input) setPMT(pid uint16, sec []byte) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.pmtPID == pid && string(in.pmt) == string(sec) {
		return
	}
	in.pmtPID, in.pmt = pid, append([]byte(nil), sec...)
	in.gen++
}

func (in *input) table() (pid uint16, sec []byte, gen int) {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.pmtPID, in.pmt, in.gen
}

func (in *input) end(err error) {
	if err == nil {
		err = io.EOF
	}
	in.mu.Lock()
	in.err = err
	in.mu.Unlock()
	close(in.units)
}

func (in *input) ended() error {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.err
}

// send hands a unit to the merge; false once the merge has been closed.
func (in *input) send(u unit) bool {
	select {
	case in.units <- u:
		return true
	case <-in.done:
		return false
	}
}

// next reads one transport packet, skipping to the next sync byte when the
// stream is not on one.
func next(r io.Reader, pkt []byte) error {
	if _, err := io.ReadFull(r, pkt); err != nil {
		return err
	}
	for pkt[0] != 0x47 {
		i := 1
		for i < pktSize && pkt[i] != 0x47 {
			i++
		}
		n := copy(pkt, pkt[i:])
		if _, err := io.ReadFull(r, pkt[n:]); err != nil {
			return err
		}
	}
	return nil
}

// scanVideo cuts the video stream into frames. Every packet is kept in its
// place but the PMT's, whose position is noted instead.
func (in *input) scanVideo() {
	var (
		pkt      [pktSize]byte
		pmtPID   uint16
		videoPID uint16
		asm      tspes.SectionAssembler
		cur      unit
	)
	flush := func() bool {
		if len(cur.pkts) == 0 && len(cur.pmtAt) == 0 {
			return true
		}
		u := cur
		cur = unit{}
		return in.send(u)
	}
	for {
		if err := next(in.r, pkt[:]); err != nil {
			flush()
			in.end(err)
			return
		}
		pid := tspes.PID(pkt[:])
		tei := pkt[1]&0x80 != 0
		switch {
		case pid == 0x1fff:
			continue
		case pid == 0 && tspes.PUSI(pkt[:]) && !tei:
			if p := tspes.PMTPID(pkt[:]); p != 0 && p != pmtPID {
				pmtPID = p
				asm.Reset()
			}
		case pmtPID != 0 && pid == pmtPID:
			if sec := asm.Feed(pkt[:]); sec != nil && !tei {
				in.setPMT(pmtPID, sec)
				if m, ok := tspes.ParsePMTSection(sec); ok {
					videoPID = m.VideoPID
				}
				cur.pmtAt = append(cur.pmtAt, len(cur.pkts))
			}
			continue
		case videoPID != 0 && pid == videoPID && tspes.PUSI(pkt[:]):
			if !flush() {
				return
			}
			cur.ts, cur.hasTS = dts(pkt[:])
		}
		if len(cur.pkts) >= maxUnitBytes && !cur.hasTS {
			if !flush() {
				return
			}
		}
		cur.pkts = append(cur.pkts, pkt[:]...)
	}
}

// scanAudio cuts an audio stream into frames of the elementary streams its
// PMT declares; nothing else of it is kept.
func (in *input) scanAudio() {
	var (
		pkt     [pktSize]byte
		pmtPID  uint16
		asm     tspes.SectionAssembler
		es      map[uint16]bool
		cur     unit
		started bool
	)
	for {
		if err := next(in.r, pkt[:]); err != nil {
			if started && len(cur.pkts) > 0 {
				in.send(cur)
			}
			in.end(err)
			return
		}
		pid := tspes.PID(pkt[:])
		tei := pkt[1]&0x80 != 0
		switch {
		case pid == 0 && tspes.PUSI(pkt[:]) && !tei:
			if p := tspes.PMTPID(pkt[:]); p != 0 && p != pmtPID {
				pmtPID = p
				asm.Reset()
			}
			continue
		case pmtPID != 0 && pid == pmtPID:
			if sec := asm.Feed(pkt[:]); sec != nil && !tei {
				in.setPMT(pmtPID, sec)
				es = map[uint16]bool{}
				if list, ok := tspes.PMTStreamsSection(sec); ok {
					for _, e := range list {
						es[e.PID] = e.Type != 0x00 && e.PID != 0 // see rebuild
					}
				}
			}
			continue
		}
		if !es[pid] {
			continue
		}
		if tspes.PUSI(pkt[:]) {
			if started && len(cur.pkts) > 0 && !in.send(cur) {
				return
			}
			cur = unit{}
			cur.ts, cur.hasTS = tspes.PTS(pkt[:])
			started = true
		}
		if started {
			cur.pkts = append(cur.pkts, pkt[:]...)
		}
	}
}

// dts reads a PES-starting packet's decode time: its DTS, or its PTS when the
// header carries only that.
func dts(pkt []byte) (int64, bool) {
	off := tspes.PayloadOffset(pkt)
	if off < 0 || off+19 > len(pkt) {
		return tspes.PTS(pkt)
	}
	p := pkt[off:]
	if p[0] != 0 || p[1] != 0 || p[2] != 1 || (p[7]>>6)&0x3 != 3 || p[8] < 10 {
		return tspes.PTS(pkt)
	}
	v := int64(p[14]&0x0e)<<29 | int64(p[15])<<22 | int64(p[16]&0xfe)<<14 | int64(p[17])<<7 | int64(p[18])>>1
	return v & 0x1ffffffff, true
}

// delta is a − b on the 33-bit clock, the short way round.
func delta(a, b int64) int64 {
	d := (a - b) & (1<<33 - 1)
	if d >= 1<<32 {
		d -= 1 << 33
	}
	return d
}

// Merger is the joined stream. Read it; Close it to stop its inputs.
type Merger struct {
	video *input
	audio []*input
	lang  []string

	pr   *io.PipeReader
	pw   *io.PipeWriter
	done chan struct{}
	once sync.Once

	// The PMT written in place of the video stream's.
	pmtPID uint16
	pmtCC  byte
	built  []byte
	ver    byte
	gens   []int
	pids   []map[uint16]uint16 // per audio stream: its PID → the PID it is written on
	out    []byte
}

// New joins video with its audio streams, in the order given. languages, when
// not nil, names each audio stream's language (ISO 639-2, three letters; ""
// for none), written into the PMT for a stream whose own table does not say.
// Reading starts at once; the result must be closed.
func New(video io.ReadCloser, audio []io.ReadCloser, languages []string) (*Merger, error) {
	if len(audio) == 0 || len(audio) > MaxAudio {
		return nil, fmt.Errorf("tsmerge: %d audio streams", len(audio))
	}
	m := &Merger{done: make(chan struct{}), gens: make([]int, len(audio)+1), lang: languages}
	m.pr, m.pw = io.Pipe()
	m.video = &input{r: video, units: make(chan unit, queueUnits), done: m.done}
	for _, r := range audio {
		m.audio = append(m.audio, &input{r: r, units: make(chan unit, queueUnits), done: m.done})
		m.pids = append(m.pids, map[uint16]uint16{})
	}
	go m.video.scanVideo()
	for _, a := range m.audio {
		go a.scanAudio()
	}
	go m.run()
	return m, nil
}

func (m *Merger) Read(p []byte) (int, error) { return m.pr.Read(p) }

// Close stops the merge and closes every input.
func (m *Merger) Close() error {
	m.stop()
	return m.pr.Close()
}

// stop ends the readers and closes the inputs, which stops the pulls behind
// them. The read side stays open: what is left to read is why it ended.
func (m *Merger) stop() {
	m.once.Do(func() {
		close(m.done)
		m.video.r.Close()
		for _, a := range m.audio {
			a.r.Close()
		}
	})
}

// IdleBound is the longest healthy silence of the inputs, for a consumer that
// times its source out (nativesrc.IdleBound).
func (m *Merger) IdleBound() time.Duration {
	var d time.Duration
	for _, in := range append([]*input{m.video}, m.audio...) {
		if b, ok := in.r.(interface{ IdleBound() time.Duration }); ok && b.IdleBound() > d {
			d = b.IdleBound()
		}
	}
	return d
}

// wait takes the next frame of audio stream i, for as long as until allows.
// ok is false when none came in time; ended when the stream is over.
func (m *Merger) wait(i int, until time.Time) (u unit, ok, ended bool) {
	select {
	case u, open := <-m.audio[i].units:
		return u, open, !open
	default:
	}
	d := time.Until(until)
	if d <= 0 {
		return unit{}, false, false
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case u, open := <-m.audio[i].units:
		return u, open, !open
	case <-t.C:
		return unit{}, false, false
	case <-m.done:
		return unit{}, false, true
	}
}

func (m *Merger) run() {
	n := len(m.audio)
	heads := make([]*unit, n)
	late := make([]bool, n)     // went quiet for maxWait: not waited for until it speaks
	begun := make([]bool, n)    // has had a frame written
	finished := make([]bool, n) // ended cleanly: its playlist is over
	last := make([]int64, n)    // the time of its last frame written
	fail := func(err error) {
		if err == nil {
			err = io.EOF
		}
		m.pw.CloseWithError(err)
		m.stop()
	}
	// take fills heads[i]; false when the programme has to end.
	take := func(i int, until time.Time) bool {
		if heads[i] != nil || finished[i] {
			return true
		}
		if late[i] {
			until = time.Time{}
		}
		u, ok, ended := m.wait(i, until)
		if ended {
			select {
			case <-m.done:
				fail(io.ErrClosedPipe)
				return false
			default:
			}
			if err := m.audio[i].ended(); err != io.EOF {
				fail(fmt.Errorf("%w: %v", ErrAudioEnded, err))
				return false
			}
			finished[i] = true
			return true
		}
		if ok {
			heads[i], late[i] = &u, false
		} else {
			late[i] = true
		}
		return true
	}

	// Nothing goes out before every audio stream has said what it is (its PMT
	// comes ahead of its first frame), or has had its time to.
	start := time.Now().Add(maxWait)
	for i := range m.audio {
		if !take(i, start) {
			return
		}
	}
	for {
		var v unit
		var open bool
		select {
		case v, open = <-m.video.units:
		case <-m.done:
			fail(io.ErrClosedPipe)
			return
		}
		if !open {
			fail(m.video.ended())
			return
		}
		m.out = m.out[:0]
		if v.hasTS {
			for i := range m.audio {
				for {
					if !take(i, time.Now().Add(maxWait)) {
						return
					}
					h := heads[i]
					if h == nil {
						if finished[i] && begun[i] && delta(v.ts, last[i]) > soundLess {
							fail(fmt.Errorf("%w: its playlist finished and the video goes on", ErrAudioEnded))
							return
						}
						break // nothing of it has come: the frame goes without
					}
					d := delta(h.ts, v.ts)
					if h.hasTS && d > 0 && d < farApart {
						break // later than this frame: after it
					}
					heads[i] = nil
					if !begun[i] && h.hasTS && d < -startTrim && d > -farApart {
						continue // from before the video began
					}
					begun[i] = true
					if h.hasTS {
						last[i] = h.ts
					}
					m.audioOut(i, h)
				}
			}
		}
		m.videoOut(&v)
		if _, err := m.pw.Write(m.out); err != nil {
			m.stop() // the reader has gone
			return
		}
	}
}

// audioOut appends an audio frame, its packets on the PIDs the PMT gives them.
func (m *Merger) audioOut(i int, u *unit) {
	m.rebuild()
	at := len(m.out)
	m.out = append(m.out, u.pkts...)
	for p := m.out[at:]; len(p) >= pktSize; p = p[pktSize:] {
		pid := tspes.PID(p)
		if np, ok := m.pids[i][pid]; ok && np != pid {
			p[1] = p[1]&0xe0 | byte(np>>8)
			p[2] = byte(np)
		}
	}
}

// videoOut appends a video frame, with the joined PMT wherever the stream's
// own stood.
func (m *Merger) videoOut(u *unit) {
	if len(u.pmtAt) == 0 {
		m.out = append(m.out, u.pkts...)
		return
	}
	m.rebuild()
	prev := 0
	for _, at := range u.pmtAt {
		m.out = append(m.out, u.pkts[prev:at]...)
		m.pmtOut()
		prev = at
	}
	m.out = append(m.out, u.pkts[prev:]...)
}

// pmtOut appends the joined PMT as packets of the video stream's PMT PID.
func (m *Merger) pmtOut() {
	if m.built == nil {
		return
	}
	payload := append([]byte{0x00}, m.built...) // pointer_field
	for first := true; len(payload) > 0; first = false {
		var p [pktSize]byte
		for i := range p {
			p[i] = 0xff
		}
		p[0], p[1], p[2], p[3] = 0x47, byte(m.pmtPID>>8)&0x1f, byte(m.pmtPID), 0x10|m.pmtCC
		if first {
			p[1] |= 0x40
		}
		m.pmtCC = (m.pmtCC + 1) & 0x0f
		payload = payload[copy(p[4:], payload):]
		m.out = append(m.out, p[:]...)
	}
}

// rebuild makes the joined PMT again when a stream's own table has changed:
// the video stream's, with the audio streams' entries after its own.
func (m *Merger) rebuild() {
	pid, vsec, vgen := m.video.table()
	changed := m.built == nil || vgen != m.gens[0]
	type table struct {
		sec []byte
		gen int
	}
	tables := make([]table, len(m.audio))
	for i, a := range m.audio {
		_, tables[i].sec, tables[i].gen = a.table()
		changed = changed || tables[i].gen != m.gens[i+1]
	}
	if !changed || len(vsec) < 16 {
		return
	}
	end := 3 + (int(vsec[1]&0x0f)<<8 | int(vsec[2])) - 4
	progInfo := int(vsec[10]&0x0f)<<8 | int(vsec[11])
	if end > len(vsec)-4 || 12+progInfo > end {
		return
	}
	used := map[uint16]bool{0: true, 0x11: true, 0x1fff: true, pid: true, uint16(vsec[8]&0x1f)<<8 | uint16(vsec[9]): true}
	for i := 12 + progInfo; i+5 <= end; i += 5 + (int(vsec[i+3]&0x0f)<<8 | int(vsec[i+4])) {
		used[uint16(vsec[i+1]&0x1f)<<8|uint16(vsec[i+2])] = true
	}
	body := append([]byte(nil), vsec[3:end]...)
	free := uint16(0x100)
	for a, t := range tables {
		sec := t.sec
		if len(sec) < 16 {
			continue // has not said what it is yet
		}
		aend := 3 + (int(sec[1]&0x0f)<<8 | int(sec[2])) - 4
		if aend > len(sec)-4 {
			continue
		}
		pids := map[uint16]uint16{}
		for i := 12 + (int(sec[10]&0x0f)<<8 | int(sec[11])); i+5 <= aend; {
			n := 5 + (int(sec[i+3]&0x0f)<<8 | int(sec[i+4]))
			if i+n > aend {
				break
			}
			from := uint16(sec[i+1]&0x1f)<<8 | uint16(sec[i+2])
			if sec[i] == 0x00 || from == 0 {
				i += n // a reserved type or the PAT's PID: padding, not a stream
				continue
			}
			to := from
			// The PID it was given before, if it still can have it: a table that
			// changes must not move a stream that is playing.
			if was, ok := m.pids[a][from]; ok && !used[was] {
				to = was
			}
			for used[to] {
				for used[free] {
					free++
				}
				to = free
			}
			used[to] = true
			pids[from] = to
			entry := append([]byte(nil), sec[i:i+n]...)
			entry[1] = entry[1]&0xe0 | byte(to>>8)
			entry[2] = byte(to)
			body = append(body, withLanguage(entry, m.language(a))...)
			i += n
		}
		m.pids[a] = pids
		m.gens[a+1] = t.gen
	}
	if len(body)+4 > tspes.MaxSectionBytes-3 {
		return // would not be a section: the last one stands
	}
	// version_number moves with the content, or a demuxer keeps the table it has.
	body[2] = 0xc1 | m.ver<<1
	sec := tsmux.Section(0x02, body)
	if m.built != nil && string(sec) != string(m.built) {
		m.ver = (m.ver + 1) & 0x1f
		body[2] = 0xc1 | m.ver<<1
		sec = tsmux.Section(0x02, body)
	}
	m.built, m.pmtPID, m.gens[0] = sec, pid, vgen
}

func (m *Merger) language(a int) string {
	if a < len(m.lang) && len(m.lang[a]) == 3 {
		return m.lang[a]
	}
	return ""
}

// withLanguage adds an ISO_639_language_descriptor to a PMT entry that has
// none, so a player can name the track.
func withLanguage(entry []byte, lang string) []byte {
	if lang == "" {
		return entry
	}
	for i := 5; i+2 <= len(entry); i += 2 + int(entry[i+1]) {
		if entry[i] == 0x0a {
			return entry
		}
	}
	n := len(entry) - 5 + 6
	entry[3] = entry[3]&0xf0 | byte(n>>8)&0x0f
	entry[4] = byte(n)
	return append(entry, 0x0a, 0x04, lang[0], lang[1], lang[2], 0x00)
}
