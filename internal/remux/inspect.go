// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package remux

import (
	"fmt"
	"strings"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// noTablesAfter is how long a source may run without a PAT+PMT before the log
// says so. A source that never sends tables plays nowhere, and the operator
// reading <id>.errors should not have to infer that from silence.
const noTablesAfter = 10 * time.Second

// inspector watches the passing packets for the few facts an operator needs in
// the stream's log: what the source actually carries, and the traits that make a
// byte-for-byte copy behave differently from ffmpeg's remux (a multi-programme
// source handed over whole, scrambled packets, a programme with no audio).
//
// It reports once, in ffmpeg's shape — `Input #0, mpegts, from '…'` with a
// Stream line per elementary stream — because the panel writes this next to the
// ffmpeg logs of every other stream and an operator should not have to learn a
// second format to read it.
type inspector struct {
	notef   func(string, ...any) // printed unless the log is quiet
	warnf   func(string, ...any)
	input   string   // source URL, already redacted
	outputs []string // "hls, to '…'", "mpegts, to 'unix:…'"

	started   time.Time
	pmtPID    uint16
	havePMT   bool
	programs  []uint16
	reported  bool
	warnedCA  bool
	warnedNoT bool

	// pmtSec is the PMT section being reassembled. A section longer than the
	// ~183 payload bytes of one packet is ordinary on a DVB-derived source (CA
	// descriptors in program_info, then video, then several audio, teletext and
	// subtitle streams with language descriptors), and it continues in the
	// packets that follow the PUSI one.
	pmtSec []byte
}

// maxSection is the largest a PSI section can be: section_length is 12 bits but
// capped at 1021 by the standard, plus the 3 bytes before it. It bounds what a
// malformed or lost-continuity stream can make the inspector accumulate.
const maxSection = 3 + 1021

func newInspector(input string, outputs []string, notef, warnf func(string, ...any)) *inspector {
	return &inspector{notef: notef, warnf: warnf, input: input, outputs: outputs, started: time.Now()}
}

// feed folds one 188-byte packet in. It does nothing once the source has been
// reported, beyond the scrambling check, so the steady-state cost is one
// comparison per packet.
func (i *inspector) feed(pkt []byte) {
	if len(pkt) != tspes.PacketSize {
		return
	}
	// transport_scrambling_control: the payload is encrypted upstream. Neither
	// this nor ffmpeg's copy can decrypt it — the viewer gets a black picture —
	// so say it plainly and once.
	if pkt[3]&0xc0 != 0 && !i.warnedCA {
		i.warnedCA = true
		i.warnf("source packets are scrambled (transport_scrambling_control set): a copy cannot decrypt them and the picture will be black")
	}
	if i.reported {
		return
	}

	pid := tspes.PID(pkt)
	pusi := tspes.PUSI(pkt)
	switch {
	case pid == 0x0000 && pusi:
		if progs := tspes.PATPrograms(pkt); len(progs) > 0 {
			i.programs = progs
		}
		if p := tspes.PMTPID(pkt); p != 0 {
			i.pmtPID = p
		}
	case i.pmtPID != 0 && pid == i.pmtPID:
		if es, ok := i.foldPMT(pkt, pusi); ok {
			i.havePMT = true
			i.report(es)
		}
	}

	if !i.havePMT && !i.warnedNoT && time.Since(i.started) > noTablesAfter {
		i.warnedNoT = true
		i.warnf("no PAT/PMT after %s: the source does not look like an MPEG-TS programme", noTablesAfter)
	}
}

// foldPMT collects one packet of the PMT section and, once the whole section is
// in hand, returns its elementary streams. Reporting on the first packet alone
// truncated the stream list at the packet boundary — and then warned about the
// audio it had just dropped.
func (i *inspector) foldPMT(pkt []byte, pusi bool) ([]tspes.ES, bool) {
	off := tspes.PayloadOffset(pkt)
	if off < 0 {
		return nil, false // adaptation only: not part of the section
	}
	if pusi {
		off += 1 + int(pkt[off]) // pointer_field
		if off >= len(pkt) {
			i.pmtSec = i.pmtSec[:0]
			return nil, false
		}
		i.pmtSec = append(i.pmtSec[:0], pkt[off:]...)
	} else {
		if len(i.pmtSec) == 0 {
			return nil, false // joined mid-section: wait for the next PUSI
		}
		i.pmtSec = append(i.pmtSec, pkt[off:]...)
	}

	sec := i.pmtSec
	if len(sec) < 3 {
		return nil, false
	}
	if sec[0] != 0x02 {
		i.pmtSec = i.pmtSec[:0]
		return nil, false
	}
	total := 3 + (int(sec[1]&0x0f)<<8 | int(sec[2]))
	if total > maxSection {
		i.pmtSec = i.pmtSec[:0]
		return nil, false
	}
	if len(sec) < total {
		return nil, false // continues in the next packet
	}
	es, ok := parsePMTSection(sec[:total])
	i.pmtSec = i.pmtSec[:0]
	return es, ok
}

// parsePMTSection reads the ES loop of a COMPLETE PMT section. It is
// tspes.ParsePMTStreams' loop without the clamp that function needs because it
// is handed a single packet rather than a reassembled section.
func parsePMTSection(sec []byte) ([]tspes.ES, bool) {
	if len(sec) < 16 {
		return nil, false
	}
	progInfoLen := int(sec[10]&0x0f)<<8 | int(sec[11])
	end := len(sec) - 4 // the ES loop runs to the CRC
	var out []tspes.ES
	for i := 12 + progInfoLen; i+5 <= end; {
		esInfoLen := int(sec[i+3]&0x0f)<<8 | int(sec[i+4])
		out = append(out, tspes.ES{PID: uint16(sec[i+1]&0x1f)<<8 | uint16(sec[i+2]), Type: sec[i]})
		i += 5 + esInfoLen
	}
	return out, len(out) > 0
}

// report writes the input/output summary, then the traits worth a warning.
func (i *inspector) report(es []tspes.ES) {
	i.reported = true

	prog := "Program 1"
	if len(i.programs) > 0 {
		prog = fmt.Sprintf("Program %d", i.programs[0])
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Input #0, mpegts, from '%s':\n  %s", i.input, prog)
	video, audio := 0, 0
	for n, e := range es {
		switch e.Kind() {
		case "Video":
			video++
		case "Audio":
			audio++
		}
		fmt.Fprintf(&b, "\n    Stream #0:%d[0x%03x]: %s: %s", n, e.PID, e.Kind(), tspes.StreamTypeName(e.Type))
	}
	for n, o := range i.outputs {
		fmt.Fprintf(&b, "\n  Output #%d, %s", n, o)
	}
	i.notef("%s", b.String())

	if len(i.programs) > 1 {
		i.warnf("source carries %d programmes; a passthrough copy hands the player all of them, and it may pick another one than the panel's ffmpeg would. Use the ffmpeg backend for this stream if the wrong programme plays", len(i.programs))
	}
	if video == 0 {
		i.warnf("the programme declares no video stream")
	}
	if audio == 0 {
		i.warnf("the programme declares no audio stream: the viewer gets a picture with no sound")
	}
}
