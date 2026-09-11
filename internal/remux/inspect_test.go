// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package remux

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// pmt builds a PMT packet on pmtPID declaring the given (stream_type, PID) pairs.
func pmt(pmtPID int, es ...[2]int) []byte {
	p := make([]byte, tspes.PacketSize)
	p[0] = 0x47
	p[1] = byte(0x40 | (pmtPID>>8)&0x1f) // PUSI
	p[2] = byte(pmtPID & 0xff)
	p[3] = 0x10 // payload only
	p[4] = 0x00 // pointer_field
	sec := p[5:]
	secLen := 9 + 5*len(es) + 4 // program_number..program_info_length, the ES loop, CRC
	sec[0] = 0x02               // table_id
	sec[1], sec[2] = byte(0xb0|secLen>>8), byte(secLen&0xff)
	sec[3], sec[4] = 0x00, 0x01 // program_number 1
	sec[8], sec[9] = 0xe1, 0x00 // PCR_PID 0x100
	sec[10], sec[11] = 0x00, 0x00
	off := 12
	for _, e := range es {
		sec[off] = byte(e[0])
		sec[off+1] = byte(0xe0 | (e[1]>>8)&0x1f)
		sec[off+2] = byte(e[1] & 0xff)
		sec[off+3], sec[off+4] = 0x00, 0x00
		off += 5
	}
	return p
}

func pat(pmtPID int, programs ...int) []byte {
	p := make([]byte, tspes.PacketSize)
	p[0] = 0x47
	p[1], p[2] = 0x40, 0x00
	p[3] = 0x10
	sec := p[5:]
	secLen := 5 + 4*len(programs) + 4
	sec[0] = 0x00
	sec[1], sec[2] = byte(0xb0|secLen>>8), byte(secLen&0xff)
	sec[3], sec[4] = 0x00, 0x01
	off := 8
	for _, prog := range programs {
		sec[off], sec[off+1] = byte(prog>>8), byte(prog&0xff)
		sec[off+2] = byte(0xe0 | (pmtPID>>8)&0x1f)
		sec[off+3] = byte(pmtPID & 0xff)
		off += 4
	}
	return p
}

func collect(lines *[]string) func(string, ...any) {
	return func(format string, a ...any) {
		*lines = append(*lines, fmt.Sprintf(format, a...))
	}
}

// TestInspectorReportsTheSourceLikeFfmpeg: the panel points the remuxer's stderr
// at <id>.errors, beside the ffmpeg logs of every other stream, so the summary
// has to read like one — and it has to arrive without -loglevel info, because
// the panel does not pass it.
func TestInspectorReportsTheSourceLikeFfmpeg(t *testing.T) {
	var notes []string
	insp := newInspector("http://src/live.ts", []string{"hls, to '/streams/42_.m3u8'", "mpegts, to 'unix:/run/42.sock'"}, collect(&notes), collect(&notes))

	insp.feed(pat(0x1000, 1))
	insp.feed(pmt(0x1000, [2]int{0x1b, 0x100}, [2]int{0x0f, 0x101}))

	if len(notes) != 1 {
		t.Fatalf("notes = %q, want just the input summary", notes)
	}
	for _, want := range []string{
		"Input #0, mpegts, from 'http://src/live.ts':",
		"Program 1",
		"Stream #0:0[0x100]: Video: h264",
		"Stream #0:1[0x101]: Audio: aac",
		"Output #0, hls, to '/streams/42_.m3u8'",
		"Output #1, mpegts, to 'unix:/run/42.sock'",
	} {
		if !strings.Contains(notes[0], want) {
			t.Errorf("summary is missing %q:\n%s", want, notes[0])
		}
	}

	// Reported once: the tables repeat several times a second.
	insp.feed(pmt(0x1000, [2]int{0x1b, 0x100}, [2]int{0x0f, 0x101}))
	if len(notes) != 1 {
		t.Fatalf("the summary was written %d times, want once", len(notes))
	}
}

// TestInspectorWarnsAboutWhatBreaksPlayback: the traits that make a byte-for-byte
// copy behave differently from ffmpeg's remux are exactly what an operator needs
// told, because the picture is black either way and only the log says why.
func TestInspectorWarnsAboutWhatBreaksPlayback(t *testing.T) {
	var notes []string
	insp := newInspector("udp://239.0.0.1:1234", nil, collect(&notes), collect(&notes))
	insp.feed(pat(0x1000, 1, 2, 3))
	insp.feed(pmt(0x1000, [2]int{0x1b, 0x100})) // video only

	joined := strings.Join(notes, "\n")
	if !strings.Contains(joined, "3 programmes") {
		t.Errorf("a multi-programme source must be called out:\n%s", joined)
	}
	if !strings.Contains(joined, "no audio stream") {
		t.Errorf("a programme with no audio must be called out:\n%s", joined)
	}

	// Scrambled payload: neither this nor ffmpeg can decrypt it.
	before := len(notes)
	scr := make([]byte, tspes.PacketSize)
	scr[0], scr[1], scr[2], scr[3] = 0x47, 0x01, 0x00, 0xd0 // transport_scrambling_control = 2
	insp.feed(scr)
	insp.feed(scr)
	if len(notes) != before+1 {
		t.Fatalf("scrambling reported %d times, want once", len(notes)-before)
	}
	if !strings.Contains(notes[len(notes)-1], "scrambled") {
		t.Errorf("wrong warning: %s", notes[len(notes)-1])
	}
}
