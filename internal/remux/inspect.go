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
}

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
	case i.pmtPID != 0 && pid == i.pmtPID && pusi:
		if es, ok := tspes.ParsePMTStreams(pkt); ok {
			i.havePMT = true
			i.report(es)
		}
	}

	if !i.havePMT && !i.warnedNoT && time.Since(i.started) > noTablesAfter {
		i.warnedNoT = true
		i.warnf("no PAT/PMT after %s: the source does not look like an MPEG-TS programme", noTablesAfter)
	}
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
