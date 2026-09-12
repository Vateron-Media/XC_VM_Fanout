// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"strconv"
	"strings"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// feedClock publishes n one-second GOPs on the video PID, stamping each keyframe
// with a PCR that advances from pcr; it returns the last one.
func feedClock(s *State, pcr int64, n int) int64 {
	for g := 0; g < n; g++ {
		pcr = (pcr + 90000) % ptsWrap
		s.Update(tsfixture.KeyframePCR(0x101, pcr, pcr))
		for i := 0; i < 20; i++ {
			s.Update(tsfixture.Fill(0x101))
		}
	}
	return pcr
}

func ringState(window int64) *State {
	s := New(1<<24, window)
	s.Configure(window, 0, 0)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	return s
}

// TestRingKeepsPruningAcrossAClockReset: a producer restart starts its PCR near
// zero again. The prune subtracted raw PCRs, got a negative span, took it for
// "inside the window" and stopped dropping — the ring grew to its byte backstop
// (120 MB at the default 40 s). It must stay the window it was asked for.
func TestRingKeepsPruningAcrossAClockReset(t *testing.T) {
	s := ringState(10000) // 10 s
	feedClock(s, 1000*90000, 30)
	feedClock(s, 0, 60) // restarted: the clock is back near zero
	if n := len(s.gops); n > 12 {
		t.Fatalf("%d one-second GOPs retained 60 s after a clock reset, want a 10 s window (~11)", n)
	}
}

// TestRingKeepsPruningAcrossThePCRWrap: the 33-bit PCR rolls over every 26.5 h;
// every stream crosses it within a day and a bit of uptime.
func TestRingKeepsPruningAcrossThePCRWrap(t *testing.T) {
	s := ringState(10000)
	feedClock(s, ptsWrap-20*90000, 60) // crosses the wrap 20 s in
	if n := len(s.gops); n > 12 {
		t.Fatalf("%d GOPs retained across the PCR wrap, want a 10 s window (~11)", n)
	}
	// And the prebuffer walk-back still measures seconds, not wrapped ticks.
	snap := s.Snapshot(5000)
	if gops := countKeyframes(snap); gops < 5 || gops > 7 {
		t.Fatalf("a 5 s join burst spans %d one-second GOPs, want ~6", gops)
	}
}

// TestRingClockIgnoresASecondPCR: a source carrying an unrelated PCR on another
// PID interleaved two clocks, and the ring's duration became noise. Once the
// programme's declared PCR PID has shown a PCR, it alone drives the clock.
func TestRingClockIgnoresASecondPCR(t *testing.T) {
	s := New(1<<24, 10000)
	s.Configure(10000, 0, 0)
	s.Update(tsfixture.PAT(0x100))
	pmt := tsfixture.PMT(0x100, 0x101)
	pmt[13], pmt[14] = 0xe1, 0x01 // PCR_PID = 0x101, the video
	s.Update(pmt)

	pcr := int64(0)
	stray := int64(50000 * 90000) // another programme's clock, running backwards from here
	for g := 0; g < 60; g++ {
		pcr += 90000
		s.Update(pcrPacket(0x101, pcr)) // the programme's clock, on its PCR PID
		for i := 0; i < 20; i++ {
			s.Update(tsfixture.Fill(0x101))
		}
		stray -= 45000
		s.Update(pcrPacket(0x1ff, stray)) // the stray clock lands last, just before the keyframe
		s.Update(tsfixture.Keyframe(0x101, pcr))
	}
	if n := len(s.gops); n > 12 {
		t.Fatalf("%d one-second GOPs retained with a stray PCR on another PID, want a 10 s window (~11)", n)
	}
}

// pcrPacket is an adaptation-only packet on pid carrying just a PCR.
func pcrPacket(pid int, pcr int64) []byte {
	p := make([]byte, PacketSize)
	p[0] = 0x47
	p[1], p[2] = byte(pid>>8)&0x1f, byte(pid)
	p[3] = 0x20 // adaptation field only
	p[4] = 183
	p[5] = 0x10 // PCR_flag
	p[6], p[7], p[8], p[9] = byte(pcr>>25), byte(pcr>>17), byte(pcr>>9), byte(pcr>>1)
	p[10] = byte(pcr&1) << 7
	return p
}

func countKeyframes(ts []byte) int {
	n := 0
	for off := 0; off+PacketSize <= len(ts); off += PacketSize {
		p := ts[off : off+PacketSize]
		pid := int(p[1]&0x1f)<<8 | int(p[2])
		if pid == 0x101 && (p[3]>>4)&0x2 != 0 && p[4] > 0 && p[5]&0x40 != 0 {
			n++
		}
	}
	return n
}

// TestRingClockAdvancesOnLongKeyframelessBlocks: a slow source with no
// detectable keyframes (radio, say) has its blocks cut at maxGOP, and below
// ~1.4 Mbit/s each one spans more than a minute. Capping the clock's forward
// step froze it there — every block got the same time, nothing was pruned, and
// the ring grew to its byte backstop, hours of 128 kbit/s audio sent whole as
// every join burst.
func TestRingClockAdvancesOnLongKeyframelessBlocks(t *testing.T) {
	const perBlock = 50
	s := New(PacketSize*perBlock, 10000) // maxGOP: a block every 50 packets
	s.Configure(10000, 0, 0)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMTType(0x100, 0x101, 0x0f)) // AAC only: no video, no keyframes
	pcr := int64(0)
	for b := 0; b < 20; b++ {
		for i := 0; i < perBlock; i++ {
			pcr += 90 * 90000 / perBlock // 90 s of audio per block
			s.Update(pcrPacket(0x101, pcr))
		}
	}
	if n := len(s.gops); n > 2 {
		t.Fatalf("%d 90-second blocks retained in a 10 s window, want the newest one or two", n)
	}
}

// TestHLSMarksARestartAsADiscontinuity: a producer restart starts its PTS over.
// The in-RAM HLS read every backwards step as the 33-bit wrap and closed a
// segment of ~26 hours — #EXTINF and #EXT-X-TARGETDURATION of ~95000 s, players
// broken until it left the window. The segment must end at the restart with a
// sane duration, and the next one must say the timeline jumped.
func TestHLSMarksARestartAsADiscontinuity(t *testing.T) {
	s := New(1<<24, 60000)
	s.Configure(60000, 6000, 6) // 6 s segments, 6 listed
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	gop := func(t90 int64) {
		s.Update(tsfixture.KeyframePCR(0x101, t90, t90))
		for i := 0; i < 20; i++ {
			s.Update(tsfixture.Fill(0x101))
		}
	}
	for g := int64(0); g < 20; g++ { // 40 s at a high clock
		gop(1000*90000 + g*2*90000)
	}
	for g := int64(1); g <= 30; g++ { // restarted: the clock starts over
		gop(g * 2 * 90000)
	}

	pl := s.HLSPlaylist()
	var maxDur float64
	for _, l := range strings.Split(pl, "\n") {
		if strings.HasPrefix(l, "#EXTINF:") {
			v, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimPrefix(l, "#EXTINF:"), ","), 64)
			if v > maxDur {
				maxDur = v
			}
		}
	}
	if maxDur > 12 {
		t.Fatalf("a %0.fs segment after a producer restart, want at most a couple of GOPs past the 6 s target:\n%s", maxDur, pl)
	}

	// The restart's segment has left the 6-segment window by now; the
	// discontinuity must be counted, as the spec requires.
	if !strings.Contains(pl, "#EXT-X-DISCONTINUITY-SEQUENCE:1") {
		t.Fatalf("the restart's discontinuity is not counted once it left the window:\n%s", pl)
	}

	// And while it was in the window, it was marked.
	found := false
	for _, sg := range s.segs {
		found = found || sg.disc
	}
	if !found && s.discDropped == 0 {
		t.Fatal("no segment was marked as following the restart")
	}
}

// TestHLSListsTheDiscontinuityWhileInWindow: right after a restart the playlist
// carries #EXT-X-DISCONTINUITY before the first segment on the new timeline.
func TestHLSListsTheDiscontinuityWhileInWindow(t *testing.T) {
	s := New(1<<24, 60000)
	s.Configure(60000, 6000, 6)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	gop := func(t90 int64) {
		s.Update(tsfixture.KeyframePCR(0x101, t90, t90))
		for i := 0; i < 20; i++ {
			s.Update(tsfixture.Fill(0x101))
		}
	}
	for g := int64(0); g < 20; g++ {
		gop(1000*90000 + g*2*90000)
	}
	for g := int64(1); g <= 8; g++ {
		gop(g * 2 * 90000)
	}
	pl := s.HLSPlaylist()
	if !strings.Contains(pl, "#EXT-X-DISCONTINUITY\n") {
		t.Fatalf("no #EXT-X-DISCONTINUITY right after a producer restart:\n%s", pl)
	}
}
