// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"bytes"
	"testing"
)

// TestHLSSegmentRefusesPartiallyAgedSegment pins the fix for a torn-segment bug.
//
// A segment is a VIEW over a run of ring GOPs, [startID..endID]. HLSSegment used
// to accept it whenever ANY GOP in that range was still in the ring, so the
// window where the ring had pruned past startID but not yet past endID served a
// segment that began mid-range: no keyframe at the head, and fewer seconds than
// its own #EXTINF advertised. That is not a rare edge — it is the steady state at
// the tail of a tight ring, which is exactly what a gated (unwatched) stream has.
// A player hits a decode error on it, where a 404 would simply make it skip.
//
// Here the ring is shrunk until the oldest listed segment has lost its first GOP
// but kept its last, and the segment must read back nil.
func TestHLSSegmentRefusesPartiallyAgedSegment(t *testing.T) {
	const tick = 90000 // 1 s in 90 kHz ticks

	// 60 s buffer, keyframes 1 s apart, 3 s segments: each segment spans 3 GOPs,
	// so there is room for the ring edge to land in the MIDDLE of one.
	s := New(1<<20, 60*1000)
	s.Configure(60*1000, 3000, 10)
	s.Update(patPacket(0x100))
	s.Update(pmtPacket())
	for i := 0; i < 30; i++ {
		s.Update(keyPTS(int64(i) * tick))
	}

	// Find a segment that spans more than one GOP and is currently servable.
	var target hlsSeg
	for _, sg := range s.segs {
		if sg.endID > sg.startID {
			target = sg
			break
		}
	}
	if target.endID == 0 && target.startID == 0 {
		t.Fatal("no multi-GOP segment was cut; the fixture cannot exercise the bug")
	}
	if s.HLSSegment(target.seq) == nil {
		t.Fatalf("segment %d should assemble while the whole ring is present", target.seq)
	}

	// Now drop GOPs from the front until the target's FIRST is gone but its LAST
	// is not: the exact straddle the old code served as a valid segment.
	for len(s.gops) > 0 && s.gops[0].id <= target.startID {
		s.gops = s.gops[1:]
	}
	if len(s.gops) == 0 || s.gops[0].id > target.endID {
		t.Skip("ring layout left no straddling state to test")
	}

	if got := s.HLSSegment(target.seq); got != nil {
		t.Fatalf("segment %d assembled from %d bytes after its first GOP aged out; "+
			"a segment that does not start at its keyframe must be nil, not a torn read",
			target.seq, len(got))
	}
}

// TestHLSSegmentStillServesWhollyPresentSegment is the other half: the stricter
// check must not start refusing segments the ring can still assemble in full.
func TestHLSSegmentStillServesWhollyPresentSegment(t *testing.T) {
	s := feedHLS(t, 30, 2000, 3, 8, 2)
	pl := s.HLSPlaylist()
	if pl == "" {
		t.Fatal("expected a playlist")
	}
	seq := lastSeq(t, pl)
	seg := s.HLSSegment(seq)
	if seg == nil {
		t.Fatalf("segment %d is wholly in the ring and must still assemble", seq)
	}
	if !bytes.Equal(seg[:PacketSize], patPacket(0x100)) {
		t.Fatal("segment must still start with the latest PAT")
	}
	if len(seg)%PacketSize != 0 {
		t.Fatalf("segment not packet-aligned: %d bytes", len(seg))
	}
}

// TestResetReleasesTheRing: once a stream's producer is stopped for good, the
// bytes still in the ring are a frozen picture of whenever the source was last
// alive. Nothing prunes them again (prune only runs from Update), so they stayed
// resident for as long as the channel remained registered — at the defaults, ~20
// seconds of video per idle channel, forever. Reset is what the reaper's
// idle-stop calls to give that back.
func TestResetReleasesTheRing(t *testing.T) {
	s := feedHLS(t, 30, 2000, 6, 8, 2)
	if len(s.gops) == 0 || len(s.segs) == 0 {
		t.Fatal("fixture produced no ring/segments to release")
	}
	if s.HLSPlaylist() == "" {
		t.Fatal("expected a playlist before the reset")
	}

	s.Reset()

	if len(s.gops) != 0 {
		t.Errorf("Reset left %d GOPs in the ring", len(s.gops))
	}
	if len(s.segs) != 0 {
		t.Errorf("Reset left %d HLS segments listed", len(s.segs))
	}
	if len(s.freeBufs) != 0 {
		t.Errorf("Reset kept %d recycled GOP buffers; the point is to hand the memory back", len(s.freeBufs))
	}
	if pl := s.HLSPlaylist(); pl != "" {
		t.Errorf("Reset must invalidate the cached playlist, got:\n%s", pl)
	}

	// A restarted puller must be able to refill it, and GOP ids must not restart
	// (a stale playlist elsewhere would otherwise collide with fresh segments).
	firstNewID := s.nextGOPID
	s.Update(keyPTS(100 * 90000))
	if len(s.gops) != 1 {
		t.Fatalf("after Reset the ring took %d GOPs from one keyframe, want 1", len(s.gops))
	}
	if s.gops[0].id != firstNewID {
		t.Errorf("GOP ids restarted after Reset (%d, want %d): ids must stay monotonic", s.gops[0].id, firstNewID)
	}
}
