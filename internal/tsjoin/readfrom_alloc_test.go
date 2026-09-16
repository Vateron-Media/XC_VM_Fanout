// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// edgeState is a ring holding eight 1 s GOPs, with a cursor at the head of the
// open block — the shape of a viewer parked at the live edge, which reads a
// single run of the newest block once per published chunk.
func edgeState(t *testing.T) (*State, Cursor) {
	t.Helper()
	s := New(1<<24, 40_000)
	s.Configure(40_000, 6_000, 6)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	for g := int64(0); g < 8; g++ {
		s.Update(tsfixture.KeyframePCR(0x101, g*90000, g*90000))
		for i := 0; i < 20; i++ {
			s.Update(tsfixture.Fill(0x101))
		}
	}
	open := s.gops[len(s.gops)-1]
	return s, Cursor{GOP: open.id}
}

// TestReadFromIntoAllocatesNothingPerRead: under ADR 0004 a viewer at the live
// edge calls Follow — and so ReadFrom — about once per published chunk, roughly
// 80 times a second on an 8 Mbit/s stream. Building the parts slice by appending
// to nil made that one allocation per viewer per chunk, which on a stream with a
// thousand viewers is more garbage than the single broadcast buffer per chunk
// that ADR 0004 removed. A follower that owns its slice across its session pays
// none of it.
func TestReadFromIntoAllocatesNothingPerRead(t *testing.T) {
	s, c := edgeState(t)

	var got int
	fresh := testing.AllocsPerRun(200, func() {
		parts, _, _, _, pin := s.ReadFrom(c, 1<<20)
		got = len(parts)
		s.Unpin(pin)
	})
	if got != 1 {
		t.Fatalf("fixture: ReadFrom returned %d parts, want the single run at the live edge", got)
	}
	if fresh < 1 {
		t.Fatalf("fixture: ReadFrom allocated %v per read, so there is nothing to reuse", fresh)
	}

	dst := make([][]byte, 0, 8)
	reused := testing.AllocsPerRun(200, func() {
		parts, _, _, _, pin := s.ReadFromInto(dst, c, 1<<20)
		got = len(parts)
		s.Unpin(pin)
	})
	if got != 1 {
		t.Fatalf("ReadFromInto returned %d parts, want the same single run ReadFrom gives", got)
	}
	if reused != 0 {
		t.Errorf("ReadFromInto allocated %v per read into a caller-owned slice (ReadFrom: %v), want 0", reused, fresh)
	}
}

// TestReadFromIntoReadsTheSameBytes: the caller-owned slice must not change what
// a reader gets — same parts, same cursor, same flags — or a follower switching
// to it would skip or repeat bytes.
func TestReadFromIntoReadsTheSameBytes(t *testing.T) {
	s, _ := edgeState(t)
	// Walk the whole ring in short runs, both ways, and compare step by step.
	a, b := Cursor{GOP: s.gops[0].id}, Cursor{GOP: s.gops[0].id}
	dst := make([][]byte, 0, 8)
	for i := 0; i < 40; i++ {
		wantParts, wantNext, wantEnd, wantBehind, wantPin := s.ReadFrom(a, 1000)
		gotParts, gotNext, gotEnd, gotBehind, gotPin := s.ReadFromInto(dst, b, 1000)
		if len(gotParts) != len(wantParts) || gotNext != wantNext || gotEnd != wantEnd || gotBehind != wantBehind {
			t.Fatalf("step %d: ReadFromInto gave %d parts/%v/%v/%v, ReadFrom gave %d parts/%v/%v/%v",
				i, len(gotParts), gotNext, gotEnd, gotBehind, len(wantParts), wantNext, wantEnd, wantBehind)
		}
		for j := range wantParts {
			if string(gotParts[j]) != string(wantParts[j]) {
				t.Fatalf("step %d part %d: ReadFromInto returned different bytes", i, j)
			}
		}
		s.Unpin(wantPin)
		s.Unpin(gotPin)
		a, b = wantNext, gotNext
		if wantEnd {
			break
		}
	}
	if a != b {
		t.Fatalf("cursors diverged: ReadFrom at %+v, ReadFromInto at %+v", a, b)
	}
}
