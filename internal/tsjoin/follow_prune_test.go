// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import "testing"

func partsLen(parts [][]byte) int {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	return n
}

// TestReaderThatTookAllOfAPrunedBlockIsNotBehind: a live viewer parks at the end
// of the newest block. When the ring is shorter than one GOP (a small buffer, a
// long-GOP source) the next keyframe prunes that block, and ReadFrom called the
// viewer "behind" — which serveLive drops — though it had lost nothing. Every
// viewer of such a stream was cut at every keyframe.
func TestReaderThatTookAllOfAPrunedBlockIsNotBehind(t *testing.T) {
	const tick = 90000
	s := New(10*1024*1024, 2_000) // a 2 s ring
	filler := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})

	s.Update(keyPCR(0))
	s.Update(filler)
	_, c := s.JoinStart(nil, 0)
	parts, c, atEnd, behind, pin := s.ReadFrom(c, 1<<20)
	s.Unpin(pin)
	if !atEnd || behind || partsLen(parts) != 2*PacketSize {
		t.Fatalf("first read: atEnd=%v behind=%v bytes=%d", atEnd, behind, partsLen(parts))
	}

	// The next keyframe is 4 s on: the 2 s ring drops the block just read.
	s.Update(keyPCR(4 * tick))
	s.Update(filler)
	parts, _, _, behind, pin = s.ReadFrom(c, 1<<20)
	s.Unpin(pin)
	if behind {
		t.Fatal("a reader that had taken every byte of the pruned block was reported behind")
	}
	if got := partsLen(parts); got != 2*PacketSize {
		t.Fatalf("read %d bytes after the prune, want the new block's %d", got, 2*PacketSize)
	}
}

// TestReaderThatMissedPartOfAPrunedBlockIsBehind: the counterpart. Bytes landed
// in the block after the reader's last read, and the block left the ring before
// it came back for them — those bytes are gone, and the reader is behind.
func TestReaderThatMissedPartOfAPrunedBlockIsBehind(t *testing.T) {
	const tick = 90000
	s := New(10*1024*1024, 2_000)
	filler := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})

	s.Update(keyPCR(0))
	_, c := s.JoinStart(nil, 0)
	_, c, _, _, pin := s.ReadFrom(c, 1<<20)
	s.Unpin(pin)
	s.Update(filler) // lands after the read
	s.Update(keyPCR(4 * tick))

	if _, _, _, behind, _ := s.ReadFrom(c, 1<<20); !behind {
		t.Fatal("a reader that never got the pruned block's last packet was not reported behind")
	}
}
