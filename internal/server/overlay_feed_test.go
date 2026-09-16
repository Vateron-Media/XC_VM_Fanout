// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

// catFFmpeg installs a stand-in for ffmpeg that copies its stdin straight to its
// stdout. overlayTSWindow's write() callback then receives EXACTLY the byte
// stream the real encoder would have been fed, which is what these tests are
// about: what reaches the decoder, not what it makes of it.
func catFFmpeg(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "catffmpeg")
	if err := os.WriteFile(p, []byte("#!/bin/sh\nexec cat\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// shortOverlayWindow shrinks the banner window so a test does not sit out the
// production 5 s; restored when the test ends.
func shortOverlayWindow(t *testing.T, d time.Duration) {
	t.Helper()
	prev := overlayTSDuration
	overlayTSDuration = d
	t.Cleanup(func() { overlayTSDuration = prev })
}

// ringFeeder publishes keyframe-led ring blocks whose every packet carries a
// generation number, so a reader can say exactly which published packet it got
// back — and therefore whether it got one twice, or went backwards.
type ringFeeder struct {
	st   *Stream
	gen  uint16
	secs int64
}

func (f *ringFeeder) stamp(p []byte) []byte {
	p[tsfixture.GenOffset] = byte(f.gen >> 8)
	p[tsfixture.GenOffset+1] = byte(f.gen)
	f.gen++
	return p
}

// block publishes one random-access block — a keyframe carrying a PCR (the ring
// needs a clock to retain any history) plus filler — worth a second of stream
// time, and reports the generations it spans.
func (f *ringFeeder) block() (first, last uint16) {
	first = f.gen
	blk := f.stamp(tsfixture.KeyframePCR(0x101, f.secs*90000, f.secs*90000))
	for i := 0; i < 8; i++ {
		blk = append(blk, f.stamp(tsfixture.Fill(0x101))...)
	}
	f.secs++
	last = f.gen - 1
	f.st.Publish(blk)
	return first, last
}

// videoGens reads the generation stamps back out of a TS byte stream, in order.
// PAT/PMT carry none and are skipped: they are repeated program info, not
// stream content, and a decoder is happy to see them more than once.
func videoGens(t *testing.T, b []byte) []uint16 {
	t.Helper()
	var out []uint16
	for off := 0; off+tsfixture.P <= len(b); off += tsfixture.P {
		p := b[off : off+tsfixture.P]
		if p[0] != 0x47 {
			t.Fatalf("byte %d is not on a TS packet boundary (0x%02x)", off, p[0])
		}
		if pid := int(p[1]&0x1f)<<8 | int(p[2]); pid != 0x101 {
			continue
		}
		out = append(out, tsfixture.ReadGen(p))
	}
	return out
}

// followToEdge drains the ring from cur the way serveLive does, leaving the
// cursor where a caught-up viewer parked at the live edge holds it.
func followToEdge(t *testing.T, st *Stream, cur tsjoin.Cursor) tsjoin.Cursor {
	t.Helper()
	for {
		b, next, atEnd, _, behind, ended := st.Hub.Follow(cur, joinRunBytes)
		b.Release()
		if behind || ended {
			t.Fatalf("follow to edge: behind=%v ended=%v", behind, ended)
		}
		cur = next
		if atEnd {
			return cur
		}
	}
}

// TestOverlayTSWindowFeedsOneContiguousStream is the "send message" banner's own
// no-gap-no-duplication claim, checked on the bytes the encoder is actually fed.
//
// serveLive only looks for a queued signal at the top of its loop — which is
// reached right after a wake (a Publish has appended bytes since the cursor was
// last set) or while the viewer is still working through its join history. So the
// cursor handed to overlayTSWindow is always BEHIND the live edge. Seeding ffmpeg
// with a fresh live snapshot and then following that older cursor spliced two
// different places in the stream together: the viewer got the newest block twice,
// or the live edge followed by mid-GOP bytes from seconds earlier, and decoded
// macroblock garbage under the banner until the next keyframe.
func TestOverlayTSWindowFeedsOneContiguousStream(t *testing.T) {
	shortOverlayWindow(t, 250*time.Millisecond)

	cases := []struct {
		name string
		// cursor picks the viewer's place in the ring at the moment the signal
		// is taken, from the join cursor and the caught-up cursor.
		cursor func(join, edge tsjoin.Cursor) tsjoin.Cursor
	}{
		{
			name:   "parked at the live edge when the signal arrived",
			cursor: func(_, edge tsjoin.Cursor) tsjoin.Cursor { return edge },
		},
		{
			name:   "still working through its join history",
			cursor: func(join, _ tsjoin.Cursor) tsjoin.Cursor { return join },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mgr := NewManager(1<<20, 30000, 2, 6, 0)
			mgr.SetOverlay(catFFmpeg(t), "/font.ttf")
			st := mgr.GetOrCreate("ov")

			f := &ringFeeder{st: st}
			st.Publish(tsfixture.PAT(0x100))
			st.Publish(tsfixture.PMT(0x100, 0x101))
			for i := 0; i < 4; i++ {
				f.block()
			}

			_, join := st.Hub.Join(30000) // a viewer asking for the whole ring
			edge := followToEdge(t, st, join)
			cur := tc.cursor(join, edge)

			// A chunk lands between the viewer's last read and the signal check:
			// that wake is precisely what brings serveLive back to the top of its
			// loop, so this is the normal state, not a corner case.
			_, lastGen := f.block()

			var mu sync.Mutex
			var got []byte
			write := func(b []byte) error {
				mu.Lock()
				got = append(got, b...)
				mu.Unlock()
				return nil
			}
			sig := pendingSignal{text: "hello", fontSize: 20, color: "white", x: 10, y: 10}
			endCur, alive := mgr.overlayTSWindow(st, cur, write, sig, "h264")
			if !alive {
				t.Fatal("the viewer connection never broke; overlayTSWindow must report it alive")
			}

			mu.Lock()
			gens := videoGens(t, got)
			mu.Unlock()
			if len(gens) == 0 {
				t.Fatal("the overlay fed the encoder no video at all")
			}
			for i := 1; i < len(gens); i++ {
				if gens[i] != gens[i-1]+1 {
					t.Fatalf("the encoder was fed a spliced stream: packet %d is generation %d, "+
						"straight after generation %d — %s. Full run: %v",
						i, gens[i], gens[i-1],
						map[bool]string{true: "the stream jumped backwards", false: "a gap"}[gens[i] <= gens[i-1]],
						gens)
				}
			}
			if gens[len(gens)-1] != lastGen {
				t.Fatalf("the overlay stopped at generation %d, but the ring reached %d", gens[len(gens)-1], lastGen)
			}

			// And the raw fan-out must resume exactly where the window stopped:
			// nothing already shown is sent a second time, nothing is skipped.
			b, _, atEnd, _, behind, ended := st.Hub.Follow(endCur, joinRunBytes)
			defer b.Release()
			if behind || ended {
				t.Fatalf("resume cursor %+v is unusable: behind=%v ended=%v", endCur, behind, ended)
			}
			if !atEnd || len(b.Parts) != 0 {
				t.Fatalf("resuming raw from %+v would re-send %d run(s) the overlay already showed (atEnd=%v)",
					endCur, len(b.Parts), atEnd)
			}
		})
	}
}
