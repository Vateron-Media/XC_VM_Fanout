// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package ingest copies an MPEG-TS byte stream into a publish callback in
// 188-byte-aligned chunks. Bytes that do not yet complete a packet are held
// over until the next read, so the join parser downstream always sees whole
// packets.
package ingest

import "io"

const (
	// PacketSize is the MPEG-TS packet length.
	PacketSize = 188

	// syncByte opens every MPEG-TS packet.
	syncByte = 0x47
)

// Copy reads r and calls publish with packet-aligned chunks until r returns an
// error (io.EOF on a clean end), which it returns. The slice passed to publish
// is only valid for the duration of the call; publish must copy what it keeps.
//
// Alignment is checked, not assumed. A source's bytes are not guaranteed to
// start on a packet boundary or to stay on one: a body handed over part-way
// through a packet, a stray datagram on the UDP port, an RTP header strip that
// came out a few bytes off, a truncated or padded HLS segment. Carrying only
// len%PacketSize forward let one such input shift every later chunk by the same
// few bytes — for good, because nothing downstream ever looks back. tsjoin then
// skipped every packet, so the stream had no PAT, no PMT and no keyframes: no
// joins and no HLS, while the bytes still counted as data and left the channel
// looking alive.
//
// The read lands DIRECTLY in the carry-over buffer's free tail rather than in a
// scratch slice that is then appended: this is the daemon's innermost loop —
// every byte of every stream passes through it — and the append was a second
// full copy of the whole byte stream for nothing. Packets are published in
// place out of that same buffer, so a stream in sync costs one byte comparison
// per packet and nothing else. buf is sized chunkSize+PacketSize and never
// carries more than PacketSize bytes over, so the tail always has room for a
// full chunkSize read and the slice never grows.
func Copy(r io.Reader, chunkSize int, publish func([]byte)) error {
	if chunkSize < PacketSize {
		chunkSize = PacketSize
	}
	chunkSize -= chunkSize % PacketSize

	buf := make([]byte, 0, chunkSize+PacketSize)
	synced := false
	for {
		n, err := r.Read(buf[len(buf):cap(buf)])
		if n > 0 {
			buf = buf[:len(buf)+n]
			for {
				if !synced {
					off := resync(buf)
					if off < 0 {
						// No boundary to be sure of yet. Keep only the bytes a
						// later read could still confirm one in.
						buf = drop(buf, len(buf)-PacketSize)
						break
					}
					buf, synced = drop(buf, off), true
				}
				whole := aligned(buf)
				if whole > 0 {
					publish(buf[:whole])
					buf = drop(buf, whole)
				}
				if len(buf) == 0 || buf[0] == syncByte {
					break // in sync; at most a partial packet carried over
				}
				synced = false // the source shifted mid-stream
			}
		}
		if err != nil {
			return err
		}
	}
}

// aligned is the length of the run of whole packets at the head of b, each
// opening with the sync byte.
func aligned(b []byte) int {
	i := 0
	for i+PacketSize <= len(b) && b[i] == syncByte {
		i += PacketSize
	}
	return i
}

// resync finds where the packet boundary really is: the first sync byte that
// the following packet's own sync byte confirms. A lone 0x47 in a payload is
// not a boundary — this is P2PTV's rule, the one internal/remux already uses on
// its own reads. -1 means there is nothing here to be sure of yet.
func resync(b []byte) int {
	for i := 0; i+PacketSize < len(b); i++ {
		if b[i] == syncByte && b[i+PacketSize] == syncByte {
			return i
		}
	}
	return -1
}

// drop removes the first n bytes of b in place, keeping its capacity.
func drop(b []byte, n int) []byte {
	if n <= 0 {
		return b
	}
	return b[:copy(b, b[n:])]
}
