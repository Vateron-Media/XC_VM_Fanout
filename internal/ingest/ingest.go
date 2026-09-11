// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package ingest copies an MPEG-TS byte stream into a publish callback in
// 188-byte-aligned chunks. Bytes that do not yet complete a packet are held
// over until the next read, so the join parser downstream always sees whole
// packets.
package ingest

import "io"

// PacketSize is the MPEG-TS packet length.
const PacketSize = 188

// Copy reads r and calls publish with packet-aligned chunks until r returns an
// error (io.EOF on a clean end), which it returns. The slice passed to publish
// is only valid for the duration of the call; publish must copy what it keeps.
//
// The read lands DIRECTLY in the carry-over buffer's free tail rather than in a
// scratch slice that is then appended: this is the daemon's innermost loop —
// every byte of every stream passes through it — and the append was a second
// full copy of the whole byte stream for nothing. buf is sized chunkSize+
// PacketSize and never holds more than PacketSize-1 carried-over bytes, so the
// tail always has room for a full chunkSize read and the slice never grows.
func Copy(r io.Reader, chunkSize int, publish func([]byte)) error {
	if chunkSize < PacketSize {
		chunkSize = PacketSize
	}
	chunkSize -= chunkSize % PacketSize

	buf := make([]byte, 0, chunkSize+PacketSize)
	for {
		n, err := r.Read(buf[len(buf):cap(buf)])
		if n > 0 {
			buf = buf[:len(buf)+n]
			whole := len(buf) - (len(buf) % PacketSize)
			if whole > 0 {
				publish(buf[:whole])
				rem := len(buf) - whole
				copy(buf, buf[whole:])
				buf = buf[:rem]
			}
		}
		if err != nil {
			return err
		}
	}
}
