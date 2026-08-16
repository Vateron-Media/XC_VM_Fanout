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
func Copy(r io.Reader, chunkSize int, publish func([]byte)) error {
	if chunkSize < PacketSize {
		chunkSize = PacketSize
	}
	chunkSize -= chunkSize % PacketSize

	buf := make([]byte, 0, chunkSize+PacketSize)
	tmp := make([]byte, chunkSize)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
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
