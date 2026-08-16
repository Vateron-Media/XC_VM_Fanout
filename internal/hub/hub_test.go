package hub

import (
	"bytes"
	"testing"
)

func mkPkt(marker byte) []byte {
	p := make([]byte, 188)
	p[0] = 0x47
	p[3] = marker
	return p
}

// TestFanoutIdentical asserts every subscriber receives the exact same live
// byte stream from a single producer.
func TestFanoutIdentical(t *testing.T) {
	h := New(1<<20, 0)

	const nSubs = 3
	subs := make([]*Sub, nSubs)
	for i := range subs {
		subs[i], _ = h.Subscribe(0)
	}

	const nChunks = 100 // < subQueue, so nothing is dropped
	go func() {
		for i := 0; i < nChunks; i++ {
			h.Publish(mkPkt(byte(i)))
		}
	}()

	outs := make([]*bytes.Buffer, nSubs)
	for i, s := range subs {
		outs[i] = &bytes.Buffer{}
		for j := 0; j < nChunks; j++ {
			outs[i].Write(<-s.C())
		}
	}

	if outs[0].Len() != nChunks*188 {
		t.Fatalf("sub 0 got %d bytes, want %d", outs[0].Len(), nChunks*188)
	}
	for i := 1; i < nSubs; i++ {
		if !bytes.Equal(outs[0].Bytes(), outs[i].Bytes()) {
			t.Fatalf("subscriber %d received a different byte stream than subscriber 0", i)
		}
	}
}

// TestSlowSubscriberDropped asserts a subscriber that never reads is dropped
// once its queue overflows, and does not block the producer.
func TestSlowSubscriberDropped(t *testing.T) {
	h := New(1<<20, 0)
	slow, _ := h.Subscribe(0)

	// Publish well past the queue depth without ever reading `slow`.
	for i := 0; i < subQueue+50; i++ {
		h.Publish(mkPkt(byte(i)))
	}

	select {
	case <-slow.Done():
		// dropped as expected
	default:
		t.Fatalf("slow subscriber should have been dropped")
	}
	if h.Count() != 0 {
		t.Fatalf("hub should have 0 subscribers after dropping the slow one, got %d", h.Count())
	}
}
