package clusteragent

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// AEAD-framed relays (the panel's D11, its RelaySeal): what a parent sends a
// child that relays one of its streams is sealed, so a passive observer reads
// nothing and an active one changes, drops or reorders nothing undetected.
//
// The child picks a fresh 32-byte session key per connect and SEALs it to the
// parent's box key (purpose "relay", context "relay|<parent>|<stream>"): the
// node's, from the signed node list, or MAIN's panel box key. It goes in the
// target as rk=<base64url>, which the relay proof signs. The parent answers
// with X-XCVM-Relay-Seal: v1 and frames its bytes:
//
//	u32 len ‖ AES-256-GCM(key, nonce = 0⁴ ‖ u64 counter from 0, aad "xcvm relay v1")
//
// each frame's plaintext at most 64 KiB. A parent the servers section marks as
// sealing (relay_seal) is taken sealed only: an unsealed answer, or a frame
// that does not open, ends the read.

const (
	relaySealPurpose = "relay"
	relaySealParam   = "rk"
	relaySealHeader  = "X-XCVM-Relay-Seal"
	relaySealVersion = "v1"
	relayFrameMax    = 64 << 10
	relayTag         = 16
)

var relayAAD = []byte("xcvm relay v1")

func relaySealContext(parent, stream int64) string {
	return fmt.Sprintf("relay|%d|%d", parent, stream)
}

// relayKey is the session key for stream from parent, and the sealed form the
// target carries; nil when the parent does not seal.
func (p *RelayProxy) relayKey(parent, stream int64) (key []byte, sealed string, err error) {
	rt, err := p.serverRoutes()
	if err != nil {
		return nil, "", err
	}
	if !rt.byID[parent].seal {
		return nil, "", nil
	}
	var pub []byte
	if parent == rt.mainSid {
		pub = p.a.Client.State.PanelBoxPub
	} else if n, ok := rt.nodes[parent]; ok && n.state == "active" {
		pub = n.box
	}
	if len(pub) != 32 {
		return nil, "", fmt.Errorf("no box key for server %d", parent)
	}
	key = make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, "", err
	}
	b, err := cc.Seal(pub, relaySealPurpose, relaySealContext(parent, stream), key)
	if err != nil {
		return nil, "", err
	}
	return key, base64.RawURLEncoding.EncodeToString(b), nil
}

// copyRelayFrames writes the plaintext of the frames read from r to w,
// flushing each: nil at a clean end between frames, else why it stopped.
func copyRelayFrames(w io.Writer, r io.Reader, key []byte) error {
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	fl, _ := w.(http.Flusher)
	var hdr [4]byte
	nonce := make([]byte, gcm.NonceSize())
	buf := make([]byte, relayFrameMax+relayTag)
	for counter := uint64(0); ; counter++ {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n < relayTag || n > relayFrameMax+relayTag {
			return errors.New("a frame of the wrong length")
		}
		if _, err := io.ReadFull(r, buf[:n]); err != nil {
			return err
		}
		binary.BigEndian.PutUint64(nonce[4:], counter)
		pt, err := gcm.Open(buf[:0], nonce, buf[:n], relayAAD)
		if err != nil {
			return errors.New("a frame does not open")
		}
		if _, err := w.Write(pt); err != nil {
			return err
		}
		if fl != nil {
			fl.Flush()
		}
	}
}
