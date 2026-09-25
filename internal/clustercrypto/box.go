package clustercrypto

import (
	"crypto/rand"
	"errors"
	"io"
)

// XCVM-BOX-v1: a session body, both directions.
//
//	box = "xb1" ‖ nonce[12] ‖ AES-256-GCM(K_enc_up | K_enc_down, pt, aad) ‖ tag[16]
//	aad = "xb1" ‖ lp(context)        context = the canonical request/response context
//
// No compression before encryption (CRIME).
const (
	boxMagic    = "xb1"
	BoxOverhead = 3 + 12 + 16
)

func boxAAD(context []byte) []byte {
	return append([]byte(boxMagic), lp(context)...)
}

// Box encrypts a body under a session key, bound to its context.
func Box(key, context, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return BoxWith(key, nonce, context, plaintext)
}

// BoxWith is the deterministic form, for vectors.
func BoxWith(key, nonce, context, plaintext []byte) ([]byte, error) {
	if len(key) != 32 || len(nonce) != 12 {
		return nil, errors.New("clustercrypto: BOX key or nonce length")
	}
	a, err := gcm(key)
	if err != nil {
		return nil, err
	}
	out := append([]byte(boxMagic), nonce...)
	return a.Seal(out, nonce, plaintext, boxAAD(context)), nil
}

// Unbox opens a box; a body that is not one is refused.
func Unbox(key, context, boxed []byte) ([]byte, error) {
	if len(key) != 32 || len(boxed) < BoxOverhead || string(boxed[:3]) != boxMagic {
		return nil, ErrOpen
	}
	a, err := gcm(key)
	if err != nil {
		return nil, ErrOpen
	}
	pt, err := a.Open(nil, boxed[3:15], boxed[15:], boxAAD(context))
	if err != nil {
		return nil, ErrOpen
	}
	return pt, nil
}
