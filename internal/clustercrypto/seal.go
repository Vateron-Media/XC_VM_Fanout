package clustercrypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"io"
)

// XCVM-SEAL-v1: an anonymous-sender box to an X25519 public key.
//
//	sealed = "xs1" ‖ eph_pub[32] ‖ nonce[12] ‖ AES-256-GCM(key, pt, aad) ‖ tag[16]
//	shared = X25519(eph_sk, rcpt_pub)       an all-zero result is refused
//	key    = HKDF-SHA256(ikm = shared, salt = eph_pub ‖ rcpt_pub, info = "xcvm/seal/v1/" ‖ purpose)
//	aad    = "xs1" ‖ lp(purpose) ‖ lp(context)
const (
	sealMagic    = "xs1"
	SealOverhead = 3 + 32 + 12 + 16
)

// ErrOpen is every "does not open" outcome: wrong key, purpose, context,
// tampering or a malformed blob. Callers learn nothing more.
var ErrOpen = errors.New("clustercrypto: does not open")

// X25519Public returns the public key of a 32-byte X25519 secret.
func X25519Public(sk []byte) ([]byte, error) {
	k, err := ecdh.X25519().NewPrivateKey(sk)
	if err != nil {
		return nil, err
	}
	return k.PublicKey().Bytes(), nil
}

// NewX25519 generates a fresh X25519 secret key (the per-epoch key a token is
// sealed to).
func NewX25519() (sk, pub []byte, err error) {
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	return k.Bytes(), k.PublicKey().Bytes(), nil
}

// x25519 refuses the all-zero shared secret (crypto/ecdh does).
func x25519(sk, pub []byte) ([]byte, error) {
	k, err := ecdh.X25519().NewPrivateKey(sk)
	if err != nil {
		return nil, err
	}
	p, err := ecdh.X25519().NewPublicKey(pub)
	if err != nil {
		return nil, err
	}
	return k.ECDH(p)
}

func sealAAD(purpose, context string) []byte {
	aad := append([]byte(sealMagic), lps(purpose)...)
	return append(aad, lps(context)...)
}

func sealKey(shared, ephPub, rcptPub []byte, purpose string) []byte {
	salt := append(append([]byte{}, ephPub...), rcptPub...)
	return hkdf(shared, salt, []byte("xcvm/seal/v1/"+purpose), 32)
}

func gcm(key []byte) (cipher.AEAD, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// Seal seals plaintext to rcptPub for a purpose and context.
func Seal(rcptPub []byte, purpose, context string, plaintext []byte) ([]byte, error) {
	ephSk := make([]byte, 32)
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, ephSk); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return SealWith(ephSk, nonce, rcptPub, purpose, context, plaintext)
}

// SealWith is the deterministic form, for vectors.
func SealWith(ephSk, nonce, rcptPub []byte, purpose, context string, plaintext []byte) ([]byte, error) {
	if len(ephSk) != 32 || len(nonce) != 12 || len(rcptPub) != 32 {
		return nil, errors.New("clustercrypto: SEAL key or nonce length")
	}
	ephPub, err := X25519Public(ephSk)
	if err != nil {
		return nil, err
	}
	shared, err := x25519(ephSk, rcptPub)
	if err != nil {
		return nil, errors.New("clustercrypto: SEAL recipient key is not usable")
	}
	a, err := gcm(sealKey(shared, ephPub, rcptPub, purpose))
	if err != nil {
		return nil, err
	}
	out := append([]byte(sealMagic), ephPub...)
	out = append(out, nonce...)
	return a.Seal(out, nonce, plaintext, sealAAD(purpose, context)), nil
}

// Open opens a sealed blob with the recipient's secret key.
func Open(rcptSk []byte, purpose, context string, sealed []byte) ([]byte, error) {
	if len(rcptSk) != 32 || len(sealed) < SealOverhead || string(sealed[:3]) != sealMagic {
		return nil, ErrOpen
	}
	ephPub := sealed[3:35]
	shared, err := x25519(rcptSk, ephPub)
	if err != nil {
		return nil, ErrOpen
	}
	rcptPub, err := X25519Public(rcptSk)
	if err != nil {
		return nil, ErrOpen
	}
	a, err := gcm(sealKey(shared, ephPub, rcptPub, purpose))
	if err != nil {
		return nil, ErrOpen
	}
	pt, err := a.Open(nil, sealed[35:47], sealed[47:], sealAAD(purpose, context))
	if err != nil {
		return nil, ErrOpen
	}
	return pt, nil
}
