// Package clustercrypto is the agent's half of the XC_VM cluster protocol
// (MAIN ↔ LB API, cluster API version 1).
//
// Three codebases must produce the same bytes: xcvm_core (the panel's
// cryptographic root, Rust), the panel (PHP) and this agent. The wire formats
// are fixed by two vector files, copied verbatim into testdata/:
//
//   - cluster_vectors.json (owned by xcvm_core, ADR-002): XCVM-SEAL-v1,
//     XCVM-BOX-v1, the token document and the panel signature domain;
//   - cluster_canonical_vectors.json (owned by the panel, ADR 0004): the
//     canonical request/response contexts and their MAC.
//
// Changing a formula here without new vectors from their owner is a protocol
// break. The agent never holds the cluster root: it receives its session
// secret T inside a token sealed to a per-epoch X25519 key, verifies the
// panel's signature on it and derives the four session keys from T.
//
// Standard library only (crypto/ecdh, crypto/ed25519, AES-GCM, HMAC); HKDF is
// the few lines of RFC 5869 below, so the module keeps building on go 1.21.
package clustercrypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
)

// lp is a length-prefixed byte string: u32 big-endian length ‖ bytes.
func lp(b []byte) []byte {
	out := make([]byte, 4, 4+len(b))
	binary.BigEndian.PutUint32(out, uint32(len(b)))
	return append(out, b...)
}

func lps(s string) []byte { return lp([]byte(s)) }

func u32(v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return b[:]
}

func u64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

func sum256(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}

func hmac256(key []byte, parts ...[]byte) []byte {
	m := hmac.New(sha256.New, key)
	for _, p := range parts {
		m.Write(p)
	}
	return m.Sum(nil)
}

// hkdf is HKDF-SHA256 (RFC 5869) for outputs up to 32 bytes, matching PHP's
// hash_hkdf(): an empty salt means a zero-filled one of hash length.
func hkdf(ikm, salt, info []byte, n int) []byte {
	if n > sha256.Size {
		panic("clustercrypto: hkdf output longer than one block")
	}
	if len(salt) == 0 {
		salt = make([]byte, sha256.Size)
	}
	prk := hmac256(salt, ikm)
	return hmac256(prk, info, []byte{1})[:n]
}

// SHA256 is the body hash the MAC and the node request signature cover.
func SHA256(b []byte) []byte { return sum256(b) }

// HKDF is HKDF-SHA256 for outputs up to 32 bytes (the enrolment-code keys).
func HKDF(ikm, salt, info []byte, n int) []byte { return hkdf(ikm, salt, info, n) }

// U32 is the canonical big-endian 32-bit encoding.
func U32(v uint32) []byte { return u32(v) }
