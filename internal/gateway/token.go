// Package gateway answers a node's HLS segment, key and playlist-refresh
// requests without PHP-FPM (Phase 12, the panel's
// docs/superpowers/specs/2026-10-08-lb-segment-gateway-and-native-restreamer-design.md).
// Whatever it does not fully own goes back to PHP, so every step can be undone.
//
// token.go is the stream-link token as the panel's Encryption mints and
// readToken opens it, byte for byte: PHP stays the fallback for every request,
// so a token one side mints the other must read. testdata/gateway_token_vectors.json
// is the panel's (tests/Support), passed by both.
package gateway

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	sealNonce = 12
	sealTag   = 16
	sealLabel = "xc_vm stream token v2|"
)

// Key is one secret or context the node holds, as the bytes PHP uses, with
// the end of its window (unix seconds, inclusive; 0: no end).
type Key struct {
	Value []byte
	Until int64
}

func (k Key) usable(now int64) bool {
	return len(k.Value) > 0 && (k.Until == 0 || now <= k.Until)
}

// Keys is the node's token material, each list current first, then the value
// it replaced (Encryption::readToken's sources).
type Keys struct {
	Viewer  []Key // the node's own viewer keys (ViewerKey::own)
	Shared  []Key // live_streaming_pass, then StreamSecret::previous
	Context []Key // OPENSSL_EXTRA, then OpensslExtra::previous
}

// TokenState is what reading a token came to.
type TokenState int

const (
	// Refused: no key the node holds opens it, as readToken returns false.
	Refused TokenState = iota
	// Opened: the plaintext is what readToken returns.
	Opened
	// Unsure: not written the way PHP writes tokens (PHP's lenient base64
	// may read it otherwise), so PHP decides.
	Unsure
)

// Read opens token as Encryption::readToken does with the node's context:
// its viewer keys, the stream secret, the stream secret under the replaced
// context, the replaced stream secret; then, only where acceptLegacy (the
// secure_stream_tokens setting is off), the legacy CBC format in that order.
func (k Keys) Read(token string, acceptLegacy bool, now int64) ([]byte, TokenState) {
	raw, ok := decodeToken(token)
	if !ok {
		return nil, Unsure
	}
	ctx := pick(k.Context, 0, now)
	for _, v := range k.Viewer {
		if v.usable(now) {
			if plain, ok := open(raw, v.Value, ctx); ok {
				return plain, Opened
			}
		}
	}
	shared := pick(k.Shared, 0, now)
	if shared == nil {
		// No shared secret: under an empty key anyone who knows the context could seal.
		return nil, Refused
	}
	if plain, ok := open(raw, shared, ctx); ok {
		return plain, Opened
	}
	prevCtx := other(pick(k.Context, 1, now), ctx)
	if prevCtx != nil {
		if plain, ok := open(raw, shared, prevCtx); ok {
			return plain, Opened
		}
	}
	oldShared := other(pick(k.Shared, 1, now), shared)
	if oldShared != nil {
		if plain, ok := open(raw, oldShared, ctx); ok {
			return plain, Opened
		}
	}
	if !acceptLegacy {
		return nil, Refused
	}
	if plain, ok := decryptLegacy(raw, shared, ctx); ok {
		return plain, Opened
	}
	if prevCtx != nil {
		if plain, ok := decryptLegacy(raw, shared, prevCtx); ok {
			return plain, Opened
		}
	}
	if oldShared != nil {
		if plain, ok := decryptLegacy(raw, oldShared, ctx); ok {
			return plain, Opened
		}
	}
	return nil, Refused
}

// pick is list[i]'s value while its window is open, else nil.
func pick(list []Key, i int, now int64) []byte {
	if i < len(list) && list[i].usable(now) {
		return list[i].Value
	}
	return nil
}

// other is v unless it is the value in use (PHP skips a "previous" equal to the current one).
func other(v, current []byte) []byte {
	if v == nil || bytes.Equal(v, current) {
		return nil
	}
	return v
}

// decodeToken is Encryption::base64urlDecode for a token written the way PHP
// writes one: base64url without padding. PHP's base64_decode is lenient (it
// skips any other byte), so anything else is not read here.
func decodeToken(token string) ([]byte, bool) {
	if len(token)%4 == 1 {
		return nil, false
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return nil, false
		}
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	return raw, err == nil
}

func sealKey(key, context []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(sealLabel))
	m.Write(context)
	return m.Sum(nil)
}

// aeads caches each (key, context)'s sealing cipher: deriving it (an
// HMAC-SHA256 and the AES key schedule) was most of a token's cost. Keys and
// contexts come from the node's policy alone; a rotation adds a few, and the
// cache starts over past aeadCap.
var (
	aeads    sync.Map
	aeadSize atomic.Int32
)

const aeadCap = 256

func aeadFor(key, context []byte) cipher.AEAD {
	k := strconv.Itoa(len(key)) + ":" + string(key) + string(context)
	if a, ok := aeads.Load(k); ok {
		return a.(cipher.AEAD)
	}
	if aeadSize.Add(1) > aeadCap {
		aeads.Range(func(k, _ any) bool { aeads.Delete(k); return true })
		aeadSize.Store(1)
	}
	a := newGCM(sealKey(key, context))
	aeads.Store(k, a)
	return a
}

// Seal is Encryption::seal with the nonce given (PHP draws a fresh one):
// base64url(nonce ‖ AES-256-GCM ciphertext ‖ tag).
func Seal(plain, key, context, nonce []byte) string {
	gcm := aeadFor(key, context)
	out := append(append([]byte{}, nonce...), gcm.Seal(nil, nonce, plain, nil)...)
	return base64.RawURLEncoding.EncodeToString(out)
}

// open is Encryption::open on the decoded token.
func open(raw, key, context []byte) ([]byte, bool) {
	if len(raw) < sealNonce+sealTag {
		return nil, false
	}
	plain, err := aeadFor(key, context).Open(nil, raw[:sealNonce], raw[sealNonce:], nil)
	if err != nil {
		return nil, false
	}
	if plain == nil {
		plain = []byte{}
	}
	return plain, true
}

func newGCM(key []byte) cipher.AEAD {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err) // a 32-byte HMAC-SHA256: never
	}
	gcm, err := cipher.NewGCMWithTagSize(block, sealTag)
	if err != nil {
		panic(err)
	}
	return gcm
}

// legacyKeyIV is Encryption::encrypt's AES-256-CBC key, md5(sha1(context) . key)
// as 32 hex characters, and its IV, the first 16 of md5(sha1(key)). PHP's
// derivation, not a choice made here: a token must open on both sides, and
// no hash in it signs anything (the format is read only where
// secure_stream_tokens is off).
func legacyKeyIV(key, context []byte) (k, iv []byte) {
	h1 := sha1.Sum(context)                                          // nosemgrep: go.lang.security.audit.crypto.use_of_weak_crypto.use-of-sha1
	k5 := md5.Sum(append([]byte(hex.EncodeToString(h1[:])), key...)) // nosemgrep: go.lang.security.audit.crypto.use_of_weak_crypto.use-of-md5
	h2 := sha1.Sum(key)                                              // nosemgrep: go.lang.security.audit.crypto.use_of_weak_crypto.use-of-sha1
	i5 := md5.Sum([]byte(hex.EncodeToString(h2[:])))                 // nosemgrep: go.lang.security.audit.crypto.use_of_weak_crypto.use-of-md5
	return []byte(hex.EncodeToString(k5[:])), []byte(hex.EncodeToString(i5[:]))[:aes.BlockSize]
}

// EncryptLegacy is Encryption::encrypt: the CBC format servers on an older
// version read, minted where secure_stream_tokens is off.
func EncryptLegacy(plain, key, context []byte) string {
	k, iv := legacyKeyIV(key, context)
	block, _ := aes.NewCipher(k)
	pad := aes.BlockSize - len(plain)%aes.BlockSize
	buf := append(append([]byte{}, plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(buf, buf)
	return base64.RawURLEncoding.EncodeToString(buf)
}

// decryptLegacy is Encryption::decrypt on the decoded token: OpenSSL's
// PKCS#7 check, so under a wrong key it returns what PHP returns, garbage
// whenever the padding happens to hold.
func decryptLegacy(raw, key, context []byte) ([]byte, bool) {
	if len(raw) == 0 || len(raw)%aes.BlockSize != 0 {
		return nil, false
	}
	k, iv := legacyKeyIV(key, context)
	block, _ := aes.NewCipher(k)
	buf := make([]byte, len(raw))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(buf, raw)
	pad := int(buf[len(buf)-1])
	if pad < 1 || pad > aes.BlockSize {
		return nil, false
	}
	for _, b := range buf[len(buf)-pad:] {
		if int(b) != pad {
			return nil, false
		}
	}
	return buf[:len(buf)-pad], true
}
