// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package hlscrypt AES-128-CBC encrypts HLS segments to match the panel's
// encrypt_hls scheme, so the daemon can serve encrypted HLS (ADR 0003, Phase B
// encrypted). The ciphertext is byte-identical to PHP's
// openssl_encrypt(data, "aes-128-cbc", key, OPENSSL_RAW_DATA, iv): AES-128-CBC
// with PKCS#7 padding, one fixed key+iv per stream (the panel's <id>_.key /
// <id>_.iv), which the #EXT-X-KEY line in the playlist declares.
//
// It decrypts too, for the other end of the same scheme: an upstream serving
// AES-128 HLS, which internal/nativesrc reads by fetching the key the playlist
// names and turning the segments back into the MPEG-TS the ring wants.
package hlscrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
	"errors"
	"fmt"
)

// EncryptCBC returns the AES-128-CBC + PKCS#7 ciphertext of data, or nil if the
// key is not a valid AES key or iv is not one block.
func EncryptCBC(data, key, iv []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil || len(iv) != aes.BlockSize {
		return nil
	}

	// One buffer, encrypted in place. CryptBlocks permits dst and src to alias
	// exactly, so the separate plaintext and ciphertext copies this used to make
	// were a second full segment allocation for nothing: at a few MB per segment,
	// per stream, every hls_target_sec, that was the dominant garbage on the
	// encrypted-HLS path.
	pad := aes.BlockSize - len(data)%aes.BlockSize
	buf := make([]byte, len(data)+pad)
	copy(buf, data)
	for i := len(data); i < len(buf); i++ {
		buf[i] = byte(pad)
	}

	cipher.NewCBCEncrypter(block, iv).CryptBlocks(buf, buf)
	return buf
}

// ErrCiphertext reports a body that cannot be AES-128-CBC ciphertext under the
// key and IV given: a wrong length, or padding that does not decode. An
// upstream that changed its key without saying so looks exactly like this, as
// does a segment fetched through something that rewrote it, so the caller
// treats it as a failed segment rather than as a format refusal.
var ErrCiphertext = errors.New("hlscrypt: not valid AES-128-CBC ciphertext")

// DecryptCBC returns the plaintext of an AES-128-CBC + PKCS#7 segment. It is
// the inverse of EncryptCBC, and decrypts in place for the same reason that one
// encrypts in place: a segment is megabytes, and a second copy of every one of
// them is the dominant garbage on this path.
//
// The padding is verified in constant time. That is not because an upstream is
// an adversary worth a padding oracle — it is not, the plaintext goes straight
// to viewers — but because the check is on the hot path either way and the
// constant-time form costs nothing.
func DecryptCBC(data, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: key: %v", ErrCiphertext, err)
	}
	if len(iv) != aes.BlockSize {
		return nil, fmt.Errorf("%w: iv is %d bytes, want %d", ErrCiphertext, len(iv), aes.BlockSize)
	}
	if len(data) == 0 || len(data)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("%w: %d bytes is not a whole number of %d-byte blocks",
			ErrCiphertext, len(data), aes.BlockSize)
	}

	cipher.NewCBCDecrypter(block, iv).CryptBlocks(data, data)

	// PKCS#7: the last byte is the pad length, and every padding byte equals it.
	pad := int(data[len(data)-1])
	if pad == 0 || pad > aes.BlockSize || pad > len(data) {
		return nil, fmt.Errorf("%w: pad length %d", ErrCiphertext, pad)
	}
	want := make([]byte, pad)
	for i := range want {
		want[i] = byte(pad)
	}
	if subtle.ConstantTimeCompare(data[len(data)-pad:], want) != 1 {
		return nil, fmt.Errorf("%w: padding does not decode", ErrCiphertext)
	}
	return data[:len(data)-pad], nil
}
