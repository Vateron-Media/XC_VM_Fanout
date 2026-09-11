// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package hlscrypt AES-128-CBC encrypts HLS segments to match the panel's
// encrypt_hls scheme, so the daemon can serve encrypted HLS (ADR 0003, Phase B
// encrypted). The ciphertext is byte-identical to PHP's
// openssl_encrypt(data, "aes-128-cbc", key, OPENSSL_RAW_DATA, iv): AES-128-CBC
// with PKCS#7 padding, one fixed key+iv per stream (the panel's <id>_.key /
// <id>_.iv), which the #EXT-X-KEY line in the playlist declares.
package hlscrypt

import (
	"crypto/aes"
	"crypto/cipher"
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
