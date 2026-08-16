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

	pad := aes.BlockSize - len(data)%aes.BlockSize
	padded := make([]byte, len(data)+pad)
	copy(padded, data)
	for i := len(data); i < len(padded); i++ {
		padded[i] = byte(pad)
	}

	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(out, padded)
	return out
}
