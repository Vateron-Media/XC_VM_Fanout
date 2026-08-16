package hlscrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"testing"
)

func TestEncryptCBCRoundTrip(t *testing.T) {
	key := []byte("0123456789abcdef")
	iv := []byte("abcdef0123456789")
	data := []byte("hello mpegts segment payload not block aligned!!")

	ct := EncryptCBC(data, key, iv)
	if ct == nil || len(ct) == 0 || len(ct)%aes.BlockSize != 0 {
		t.Fatalf("bad ciphertext: len=%d", len(ct))
	}

	block, _ := aes.NewCipher(key)
	pt := make([]byte, len(ct))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(pt, ct)

	pad := int(pt[len(pt)-1]) // PKCS#7
	if pad < 1 || pad > aes.BlockSize {
		t.Fatalf("bad padding byte: %d", pad)
	}
	if !bytes.Equal(pt[:len(pt)-pad], data) {
		t.Fatal("round-trip mismatch")
	}
}

func TestEncryptCBCRejectsBadKeyIV(t *testing.T) {
	if EncryptCBC([]byte("x"), []byte("short"), make([]byte, 16)) != nil {
		t.Fatal("accepted a bad key length")
	}
	if EncryptCBC([]byte("x"), make([]byte, 16), []byte("shortiv")) != nil {
		t.Fatal("accepted a bad iv length")
	}
}
