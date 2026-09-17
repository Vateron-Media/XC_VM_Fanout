// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package hlscrypt

import (
	"bytes"
	"crypto/aes"
	"errors"
	"testing"
)

// The two halves are one scheme: whatever EncryptCBC writes, DecryptCBC must
// read back byte for byte, at every length around a block boundary — including
// the exact multiple, where PKCS#7 adds a whole block of padding.
func TestRoundTrip(t *testing.T) {
	key := []byte("0123456789abcdef")
	iv := []byte("fedcba9876543210")
	for _, n := range []int{1, 15, 16, 17, 188, 4096, 4097} {
		plain := make([]byte, n)
		for i := range plain {
			plain[i] = byte(i * 7)
		}
		ct := EncryptCBC(plain, key, iv)
		if ct == nil {
			t.Fatalf("n=%d: EncryptCBC returned nil", n)
		}
		if len(ct)%aes.BlockSize != 0 || len(ct) <= n {
			t.Fatalf("n=%d: ciphertext is %d bytes", n, len(ct))
		}
		got, err := DecryptCBC(ct, key, iv)
		if err != nil {
			t.Fatalf("n=%d: DecryptCBC: %v", n, err)
		}
		if !bytes.Equal(got, plain) {
			t.Errorf("n=%d: round trip changed the bytes", n)
		}
	}
}

// A segment that is not ciphertext under this key must fail as a bad segment,
// never as a panic and never as silent garbage handed to viewers.
func TestDecryptRejectsWhatIsNotCiphertext(t *testing.T) {
	key := []byte("0123456789abcdef")
	iv := []byte("fedcba9876543210")
	good := EncryptCBC([]byte("hello"), key, iv)

	cases := []struct {
		name    string
		data    []byte
		key, iv []byte
	}{
		{"empty body", nil, key, iv},
		{"not a whole block", []byte("short"), key, iv},
		{"key of the wrong length", append([]byte{}, good...), []byte("too short"), iv},
		{"iv of the wrong length", append([]byte{}, good...), key, []byte("nope")},
		{"wrong key: padding will not decode", append([]byte{}, good...), []byte("ffffffffffffffff"), iv},
		{"plain TS bytes, block-aligned", bytes.Repeat([]byte{0x47}, 2*aes.BlockSize), key, iv},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := DecryptCBC(c.data, c.key, c.iv)
			if err == nil {
				t.Fatalf("accepted %d bytes and returned %d", len(c.data), len(got))
			}
			if !errors.Is(err, ErrCiphertext) {
				t.Errorf("err = %v, want it to wrap ErrCiphertext so the caller can classify it", err)
			}
		})
	}
}

// The panel's own scheme is the reference: a key and IV of exactly 16 bytes,
// and a plaintext that is a whole number of TS packets.
func TestDecryptsThePanelsOwnSegments(t *testing.T) {
	key := []byte("xc_vm_stream_key")
	iv := []byte("xc_vm_stream_iv_")
	seg := bytes.Repeat([]byte{0x47, 0x40, 0x11, 0x10}, 47*188/4) // 47 packets
	got, err := DecryptCBC(EncryptCBC(seg, key, iv), key, iv)
	if err != nil {
		t.Fatalf("DecryptCBC: %v", err)
	}
	if !bytes.Equal(got, seg) {
		t.Error("a panel-encrypted segment did not come back intact")
	}
	if len(got)%188 != 0 {
		t.Errorf("plaintext is %d bytes, not a whole number of TS packets", len(got))
	}
}
