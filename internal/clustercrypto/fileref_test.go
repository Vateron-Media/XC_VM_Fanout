package clustercrypto

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestFileRef(t *testing.T) {
	path := "/home/xc_vm/vods/movie.mkv"
	got := FileRef(1, path)

	h := sha256.New()
	h.Write([]byte("xcvm-file-ref-v1"))
	h.Write(U32(1))
	h.Write(U32(uint32(len(path))))
	h.Write([]byte(path))
	want := hex.EncodeToString(h.Sum(nil))[:32]

	if got != want || len(got) != 32 {
		t.Fatalf("FileRef = %q (len %d), want %q", got, len(got), want)
	}
}

func TestFileRefOwnerAndLengthBind(t *testing.T) {
	// A different owner id, or a path of a different length, must change the ref:
	// both are folded into the hash (u32be owner, u32be len), so the port cannot
	// drop either field and stay interop-faithful.
	base := FileRef(1, "/mnt/a.ts")
	if FileRef(2, "/mnt/a.ts") == base {
		t.Fatal("owner id not bound into the ref")
	}
	if FileRef(1, "/mnt/aa.ts") == base {
		t.Fatal("path length not bound into the ref")
	}
}
