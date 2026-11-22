package clustercrypto

import (
	"crypto/sha256"
	"encoding/hex"
)

// FileRef is the opaque reference a file ticket binds a path to: the first 32
// hex chars of sha256("xcvm-file-ref-v1" ‖ u32be(ownerSid) ‖ u32be(len(path)) ‖
// path). The owner id and the path length are folded in so a ref names exactly
// one (owner, path); the owner side recomputes it and compares constant-time
// against the ticket's ref before serving. Mirrors the panel's DataPlane::ref.
func FileRef(ownerSid int64, path string) string {
	h := sha256.New()
	h.Write([]byte("xcvm-file-ref-v1"))
	h.Write(U32(uint32(ownerSid)))
	h.Write(U32(uint32(len(path))))
	h.Write([]byte(path))
	return hex.EncodeToString(h.Sum(nil))[:32]
}
