package clustercrypto

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
)

// X-XCVM-File-Digest: the owner of a file vouching for what /xfile served
// (the panel's Core\Cluster\Crypto\FileDigest).
//
//	doc    = JSON {"v":1, "typ":"xcvm-file-digest", "tid", "owner_sid", "size", "sha256", "iat"
//	               [, "offset", "total" [, "nonce"]]} (sorted keys)
//	header = b64url(doc) "." b64url(sig)
//
// MAIN signs as the panel (tag dig), a load balancer with its node key
// (purpose digest). /xfile serves a file in chunks of at most FileChunk
// bytes, each with a digest of its own naming its offset and the file's
// total size, and the request it answers: nonce is the hex of the nonce in
// that request's X-XCVM-File-Auth. An owner from before nonce signs none; its
// digest is taken only while its iat is within the request window
// (FileDigest.Answers).

// FileDigestMaxHeader is the longest header accepted, in bytes.
const FileDigestMaxHeader = 2048

// FileChunk is the largest chunk one /xfile response carries.
const FileChunk = 4 << 20

var errInvalid = errors.New("clustercrypto: invalid file digest fields")

var (
	sha256HexRe = regexp.MustCompile(`^[0-9a-f]{64}$`)
	nonceHexRe  = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// FileDigest is a verified digest document.
type FileDigest struct {
	Tid      string `json:"tid"`
	OwnerSid int64  `json:"owner_sid"`
	Size     int64  `json:"size"`
	Sha256   string `json:"sha256"`
	Iat      int64  `json:"iat"`
	Offset   *int64 `json:"offset"`
	Total    *int64 `json:"total"`
	Nonce    string `json:"nonce"`
}

// FileDigestDoc is the document a node signs, its fields in the order the
// panel's FileDigest sorts them (ksort): a byte of difference and the
// fetcher's verification fails. Offset and Total are both set (a chunk) or
// both nil (a whole file); nonceHex, the answered request's File-Auth nonce,
// is a chunk's only ("": none, as an owner from before it signed).
func FileDigestDoc(tid string, ownerSid, size int64, sha256Hex string, iat int64, offset, total *int64, nonceHex string) ([]byte, error) {
	type chunkDoc struct {
		Iat      int64  `json:"iat"`
		Nonce    string `json:"nonce,omitempty"`
		Offset   int64  `json:"offset"`
		OwnerSid int64  `json:"owner_sid"`
		Sha256   string `json:"sha256"`
		Size     int64  `json:"size"`
		Tid      string `json:"tid"`
		Total    int64  `json:"total"`
		Typ      string `json:"typ"`
		V        int    `json:"v"`
	}
	type wholeDoc struct {
		Iat      int64  `json:"iat"`
		OwnerSid int64  `json:"owner_sid"`
		Sha256   string `json:"sha256"`
		Size     int64  `json:"size"`
		Tid      string `json:"tid"`
		Typ      string `json:"typ"`
		V        int    `json:"v"`
	}
	if !sha256HexRe.MatchString(sha256Hex) || size < 0 || ownerSid <= 0 || (offset == nil) != (total == nil) ||
		(offset != nil && (*offset < 0 || *total < *offset+size)) || (nonceHex != "" && (offset == nil || !nonceHexRe.MatchString(nonceHex))) {
		return nil, errInvalid
	}
	var v any = wholeDoc{Iat: iat, OwnerSid: ownerSid, Sha256: sha256Hex, Size: size, Tid: tid, Typ: "xcvm-file-digest", V: 1}
	if offset != nil {
		v = chunkDoc{Iat: iat, Nonce: nonceHex, Offset: *offset, OwnerSid: ownerSid, Sha256: sha256Hex, Size: size, Tid: tid, Total: *total, Typ: "xcvm-file-digest", V: 1}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// VerifyFileDigest checks a header: under the panel key (tag dig) when
// panelPub is set, else under the owner node's key (purpose digest). It
// must name tid.
func VerifyFileDigest(header, tid string, panelPub, nodePub []byte) (*FileDigest, bool) {
	doc, sig, ok := SplitSigned(header, FileDigestMaxHeader)
	if !ok {
		return nil, false
	}
	if panelPub != nil {
		ok = VerifyPanel(panelPub, "dig", doc, sig)
	} else {
		ok = nodePub != nil && VerifyNode(nodePub, "digest", doc, sig)
	}
	if !ok {
		return nil, false
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(doc, &raw) != nil {
		return nil, false
	}
	var d FileDigest
	var head struct {
		V   json.RawMessage `json:"v"`
		Typ string          `json:"typ"`
	}
	if json.Unmarshal(doc, &head) != nil || string(head.V) != "1" || head.Typ != "xcvm-file-digest" {
		return nil, false
	}
	for _, k := range []string{"size", "owner_sid"} {
		if _, isInt := jsonInt(raw[k]); !isInt {
			return nil, false
		}
	}
	if json.Unmarshal(doc, &d) != nil || d.Tid != tid || !sha256HexRe.MatchString(d.Sha256) {
		return nil, false
	}
	_, hasOff := raw["offset"]
	_, hasTot := raw["total"]
	if hasOff || hasTot {
		if _, i1 := jsonInt(raw["offset"]); !i1 {
			return nil, false
		}
		if _, i2 := jsonInt(raw["total"]); !i2 {
			return nil, false
		}
		if d.Offset == nil || d.Total == nil || *d.Offset < 0 || *d.Total < *d.Offset+d.Size {
			return nil, false
		}
	}
	if _, has := raw["nonce"]; has && (!hasOff || !nonceHexRe.MatchString(d.Nonce)) {
		return nil, false
	}
	return &d, true
}

// Answers reports whether the verified digest answers the request that
// carried nonce (16 bytes, the X-XCVM-File-Auth one), judged at nowMs on
// MAIN's clock: it names that nonce, or, from an owner that names none, its
// iat is within the request window (RelayWindowMs, and a second for iat's
// rounding). So an old answer for the same chunk passes for a new request only
// from an owner from before the nonce, and only inside the window.
func (d *FileDigest) Answers(nonce []byte, nowMs int64) bool {
	if d.Nonce != "" {
		return subtle.ConstantTimeCompare([]byte(d.Nonce), []byte(hex.EncodeToString(nonce))) == 1
	}
	if d.Iat < 0 || d.Iat > 1<<40 {
		return false
	}
	skew := d.Iat*1000 - nowMs
	return skew <= RelayWindowMs+1000 && -skew <= RelayWindowMs+1000
}

// ChunkMatches reports whether a received chunk is the one the verified
// digest names at offset.
func (d *FileDigest) ChunkMatches(offset int64, body []byte) bool {
	if d.Offset == nil || *d.Offset != offset || d.Size != int64(len(body)) {
		return false
	}
	sum := sha256.Sum256(body)
	return subtle.ConstantTimeCompare([]byte(hex.EncodeToString(sum[:])), []byte(d.Sha256)) == 1
}
