package clustercrypto

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
)

// Panel-signed data-plane tickets (the panel's Core\Cluster\Crypto\Ticket;
// vectors in testdata/cluster_dataplane_vectors.json):
//
//	wire = b64url(doc) "." b64url(Ed25519 panel sig over "xcvm-sig-v1" ‖ lp(tag) ‖ doc)
//	doc  = JSON {"v":1, "typ", "tid", "iat", "exp", ...} (sorted keys)
//
// A relay ticket (tag rly, typ xcvm-relay, at most 24 h) names the child
// node, its generation, the parent and the stream; a file ticket (tag fil,
// typ xcvm-file, at most 6 h) the fetcher, its generation, the owner, the
// file's stable ref and the owner's opaque name for the file.

// TicketKind is a ticket tag's document type and longest lifetime.
type TicketKind struct {
	Typ    string
	MaxSec int64
}

// TicketKinds are the two ticket tags.
var TicketKinds = map[string]TicketKind{
	"rly": {Typ: "xcvm-relay", MaxSec: 86400},
	"fil": {Typ: "xcvm-file", MaxSec: 21600},
}

// TicketSkew is how far in the future a ticket's iat may be, in seconds.
const TicketSkew = 120

// TicketMaxWire is the longest wire ticket accepted, in bytes.
const TicketMaxWire = 4096

// Ticket is a verified ticket: its document's fields.
type Ticket struct {
	Tag    string
	Tid    string
	Iat    int64
	Exp    int64
	Fields map[string]json.RawMessage
}

// Int is an integer field of the ticket, and whether it is one.
func (t *Ticket) Int(name string) (int64, bool) {
	raw, ok := t.Fields[name]
	if !ok {
		return 0, false
	}
	return jsonInt(raw)
}

// String is a string field of the ticket, and whether it is one.
func (t *Ticket) String(name string) (string, bool) {
	raw, ok := t.Fields[name]
	if !ok {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", false
	}
	return s, true
}

// SplitSigned splits b64url(doc) "." b64url(sig), refusing anything longer
// than max bytes (the panel's Enc::splitSigned).
func SplitSigned(wire string, max int) (doc, sig []byte, ok bool) {
	if len(wire) > max || strings.Count(wire, ".") != 1 {
		return nil, nil, false
	}
	d, s, _ := strings.Cut(wire, ".")
	doc, err1 := b64urlStrict(d)
	sig, err2 := b64urlStrict(s)
	if err1 != nil || err2 != nil {
		return nil, nil, false
	}
	return doc, sig, true
}

// JoinSigned is b64url(doc) "." b64url(sig).
func JoinSigned(doc, sig []byte) string {
	return base64.RawURLEncoding.EncodeToString(doc) + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// b64urlStrict decodes unpadded base64url; empty input or any other
// character is refused, as the panel refuses it.
func b64urlStrict(s string) ([]byte, error) {
	if s == "" {
		return nil, base64.CorruptInputError(0)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return nil, base64.CorruptInputError(i)
		}
	}
	return base64.RawURLEncoding.DecodeString(s)
}

// VerifyTicket checks a wire ticket's panel signature under tag, its type
// and its lifetime at now (unix seconds), and returns it; ok is false for
// anything else.
func VerifyTicket(panelPub []byte, tag, wire string, now int64) (*Ticket, bool) {
	kind, known := TicketKinds[tag]
	if !known {
		return nil, false
	}
	doc, sig, ok := SplitSigned(wire, TicketMaxWire)
	if !ok || !VerifyPanel(panelPub, tag, doc, sig) {
		return nil, false
	}
	var fields map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(doc))
	if dec.Decode(&fields) != nil || dec.More() {
		return nil, false
	}
	t := &Ticket{Tag: tag, Fields: fields}
	v, okV := t.Int("v")
	typ, okT := t.String("typ")
	tid, okD := t.String("tid")
	iat, okI := t.Int("iat")
	exp, okE := t.Int("exp")
	if !okV || v != 1 || !okT || typ != kind.Typ || !okD || !okI || !okE {
		return nil, false
	}
	if exp-iat > kind.MaxSec || iat-TicketSkew > now || now >= exp {
		return nil, false
	}
	t.Tid, t.Iat, t.Exp = tid, iat, exp
	return t, true
}

// jsonInt reads a JSON integer (no fraction, no exponent), as PHP's is_int
// takes a decoded value.
func jsonInt(raw json.RawMessage) (int64, bool) {
	s := strings.TrimSpace(string(raw))
	if s == "" || strings.ContainsAny(s, ".eE") {
		return 0, false
	}
	var n json.Number
	if json.Unmarshal(raw, &n) != nil {
		return 0, false
	}
	i, err := n.Int64()
	return i, err == nil
}
