package clustercrypto

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// Token is the panel-signed token document (tag "tok"), as it arrives sealed
// to the agent's per-epoch key (purpose "token", context = node uuid):
//
//	body = u32(len(doc)) ‖ doc ‖ Ed25519 panel sig over doc (tag "tok")
type Token struct {
	V           int    `json:"v"`
	Typ         string `json:"typ"`
	NodeUUID    string `json:"node_uuid"`
	ServerID    int64  `json:"server_id"`
	Gen         int64  `json:"gen"`
	Epoch       uint64 `json:"epoch"`
	Iat         int64  `json:"iat"`
	Nbf         int64  `json:"nbf"`
	Exp         int64  `json:"exp"`
	Kid         string `json:"kid"`
	RotationMin int    `json:"rotation_min"`
	GraceMin    int    `json:"grace_min"`
	RefreshAt   int64  `json:"refresh_at"`
	T           string `json:"token"`
}

// SessionKeys are the four keys of one epoch, derived from T.
type SessionKeys struct {
	MacUp, MacDown, EncUp, EncDown []byte
}

// DeriveSession derives the session keys from the token secret T.
func DeriveSession(t []byte) SessionKeys {
	return SessionKeys{
		MacUp:   hmac256(t, []byte("xcvm/mac/up")),
		MacDown: hmac256(t, []byte("xcvm/mac/down")),
		EncUp:   hmac256(t, []byte("xcvm/enc/up")),
		EncDown: hmac256(t, []byte("xcvm/enc/down")),
	}
}

// OpenToken opens a sealed token with the per-epoch secret key, checks the
// panel signature against the pinned key and that the document is this
// node's, and returns it with its session keys. Any failure is ErrOpen or a
// descriptive error; the token is never used unverified.
func OpenToken(ephSk, panelPub []byte, nodeUUID string, sealed []byte) (*Token, SessionKeys, error) {
	body, err := Open(ephSk, "token", nodeUUID, sealed)
	if err != nil {
		return nil, SessionKeys{}, err
	}
	if len(body) < 4 {
		return nil, SessionKeys{}, ErrOpen
	}
	n := binary.BigEndian.Uint32(body[:4])
	if uint64(n) > uint64(len(body)-4) || len(body)-4-int(n) != 64 {
		return nil, SessionKeys{}, ErrOpen
	}
	doc, sig := body[4:4+n], body[4+n:]
	if !VerifyPanel(panelPub, "tok", doc, sig) {
		return nil, SessionKeys{}, errors.New("clustercrypto: token signature")
	}
	var tok Token
	if err := json.Unmarshal(doc, &tok); err != nil {
		return nil, SessionKeys{}, err
	}
	if tok.V != 1 || tok.Typ != "xcvm-token" || tok.NodeUUID != nodeUUID {
		return nil, SessionKeys{}, errors.New("clustercrypto: token is not for this node")
	}
	t, err := hex.DecodeString(tok.T)
	if err != nil || len(t) != 32 {
		return nil, SessionKeys{}, errors.New("clustercrypto: token secret")
	}
	return &tok, DeriveSession(t), nil
}
