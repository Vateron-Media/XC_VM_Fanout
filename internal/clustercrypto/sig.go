package clustercrypto

import (
	"crypto/ed25519"
)

// PanelTags is the closed registry of panel signature tags. A tag outside it
// never verifies.
var PanelTags = map[string]bool{
	"cmd": true, "blk": true, "den": true, "hlt": true, "dig": true, "pol": true, "rep": true, "bnd": true,
	"rly": true, "fil": true, "nod": true, "cfg": true, "pre": true, "tok": true, "lea": true,
}

// NodePurposes is the closed set of node signature purposes.
var NodePurposes = map[string]bool{"request": true, "enrol": true, "relay": true, "digest": true}

// PanelSigInput is what the panel signs: "xcvm-sig-v1" ‖ lp(tag) ‖ payload.
func PanelSigInput(tag string, payload []byte) []byte {
	return append(append([]byte("xcvm-sig-v1"), lps(tag)...), payload...)
}

// VerifyPanel checks a panel signature against the pinned panel key.
func VerifyPanel(panelPub []byte, tag string, payload, sig []byte) bool {
	if len(panelPub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize || !PanelTags[tag] {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(panelPub), PanelSigInput(tag, payload), sig)
}

// NodeSigInput is what a node signs: "xcvm-node-sig-v1" ‖ lp(purpose) ‖ payload.
// A separate domain from the panel's, so neither signature can stand in for
// the other.
func NodeSigInput(purpose string, payload []byte) []byte {
	return append(append([]byte("xcvm-node-sig-v1"), lps(purpose)...), payload...)
}

// SignNode signs as the node. It panics on an unknown purpose: that is a
// programming error, not input.
func SignNode(sk ed25519.PrivateKey, purpose string, payload []byte) []byte {
	if !NodePurposes[purpose] {
		panic("clustercrypto: unknown node signature purpose " + purpose)
	}
	return ed25519.Sign(sk, NodeSigInput(purpose, payload))
}

// VerifyNode checks a node signature.
func VerifyNode(nodePub []byte, purpose string, payload, sig []byte) bool {
	if len(nodePub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize || !NodePurposes[purpose] {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(nodePub), NodeSigInput(purpose, payload), sig)
}
