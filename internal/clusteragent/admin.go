package clusteragent

import (
	"crypto/ed25519"
	"encoding/hex"
	"net/http"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// Relay admission headers (RelayGuard::TICKET / ::AUTH) the downstream child's Go
// relay proxy sends when it pulls /admin/live from this node (relayproxy.go).
const (
	relayTicketHeader = "X-XCVM-Relay"
	relayAuthHeader   = "X-XCVM-Relay-Auth"
)

// admitRelay is the parent-side of RelayGuard's relay arm for /admin/<kind>: a
// downstream child pulls a stream sourced on this node with a MAIN-signed "rly"
// ticket (naming THIS node as parent_sid and the stream) plus a per-request
// node-key proof. It mirrors serveXFile's two-factor admission — the agent's
// child-side verifiedTicket checks child_sid==self; here the parent checks
// parent_sid==self and that the request is for the ticket's stream. nowMs is
// MAIN's clock as the node can vouch for it.
func (a *Agent) admitRelay(streamID int64, r *http.Request, nowMs int64) bool {
	wire := r.Header.Get(relayTicketHeader)
	auth := r.Header.Get(relayAuthHeader)
	if wire == "" || auth == "" || len(a.Client.State.PanelSignPub) != ed25519.PublicKeySize {
		return false
	}
	t, ok := cc.VerifyTicket(a.Client.State.PanelSignPub, "rly", wire, nowMs/1000)
	if !ok {
		return false
	}
	parent, okP := t.Int("parent_sid")
	sid, okS := t.Int("stream_id")
	childSid, okC := t.Int("child_sid")
	childGen, okG := t.Int("child_gen")
	if !okP || parent != a.Client.State.ServerID || !okS || sid != streamID || !okC || !okG {
		return false
	}
	childPub, ok := a.fetcherSignPub(childSid, childGen)
	if !ok {
		return false
	}
	// The proof is over the exact raw request-target the child signed; the front
	// forwards it unaltered.
	_, nonce, ok := cc.VerifyRelayAuth(childPub, auth, wire, http.MethodGet, r.RequestURI, nowMs)
	if !ok {
		return false
	}
	a.nonceOnce.Do(func() { a.nonces = newNonceCache() })
	if !a.nonces.fresh(hex.EncodeToString(nonce)) {
		return false
	}
	return true
}
