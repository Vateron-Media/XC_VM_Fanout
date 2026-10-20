package clusteragent

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

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

// loopbackTokenRe matches LoopbackToken's wire: lb1-<exp>-<hex mac>.
var loopbackTokenRe = regexp.MustCompile(`^lb1-(\d{1,12})-([0-9a-f]{64})\z`)

// admitPassword is RelayGuard's legacy password arm for /admin: the shared
// live_streaming_pass, admitted only from an allowed server IP whose node has the
// DATAPLANE flow OFF (a legacy, pre-relay-ticket child; a data-plane child must
// present a relay ticket instead).
func (a *Agent) admitPassword(given, ip string) bool {
	return a.passMatches(a.secret("live_streaming_pass"), given) && a.passwordAllowed(ip)
}

// passwordAllowed ports RelayGuard::passwordAllowed: ip is in getAllowedIPs and
// no server whose server_ip/private_ip is ip has a present node with the
// DATAPLANE flow on (a null node, or a dataplane-off node, does not refuse).
func (a *Agent) passwordAllowed(ip string) bool {
	if ip == "" || !a.allowedIPs()[ip] || a.ReplicaDir == "" {
		return false
	}
	b, err := os.ReadFile(filepath.Join(a.ReplicaDir, "servers.json"))
	if err != nil {
		return false
	}
	var doc struct {
		Data struct {
			Servers []struct {
				ID        int64   `json:"id"`
				ServerIP  *string `json:"server_ip"`
				PrivateIP *string `json:"private_ip"`
			} `json:"servers"`
			Nodes []struct {
				Sid       int64 `json:"sid"`
				Dataplane bool  `json:"dataplane"`
			} `json:"nodes"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &doc) != nil {
		return false
	}
	dpOn := make(map[int64]bool, len(doc.Data.Nodes))
	for _, n := range doc.Data.Nodes {
		dpOn[n.Sid] = n.Dataplane
	}
	for _, s := range doc.Data.Servers {
		if (s.ServerIP != nil && strings.TrimSpace(*s.ServerIP) == ip) ||
			(s.PrivateIP != nil && strings.TrimSpace(*s.PrivateIP) == ip) {
			if dpOn[s.ID] {
				return false
			}
		}
	}
	return true
}

// loopbackMAC is LoopbackToken::mac: HMAC-SHA256(key, "xcvm-loopback-v1" ‖
// u64be(streamID) ‖ u64be(exp)), hex.
func loopbackMAC(key []byte, streamID, exp int64) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("xcvm-loopback-v1"))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(streamID))
	m.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(exp))
	m.Write(buf[:])
	return hex.EncodeToString(m.Sum(nil))
}

// verifyLoopbackToken is LoopbackToken::verify: a well-formed lb1-<exp>-<mac>
// that has not expired and whose MAC matches under the node's loopback key.
func verifyLoopbackToken(key []byte, streamID int64, tok string, now int64) bool {
	if streamID <= 0 || len(key) == 0 {
		return false
	}
	m := loopbackTokenRe.FindStringSubmatch(tok)
	if m == nil {
		return false
	}
	exp, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || exp < now {
		return false
	}
	return hmac.Equal([]byte(loopbackMAC(key, streamID, exp)), []byte(m[2]))
}

// admitLoopback is RelayGuard's loopback arm: an lb1- token the node's own
// recorder presents over 127.0.0.1 only (a non-loopback peer is refused). The key
// is config/cluster/loopback.key (LoopbackKeyFile); with none, the arm is off.
func (a *Agent) admitLoopback(streamID int64, tok, clientIP string, now int64) bool {
	if ip := net.ParseIP(clientIP); ip == nil || !ip.IsLoopback() {
		return false
	}
	if a.LoopbackKeyFile == "" {
		return false
	}
	key, err := os.ReadFile(a.LoopbackKeyFile)
	if err != nil || len(key) == 0 {
		return false
	}
	return verifyLoopbackToken(key, streamID, tok, now)
}
