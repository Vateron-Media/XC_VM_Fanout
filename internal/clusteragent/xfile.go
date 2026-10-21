package clusteragent

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"sync"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// serveXFile is the owner side of /xfile, in Go, byte-for-byte as the panel's
// FileTicketServer::serve: a fetcher reads a file this node owns, authorised by
// a panel-signed file ticket plus a per-request node-key proof, every chunk
// answered by a node-signed digest. The node signing key and the nonce window
// stay in this process; the front only proxies the public request here.
//
// The response is identical to PHP's; the one owner-side addition (improvement
// #1) is that each refusal records its reason and a metric, so a misconfigured
// cluster is debuggable without changing a single on-wire byte.

// xfileStats counts serves and refusals-by-reason for an operator.
type xfileStats struct {
	mu       sync.Mutex
	serves   uint64
	refusals map[string]uint64
}

func (s *xfileStats) serve() {
	s.mu.Lock()
	s.serves++
	s.mu.Unlock()
}

func (s *xfileStats) refuse(reason string) {
	s.mu.Lock()
	if s.refusals == nil {
		s.refusals = map[string]uint64{}
	}
	s.refusals[reason]++
	s.mu.Unlock()
}

// snapshot returns the serve count and a copy of the refusal counters (tests).
func (s *xfileStats) snapshot() (uint64, map[string]uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make(map[string]uint64, len(s.refusals))
	for k, v := range s.refusals {
		cp[k] = v
	}
	return s.serves, cp
}

// parseXfileUint parses an unsigned decimal exactly as the panel's uint(): no
// sign, no leading zero (except "0"), at most 16 digits. Anything else is a 400.
func parseXfileUint(s string) (int64, bool) {
	if s == "" || len(s) > 16 {
		return 0, false
	}
	if s != "0" && s[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func (a *Agent) serveXFile(w http.ResponseWriter, r *http.Request) {
	// nosniff on every response (the panel's controller sets it for all statuses).
	w.Header().Set("X-Content-Type-Options", "nosniff")
	refuse := func(reason string, status int) {
		a.xfileMetrics.refuse(reason)
		if a.Logf != nil {
			a.Logf("xfile: refused (%s): %d", reason, status)
		}
		w.WriteHeader(status)
	}

	wire := r.Header.Get("X-XCVM-File")
	auth := r.Header.Get("X-XCVM-File-Auth")
	if wire == "" || auth == "" || len(a.Client.State.PanelSignPub) != ed25519.PublicKeySize {
		refuse("missing ticket/auth/panel", http.StatusNotFound)
		return
	}

	now := a.Client.MainNowMs()
	ticket, ok := cc.VerifyTicket(a.Client.State.PanelSignPub, "fil", wire, now/1000)
	if !ok {
		refuse("ticket verify", http.StatusNotFound)
		return
	}
	owner, okO := ticket.Int("owner_sid")
	fetcherSid, okF := ticket.Int("fetcher_sid")
	fetcherGen, okG := ticket.Int("fetcher_gen")
	ref, okR := ticket.String("ref")
	file, okFile := ticket.String("file")
	if !okO || owner != a.Client.State.ServerID || !okF || !okG || !okR || !okFile {
		refuse("ticket fields", http.StatusNotFound)
		return
	}

	fetcherPub, ok := a.fetcherSignPub(fetcherSid, fetcherGen)
	if !ok {
		refuse("fetcher inactive/gen", http.StatusNotFound)
		return
	}
	// The proof is verified over the exact raw request-target the fetcher
	// signed; the front forwards it unaltered.
	_, nonce, ok := cc.VerifyRelayAuth(fetcherPub, auth, wire, http.MethodGet, r.RequestURI, now)
	if !ok {
		refuse("relay-auth", http.StatusNotFound)
		return
	}
	a.nonceOnce.Do(func() { a.nonces = newNonceCache() })
	if !a.nonces.fresh(hex.EncodeToString(nonce)) {
		refuse("nonce replay", http.StatusNotFound)
		return
	}

	offset, okOff := parseXfileUint(r.URL.Query().Get("o"))
	length, okLen := parseXfileUint(r.URL.Query().Get("n"))
	if !okOff || !okLen || length < 1 || length > cc.FileChunk {
		refuse("bad o/n", http.StatusBadRequest)
		return
	}

	path, ok := a.resolveFilePath(file, ref)
	if !ok || !extAllowed(path) || !pathAllowed(path, a.allowedRoots()) {
		refuse("path not allowed", http.StatusNotFound)
		return
	}

	chunk, total, err := openChunk(path, offset, length)
	if err == errOffsetPastEnd {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(total, 10))
		refuse("offset past end", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if err != nil {
		refuse("open/read", http.StatusNotFound)
		return
	}

	if len(a.Client.State.NodeSignSeed) != ed25519.SeedSize {
		refuse("no node key", http.StatusServiceUnavailable)
		return
	}
	digestTotal := total
	if end := offset + int64(len(chunk)); end > digestTotal {
		digestTotal = end
	}
	sum := sha256.Sum256(chunk)
	off, dt := offset, digestTotal
	doc, err := cc.FileDigestDoc(ticket.Tid, owner, int64(len(chunk)), hex.EncodeToString(sum[:]), now/1000, &off, &dt, hex.EncodeToString(nonce))
	if err != nil {
		refuse("digest doc", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("X-XCVM-File-Digest", cc.JoinSigned(doc, cc.SignNode(a.Client.State.SignKey(), "digest", doc)))

	body := chunk
	if rk := r.URL.Query().Get(relaySealParam); rk != "" {
		key, err := a.openFileSealKey(rk, owner, ticket.Tid)
		if err != nil {
			refuse("rk open", http.StatusNotFound)
			return
		}
		var framed bytes.Buffer
		if err := writeRelayFrames(&framed, chunk, key); err != nil {
			refuse("rk frame", http.StatusServiceUnavailable)
			return
		}
		body = framed.Bytes()
		w.Header().Set(relaySealHeader, relaySealVersion)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	a.xfileMetrics.serve()
	// body is authorized file bytes — the ticket's ref binds owner+path and the
	// query only selects which bytes — served as application/octet-stream with
	// X-Content-Type-Options: nosniff to a node's fetcher agent, never rendered as
	// a web page (faithful to the PHP owner). Not reflected input; no XSS.
	// nosemgrep: go.net.xss.no-direct-write-to-responsewriter-taint.no-direct-write-to-responsewriter-taint
	_, _ = w.Write(body)
}
