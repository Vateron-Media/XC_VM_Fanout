package clusteragent

import (
	"errors"
	"os"
	"path/filepath"
	"time"
)

// xfileRouteTTL bounds how stale the owner's view of the signed node list may
// be when it resolves a fetcher for /xfile. It matches the panel's
// DataPlaneTrust::node (5s), and is deliberately far shorter than the relay
// proxy's 60s cache so a re-enrolled node's new generation is seen promptly.
const xfileRouteTTL = 5 * time.Second

// fetcherRoutes returns the parsed servers.json, re-reading it when the file
// changed or the short TTL elapsed. It is the owner-side counterpart to
// RelayProxy.serverRoutes, with its own cache (that one is per-proxy and 60s).
func (a *Agent) fetcherRoutes() (*serverRoutes, error) {
	a.xfileRouteMu.Lock()
	defer a.xfileRouteMu.Unlock()
	name := filepath.Join(a.ReplicaDir, "servers.json")
	fi, err := os.Stat(name)
	if err != nil {
		return nil, errors.New("no servers section")
	}
	if a.xfileRoutes != nil && fi.ModTime().Equal(a.xfileRouteMod) && time.Since(a.xfileRouteAt) < xfileRouteTTL {
		return a.xfileRoutes, nil
	}
	b, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	rt, err := parseServerRoutes(b, a.Client.State.ServerID)
	if err != nil {
		return nil, err
	}
	a.xfileRoutes, a.xfileRouteMod, a.xfileRouteAt = rt, fi.ModTime(), time.Now()
	return rt, nil
}

// fetcherSignPub returns the ed25519 public key of fetcher node sid, but only
// when it is active at exactly generation gen (the panel's RelayGuard::child).
// A re-enrolled node at a new gen, an inactive node, an unknown sid, or an
// unreadable servers.json all refuse. The exact-generation match is a security
// property: a node that re-enrolled must not pass a proof for an old ticket.
func (a *Agent) fetcherSignPub(sid, gen int64) ([]byte, bool) {
	rt, err := a.fetcherRoutes()
	if err != nil {
		return nil, false
	}
	n, ok := rt.nodes[sid]
	if !ok || n.state != "active" || n.gen != gen || len(n.pub) != 32 {
		return nil, false
	}
	return n.pub, true
}
