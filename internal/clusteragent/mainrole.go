package clusteragent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// MAIN's data-plane client (XC_VM ADR 0004, Phase 9's eighth increment).
//
// MAIN reads files other servers own (a source probe, a node's certbot log,
// the movies and created channels MAIN runs) and pulls streams from load
// balancer parents. Before this it did so with the legacy URLs, the stream
// secret in them, which is what kept every node's legacy /api answering.
// Now MAIN runs this agent as `xc_agent run -role main`: the loopback relay
// proxy (relayproxy.go) with an identity of MAIN's own.
//
//   - The key: `xc_agent keygen -state config/cluster/main_agent.json -uuid
//     <uuid>`, run by MAIN's `cluster:main-dataplane on`. Its public half is
//     in the signed node list as MAIN's entry (sid, gen, ed_pub, active,
//     dataplane), so a parent or an owner checks MAIN's proofs as it checks
//     any node's: nothing new on their side.
//   - main.json beside it (MainIdentity), written by MAIN's PHP: the server
//     id, the generation, the panel key tickets verify under, and whether
//     MAIN's data plane is on. The agent follows it every MainIdentityEvery:
//     the flag and the generation at once; a new server id, uuid or panel
//     key by exiting, so the supervisor starts it again on the new one.
//   - replica/servers.json and replica/tickets.json, written by MAIN's PHP:
//     the servers section as the nodes get it (routes, node keys) and the
//     tickets MAIN minted for itself (fetcher_sid / child_sid = MAIN). The
//     agent re-reads tickets.json when it changes (TicketsFollowFile).
//   - MAIN's clock is this machine's: no offset, no anchor.
//
// Nothing else of a node's agent runs here: no hello, heartbeat, replica
// sync, commands or events. Without main_agent.json the listener runs alone
// and answers 503, as before.

// MainIdentityFile is main.json, beside MAIN's key state.
const MainIdentityFile = "main.json"

// MainStateFile is MAIN's key state, beside the node agent's.
const MainStateFile = "main_agent.json"

// MainIdentityEvery is how often MAIN's agent re-reads main.json.
var MainIdentityEvery = 2 * time.Second

// MainIdentity is main.json.
type MainIdentity struct {
	V            int    `json:"v"`
	ServerID     int64  `json:"server_id"`
	NodeUUID     string `json:"node_uuid"`
	Gen          int64  `json:"gen"`
	PanelSignPub []byte `json:"panel_sign_pub"`
	Dataplane    bool   `json:"dataplane"`
}

// ReadMainIdentity reads main.json; a document that is not a whole identity
// is an error.
func ReadMainIdentity(path string) (*MainIdentity, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var id MainIdentity
	if err := json.Unmarshal(b, &id); err != nil {
		return nil, fmt.Errorf("clusteragent: %s: %w", path, err)
	}
	if id.V != 1 || id.ServerID <= 0 || id.Gen <= 0 || len(id.PanelSignPub) != 32 || !uuidRe.MatchString(id.NodeUUID) {
		return nil, fmt.Errorf("clusteragent: %s is not a MAIN identity", path)
	}
	return &id, nil
}

// NewMainAgent is MAIN's agent: the key state xc_agent keygen wrote at
// statePath, and main.json beside it. Its replica directory and relay key
// live beside the state too, where MAIN's PHP writes and reads them.
func NewMainAgent(statePath, relayAddr string, logf func(string, ...any)) (*Agent, error) {
	st, err := loadRaw(statePath)
	if err != nil {
		return nil, err
	}
	if len(st.NodeSignSeed) != 32 || !uuidRe.MatchString(st.NodeUUID) {
		return nil, errors.New("clusteragent: MAIN's key state is incomplete (run xc_agent keygen)")
	}
	dir := filepath.Dir(statePath)
	id, err := ReadMainIdentity(filepath.Join(dir, MainIdentityFile))
	if err != nil {
		return nil, err
	}
	if id.NodeUUID != st.NodeUUID {
		return nil, errors.New("clusteragent: main.json names another key")
	}
	st.ServerID, st.PanelSignPub = id.ServerID, id.PanelSignPub
	a := &Agent{
		Client: NewClient(st, "xc_agent/main"), Logf: logf,
		ReplicaDir: filepath.Join(dir, "replica"), RelayAddr: relayAddr, RelayKeyDir: dir,
		MainIdentityPath: filepath.Join(dir, MainIdentityFile), TicketsFollowFile: true,
	}
	a.mode.Store(1)
	a.adoptMainIdentity(id)
	return a, nil
}

// adoptMainIdentity takes the parts of main.json that change while MAIN's
// agent runs: whether the data plane is on, and the generation tickets must
// name.
func (a *Agent) adoptMainIdentity(id *MainIdentity) {
	a.selfGen.Store(id.Gen)
	if id.Dataplane {
		a.flows.Store(FlowDataplane)
	} else {
		a.flows.Store(0)
	}
}

// followMainIdentity re-reads main.json. It returns an error when the
// identity the agent runs with is gone or changed (another server id, key
// or panel key): the caller exits, and the supervisor starts it anew.
func (a *Agent) followMainIdentity() error {
	id, err := ReadMainIdentity(a.MainIdentityPath)
	if err != nil {
		a.flows.Store(0)
		return fmt.Errorf("MAIN's identity is gone: %w", err)
	}
	st := a.Client.State
	if id.NodeUUID != st.NodeUUID || id.ServerID != st.ServerID || string(id.PanelSignPub) != string(st.PanelSignPub) {
		a.flows.Store(0)
		return errors.New("MAIN's identity changed")
	}
	was := a.flows.Load()
	a.adoptMainIdentity(id)
	if now := a.flows.Load(); now != was {
		a.logf("cluster: MAIN's data plane %s (gen %d)", map[bool]string{true: "on", false: "off"}[now != 0], id.Gen)
	}
	return nil
}

// MainDigestN1File is where MAIN's agent reports the owners whose chunk
// digest named no request (DigestN1Report), beside its key state: MAIN's
// agent sends no heartbeat, so MAIN's PHP reads this instead of digest_n1.
const MainDigestN1File = "main_digest_n1.json"

// MainDigestN1Every is how often an unchanged report is written again, so
// MAIN's PHP can tell a running agent's report from a stopped one's.
var MainDigestN1Every = 10 * time.Minute

// writeMainDigestN1 writes MAIN's report when it changed, or when the last
// write is MainDigestN1Every old: {"owners": [...], "at_ms": <unix ms>}.
func (a *Agent) writeMainDigestN1(last *string, lastAt *time.Time, now time.Time) {
	owners, _ := json.Marshal(a.DigestN1Report(now))
	if string(owners) == *last && now.Sub(*lastAt) < MainDigestN1Every {
		return
	}
	doc, _ := json.Marshal(map[string]any{"owners": json.RawMessage(owners), "at_ms": now.UnixMilli()})
	if err := writeFile(filepath.Join(filepath.Dir(a.MainIdentityPath), MainDigestN1File), doc, fileWrite{perm: 0o644}); err != nil {
		a.logf("cluster: %s: %v", MainDigestN1File, err)
		return
	}
	*last, *lastAt = string(owners), now
}

// RunMain serves the loopback proxy as MAIN until ctx ends, or until MAIN's
// identity changes (an error: the supervisor restarts the agent on the new
// one).
func (a *Agent) RunMain(ctx context.Context) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		t := time.NewTicker(MainIdentityEvery)
		defer t.Stop()
		var reported string
		var reportedAt time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			if err := a.followMainIdentity(); err != nil {
				cancel(err)
				return
			}
			a.writeMainDigestN1(&reported, &reportedAt, time.Now())
		}
	}()
	err := a.ServeRelayProxy(ctx, a.RelayAddr, a.RelayKeyDir)
	if cause := context.Cause(ctx); err == nil && cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}
