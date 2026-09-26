package clusteragent

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// The node replica (plan, section 9, Phase 7): what MAIN sends so the node can
// serve without MAIN's database, fetched through the config op and kept on
// disk exactly as it came, sealed to this node's box key and panel-signed:
//
//	replica/state.json                 {blocklist_seq, blocklist_etag}
//	replica/blocklist.rep              the last whole blocklist section (rep)
//	replica/blocklist.d/<seq>.blk      blk deltas since that section, in seq order
//
// A record is only written once it opens for this node and verifies against
// the pinned panel key; a rep record must name this node. A new section
// replaces the deltas. Nothing here applies the replica yet: cluster:apply
// reads these files (the replica is in shadow until the CONFIG flow is on).

// ReplicaPoll is how often the agent asks MAIN for what changed.
var ReplicaPoll = 60 * time.Second

// ReplicaFullEvery is how often the agent asks for the whole blocklist again,
// the plan's daily safety net against a change the log missed.
var ReplicaFullEvery = 24 * time.Hour

// ReplicaMaxDeltas is how many deltas the agent keeps before it asks for the
// whole section instead.
var ReplicaMaxDeltas = 1000

const replicaPurpose = "replica"

var etagRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ReplicaState is where the node's replica stands.
type ReplicaState struct {
	BlocklistSeq  int64  `json:"blocklist_seq"`
	BlocklistEtag string `json:"blocklist_etag"`
	FullAt        int64  `json:"full_at"` // unix seconds of the last whole section
}

type configReply struct {
	Blocklist struct {
		Seq       int64  `json:"seq"`
		More      bool   `json:"more"`
		Unchanged bool   `json:"unchanged"`
		Delta     string `json:"delta"`
		Section   *struct {
			Etag   string `json:"etag"`
			Sealed string `json:"sealed"`
		} `json:"section"`
	} `json:"blocklist"`
}

// OpenRecord opens a replica record sealed to this node and checks the panel
// signature for its tag. It returns the signed payload.
func (c *Client) OpenRecord(sealed []byte, tag string) ([]byte, error) {
	c.State.mu.Lock()
	sk, uuid, pub := c.State.NodeBoxSk, c.State.NodeUUID, c.State.PanelSignPub
	c.State.mu.Unlock()
	body, err := cc.Open(sk, replicaPurpose, uuid, sealed)
	if err != nil {
		return nil, fmt.Errorf("clusteragent: replica record does not open for this node: %w", err)
	}
	if len(body) < 4 {
		return nil, errors.New("clusteragent: replica record too short")
	}
	n := int(binary.BigEndian.Uint32(body))
	if n < 0 || 4+n > len(body) {
		return nil, errors.New("clusteragent: replica record length")
	}
	payload, sig := body[4:4+n], body[4+n:]
	if !cc.VerifyPanel(pub, tag, payload, sig) {
		return nil, fmt.Errorf("clusteragent: replica record: bad %s signature", tag)
	}
	return payload, nil
}

// LoadReplicaState reads the replica's state; a missing file is a new node.
func LoadReplicaState(dir string) ReplicaState {
	var st ReplicaState
	if b, err := os.ReadFile(filepath.Join(dir, "state.json")); err == nil {
		_ = json.Unmarshal(b, &st)
	}
	return st
}

func writeFileAtomic(path string, b []byte) error {
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	if err := os.WriteFile(tmp, b, 0o640); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// SyncReplica asks MAIN for what changed since the replica's state and stores
// it; it repeats while MAIN has more.
func (a *Agent) SyncReplica(ctx context.Context) error {
	dir := a.ReplicaDir
	if dir == "" {
		return nil
	}
	deltas := filepath.Join(dir, "blocklist.d")
	if err := os.MkdirAll(deltas, 0o750); err != nil {
		return err
	}
	st := LoadReplicaState(dir)
	for round := 0; round < 100; round++ {
		since := st.BlocklistSeq
		now := time.Now().Unix()
		if n, _ := os.ReadDir(deltas); len(n) >= ReplicaMaxDeltas || now-st.FullAt >= int64(ReplicaFullEvery/time.Second) {
			since = 0
		}
		var r configReply
		payload := map[string]any{"blocklist_since": since, "have": map[string]string{"blocklist": st.BlocklistEtag}}
		if err := a.Client.Call(ctx, "config", payload, &r, false); err != nil {
			return err
		}
		b := r.Blocklist
		switch {
		case b.Section != nil:
			if err := a.storeSection(dir, deltas, b.Section.Etag, b.Section.Sealed, b.Seq); err != nil {
				return err
			}
			st = ReplicaState{BlocklistSeq: b.Seq, BlocklistEtag: b.Section.Etag, FullAt: now}
		case b.Unchanged:
			st.BlocklistSeq, st.FullAt = b.Seq, now
		case b.Delta != "":
			if err := a.storeDelta(deltas, b.Delta, st.BlocklistSeq, b.Seq); err != nil {
				return err
			}
			st.BlocklistSeq = b.Seq
		default:
			if b.Seq > st.BlocklistSeq {
				st.BlocklistSeq = b.Seq
			}
		}
		out, _ := json.Marshal(st)
		if err := writeFileAtomic(filepath.Join(dir, "state.json"), out); err != nil {
			return err
		}
		if !b.More {
			return nil
		}
	}
	return nil
}

func (a *Agent) storeSection(dir, deltas, etag, sealedB64 string, seq int64) error {
	sealed, err := base64.StdEncoding.DecodeString(sealedB64)
	if err != nil || !etagRe.MatchString(etag) {
		return errors.New("clusteragent: config: bad blocklist section")
	}
	payload, err := a.Client.OpenRecord(sealed, "rep")
	if err != nil {
		return err
	}
	var doc struct {
		Section string `json:"section"`
		Node    string `json:"node"`
		Etag    string `json:"etag"`
		Seq     int64  `json:"seq"`
	}
	if err := json.Unmarshal(payload, &doc); err != nil || doc.Section != "blocklist" || doc.Node != a.Client.State.NodeUUID || doc.Etag != etag || doc.Seq != seq {
		return errors.New("clusteragent: config: the blocklist section is not this node's, or not the one announced")
	}
	if err := writeFileAtomic(filepath.Join(dir, "blocklist.rep"), sealed); err != nil {
		return err
	}
	// The section holds everything up to its seq: the deltas before it go.
	old, _ := os.ReadDir(deltas)
	for _, e := range old {
		os.Remove(filepath.Join(deltas, e.Name()))
	}
	return nil
}

func (a *Agent) storeDelta(deltas, sealedB64 string, after, seq int64) error {
	sealed, err := base64.StdEncoding.DecodeString(sealedB64)
	if err != nil {
		return errors.New("clusteragent: config: bad blocklist delta")
	}
	payload, err := a.Client.OpenRecord(sealed, "blk")
	if err != nil {
		return err
	}
	var doc struct {
		Seq    int64    `json:"seq"`
		Add    []string `json:"add"`
		Remove []string `json:"remove"`
	}
	if err := json.Unmarshal(payload, &doc); err != nil || doc.Seq != seq || seq <= after {
		return errors.New("clusteragent: config: the blocklist delta is not the one announced")
	}
	return writeFileAtomic(filepath.Join(deltas, fmt.Sprintf("%019d.blk", seq)), sealed)
}

// ReplicaDeltas lists the stored deltas in seq order (for cluster:apply and tests).
func ReplicaDeltas(dir string) []string {
	entries, _ := os.ReadDir(filepath.Join(dir, "blocklist.d"))
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".blk") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// RunReplica keeps the replica current until ctx ends.
func (a *Agent) RunReplica(ctx context.Context) {
	for {
		if err := a.SyncReplica(ctx); err != nil && ctx.Err() == nil {
			a.logf("cluster: replica: %v", err)
		}
		if !sleep(ctx, jitter(ReplicaPoll)) {
			return
		}
	}
}
