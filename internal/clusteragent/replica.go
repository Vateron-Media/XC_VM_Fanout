package clusteragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
//	replica/blocklist.json             the section with its deltas applied, for PHP
//	replica/<name>.rep                 a section sent whole (rep): settings,
//	                                   servers, node, crontab, cluster, secrets
//	replica/<name>.json                its data, for PHP: {etag, data}
//
// A record is only written once it opens for this node and verifies against
// the pinned panel key; a rep record must name this node, and a whole
// section its name and the ETag MAIN announced. Each .rep is written before
// its .json: PHP reads a section only where its .json exists, and then
// verifies the .rep. A new blocklist section replaces the deltas. After a
// change the agent writes blocklist.json from the verified records (PHP
// holds no key to open them) and runs Apply (cluster:apply), which diffs the
// replica against the node's own caches in shadow and writes them once the
// CONFIG flow is on.
//
// The whole sections (ADR 0004, Phase 7, fourth to sixth increments) are
// asked for by the ETag held (config's `have`, "" for none) and kept per
// name in state.json (settings_etag, whole_etags). MAIN answers a name with
// {"unchanged": true}, with {etag, sealed}, or not at all ("not served":
// keep what is held; so is a 503 DB to the whole call). `secrets` carries
// the node's stream secret and OPENSSL_EXTRA: its files are 0600, and the
// agent never logs it in any form (data, record, sealed bytes, ETag or kid);
// an error names the section only.

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

// WholeSections are the sections MAIN sends whole, as named in `have`.
var WholeSections = []string{"settings", "servers", "node", "crontab", "cluster", "secrets"}

// secretSection is the one section that carries secrets.
const secretSection = "secrets"

// ReplicaState is where the node's replica stands.
type ReplicaState struct {
	BlocklistSeq  int64  `json:"blocklist_seq"`
	BlocklistEtag string `json:"blocklist_etag"`
	FullAt        int64  `json:"full_at"` // unix seconds of the last whole section
	SettingsEtag  string `json:"settings_etag"`
	// WholeEtags is the ETag held per whole section but settings.
	WholeEtags map[string]string `json:"whole_etags,omitempty"`
}

// etag is the ETag held for a whole section, "" for none.
func (st *ReplicaState) etag(name string) string {
	if name == "settings" {
		return st.SettingsEtag
	}
	return st.WholeEtags[name]
}

func (st *ReplicaState) setEtag(name, etag string) {
	if name == "settings" {
		st.SettingsEtag = etag
		return
	}
	if st.WholeEtags == nil {
		st.WholeEtags = map[string]string{}
	}
	if etag == "" {
		delete(st.WholeEtags, name)
		return
	}
	st.WholeEtags[name] = etag
}

// wholeReply is a section MAIN sends whole: unchanged, or the sealed record.
type wholeReply struct {
	Unchanged bool   `json:"unchanged"`
	Etag      string `json:"etag"`
	Sealed    string `json:"sealed"`
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

func writeFileAtomic(path string, b []byte) error { return writeFileMode(path, b, 0o640) }

// writeFileMode writes path atomically (temp file, fsync, rename) with mode
// perm, whatever the umask or a stale temp file.
func writeFileMode(path string, b []byte, perm os.FileMode) error {
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err := f.Chmod(perm); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// The rename itself durable, so files written in turn (a .rep before
	// its .json) reach the disk in that order.
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// SyncReplica asks MAIN for what changed since the replica's state and stores
// it; it repeats while MAIN has more.
func (a *Agent) SyncReplica(ctx context.Context) error {
	_, err := a.syncReplica(ctx)
	return err
}

// syncReplica is SyncReplica, reporting whether it ran cluster:apply.
func (a *Agent) syncReplica(ctx context.Context) (applied bool, err error) {
	dir := a.ReplicaDir
	if dir == "" {
		return false, nil
	}
	a.replicaMu.Lock()
	defer a.replicaMu.Unlock()
	deltas := filepath.Join(dir, "blocklist.d")
	if err := os.MkdirAll(deltas, 0o750); err != nil {
		return false, err
	}
	// First at the agent's start, then whenever the node's keys changed
	// (an enrolment, a re-enrolment, a new panel key): records that no
	// longer verify are asked for again.
	if keys := a.replicaKeyPrint(); keys != a.replicaKeys {
		a.recheckLocked(dir)
		a.replicaKeys = keys
	}
	st := LoadReplicaState(dir)
	changed, wholeChanged := false, false
	defer func() {
		if _, err := os.Stat(filepath.Join(dir, "blocklist.json")); changed || (err != nil && st.BlocklistEtag != "") {
			if err := a.materialise(dir, st); err != nil {
				a.logf("cluster: replica: %v", err)
				changed = false
			} else {
				changed = true
			}
		}
		if (changed || wholeChanged) && a.Apply != nil {
			a.runApply(ctx)
			applied = true
		}
	}()
	for round := 0; round < 100; round++ {
		since := st.BlocklistSeq
		now := time.Now().Unix()
		if n, _ := os.ReadDir(deltas); len(n) >= ReplicaMaxDeltas || now-st.FullAt >= int64(ReplicaFullEvery/time.Second) {
			since = 0
		}
		have := map[string]string{"blocklist": st.BlocklistEtag}
		for _, name := range WholeSections {
			have[name] = st.etag(name)
		}
		var raw json.RawMessage
		if err := a.Client.Call(ctx, "config", map[string]any{"blocklist_since": since, "have": have}, &raw, false); err != nil {
			// A 503 DB among them: keep every file and ETag held.
			return false, err
		}
		var r configReply
		var parts map[string]json.RawMessage
		if json.Unmarshal(raw, &r) != nil || json.Unmarshal(raw, &parts) != nil {
			return false, errors.New("clusteragent: config: unreadable reply")
		}
		var firstErr error
		for _, name := range WholeSections {
			w, ok, err := wholePart(parts, name)
			if err == nil && ok {
				err = a.storeWhole(dir, name, w)
				if err == nil {
					st.setEtag(name, w.Etag)
					wholeChanged = true
				}
			}
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		b := r.Blocklist
		switch {
		case b.Section != nil:
			if err := a.storeSection(dir, deltas, b.Section.Etag, b.Section.Sealed, b.Seq); err != nil {
				a.saveReplicaState(dir, st)
				return false, err
			}
			st.BlocklistSeq, st.BlocklistEtag, st.FullAt = b.Seq, b.Section.Etag, now
			changed = true
		case b.Unchanged:
			st.BlocklistSeq, st.FullAt = b.Seq, now
		case b.Delta != "":
			if err := a.storeDelta(deltas, b.Delta, st.BlocklistSeq, b.Seq); err != nil {
				a.saveReplicaState(dir, st)
				return false, err
			}
			st.BlocklistSeq = b.Seq
			changed = true
		default:
			if b.Seq > st.BlocklistSeq {
				st.BlocklistSeq = b.Seq
			}
		}
		if err := a.saveReplicaState(dir, st); err != nil {
			return false, err
		}
		if firstErr != nil {
			return false, firstErr
		}
		if !b.More {
			return false, nil
		}
	}
	return false, nil
}

// wholePart is the reply's part for a whole section; ok is false when MAIN
// did not serve it or it is unchanged. The error names the section only.
func wholePart(parts map[string]json.RawMessage, name string) (*wholeReply, bool, error) {
	raw, ok := parts[name]
	if !ok || string(raw) == "null" {
		return nil, false, nil
	}
	var w wholeReply
	if json.Unmarshal(raw, &w) != nil {
		return nil, false, fmt.Errorf("clusteragent: config: bad %s section", name)
	}
	if w.Unchanged || (w.Etag == "" && w.Sealed == "") {
		return nil, false, nil
	}
	return &w, true, nil
}

// saveReplicaState writes replica/state.json, 0600: it holds the secrets
// section's ETag, a hash of the secrets.
func (a *Agent) saveReplicaState(dir string, st ReplicaState) error {
	out, _ := json.Marshal(st)
	return writeFileMode(filepath.Join(dir, "state.json"), out, 0o600)
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

// storeWhole keeps a section sent whole once it verifies, and writes its data
// for PHP (<name>.json: {etag, data}), the .rep first. The secrets section's
// files are 0600. No error carries a section's content.
func (a *Agent) storeWhole(dir, name string, w *wholeReply) error {
	sealed, err := base64.StdEncoding.DecodeString(w.Sealed)
	if err != nil || !etagRe.MatchString(w.Etag) {
		return fmt.Errorf("clusteragent: config: bad %s section", name)
	}
	data, err := a.openWhole(sealed, name, w.Etag)
	if err != nil {
		return err
	}
	if tok, ok := a.Client.Current(); ok && data.Gen != nil && *data.Gen != tok.Gen {
		return fmt.Errorf("clusteragent: config: the %s section is for another generation of this node", name)
	}
	perm := os.FileMode(0o640)
	if name == secretSection {
		perm = 0o600
	}
	if err := writeFileMode(filepath.Join(dir, name+".rep"), sealed, perm); err != nil {
		return fmt.Errorf("clusteragent: config: writing the %s section", name)
	}
	if err := writeFileMode(filepath.Join(dir, name+".json"), wholeJSON(w.Etag, data.Data), perm); err != nil {
		return fmt.Errorf("clusteragent: config: writing the %s section", name)
	}
	return nil
}

// wholeJSON is <name>.json: {"data": <data exactly as signed>, "etag": …}.
// Assembled, not marshalled: encoding/json would escape <, > and & in the
// data. The ETag is 64 hex digits.
func wholeJSON(etag string, data json.RawMessage) []byte {
	out := append([]byte(`{"data":`), data...)
	return append(out, `,"etag":"`+etag+`"}`...)
}

type wholeDoc struct {
	Section string          `json:"section"`
	Node    string          `json:"node"`
	Gen     *int64          `json:"gen"`
	Etag    string          `json:"etag"`
	Data    json.RawMessage `json:"data"`
}

// openWhole opens a whole section's record and checks that it is this
// node's section name with the ETag given.
func (a *Agent) openWhole(sealed []byte, name, etag string) (*wholeDoc, error) {
	payload, err := a.Client.OpenRecord(sealed, "rep")
	if err != nil {
		return nil, fmt.Errorf("clusteragent: config: the %s section does not verify", name)
	}
	var doc wholeDoc
	if err := json.Unmarshal(payload, &doc); err != nil || doc.Section != name || doc.Node != a.Client.State.NodeUUID || doc.Etag != etag || len(doc.Data) == 0 {
		return nil, fmt.Errorf("clusteragent: config: the %s section is not this node's, or not the one announced", name)
	}
	return &doc, nil
}

// recheckReplica opens and verifies every stored record with the node's
// current keys. syncReplica runs it at the agent's start and whenever those
// keys changed (an enrolment, a re-enrolment, a new panel key), off the
// heartbeat loop: it waits for a running sync and its apply. A whole section
// whose record fails gets its held ETag reset, and a blocklist that fails
// its ETag and seq, so the next config call fetches them again: MAIN would
// otherwise answer unchanged while PHP refuses the stored records.
func (a *Agent) recheckReplica() {
	dir := a.ReplicaDir
	if dir == "" {
		return
	}
	a.replicaMu.Lock()
	defer a.replicaMu.Unlock()
	a.recheckLocked(dir)
	a.replicaKeys = a.replicaKeyPrint()
}

// replicaKeyPrint names the keys the stored records verify under: the
// node's uuid and box key, and the pinned panel key.
func (a *Agent) replicaKeyPrint() string {
	st := a.Client.State
	st.mu.Lock()
	defer st.mu.Unlock()
	h := sha256.New()
	for _, b := range [][]byte{[]byte(st.NodeUUID), st.NodeBoxSk, st.PanelSignPub} {
		h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(b))))
		h.Write(b)
	}
	return string(h.Sum(nil))
}

// recheckLocked is recheckReplica with replicaMu held.
func (a *Agent) recheckLocked(dir string) {
	st := LoadReplicaState(dir)
	changed := false
	for _, name := range WholeSections {
		etag := st.etag(name)
		if etag == "" {
			continue
		}
		sealed, err := os.ReadFile(filepath.Join(dir, name+".rep"))
		if err == nil {
			_, err = a.openWhole(sealed, name, etag)
		}
		if err != nil {
			st.setEtag(name, "")
			changed = true
			a.logf("cluster: replica: the stored %s section no longer verifies; fetching it again", name)
		}
	}
	if st.BlocklistEtag != "" {
		if err := a.verifyBlocklist(dir, st.BlocklistEtag); err != nil {
			st.BlocklistEtag, st.BlocklistSeq, st.FullAt = "", 0, 0
			changed = true
			a.logf("cluster: replica: the stored blocklist no longer verifies; fetching it again")
		}
	}
	if changed {
		if err := a.saveReplicaState(dir, st); err != nil {
			a.logf("cluster: replica: %v", err)
		}
	}
}

// verifyBlocklist opens the stored blocklist section and its deltas.
func (a *Agent) verifyBlocklist(dir, etag string) error {
	sealed, err := os.ReadFile(filepath.Join(dir, "blocklist.rep"))
	if err != nil {
		return err
	}
	payload, err := a.Client.OpenRecord(sealed, "rep")
	if err != nil {
		return err
	}
	var doc wholeDoc
	if err := json.Unmarshal(payload, &doc); err != nil || doc.Section != "blocklist" || doc.Node != a.Client.State.NodeUUID || doc.Etag != etag {
		return errors.New("clusteragent: replica: the stored blocklist is not this node's")
	}
	for _, name := range ReplicaDeltas(dir) {
		b, err := os.ReadFile(filepath.Join(dir, "blocklist.d", name))
		if err != nil {
			return err
		}
		if _, err := a.Client.OpenRecord(b, "blk"); err != nil {
			return err
		}
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

// materialise writes blocklist.json: the stored section with its deltas
// applied in seq order, each opened and verified again.
func (a *Agent) materialise(dir string, st ReplicaState) error {
	sealed, err := os.ReadFile(filepath.Join(dir, "blocklist.rep"))
	if err != nil {
		return err
	}
	payload, err := a.Client.OpenRecord(sealed, "rep")
	if err != nil {
		return err
	}
	var doc struct {
		Node string                     `json:"node"`
		Seq  int64                      `json:"seq"`
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &doc); err != nil || doc.Node != a.Client.State.NodeUUID {
		return errors.New("clusteragent: replica: the stored section is not this node's")
	}
	if doc.Data == nil {
		doc.Data = map[string]json.RawMessage{}
	}
	var ips []string
	if raw, ok := doc.Data["ip"]; ok {
		if err := json.Unmarshal(raw, &ips); err != nil {
			return errors.New("clusteragent: replica: the section's ip list")
		}
	}
	set := make(map[string]bool, len(ips))
	for _, ip := range ips {
		set[ip] = true
	}
	seq := doc.Seq
	for _, name := range ReplicaDeltas(dir) {
		b, err := os.ReadFile(filepath.Join(dir, "blocklist.d", name))
		if err != nil {
			return err
		}
		p, err := a.Client.OpenRecord(b, "blk")
		if err != nil {
			return err
		}
		var d struct {
			Seq    int64    `json:"seq"`
			Add    []string `json:"add"`
			Remove []string `json:"remove"`
		}
		if err := json.Unmarshal(p, &d); err != nil || d.Seq <= seq {
			return errors.New("clusteragent: replica: a stored delta is out of order")
		}
		for _, ip := range d.Remove {
			delete(set, ip)
		}
		for _, ip := range d.Add {
			set[ip] = true
		}
		seq = d.Seq
	}
	ips = ips[:0]
	for ip := range set {
		ips = append(ips, ip)
	}
	sort.Strings(ips)
	ipJSON, _ := json.Marshal(ips)
	doc.Data["ip"] = ipJSON
	out, err := json.Marshal(map[string]any{"seq": seq, "etag": st.BlocklistEtag, "data": doc.Data})
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(dir, "blocklist.json"), out)
}

// ErrNothingToApply is cluster:apply's exit 2: no replica to apply yet.
var ErrNothingToApply = errors.New("cluster:apply: no replica to apply")

// ApplyViaPHP runs the node's `console.php cluster:apply`, which applies the
// materialised replica (shadow diff, or the caches once CONFIG is on). Its
// exit codes: 0 applied or compared, 2 nothing to apply (ErrNothingToApply),
// 3 a part of the report failed; that and any other failure carries the
// output (one JSON line, which PHP keeps free of secrets) to be logged.
func ApplyViaPHP(php, console string, timeout time.Duration) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		out, err := exec.CommandContext(ctx, php, console, "cluster:apply").CombinedOutput()
		var ee *exec.ExitError
		if errors.As(err, &ee) && ee.ExitCode() == 2 {
			return ErrNothingToApply
		}
		if err != nil {
			return fmt.Errorf("cluster:apply: %v: %s", err, bytes.TrimSpace(out))
		}
		return nil
	}
}

// ApplyDebounce is the least time between two runs of cluster:apply: a
// burst of changes (config.changed, a flows change, a sync) runs it once
// more at most a second after the one before.
var ApplyDebounce = time.Second

// runApply runs Apply, one at a time and at most once per ApplyDebounce, and
// logs a failed run with its output. Nothing is fetched again because of it:
// cron:cache applies again every minute while CONFIG is on, and the next
// change runs cluster:apply again.
func (a *Agent) runApply(ctx context.Context) {
	if a.Apply == nil {
		return
	}
	a.applyMu.Lock()
	defer a.applyMu.Unlock()
	if wait := ApplyDebounce - time.Since(a.lastApply); !a.lastApply.IsZero() && wait > 0 && !sleep(ctx, wait) {
		return
	}
	a.lastApply = time.Now()
	if err := a.Apply(ctx); err != nil && !errors.Is(err, ErrNothingToApply) && ctx.Err() == nil {
		a.logf("cluster: replica: apply: %v", err)
	}
}

// FlowConfig is the CONFIG flow bit (MAIN's NodeRegistry::FLOW_CONFIG).
const FlowConfig = 32

// configFlow notes the flows MAIN sent: a change of the CONFIG bit, either
// way, runs cluster:apply, since that is when the caches change hands.
func (a *Agent) configFlow(flows int) {
	now := int64(flows&FlowConfig) + 1
	if old := a.configSeen.Swap(now); old != 0 && old != now && a.ReplicaDir != "" {
		a.kick(a.kickChans().apply)
	}
}

type replicaKicks struct{ sync, apply chan struct{} }

func (a *Agent) kickChans() replicaKicks {
	a.kickOnce.Do(func() {
		a.syncKick, a.applyKick = make(chan struct{}, 1), make(chan struct{}, 1)
	})
	return replicaKicks{a.syncKick, a.applyKick}
}

func (a *Agent) kick(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default: // one is pending already: coalesced
	}
}

// ConfigChanged is the config.changed command: a replica sync at once,
// coalesced with one already running, without waiting for it.
func (a *Agent) ConfigChanged() {
	if a.ReplicaDir != "" {
		a.kick(a.kickChans().sync)
	}
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

// RunReplica keeps the replica current until ctx ends. Its first sync checks
// the stored records against the node's keys (syncReplica), and it runs
// cluster:apply once after that sync (tmp/cache/ does not survive a reboot).
func (a *Agent) RunReplica(ctx context.Context) {
	kicks := a.kickChans()
	for first := true; ; first = false {
		wait := jitter(ReplicaPoll)
		applied, err := a.syncReplica(ctx)
		if err != nil && ctx.Err() == nil {
			if w, ok := busyWait(err); ok {
				// MAIN is busy: ask again when it says, not a minute later.
				wait = w
			}
			if laneRefusal(err) != nil {
				a.busyRefusals.Add(1) // busy, not failing: no error logged
			} else {
				a.logf("cluster: replica: %v (next in %s)", err, wait.Round(time.Second))
			}
		}
		if first && !applied && ctx.Err() == nil {
			a.runApply(ctx)
		}
		if !a.waitReplica(ctx, wait, kicks) {
			return
		}
	}
}

// waitReplica waits for the next sync: its time or config.changed. A change
// of the CONFIG flow meanwhile runs cluster:apply.
func (a *Agent) waitReplica(ctx context.Context, d time.Duration, kicks replicaKicks) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		case <-kicks.sync:
			return true
		case <-kicks.apply:
			a.runApply(ctx)
		}
	}
}
