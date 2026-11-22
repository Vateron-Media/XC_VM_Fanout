package clusteragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Artefacts (ADR 0004, "Artefacts (Phase 4, third increment)", the agent's
// contract): files MAIN grants a node through a signed command, whose
// `args.artefact` names the file by an id, never a path, with its size, its
// SHA-256 and the command's expiry:
//
//	GRANT = {"id", "name", "size", "sha256", "mtime", "ctime", "exp"}
//
// Two commands carry one: `artefact.fetch {artefact}` (an off-air video,
// placed by the node's PHP) and a `node.root` whose args has an `artefact`
// (a custom module's archive, the pinned agent; staged and checked by root).
// The agent checks the grant's shape, downloads the file through the
// `artefact` op beside the command loop (one grant at a time, one request
// in flight), checks its size and SHA-256 itself, and only then hands the
// command to `console.php cluster:exec` as any other; what PHP or root
// answers is acked as today.
//
//   - A command whose artefact downloads is kept in state.json (HeldCmds,
//     with its doc and sig) until it is acked, so it survives a restart,
//     and the long-poll's high-water is raised past it at once: kills and
//     everything else after it go on.
//   - Root refuses a `node.root` whose seq is not above the highest it ran,
//     so root commands go to cluster:exec one at a time in seq order: a
//     `node.root` after one whose artefact downloads waits, kept with it.
//   - The download is config/cluster/artefacts/.<cmd_id>.part (0600, the
//     directory 0700), resumed from its size after a restart, and renamed to
//     <cmd_id> once its size and SHA-256 are the grant's. An artefact.fetch's
//     is removed once cluster:exec returned for it; a node.root's belongs to
//     root from the hand-over (cluster:root removes it), and the agent sweeps
//     only what is left after ArtefactKeep.
//   - The agent says FeatureArtefact at hello only while the node's PHP
//     runs artefact.fetch (`cluster:exec --types`, asked before each hello):
//     MAIN grants nothing to an agent that does not say it.
//
// Chunks, their data and a file's content are never logged: only the
// command's id and the artefact's.

// FeatureArtefact, said at hello, has MAIN grant this node artefacts.
const FeatureArtefact = "artefact"

// TypeArtefactFetch is the command that grants an off-air video.
const TypeArtefactFetch = "artefact.fetch"

var (
	// ArtefactChunk is the largest chunk asked for (MAIN's MAX_CHUNK).
	ArtefactChunk int64 = 4 << 20
	// ArtefactChunkMin is the smallest chunk a timeout halves it to.
	ArtefactChunkMin int64 = 256 << 10
	// ArtefactTimeout is the artefact op's own timeout: a 4 MiB chunk is a
	// reply of about 5.6 MB.
	ArtefactTimeout = 60 * time.Second
	// ArtefactRetryMin and ArtefactRetryMax bound the wait before a chunk
	// is asked for again after a transport error or a 503 DB.
	ArtefactRetryMin = time.Second
	ArtefactRetryMax = 30 * time.Second
	// ArtefactPause is how often a paused download (the node quarantined,
	// its session refused, no token) and an ack not yet through try again.
	ArtefactPause = 10 * time.Second
	// ArtefactIdle is how often the worker looks with nothing held.
	ArtefactIdle = time.Minute
	// ArtefactKeep is how long a download is left for its owner (root)
	// before the agent removes it.
	ArtefactKeep = 25 * time.Hour
)

// Grant is an artefact grant, checked for shape.
type Grant struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	Exp    int64  `json:"exp"`
}

// HeldCmd is a command kept for the artefact worker: one whose artefact
// downloads, or a node.root waiting behind one. It stays in state.json
// until it is acked.
type HeldCmd struct {
	CmdID string `json:"cmd_id"`
	Seq   uint64 `json:"seq"`
	Type  string `json:"type"`
	Exp   int64  `json:"exp"`
	Doc   string `json:"doc"`
	Sig   string `json:"sig"`
	// Grant is the artefact to download before the command is handed on;
	// nil for a node.root that only waits.
	Grant *Grant `json:"grant,omitempty"`
	// Chunk is the chunk length once a timeout halved it (0: ArtefactChunk).
	Chunk int64 `json:"chunk,omitempty"`
	// Done: downloaded and checked, <cmd_id> in place.
	Done bool `json:"done,omitempty"`
	// Handed: its outcome is known (handed on, or the download failed) and
	// only the ack is left, with OK and Result.
	Handed bool   `json:"handed,omitempty"`
	OK     bool   `json:"ok,omitempty"`
	Result []byte `json:"result,omitempty"`
}

var (
	grantNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	cmdIDRe     = regexp.MustCompile(`^[0-9a-f]{32}$`)
	moduleRe    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)
	moduleVerRe = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z._-]{0,31}$`)
	// The off-air names MAIN's `cluster` section carries (ReplicaSections::OFF_AIR).
	offAirNames = map[string]bool{"connected": true, "not_on_air": true, "banned": true, "expired": true, "expiring": true}
)

// validArtefactID reports whether id is one of the kinds MAIN's registry
// serves: offair/<name> and module/<name>/<version>. The agent, the fanout
// daemon and xcvm_core are not among them: every node takes those from their
// GitHub releases itself.
func validArtefactID(id string) bool {
	p := strings.Split(id, "/")
	switch p[0] {
	case "offair":
		return len(p) == 2 && offAirNames[p[1]]
	case "module":
		return len(p) == 3 && moduleRe.MatchString(p[1]) && moduleVerRe.MatchString(p[2]) && !strings.Contains(p[2], "..")
	}
	return false
}

// carriesGrant reports whether a command carries an artefact grant:
// artefact.fetch always, a node.root whose args has an `artefact`.
func carriesGrant(cmd *Command) bool {
	switch cmd.Type {
	case TypeArtefactFetch:
		return true
	case "node.root":
		_, ok := cmd.Args["artefact"]
		return ok
	}
	return false
}

// errMalformedGrant is a grant that is not fetched.
var errMalformedGrant = errors.New("a malformed grant")

// parseGrant checks the grant a command's signed doc carries, and returns
// it with its id as the refusal names it ("?" when it has none).
// mtime, ctime and unknown keys are ignored.
func parseGrant(cmdID, doc string) (*Grant, string, error) {
	var raw struct {
		Args struct {
			Artefact json.RawMessage `json:"artefact"`
		} `json:"args"`
	}
	var f map[string]json.RawMessage
	if json.Unmarshal([]byte(doc), &raw) != nil || json.Unmarshal(raw.Args.Artefact, &f) != nil || f == nil {
		return nil, "?", errMalformedGrant
	}
	var g Grant
	id := "?"
	if json.Unmarshal(f["id"], &g.ID) == nil && g.ID != "" {
		id = g.ID
	}
	integer := func(k string) (int64, bool) {
		n, err := strconv.ParseInt(string(f[k]), 10, 64)
		return n, err == nil
	}
	var sizeOK, expOK bool
	g.Size, sizeOK = integer("size")
	g.Exp, expOK = integer("exp")
	switch {
	case !cmdIDRe.MatchString(cmdID), !validArtefactID(g.ID),
		json.Unmarshal(f["name"], &g.Name) != nil, !grantNameRe.MatchString(g.Name),
		!sizeOK, g.Size < 1,
		json.Unmarshal(f["sha256"], &g.SHA256) != nil, !etagRe.MatchString(g.SHA256),
		!expOK:
		return nil, id, errMalformedGrant
	}
	return &g, id, nil
}

// holdCommand keeps a verified command for the artefact worker when it
// must wait for a download: one that carries a grant (while the agent says
// FeatureArtefact), or a node.root behind a held one. The high-water is
// raised past it as it is kept. refusal is the ack of a malformed grant,
// whose command does not run.
func (a *Agent) holdCommand(cmd *Command, w WireCommand) (held bool, refusal []byte) {
	if a.ArtefactDir == "" || a.run == nil {
		return false, nil
	}
	h := HeldCmd{CmdID: cmd.CmdID, Seq: cmd.Seq, Type: cmd.Type, Exp: cmd.Exp, Doc: w.Doc, Sig: w.Sig}
	st := a.Client.State
	if carriesGrant(cmd) && a.artefactOn.Load() {
		g, id, err := parseGrant(cmd.CmdID, w.Doc)
		if err != nil {
			a.logf("cluster: command %s: artefact %s: %v; not fetched", cmd.CmdID, id, err)
			return false, []byte("artefact refused: " + id + ": " + err.Error())
		}
		h.Grant = g
	} else if cmd.Type != "node.root" || !st.rootHeld() {
		return false, nil
	}
	if err := st.hold(h); err != nil {
		a.logf("cluster: keeping command %s: %v", cmd.CmdID, err)
	}
	a.kickArtefacts()
	return true, nil
}

// hold keeps h and raises the high-water past it, in one save.
func (st *State) hold(h HeldCmd) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, k := range st.HeldCmds {
		if k.CmdID == h.CmdID {
			return nil // a redelivery: never downloaded twice
		}
	}
	st.HeldCmds = append(st.HeldCmds, h)
	sort.Slice(st.HeldCmds, func(i, j int) bool { return st.HeldCmds[i].Seq < st.HeldCmds[j].Seq })
	if h.Seq > st.CmdSeq {
		st.CmdSeq = h.Seq
	}
	return st.saveLocked()
}

// rootHeld reports whether a node.root is kept, not yet handed on.
func (st *State) rootHeld() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, k := range st.HeldCmds {
		if k.Type == "node.root" && !k.Handed {
			return true
		}
	}
	return false
}

// held is a copy of the kept commands, in seq order.
func (st *State) held() []HeldCmd {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]HeldCmd(nil), st.HeldCmds...)
}

// updateHeld changes a kept command and saves; drop removes it instead.
func (st *State) updateHeld(cmdID string, change func(*HeldCmd), drop bool) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := st.HeldCmds[:0]
	for _, k := range st.HeldCmds {
		if k.CmdID == cmdID {
			if drop {
				continue
			}
			change(&k)
		}
		out = append(out, k)
	}
	st.HeldCmds = out
	return st.saveLocked()
}

func (a *Agent) artefactKicks() chan struct{} {
	a.artefactOnce.Do(func() { a.artefactKick = make(chan struct{}, 1) })
	return a.artefactKick
}

// kickArtefacts has the worker look at the kept commands now.
func (a *Agent) kickArtefacts() {
	select {
	case a.artefactKicks() <- struct{}{}:
	default:
	}
}

// RunArtefacts downloads the kept commands' artefacts and hands the
// commands on, until ctx ends.
func (a *Agent) RunArtefacts(ctx context.Context) {
	a.sweepArtefacts()
	swept := time.Now()
	for ctx.Err() == nil {
		wait := a.artefactPass(ctx)
		if time.Since(swept) >= time.Hour {
			a.sweepArtefacts()
			swept = time.Now()
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
		case <-a.artefactKicks():
		case <-t.C:
		}
		t.Stop()
	}
}

// artefactPass goes through the kept commands in seq order once: acks
// what is only left to ack, downloads what is not downloaded (one at a
// time), and hands on what is. A node.root waits while one before it is
// kept. It returns how long until the next pass is due.
func (a *Agent) artefactPass(ctx context.Context) time.Duration {
	wait := ArtefactIdle
	rootWaits, paused := false, false
	for _, h := range a.Client.State.held() {
		if ctx.Err() != nil {
			return wait
		}
		if h.Handed {
			if !a.ackHeld(ctx, h) {
				wait = ArtefactPause
			}
			continue
		}
		root := h.Type == "node.root"
		if root && rootWaits {
			continue
		}
		if h.Grant != nil && !h.Done {
			if paused {
				rootWaits = rootWaits || root
				continue
			}
			switch ok, failed := a.download(ctx, h); {
			case failed != "":
				a.finishHeld(ctx, h, false, []byte(failed))
				continue
			case !ok:
				// Kept for the next pass: the node or its session cannot
				// fetch now (or the agent stops).
				paused, wait = true, ArtefactPause
				rootWaits = rootWaits || root
				continue
			}
		}
		a.handOver(ctx, h)
	}
	return wait
}

// handOver runs a kept command as any other, through cluster:exec, and
// acks what it answered.
func (a *Agent) handOver(ctx context.Context, h HeldCmd) {
	var cmd Command
	if err := json.Unmarshal([]byte(h.Doc), &cmd); err != nil {
		a.finishHeld(ctx, h, false, []byte("refused: unreadable"))
		return
	}
	ok, result := a.run(ctx, &cmd, WireCommand{Doc: h.Doc, Sig: h.Sig, Seq: h.Seq})
	if h.Type == TypeArtefactFetch && h.Grant != nil {
		// cluster:exec removes it once it placed or refused the video; a
		// PHP that refused the command before reading it leaves it.
		os.Remove(filepath.Join(a.ArtefactDir, h.CmdID))
	}
	a.finishHeld(ctx, h, ok, result)
}

// finishHeld records a kept command's outcome, so it is never run twice,
// and acks it.
func (a *Agent) finishHeld(ctx context.Context, h HeldCmd, ok bool, result []byte) {
	if len(result) > MaxResult {
		result = result[:MaxResult]
	}
	h.Handed, h.OK, h.Result = true, ok, result
	if err := a.Client.State.updateHeld(h.CmdID, func(k *HeldCmd) { *k = h }, false); err != nil {
		a.logf("cluster: saving command %s: %v", h.CmdID, err)
	}
	a.ackHeld(ctx, h)
}

// ackHeld acks a kept command whose outcome is known, and forgets it once
// MAIN has the ack, refused it for good, or the command is long expired.
// It reports whether the command is gone.
func (a *Agent) ackHeld(ctx context.Context, h HeldCmd) bool {
	err := a.ack(ctx, h.CmdID, h.OK, h.Result)
	if err != nil && ackAgain(err) && a.Client.MainNowMs()/1000 < h.Exp+int64(ArtefactKeep/time.Second) {
		return false
	}
	if err != nil {
		a.logf("cluster: acking command %s: %v", h.CmdID, err)
	}
	if err := a.Client.State.updateHeld(h.CmdID, nil, true); err != nil {
		a.logf("cluster: saving command %s: %v", h.CmdID, err)
	}
	return true
}

// ackAgain reports whether an ack that failed with err may pass later.
func ackAgain(err error) bool {
	var d *Denial
	if !errors.As(err, &d) {
		return true // transport, no token
	}
	switch d.Reason {
	case "RATE_LIMITED", "STARTING", "REPLAY", "DB", "NOT_ACTIVE", "LICENCE_INVALID", "TOKEN_EXPIRED", "CLOCK":
		return true
	}
	return false
}

// artefactReply is a chunk as the artefact op sends it.
type artefactReply struct {
	Grant  string `json:"grant"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	Data   string `json:"data"`
	EOF    *bool  `json:"eof"`
}

// errBadReply is a chunk that is not the one asked for.
var errBadReply = errors.New("bad reply")

// Artefact asks MAIN for a chunk of a grant's artefact over the bulk
// client, and returns its bytes once they are the chunk asked for.
func (c *Client) Artefact(ctx context.Context, grant *Grant, cmdID string, offset, length int64) ([]byte, error) {
	s, ok := c.current()
	if !ok {
		return nil, ErrNoEpoch
	}
	var raw json.RawMessage
	if err := c.callVia(ctx, c.BulkHTTP, s, "artefact", map[string]any{"grant": cmdID, "offset": offset, "length": length}, &raw, false); err != nil {
		var se *json.SyntaxError
		if errors.As(err, &se) {
			return nil, errBadReply
		}
		return nil, err
	}
	var r artefactReply
	if json.Unmarshal(raw, &r) != nil || r.EOF == nil {
		return nil, errBadReply
	}
	data, err := base64.StdEncoding.DecodeString(r.Data)
	if err != nil || r.Grant != cmdID || r.Offset != offset || r.Length < 1 || r.Length > length || int64(len(data)) != r.Length ||
		r.Size != grant.Size || r.SHA256 != grant.SHA256 || *r.EOF != (offset+r.Length == grant.Size) {
		return nil, errBadReply
	}
	return data, nil
}

// download fetches a kept command's artefact into its .part and, once its
// size and SHA-256 are the grant's, renames it to <cmd_id>. ok: done;
// failed: the download is given up and failed is the ack's result; neither:
// kept for later (the part stays).
func (a *Agent) download(ctx context.Context, h HeldCmd) (ok bool, failed string) {
	g := h.Grant
	fail := func(reason string) string { return "artefact " + g.ID + ": " + reason }
	if err := os.MkdirAll(a.ArtefactDir, 0o700); err != nil {
		a.logf("cluster: artefact %s (command %s): %v", g.ID, h.CmdID, err)
		return false, ""
	}
	os.Chmod(a.ArtefactDir, 0o700)
	part := filepath.Join(a.ArtefactDir, "."+h.CmdID+".part")
	f, err := os.OpenFile(part, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		a.logf("cluster: artefact %s (command %s): %v", g.ID, h.CmdID, err)
		return false, ""
	}
	give := func(reason string) (bool, string) {
		f.Close()
		os.Remove(part)
		a.logf("cluster: artefact %s (command %s): %s; not run", g.ID, h.CmdID, reason)
		return false, reason
	}
	fi, err := f.Stat()
	if err != nil || f.Chmod(0o600) != nil {
		f.Close()
		return false, ""
	}
	off := fi.Size()
	if off > g.Size {
		// Longer than the grant: started over.
		if err := f.Truncate(0); err != nil {
			f.Close()
			return false, ""
		}
		off = 0
	}
	if off < g.Size {
		a.logf("cluster: artefact %s (command %s): fetching %d bytes from %d", g.ID, h.CmdID, g.Size, off)
	}
	chunk := h.Chunk
	if chunk <= 0 {
		chunk = ArtefactChunk
	}
	backoff := ArtefactRetryMin
	for off < g.Size {
		if g.Exp <= a.Client.MainNowMs()/1000 {
			return give(fail("expired"))
		}
		data, err := a.Client.Artefact(ctx, g, h.CmdID, off, min(chunk, g.Size-off))
		if err == nil {
			if _, err := f.WriteAt(data, off); err != nil {
				a.logf("cluster: artefact %s (command %s): writing the download: %v", g.ID, h.CmdID, err)
				f.Close()
				return false, ""
			}
			off += int64(len(data))
			backoff = ArtefactRetryMin
			continue
		}
		if ctx.Err() != nil {
			f.Close()
			return false, ""
		}
		var d *Denial
		var ne net.Error
		switch {
		case errors.Is(err, errBadReply):
			return give(fail("bad reply"))
		case errors.Is(err, ErrNoEpoch):
			f.Close()
			return false, ""
		case laneRefusal(err) != nil:
			// MAIN's bulk lane is busy, not failing: counted, never logged.
			a.busyRefusals.Add(1)
			w, _ := busyWait(err)
			if !sleep(ctx, w) {
				f.Close()
				return false, ""
			}
			continue
		case errors.As(err, &d):
			if w, busy := busyWait(err); busy {
				if !sleep(ctx, w) {
					f.Close()
					return false, ""
				}
				continue
			}
			if w, again := replayWait(d); again {
				if d.MainTimeMs > 0 {
					a.Client.setMainTime(d.MainTimeMs)
				}
				if !sleep(ctx, w) {
					f.Close()
					return false, ""
				}
				continue
			}
			switch d.Reason {
			case "GRANT_INVALID", "ARTEFACT_CHANGED", "BAD_RANGE", "BAD_REQUEST", "UNKNOWN_OP":
				return give(fail(d.Reason))
			case "NOT_ACTIVE", "LICENCE_INVALID", "NODE_REVOKED", "TOKEN_EXPIRED", "CLOCK":
				// As for any op: resumed once the node is active and its
				// session works again, while the grant lives.
				a.logf("cluster: artefact %s (command %s): %v; resumed later", g.ID, h.CmdID, err)
				f.Close()
				return false, ""
			}
		case errors.As(err, &ne) && ne.Timeout():
			// A slow link: the same offset again, in smaller chunks for the
			// rest of this download.
			if half := max(ArtefactChunkMin, chunk/2); half < chunk {
				chunk = half
				a.Client.State.updateHeld(h.CmdID, func(k *HeldCmd) { k.Chunk = chunk }, false)
			}
		}
		// A transport error, an unsigned 429, 502 or 504, 503 DB.
		a.logf("cluster: artefact %s (command %s): %v (again in %s)", g.ID, h.CmdID, err, backoff)
		if !sleep(ctx, backoff) {
			f.Close()
			return false, ""
		}
		backoff = min(backoff*2, ArtefactRetryMax)
	}
	// Our own check, before anything runs the command.
	if err := f.Sync(); err != nil {
		f.Close()
		return false, ""
	}
	sum := sha256.New()
	n, err := io.Copy(sum, io.NewSectionReader(f, 0, g.Size+1))
	if err != nil {
		f.Close()
		return false, ""
	}
	switch {
	case n != g.Size:
		return give("artefact refused: " + g.ID + " (" + g.Name + "): size mismatch")
	case hex.EncodeToString(sum.Sum(nil)) != g.SHA256:
		f.Close()
		os.Remove(part)
		a.logf("cluster: artefact %s: sha256 mismatch (command %s); not run", g.ID, h.CmdID)
		return false, "artefact refused: " + g.ID + " (" + g.Name + "): sha256 mismatch"
	}
	if err := f.Close(); err != nil {
		return false, ""
	}
	final := filepath.Join(a.ArtefactDir, h.CmdID)
	if err := os.Rename(part, final); err != nil {
		a.logf("cluster: artefact %s (command %s): %v", g.ID, h.CmdID, err)
		return false, ""
	}
	os.Chmod(final, 0o600)
	if d, err := os.Open(a.ArtefactDir); err == nil {
		d.Sync()
		d.Close()
	}
	if err := a.Client.State.updateHeld(h.CmdID, func(k *HeldCmd) { k.Done = true }, false); err != nil {
		a.logf("cluster: saving command %s: %v", h.CmdID, err)
	}
	a.logf("cluster: artefact %s (command %s): downloaded and checked", g.ID, h.CmdID)
	return true, ""
}

// sweepArtefacts removes what is left in the artefacts directory: a part no
// kept command downloads, and anything else after ArtefactKeep (a node.root's
// download is root's until then).
func (a *Agent) sweepArtefacts() {
	if a.ArtefactDir == "" {
		return
	}
	entries, err := os.ReadDir(a.ArtefactDir)
	if err != nil {
		return
	}
	kept := map[string]bool{}
	for _, h := range a.Client.State.held() {
		if !h.Handed {
			kept[h.CmdID] = true
		}
	}
	for _, e := range entries {
		name := e.Name()
		id := strings.TrimSuffix(strings.TrimPrefix(name, "."), ".part")
		if kept[id] {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		part := strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".part")
		if part || time.Since(info.ModTime()) >= ArtefactKeep {
			os.RemoveAll(filepath.Join(a.ArtefactDir, name))
		}
	}
}

// FeatureTypedStarts: the node's PHP runs stream.start and vod.start, so
// MAIN sends a start typed rather than as node.rpc (XC_VM's ClusterRoute).
const FeatureTypedStarts = "typed_starts"

// checkTypes asks the node's PHP which command types it runs, and says
// FeatureTypedStarts at the next hello while stream.start is one, and
// FeatureArtefact while artefact.fetch is.
func (a *Agent) checkTypes(ctx context.Context) {
	if a.Types == nil {
		return
	}
	types, err := a.Types(ctx)
	a.typedStarts.Store(err == nil && slices.Contains(types, "stream.start"))
	if a.ArtefactDir == "" {
		return
	}
	on := err == nil && slices.Contains(types, TypeArtefactFetch)
	if a.artefactOn.Swap(on) != on || (!on && !a.typesSeen.Swap(true)) {
		if on {
			a.logf("cluster: the node's PHP runs artefact.fetch: artefacts on")
		} else if err != nil {
			a.logf("cluster: the node's PHP does not list its command types (%v): artefacts off", err)
		} else {
			a.logf("cluster: the node's PHP does not run artefact.fetch: artefacts off")
		}
	}
}

// TypesViaPHP asks `console.php cluster:exec --types` which command types
// the node's PHP runs, as commands are run (same PHP, same timeout, stdin
// closed). A PHP from before the option reads the empty stdin as a command
// and exits 2.
func TypesViaPHP(php, console string, timeout time.Duration) func(ctx context.Context) ([]string, error) {
	return func(ctx context.Context) ([]string, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		c := exec.CommandContext(ctx, php, console, "cluster:exec", "--types")
		var out, errOut limitedBuffer
		out.max, errOut.max = 64<<10, 4096
		c.Stdout, c.Stderr = &out, &errOut
		if err := c.Run(); err != nil {
			return nil, fmt.Errorf("cluster:exec --types: %v: %s", err, bytes.TrimSpace(errOut.Bytes()))
		}
		var types []string
		if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &types); err != nil {
			return nil, errors.New("cluster:exec --types: not a list of types")
		}
		return types, nil
	}
}
