package clusteragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// Commands (plan, section 7, Phase 4): MAIN queues typed commands for the
// node, each panel-signed with tag "cmd". The agent holds a long-poll on
// `commands`, checks every command before acting on it — the panel's
// signature under the pinned key, this node's uuid and generation, a seq
// above the node's high-water, not expired on MAIN's clock — runs it and
// `ack`s it with the result. Delivery is at least once; the high-water,
// persisted before the ack, keeps a replay from running a command twice.
//
// Every command goes to the node's PHP (`console.php cluster:exec`), which
// verifies it again and runs it with the legacy handlers, or hands a
// node.root to root. A command that carries an artefact grant waits for its
// download beside the loop first (artefact.go).

// Command is a signed command document. Action is set for node.rpc and
// node.root only, at the top level of the document (never among Args):
// xcvm_core classes a command by its type and that action
// (clustercrypto/testdata/cluster_commands.json, its registry).
type Command struct {
	V         int            `json:"v"`
	Type      string         `json:"type"`
	Action    string         `json:"action,omitempty"`
	Exp       int64          `json:"exp"`
	Iat       int64          `json:"iat"`
	CmdID     string         `json:"cmd_id"`
	Seq       uint64         `json:"seq"`
	NodeUUID  string         `json:"node_uuid"`
	Gen       int64          `json:"gen"`
	DedupeKey *string        `json:"dedupe_key"`
	Args      map[string]any `json:"args"`
}

// WireCommand is a command as MAIN sends it: the signed JSON and its signature.
type WireCommand struct {
	Doc string `json:"doc"`
	Sig string `json:"sig"`
	Seq uint64 `json:"seq"`
}

// Executor runs one verified command and returns its outcome.
type Executor func(ctx context.Context, cmd *Command, wire WireCommand) (ok bool, result []byte)

// MaxResult caps a command's result sent back in its ack.
const MaxResult = 64 << 10

// TypeRotateNow is the one command the agent runs itself: the token it would
// rotate is the agent's, and the node's PHP has no idea what it is.
const TypeRotateNow = "token.rotate_now"

// CommandsWait is how long MAIN holds a commands poll with nothing to send.
var CommandsWait = 20 * time.Second

// CommandsIdle is how often a node without the COMMANDS flow looks again.
var CommandsIdle = 2 * time.Second

// FlowCommands is the COMMANDS flow bit (MAIN's NodeRegistry::FLOW_COMMANDS).
const FlowCommands = 2

// ExecViaPHP runs commands through the node's `console.php cluster:exec`.
func ExecViaPHP(php, console string, timeout time.Duration) Executor {
	return func(ctx context.Context, cmd *Command, wire WireCommand) (bool, []byte) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		in, _ := json.Marshal(map[string]string{"doc": wire.Doc, "sig": wire.Sig})
		c := exec.CommandContext(ctx, php, console, "cluster:exec")
		c.Stdin = bytes.NewReader(in)
		var out, errOut limitedBuffer
		out.max, errOut.max = MaxResult, 4096
		c.Stdout, c.Stderr = &out, &errOut
		if err := c.Run(); err != nil {
			return false, []byte(fmt.Sprintf("cluster:exec: %v: %s", err, bytes.TrimSpace(errOut.Bytes())))
		}
		return true, out.Bytes()
	}
}

type limitedBuffer struct {
	bytes.Buffer
	max int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := b.max - b.Len(); room > 0 {
		b.Buffer.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

// verifyCommand checks a command for this node; the error says why not.
func (c *Client) verifyCommand(w WireCommand) (*Command, error) {
	sig, err := base64.RawURLEncoding.DecodeString(w.Sig)
	if err != nil || !cc.VerifyPanel(c.State.PanelSignPub, "cmd", []byte(w.Doc), sig) {
		return nil, errors.New("bad signature")
	}
	var cmd Command
	if err := json.Unmarshal([]byte(w.Doc), &cmd); err != nil {
		return nil, errors.New("unreadable")
	}
	c.State.mu.Lock()
	high := c.State.CmdSeq
	c.State.mu.Unlock()
	tok, ok := c.Current()
	switch {
	case cmd.NodeUUID != c.State.NodeUUID:
		return nil, errors.New("for another node")
	case ok && cmd.Gen != tok.Gen:
		return nil, fmt.Errorf("for generation %d, this node is %d", cmd.Gen, tok.Gen)
	case cmd.Seq <= high:
		return nil, fmt.Errorf("seq %d not above %d", cmd.Seq, high)
	case cmd.Exp <= c.MainNowMs()/1000:
		return nil, errors.New("expired")
	}
	return &cmd, nil
}

// PollCommands asks MAIN for commands after the node's high-water, holding
// up to wait when there are none.
func (c *Client) PollCommands(ctx context.Context, wait time.Duration) ([]WireCommand, error) {
	c.State.mu.Lock()
	after := c.State.CmdSeq
	c.State.mu.Unlock()
	var reply struct {
		Commands []WireCommand `json:"commands"`
	}
	ctx, cancel := context.WithTimeout(ctx, wait+15*time.Second)
	defer cancel()
	if err := c.Call(ctx, "commands", map[string]any{"after_seq": after, "wait_ms": wait.Milliseconds()}, &reply, false); err != nil {
		return nil, err
	}
	return reply.Commands, nil
}

// fate is what became of one command handleCommand was handed.
type fate int

const (
	// cmdTaken: the command left the agent's queue on MAIN (the high-water
	// went past it, or MAIN took its refusal).
	cmdTaken fate = iota
	// cmdStuck: MAIN will keep handing it out whatever the agent does (no
	// cmd_id to ack it by, or an ack MAIN refused for good).
	cmdStuck
	// cmdUnacked: its refusal's ack failed for now (MAIN down, busy or
	// restarting): ack it again on a later delivery.
	cmdUnacked
)

// handleCommand verifies, runs and acknowledges one command, and raises the
// high-water. A command that fails its checks is acknowledged as refused, by
// its cmd_id, and leaves the high-water where it was: its seq is only what
// an unverified document claims, and one near 2^64 would otherwise stop
// every later command for good (MAIN takes a refused command out of the
// queue on its ack, so it is not handed out again).
//
// It reports what became of the command: one not taken is handed out again
// on the next poll, at once (RunCommands).
func (a *Agent) handleCommand(ctx context.Context, w WireCommand, run Executor) fate {
	c := a.Client
	cmd, err := c.verifyCommand(w)
	ok, result := false, []byte(nil)
	var id string
	var seq uint64 // the high-water to raise to: a verified command's only
	if err != nil {
		var probe struct {
			CmdID string `json:"cmd_id"`
			Seq   uint64 `json:"seq"`
		}
		json.Unmarshal([]byte(w.Doc), &probe)
		id, result = probe.CmdID, []byte("refused: "+err.Error())
		a.logf("cluster: command %s refused: %v", probe.CmdID, err)
		c.State.mu.Lock()
		high := c.State.CmdSeq
		c.State.mu.Unlock()
		if probe.Seq <= high {
			return cmdStuck // already handled: a redelivery
		}
	} else if k, done := c.State.kept(cmd.CmdID); done {
		// Run already from a LICENCE_INVALID denial (sealed.go): ack its
		// result, never run it twice.
		id, ok, result = cmd.CmdID, k.OK, k.Result
		seq = cmd.Seq
	} else if held, refusal := a.holdCommand(cmd, w); held {
		// Its artefact downloads beside this loop (artefact.go): kept, with
		// the high-water past it, and acked once it is handed on.
		return cmdTaken
	} else if refusal != nil {
		id, result = cmd.CmdID, refusal
		seq = cmd.Seq
	} else if cmd.Type == TypeRotateNow {
		// The agent's own: the token lives here, not in the node's PHP, which
		// would refuse the type. An operator asking for a rotation wants it
		// before the refresh window would have come round.
		id, ok, result = cmd.CmdID, true, []byte("rotating")
		seq = cmd.Seq
		a.refreshLater(ctx)
	} else {
		id = cmd.CmdID
		ok, result = run(ctx, cmd, w)
		seq = cmd.Seq
	}
	c.State.mu.Lock()
	if seq > c.State.CmdSeq {
		c.State.CmdSeq = seq
	}
	serr := c.State.saveLocked()
	c.State.mu.Unlock()
	if serr != nil {
		a.logf("cluster: saving command high-water: %v", serr)
	}
	if id == "" {
		if seq > 0 {
			return cmdTaken
		}
		return cmdStuck
	}
	aerr := a.ack(ctx, id, ok, result)
	if aerr == nil || seq > 0 {
		return cmdTaken
	}
	a.logf("cluster: command %s: refusal not acked: %v", id, aerr)
	if ackRefused(aerr) {
		return cmdStuck
	}
	return cmdUnacked
}

// ackRefused is MAIN refusing an ack for good: a verified BAD_REQUEST (a
// cmd_id it holds no command under for this node) or an UNKNOWN_OP. Any
// other failure (no answer, a 5xx, a session to redo) may pass.
func ackRefused(err error) bool {
	var d *Denial
	return errors.As(err, &d) && (d.Reason == "BAD_REQUEST" || d.Reason == "UNKNOWN_OP")
}

// ack sends a command's outcome, trying three times; it returns the last
// error.
func (a *Agent) ack(ctx context.Context, id string, ok bool, result []byte) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		var r struct {
			OK bool `json:"ok"`
		}
		err = a.Client.Call(ctx, "ack", map[string]any{"cmd_id": id, "ok": ok, "result": string(result)}, &r, false)
		if err == nil || fatal(err) || ackRefused(err) || ctx.Err() != nil {
			return err
		}
		if attempt == 2 || !sleep(ctx, time.Duration(attempt+1)*time.Second) {
			return err
		}
	}
	return err
}

// RunCommands keeps a commands long-poll open until ctx ends.
//
// A refused command MAIN keeps handing out is remembered by its document and
// skipped: for good when nothing the agent does will take it off the queue
// (cmdStuck), until a later try when only its refusal's ack failed, MAIN
// being down or busy for now (cmdUnacked: acked again then, backing off from
// RefusalAckRetry to a minute). A poll that brings nothing the agent could
// take pauses, up to CommandsStuck, before the next: MAIN answers at once
// while such a row sits above the high-water, and the loop would otherwise
// spin against it, one PHP worker a request, for as long as the row's own
// exp says.
func (a *Agent) RunCommands(ctx context.Context, run Executor) {
	backoff, stuck := time.Second, time.Duration(0)
	type skipped struct {
		until time.Time // zero: for good
		tries int
	}
	skip := map[[sha256.Size]byte]skipped{}
	for ctx.Err() == nil {
		// Only a node whose COMMANDS flow is on holds a poll open on MAIN
		// (each one holds a PHP worker there).
		if a.flows.Load()&FlowCommands == 0 {
			if !sleep(ctx, CommandsIdle) {
				return
			}
			continue
		}
		cmds, err := a.Client.PollCommands(ctx, CommandsWait)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !errors.Is(err, ErrNoEpoch) {
				a.logf("cluster: commands: %v", err)
			}
			if w, ok := busyWait(err); ok {
				// MAIN is starting: poll again when it says.
				if !sleep(ctx, w) {
					return
				}
				continue
			}
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		taken := len(cmds) == 0 // an empty reply is a long-poll that ran out
		for _, w := range cmds {
			key := sha256.Sum256([]byte(w.Doc))
			sk, seen := skip[key]
			if seen && (sk.until.IsZero() || time.Now().Before(sk.until)) {
				continue
			}
			switch a.handleCommand(ctx, w, run) {
			case cmdTaken:
				delete(skip, key)
				taken = true
				continue
			case cmdStuck:
				sk = skipped{}
			case cmdUnacked:
				sk.until = time.Now().Add(min(RefusalAckRetry<<min(sk.tries, 10), time.Minute))
				sk.tries++
			}
			if !seen && len(skip) >= maxSkipped {
				clear(skip)
			}
			skip[key] = sk
		}
		if taken {
			stuck = 0
			continue
		}
		stuck = min(max(2*stuck, 250*time.Millisecond), CommandsStuck)
		if !sleep(ctx, stuck) {
			return
		}
	}
}

// CommandsStuck caps the pause between polls that bring only refused
// commands MAIN keeps handing out (RunCommands): a genuine command queued
// behind such a row waits at most this long.
var CommandsStuck = 5 * time.Second

// RefusalAckRetry is how long RunCommands first waits before acking again a
// refusal whose ack failed for now; each failure doubles it, up to a minute.
var RefusalAckRetry = 2 * time.Second

// maxSkipped bounds RunCommands' memory of the refused commands it skips.
const maxSkipped = 1024
