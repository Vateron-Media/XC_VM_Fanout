package clusteragent

import (
	"bytes"
	"context"
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
// This increment hands every command to the node's PHP (`console.php
// cluster:exec`), which verifies it again and runs it with the legacy
// handlers; root actions are not carried yet.

// Command is a signed command document.
type Command struct {
	V         int            `json:"v"`
	Type      string         `json:"type"`
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

// handleCommand verifies, runs and acknowledges one command, and raises the
// high-water. A command that fails its checks is acknowledged as refused.
func (a *Agent) handleCommand(ctx context.Context, w WireCommand, run Executor) {
	c := a.Client
	cmd, err := c.verifyCommand(w)
	ok, result := false, []byte(nil)
	var id string
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
			return // already handled: a redelivery
		}
		w.Seq = probe.Seq
	} else {
		id = cmd.CmdID
		ok, result = run(ctx, cmd, w)
		w.Seq = cmd.Seq
	}
	c.State.mu.Lock()
	if w.Seq > c.State.CmdSeq {
		c.State.CmdSeq = w.Seq
	}
	serr := c.State.saveLocked()
	c.State.mu.Unlock()
	if serr != nil {
		a.logf("cluster: saving command high-water: %v", serr)
	}
	if id == "" {
		return
	}
	for attempt := 0; attempt < 3; attempt++ {
		var r struct {
			OK bool `json:"ok"`
		}
		err := c.Call(ctx, "ack", map[string]any{"cmd_id": id, "ok": ok, "result": string(result)}, &r, false)
		if err == nil {
			return
		}
		if fatal(err) || ctx.Err() != nil {
			return
		}
		if !sleep(ctx, time.Duration(attempt+1)*time.Second) {
			return
		}
	}
}

// RunCommands keeps a commands long-poll open until ctx ends.
func (a *Agent) RunCommands(ctx context.Context, run Executor) {
	backoff := time.Second
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
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		for _, w := range cmds {
			a.handleCommand(ctx, w, run)
		}
	}
}
