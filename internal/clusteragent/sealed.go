package clusteragent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// Kills while the licence is gone (ADR 0004, "Acceptance tests that found
// gaps", "Kills in the hard revocation mode"): with lb_revocation_mode=hard
// and no licence, MAIN refuses the node's session, so the long-poll and every
// MAC'd reply stop. Its pending restrictive commands (kills, drops, stops)
// then ride the panel-signed LICENCE_INVALID the node's next request gets:
//
//	{reason: "LICENCE_INVALID", commands_sealed: base64(SEAL(JSON [{doc, sig, seq}, …]))}
//
// sealed to the node's box key, purpose "commands", the node uuid as context,
// in the long-poll's shape. The agent opens the list, checks each command as
// on the long-poll (the panel's signature, this node, its generation, a seq
// above the high-water, not expired) and runs it, but does not raise its
// long-poll high-water: a granting command queued below a kill (an RPC, a
// root command) would otherwise never be handed out once the licence is
// back. It keeps the cmd_ids it ran, with their results, until they expire,
// and acks them once the session works again; a kept command the long-poll
// hands out again is acked with its result, not run twice.
//
// While the licence is gone and the node still holds a token, the heartbeat
// loop keeps going (each LICENCE_INVALID may carry kills) instead of re-keying:
// a re-key needs a licence too. The contents are never logged.

// SealCommands is the SEAL purpose of the commands a LICENCE_INVALID carries.
const SealCommands = "commands"

// SealedCmd is a command the agent ran from a LICENCE_INVALID denial.
type SealedCmd struct {
	CmdID  string `json:"cmd_id"`
	Seq    uint64 `json:"seq"`
	Exp    int64  `json:"exp"`
	OK     bool   `json:"ok"`
	Result []byte `json:"result,omitempty"`
	Acked  bool   `json:"acked,omitempty"`
}

// openSealedCommands opens a denial's commands_sealed for this node.
func (c *Client) openSealedCommands(b64 string) ([]WireCommand, error) {
	sealed, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, errors.New("clusteragent: commands_sealed is not base64")
	}
	c.State.mu.Lock()
	sk, uuid := c.State.NodeBoxSk, c.State.NodeUUID
	c.State.mu.Unlock()
	plain, err := cc.Open(sk, SealCommands, uuid, sealed)
	if err != nil {
		return nil, errors.New("clusteragent: commands_sealed does not open for this node")
	}
	var cmds []WireCommand
	if err := json.Unmarshal(plain, &cmds); err != nil {
		return nil, errors.New("clusteragent: commands_sealed is not a command list")
	}
	return cmds, nil
}

// kept returns the command run from a denial under cmdID, if any.
func (st *State) kept(cmdID string) (SealedCmd, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, k := range st.SealedCmds {
		if k.CmdID == cmdID {
			return k, true
		}
	}
	return SealedCmd{}, false
}

// keepSealed records the commands just run and drops the expired ones.
func (st *State) keepSealed(nowS int64, add []SealedCmd, acked []string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	done := map[string]bool{}
	for _, id := range acked {
		done[id] = true
	}
	out := st.SealedCmds[:0]
	for _, k := range st.SealedCmds {
		if k.Exp <= nowS {
			continue
		}
		if done[k.CmdID] {
			k.Acked = true
		}
		out = append(out, k)
	}
	st.SealedCmds = append(out, add...)
	return st.saveLocked()
}

// takeSealed runs the commands a LICENCE_INVALID denial carries. Only one
// runs at a time.
func (a *Agent) takeSealed(ctx context.Context, d *Denial) {
	if d == nil || d.Reason != "LICENCE_INVALID" || d.CommandsSealed == "" || a.run == nil {
		return
	}
	a.sealedMu.Lock()
	defer a.sealedMu.Unlock()
	// A LICENCE_INVALID is MAIN refusing the session: a licence fence among
	// these commands is taken (fence.go).
	a.setFenced(true)
	c := a.Client
	cmds, err := c.openSealedCommands(d.CommandsSealed)
	if err != nil {
		a.logf("cluster: licence gone: %v", err)
		return
	}
	var ran []SealedCmd
	refused := 0
	for _, w := range cmds {
		cmd, err := c.verifyCommand(w)
		if err != nil {
			refused++
			continue
		}
		if _, done := c.State.kept(cmd.CmdID); done {
			continue
		}
		ok, result := a.run(ctx, cmd, w)
		if len(result) > MaxResult {
			result = result[:MaxResult]
		}
		ran = append(ran, SealedCmd{CmdID: cmd.CmdID, Seq: cmd.Seq, Exp: cmd.Exp, OK: ok, Result: result})
	}
	if len(ran) > 0 {
		if err := c.State.keepSealed(c.MainNowMs()/1000, ran, nil); err != nil {
			a.logf("cluster: saving the commands run while the licence is gone: %v", err)
		}
	}
	if len(ran) > 0 || refused > 0 {
		a.logf("cluster: licence gone: ran %d restrictive command(s) MAIN sent with its refusal (%d refused)", len(ran), refused)
	}
}

// ackSealed acks the commands run from denials once the session works again.
func (a *Agent) ackSealed(ctx context.Context) {
	c := a.Client
	c.State.mu.Lock()
	var todo []SealedCmd
	for _, k := range c.State.SealedCmds {
		if !k.Acked {
			todo = append(todo, k)
		}
	}
	c.State.mu.Unlock()
	if len(todo) == 0 {
		return
	}
	var acked []string
	for _, k := range todo {
		var r struct {
			OK bool `json:"ok"`
		}
		if err := c.Call(ctx, "ack", map[string]any{"cmd_id": k.CmdID, "ok": k.OK, "result": string(k.Result)}, &r, false); err != nil {
			a.logf("cluster: acking a command run while the licence was gone: %v", err)
			break
		}
		acked = append(acked, k.CmdID)
	}
	if err := c.State.keepSealed(c.MainNowMs()/1000, nil, acked); err != nil {
		a.logf("cluster: saving acked commands: %v", err)
	}
}

// licenceGone reports whether err is MAIN refusing the session for want of a
// licence while the node still holds a token: the node keeps heartbeating
// (FENCED) rather than re-keying.
func (a *Agent) licenceGone(err error) bool {
	var d *Denial
	if !errors.As(err, &d) || d.Reason != "LICENCE_INVALID" {
		return false
	}
	_, ok := a.Client.Current()
	return ok
}
