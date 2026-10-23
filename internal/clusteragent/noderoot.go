package clusteragent

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

// node.root in Go (P6): on a PHP-free serve-only node the agent executes the
// essential node-management commands itself (artefact.go already verified them —
// Ed25519 + a monotonic seq pin), replacing console.php cluster:exec. The rest
// are deferred to P5 and dropped (audited). The receiving/verification is
// unchanged; only the execution moves. See
// docs/superpowers/plans/2026-10-23-lb-p6-install-php-free.md.

// nodeRootRunner runs one privileged command.
type nodeRootRunner func(ctx context.Context, name string, args ...string) error

// nodeRootRun runs one privileged command; a test seam. The agent's service runs
// as root, so no sudo is needed. A self-stopping command captures it into a local
// before its goroutine fires, so a test swapping this global never races it.
var nodeRootRun nodeRootRunner = func(ctx context.Context, name string, args ...string) error {
	return exec.CommandContext(ctx, name, args...).Run()
}

// nodeRootStopDelay is how long a self-stopping command waits before firing, so
// its ack is persisted first. A test sets it to 0.
var nodeRootStopDelay = time.Second

// nodeRootEssential are the node.root actions a PHP-free node runs in Go. B1
// ports the lifecycle commands; B2 adds reload_nginx/flush/ramdisk/sysctl/governor.
var nodeRootEssential = map[string]bool{
	"reboot": true, "restart_services": true, "stop_services": true,
}

// nodeRootSelfStopping are the actions that stop the agent itself: their ack is
// persisted before the shell fires, so a respawn does not repeat them.
var nodeRootSelfStopping = map[string]bool{
	"reboot": true, "restart_services": true, "stop_services": true,
}

// execNodeRoot runs one essential action's privileged shell (ports the
// RootSignalsCronJob case). The agent runs as root; the PHP's DB syslog line is
// dropped (a serve-only node has no DB — the agent logs instead).
func execNodeRoot(ctx context.Context, run nodeRootRunner, action string, _ map[string]any) error {
	switch action {
	case "reboot":
		return run(ctx, "reboot")
	case "stop_services":
		return run(ctx, "systemctl", "stop", "xc_vm")
	case "restart_services":
		if err := run(ctx, "systemctl", "stop", "xc_vm"); err != nil {
			return err
		}
		return run(ctx, "systemctl", "start", "xc_vm")
	}
	return fmt.Errorf("node.root: no Go handler for %s", action)
}

// execNodeRootCmd runs a verified node.root command on a PHP-free node and is
// always acked (handled). A self-stopping action fires just after we return, so
// finishHeld/ack persists first and a respawn does not repeat it. A non-essential
// action is dropped (audited) — deferred to P5.
func (a *Agent) execNodeRootCmd(ctx context.Context, cmd *Command) (bool, []byte) {
	if !nodeRootEssential[cmd.Action] {
		a.logf("cluster: node.root %s not run on this PHP-free node (deferred to P5)", cmd.Action)
		return true, []byte(`{"result":true,"detail":"deferred"}`)
	}
	run, delay := nodeRootRun, nodeRootStopDelay // captured here (no global read in the goroutine)
	if nodeRootSelfStopping[cmd.Action] {
		action, args := cmd.Action, cmd.Args
		go func() {
			time.Sleep(delay)
			if err := execNodeRoot(context.Background(), run, action, args); err != nil {
				a.logf("cluster: node.root %s: %v", action, err)
			}
		}()
		return true, []byte(`{"result":true,"detail":"issued"}`)
	}
	if err := execNodeRoot(ctx, run, cmd.Action, cmd.Args); err != nil {
		return false, []byte(err.Error())
	}
	return true, []byte(`{"result":true}`)
}

// DropExec acks (handled) any command that reaches it on a PHP-free node with no
// cluster:exec. node.root is intercepted before this (localExec); this catches
// the rest (node.rpc, artefact.fetch, …) so a missing console.php never panics.
func DropExec(_ context.Context, _ *Command, _ WireCommand) (bool, []byte) {
	return true, []byte(`{"result":true,"detail":"no PHP on this node"}`)
}
