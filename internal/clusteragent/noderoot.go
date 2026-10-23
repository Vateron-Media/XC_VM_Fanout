package clusteragent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
// ported the lifecycle commands; P5a adds reload_nginx + ramdisk + set_sysctl;
// P5c adds flush (empty the viewer-ban ipsets, bansync.go). set_governor (cpufreq
// output capture) and set_port (the Go front's port rebind) stay deferred to P5b.
var nodeRootEssential = map[string]bool{
	"reboot": true, "restart_services": true, "stop_services": true,
	"reload_nginx": true, "enable_ramdisk": true, "disable_ramdisk": true, "set_sysctl": true,
	"flush": true,
}

// nodeRootSelfStopping are the actions that stop the agent itself: their ack is
// persisted before the shell fires, so a respawn does not repeat them.
var nodeRootSelfStopping = map[string]bool{
	"reboot": true, "restart_services": true, "stop_services": true,
}

// execNodeRoot runs one essential action's privileged shell (ports the
// RootSignalsCronJob case). The agent runs as root; the PHP's DB syslog line is
// dropped (a serve-only node has no DB — the agent logs instead).
func execNodeRoot(ctx context.Context, run nodeRootRunner, home, action string, args map[string]any) error {
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
	case "reload_nginx":
		// http-nginx is gone on a serve-only node; reload nginx_rtmp only (the
		// RTMP byte server stays), as xc_vm — matching `service reload`.
		return run(ctx, "sudo", "-u", "xc_vm", filepath.Join(home, "bin/nginx_rtmp/sbin/nginx_rtmp"), "-s", "reload")
	case "enable_ramdisk":
		return setRamdisk(ctx, run, home, true)
	case "disable_ramdisk":
		return setRamdisk(ctx, run, home, false)
	case "set_sysctl":
		return setSysctl(ctx, run, args)
	}
	return fmt.Errorf("node.root: no Go handler for %s", action)
}

// ramdiskMount is the fstab line the installers write for the streams tmpfs,
// matching RootSignalsCronJob::RAMDISK_MOUNT exactly; the toggle keys on it.
const ramdiskMount = "tmpfs /home/xc_vm/content/streams"

// fstabPath and sysctlPath are the files the ramdisk/sysctl actions edit;
// overridable in tests.
var (
	fstabPath  = "/etc/fstab"
	sysctlPath = "/etc/sysctl.conf"
)

// ramdiskFstab toggles the streams tmpfs line on/off, a port of
// RootSignalsCronJob::ramdiskFstab: enabling uncomments "#tmpfs …", disabling
// comments "tmpfs …"; every other line, and one already in the wanted state, is
// left as it is.
func ramdiskFstab(fstab string, enable bool) string {
	lines := strings.Split(fstab, "\n")
	for i, line := range lines {
		if enable && strings.HasPrefix(line, "#"+ramdiskMount) {
			lines[i] = strings.TrimLeft(line, "#")
		} else if !enable && strings.HasPrefix(line, ramdiskMount) {
			lines[i] = "#" + line
		}
	}
	return strings.Join(lines, "\n")
}

// setRamdisk rewrites /etc/fstab's streams tmpfs line, mounts or lazily
// unmounts STREAMS_PATH, and hands it back to xc_vm — a port of the
// enable/disable_ramdisk cases. Serve-only has no DB, so the PHP state write is dropped.
func setRamdisk(ctx context.Context, run nodeRootRunner, home string, enable bool) error {
	cur, err := os.ReadFile(fstabPath)
	if err != nil {
		return err
	}
	if err := os.WriteFile(fstabPath, []byte(ramdiskFstab(string(cur), enable)), 0o644); err != nil {
		return err
	}
	streams := filepath.Join(home, "content/streams")
	if enable {
		if err := run(ctx, "mount", streams); err != nil {
			return err
		}
	} else if err := run(ctx, "umount", "-l", streams); err != nil {
		return err
	}
	return run(ctx, "chown", "-R", "xc_vm:xc_vm", streams)
}

// setSysctl writes /etc/sysctl.conf and applies it only when MAIN's body differs
// from the current file, a port of the set_sysctl case; the DB state write is dropped.
func setSysctl(ctx context.Context, run nodeRootRunner, args map[string]any) error {
	body, _ := args["data"].(string)
	if body == "" {
		return nil
	}
	if cur, _ := os.ReadFile(sysctlPath); string(cur) == body {
		return nil
	}
	_ = run(ctx, "modprobe", "ip_conntrack")
	if err := os.WriteFile(sysctlPath, []byte(body), 0o644); err != nil {
		return err
	}
	return run(ctx, "sysctl", "-p")
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
	if cmd.Action == "flush" {
		// Empty the viewer-ban ipsets; the next chore tick re-fills from the
		// replica (bansync.go). Needs the agent's ReplicaDir, so handled here.
		a.flushBans(ctx)
		return true, []byte(`{"result":true}`)
	}
	run, delay, home := nodeRootRun, nodeRootStopDelay, a.NodeHome // captured here (no global read in the goroutine)
	if nodeRootSelfStopping[cmd.Action] {
		action, args := cmd.Action, cmd.Args
		go func() {
			time.Sleep(delay)
			if err := execNodeRoot(context.Background(), run, home, action, args); err != nil {
				a.logf("cluster: node.root %s: %v", action, err)
			}
		}()
		return true, []byte(`{"result":true,"detail":"issued"}`)
	}
	if err := execNodeRoot(ctx, run, home, cmd.Action, cmd.Args); err != nil {
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
