package clusteragent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
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

// privEscalate is true when the agent is NOT root. A serve-only node's agent runs
// as xc_vm (so the xc_vm front keeps owning the files they share — the agent
// socket, the gateway policy, the replica caches); it reaches root for the few
// privileged node.root/ban operations through `sudo -n`, for which the install
// grants xc_vm a NOPASSWD allowlist. When the agent is root the commands run
// directly. Tests force it false (TestMain) to pin the direct, un-escalated path.
var privEscalate = os.Geteuid() != 0

// privCmd prepends `sudo -n` to a privileged command when the agent is not root.
func privCmd(name string, args []string) (string, []string) {
	if privEscalate {
		return "sudo", append([]string{"-n", name}, args...)
	}
	return name, args
}

// noderootHelper is the root-owned trust boundary a serve-only node's xc_vm agent
// escalates through: the sudoers grant is to this one file only.
const noderootHelper = "/usr/local/sbin/xcvm-noderoot"

// stageDir is where the agent drops the files the helper reads (sysctl.pending,
// bans.pending). The helper hardcodes the same path; a var only for tests.
var stageDir = "/home/xc_vm/tmp/cluster"

// helperVerb runs `sudo -n xcvm-noderoot <verb> [args]` (via nodeRootRun/privCmd).
func helperVerb(ctx context.Context, verb string, args ...string) error {
	return nodeRootRun(ctx, noderootHelper, append([]string{verb}, args...)...)
}

// writeStage writes a helper staging file (as the agent user, 0600).
func writeStage(name string, data []byte) error {
	if err := os.MkdirAll(stageDir, 0o750); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(stageDir, name), data, 0o600)
}

// nodeRootRun runs one privileged command; a test seam. On a serve-only node the
// agent is xc_vm, so the command is escalated with `sudo -n` (privCmd); as root it
// runs directly. A self-stopping command captures it into a local before its
// goroutine fires, so a test swapping this global never races it.
var nodeRootRun nodeRootRunner = func(ctx context.Context, name string, args ...string) error {
	n, a := privCmd(name, args)
	return exec.CommandContext(ctx, n, a...).Run()
}

// nodeRootOutput runs a command and returns its combined output; a test seam for
// the cpufreq-info reads set_governor makes. These are /sys reads that need no
// privilege, so they are not escalated. A missing binary returns an error, so
// set_governor skips (as PHP's `which cpufreq-info` guard does).
var nodeRootOutput = func(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// nodeRootStopDelay is how long a self-stopping command waits before firing, so
// its ack is persisted first. A test sets it to 0.
var nodeRootStopDelay = time.Second

// nodeRootEssential are the node.root actions a PHP-free node runs in Go. B1
// ported the lifecycle commands; P5a adds reload_nginx + ramdisk + set_sysctl;
// P5c adds flush (the viewer-ban ipsets, bansync.go); P5b adds set_governor and
// set_port. (OPENSSL_EXTRA / rotate_sign_key / install_config / seed_connections
// are still dropped pending confirmation they are moot or Go-owned on a serve-only
// node — see the P5 design doc.)
var nodeRootEssential = map[string]bool{
	"reboot": true, "restart_services": true, "stop_services": true,
	"reload_nginx": true, "enable_ramdisk": true, "disable_ramdisk": true, "set_sysctl": true,
	"flush": true, "set_governor": true, "set_port": true,
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
		if privEscalate {
			return helperVerb(ctx, "reboot")
		}
		return run(ctx, "reboot")
	case "stop_services":
		if privEscalate {
			return helperVerb(ctx, "service", "stop")
		}
		return run(ctx, "systemctl", "stop", "xc_vm")
	case "restart_services":
		if privEscalate {
			return helperVerb(ctx, "service", "restart")
		}
		if err := run(ctx, "systemctl", "stop", "xc_vm"); err != nil {
			return err
		}
		return run(ctx, "systemctl", "start", "xc_vm")
	case "reload_nginx":
		return reloadNginxRtmp(ctx, run, home)
	case "enable_ramdisk":
		return setRamdisk(ctx, run, home, true)
	case "disable_ramdisk":
		return setRamdisk(ctx, run, home, false)
	case "set_sysctl":
		return setSysctl(ctx, run, args)
	case "set_governor":
		return setGovernor(ctx, run, args)
	case "set_port":
		return setPort(ctx, run, home, args)
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
	if privEscalate { // the helper edits fstab, mounts and chowns itself
		mode := "off"
		if enable {
			mode = "on"
		}
		return helperVerb(ctx, "ramdisk", mode)
	}
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
	if privEscalate { // the helper validates the staged body and applies it
		if err := writeStage("sysctl.pending", []byte(body)); err != nil {
			return err
		}
		return helperVerb(ctx, "sysctl")
	}
	_ = run(ctx, "modprobe", "ip_conntrack")
	if err := os.WriteFile(sysctlPath, []byte(body), 0o644); err != nil {
		return err
	}
	return run(ctx, "sysctl", "-p")
}

// reloadNginxRtmp reloads nginx_rtmp (the only nginx on a serve-only node), as
// xc_vm — matching `service reload`. Shared by reload_nginx and set_port.
func reloadNginxRtmp(ctx context.Context, run nodeRootRunner, home string) error {
	bin := filepath.Join(home, "bin/nginx_rtmp/sbin/nginx_rtmp")
	if privEscalate {
		// The agent is already xc_vm — reload nginx_rtmp directly (it owns the master).
		return exec.CommandContext(ctx, bin, "-s", "reload").Run()
	}
	// The agent is root — drop to xc_vm, matching `service reload`.
	return run(ctx, "sudo", "-u", "xc_vm", bin, "-s", "reload")
}

// setGovernor sets the CPU frequency governor on every core when MAIN sends a
// supported one that differs from the current — a port of the set_governor case.
// No cpufreq-info, or a governor the host does not offer, is a no-op (as PHP's
// `which cpufreq-info` guard is). The PHP DB state write is dropped (no DB).
func setGovernor(ctx context.Context, run nodeRootRunner, args map[string]any) error {
	gov, _ := args["data"].(string)
	if gov == "" {
		return nil
	}
	avail, err := nodeRootOutput(ctx, "cpufreq-info", "-g")
	if err != nil || !fieldIn(string(avail), gov) {
		return nil
	}
	if cur, _ := nodeRootOutput(ctx, "cpufreq-info", "-p"); curGovernor(string(cur)) == gov {
		return nil
	}
	if privEscalate { // the helper sets every core
		return helperVerb(ctx, "governor", gov)
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		if err := run(ctx, "cpufreq-set", "-c", strconv.Itoa(i), "-g", gov); err != nil {
			return err
		}
	}
	return nil
}

// fieldIn reports whether want is one of the whitespace-separated fields of s
// (the governors `cpufreq-info -g` lists).
func fieldIn(s, want string) bool {
	for _, f := range strings.Fields(s) {
		if f == want {
			return true
		}
	}
	return false
}

// curGovernor is the active governor from `cpufreq-info -p` ("min max governor").
func curGovernor(s string) string {
	if f := strings.Fields(s); len(f) >= 3 {
		return f[2]
	}
	return ""
}

// setPort rewrites the node's listen ports from a set_port command — a port of
// that case for a serve-only node. The Go front hot-rebinds ports/http.conf and
// ports/https.conf itself (front.WatchPorts), so writing the files is enough; only
// the nginx_rtmp configs need a reload. type 0 = plain-HTTP ports (+ the nginx_rtmp
// live.conf callbacks), 1 = HTTPS ports, 2 = the nginx_rtmp listen port.
func setPort(ctx context.Context, run nodeRootRunner, home string, args map[string]any) error {
	ports := toPorts(args["ports"])
	reload, _ := args["reload"].(bool)
	switch argInt(args["type"]) {
	case 0:
		if len(ports) == 0 {
			return nil
		}
		if err := writeFileMode(filepath.Join(home, "bin/nginx/conf/ports/http.conf"), []byte(listenLines(ports, "")), 0o644); err != nil {
			return err
		}
		live := ""
		for _, d := range []string{"on_play", "on_publish", "on_play_done"} {
			live += fmt.Sprintf("%s http://127.0.0.1:%d/stream/rtmp; ", d, ports[0])
		}
		if err := writeFileMode(filepath.Join(home, "bin/nginx_rtmp/conf/live.conf"), []byte(strings.TrimRight(live, " ")), 0o644); err != nil {
			return err
		}
		if reload {
			return reloadNginxRtmp(ctx, run, home)
		}
	case 1:
		// The Go front binds these; WatchPorts picks up the change — no reload.
		return writeFileMode(filepath.Join(home, "bin/nginx/conf/ports/https.conf"), []byte(listenLines(ports, " ssl")), 0o644)
	case 2:
		if len(ports) == 0 {
			return nil
		}
		if err := writeFileMode(filepath.Join(home, "bin/nginx_rtmp/conf/port.conf"), []byte(fmt.Sprintf("listen %d;", ports[0])), 0o644); err != nil {
			return err
		}
		if reload {
			return reloadNginxRtmp(ctx, run, home)
		}
	}
	return nil
}

// listenLines renders nginx listen directives for a port set, matching set_port:
// "listen <p>;" (suffix "") or "listen <p> ssl;" (suffix " ssl"), space-joined, as
// the front's ParseListenSet reads them.
func listenLines(ports []int, suffix string) string {
	out := make([]string, 0, len(ports))
	for _, p := range ports {
		out = append(out, fmt.Sprintf("listen %d%s;", p, suffix))
	}
	return strings.Join(out, " ")
}

// toPorts parses a set_port `ports` arg (a JSON array of numbers or numeric
// strings) into valid listen ports (80–65535), as PHP's is_numeric guard does.
func toPorts(v any) []int {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	var out []int
	for _, e := range arr {
		if p := argInt(e); p >= 80 && p <= 65535 {
			out = append(out, p)
		}
	}
	return out
}

// argInt coerces a JSON number or numeric string to an int (0 if neither).
func argInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case string:
		i, _ := strconv.Atoi(strings.TrimSpace(n))
		return i
	}
	return 0
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
