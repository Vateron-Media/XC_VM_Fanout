package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// On a PHP-free node (NodeRootGo), an essential node.root action runs in Go and
// never reaches the PHP executor; a deferred one is dropped; both are acked
// (the high-water advances). With NodeRootGo off, every node.root goes to PHP.
func TestNodeRootGoRoutesEssentialToGo(t *testing.T) {
	f, st := newFake(t)
	ranShell := make(chan string, 4)
	old, oldDelay := nodeRootRun, nodeRootStopDelay
	nodeRootRun = func(_ context.Context, name string, args ...string) error {
		ranShell <- name + " " + strings.Join(args, " ")
		return nil
	}
	nodeRootStopDelay = 0
	defer func() { nodeRootRun, nodeRootStopDelay = old, oldDelay }()

	now := time.Now().Unix()
	wire := func(action string, seq uint64) WireCommand {
		doc := map[string]any{"v": 1, "type": "node.root", "action": action, "exp": now + 600, "iat": now, "cmd_id": fmt.Sprintf("%032x", seq), "seq": seq,
			"node_uuid": f.uuid, "gen": 1, "dedupe_key": nil, "args": map[string]any{}}
		b, _ := json.Marshal(doc)
		return WireCommand{Doc: string(b), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(f.panel, cc.PanelSigInput("cmd", b))), Seq: seq}
	}
	phpRan := 0
	php := func(context.Context, *Command, WireCommand) (bool, []byte) { phpRan++; return true, []byte("php") }

	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, NodeRootGo: true}
	a.handleCommand(context.Background(), wire("restart_services", 1), a.localExec(php)) // essential (self-stopping)
	if phpRan != 0 {
		t.Fatalf("essential node.root reached PHP %d time(s)", phpRan)
	}
	select {
	case got := <-ranShell:
		if !strings.Contains(got, "xc_vm") {
			t.Fatalf("restart_services ran %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restart_services did not run in Go")
	}

	a.handleCommand(context.Background(), wire("rotate_db", 2), a.localExec(php)) // deferred to P5: dropped
	if phpRan != 0 {
		t.Fatalf("deferred node.root reached PHP %d time(s)", phpRan)
	}
	if st.CmdSeq != 2 {
		t.Fatalf("high-water %d, want 2 (both acked)", st.CmdSeq)
	}

	// NodeRootGo off: node.root is handed to PHP as today.
	b := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	b.handleCommand(context.Background(), wire("restart_services", 3), b.localExec(php))
	if phpRan != 1 {
		t.Fatalf("with NodeRootGo off, node.root must reach PHP; phpRan=%d", phpRan)
	}
}

func TestNodeRootClassification(t *testing.T) {
	for _, a := range []string{"reboot", "restart_services", "stop_services"} {
		if !nodeRootEssential[a] {
			t.Errorf("%s must be essential (ported in B1)", a)
		}
		if !nodeRootSelfStopping[a] {
			t.Errorf("%s stops the agent and must be self-stopping", a)
		}
	}
	for _, a := range []string{"reload_nginx", "enable_ramdisk", "disable_ramdisk", "set_sysctl"} {
		if !nodeRootEssential[a] {
			t.Errorf("%s must be essential (ported in P5a)", a)
		}
		if nodeRootSelfStopping[a] {
			t.Errorf("%s does not stop the agent and must not be self-stopping", a)
		}
	}
	// flush / set_governor / set_port stay deferred to P5b.
	for _, a := range []string{"rotate_db", "install_module", "update", "set_port", "set_governor", "flush"} {
		if nodeRootEssential[a] {
			t.Errorf("%s is deferred, must not be essential", a)
		}
	}
}

// P5a ports four discrete node.root actions to Go: reload_nginx (nginx_rtmp),
// enable/disable_ramdisk (the /etc/fstab tmpfs toggle + mount/chown), and
// set_sysctl (write + apply on change). Each is a faithful port of its
// RootSignalsCronJob case, exercised through the shell seam + temp files.
func TestNodeRootP5aActions(t *testing.T) {
	var ran []string
	run := func(_ context.Context, name string, args ...string) error {
		ran = append(ran, strings.TrimSpace(name+" "+strings.Join(args, " ")))
		return nil
	}
	ctx := context.Background()
	const home = "/home/xc_vm"

	// reload_nginx → reload nginx_rtmp only, as xc_vm.
	ran = nil
	if err := execNodeRoot(ctx, run, home, "reload_nginx", nil); err != nil {
		t.Fatal(err)
	}
	if len(ran) != 1 || !strings.Contains(ran[0], "-u xc_vm") || !strings.Contains(ran[0], "bin/nginx_rtmp/sbin/nginx_rtmp -s reload") {
		t.Fatalf("reload_nginx ran %v", ran)
	}

	dir := t.TempDir()
	oldF, oldS := fstabPath, sysctlPath
	defer func() { fstabPath, sysctlPath = oldF, oldS }()
	fstabPath = filepath.Join(dir, "fstab")
	sysctlPath = filepath.Join(dir, "sysctl.conf")

	// enable_ramdisk uncomments the tmpfs line, then mounts + chowns.
	if err := os.WriteFile(fstabPath, []byte("UUID=x / ext4 defaults 0 1\n#"+ramdiskMount+" tmpfs defaults,size=50% 0 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ran = nil
	if err := execNodeRoot(ctx, run, home, "enable_ramdisk", nil); err != nil {
		t.Fatal(err)
	}
	fs, _ := os.ReadFile(fstabPath)
	if strings.Contains(string(fs), "#"+ramdiskMount) || !strings.Contains(string(fs), "\n"+ramdiskMount) {
		t.Fatalf("enable_ramdisk fstab = %q", fs)
	}
	if len(ran) != 2 || !strings.HasPrefix(ran[0], "mount "+home) || !strings.HasPrefix(ran[1], "chown -R xc_vm:xc_vm "+home) {
		t.Fatalf("enable_ramdisk ran %v", ran)
	}

	// disable_ramdisk comments it, then lazily unmounts + chowns.
	ran = nil
	if err := execNodeRoot(ctx, run, home, "disable_ramdisk", nil); err != nil {
		t.Fatal(err)
	}
	fs, _ = os.ReadFile(fstabPath)
	if !strings.Contains(string(fs), "#"+ramdiskMount) {
		t.Fatalf("disable_ramdisk fstab = %q", fs)
	}
	if len(ran) != 2 || !strings.HasPrefix(ran[0], "umount -l "+home) || !strings.HasPrefix(ran[1], "chown -R ") {
		t.Fatalf("disable_ramdisk ran %v", ran)
	}
	if !strings.Contains(string(fs), "UUID=x / ext4 defaults 0 1") {
		t.Fatalf("ramdisk toggle disturbed another fstab line: %q", fs)
	}

	// set_sysctl writes + applies only when MAIN's body differs.
	if err := os.WriteFile(sysctlPath, []byte("net.ipv4.old = 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ran = nil
	if err := execNodeRoot(ctx, run, home, "set_sysctl", map[string]any{"data": "net.ipv4.new = 2\n"}); err != nil {
		t.Fatal(err)
	}
	if sc, _ := os.ReadFile(sysctlPath); string(sc) != "net.ipv4.new = 2\n" {
		t.Fatalf("set_sysctl wrote %q", sc)
	}
	if len(ran) != 2 || ran[0] != "modprobe ip_conntrack" || ran[1] != "sysctl -p" {
		t.Fatalf("set_sysctl ran %v", ran)
	}
	// Unchanged body is a no-op (no re-apply).
	ran = nil
	if err := execNodeRoot(ctx, run, home, "set_sysctl", map[string]any{"data": "net.ipv4.new = 2\n"}); err != nil {
		t.Fatal(err)
	}
	if len(ran) != 0 {
		t.Fatalf("set_sysctl re-applied an unchanged config: %v", ran)
	}
}

func TestRamdiskFstabToggleIsFaithful(t *testing.T) {
	// Already in the wanted state → unchanged; other lines never touched.
	base := "UUID=1 / ext4 defaults 0 1\n" + ramdiskMount + " tmpfs defaults 0 0\n"
	if got := ramdiskFstab(base, true); got != base {
		t.Fatalf("enable of an already-enabled fstab changed it: %q", got)
	}
	off := ramdiskFstab(base, false)
	if !strings.Contains(off, "#"+ramdiskMount) || !strings.Contains(off, "UUID=1 / ext4 defaults 0 1") {
		t.Fatalf("disable = %q", off)
	}
	if ramdiskFstab(off, false) != off {
		t.Fatal("disable of an already-disabled fstab changed it")
	}
	if ramdiskFstab(off, true) != base {
		t.Fatal("enable did not restore the original line")
	}
}
