package clusteragent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// helperEnv forces privEscalate (the serve-only xc_vm agent) and records every
// privileged command; the staging dir is a temp dir.
func helperEnv(t *testing.T) *[]string {
	t.Helper()
	oldP, oldS, oldR, oldO := privEscalate, stageDir, nodeRootRun, nodeRootOutput
	privEscalate, stageDir = true, filepath.Join(t.TempDir(), "cluster")
	var ran []string
	nodeRootRun = func(_ context.Context, name string, args ...string) error {
		ran = append(ran, name+" "+strings.Join(args, " "))
		return nil
	}
	t.Cleanup(func() { privEscalate, stageDir, nodeRootRun, nodeRootOutput = oldP, oldS, oldR, oldO })
	return &ran
}

func wantRan(t *testing.T, ran *[]string, want ...string) {
	t.Helper()
	for i := range want {
		want[i] = noderootHelper + " " + want[i]
	}
	if fmt.Sprint(*ran) != fmt.Sprint(want) {
		t.Fatalf("ran %q, want %q", *ran, want)
	}
	*ran = nil
}

func TestHelperVerbsServiceRebootRamdisk(t *testing.T) {
	ran := helperEnv(t)
	ctx := context.Background()
	for action, want := range map[string]string{
		"reboot": "reboot", "stop_services": "service stop", "restart_services": "service restart",
		"enable_ramdisk": "ramdisk on", "disable_ramdisk": "ramdisk off",
	} {
		if err := execNodeRoot(ctx, nodeRootRun, t.TempDir(), action, nil); err != nil {
			t.Fatal(err)
		}
		wantRan(t, ran, want)
	}
}

func TestHelperVerbSysctlStagesBody(t *testing.T) {
	ran := helperEnv(t)
	old := sysctlPath
	sysctlPath = filepath.Join(t.TempDir(), "none")
	defer func() { sysctlPath = old }()
	body := "net.core.somaxconn = 1024\n"
	if err := setSysctl(context.Background(), nodeRootRun, map[string]any{"data": body}); err != nil {
		t.Fatal(err)
	}
	wantRan(t, ran, "sysctl")
	got, err := os.ReadFile(filepath.Join(stageDir, "sysctl.pending"))
	if err != nil || string(got) != body {
		t.Fatalf("sysctl.pending = %q, %v", got, err)
	}
	if fi, _ := os.Stat(filepath.Join(stageDir, "sysctl.pending")); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}
	// unchanged body: still a no-op
	if err := os.WriteFile(sysctlPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = setSysctl(context.Background(), nodeRootRun, map[string]any{"data": body})
	wantRan(t, ran)
}

func TestHelperVerbGovernor(t *testing.T) {
	ran := helperEnv(t)
	nodeRootOutput = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "-g" {
			return []byte("ondemand performance"), nil
		}
		return []byte("1 2 ondemand"), nil
	}
	if err := setGovernor(context.Background(), nodeRootRun, map[string]any{"data": "performance"}); err != nil {
		t.Fatal(err)
	}
	wantRan(t, ran, "governor performance")
	_ = setGovernor(context.Background(), nodeRootRun, map[string]any{"data": "ondemand"}) // current: no-op
	wantRan(t, ran)
}

func TestHelperVerbBans(t *testing.T) {
	ran := helperEnv(t)
	calls := stubBanExec(t)
	dir := t.TempDir()
	writeBanlist(t, dir, []string{"8.8.8.8", "1.2.3.0/24", "10.0.0.1", "2606:4700:4700::1111"})
	a := &Agent{NodeRootGo: true, ReplicaDir: dir, Logf: t.Logf}
	a.syncBans(context.Background())
	wantRan(t, ran, "ban-sync")
	got, _ := os.ReadFile(filepath.Join(stageDir, "bans.pending"))
	if string(got) != "1.2.3.0/24\n8.8.8.8\n2606:4700:4700::1111\n" {
		t.Fatalf("bans.pending = %q", got)
	}
	a.flushBans(context.Background())
	wantRan(t, ran, "ban-flush")
	if len(*calls) != 0 {
		t.Errorf("raw ipset/iptables used under privEscalate: %v", *calls)
	}
}
