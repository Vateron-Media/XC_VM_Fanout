package clusteragent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type banCall struct {
	name, stdin string
	args        []string
}

func stubBanExec(t *testing.T) *[]banCall {
	t.Helper()
	var calls []banCall
	oldExec, oldHave := banExec, banHaveIpset
	banExec = func(_ context.Context, stdin, name string, args ...string) ([]byte, error) {
		calls = append(calls, banCall{name: name, stdin: stdin, args: append([]string(nil), args...)})
		return nil, nil
	}
	banHaveIpset = func() bool { return true }
	t.Cleanup(func() { banExec, banHaveIpset = oldExec, oldHave })
	return &calls
}

// writeBanlist writes a blocklist.json carrying only the IP list (reusing the
// shared writeBlocklist helper from rtmpgates_test.go).
func writeBanlist(t *testing.T, dir string, ips []string) {
	writeBlocklist(t, dir, map[string]any{"ip": ips})
}

func TestBanClassify(t *testing.T) {
	cases := []struct {
		in, set, member string
		ok              bool
	}{
		{"8.8.8.8", banSet4, "8.8.8.8", true},
		{"2606:4700:4700::1111", banSet6, "2606:4700:4700::1111", true},
		{"10.0.0.1", "", "", false},    // private
		{"127.0.0.1", "", "", false},   // loopback
		{"0.0.0.0", "", "", false},     // unspecified
		{"224.0.0.1", "", "", false},   // multicast
		{"169.254.1.1", "", "", false}, // link-local
		{"fe80::1", "", "", false},
		{"::1", "", "", false},
		{"fc00::1", "", "", false},     // ULA
		{"2001:db8::1", "", "", false}, // documentation range
		{"1.2.3.0/24", banSet4, "1.2.3.0/24", true},
		{"1.2.0.0/16", banSet4, "1.2.0.0/16", true},
		{"1.2.3.4/8", "", "", false},      // prefix < RANGE_MIN_PREFIX
		{"1.2.3.4/32", "", "", false},     // /32 is not a range (> 31)
		{"2001:4860::/32", "", "", false}, // IPv6 range: skipped (iptables-only)
		{"10.0.0.0/8", "", "", false},     // private network
		{"garbage", "", "", false},
	}
	for _, c := range cases {
		set, member, ok := banClassify(c.in)
		if ok != c.ok || set != c.set || (ok && member != c.member) {
			t.Errorf("banClassify(%q) = (%q,%q,%v), want (%q,%q,%v)", c.in, set, member, ok, c.set, c.member, c.ok)
		}
	}
}

func TestSyncBansRestoresAndDebounces(t *testing.T) {
	calls := stubBanExec(t)
	dir := t.TempDir()
	writeBanlist(t, dir, []string{"8.8.8.8", "1.1.1.1", "2606:4700::1111", "10.0.0.5"}) // 10.x skipped
	a := &Agent{NodeRootGo: true, ReplicaDir: dir, Logf: t.Logf}
	a.syncBans(context.Background())

	var restore string
	ruleSets := map[string]bool{}
	for _, c := range *calls {
		if c.name == "ipset" && len(c.args) >= 1 && c.args[0] == "restore" {
			restore += c.stdin
		}
		if c.name == "iptables" || c.name == "ip6tables" {
			for i, arg := range c.args {
				if arg == "--match-set" && i+1 < len(c.args) {
					ruleSets[c.args[i+1]] = true
				}
			}
		}
	}
	if restore == "" {
		t.Fatal("no ipset restore issued")
	}
	for _, want := range []string{
		"create xcvm_block4 hash:ip family inet", "create xcvm_block4_new", "flush xcvm_block4_new",
		"add xcvm_block4_new 8.8.8.8", "add xcvm_block4_new 1.1.1.1",
		"swap xcvm_block4_new xcvm_block4", "destroy xcvm_block4_new",
		"create xcvm_block6 hash:ip family inet6", "add xcvm_block6_new 2606:4700::1111",
	} {
		if !strings.Contains(restore, want) {
			t.Errorf("restore stdin missing %q\n---\n%s", want, restore)
		}
	}
	if strings.Contains(restore, "10.0.0.5") {
		t.Error("a private IP was synced into the ban set")
	}
	if !ruleSets["xcvm_block4"] || !ruleSets["xcvm_block6"] {
		t.Errorf("INPUT DROP rules not ensured for both families: %v", ruleSets)
	}
	if _, err := os.Stat(filepath.Join(dir, banMarkerName)); err != nil {
		t.Errorf("sync marker not written: %v", err)
	}

	// Unchanged list within the self-heal window → no new commands.
	n := len(*calls)
	a.syncBans(context.Background())
	if len(*calls) != n {
		t.Errorf("an unchanged blocklist was re-synced (%d new calls)", len(*calls)-n)
	}

	// A changed list re-syncs.
	writeBanlist(t, dir, []string{"8.8.8.8"})
	a.syncBans(context.Background())
	if len(*calls) == n {
		t.Error("a changed blocklist was not re-synced")
	}
}

func TestSyncBansGate(t *testing.T) {
	calls := stubBanExec(t)
	dir := t.TempDir()
	writeBanlist(t, dir, []string{"8.8.8.8"})
	// NodeRootGo off → no-op.
	(&Agent{NodeRootGo: false, ReplicaDir: dir, Logf: t.Logf}).syncBans(context.Background())
	// No replica → no-op.
	(&Agent{NodeRootGo: true, ReplicaDir: "", Logf: t.Logf}).syncBans(context.Background())
	if len(*calls) != 0 {
		t.Errorf("syncBans ran while gated off: %d calls", len(*calls))
	}
	// ipset absent → no-op even when otherwise eligible.
	banHaveIpset = func() bool { return false }
	(&Agent{NodeRootGo: true, ReplicaDir: dir, Logf: t.Logf}).syncBans(context.Background())
	if len(*calls) != 0 {
		t.Errorf("syncBans ran without ipset: %d calls", len(*calls))
	}
}

func TestFlushBansEmptiesSetsAndDropsMarker(t *testing.T) {
	calls := stubBanExec(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, banMarkerName)
	if err := os.WriteFile(marker, []byte("x\n1"), 0o640); err != nil {
		t.Fatal(err)
	}
	(&Agent{NodeRootGo: true, ReplicaDir: dir, Logf: t.Logf}).flushBans(context.Background())
	flushed := map[string]bool{}
	for _, c := range *calls {
		if c.name == "ipset" && len(c.args) == 2 && c.args[0] == "flush" {
			flushed[c.args[1]] = true
		}
	}
	if !flushed[banSet4] || !flushed[banSet6] {
		t.Errorf("flush did not empty both sets: %v", flushed)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Error("flush did not drop the sync marker")
	}
}
