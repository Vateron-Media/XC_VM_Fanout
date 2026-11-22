package clusteragent

import (
	"os"
	"testing"
)

// TestMain forces privEscalate off for the whole package so the node.root and ban
// tests exercise the direct (root) exec and file-write paths deterministically,
// independent of the euid the test process happens to run under. The sudo-n
// escalation itself is a thin production-only wrapper (privCmd/privWriteFile).
func TestMain(m *testing.M) {
	privEscalate = false
	os.Exit(m.Run())
}

// TestPrivCmdEscalates: as xc_vm (privEscalate) a privileged command is wrapped in
// `sudo -n`; as root it runs unchanged.
func TestPrivCmdEscalates(t *testing.T) {
	defer func(o bool) { privEscalate = o }(privEscalate)

	privEscalate = true
	n, a := privCmd("ipset", []string{"restore"})
	if n != "sudo" || len(a) != 3 || a[0] != "-n" || a[1] != "ipset" || a[2] != "restore" {
		t.Fatalf("escalated = %q %v, want sudo -n ipset restore", n, a)
	}

	privEscalate = false
	n, a = privCmd("ipset", []string{"restore"})
	if n != "ipset" || len(a) != 1 || a[0] != "restore" {
		t.Fatalf("direct = %q %v, want ipset restore", n, a)
	}
}
