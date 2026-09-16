// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
)

// cgroupTree lays out a fixture /sys/fs/cgroup and a /proc/self/cgroup beside
// it, and returns both paths.
func cgroupTree(t *testing.T, procCgroup string, files map[string]string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "cgroup")
	for name, body := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	proc := filepath.Join(dir, "self-cgroup")
	if procCgroup == "" {
		return root, filepath.Join(dir, "absent")
	}
	if err := os.WriteFile(proc, []byte(procCgroup), 0o644); err != nil {
		t.Fatal(err)
	}
	return root, proc
}

// TestMemoryBudgetSeesTheCgroupThisProcessIsIn: the soft memory limit has to be
// derived from the cgroup the daemon ACTUALLY runs in, not from the root one.
//
// On a cgroup v2 host the root has no memory.max at all, and a unit's
// MemoryMax=2G lives at /sys/fs/cgroup/system.slice/<unit>/memory.max. Reading
// only the root meant such a node fell back to 50% of the box's RAM — an 8 GiB
// soft limit under a 2 GiB ceiling — so the GC never tightened before the
// kernel OOM-killed the daemon and every viewer on the node dropped.
func TestMemoryBudgetSeesTheCgroupThisProcessIsIn(t *testing.T) {
	const host = 16 << 30
	for _, c := range []struct {
		name     string
		proc     string
		files    map[string]string
		want     int64
		wantSrc  string
		wantFrac float64
	}{
		{
			name:     "v2 unit limit under a root that has none",
			proc:     "0::/system.slice/xc_fanout.service\n",
			files:    map[string]string{"system.slice/xc_fanout.service/memory.max": "2147483648\n"},
			want:     2 << 30,
			wantSrc:  "cgroup v2 limit",
			wantFrac: defaults.MemLimitFraction,
		},
		{
			name: "v2 limit on a parent slice",
			proc: "0::/system.slice/xc_fanout.service\n",
			files: map[string]string{
				"system.slice/xc_fanout.service/memory.max": "max\n",
				"system.slice/memory.max":                   "4294967296\n",
			},
			want:     4 << 30,
			wantSrc:  "cgroup v2 limit",
			wantFrac: defaults.MemLimitFraction,
		},
		{
			name: "v2 takes the tightest of the chain",
			proc: "0::/system.slice/xc_fanout.service\n",
			files: map[string]string{
				"system.slice/xc_fanout.service/memory.max": "1073741824\n",
				"system.slice/memory.max":                   "4294967296\n",
			},
			want:     1 << 30,
			wantSrc:  "cgroup v2 limit",
			wantFrac: defaults.MemLimitFraction,
		},
		{
			name:     "v2 inside a container namespace, where the root IS our cgroup",
			proc:     "0::/\n",
			files:    map[string]string{"memory.max": "2147483648\n"},
			want:     2 << 30,
			wantSrc:  "cgroup v2 limit",
			wantFrac: defaults.MemLimitFraction,
		},
		{
			name: "v2 with no limit anywhere falls back to the host",
			proc: "0::/user.slice/user-0.slice/session-1.scope\n",
			files: map[string]string{
				"user.slice/user-0.slice/session-1.scope/memory.max": "max\n",
				"user.slice/memory.max":                              "max\n",
			},
			want:     host,
			wantSrc:  "system RAM",
			wantFrac: defaults.MemLimitHostFraction,
		},
		{
			name:     "v1 limit on the unit's own cgroup",
			proc:     "9:memory:/system.slice/xc_fanout.service\n8:cpu,cpuacct:/\n",
			files:    map[string]string{"memory/system.slice/xc_fanout.service/memory.limit_in_bytes": "2147483648\n"},
			want:     2 << 30,
			wantSrc:  "cgroup v1 limit",
			wantFrac: defaults.MemLimitFraction,
		},
		{
			name:     "v1 unlimited sentinel is not a budget",
			proc:     "9:memory:/\n",
			files:    map[string]string{"memory/memory.limit_in_bytes": "9223372036854771712\n"},
			want:     host,
			wantSrc:  "system RAM",
			wantFrac: defaults.MemLimitHostFraction,
		},
		{
			name:     "no cgroup filesystem at all",
			want:     host,
			wantSrc:  "system RAM",
			wantFrac: defaults.MemLimitHostFraction,
		},
	} {
		root, proc := cgroupTree(t, c.proc, c.files)
		got, src, frac := memoryBudgetFrom(root, proc, host)
		if got != c.want || src != c.wantSrc || frac != c.wantFrac {
			t.Errorf("%s: budget = %d %q %.2f, want %d %q %.2f", c.name, got, src, frac, c.want, c.wantSrc, c.wantFrac)
		}
	}
}
