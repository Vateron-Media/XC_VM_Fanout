// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/config"
)

// recordingApplier stands in for the Manager: it records the tuning the poll
// loop applies, without a live stream registry behind it.
type recordingApplier struct {
	mu  sync.Mutex
	got []config.Values
}

func (r *recordingApplier) ApplyConfig(v config.Values) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, v)
}

func (r *recordingApplier) last() (config.Values, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.got) == 0 {
		return config.Values{}, false
	}
	return r.got[len(r.got)-1], true
}

func (r *recordingApplier) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

// TestPollConfigRetriesAfterAFailedLoad: a poll that could not read the file
// must leave the mtime gate where it was, so the NEXT tick tries again. That is
// what the package doc and the docs promise.
//
// The gate was armed from the stat before config.Load ran. The panel writes the
// file non-atomically (file_put_contents: truncate, then write) and Linux
// stamps mtime from the coarse clock, so a poll landing between the truncate
// and the write read an empty file, failed to parse, and armed the gate with
// the very mtime the completed write then carried. Every later tick saw no
// change and skipped the file: the admin's edit was lost until the next save.
func TestPollConfigRetriesAfterAFailedLoad(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "off") // keep the poll from touching this test binary's heap limit
	path := filepath.Join(t.TempDir(), "config.json")

	bad := []byte(`{"grace_sec": 33,}`) // a torn mid-write, or a hand edit in progress
	good := []byte(`{"grace_sec": 33}`)
	for len(good) < len(bad) {
		good = append(good, ' ') // same size AND same mtime: only a retry can find it
	}
	if err := os.WriteFile(path, bad, 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	mod := fi.ModTime()

	app := &recordingApplier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pollConfig(ctx, path, time.Second, app)

	time.Sleep(1500 * time.Millisecond) // let the first tick fail on the malformed file
	if n := app.count(); n != 0 {
		t.Fatalf("a malformed config was applied %d time(s)", n)
	}

	if err := os.WriteFile(path, good, 0o644); err != nil { // the writer finishes
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mod, mod); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := app.last(); ok {
			if v.GraceSec != 33 {
				t.Fatalf("applied grace_sec = %d, want 33", v.GraceSec)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the completed config was never re-read: a failed load armed the gate, so the panel's edit is lost until the next save")
}
