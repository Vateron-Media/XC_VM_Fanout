// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package main

import (
	"context"
	"encoding/json"
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
	go pollConfig(ctx, path, time.Second, app, nil, false)

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

// TestPollConfigKeepsBootTuningWhenTheFileIsDeletedEarly: deleting config.json
// at runtime re-saves the tuning the daemon is RUNNING with, never the built-in
// defaults — that is the self-healing contract the docs state, and it must not
// depend on how long the daemon has been up.
//
// main loads the file at boot but used to start the poll with no current
// tuning, and the first tick comes only after -config-interval (60s). A
// deletion inside that window therefore took the other branch: the file was
// re-created with the DEFAULTS and those defaults were applied — supervise back
// to false, so every hand-over from the panel started answering 501, and the
// rings retuned from the operator's prebuffer to 40s. The same deletion one
// tick later kept the tuning.
func TestPollConfigKeepsBootTuningWhenTheFileIsDeletedEarly(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "off")
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"supervise": true, "prebuffer_max_sec": 20}`), 0o644); err != nil {
		t.Fatal(err)
	}
	boot, _, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !boot.Supervise || boot.PrebufferMaxSec != 20 {
		t.Fatalf("fixture did not load: supervise=%v prebuffer_max_sec=%d", boot.Supervise, boot.PrebufferMaxSec)
	}
	if err := os.Remove(path); err != nil { // unlink+create, before the first tick
		t.Fatal(err)
	}

	app := &recordingApplier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pollConfig(ctx, path, time.Second, app, &boot, false)

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		var v config.Values
		if err := json.Unmarshal(b, &v); err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if !v.Supervise || v.PrebufferMaxSec != 20 {
			t.Fatalf("the recreated config reset the live tuning: supervise=%v prebuffer_max_sec=%d", v.Supervise, v.PrebufferMaxSec)
		}
		if applied, ok := app.last(); ok && (!applied.Supervise || applied.PrebufferMaxSec != 20) {
			t.Fatalf("built-in defaults were applied over the boot tuning: supervise=%v prebuffer_max_sec=%d", applied.Supervise, applied.PrebufferMaxSec)
		}
		return
	}
	t.Fatal("the deleted config was never recreated")
}
