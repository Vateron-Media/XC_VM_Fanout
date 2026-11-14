package clusteragent

import (
	"sync"
	"testing"
	"time"
)

// A playlist read during an idle-close must not leave MAIN ended while the
// registry still advertises that same lifecycle as open.
func TestRegistryReapAndConcurrentTouchAgreeWithEmittedState(t *testing.T) {
	r, _, now := newTestRegistry(t)
	initial := rec(7, "x", 100, 100)
	initial["retire_token"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := r.Put("viewer", initial); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(31 * time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var emitted []map[string]any
	r.emit = func(events []map[string]any) error {
		for _, event := range events {
			record, _ := event["d"].(map[string]any)["record"].(map[string]any)
			if num(record["hls_end"]) != 0 {
				close(entered)
				<-release
			}
			mu.Lock()
			emitted = append(emitted, clone(record))
			mu.Unlock()
		}
		return nil
	}
	reaped := make(chan int, 1)
	go func() { reaped <- r.Reap(30 * time.Second) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reaper did not reach its spool")
	}
	touched := make(chan error, 1)
	go func() { _, err := r.Touch("viewer", 131); touched <- err }()
	// Before the fix the touch finishes while the old close is still pending.
	// After the fix it waits, then opens a new lifecycle after that close.
	touchFinished := false
	select {
	case err := <-touched:
		if err != nil {
			t.Fatal(err)
		}
		touchFinished = true
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case <-reaped:
	case <-time.After(5 * time.Second):
		t.Fatal("reaper did not finish")
	}
	if !touchFinished {
		select {
		case err := <-touched:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("touch did not finish")
		}
	}
	current := r.Get("viewer")
	mu.Lock()
	defer mu.Unlock()
	if len(emitted) == 0 {
		t.Fatal("no emitted records")
	}
	last := emitted[len(emitted)-1]
	if num(last["hls_end"]) != num(current["hls_end"]) || last["retire_token"] != current["retire_token"] {
		t.Fatalf("MAIN's last event and registry disagree: event=%v registry=%v", last, current)
	}
	if num(current["hls_end"]) != 0 {
		t.Fatal("the new playlist read must stay open")
	}
}

func TestRegistryInFlightPutCannotUndoMainClose(t *testing.T) {
	r, _, now := newTestRegistry(t)
	initial := rec(7, "x", 100, 100)
	initial["container"] = "ts"
	initial["retire_token"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := r.Put("viewer", initial); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(11 * time.Second) // The next touch is due on P0.
	entered, release := make(chan struct{}), make(chan struct{})
	r.emit = func([]map[string]any) error { close(entered); <-release; return nil }
	updated := clone(initial)
	updated["hls_last_read"] = 120
	putDone := make(chan error, 1)
	go func() { putDone <- r.Put("viewer", updated) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("put did not reach spool")
	}
	if d := r.Digest(); d.Count != 1 {
		t.Fatalf("digest %+v", d)
	}
	closed := make(chan error, 1)
	go func() { closed <- r.Close("viewer", true) }()
	closeFinished := false
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
		closeFinished = true
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-putDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("put did not finish")
	}
	if !closeFinished {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("close did not finish")
		}
	}
	if current := r.Get("viewer"); current != nil {
		t.Fatalf("acknowledged MAIN close was undone: %v", current)
	}
}
