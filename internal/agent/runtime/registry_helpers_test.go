package clusteragent

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// emitThenLocked applies a change only once its events are spooled; with no
// events it spools nothing and applies the change.
func TestRegistryEmitThenLocked(t *testing.T) {
	var fail error
	emitted := 0
	r := NewRegistry(filepath.Join(t.TempDir(), "registry.snap"), func(ev []map[string]any) error {
		if fail != nil {
			return fail
		}
		emitted += len(ev)
		return nil
	}, t.Logf)
	applied := 0
	apply := func() {
		// Runs under r.mu: a TryLock must fail.
		if r.mu.TryLock() {
			r.mu.Unlock()
			t.Error("apply ran without the registry's lock")
		}
		applied++
	}
	fail = errors.New("spool full")
	if err := r.emitThenLocked(nil, apply); err != nil || emitted != 0 || applied != 1 {
		t.Fatalf("no events: err %v, emitted %d, applied %d", err, emitted, applied)
	}
	ev := []map[string]any{{"type": "conn.remove", "d": map[string]any{"uuid": "a"}}}
	if err := r.emitThenLocked(ev, apply); err != fail || applied != 1 {
		t.Fatalf("a failed spool: err %v, applied %d", err, applied)
	}
	fail = nil
	if err := r.emitThenLocked(ev, apply); err != nil || emitted != 1 || applied != 2 {
		t.Fatalf("err %v, emitted %d, applied %d", err, emitted, applied)
	}
}

func TestRegistrySortedKeys(t *testing.T) {
	r := NewRegistry(filepath.Join(t.TempDir(), "registry.snap"), func([]map[string]any) error { return nil }, t.Logf)
	r.Seed([]map[string]any{{"uuid": "c"}, {"uuid": "a"}, {"uuid": "b"}}, false)
	r.mu.Lock()
	keys := r.sortedKeysLocked()
	r.mu.Unlock()
	if strings.Join(keys, ",") != "a,b,c" {
		t.Fatalf("keys %v", keys)
	}
	var got []string
	for _, c := range r.Records() {
		got = append(got, c["uuid"].(string))
	}
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("records %v", got)
	}
}
