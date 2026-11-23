package supervisor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStreamLogIsPrivateAndRedacted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stream.log")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	st := &stream{
		id:   "7",
		sup:  &Supervisor{now: time.Now},
		spec: Spec{LogPath: path},
	}
	st.emit(EventStreamStart, "http://host/live/user/secret/7.ts?token=hidden")
	entries := readLog(t, path)
	if len(entries) != 1 || entries[0].Source != "http://host/live/xxxxx/xxxxx/7.ts?token=xxxxx" {
		t.Fatalf("unredacted stream log: %+v", entries)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("stream log mode = %o", got)
	}
}

func TestStructuredDriverStartsThroughAllowlist(t *testing.T) {
	sup := New(func(context.Context, string, string) (Process, error) {
		return nil, errors.New("legacy launcher must not be used")
	}, func(string, time.Time) bool { return true }).WithDrivers(map[string]string{"sleep": "/bin/sleep"})
	spec := Spec{
		Sources: []Source{{DriverID: "sleep", DriverArgs: []string{"30"}}},
		Policy:  Policy{StartTimeoutSec: 1, StopFailures: 1},
	}
	if err := sup.Supervise("9", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "structured driver start", func() bool { return sup.State("9").Running })
	if !sup.Release("9") {
		t.Fatal("structured driver was not supervised")
	}
}

func TestStructuredDriverRejectsCommandsAndUnknownIDs(t *testing.T) {
	for _, src := range []Source{
		{DriverID: "acme", Cmd: "/tmp/client-driver"},
		{DriverID: "acme", FallbackCmd: "/tmp/client-fallback"},
		{DriverID: "acme", ProbeCmd: "/tmp/client-probe"},
	} {
		if err := (&Spec{Sources: []Source{src}}).valid(); err == nil {
			t.Fatalf("structured driver accepted client command: %+v", src)
		}
	}
	sup := New(nil, nil).WithDrivers(map[string]string{"acme": "/trusted/acme"})
	unknown := Spec{Sources: []Source{{DriverID: "unknown", DriverArgs: []string{"run"}}}}
	if err := sup.Supervise("7", unknown); err == nil {
		t.Fatal("unknown driver ID accepted")
	}
}
