//go:build unix

package supervisor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These exercise the REAL launcher against real processes. Every other test in
// this package supplies an in-process fake, which is what makes the restart
// logic testable — but it means the code that actually spawns, signals and reaps
// an encoder is only covered here. It is unix-only for the same reason the
// launcher is.

// TestShellLauncherRunsAndReaps: the basic contract — a command runs, its pid is
// real, and Wait returns when it ends.
func TestShellLauncherRunsAndReaps(t *testing.T) {
	proc, err := shellLauncher(context.Background(), "exit 0", "")
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	if proc.Pid() <= 0 {
		t.Fatalf("Pid() = %d, want a real pid", proc.Pid())
	}
	done := make(chan error, 1)
	go func() { done <- proc.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Wait never returned for a command that exits immediately")
	}
}

// TestShellLauncherPidIsTheEncoderNotTheShell is why the launcher runs
// `sh -c "exec …"`. Without exec, the daemon's child is the shell and the real
// process is its grandchild: the pid published to the panel would be the
// shell's, and killing it would orphan the encoder still holding the source.
func TestShellLauncherPidIsTheEncoderNotTheShell(t *testing.T) {
	// `sleep` is a real binary, so with exec the child IS sleep.
	proc, err := shellLauncher(context.Background(), "sleep 30", "")
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	defer proc.Kill()

	cmdline, alive := findProcess(proc.Pid())
	if !alive {
		t.Fatal("the launched process is not alive")
	}
	if !strings.Contains(cmdline, "sleep") {
		t.Errorf("pid %d is running %q, want the command itself — `exec` did not replace the shell",
			proc.Pid(), cmdline)
	}
}

// TestShellLauncherKillStopsIt, and Wait then reports the death rather than
// hanging. A Kill that does not actually stop the encoder means a stream can
// never be restarted.
func TestShellLauncherKillStopsIt(t *testing.T) {
	proc, err := shellLauncher(context.Background(), "sleep 60", "")
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	pid := proc.Pid()

	done := make(chan error, 1)
	go func() { done <- proc.Wait() }()

	proc.Kill()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return after Kill")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, alive := findProcess(pid); !alive {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pid %d survived Kill", pid)
}

// TestShellLauncherKillsTheWholeGroup: an encoder that spawned children must not
// leave them behind holding the source. That is what Setpgid plus the negative
// kill is for.
func TestShellLauncherKillsTheWholeGroup(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "child.pid")
	// The shell backgrounds a child, records its pid, then waits.
	cmd := "sh -c 'sleep 60 & echo $! > " + marker + "; wait'"
	proc, err := shellLauncher(context.Background(), cmd, "")
	if err != nil {
		t.Fatalf("launch: %v", err)
	}

	var childPID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && childPID == 0 {
		childPID = readPID(marker)
		time.Sleep(20 * time.Millisecond)
	}
	if childPID == 0 {
		proc.Kill()
		t.Skip("the child never recorded its pid; nothing to assert about")
	}

	proc.Kill()
	_ = proc.Wait()

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, alive := findProcess(childPID); !alive {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("child pid %d outlived its group's kill and is still holding whatever it had open", childPID)
}

// TestShellLauncherWritesStderrWhereThePanelLooks: buildLive used to redirect
// ffmpeg's stderr into the stream's .errors file, and ops tooling reads it. The
// daemon took over the redirect, so it has to keep landing in the same place.
func TestShellLauncherWritesStderrWhereThePanelLooks(t *testing.T) {
	dir := t.TempDir()
	errPath := filepath.Join(dir, "5.errors")

	proc, err := shellLauncher(context.Background(), "echo 'something broke' 1>&2", errPath)
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	_ = proc.Wait()

	b, err := os.ReadFile(errPath)
	if err != nil {
		t.Fatalf("errors file not written: %v", err)
	}
	if !strings.Contains(string(b), "something broke") {
		t.Errorf("errors file = %q, want the command's stderr", string(b))
	}
}

// TestShellLauncherSurvivesItsContext pins the fix that makes adoption possible.
// The launcher must NOT tie the encoder's life to the context: that context dies
// with the supervisor, so binding to it means a daemon shutdown kills every
// encoder on the node and there is nothing left for the next daemon to adopt.
func TestShellLauncherSurvivesItsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	proc, err := shellLauncher(ctx, "sleep 30", "")
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	defer proc.Kill()

	cancel()
	time.Sleep(300 * time.Millisecond)

	if _, alive := findProcess(proc.Pid()); !alive {
		t.Fatal("cancelling the context killed the encoder; a daemon shutdown would take every channel " +
			"on the node off air and adoption could never fire")
	}
}

// TestFindProcessDistinguishesLiveFromDead underpins adoption: a wrong answer
// here either duplicates encoders or abandons them.
func TestFindProcessDistinguishesLiveFromDead(t *testing.T) {
	proc, err := shellLauncher(context.Background(), "sleep 30", "")
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	pid := proc.Pid()
	if _, alive := findProcess(pid); !alive {
		t.Error("a running process was reported dead")
	}

	proc.Kill()
	_ = proc.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, alive := findProcess(pid); !alive {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, alive := findProcess(pid); alive {
		t.Error("a killed process was still reported alive")
	}
	if _, alive := findProcess(0); alive {
		t.Error("pid 0 reported alive")
	}
	if _, alive := findProcess(-1); alive {
		t.Error("a negative pid reported alive")
	}
}
