// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeWorld is a controllable set of "running processes" for adoption tests.
type fakeWorld struct {
	mu    sync.Mutex
	procs map[int]string // pid -> cmdline
	kills []int
}

func newWorld() *fakeWorld { return &fakeWorld{procs: map[int]string{}} }

func (w *fakeWorld) add(pid int, cmdline string) {
	w.mu.Lock()
	w.procs[pid] = cmdline
	w.mu.Unlock()
}

func (w *fakeWorld) remove(pid int) {
	w.mu.Lock()
	delete(w.procs, pid)
	w.mu.Unlock()
}

func (w *fakeWorld) find(pid int) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	cmd, ok := w.procs[pid]
	return cmd, ok
}

func (w *fakeWorld) kill(pid int) {
	w.mu.Lock()
	w.kills = append(w.kills, pid)
	delete(w.procs, pid)
	w.mu.Unlock()
}

func (w *fakeWorld) killed() []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]int(nil), w.kills...)
}

func writePID(t *testing.T, path string, pid int) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestAdoptableRequiresBothLivenessAndIdentity is the heart of it. A live pid
// alone proves nothing: pids are recycled, and the number in a stale pid file
// may belong to anything by the time the daemon comes back. Adopting the wrong
// process means the daemon watches something unrelated forever while the real
// encoder runs unsupervised.
func TestAdoptableRequiresBothLivenessAndIdentity(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "5_.pid")
	w := newWorld()

	spec := Spec{PIDPath: pidPath, AdoptMatch: "/streams/5_"}

	// Nothing running.
	writePID(t, pidPath, 4242)
	if _, ok := adoptable(spec, w.find); ok {
		t.Error("adopted a pid that is not running")
	}

	// Running, but it is somebody else's process that inherited the pid.
	w.add(4242, "/usr/bin/postgres -D /var/lib/pgsql")
	if _, ok := adoptable(spec, w.find); ok {
		t.Error("adopted a recycled pid belonging to an unrelated process")
	}

	// Running, and it is our encoder.
	w.add(4242, "ffmpeg -i http://src -f hls /home/xc_vm/streams/5_.m3u8")
	pid, ok := adoptable(spec, w.find)
	if !ok || pid != 4242 {
		t.Errorf("did not adopt the real encoder: pid=%d ok=%v", pid, ok)
	}
}

// TestAdoptionIsOffWithoutAMatch: an empty AdoptMatch must disable adoption
// rather than match everything. The panel supplies the token because it knows
// the paths; the daemon must not fall back to guessing.
func TestAdoptionIsOffWithoutAMatch(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "5_.pid")
	w := newWorld()
	w.add(4242, "ffmpeg -i whatever")
	writePID(t, pidPath, 4242)

	if _, ok := adoptable(Spec{PIDPath: pidPath}, w.find); ok {
		t.Error("adopted with no AdoptMatch configured")
	}
}

// TestAdoptableHandlesAMissingOrJunkPIDFile: none of these may panic or adopt.
func TestAdoptableHandlesAMissingOrJunkPIDFile(t *testing.T) {
	dir := t.TempDir()
	w := newWorld()
	w.add(1, "ffmpeg /streams/5_")

	for _, c := range []struct{ name, contents string }{
		{"empty", ""},
		{"not a number", "banana"},
		{"zero", "0"},
		{"negative", "-1"},
		{"whitespace", "   \n"},
	} {
		path := filepath.Join(dir, c.name)
		if err := os.WriteFile(path, []byte(c.contents), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, ok := adoptable(Spec{PIDPath: path, AdoptMatch: "/streams/5_"}, w.find); ok {
			t.Errorf("%s pid file was adopted", c.name)
		}
	}
	if _, ok := adoptable(Spec{PIDPath: filepath.Join(dir, "nope"), AdoptMatch: "x"}, w.find); ok {
		t.Error("adopted from a pid file that does not exist")
	}
}

// TestSuperviseAdoptsInsteadOfStartingASecondEncoder is the operational claim:
// a daemon restart must not put two encoders on one source, and must not take
// the channel off air either.
func TestSuperviseAdoptsInsteadOfStartingASecondEncoder(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	w := newWorld()
	const survivor = 9001
	w.add(survivor, "ffmpeg -i http://src -f hls /home/xc_vm/streams/5_.m3u8")
	h.sup.find = w.find
	h.sup.killPID = w.kill

	spec := baseSpec(dir)
	spec.AdoptMatch = "/streams/5_"
	writePID(t, spec.PIDPath, survivor)

	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "adoption", func() bool { return h.sup.State("5").Running })

	st := h.sup.State("5")
	if !st.Adopted {
		t.Error("state does not report the encoder as adopted")
	}
	if st.PID != survivor {
		t.Errorf("PID = %d, want the survivor %d", st.PID, survivor)
	}
	if n := h.launchCount(); n != 0 {
		t.Errorf("launched %d encoder(s) alongside a healthy survivor; that is two on one source", n)
	}
}

// TestAdoptedEncoderIsRestartedWhenItDies: adoption is not a one-way door. Once
// the survivor exits, the stream is restarted normally — and the replacement is
// started by us, not adopted again from a stale pid file.
func TestAdoptedEncoderIsRestartedWhenItDies(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	w := newWorld()
	const survivor = 9002
	w.add(survivor, "ffmpeg /home/xc_vm/streams/5_.m3u8")
	h.sup.find = w.find
	h.sup.killPID = w.kill

	spec := baseSpec(dir)
	spec.AdoptMatch = "/streams/5_"
	writePID(t, spec.PIDPath, survivor)

	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "adoption", func() bool { return h.sup.State("5").Adopted })

	// The survivor exits. The pid file still names it, so a naive re-adopt would
	// loop; the liveness check is what stops that.
	w.remove(survivor)

	h.nextProcess(t)
	waitFor(t, "a freshly launched replacement", func() bool {
		st := h.sup.State("5")
		return st.Running && !st.Adopted
	})
	if n := h.launchCount(); n != 1 {
		t.Errorf("launched %d times, want exactly 1 replacement", n)
	}
}

// TestReleaseKillsAnAdoptedEncoder: an inherited process is not our child, so
// stopping it takes a signal rather than a Wait. It must still actually stop —
// otherwise DELETE /monitor/<id> leaves an unsupervised encoder running.
func TestReleaseKillsAnAdoptedEncoder(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	w := newWorld()
	const survivor = 9003
	w.add(survivor, "ffmpeg /home/xc_vm/streams/5_.m3u8")
	h.sup.find = w.find
	h.sup.killPID = w.kill

	spec := baseSpec(dir)
	spec.AdoptMatch = "/streams/5_"
	writePID(t, spec.PIDPath, survivor)
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "adoption", func() bool { return h.sup.State("5").Adopted })

	h.sup.Release("5")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, k := range w.killed() {
			if k == survivor {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("releasing a stream left its adopted encoder running")
}

// TestDetachLeavesTheEncoderRunning is the behaviour that makes adoption worth
// having, and the one that is easiest to break by reflex.
//
// A daemon shutdown must NOT stop the encoders. They are left orphaned so the
// next daemon adopts them, which is what keeps every channel on the node on air
// across a restart or an upgrade. Killing them here — the obvious thing to do in
// a shutdown path — would turn a routine daemon upgrade into a node-wide outage.
func TestDetachLeavesTheEncoderRunning(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	w := newWorld()
	h.sup.find = w.find
	h.sup.killPID = w.kill

	spec := baseSpec(dir)
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	proc := h.nextProcess(t)
	waitFor(t, "start", func() bool { return h.sup.State("5").Running })

	killed := make(chan struct{})
	var once sync.Once
	proc.kill = func() { once.Do(func() { close(killed) }) }

	if n := h.sup.DetachAll(); n != 1 {
		t.Fatalf("DetachAll reported %d streams, want 1", n)
	}

	select {
	case <-killed:
		t.Fatal("shutdown killed the encoder; the next daemon has nothing to adopt and the channel is off air")
	case <-time.After(150 * time.Millisecond):
	}

	if st := h.sup.State("5"); st.Supervised {
		t.Error("detached stream still reports as supervised")
	}
	// The pid file must survive: it is how the next daemon finds the survivor.
	if _, err := os.Stat(spec.PIDPath); err != nil {
		t.Errorf("detach removed the pid file (%v); adoption reads it to find the encoder", err)
	}
}

// TestReleaseStillKills: detach is for shutdown, but an explicit DELETE of a
// stream must genuinely stop its encoder — otherwise unregistering a channel
// leaves an unsupervised ffmpeg holding the source forever.
func TestReleaseStillKills(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	if err := h.sup.Supervise("5", baseSpec(dir)); err != nil {
		t.Fatal(err)
	}
	proc := h.nextProcess(t)
	waitFor(t, "start", func() bool { return h.sup.State("5").Running })

	killed := make(chan struct{})
	var once sync.Once
	proc.kill = func() { once.Do(func() { close(killed) }) }

	h.sup.Release("5")
	select {
	case <-killed:
	case <-time.After(2 * time.Second):
		t.Fatal("Release did not stop the encoder")
	}
}
