package supervisor

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestSelectNextOnFailureMatchesThePanel pins the two modes of
// StreamProcess::rotateSourcesPastCurrent, which is where this policy lives on
// the PHP side.
func TestSelectNextOnFailureMatchesThePanel(t *testing.T) {
	cases := []struct {
		name           string
		cur, n         int
		priorityBackup bool
		want           int
	}{
		{"priority backup retries from the top", 2, 4, true, 0},
		{"priority backup, already on the top", 0, 4, true, 0},
		{"no priority backup rotates on", 0, 3, false, 1},
		{"no priority backup rotates on again", 1, 3, false, 2},
		{"no priority backup wraps around", 2, 3, false, 0},
		{"a single source has nowhere to go", 0, 1, false, 0},
		{"a single source, priority backup", 0, 1, true, 0},
	}
	for _, c := range cases {
		if got := selectNextOnFailure(c.cur, c.n, c.priorityBackup); got != c.want {
			t.Errorf("%s: selectNextOnFailure(%d,%d,%v) = %d, want %d",
				c.name, c.cur, c.n, c.priorityBackup, got, c.want)
		}
	}
}

func srcs(n int, withProbe bool) []Source {
	out := make([]Source, n)
	for i := range out {
		out[i] = Source{Label: string(rune('a' + i)), Cmd: "run"}
		if withProbe {
			out[i].ProbeCmd = "probe"
		}
	}
	return out
}

// TestBackupSwitchPrefersTheHighestReachable: the list is in priority order, so
// the first source above the current one that answers is the best available.
func TestBackupSwitchPrefersTheHighestReachable(t *testing.T) {
	list := srcs(4, true)
	// Only sources 1 and 2 answer; 0 is still down.
	probe := func(_ context.Context, cmd string) bool { return cmd == "probe-1" || cmd == "probe-2" }
	list[0].ProbeCmd = "probe-0"
	list[1].ProbeCmd = "probe-1"
	list[2].ProbeCmd = "probe-2"

	if got := backupSwitch(context.Background(), probe, list, 3); got != 1 {
		t.Errorf("switched to %d, want 1 — the highest-priority source that answered", got)
	}
}

// TestBackupSwitchNeverLooksDownTheList: switching to a LOWER-priority source
// would be a demotion, and the whole point of the check is to climb back up.
func TestBackupSwitchNeverLooksDownTheList(t *testing.T) {
	list := srcs(4, true)
	everythingAnswers := func(context.Context, string) bool { return true }

	if got := backupSwitch(context.Background(), everythingAnswers, list, 0); got != -1 {
		t.Errorf("got %d while already on the top source; want no switch", got)
	}
}

// TestBackupSwitchSkipsUnprobeableSources: a source that cannot be tested must
// not be switched to on faith. Moving a channel that is at least delivering onto
// an unverified feed is a worse outcome than staying on the backup.
func TestBackupSwitchSkipsUnprobeableSources(t *testing.T) {
	list := srcs(3, false) // no ProbeCmd anywhere
	everythingAnswers := func(context.Context, string) bool { return true }

	if got := backupSwitch(context.Background(), everythingAnswers, list, 2); got != -1 {
		t.Errorf("switched to %d on an untestable source; want no switch", got)
	}
}

// TestBackupSwitchWithoutAProberDoesNothing: no prober configured means the
// speculative switch is simply off, not that it guesses.
func TestBackupSwitchWithoutAProberDoesNothing(t *testing.T) {
	if got := backupSwitch(context.Background(), nil, srcs(3, true), 2); got != -1 {
		t.Errorf("got %d with no prober; want no switch", got)
	}
}

// TestForcedSwitchRestartsOnTheChosenSource is the replacement for the panel's
// `<id>.force` signal file: an operator picks a source and the stream moves to
// it, logged as the panel's own FORCE_SOURCE action.
func TestForcedSwitchRestartsOnTheChosenSource(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	vit := &vitalsStub{}
	vit.set(Vitals{LastData: time.Now()})
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 10 * time.Millisecond

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup"},
	}
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "start on the primary", func() bool { return h.sup.State("5").Source == "primary" })

	if err := h.sup.ForceSource("5", 1); err != nil {
		t.Fatalf("ForceSource: %v", err)
	}
	waitFor(t, "switch to the backup", func() bool { return h.sup.State("5").Source == "backup" })

	h.mu.Lock()
	last := h.launched[len(h.launched)-1]
	h.mu.Unlock()
	if last != "ffmpeg -i backup" {
		t.Errorf("last launched %q, want the backup's command", last)
	}
	if st := h.sup.State("5"); st.SourceIdx != 1 {
		t.Errorf("SourceIdx = %d, want 1", st.SourceIdx)
	}
	got := actions(readLog(t, dirLog(dir)))
	if !contains(got, EventForceSource) {
		t.Errorf("event trail = %v, want %s", got, EventForceSource)
	}
}

// TestForcedSwitchRejectsAnUnknownSource: clamping a bad index would silently
// start the wrong channel.
func TestForcedSwitchRejectsAnUnknownSource(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	if err := h.sup.Supervise("5", baseSpec(dir)); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "start", func() bool { return h.sup.State("5").Running })

	if err := h.sup.ForceSource("5", 7); err == nil {
		t.Error("accepted a source index the stream does not have")
	}
	if err := h.sup.ForceSource("5", -1); err == nil {
		t.Error("accepted a negative source index")
	}
	if err := h.sup.ForceSource("does-not-exist", 0); err == nil {
		t.Error("accepted a force for a stream that is not supervised here")
	}
}

// TestFailedStartRotatesToTheNextSource: a source that will not start must not
// be retried forever while a working backup sits unused.
func TestFailedStartRotatesToTheNextSource(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	h.failNextLaunches(errTest, errTest)

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup"},
	}
	spec.Policy.PriorityBackupSec = 0 // rotate past the failure
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "a start on the backup", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, c := range h.launched {
			if c == "ffmpeg -i backup" {
				return true
			}
		}
		return false
	})
}

// TestPriorityBackupClimbsBack: a stream that failed over to a backup returns to
// its preferred feed once that feed answers again — the behaviour the panel's
// priority_backup setting exists for.
func TestPriorityBackupClimbsBack(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	vit := &vitalsStub{}
	vit.set(Vitals{LastData: time.Now()})
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 10 * time.Millisecond

	var mu sync.Mutex
	primaryUp := false
	h.sup.WithProber(func(_ context.Context, cmd string) bool {
		mu.Lock()
		defer mu.Unlock()
		return cmd == "probe-primary" && primaryUp
	})

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary", ProbeCmd: "probe-primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup", ProbeCmd: "probe-backup"},
	}
	spec.Policy.PriorityBackupSec = 1
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "start", func() bool { return h.sup.State("5").Running })

	// Move it to the backup, as a failover would have.
	if err := h.sup.ForceSource("5", 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "running on the backup", func() bool { return h.sup.State("5").Source == "backup" })

	// While the primary is still down, it must stay put.
	time.Sleep(120 * time.Millisecond)
	if st := h.sup.State("5"); st.Source != "backup" {
		t.Fatalf("left the backup while the primary was down (now %q)", st.Source)
	}

	mu.Lock()
	primaryUp = true
	mu.Unlock()

	waitFor(t, "climb back to the primary", func() bool { return h.sup.State("5").Source == "primary" })
	if got := actions(readLog(t, dirLog(dir))); !contains(got, EventPrioritySwitch) {
		t.Errorf("event trail = %v, want %s", got, EventPrioritySwitch)
	}
}

var errTest = &testErr{}

type testErr struct{}

func (*testErr) Error() string { return "test failure" }
