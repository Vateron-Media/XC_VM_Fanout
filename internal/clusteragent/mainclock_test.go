package clusteragent

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeMono is a CLOCK_MONOTONIC the test moves by hand, in one boot.
type fakeMono struct {
	ns   int64
	boot string
}

func (f *fakeMono) clock() mainClock {
	return mainClock{mono: func() int64 { return f.ns }, boot: func() string { return f.boot }}
}

func (f *fakeMono) advance(d time.Duration) { f.ns += int64(d) }

func TestTheMainClockMovesOnlyOnAFresherStatementOfMains(t *testing.T) {
	mono := &fakeMono{ns: 5e9, boot: "b1"}
	c := mono.clock()
	if c.nowMs() != 0 {
		t.Fatal("an anchor before MAIN was ever heard")
	}
	c.heard(0)
	c.heard(-5)
	if c.nowMs() != 0 {
		t.Fatal("no time is not a time")
	}

	c.heard(1_800_000_000_000)
	mono.advance(5 * time.Second)
	if got := c.nowMs(); got != 1_800_000_005_000 {
		t.Fatalf("5 s on the monotonic clock: %d", got)
	}
	// A replayed older reply, and one repeating the number held: nothing.
	c.heard(1_800_000_000_000)
	c.heard(1_799_999_000_000)
	if got := c.nowMs(); got != 1_800_000_005_000 {
		t.Fatalf("an older statement moved the anchor: %d", got)
	}
	// A fresh statement is MAIN's own word, even below what the anchor had
	// extrapolated to (the time the reply was in flight, or MAIN's own clock
	// corrected): the anchor is MAIN's, not this machine's.
	c.heard(1_800_000_004_800)
	if got := c.nowMs(); got != 1_800_000_004_800 {
		t.Fatalf("a fresh statement did not win: %d", got)
	}
	mono.advance(time.Second)
	if got := c.nowMs(); got != 1_800_000_005_800 {
		t.Fatalf("after it: %d", got)
	}
}

func TestARestartResumesTheMainClockAndNeverStartsItAgain(t *testing.T) {
	mono := &fakeMono{ns: 100e9, boot: "b1"}
	before := mono.clock()
	before.heard(1_800_000_000_000)
	mono.advance(30 * time.Second)
	seen, mark := before.mark()
	if seen != 1_800_000_000_000 || mark == nil || mark.MainMs != 1_800_000_030_000 || mark.MonoNs != 130e9 || mark.BootID != "b1" {
		t.Fatalf("mark %d %+v", seen, mark)
	}

	// The same boot: CLOCK_MONOTONIC went on counting while the agent was
	// down, so the whole outage counts, however long ago the mark was saved.
	mono.advance(20 * time.Minute)
	same := mono.clock()
	same.resume(seen, mark)
	if got := same.nowMs(); got != 1_800_000_030_000+20*60*1000 {
		t.Fatalf("same boot: %d", got)
	}
	// And what MAIN said before the restart is still what a replay is held to.
	same.heard(1_800_000_000_000)
	if got := same.nowMs(); got != 1_800_000_030_000+20*60*1000 {
		t.Fatalf("a replay after the restart moved it: %d", got)
	}

	// Another boot: the monotonic clock started again, so the time the machine
	// was down is not known — the anchor starts from the mark and counts from
	// the restart, undercounting (serving longer), never over.
	other := &fakeMono{ns: 3e9, boot: "b2"}
	rebooted := other.clock()
	rebooted.resume(seen, mark)
	other.advance(time.Second)
	if got := rebooted.nowMs(); got != 1_800_000_031_000 {
		t.Fatalf("another boot: %d", got)
	}
	// No boot id to compare (off Linux) is another boot.
	noBoot := &fakeMono{ns: 500e9}
	blind := noBoot.clock()
	blind.resume(seen, &ClockMark{MainMs: mark.MainMs, MonoNs: mark.MonoNs})
	if got := blind.nowMs(); got != mark.MainMs {
		t.Fatalf("no boot id: %d", got)
	}

	// Only the highest number seen, from a state saved before the mark was
	// kept: the anchor starts there.
	bare := (&fakeMono{ns: 1, boot: "b1"}).clock()
	bare.resume(1_800_000_000_000, nil)
	if got := bare.nowMs(); got != 1_800_000_000_000 {
		t.Fatalf("seen only: %d", got)
	}
	// Nothing kept: no anchor, which the node's PHP reads as "serve".
	none := (&fakeMono{ns: 1, boot: "b1"}).clock()
	none.resume(0, nil)
	if none.nowMs() != 0 {
		t.Fatal("an anchor from nothing")
	}
	if _, m := none.mark(); m != nil {
		t.Fatalf("a mark without an anchor: %+v", m)
	}
}

func TestTheMainClockIsSavedAndResumedThroughTheState(t *testing.T) {
	_, st := newFake(t)
	c := NewClient(st, "xc_agent/test")
	c.setMainTime(time.Now().UnixMilli())
	if err := c.saveClock(); err != nil {
		t.Fatal(err)
	}
	back, err := loadRaw(st.path)
	if err != nil {
		t.Fatal(err)
	}
	if back.MainSeenMs <= 0 || back.MainAnchor == nil || back.MainAnchor.BootID != bootID() {
		t.Fatalf("saved %d %+v", back.MainSeenMs, back.MainAnchor)
	}
	time.Sleep(20 * time.Millisecond)
	again := NewClient(back, "xc_agent/test")
	if got := again.clock.nowMs(); got < back.MainAnchor.MainMs+20 {
		t.Fatalf("resumed at %d, the mark was %d 20 ms ago", got, back.MainAnchor.MainMs)
	}
}

func TestTheRekeyChallengeStampsRequestsButAnchorsNothing(t *testing.T) {
	// The challenge is signed but answers no request of the node's, so an old
	// copy replays: it may set the stamp MAIN checks the window of, never the
	// clock a lease is judged against. The re-key document that follows names
	// the request, and anchors.
	m, st, _ := newRekeyMain(t)
	m.licensed = false
	c := NewClient(st, "xc_agent/test")
	if _, err := c.Rekey(context.Background(), map[string]any{}); !errors.Is(err, ErrUnlicensed) {
		t.Fatalf("unlicensed: %v", err)
	}
	if c.offsetAtMs.Load() == 0 {
		t.Fatal("the challenge's time did not reach the request stamp")
	}
	if c.clock.nowMs() != 0 {
		t.Fatal("the challenge anchored MAIN's clock")
	}
	m.mu.Lock()
	m.licensed = true
	m.mu.Unlock()
	if _, err := c.Rekey(context.Background(), map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if c.clock.nowMs() == 0 {
		t.Fatal("the signed re-key document did not anchor MAIN's clock")
	}
}
