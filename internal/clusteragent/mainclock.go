package clusteragent

import (
	"sync"
	"time"
)

// mainClock is MAIN's clock as this node can vouch for it: the anchor a lease
// is judged against once MAIN cannot be reached (ADR 0004, Phase 9). It is not
// the Client's offset (MainNowMs), which stamps requests and is also taken
// from the re-key challenge — a signed document bound to no request, so an old
// copy replays — and which follows this machine's wall clock wherever it is
// moved.
//
//   - Only authenticated statements move it: a MAC'd reply, a verified denial,
//     a panel-signed re-key document (Client.setMainTime).
//   - It re-anchors on MAIN's own number, and only on one higher than any it
//     has seen: a replayed older reply changes nothing, and a fresh statement
//     wins over what the anchor had extrapolated to.
//   - Between statements it advances on CLOCK_MONOTONIC (monotonicNs), so
//     moving this machine's wall clock, either way, moves nothing.
//   - A restart resumes it (ClockMark, saved in the state at most once a
//     minute): within the same boot, by CLOCK_MONOTONIC's own count of the time
//     since the mark, which the restart does not reset; after a reboot, from
//     the mark, counting from the restart — undercounting the time the machine
//     was down, which serves viewers longer rather than shorter.
//
// Its answer is 0 until MAIN has been heard on this node (or a mark resumed),
// and a reader with a judgement to make reads 0 as "no anchor", never as 1970.
type mainClock struct {
	mu sync.Mutex
	// mono is CLOCK_MONOTONIC in ns, boot this boot's id; nil takes
	// monotonicNs and bootID (tests replace them).
	mono func() int64
	boot func() string
	// seenMs is the highest main_time_ms an authenticated statement carried.
	seenMs int64
	// atMs is MAIN's time at the CLOCK_MONOTONIC reading atMono; 0: no anchor.
	atMs, atMono int64
}

// ClockMark is MAIN's clock as the agent held it when it last saved its state:
// MAIN's time MainMs at CLOCK_MONOTONIC reading MonoNs of boot BootID.
type ClockMark struct {
	MainMs int64  `json:"main_ms"`
	MonoNs int64  `json:"mono_ns"`
	BootID string `json:"boot_id"`
}

// ClockSaveEvery is how often the agent saves its MAIN clock in its state
// (State.MainAnchor), so a restart resumes it (mainClock).
var ClockSaveEvery = time.Minute

func (m *mainClock) monoNow() int64 {
	if m.mono != nil {
		return m.mono()
	}
	return monotonicNs()
}

func (m *mainClock) bootNow() string {
	if m.boot != nil {
		return m.boot()
	}
	return bootID()
}

// heard takes MAIN's time from an authenticated statement.
func (m *mainClock) heard(mainMs int64) {
	if mainMs <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if mainMs <= m.seenMs {
		return
	}
	m.seenMs, m.atMs, m.atMono = mainMs, mainMs, m.monoNow()
}

// nowMs is MAIN's time now as the anchor has it, or 0 without one.
func (m *mainClock) nowMs() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nowLocked(m.monoNow())
}

func (m *mainClock) nowLocked(mono int64) int64 {
	if m.atMs <= 0 {
		return 0
	}
	return m.atMs + max(0, mono-m.atMono)/int64(time.Millisecond)
}

// mark is what the state keeps of the clock (State.MainSeenMs and
// State.MainAnchor); a nil mark without an anchor.
func (m *mainClock) mark() (seenMs int64, mark *ClockMark) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mono := m.monoNow()
	if now := m.nowLocked(mono); now > 0 {
		mark = &ClockMark{MainMs: now, MonoNs: mono, BootID: m.bootNow()}
	}
	return m.seenMs, mark
}

// resume restores the clock a state kept. A mark of this boot counts the time
// since it on CLOCK_MONOTONIC; one of another boot (or with no boot id to
// compare) starts from the mark itself. It never resumes below the highest
// number MAIN was seen to say.
func (m *mainClock) resume(seenMs int64, mark *ClockMark) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seenMs = max(m.seenMs, seenMs)
	mono := m.monoNow()
	at := seenMs
	if mark != nil && mark.MainMs > 0 {
		from := mark.MainMs
		if boot := m.bootNow(); boot != "" && boot == mark.BootID && mono >= mark.MonoNs {
			from += (mono - mark.MonoNs) / int64(time.Millisecond)
		}
		at = max(at, from)
	}
	if at > m.nowLocked(mono) {
		m.atMs, m.atMono = at, mono
	}
}
