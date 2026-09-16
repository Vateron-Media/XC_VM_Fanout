// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"fmt"
	"time"
)

// Source-selection events, matching MonitorCommand.php's action names.
const (
	EventPrioritySwitch = "PRIORITY_SWITCH"
	EventForceSource    = "FORCE_SOURCE"
)

// probeTimeout bounds a single source-reachability probe. Generous enough for a
// slow origin to answer, short enough that a hung one cannot hold up the health
// loop for long.
const probeTimeout = 15 * time.Second

// Prober reports whether a source is currently reachable. cmd is a probe command
// line the panel composed (an ffprobe invocation with the stream's own fetch
// arguments); reachable means it exited successfully.
//
// It is a command rather than a URL for the same reason the encoder is: the
// panel knows the per-protocol arguments, the cookies, the user-agent and the
// proxy this source needs, and re-deriving any of that here would be a second
// implementation to keep in step.
type Prober func(ctx context.Context, cmd string) bool

// selectNextOnFailure returns the source index a new failure PASS starts at —
// where the list is picked up again once every source has been tried once (see
// advanceAfterFailure, which walks the list within a pass).
//
// It mirrors StreamProcess::rotateSourcesPastCurrent, which decides the same
// thing on the PHP side and whose two modes are the whole of the policy:
//
//   - priority backup ON: the list stays in priority order, so the next pass
//     starts again from the top. The highest-priority source is always
//     preferred, and backupSwitch below is what brings the stream back to it
//     later.
//   - priority backup OFF: rotate past the current one, so a retry moves on to
//     the next source instead of hammering the one that just failed.
func selectNextOnFailure(cur, n int, priorityBackup bool) int {
	if n <= 1 {
		return 0
	}
	if priorityBackup {
		return 0
	}
	return (cur + 1) % n
}

// backupSwitch looks for a higher-priority source that has come back, and
// returns its index or -1.
//
// Only sources ABOVE the current one are considered, and the first reachable one
// wins — the list is in priority order, so that is the best available. A source
// with no probe command cannot be tested and is skipped rather than switched to
// blindly: an unverified switch would take a working channel off a source that
// is at least delivering.
func backupSwitch(ctx context.Context, probe Prober, sources []Source, cur int) int {
	if probe == nil || cur <= 0 || cur > len(sources) {
		return -1
	}
	for i := 0; i < cur && i < len(sources); i++ {
		if sources[i].ProbeCmd == "" {
			continue
		}
		if probe(ctx, sources[i].ProbeCmd) {
			return i
		}
	}
	return -1
}

// ForceSource queues a switch to a specific source, replacing the panel's
// `<signals>/<id>.force` file. The switch takes effect on the next health tick:
// the running encoder is stopped and the stream restarted on the chosen source.
//
// An out-of-range index is refused rather than clamped — silently starting the
// wrong channel is worse than an error the caller can see.
func (s *Supervisor) ForceSource(id string, idx int) error {
	s.mu.Lock()
	st := s.procs[id]
	s.mu.Unlock()
	if st == nil {
		return fmt.Errorf("stream %s is not supervised here", id)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if idx < 0 || idx >= len(st.spec.Sources) {
		return fmt.Errorf("source %d out of range (stream has %d)", idx, len(st.spec.Sources))
	}
	if idx == st.srcIdx && st.running {
		// Already on it: restarting a working encoder onto the source it
		// already has would be an outage for nothing. Only while it RUNS,
		// though — a failing loop walks the list between attempts, so a force on
		// the index it happens to be sitting at this instant still has to be
		// queued, or the operator gets a 204 and a stream that walks off the
		// source they picked.
		return nil
	}
	st.forced = idx
	return nil
}

// takeForced consumes a queued forced switch, or returns -1.
func (st *stream) takeForced() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	idx := st.forced
	st.forced = -1
	return idx
}

// switchTo moves the stream onto a different source for its next start. The
// choice was made deliberately (an operator's force, a priority switch), so it
// also begins a fresh failure pass from there.
func (st *stream) switchTo(idx int) {
	st.mu.Lock()
	if idx >= 0 && idx < len(st.spec.Sources) {
		st.srcIdx = idx
		st.srcTried = 0
	}
	st.mu.Unlock()
}

// beginSourceWalk forgets how far the current failure pass had walked, so the
// next failed start begins a new one. Called when a start worked: the list is
// only walked to get OFF a source that will not start.
func (st *stream) beginSourceWalk() {
	st.mu.Lock()
	st.srcTried = 0
	st.mu.Unlock()
}

// advanceAfterFailure picks the source the next start attempt should use.
//
// One failed start does not end the list: a pass tries every source once before
// selectNextOnFailure decides where the next pass begins. The walk is what makes
// failover happen at all with priority backup on — that policy answers "the top"
// for every failure, so without it the same dead source is relaunched forever
// and the backup is never tried. PHP reached the backup a different way: its
// startStream probed each source in turn inside ONE start attempt, and the list
// order (which is all rotateSourcesPastCurrent decides) only chose where that
// walk began.
func (st *stream) advanceAfterFailure() {
	st.mu.Lock()
	defer st.mu.Unlock()
	n := len(st.spec.Sources)
	if n <= 1 {
		st.srcIdx = 0
		return
	}
	st.srcTried++
	if st.srcTried >= n {
		// Every source has now failed once: hand the choice back to the policy.
		st.srcTried = 0
		st.srcIdx = selectNextOnFailure(st.srcIdx, n, st.spec.Policy.PriorityBackupSec > 0)
		return
	}
	st.srcIdx = (st.srcIdx + 1) % n
}

// dueForBackupCheck reports whether it is time to look for a higher-priority
// source, and records that we looked. Off entirely when the interval is 0.
func (st *stream) dueForBackupCheck(now time.Time) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	every := st.spec.Policy.PriorityBackupSec
	if every <= 0 || st.srcIdx <= 0 {
		return false
	}
	if !st.backupCheckedAt.IsZero() && now.Sub(st.backupCheckedAt) < time.Duration(every)*time.Second {
		return false
	}
	st.backupCheckedAt = now
	return true
}
