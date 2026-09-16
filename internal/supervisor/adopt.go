// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"
)

// Re-adoption: picking up an encoder that outlived the daemon.
//
// A supervised encoder is the daemon's own child, which is what makes Wait and
// signals work. It also means a daemon that dies unexpectedly leaves those
// encoders orphaned: still running, still holding their source, no longer
// watched by anyone. On the next start the daemon would launch a SECOND encoder
// for the same stream, and the two would fight over the source while the panel
// saw whichever pid was written last.
//
// The fix is not to kill everything on start-up -- that would take every channel
// on the node off air for the length of a restart, turning a daemon upgrade into
// an outage. It is to recognise the survivor and resume watching it.
//
// A survivor cannot be Wait()ed on, because it is no longer our child; the
// kernel reparented it to init. So an adopted process is watched by polling its
// liveness instead. That is the whole difference, and it is why adoption has its
// own Process implementation rather than a flag on the normal one.

// adoptPollInterval is how often an adopted encoder is checked for liveness.
// Cheap (one signal-0 per stream) and well inside every health window.
const adoptPollInterval = time.Second

// ProcessFinder reports a process's command line and whether it is alive. It is
// an interface point so adoption can be tested without real processes.
type ProcessFinder func(pid int) (cmdline string, alive bool)

// adoptedProcess is an encoder the daemon did not start and cannot Wait on.
type adoptedProcess struct {
	pid    int
	find   ProcessFinder
	sleep  func(context.Context, time.Duration) bool
	kill   func(pid int)
	done   chan struct{}
	closer func()
}

func (p *adoptedProcess) Pid() int { return p.pid }

// Wait polls until the process is gone. It reports nil: an adopted encoder's
// exit status belongs to init, not to us, so "it ended" is all that can honestly
// be said about it.
func (p *adoptedProcess) Wait() error {
	for {
		if _, alive := p.find(p.pid); !alive {
			return nil
		}
		select {
		case <-p.done:
			return nil
		default:
		}
		if !p.sleep(context.Background(), adoptPollInterval) {
			return nil
		}
	}
}

func (p *adoptedProcess) Kill() {
	if p.closer != nil {
		p.closer()
	}
	p.kill(p.pid)
}

// readPID reads a pid file, or 0.
func readPID(path string) int {
	if path == "" {
		return 0
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

// adoptable reports whether the pid in the spec's pid file is an encoder this
// stream may resume watching.
//
// Both conditions matter. The pid must be alive, and its command line must carry
// the spec's AdoptMatch -- because pids are recycled, and adopting an unrelated
// process would mean the daemon watches the wrong thing forever while the real
// encoder runs unsupervised. AdoptMatch is supplied by the panel (which knows
// the paths that identify this stream's encoder) rather than guessed at here, so
// an empty one simply disables adoption instead of matching loosely.
// The command line comes back with the pid because it also says WHICH source the
// survivor is serving -- see matchRunningSource.
func adoptable(spec Spec, find ProcessFinder) (int, string, bool) {
	if spec.AdoptMatch == "" || find == nil {
		return 0, "", false
	}
	pid := readPID(spec.PIDPath)
	if pid <= 0 {
		return 0, "", false
	}
	cmdline, alive := find(pid)
	if !alive {
		return 0, "", false
	}
	if !strings.Contains(cmdline, spec.AdoptMatch) {
		return 0, "", false
	}
	return pid, cmdline, true
}

// matchRunningSource works out which of the spec's sources an adopted encoder is
// actually serving, by comparing its command line with the commands the panel
// handed us. It reports the index, whether that source's FallbackCmd is what is
// running, and false when nothing matches unambiguously.
//
// It matters because a survivor is not necessarily on source 0: the daemon that
// started it may have failed it over to a backup, and the spec the panel re-PUTs
// is still in priority order. Assuming the top source published the wrong
// current_source, made a force back to it a no-op ("already on it"), and left
// the priority-backup climb switched off, since that only runs below the top.
//
// The comparison is on normalised text because /proc/<pid>/cmdline is the argv
// the SHELL produced: the quoting and the whitespace runs of the panel's command
// line are gone from it. A survivor whose line matches nothing is adopted all
// the same and keeps the index the spec starts at -- not adopting it would put a
// second encoder on the source, which is the thing adoption exists to prevent.
func matchRunningSource(cmdline string, sources []Source) (int, bool, bool) {
	want := normaliseCmd(cmdline)
	if want == "" {
		return 0, false, false
	}
	idx, fallback, found := 0, false, false
	for i, s := range sources {
		for _, c := range []struct {
			cmd string
			fb  bool
		}{{s.Cmd, false}, {s.FallbackCmd, true}} {
			if c.cmd == "" || normaliseCmd(c.cmd) != want {
				continue
			}
			if found && (idx != i || fallback != c.fb) {
				// Two sources run the same command: which one is up is not
				// something this can answer, so it does not guess.
				return 0, false, false
			}
			idx, fallback, found = i, c.fb, true
		}
	}
	return idx, fallback, found
}

// normaliseCmd renders a command line the way both a shell command and the argv
// it turns into can be compared: without quotes, and with every run of
// whitespace as one space.
func normaliseCmd(s string) string {
	return strings.Join(strings.Fields(strings.NewReplacer("'", "", `"`, "").Replace(s)), " ")
}
