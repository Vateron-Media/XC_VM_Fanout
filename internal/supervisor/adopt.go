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
func adoptable(spec Spec, find ProcessFinder) (int, bool) {
	if spec.AdoptMatch == "" || find == nil {
		return 0, false
	}
	pid := readPID(spec.PIDPath)
	if pid <= 0 {
		return 0, false
	}
	cmdline, alive := find(pid)
	if !alive {
		return 0, false
	}
	if !strings.Contains(cmdline, spec.AdoptMatch) {
		return 0, false
	}
	return pid, true
}
