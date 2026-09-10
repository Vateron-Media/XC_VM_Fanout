//go:build unix

package supervisor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// shellLauncher runs one encoder command.
//
// The command arrives as a shell line because StreamProcess::buildLive composes
// one — quoted paths, a `-f tee` output whose argument contains pipes and
// brackets, `{...}` placeholders already substituted. Re-deriving that as an
// argv here is precisely the duplication this design exists to avoid, so it goes
// to /bin/sh.
//
// `exec` in front of it is what makes supervision work. Without it the daemon's
// child is the shell and ffmpeg is its grandchild: killing the child leaves
// ffmpeg orphaned and still writing, and the pid published to the panel is the
// shell's, not the encoder's. With `exec`, the shell replaces itself with
// ffmpeg, so the daemon's direct child IS the encoder — one pid, signals land
// where they should, and Wait reaps the thing that actually matters.
//
// The command must NOT carry buildLive's trailing `>/dev/null 2>>… & echo $! >…`
// tail: backgrounding it would return control immediately and leave nothing to
// supervise. Redirection and the pid file are the daemon's job now.
func shellLauncher(ctx context.Context, cmdline, stderrPath string) (Process, error) {
	if cmdline == "" {
		return nil, errors.New("empty command")
	}
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "exec "+cmdline)

	// Give the encoder its own process group, so a kill can take down anything
	// it spawned rather than leaving strays behind holding the source open.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var errFile *os.File
	if stderrPath != "" {
		f, err := os.OpenFile(stderrPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			errFile, cmd.Stderr = f, f
		}
		// A stderr file we cannot open is not a reason to refuse to start the
		// stream; it just means this run is not logged to it.
	}

	if err := cmd.Start(); err != nil {
		if errFile != nil {
			_ = errFile.Close()
		}
		return nil, err
	}
	return &execProcess{cmd: cmd, errFile: errFile}, nil
}

// execProcess adapts exec.Cmd to Process. Wait is guarded by a Once because the
// startup-confirmation path and the watch path both want to wait on the same
// process, and exec.Cmd allows Wait exactly once.
type execProcess struct {
	cmd     *exec.Cmd
	errFile *os.File

	once sync.Once
	err  error
	done chan struct{}
	init sync.Once
}

func (p *execProcess) Pid() int {
	if p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *execProcess) Wait() error {
	p.init.Do(func() { p.done = make(chan struct{}) })
	p.once.Do(func() {
		p.err = p.cmd.Wait()
		if p.errFile != nil {
			_ = p.errFile.Close()
		}
		close(p.done)
	})
	<-p.done
	return p.err
}

// Kill ends the encoder and everything it started, then lets Wait reap it.
// Safe on an already-dead process.
func (p *execProcess) Kill() {
	if p.cmd.Process == nil {
		return
	}
	pid := p.cmd.Process.Pid
	// Negative pid = the whole process group we created in Setpgid.
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		_ = p.cmd.Process.Kill()
	}
}

// shellProber runs a source-reachability probe and reports whether it
// succeeded. The command is the panel's own ffprobe invocation, carrying that
// stream's fetch arguments; a zero exit means the source answered with something
// ffprobe could read.
//
// Bounded twice over: by probeTimeout here and by ctx, so a hung probe can never
// stall the health loop that called it. Output is discarded — the only question
// being asked is whether the source is there.
func shellProber(ctx context.Context, cmd string) bool {
	if cmd == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	c := exec.CommandContext(ctx, "/bin/sh", "-c", cmd)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return c.Run() == nil
}

// findProcess reports a pid's command line and whether it is alive.
//
// Liveness is signal 0, which the kernel answers without delivering anything.
// The command line comes from /proc/<pid>/cmdline, whose arguments are
// NUL-separated; they are joined with spaces so a caller can match a substring
// against it. Both are needed together: a live pid alone proves nothing, because
// pids are recycled and the number in a stale pid file may belong to anything by
// the time the daemon comes back.
func findProcess(pid int) (string, bool) {
	if pid <= 0 {
		return "", false
	}
	// Signal 0 checks for existence and permission without touching the process.
	if err := syscall.Kill(pid, 0); err != nil {
		return "", false
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil {
		// Alive but unreadable (a different user, or no procfs). Report it alive
		// with no command line, so an AdoptMatch can never spuriously match.
		return "", true
	}
	return strings.ReplaceAll(strings.TrimRight(string(b), "\x00"), "\x00", " "), true
}

// killProcess ends an adopted encoder and the group it leads. An adopted process
// is not our child, so there is nothing to reap afterwards.
func killProcess(pid int) {
	if pid <= 0 {
		return
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}
