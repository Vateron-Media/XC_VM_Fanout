//go:build unix

// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// CommandLauncher builds the production no-shell launcher. The request's
// argv[0] only selects native-remux versus encoder; the executable path itself
// always comes from daemon startup configuration.
func CommandLauncher(encoder, fanout string) Launcher {
	return func(ctx context.Context, cmdline, stderrPath string) (Process, error) {
		argv, err := splitCommand(cmdline)
		if err != nil {
			return nil, err
		}
		bin := ""
		switch filepath.Base(argv[0]) {
		case filepath.Base(encoder):
			bin = encoder
		case "xc_fanout":
			bin = fanout
		}
		if bin == "" {
			return nil, fmt.Errorf("monitor executable %q is not allowed", filepath.Base(argv[0]))
		}
		return launchArgv(ctx, bin, argv[1:], stderrPath)
	}
}

// shellLauncher remains the package's test seam; production uses
// CommandLauncher with daemon-configured executable paths.
func shellLauncher(ctx context.Context, cmdline, stderrPath string) (Process, error) {
	argv, err := splitCommand(cmdline)
	if err != nil {
		return nil, err
	}
	return launchArgv(ctx, argv[0], argv[1:], stderrPath)
}

func launchArgv(ctx context.Context, bin string, args []string, stderrPath string) (Process, error) {
	// exec.Command, NOT exec.CommandContext. CommandContext kills the process
	// when ctx is cancelled, and this ctx dies with the supervisor -- so a daemon
	// shutdown would take every encoder on the node with it, and the survivor
	// adoption in adopt.go could never fire on the case it exists for: a planned
	// daemon upgrade. The encoder is killed explicitly (Kill, from Release or a
	// health verdict) and is otherwise left alone, so stopping the daemon leaves
	// the channels on air for the next one to adopt.
	_ = ctx
	cmd := exec.Command(bin, args...)

	// Give the encoder its own process group, so a kill can take down anything
	// it spawned rather than leaving strays behind holding the source open.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var errFile *os.File
	if stderrPath != "" {
		f, err := os.OpenFile(stderrPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err == nil {
			if err = f.Chmod(0o600); err == nil {
				errFile, cmd.Stderr = f, f
			} else {
				_ = f.Close()
			}
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

func splitCommand(line string) ([]string, error) {
	var out []string
	var word strings.Builder
	quote := byte(0)
	escaped := false
	flush := func() {
		if word.Len() > 0 {
			out = append(out, word.String())
			word.Reset()
		}
	}
	for i := 0; i < len(line); i++ {
		c := line[i]
		if escaped {
			word.WriteByte(c)
			escaped = false
			continue
		}
		if c == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if c == quote {
				quote = 0
			} else {
				word.WriteByte(c)
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case ' ', '\t', '\r', '\n':
			flush()
		case ';', '|', '&', '<', '>':
			return nil, fmt.Errorf("monitor command contains shell operator %q", c)
		default:
			word.WriteByte(c)
		}
	}
	if escaped || quote != 0 {
		return nil, errors.New("monitor command has incomplete quoting")
	}
	flush()
	if len(out) == 0 {
		return nil, errors.New("empty command")
	}
	return out, nil
}

// CommandProber builds the production no-shell probe. Only the ffprobe
// arguments supplied by the panel are retained; the executable is derived from
// the daemon-configured encoder path.
func CommandProber(encoder string) Prober {
	probe := filepath.Join(filepath.Dir(encoder), "ffprobe")
	if filepath.Dir(encoder) == "." {
		probe = "ffprobe"
	}
	return func(ctx context.Context, line string) bool {
		argv, err := splitCommand(line)
		if err != nil {
			return false
		}
		for i, arg := range argv {
			if filepath.Base(arg) == "ffprobe" {
				return runProbe(ctx, probe, argv[i+1:])
			}
		}
		return false
	}
}

func runProbe(ctx context.Context, bin string, args []string) bool {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	c := exec.CommandContext(ctx, bin, args...)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Cancel = func() error {
		if err := syscall.Kill(-c.Process.Pid, syscall.SIGKILL); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			return c.Process.Kill()
		}
		return nil
	}
	c.WaitDelay = probeWaitDelay
	return c.Run() == nil
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
	argv, err := splitCommand(cmd)
	if err != nil {
		return false
	}
	return runProbe(ctx, argv[0], argv[1:])
}

// probeWaitDelay bounds how long a cancelled probe may be waited on after its
// group has been signalled. A probe answers a yes/no question; nothing about it
// is worth holding the watch loop for.
const probeWaitDelay = 2 * time.Second

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
