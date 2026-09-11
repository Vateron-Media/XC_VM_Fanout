// Package supervisor owns the lifetime of a stream's encoder process.
//
// It is the daemon-side half of XC_VM's per-stream watchdog (the panel's
// `console.php monitor <id>`, MonitorCommand.php): one long-lived PHP process
// per stream, started for every channel on the node, whose whole job was to
// start an ffmpeg, notice when it died or went bad, and start it again. On a
// 500-channel node that is 500 resident PHP processes doing what one supervisor
// can do here, next to the bytes it is already fanning out.
//
// The split with PHP is deliberate and is forced by a real constraint: the
// panel's database credentials live inside its compiled C extension and are
// never handed out (see docs/adr/0002-monitor-in-daemon.md), so this package
// cannot and does not read the panel's configuration. PHP keeps building the
// ffmpeg command line from the database — the daemon only ever RUNS a command
// it was handed, never composes one — and PHP stays the system of record for
// every database write. What moves here is the process and the watching of it.
package supervisor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
)

// Event names, byte-identical to the actions MonitorCommand.php passes to
// StreamProcess::streamLog, so the panel's existing log drain keeps working
// without a PHP change. Only the ones M1 can emit are listed; the health and
// source-switch events arrive with their phases.
const (
	EventStreamStart     = "STREAM_START"
	EventStreamRestart   = "STREAM_RESTART"
	EventStreamFailed    = "STREAM_FAILED"
	EventStreamStartFail = "STREAM_START_FAIL"
)

// Source is one candidate feed for a stream: the panel's own label for it (the
// source URL, or "Loopback: #<id>" — whatever it wants echoed back in logs and
// in `streams_servers.current_source`) and the command that serves it.
//
// Cmd is a shell command line, because that is what StreamProcess::buildLive
// (ffmpeg) and StreamProcess::buildNativeLive (`xc_fanout remux`) produce and
// re-deriving it here is exactly what this design avoids. It must arrive
// WITHOUT buildLive's trailing redirect-and-background tail: the daemon
// supervises the process, so it needs to be its parent and to reap it itself.
type Source struct {
	Label string `json:"label"`
	Cmd   string `json:"cmd"`
	// FallbackCmd is run in Cmd's place once Cmd has exited ExitUnsupported —
	// the same source, through a pipeline that can take it. The panel sets it
	// when Cmd is the native remuxer and the node's source backend allows an
	// ffmpeg fallback ("auto"); it is empty everywhere else, and then an
	// ExitUnsupported is an ordinary failure. The daemon never derives one.
	FallbackCmd string `json:"fallback_cmd,omitempty"`
	// ProbeCmd tests whether this source is reachable WITHOUT starting it, for
	// the priority-backup check. Optional: a source without one is never
	// switched TO speculatively, only used when the list is walked on failure.
	ProbeCmd string `json:"probe_cmd,omitempty"`
}

// Policy carries the panel settings the restart loop obeys. The names mirror the
// panel settings they come from so an operator can trace a behaviour back to the
// knob that caused it.
type Policy struct {
	// StopFailures is how many consecutive failed starts end the supervision
	// (0 = never give up), from the `stop_failures` setting.
	StopFailures int `json:"stop_failures"`
	// StreamFailSleepSec is the wait between start attempts (`stream_fail_sleep`).
	StreamFailSleepSec int `json:"stream_fail_sleep"`
	// OnDemand marks a stream started for a viewer rather than kept up, and
	// OnDemandFailureExit (`on_demand_failure_exit`) gives up on the first failed
	// start of one instead of retrying at a viewer's expense.
	OnDemand            bool `json:"on_demand"`
	OnDemandFailureExit bool `json:"on_demand_failure_exit"`
	// StartTimeoutSec bounds how long a freshly-started process has to actually
	// produce data before the start counts as failed. This replaces the panel's
	// "wait for the .m3u8 to appear, up to N checks" loop: the daemon does not
	// need a file to appear, it can see whether bytes arrived.
	StartTimeoutSec int `json:"start_timeout_sec"`
	// PriorityBackupSec is how often to check whether a higher-priority source
	// has come back, so a stream running on a backup returns to its preferred
	// feed (0 = off). The panel's `priority_backup`, whose interval was 300s.
	//
	// It also decides what a failed start falls back to: with priority backup on
	// the list stays in priority order and a retry starts from the top; without
	// it the retry rotates past the source that just failed. See
	// selectNextOnFailure, and StreamProcess::rotateSourcesPastCurrent.
	PriorityBackupSec int `json:"priority_backup_sec"`
}

// Spec is what PHP PUTs to /monitor/<id> to hand a stream over.
type Spec struct {
	Sources []Source `json:"sources"`
	Policy  Policy   `json:"policy"`
	// Health is the watchdog policy applied while the encoder runs. Omitted or
	// zero means the process is supervised and nothing about its OUTPUT is
	// judged, which is the safe default for a stream whose panel settings have
	// not been mapped over yet.
	Health Health `json:"health"`

	// PIDPath is the panel's `<streams>/<id>_.pid`. The daemon keeps writing it
	// because ProcessManager::isStreamRunning and stopStream still read it, so a
	// supervised stream stays visible to every PHP path that has not moved yet.
	PIDPath string `json:"pid_path"`
	// ErrorsPath is the panel's `<streams>/<id>.errors`, where buildLive used to
	// append ffmpeg's stderr. Keeping it means existing ops tooling still works.
	ErrorsPath string `json:"errors_path"`
	// LogPath is the panel's `<logs>/stream_log.log`. StreamProcess::streamLog
	// appends base64'd JSON lines there and a cron drains them into the database,
	// which is how the daemon emits events without a database connection.
	LogPath string `json:"log_path"`
	// ServerID is stamped into those events (the panel's SERVER_ID).
	ServerID int `json:"server_id"`

	// AdoptMatch lets the daemon resume watching an encoder that outlived a
	// daemon restart instead of starting a second one alongside it: the pid in
	// PIDPath is adopted when it is alive AND its command line contains this
	// string. The panel supplies it because it knows the paths that identify
	// this stream's encoder; empty disables adoption entirely, which is the
	// safe default. See adopt.go.
	AdoptMatch string `json:"adopt_match,omitempty"`
}

// normalise fills in the defaults the panel would otherwise have to repeat and
// clamps the values a typo could make pathological.
func (s *Spec) normalise() {
	if s.Policy.StreamFailSleepSec <= 0 {
		s.Policy.StreamFailSleepSec = 5
	}
	if s.Policy.StartTimeoutSec <= 0 {
		s.Policy.StartTimeoutSec = 30
	}
	if s.Policy.StopFailures < 0 {
		s.Policy.StopFailures = 0
	}
}

// valid reports whether a spec can be supervised at all.
func (s *Spec) valid() error {
	if len(s.Sources) == 0 {
		return errors.New("spec carries no sources")
	}
	for i, src := range s.Sources {
		if src.Cmd == "" {
			return fmt.Errorf("source %d has an empty command", i)
		}
	}
	return nil
}

// State is the answer to GET /monitor/<id>: what PHP needs to reconcile
// `streams_servers` without owning the process any more.
type State struct {
	Supervised bool `json:"supervised"`
	Running    bool `json:"running"`
	// Confirmed means the running producer has actually delivered bytes to
	// this daemon — the start was confirmed, not merely launched. Running
	// without Confirmed is a start in progress: the panel's "starting" state.
	Confirmed bool   `json:"confirmed"`
	PID       int    `json:"pid"`
	Source    string `json:"source"`
	SourceIdx int    `json:"source_idx"`
	Restarts  int    `json:"restarts"`
	Failures  int    `json:"failures"`
	UptimeMS  int64  `json:"uptime_ms"`
	GaveUp    bool   `json:"gave_up"`
	// Adopted means this encoder was inherited from a previous daemon life
	// rather than started by this one.
	Adopted bool `json:"adopted"`
	// Fallback means the running process is the current source's FallbackCmd:
	// its Cmd declared it could not serve the source (ExitUnsupported).
	Fallback  bool   `json:"fallback"`
	LastError string `json:"last_error"`
	// DaemonPID is this daemon's own pid. The panel records it as the stream's
	// monitor_pid — the daemon is the monitor now — so its "is anything
	// watching this stream?" bookkeeping has a live process to point at.
	DaemonPID int `json:"daemon_pid"`
}

// ExitUnsupported is the exit status by which a command declares that it cannot
// serve its source AT ALL — as opposed to failing to reach it. `xc_fanout remux`
// exits with it for a source its native reader does not take (fMP4 HLS, RTMP…),
// and within a second or two of starting, so the decision costs no start timeout.
//
// It is the one exit status the supervisor reads, and only when the source has a
// FallbackCmd: that source switches to the fallback for the rest of this spec's
// life, without the failure counting towards stop_failures. Any other exit is an
// ordinary failure and walks the source list as before.
const ExitUnsupported = 3

// Process is a running encoder. It exists so the restart loop can be tested
// without spawning anything: the real implementation wraps exec.Cmd, and tests
// supply one that is entirely in-process.
type Process interface {
	// Pid is the process id to publish (to the panel's pid file, and to anything
	// still reconciling on it).
	Pid() int
	// Wait blocks until the process exits and reports why. It MUST be safe to
	// call more than once and MUST return the same answer each time: the watch
	// loop waits on the process, and a health verdict then kills it and waits
	// again to reap it. A one-shot Wait deadlocks the second caller.
	Wait() error
	// Kill ends it, and must be safe to call after it has already exited.
	Kill()
}

// Launcher starts one encoder. stderrPath, when set, is opened in append mode
// and given to the process, matching where buildLive used to redirect it.
type Launcher func(ctx context.Context, cmdline, stderrPath string) (Process, error)

// Supervisor holds every stream this node is supervising.
type Supervisor struct {
	mu    sync.Mutex
	procs map[string]*stream

	launch  Launcher
	hasData func(id string) bool // "did bytes actually arrive for this stream?"
	vitals  VitalsFunc           // what the output looks like right now
	probe   Prober               // is a source reachable, without starting it
	find    ProcessFinder        // is this pid alive, and what is it running
	killPID func(pid int)        // end an adopted (non-child) process
	now     func() time.Time
	sleep   func(context.Context, time.Duration) bool

	// healthTick is how often a running stream is judged. A minute is the
	// coarsest it may be: the panel's scheduled auto-restart matches on HH:MM, so
	// a slower tick would step over the minute it is meant to fire in.
	healthTick time.Duration
}

// New returns a Supervisor. hasData is how a start is confirmed — the daemon
// asks its own registry whether the stream has produced bytes, which is the
// direct form of the question the panel could only answer by polling for a
// playlist file. A nil hasData treats a live process as started.
func New(launch Launcher, hasData func(id string) bool) *Supervisor {
	if launch == nil {
		launch = shellLauncher
	}
	if hasData == nil {
		hasData = func(string) bool { return true }
	}
	return &Supervisor{
		procs:      make(map[string]*stream),
		launch:     launch,
		hasData:    hasData,
		now:        time.Now,
		sleep:      sleepCtx,
		healthTick: 5 * time.Second,
		probe:      shellProber,
		find:       findProcess,
		killPID:    killProcess,
	}
}

// WithVitals supplies the stream-health source the watch loop judges against.
// Without it the supervisor still starts, watches and restarts encoders — it
// just cannot see whether the output is any good, so every Health rule is inert.
func (s *Supervisor) WithVitals(v VitalsFunc) *Supervisor {
	s.vitals = v
	return s
}

// WithProber supplies the source-reachability check the priority-backup switch
// needs. Without one, a stream never leaves a working source speculatively — it
// still fails over when a start fails, which needs no probe.
func (s *Supervisor) WithProber(p Prober) *Supervisor {
	s.probe = p
	return s
}

// sleepCtx waits for d, or returns false as soon as ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// stream is one supervised stream: its spec, the goroutine running the restart
// loop, and the state that loop publishes.
type stream struct {
	id     string
	sup    *Supervisor
	cancel context.CancelFunc
	done   chan struct{}

	mu   sync.Mutex
	spec Spec
	proc Process
	pid  int
	// autoRestartAt is when the scheduled restart last fired. The schedule
	// matches on HH:MM, so without this it keeps matching for the rest of that
	// minute and the stream restarts in a tight loop until the clock moves on.
	// (MonitorCommand.php has the same shape and only escaped it because its
	// restart path was slow enough to usually leave the minute — luck, not
	// design, and the daemon restarts far too quickly to rely on it.)
	autoRestartAt time.Time

	// lastTick/lastVitals/fpsPeak are the most recent health judgement's INPUTS,
	// kept so the debug snapshot can report what the watchdog is actually
	// seeing. Without them the monitor is only visible when it acts, so "is it
	// running at all, and what does it think?" has no answer short of waiting
	// for a restart.
	lastTick   time.Time
	lastVitals Vitals
	fpsPeak    float64

	// srcIdx is the source the next start uses; forced is a queued switch from
	// ForceSource (-1 = none); backupCheckedAt paces the priority-backup probe.
	srcIdx          int
	forced          int
	backupCheckedAt time.Time

	// adopted records that the RUNNING encoder was inherited, not started here.
	adopted bool
	// fallback holds the source indexes whose Cmd exited ExitUnsupported and
	// which now run their FallbackCmd; usingFallback says the RUNNING process is
	// one of those. Sticky for the life of the spec: a source that could not be
	// served natively once will not start being servable on a retry, and
	// re-trying it on every restart would add a failed start to each of them.
	fallback      map[int]bool
	usingFallback bool
	// confirmed: the running producer's start was confirmed by data arriving.
	confirmed bool
	source    string
	started   time.Time
	running   bool
	gaveUp    bool
	fails     int
	starts    int
	lastErr   string
}

// Supervise takes over (or re-takes) a stream. Re-supervising a stream that is
// already running replaces its spec and restarts it, which is what a PUT after a
// source change means. Returns an error only for a spec that cannot be run.
func (s *Supervisor) Supervise(id string, spec Spec) error {
	spec.normalise()
	if err := spec.valid(); err != nil {
		return err
	}

	s.mu.Lock()
	if old := s.procs[id]; old != nil {
		delete(s.procs, id)
		s.mu.Unlock()
		old.stop() // outside the lock: stop() waits for the loop to unwind
		s.mu.Lock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	// forced starts at -1: zero is a valid source index, so it cannot double as
	// "nothing queued".
	st := &stream{id: id, sup: s, cancel: cancel, done: make(chan struct{}), spec: spec, forced: -1}
	s.procs[id] = st
	s.mu.Unlock()

	dlog.Logf("monitor", "id=%s supervising: %d source(s), stop_failures=%d fail_sleep=%ds start_timeout=%ds",
		id, len(spec.Sources), spec.Policy.StopFailures, spec.Policy.StreamFailSleepSec, spec.Policy.StartTimeoutSec)
	go st.run(ctx)
	return nil
}

// Release stops supervising a stream and kills its process. Idempotent.
func (s *Supervisor) Release(id string) bool {
	s.mu.Lock()
	st := s.procs[id]
	delete(s.procs, id)
	s.mu.Unlock()
	if st == nil {
		return false
	}
	st.stop()
	dlog.Logf("monitor", "id=%s released", id)
	return true
}

// DetachAll stops watching every stream WITHOUT stopping its encoder, for a
// daemon shutdown or upgrade.
//
// This is the counterpart to adoption and the reason it is worth having. Killing
// the encoders on the way out would take every channel on the node off air for
// the length of a restart; leaving them running means the next daemon adopts
// them and the viewers never notice. The pid files are left in place because
// that is how the next daemon finds them.
func (s *Supervisor) DetachAll() int {
	s.mu.Lock()
	all := make([]*stream, 0, len(s.procs))
	for _, st := range s.procs {
		all = append(all, st)
	}
	s.procs = make(map[string]*stream)
	s.mu.Unlock()
	for _, st := range all {
		st.detach()
	}
	return len(all)
}

// ReleaseAll stops every supervised stream AND kills its encoder. For tests and
// explicit teardown; a shutdown wants DetachAll.
func (s *Supervisor) ReleaseAll() {
	s.mu.Lock()
	all := make([]*stream, 0, len(s.procs))
	for _, st := range s.procs {
		all = append(all, st)
	}
	s.procs = make(map[string]*stream)
	s.mu.Unlock()
	for _, st := range all {
		st.stop()
	}
}

// State reports a stream's supervision state; Supervised is false when this node
// is not supervising it at all.
func (s *Supervisor) State(id string) State {
	s.mu.Lock()
	st := s.procs[id]
	s.mu.Unlock()
	if st == nil {
		return State{}
	}
	return st.state()
}

// IDs lists the supervised stream ids.
func (s *Supervisor) IDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.procs))
	for id := range s.procs {
		out = append(out, id)
	}
	return out
}

func (st *stream) state() State {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := State{
		Supervised: true,
		Running:    st.running,
		Confirmed:  st.running && st.confirmed,
		PID:        st.pid,
		Source:     st.source,
		SourceIdx:  st.srcIdx,
		Restarts:   st.starts,
		Failures:   st.fails,
		GaveUp:     st.gaveUp,
		Adopted:    st.adopted,
		Fallback:   st.usingFallback,
		LastError:  st.lastErr,
		DaemonPID:  os.Getpid(),
	}
	if st.running && !st.started.IsZero() {
		out.UptimeMS = st.sup.now().Sub(st.started).Milliseconds()
	}
	return out
}

// detach stops the watch loop and leaves the encoder running. The pid file stays
// too: it is what the next daemon reads to find the survivor.
func (st *stream) detach() {
	st.cancel()
	<-st.done
}

// stop cancels the loop, kills the process and waits for the loop to finish, so
// a caller that stops a stream knows nothing of it is still running afterwards.
func (st *stream) stop() {
	st.cancel()
	st.mu.Lock()
	p := st.proc
	st.mu.Unlock()
	if p != nil {
		p.Kill()
	}
	<-st.done
	st.clearPIDFile()
}

// run is the restart loop, structurally the same decision tree as
// MonitorCommand's: start, confirm the start really produced output, watch until
// the process exits, then start again — with the panel's give-up rules
// (stop_failures, and on_demand_failure_exit for a stream nobody asked to keep
// up) deciding when to stop trying.
func (st *stream) run(ctx context.Context) {
	defer close(st.done)

	consecutiveFails := 0
	first := true

	for ctx.Err() == nil {
		src := st.currentSource()

		proc, err := st.startOnce(ctx, src)
		if err != nil {
			// The command could not serve this source at all, and the panel gave
			// it a fallback: that is a choice of pipeline, not a failed start, so
			// it neither counts towards stop_failures nor waits out the fail sleep.
			if errors.Is(err, errUnsupported) && st.switchToFallback(src) {
				dlog.Logf("monitor", "id=%s %v; switching source %q to its fallback command", st.id, err, src.Label)
				continue
			}
			consecutiveFails++
			st.note(err.Error(), consecutiveFails)
			st.emit(EventStreamStartFail, src.Label)
			// Try a different feed next time rather than hammering the one that
			// just failed — which of them depends on the priority-backup mode.
			st.advanceAfterFailure()
			dlog.Logf("monitor", "id=%s start failed (%d): %v", st.id, consecutiveFails, err)

			pol := st.policy()
			if pol.OnDemand && pol.OnDemandFailureExit {
				dlog.Logf("monitor", "id=%s on-demand start failed; giving up", st.id)
				st.giveUp()
				return
			}
			if pol.StopFailures > 0 && consecutiveFails >= pol.StopFailures {
				dlog.Logf("monitor", "id=%s failure limit %d reached; giving up", st.id, pol.StopFailures)
				st.giveUp()
				return
			}
			if !st.sup.sleep(ctx, time.Duration(pol.StreamFailSleepSec)*time.Second) {
				return
			}
			continue
		}

		consecutiveFails = 0
		st.markConfirmed()
		if first {
			st.emit(EventStreamStart, src.Label)
			first = false
		} else {
			st.emit(EventStreamRestart, src.Label)
		}
		dlog.Logf("monitor", "id=%s started pid=%d source=%q", st.id, proc.Pid(), src.Label)

		// Watch until the encoder exits or a health rule condemns it.
		verdict := st.watch(ctx, proc)
		st.markStopped(nil)

		if ctx.Err() != nil {
			return // we ended it (Release / shutdown), not a fault
		}
		if verdict.failed() {
			// Condemned rather than crashed: kill it before restarting, or the
			// old encoder keeps running and two of them fight over the source.
			proc.Kill()
			proc.Wait()
			st.note(verdict.reason, 0)
			// A source switch is logged against the source being switched TO,
			// which is what the panel records in current_source; every other
			// verdict is about the source that just failed.
			logSource := src.Label
			if verdict.switchSource {
				if next, ok := st.sourceAt(verdict.switchTo); ok {
					logSource = next.Label
				}
				st.switchTo(verdict.switchTo)
			}
			st.emit(verdict.event, logSource)
			dlog.Logf("monitor", "id=%s %s: %s; restarting", st.id, verdict.event, verdict.reason)
		} else {
			// Exited on its own. A remuxer can only discover some sources are not
			// servable once it is reading them (an HLS that turns fMP4 mid-life);
			// it says so the same way it does at startup.
			if isUnsupportedExit(proc.Wait()) && st.switchToFallback(src) {
				dlog.Logf("monitor", "id=%s source %q became unservable by its command; switching to its fallback", st.id, src.Label)
				continue
			}
			st.emit(EventStreamFailed, src.Label)
			dlog.Logf("monitor", "id=%s process exited; restarting", st.id)
		}

		if !st.sup.sleep(ctx, time.Duration(st.policy().StreamFailSleepSec)*time.Second) {
			return
		}
	}
}

// watch blocks until the encoder exits on its own (a zero verdict) or a health
// rule fires (a verdict naming it). It is the daemon-side replacement for the
// inner monitoring loop of MonitorCommand.php — the same conditions, judged
// against bytes the daemon has already parsed instead of against ffprobe runs,
// playlist hashes and a progress file on disk.
func (st *stream) watch(ctx context.Context, proc Process) healthVerdict {
	exited := make(chan error, 1)
	go func() { exited <- proc.Wait() }()

	health := st.spec_().Health
	base := &fpsBaseline{}
	startedAt := st.startTime()
	sup := st.sup

	// Nothing to judge: just wait for the process.
	if sup.vitals == nil {
		select {
		case <-exited:
		case <-ctx.Done():
		}
		return healthVerdict{}
	}

	tick := time.NewTicker(sup.healthTick)
	defer tick.Stop()
	for {
		select {
		case <-exited:
			return healthVerdict{}
		case <-ctx.Done():
			return healthVerdict{}
		case <-tick.C:
		}

		now := sup.now()

		// The scheduled restart is not a fault, but it is a restart, and the
		// panel logs it as its own action. Once per window: see autoRestartAt.
		if health.AutoRestart.due(now) && !st.autoRestartedIn(now) {
			st.markAutoRestart(now)
			return healthVerdict{event: EventAutoRestart, reason: "scheduled auto-restart"}
		}

		// An operator asked for a specific source: that outranks everything,
		// including whether the current one looks healthy.
		if idx := st.takeForced(); idx >= 0 {
			return healthVerdict{
				event:        EventForceSource,
				reason:       fmt.Sprintf("forced switch to source %d", idx),
				switchSource: true,
				switchTo:     idx,
			}
		}

		// Running on a backup: has the preferred feed come back?
		if st.dueForBackupCheck(now) {
			spec := st.spec_()
			if idx := backupSwitch(ctx, sup.probe, spec.Sources, st.sourceIndex()); idx >= 0 {
				return healthVerdict{
					event:        EventPrioritySwitch,
					reason:       fmt.Sprintf("higher-priority source %d is reachable again", idx),
					switchSource: true,
					switchTo:     idx,
				}
			}
		}

		v, ok := sup.vitals(st.id)
		if !ok {
			continue // the stream is not registered with the daemon (yet)
		}
		st.recordTick(now, v, base.peak)
		if verdict := health.check(now, startedAt, v, base); verdict.failed() {
			return verdict
		}
	}
}

// errUnsupported marks a start that failed because the command exited
// ExitUnsupported: it cannot serve this source, as distinct from not reaching it.
var errUnsupported = errors.New("command cannot serve this source")

// isUnsupportedExit reports whether a process's exit error carries
// ExitUnsupported. exec.ExitError exposes the status through ExitCode, and so
// may a test Process's error.
func isUnsupportedExit(err error) bool {
	var ec interface{ ExitCode() int }
	return errors.As(err, &ec) && ec.ExitCode() == ExitUnsupported
}

// commandFor picks what to run for src: its FallbackCmd once its Cmd has
// declared the source unservable, otherwise its Cmd. Reports which it chose.
func (st *stream) commandFor(src Source) (string, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if src.FallbackCmd != "" && st.fallback[st.srcIdx] {
		return src.FallbackCmd, true
	}
	return src.Cmd, false
}

// switchToFallback moves the current source onto its FallbackCmd, reporting
// false when there is nothing to switch to — no fallback, or already on it — in
// which case the caller treats the exit as the ordinary failure it then is.
func (st *stream) switchToFallback(src Source) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if src.FallbackCmd == "" || st.fallback[st.srcIdx] {
		return false
	}
	if st.fallback == nil {
		st.fallback = make(map[int]bool)
	}
	st.fallback[st.srcIdx] = true
	return true
}

// markFallback records whether the RUNNING process is a fallback command.
func (st *stream) markFallback(v bool) {
	st.mu.Lock()
	st.usingFallback = v
	st.mu.Unlock()
}

// recordTick stores what the watchdog just measured, for DebugLines.
func (st *stream) recordTick(now time.Time, v Vitals, peak float64) {
	st.mu.Lock()
	st.lastTick, st.lastVitals, st.fpsPeak = now, v, peak
	st.mu.Unlock()
}

// DebugLines narrates every supervised stream: how it is being produced, and
// what the watchdog is measuring against which rules.
//
// This exists because supervision is otherwise only visible when it ACTS. A
// healthy node logs nothing, which is indistinguishable from a watchdog that
// has silently stopped ticking — and the two call for opposite responses. The
// tick age is the load-bearing field: a stale one says the loop is stuck, not
// that the stream is fine.
func (s *Supervisor) DebugLines() []string {
	s.mu.Lock()
	all := make([]*stream, 0, len(s.procs))
	for _, st := range s.procs {
		all = append(all, st)
	}
	s.mu.Unlock()

	sort.Slice(all, func(i, j int) bool { return all[i].id < all[j].id })
	out := make([]string, 0, len(all))
	for _, st := range all {
		out = append(out, st.debugLine(s.now()))
	}
	return out
}

func (st *stream) debugLine(now time.Time) string {
	st.mu.Lock()
	defer st.mu.Unlock()

	producer := "cmd"
	switch {
	case st.adopted:
		producer = "adopted"
	case st.usingFallback:
		producer = "fallback"
	}
	uptime := "-"
	if st.running && !st.started.IsZero() {
		uptime = now.Sub(st.started).Round(time.Second).String()
	}

	var b strings.Builder
	fmt.Fprintf(&b, "id=%s producer=%s running=%v pid=%d src=%d:%q up=%s starts=%d fails=%d",
		st.id, producer, st.running, st.pid, st.srcIdx, st.source, uptime, st.starts, st.fails)
	if st.gaveUp {
		b.WriteString(" GAVE_UP")
	}
	if st.lastErr != "" {
		fmt.Fprintf(&b, " last_err=%q", st.lastErr)
	}

	if st.lastTick.IsZero() {
		b.WriteString(" | watchdog: no tick yet")
		return b.String()
	}
	h := st.spec.Health
	fmt.Fprintf(&b, " | tick_age=%s", now.Sub(st.lastTick).Round(time.Second))
	fmt.Fprintf(&b, " data_age=%s/%s", since(now, st.lastVitals.LastData), limitOrOff(h.StallSec))
	fmt.Fprintf(&b, " audio_age=%s/%s", since(now, st.lastVitals.LastAudio), limitOrOff(h.AudioLossSec))
	if h.FPSThreshold > 0 {
		fmt.Fprintf(&b, " fps=%.1f/%.1f×%.0f%%", st.lastVitals.FPS, st.fpsPeak, h.FPSThreshold*100)
	} else {
		fmt.Fprintf(&b, " fps=%.1f/off", st.lastVitals.FPS)
	}
	return b.String()
}

// since renders an age, or "never" for a zero timestamp — which is NOT the same
// as a long age: a stream that never had audio has none to lose.
func since(now, t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return now.Sub(t).Round(time.Second).String()
}

// limitOrOff renders a threshold in seconds, or "off" for a disabled check, so
// a line shows which rules are actually armed.
func limitOrOff(sec int) string {
	if sec <= 0 {
		return "off"
	}
	return strconv.Itoa(sec) + "s"
}

// startOnce launches the encoder and confirms it actually began producing. A
// process that starts and then sits there yielding nothing is a failed start,
// not a running stream — the panel learned this by waiting for a playlist file
// to appear; here the daemon asks whether bytes arrived, which is the same
// question without the filesystem in the middle.
func (st *stream) startOnce(ctx context.Context, src Source) (Process, error) {
	spec := st.spec_()

	// An encoder that outlived a previous daemon is resumed rather than
	// duplicated. Launching alongside it would put two encoders on one source,
	// and killing it on sight would take the channel off air for the length of
	// every daemon restart.
	if pid, ok := adoptable(spec, st.sup.find); ok {
		dlog.Logf("monitor", "id=%s adopting encoder pid=%d that outlived the daemon", st.id, pid)
		proc := &adoptedProcess{
			pid:   pid,
			find:  st.sup.find,
			sleep: st.sup.sleep,
			kill:  st.sup.killPID,
			done:  make(chan struct{}),
		}
		st.markAdopted(true)
		st.markFallback(false)
		st.markStarted(proc, src.Label)
		return proc, nil
	}
	st.markAdopted(false)

	cmd, fallback := st.commandFor(src)
	proc, err := st.sup.launch(ctx, cmd, spec.ErrorsPath)
	if err != nil {
		return nil, fmt.Errorf("launch: %w", err)
	}
	st.markFallback(fallback)

	// Publish the pid BEFORE announcing the state. PHP polls GET /monitor/<id>
	// and then reads <streams>/<id>_.pid; announcing "running" first leaves a
	// window where it sees a live stream whose pid file is not there yet.
	st.writePIDFile(proc.Pid())
	st.markStarted(proc, src.Label)

	// The process may die on its own during the confirmation window; watch for
	// that as well as for data, so a command that exits instantly is reported as
	// a failed start rather than waited out.
	exited := make(chan error, 1)
	go func() { exited <- proc.Wait() }()

	deadline := st.sup.now().Add(time.Duration(spec.Policy.StartTimeoutSec) * time.Second)
	for {
		select {
		case werr := <-exited:
			st.markStopped(werr)
			if isUnsupportedExit(werr) {
				return nil, fmt.Errorf("%w (exited during startup: %v)", errUnsupported, werr)
			}
			return nil, fmt.Errorf("exited during startup: %v", werr)
		case <-ctx.Done():
			proc.Kill()
			return nil, ctx.Err()
		default:
		}

		if st.sup.hasData(st.id) {
			// Hand the already-running Wait to the caller rather than starting a
			// second one on the underlying process.
			return newWaitedProcess(proc, exited), nil
		}
		if !st.sup.now().Before(deadline) {
			proc.Kill()
			<-exited
			st.markStopped(nil)
			return nil, fmt.Errorf("no data within %ds of start", spec.Policy.StartTimeoutSec)
		}
		if !st.sup.sleep(ctx, 200*time.Millisecond) {
			proc.Kill()
			<-exited
			return nil, ctx.Err()
		}
	}
}

// waitedProcess adapts a process whose Wait is already in flight (started during
// the confirmation window) so later callers can still wait on it.
//
// The result is latched: the channel carries exactly one value, and both the
// watch loop and the post-kill reap wait on this. Reading the channel directly
// would let the first caller consume the only value and leave the second blocked
// forever — which is precisely the deadlock this shape exists to prevent.
type waitedProcess struct {
	Process
	wait chan error

	once sync.Once
	done chan struct{}
	err  error
}

func newWaitedProcess(p Process, wait chan error) *waitedProcess {
	return &waitedProcess{Process: p, wait: wait, done: make(chan struct{})}
}

func (w *waitedProcess) Wait() error {
	w.once.Do(func() {
		w.err = <-w.wait
		close(w.done)
	})
	<-w.done
	return w.err
}

// ── state helpers ──────────────────────────────────────────────────────────

func (st *stream) spec_() Spec {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.spec
}

func (st *stream) policy() Policy {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.spec.Policy
}

// currentSource is the source the next start should use.
func (st *stream) currentSource() Source {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.srcIdx < 0 || st.srcIdx >= len(st.spec.Sources) {
		st.srcIdx = 0
	}
	return st.spec.Sources[st.srcIdx]
}

func (st *stream) markStarted(p Process, label string) {
	st.mu.Lock()
	st.proc, st.pid, st.source = p, p.Pid(), label
	st.started, st.running, st.confirmed = st.sup.now(), true, false
	st.starts++
	st.mu.Unlock()
}

// markConfirmed records that the running producer's start was confirmed.
func (st *stream) markConfirmed() {
	st.mu.Lock()
	st.confirmed = true
	st.mu.Unlock()
}

func (st *stream) markStopped(err error) {
	st.mu.Lock()
	st.running, st.proc, st.pid, st.confirmed = false, nil, 0, false
	if err != nil {
		st.lastErr = err.Error()
	}
	st.mu.Unlock()
}

// autoRestartedIn reports whether the scheduled restart has already fired in the
// same minute as now — the granularity the panel's schedule is expressed at.
func (st *stream) autoRestartedIn(now time.Time) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return !st.autoRestartAt.IsZero() && st.autoRestartAt.Truncate(time.Minute).Equal(now.Truncate(time.Minute))
}

func (st *stream) markAutoRestart(now time.Time) {
	st.mu.Lock()
	st.autoRestartAt = now
	st.mu.Unlock()
}

// sourceAt returns source idx, if it exists.
func (st *stream) sourceAt(idx int) (Source, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if idx < 0 || idx >= len(st.spec.Sources) {
		return Source{}, false
	}
	return st.spec.Sources[idx], true
}

func (st *stream) sourceIndex() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.srcIdx
}

func (st *stream) markAdopted(v bool) {
	st.mu.Lock()
	st.adopted = v
	st.mu.Unlock()
}

func (st *stream) startTime() time.Time {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.started
}

// note records why something happened. fails <= 0 leaves the failure tally
// alone, so a health verdict can explain itself without resetting the
// consecutive-start-failure count that stop_failures is counting.
func (st *stream) note(msg string, fails int) {
	st.mu.Lock()
	st.lastErr = msg
	if fails > 0 {
		st.fails = fails
	}
	st.mu.Unlock()
}

func (st *stream) giveUp() {
	st.mu.Lock()
	st.gaveUp = true
	st.mu.Unlock()
	st.clearPIDFile()
}

// ── panel-visible side effects ─────────────────────────────────────────────

// writePIDFile publishes the encoder's pid where the panel still looks for it.
func (st *stream) writePIDFile(pid int) {
	p := st.spec_().PIDPath
	if p == "" || pid <= 0 {
		return
	}
	if err := os.WriteFile(p, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		dlog.Logf("monitor", "id=%s could not write pid file %s: %v", st.id, p, err)
	}
}

func (st *stream) clearPIDFile() {
	if p := st.spec_().PIDPath; p != "" {
		_ = os.Remove(p)
	}
}

// logLine is one entry of the panel's stream log, in the shape
// StreamProcess::streamLog writes and StreamsLogsCronJob drains.
type logLine struct {
	ServerID int    `json:"server_id"`
	StreamID int    `json:"stream_id"`
	Action   string `json:"action"`
	Source   string `json:"source"`
	Time     int64  `json:"time"`
}

// emit appends an event to the panel's stream log. The format is the panel's:
// one base64-encoded JSON object per line. Writing this file rather than a
// database row is what lets the daemon report events at all, since the panel's
// credentials are not available to it.
//
// Best-effort by design: a stream must never stop because its log could not be
// written.
func (st *stream) emit(action, source string) {
	spec := st.spec_()
	if spec.LogPath == "" {
		return
	}
	sid, _ := strconv.Atoi(st.id)
	b, err := json.Marshal(logLine{
		ServerID: spec.ServerID,
		StreamID: sid,
		Action:   action,
		Source:   source,
		Time:     st.sup.now().Unix(),
	})
	if err != nil {
		return
	}
	f, err := os.OpenFile(spec.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		dlog.Logf("monitor", "id=%s could not open stream log %s: %v", st.id, spec.LogPath, err)
		return
	}
	defer f.Close()
	if _, err := f.WriteString(base64.StdEncoding.EncodeToString(b) + "\n"); err != nil {
		dlog.Logf("monitor", "id=%s could not append to stream log: %v", st.id, err)
	}
}
