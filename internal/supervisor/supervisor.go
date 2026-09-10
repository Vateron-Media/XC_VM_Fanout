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
	"strconv"
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
// produces and re-deriving it here is exactly what this design avoids. It must
// arrive WITHOUT buildLive's trailing redirect-and-background tail: the daemon
// supervises the process, so it needs to be its parent and to reap it itself.
type Source struct {
	Label string `json:"label"`
	Cmd   string `json:"cmd"`
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
	Supervised bool   `json:"supervised"`
	Running    bool   `json:"running"`
	PID        int    `json:"pid"`
	Source     string `json:"source"`
	Restarts   int    `json:"restarts"`
	Failures   int    `json:"failures"`
	UptimeMS   int64  `json:"uptime_ms"`
	GaveUp     bool   `json:"gave_up"`
	LastError  string `json:"last_error"`
}

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
	}
}

// WithVitals supplies the stream-health source the watch loop judges against.
// Without it the supervisor still starts, watches and restarts encoders — it
// just cannot see whether the output is any good, so every Health rule is inert.
func (s *Supervisor) WithVitals(v VitalsFunc) *Supervisor {
	s.vitals = v
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

	mu      sync.Mutex
	spec    Spec
	proc    Process
	pid     int
	// autoRestartAt is when the scheduled restart last fired. The schedule
	// matches on HH:MM, so without this it keeps matching for the rest of that
	// minute and the stream restarts in a tight loop until the clock moves on.
	// (MonitorCommand.php has the same shape and only escaped it because its
	// restart path was slow enough to usually leave the minute — luck, not
	// design, and the daemon restarts far too quickly to rely on it.)
	autoRestartAt time.Time
	source  string
	started time.Time
	running bool
	gaveUp  bool
	fails   int
	starts  int
	lastErr string
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
	st := &stream{id: id, sup: s, cancel: cancel, done: make(chan struct{}), spec: spec}
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

// ReleaseAll stops every supervised stream; for daemon shutdown.
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
		PID:        st.pid,
		Source:     st.source,
		Restarts:   st.starts,
		Failures:   st.fails,
		GaveUp:     st.gaveUp,
		LastError:  st.lastErr,
	}
	if st.running && !st.started.IsZero() {
		out.UptimeMS = st.sup.now().Sub(st.started).Milliseconds()
	}
	return out
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
			consecutiveFails++
			st.note(err.Error(), consecutiveFails)
			st.emit(EventStreamStartFail, src.Label)
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
			st.emit(verdict.event, src.Label)
			dlog.Logf("monitor", "id=%s unhealthy: %s (%s); restarting", st.id, verdict.reason, verdict.event)
		} else {
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

		v, ok := sup.vitals(st.id)
		if !ok {
			continue // the stream is not registered with the daemon (yet)
		}
		if verdict := health.check(now, startedAt, v, base); verdict.failed() {
			return verdict
		}
	}
}

// startOnce launches the encoder and confirms it actually began producing. A
// process that starts and then sits there yielding nothing is a failed start,
// not a running stream — the panel learned this by waiting for a playlist file
// to appear; here the daemon asks whether bytes arrived, which is the same
// question without the filesystem in the middle.
func (st *stream) startOnce(ctx context.Context, src Source) (Process, error) {
	spec := st.spec_()

	proc, err := st.sup.launch(ctx, src.Cmd, spec.ErrorsPath)
	if err != nil {
		return nil, fmt.Errorf("launch: %w", err)
	}

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

// currentSource is the source to start. M1 always uses the first; the list and
// this indirection exist so the priority-backup and forced-switch phases have
// somewhere to land without a spec change.
func (st *stream) currentSource() Source {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.spec.Sources[0]
}

func (st *stream) markStarted(p Process, label string) {
	st.mu.Lock()
	st.proc, st.pid, st.source = p, p.Pid(), label
	st.started, st.running = st.sup.now(), true
	st.starts++
	st.mu.Unlock()
}

func (st *stream) markStopped(err error) {
	st.mu.Lock()
	st.running, st.proc, st.pid = false, nil, 0
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
