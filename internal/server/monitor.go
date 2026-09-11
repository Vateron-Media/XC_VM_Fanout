package server

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/supervisor"
)

// serveMonitor is the control surface for per-stream encoder supervision — the
// daemon-side replacement for the panel's `console.php monitor <id>` watchdog
// (see docs/adr/0002-monitor-in-daemon.md).
//
//	PUT|POST /monitor/<id>   hand a stream over: sources + policy + the paths the
//	                         panel still reads. Re-PUTting replaces the spec and
//	                         restarts the encoder, which is what a source change
//	                         from the panel means.
//	GET      /monitor/<id>   what PHP needs to reconcile streams_servers.
//	DELETE   /monitor/<id>   stop supervising and kill the encoder.
//
// This lives on the control socket, which is PHP-only. That placement is the
// security boundary for the whole feature: the daemon runs a command line it is
// handed and never composes one, so the only thing that can tell it what to run
// is whatever can already reach the control socket.
func (m *Manager) serveMonitor(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/monitor/")
	// "<id>/source" is the forced-source sub-resource; anything else with a
	// slash in it is not a stream id.
	if rest, sub, hasSub := strings.Cut(id, "/"); hasSub {
		if sub != "source" {
			http.NotFound(w, r)
			return
		}
		m.serveMonitorSource(w, r, rest)
		return
	}
	if id == "" {
		http.NotFound(w, r)
		return
	}
	if m.sup == nil {
		http.Error(w, "supervision not enabled on this node", http.StatusNotImplemented)
		return
	}

	switch r.Method {
	case http.MethodPut, http.MethodPost:
		// Not accepting hand-overs is a 501, the same answer as a node with no
		// supervisor at all: the panel reads it as "run it yourself".
		if !m.superviseOn.Load() {
			http.Error(w, "supervision not enabled on this node", http.StatusNotImplemented)
			return
		}
		var spec supervisor.Spec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			http.Error(w, "bad spec: "+err.Error(), http.StatusBadRequest)
			return
		}
		// A re-PUT restarts the encoder, so drop any sampled counters first:
		// the new process starts its own, and differencing across the boundary
		// would read as a frame-rate collapse.
		if m.vitals != nil {
			m.vitals.forget(id)
		}
		if m.meta != nil {
			m.meta.forget(id)
		}
		if err := m.sup.Supervise(id, spec); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		dlog.Logf("ctl", "id=%s monitor spec accepted", id)
		w.WriteHeader(http.StatusNoContent)

	case http.MethodGet:
		st := m.sup.State(id)
		if !st.Supervised {
			http.NotFound(w, r)
			return
		}
		// Supervision state plus what the stream turned out to BE. The panel
		// reconciles streams_servers from this one call, which is what retires
		// its periodic ffprobe.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.monitorState(id, st))

	case http.MethodDelete:
		if m.vitals != nil {
			m.vitals.forget(id)
		}
		if m.meta != nil {
			m.meta.forget(id)
		}
		if m.sup.Release(id) {
			dlog.Logf("ctl", "id=%s monitor released", id)
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveMonitorSource forces a stream onto a specific source (POST
// /monitor/<id>/source with {"index": N}). It replaces the panel's
// `<signals>/<id>.force` file, which MonitorCommand.php polled for on every
// pass: a control call is immediate, cannot be half-written, and reports back
// whether the index was even valid.
//
// The switch is a restart on the chosen source, so it is deliberately explicit
// rather than something a health check would ever decide on its own.
func (m *Manager) serveMonitorSource(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if m.sup == nil {
		http.Error(w, "supervision not enabled on this node", http.StatusNotImplemented)
		return
	}
	var body struct {
		Index int `json:"index"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := m.sup.ForceSource(id, body.Index); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	dlog.Logf("ctl", "id=%s forced onto source %d", id, body.Index)
	w.WriteHeader(http.StatusNoContent)
}

// serveMonitors lists the streams this node is supervising, so the panel can
// reconcile after a daemon restart: anything it expected to be supervised and
// is not listed here needs handing over again.
func (m *Manager) serveMonitors(w http.ResponseWriter, _ *http.Request) {
	ids := []string{}
	if m.sup != nil {
		ids = m.sup.IDs()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ids)
}

// monitorStateView is GET /monitor/<id>'s body: supervision state plus the
// stream's measured metadata, when there is any.
type monitorStateView struct {
	supervisor.State
	Meta *StreamMeta `json:"meta,omitempty"`
}

func (m *Manager) monitorState(id string, st supervisor.State) monitorStateView {
	out := monitorStateView{State: st}
	if meta, ok := m.StreamMetadata(id); ok {
		out.Meta = &meta
	}
	return out
}

// serveMonitorStates is every supervised stream's GET /monitor/<id> in one
// response, keyed by id. The panel reconciles streams_servers from it on a
// short cadence; one call per stream per pass would be hundreds of round trips
// on a large node to learn, mostly, that nothing changed. `accepting` says
// whether a new hand-over would be taken right now, and `daemon_pid` is what the
// panel records as a supervised stream's monitor_pid — known before it hands one
// over, so the row can say "watched" before the daemon starts anything.
func (m *Manager) serveMonitorStates(w http.ResponseWriter, _ *http.Request) {
	out := struct {
		Accepting bool                        `json:"accepting"`
		DaemonPID int                         `json:"daemon_pid"`
		Features  []string                    `json:"features"`
		Streams   map[string]monitorStateView `json:"streams"`
	}{Accepting: m.sup != nil && m.superviseOn.Load(), DaemonPID: os.Getpid(), Features: Features, Streams: map[string]monitorStateView{}}
	if m.sup != nil {
		for _, id := range m.sup.IDs() {
			if st := m.sup.State(id); st.Supervised {
				out.Streams[id] = m.monitorState(id, st)
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// Features names what this daemon can be handed, for a panel that may be newer
// or older than it. The panel composes the producer commands (ADR 0002), so it
// has to know whether this binary understands them before it writes one: a
// `xc_fanout remux` line given to a daemon from before the native remuxer is not
// rejected, it is MISPARSED — the old binary takes "remux" as a positional
// argument and tries to start a second daemon, which fails on the sockets the
// running one holds, and the stream never starts. One capability list costs
// nothing and removes a whole class of half-upgraded-node failure.
//
// "drop_connection": DELETE /connections/<uuid> ends a live-TS viewer (the
// panel's kick and connection-limit eviction).
var Features = []string{"remux", "drop_connection"}

// EnableSupervision gives the manager an encoder supervisor. Streams are only
// supervised once the panel PUTs a spec for one, so enabling this changes
// nothing on its own — which is also what makes the cutover per-stream and
// reversible without a daemon deploy.
//
// The supervisor confirms a start by asking whether the stream actually produced
// bytes, which is this manager's own liveness mark: the panel could only answer
// that question by polling for a playlist file to appear.
func (m *Manager) EnableSupervision() {
	m.vitals = newVitalsSampler()
	m.meta = newMetaCache()
	m.sup = supervisor.New(nil, m.streamHasData).WithVitals(m.sample)
}

// streamHasData reports whether a registered stream has ever published a
// non-empty chunk. Used as the supervisor's start-confirmation signal.
func (m *Manager) streamHasData(id string) bool {
	st := m.Get(id)
	return st != nil && st.lastData.Load() != 0
}

// DetachSupervision stops watching every supervised stream but leaves its
// encoder running, for a daemon shutdown or upgrade.
//
// Deliberately not a kill: the next daemon adopts these processes (see
// supervisor/adopt.go), so a restart costs the viewers nothing. Killing them
// here would turn every daemon upgrade into a node-wide outage, which is
// precisely what adoption was built to avoid.
func (m *Manager) DetachSupervision() int {
	if m.sup == nil {
		return 0
	}
	return m.sup.DetachAll()
}
