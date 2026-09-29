package clusteragent

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// GET /v1/status on the local socket: what this agent knows of its standing
// with MAIN, for the node's `server:diagnose` (ADR 0004, Phase 10). Read-only,
// answered from memory and the spool directory; nothing here reaches MAIN, and
// no key or token material is in it.
//
//	{
//	  "v": 1, "version": "…", "server_id": 3, "now_ms": …,
//	  "state": "active", "mode": 1, "flows": 7, "fenced": false,
//	  "main_skew_ms": -120, "main_time_seen_ms": …,   // MAIN − local; 0/0 while MAIN never answered
//	  "last_heartbeat_ms": …, "heartbeat_sec": 2,     // 0 while no heartbeat was answered
//	  "token": {"epoch": 9, "gen": 2, "nbf": …, "exp": …, "refresh_at": …} | null,
//	  "lease": {"gen": 2, "iat": …, "exp": …} | null, "lease_refused": "",
//	  "lanes": [{"name": "p0", "files": 0, "bytes": 0, "oldest_ms": 0, "inflight": 0, "cursor": 41}, …],
//	  "busy_refusals": 0,
//	  "relay": {"bound": true} | {"bound": false, "since_ms": …, "failures": 3, "error": "…"} | null
//	}
//
// Times are unix ms on this machine's clock except where a field says MAIN's:
// token and lease windows are unix seconds on MAIN's clock, as MAIN signed them.

// StatusVersion is the document's version, raised when a field changes meaning.
const StatusVersion = 1

// LaneStatus is one event lane's backlog on disk.
type LaneStatus struct {
	Name  string `json:"name"`
	Files int    `json:"files"`
	Bytes int64  `json:"bytes"`
	// OldestMs is the mtime (local unix ms) of the oldest spooled file; 0
	// when the lane is empty.
	OldestMs int64 `json:"oldest_ms"`
	// Inflight is the number of events of the batch sent and not yet
	// confirmed by MAIN.
	Inflight int `json:"inflight"`
	// Cursor is MAIN's last applied number for the lane, -1 until a hello
	// said it.
	Cursor int64 `json:"cursor"`
}

// Status builds the document GET /v1/status answers.
func (a *Agent) Status(now time.Time) map[string]any {
	c := a.Client
	st := c.State
	doc := map[string]any{
		"v":                 StatusVersion,
		"version":           a.Version,
		"now_ms":            now.UnixMilli(),
		"mode":              a.mode.Load(),
		"flows":             a.flows.Load(),
		"fenced":            a.fenced.Load(),
		"heartbeat_sec":     int(a.heartbeatEvery() / time.Second),
		"last_heartbeat_ms": a.lastBeatMs.Load(),
		"main_skew_ms":      int64(0),
		"main_time_seen_ms": c.offsetAtMs.Load(),
		"busy_refusals":     a.busyRefusals.Load(),
		"token":             nil,
		"lease":             nil,
		"lease_refused":     "",
		"lanes":             []LaneStatus{},
		"relay":             nil,
	}
	if relay := a.RelayReport(now); relay != nil {
		doc["relay"] = relay
	}
	if s, ok := a.state.Load().(string); ok {
		doc["state"] = s
	} else {
		doc["state"] = ""
	}
	if c.offsetAtMs.Load() != 0 {
		doc["main_skew_ms"] = c.offsetMs.Load()
	}
	if tok, ok := c.Current(); ok && tok != nil {
		doc["token"] = map[string]any{"epoch": tok.Epoch, "gen": tok.Gen, "nbf": tok.Nbf, "exp": tok.Exp, "refresh_at": tok.RefreshAt}
	}
	st.mu.Lock()
	doc["server_id"] = st.ServerID
	if l := st.Lease; l != nil {
		doc["lease"] = map[string]any{"gen": l.Gen, "iat": l.Iat, "exp": l.Exp}
	}
	doc["lease_refused"] = st.LeaseRefused
	st.mu.Unlock()
	if a.SpoolDir != "" {
		lanes := make([]LaneStatus, 0, len(Lanes))
		for _, lane := range Lanes {
			lanes = append(lanes, a.laneStatus(lane))
		}
		doc["lanes"] = lanes
	}
	return doc
}

// laneStatus measures one lane's spool: its complete files, their bytes and
// the oldest one's age, and the batch in flight.
func (a *Agent) laneStatus(lane Lane) LaneStatus {
	ls := &laneSpool{lane: lane, dir: filepath.Join(a.SpoolDir, lane.Name), state: filepath.Join(a.SpoolDir, lane.Name+".inflight")}
	out := LaneStatus{Name: lane.Name, Cursor: a.cursor(lane.Name)}
	entries, _ := ls.spooled()
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue // deleted since the listing
		}
		out.Files++
		out.Bytes += info.Size()
		if ms := info.ModTime().UnixMilli(); out.OldestMs == 0 || ms < out.OldestMs {
			out.OldestMs = ms
		}
	}
	if b, err := os.ReadFile(ls.state); err == nil {
		var fl inflight
		if json.Unmarshal(b, &fl) == nil {
			out.Inflight = fl.Count
		}
	}
	return out
}

func (a *Agent) serveStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "not allowed", http.StatusMethodNotAllowed)
		return
	}
	replyJSON(w, a.Status(time.Now()))
}
