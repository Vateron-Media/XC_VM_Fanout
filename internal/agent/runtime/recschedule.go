package clusteragent

// recschedule.go is slice 3 of recording→Go: the control plane that drives the
// slice-1 archive writer and the slice-2 DVR recorder from the Go agent, on the
// RunChores tick — a cron:vod-cadence scheduler that starts a recorder per due
// recording, and a reconcile that runs an archive worker per tv_archive stream
// on this node. It replaces PHP's cron:vod spawn and StreamProcess's archive spawn.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// workerSet owns the recording/archive goroutines by an int64 key (recording id
// or stream id), replacing PHP's PID markers: it dedups (one worker per key),
// removes a worker when it returns, and cancels on stop/reconcile.
type workerEntry struct{ cancel context.CancelFunc }

type workerSet struct {
	mu      sync.Mutex
	workers map[int64]*workerEntry
}

func newWorkerSet() *workerSet { return &workerSet{workers: map[int64]*workerEntry{}} }

// start runs fn(ctx) in a goroutine keyed by id; false (no double-start) when a
// worker for id is already in flight. The goroutine removes id on return — but
// only if the map still holds this entry, so a stop()+restart cannot be undone
// by the old goroutine's cleanup.
func (s *workerSet) start(parent context.Context, id int64, fn func(ctx context.Context)) bool {
	s.mu.Lock()
	if _, ok := s.workers[id]; ok {
		s.mu.Unlock()
		return false
	}
	ctx, cancel := context.WithCancel(parent)
	e := &workerEntry{cancel: cancel}
	s.workers[id] = e
	s.mu.Unlock()
	go func() {
		defer func() {
			s.mu.Lock()
			if s.workers[id] == e {
				delete(s.workers, id)
			}
			cancel()
			s.mu.Unlock()
		}()
		fn(ctx)
	}()
	return true
}

// stop cancels id's worker but does NOT drop it — the goroutine's own cleanup
// removes it when it actually returns, so has(id) stays true until the worker has
// fully finished (its writes done), which callers wait on before teardown.
func (s *workerSet) stop(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.workers[id]; ok {
		e.cancel()
	}
}

func (s *workerSet) has(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.workers[id]
	return ok
}

func (s *workerSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.workers)
}

// stopAllExcept cancels every worker whose id is not in keep; each goroutine's
// own cleanup drops it once it returns (so has stays true until it has finished).
func (s *workerSet) stopAllExcept(keep map[int64]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, e := range s.workers {
		if !keep[id] {
			e.cancel()
		}
	}
}

// streamRecordings reads the recordings scheduled on this node for stream id
// (the replica record's data.recordings[], RECORDING_FIELDS), mapping each to a
// recording whose StreamID is id. nil when the record is missing/unreadable.
func (a *Agent) streamRecordings(id string) []recording {
	b, err := os.ReadFile(filepath.Join(streamsDir(a.ReplicaDir), id+".json"))
	if err != nil {
		return nil
	}
	var w struct {
		Data struct {
			Recordings []struct {
				ID         int64  `json:"id"`
				Start      int64  `json:"start"`
				End        int64  `json:"end"`
				Archive    int64  `json:"archive"`
				Status     int    `json:"status"`
				SourceID   int64  `json:"source_id"`
				StreamIcon string `json:"stream_icon"`
			} `json:"recordings"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &w) != nil {
		return nil
	}
	sid, _ := strconv.ParseInt(id, 10, 64)
	var out []recording
	for _, r := range w.Data.Recordings {
		out = append(out, recording{
			ID: r.ID, StreamID: sid, Start: r.Start, End: r.End,
			Archive: r.Archive == 1, StreamIcon: r.StreamIcon, SourceID: r.SourceID, Status: r.Status,
		})
	}
	return out
}

// shouldStartRecording is NodeStreams::recordingsDue plus the archive start-gate:
// the recording is scheduled on this node, not already recording/done, and either
// airing now or an archive (catch-up) job — and an archive job is held until
// now >= end+65 so every archive minute file has been written.
func shouldStartRecording(r recording, serverID, now int64) bool {
	if r.SourceID != serverID || r.Status == 1 || r.Status == 2 {
		return false
	}
	airing := r.Start <= now && now < r.End
	if !airing && !r.Archive {
		return false
	}
	if r.Archive && now < r.End+65 {
		return false
	}
	return true
}

// recSets lazily creates the recorder and archive worker sets.
func (a *Agent) recSets() (recorders, archivers *workerSet) {
	a.recInit.Do(func() {
		a.recorders = newWorkerSet()
		a.archivers = newWorkerSet()
	})
	return a.recorders, a.archivers
}

// recStatusDir holds the node's local recording-status markers — the Go twin of
// StreamRuntime's recordings/<id>.json. A finished recording is marked here the
// instant it completes, so the next tick does not re-record it in the window
// before MAIN's replica reflects the new status (the replica carries MAIN's
// lagging view, filtered on nothing). Survives a process restart, as PHP's does.
func (a *Agent) recStatusDir() string { return filepath.Join(a.TmpRoot, "recordings") }

func (a *Agent) markRecordingStatus(id int64, status int) {
	if a.TmpRoot == "" {
		return
	}
	if err := os.MkdirAll(a.recStatusDir(), 0o750); err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(a.recStatusDir(), strconv.FormatInt(id, 10)), []byte(strconv.Itoa(status)), 0o640)
}

// localRecordingStatus is StreamRuntime::recordingStatus: the node's own last
// status for a recording, or (_, false) when it has none.
func (a *Agent) localRecordingStatus(id int64) (int, bool) {
	b, err := os.ReadFile(filepath.Join(a.recStatusDir(), strconv.FormatInt(id, 10)))
	if err != nil {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, false
	}
	return n, true
}

// scheduleRecordings is cron:vod (VodCronJob): under its lock, start a recorder
// goroutine for every due recording the node holds, de-duplicated by the
// recorder worker set. A chore on the RunChores tick. The node's local status
// (recStatusDir) wins over the replica's lagging one, as NodeStreams::recordingsDue
// prefers StreamRuntime::recordingStatus.
func (a *Agent) scheduleRecordings(ctx context.Context) error {
	rel, ok := acquireCronLock(cronLockPath(a.cronsDir(), `XcVm\Cli\CronJobs\VodCronJob`))
	if !ok {
		return nil
	}
	defer rel()
	recorders, _ := a.recSets()
	now := time.Now().Unix()
	srv := a.Client.State.ServerID
	for _, sid := range heldStreams(a.ReplicaDir) {
		for _, rec := range a.streamRecordings(strconv.FormatInt(sid, 10)) {
			if ls, ok := a.localRecordingStatus(rec.ID); ok {
				rec.Status = ls // the node's own status wins over the replica's lagging one
			}
			if !shouldStartRecording(rec, srv, now) {
				continue
			}
			rec := rec
			recorders.start(ctx, rec.ID, func(c context.Context) { a.runRecording(c, rec) })
		}
	}
	return nil
}

// reconcileArchiveWorkers (StreamProcess's archive spawn, moved to the agent):
// ensure an archiveJob goroutine for each held stream whose tv_archive_server_id
// is this node and that the fanout reports running; cancel the workers for
// streams no longer wanted. A chore on the RunChores tick.
func (a *Agent) reconcileArchiveWorkers(ctx context.Context) error {
	pol, ok := a.adminPolicy()
	if !ok {
		return nil
	}
	_, archivers := a.recSets()
	srv := a.Client.State.ServerID
	wanted := map[int64]bool{}
	for _, sid := range heldStreams(a.ReplicaDir) {
		id := strconv.FormatInt(sid, 10)
		meta, ok := a.streamMeta(id)
		if !ok || meta.TvArchiveServerID != srv {
			continue
		}
		if running, _, ok := a.fanoutStreamStatus(ctx, id); !ok || !running {
			continue
		}
		wanted[sid] = true
		sid, dur := sid, int(meta.TvArchiveDuration)
		archivers.start(ctx, sid, func(c context.Context) {
			j := &archiveJob{
				streamID:     sid,
				streamsDir:   pol.streamsPath,
				archiveDir:   filepath.Join(pol.archivePath, strconv.FormatInt(sid, 10)),
				durationDays: dur,
				logf:         a.logf,
				running:      func() bool { r, _, ok := a.fanoutStreamStatus(c, strconv.FormatInt(sid, 10)); return ok && r },
			}
			j.run(c)
		})
	}
	archivers.stopAllExcept(wanted)
	return nil
}

// runRecordingSchedulers runs the recordings scheduler and the archive reconcile
// once, on a stream-serving node (FlowStreams). Ticked by RunChores; ctx ties the
// worker goroutines to the agent's lifetime. Off that flow it leaves recording to
// nothing (a serve-only LB with STREAMS is where recording runs).
func (a *Agent) runRecordingSchedulers(ctx context.Context) {
	if a.flows.Load()&FlowStreams == 0 {
		return
	}
	if err := a.scheduleRecordings(ctx); err != nil && a.Logf != nil {
		a.Logf("cluster: recordings: %v", err)
	}
	if err := a.reconcileArchiveWorkers(ctx); err != nil && a.Logf != nil {
		a.Logf("cluster: archive reconcile: %v", err)
	}
}
