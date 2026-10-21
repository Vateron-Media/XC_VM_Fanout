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

func (s *workerSet) stop(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.workers[id]; ok {
		e.cancel()
		delete(s.workers, id)
	}
}

func (s *workerSet) has(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.workers[id]
	return ok
}

// stopAllExcept cancels and drops every worker whose id is not in keep.
func (s *workerSet) stopAllExcept(keep map[int64]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, e := range s.workers {
		if !keep[id] {
			e.cancel()
			delete(s.workers, id)
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

// scheduleRecordings is cron:vod (VodCronJob): under its lock, start a recorder
// goroutine for every due recording the node holds, de-duplicated by the
// recorder worker set. A chore on the RunChores tick.
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
			if !shouldStartRecording(rec, srv, now) {
				continue
			}
			rec := rec
			recorders.start(ctx, rec.ID, func(c context.Context) { a.runRecording(c, rec) })
		}
	}
	return nil
}
