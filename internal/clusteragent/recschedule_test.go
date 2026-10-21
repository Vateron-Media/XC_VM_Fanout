package clusteragent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// recWriteStreamRecord writes stream 42's replica record with the given
// recordings[] JSON body plus the stream fields capture needs.
func recWriteStreamRecord(t *testing.T, rg *liveRig, recordingsJSON string) {
	t.Helper()
	rec := `{"data":{"stream":{"direct_source":0},"type":{"live":1},"server":{"on_demand":1},"recordings":[` + recordingsJSON + `]}}`
	if err := os.WriteFile(filepath.Join(streamsDir(rg.dir), "42.json"), []byte(rec), 0o644); err != nil {
		t.Fatal(err)
	}
}

func recState1Count(t *testing.T, spoolDir string) int {
	n := 0
	for _, s := range spooledRecStates(t, spoolDir) {
		if s == 1 {
			n++
		}
	}
	return n
}

func TestStreamRecordingsParses(t *testing.T) {
	dir := t.TempDir()
	sd := streamsDir(dir)
	os.MkdirAll(sd, 0o755)
	rec := `{"data":{"stream":{"tv_archive_server_id":3,"tv_archive_duration":7},"recordings":[` +
		`{"id":10,"stream_icon":"http://x/a.jpg","start":1000,"end":2000,"source_id":3,"archive":0,"status":0},` +
		`{"id":11,"stream_icon":"","start":0,"end":0,"source_id":5,"archive":1,"status":2}]}}`
	os.WriteFile(filepath.Join(sd, "42.json"), []byte(rec), 0o644)
	a := &Agent{ReplicaDir: dir}

	recs := a.streamRecordings("42")
	if len(recs) != 2 {
		t.Fatalf("got %d recordings, want 2", len(recs))
	}
	if r := recs[0]; r.ID != 10 || r.StreamID != 42 || r.Start != 1000 || r.End != 2000 || r.SourceID != 3 || r.Archive || r.Status != 0 || r.StreamIcon != "http://x/a.jpg" {
		t.Fatalf("rec0 = %+v", r)
	}
	if r := recs[1]; r.ID != 11 || !r.Archive || r.Status != 2 || r.SourceID != 5 {
		t.Fatalf("rec1 = %+v", r)
	}

	m, ok := a.streamMeta("42")
	if !ok || m.TvArchiveServerID != 3 || m.TvArchiveDuration != 7 {
		t.Fatalf("streamMeta tv_archive: ok=%v sid=%d dur=%d", ok, m.TvArchiveServerID, m.TvArchiveDuration)
	}
}

func TestShouldStartRecording(t *testing.T) {
	const now, srv = int64(10000), int64(3)
	cases := []struct {
		name string
		r    recording
		want bool
	}{
		{"airing live", recording{SourceID: srv, Start: 9000, End: 11000}, true},
		{"past live", recording{SourceID: srv, Start: 1000, End: 2000}, false},
		{"future live", recording{SourceID: srv, Start: 20000, End: 21000}, false},
		{"archive after end+65", recording{SourceID: srv, Archive: true, Start: 1000, End: now - 100}, true},
		{"archive within end+65", recording{SourceID: srv, Archive: true, Start: 1000, End: now - 10}, false},
		{"status recording", recording{SourceID: srv, Start: 9000, End: 11000, Status: 1}, false},
		{"status done", recording{SourceID: srv, Start: 9000, End: 11000, Status: 2}, false},
		{"other node", recording{SourceID: 5, Start: 9000, End: 11000}, false},
	}
	for _, c := range cases {
		if got := shouldStartRecording(c.r, srv, now); got != c.want {
			t.Errorf("%s: shouldStartRecording = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestWorkerSetDedups(t *testing.T) {
	s := newWorkerSet()
	started, release := make(chan struct{}), make(chan struct{})
	ok1 := s.start(context.Background(), 7, func(ctx context.Context) { close(started); <-release })
	<-started
	ok2 := s.start(context.Background(), 7, func(ctx context.Context) { t.Error("second start ran for an in-flight id") })
	if !ok1 || ok2 {
		t.Fatalf("start1=%v start2=%v, want true,false", ok1, ok2)
	}
	close(release) // worker 1 finishes → removes itself
	archiveWaitFor(t, func() bool { return !s.has(7) }, time.Second)
	done := make(chan struct{})
	if !s.start(context.Background(), 7, func(ctx context.Context) { close(done) }) {
		t.Fatal("start after completion should succeed")
	}
	<-done
}

func TestWorkerSetStopCancels(t *testing.T) {
	s := newWorkerSet()
	done := make(chan struct{})
	s.start(context.Background(), 9, func(ctx context.Context) { <-ctx.Done(); close(done) })
	s.stop(9)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel the worker")
	}
	archiveWaitFor(t, func() bool { return !s.has(9) }, time.Second)
}

func newSchedRig(t *testing.T, lf *liveFanout) *liveRig {
	rg := newLiveRig(t, lf)
	recWithLoopback(t, rg)
	recSetArchivePath(t, rg)
	rg.a.SpoolDir = t.TempDir()
	rg.a.TmpRoot = t.TempDir()
	return rg
}

func TestScheduleStartsDueRecording(t *testing.T) {
	rg := newSchedRig(t, &liveFanout{id: "42", running: true, hasData: true, liveBody: []byte("LIVE")})
	recFakeExternals(t, 7, nil, func(ctx context.Context, name string, args ...string) error {
		return os.WriteFile(args[len(args)-1], []byte("MP4"), 0o644)
	})
	now := time.Now().Unix()
	recWriteStreamRecord(t, rg, fmt.Sprintf(`{"id":55,"start":%d,"end":%d,"source_id":3,"archive":0,"status":0}`, now-10, now+3600))
	if err := rg.a.scheduleRecordings(context.Background()); err != nil {
		t.Fatal(err)
	}
	archiveWaitFor(t, func() bool {
		s := spooledRecStates(t, rg.a.SpoolDir)
		return len(s) == 2 && s[0] == 1 && s[1] == 2
	}, 3*time.Second)
	recs, _ := rg.a.recSets()
	archiveWaitFor(t, func() bool { return !recs.has(55) }, 2*time.Second)
}

func TestScheduleSkipsNotDue(t *testing.T) {
	rg := newSchedRig(t, &liveFanout{id: "42", running: true, hasData: true, liveBody: []byte("LIVE")})
	recFakeExternals(t, 7, nil, func(ctx context.Context, name string, args ...string) error {
		t.Error("recorder ran for a not-due recording")
		return nil
	})
	now := time.Now().Unix()
	recWriteStreamRecord(t, rg, fmt.Sprintf(
		`{"id":55,"start":%d,"end":%d,"source_id":3,"archive":0,"status":2},{"id":56,"start":%d,"end":%d,"source_id":5,"archive":0,"status":0}`,
		now-10, now+3600, now-10, now+3600)) // status 2 (done) + other node
	rg.a.scheduleRecordings(context.Background())
	time.Sleep(150 * time.Millisecond)
	if s := spooledRecStates(t, rg.a.SpoolDir); len(s) != 0 {
		t.Fatalf("not-due: spooled states %v, want none", s)
	}
}

func TestScheduleNoDoubleStart(t *testing.T) {
	rg := newSchedRig(t, &liveFanout{id: "42", running: true, hasData: true, liveBody: []byte("LIVE"), liveBlock: true})
	recFakeExternals(t, 7, nil, func(ctx context.Context, name string, args ...string) error {
		return os.WriteFile(args[len(args)-1], []byte("MP4"), 0o644)
	})
	now := time.Now().Unix()
	recWriteStreamRecord(t, rg, fmt.Sprintf(`{"id":55,"start":%d,"end":%d,"source_id":3,"archive":0,"status":0}`, now-10, now+3600))
	recs, _ := rg.a.recSets()
	rg.a.scheduleRecordings(context.Background())
	archiveWaitFor(t, func() bool { return recs.has(55) }, 2*time.Second) // first worker in-flight (capture blocked)
	rg.a.scheduleRecordings(context.Background())                         // must not start a duplicate
	time.Sleep(150 * time.Millisecond)
	if n := recState1Count(t, rg.a.SpoolDir); n != 1 {
		t.Fatalf("state-1 events = %d, want 1 (dedup across two schedule ticks)", n)
	}
	recs.stop(55)
	archiveWaitFor(t, func() bool { return !recs.has(55) }, 2*time.Second) // let the worker finish before TempDir cleanup
}
