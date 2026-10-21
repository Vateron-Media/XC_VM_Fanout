package clusteragent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
