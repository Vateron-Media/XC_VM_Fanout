package clusteragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// leaseStateFixture is testdata/cluster_lease_state.json, which the panel holds
// byte for byte as tests/Support/cluster_lease_state.json and feeds to
// Core\Cluster\NodeLease::verdict() (NodeLeaseTest). Its digest is recorded in
// both tests, so whichever side changes the format first fails until the file
// is copied over and both digests are updated.
const (
	leaseStateFixture       = "testdata/cluster_lease_state.json"
	leaseStateFixtureDigest = "09389ff2b7f238bf3375b8201d24a2be500ddf7b596068d0b3d4cb20974d72cc"
)

// holdLease gives st a lease MAIN signed for it: generation 1 (the fake's
// token), issued at iat, expiring at exp.
func holdLease(t *testing.T, f *fakeMain, st *State, iat, exp int64) {
	t.Helper()
	if !acceptLease(st, wireLease(f.panel, leaseDoc(st.NodeUUID, st.ServerID, 1, iat, exp), exp), onMain(iat, 1)) {
		t.Fatalf("lease refused: %s", st.LeaseRefused)
	}
}

func readLeaseFile(t *testing.T, path string) LeaseState {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc LeaseState
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		t.Fatalf("%s: %v", b, err)
	}
	return doc
}

// TestTheLeaseStateFileIsTheFixtureThePanelReads writes lease_state.json from
// fixed inputs and compares it with the fixture the panel's NodeLeaseTest
// judges: the lease's exp and iat in seconds, MAIN's clock and the write in
// milliseconds, the anchor 30 s short of the exp. XCVM_UPDATE_FIXTURES=1
// rewrites the fixture (then copy it to the panel and update both digests).
func TestTheLeaseStateFileIsTheFixtureThePanelReads(t *testing.T) {
	f, st := newFake(t)
	st.ServerID = 7
	iat, exp := int64(1800000000), int64(1800000000+12*3600)
	if !acceptLease(st, wireLease(f.panel, leaseDoc(st.NodeUUID, 7, 1, iat, exp), exp), onMain(iat, 1)) {
		t.Fatal(st.LeaseRefused)
	}
	st.Lease.Gen = 4 // as a node four generations on holds it
	c := NewClient(st, "xc_agent/test")
	mono := &fakeMono{ns: 42e9, boot: "b1"}
	c.clock = mono.clock()
	c.clock.heard(exp*1000 - 60_000)
	mono.advance(30 * time.Second)
	c.now = func() time.Time { return time.UnixMilli(1700000000000) }
	a := &Agent{Client: c, LeaseFile: filepath.Join(t.TempDir(), LeaseStateFile), Logf: t.Logf}
	a.publishLease()

	got, err := os.ReadFile(a.LeaseFile)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("XCVM_UPDATE_FIXTURES") == "1" {
		if err := os.WriteFile(leaseStateFixture, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(leaseStateFixture)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the writer's output:\n%s\nis not the fixture the panel judges:\n%s", got, want)
	}
	if sum := sha256.Sum256(want); hex.EncodeToString(sum[:]) != leaseStateFixtureDigest {
		t.Fatalf("%s: sha256 %x — copy it to the panel's tests/Support/ and update the digest in both tests", leaseStateFixture, sum)
	}
	doc := readLeaseFile(t, a.LeaseFile)
	if doc != (LeaseState{Exp: exp, Iat: iat, Gen: 4, ServerID: 7, AnchorMs: exp*1000 - 30_000, WroteAtMs: 1700000000000}) {
		t.Fatalf("doc %+v", doc)
	}
	// The node's PHP reads it as the agent's own user (PHP-FPM runs as xc_vm,
	// as the agent does): 0640 like flows.json, less the umask, never
	// writable by anyone else.
	fi, err := os.Stat(a.LeaseFile)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&^0o640 != 0 || fi.Mode().Perm()&0o400 == 0 {
		t.Fatalf("mode %v", fi.Mode().Perm())
	}
}

func TestWithoutALeaseOrAnAnchorTheFileSaysSo(t *testing.T) {
	// NodeLease serves on exp 0 ("MAIN has sent this node no lease") and on
	// anchor_ms 0 ("MAIN has never been heard on this node"): the file says
	// both plainly rather than leaving the file out.
	_, st := newFake(t)
	c := NewClient(st, "xc_agent/test")
	a := &Agent{Client: c, LeaseFile: filepath.Join(t.TempDir(), LeaseStateFile), Logf: t.Logf}
	a.publishLease()
	doc := readLeaseFile(t, a.LeaseFile)
	if doc.Exp != 0 || doc.Gen != 0 || doc.AnchorMs != 0 || doc.WroteAtMs <= 0 {
		t.Fatalf("doc %+v", doc)
	}
	c.setMainTime(time.Now().UnixMilli())
	a.publishLease()
	if doc := readLeaseFile(t, a.LeaseFile); doc.Exp != 0 || doc.AnchorMs <= 0 {
		t.Fatalf("heard, no lease: %+v", doc)
	}
}

func TestAClockMovedBackDoesNotMoveTheAnchor(t *testing.T) {
	// The anchor is MAIN's statement plus CLOCK_MONOTONIC: moving this
	// machine's wall clock back an hour moves wrote_at_ms, which NodeLease
	// only compares with that same clock, and not the anchor, which it compares
	// with the lease's exp. Forward likewise.
	f, st := newFake(t)
	now := time.Now().Unix()
	holdLease(t, f, st, now, now+3600)
	c := NewClient(st, "xc_agent/test")
	mono := &fakeMono{ns: 1e9, boot: "b1"}
	c.clock = mono.clock()
	c.clock.heard(now * 1000)
	wall := time.Unix(now, 0)
	c.now = func() time.Time { return wall }
	a := &Agent{Client: c, LeaseFile: filepath.Join(t.TempDir(), LeaseStateFile), Logf: t.Logf}

	for _, step := range []struct {
		wall time.Duration
		mono time.Duration
	}{{0, 0}, {-time.Hour, 2 * time.Second}, {3 * time.Hour, 2 * time.Second}} {
		wall = wall.Add(step.wall)
		mono.advance(step.mono)
		a.publishLease()
	}
	doc := readLeaseFile(t, a.LeaseFile)
	if doc.AnchorMs != now*1000+4000 {
		t.Fatalf("anchor %d, want MAIN's time plus the 4 s that passed (%d)", doc.AnchorMs, now*1000+4000)
	}
	if doc.WroteAtMs != wall.UnixMilli() {
		t.Fatalf("wrote_at_ms %d, want the machine's clock %d", doc.WroteAtMs, wall.UnixMilli())
	}
}

// leaseMain answers hello and heartbeat with MAIN's time, until gone.
func leaseMain(t *testing.T) (*fakeMain, *State, *atomic.Bool) {
	f, st := newFake(t)
	gone := new(atomic.Bool)
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		if gone.Load() {
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
		f.box(w, reqCtx, map[string]any{"state": "active", "mode": 1, "main_time_ms": time.Now().UnixMilli()})
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	st.Enrolled = true
	return f, st, gone
}

// The one property the node's PHP depends on: the file goes on being rewritten
// while MAIN cannot be reached, with the anchor advancing as time does, because
// a file that has gone stale reads as "serve" — a fence that stopped being
// refreshed exactly when MAIN went away would never draw.
func TestTheLeaseStateIsPublishedEveryTickEvenWithMainGone(t *testing.T) {
	f, st, gone := leaseMain(t)
	now := time.Now().Unix()
	holdLease(t, f, st, now, now+3600)
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Interval: 50 * time.Millisecond, Logf: t.Logf,
		LeaseFile: filepath.Join(filepath.Dir(st.path), LeaseStateFile)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	var doc LeaseState
	waitFor(t, "an anchored lease state", func() bool {
		b, err := os.ReadFile(a.LeaseFile)
		return err == nil && json.Unmarshal(b, &doc) == nil && doc.AnchorMs > 0
	})
	if doc.Exp != now+3600 || doc.Iat != now || doc.Gen != 1 || doc.ServerID != 3 {
		t.Fatalf("doc %+v", doc)
	}
	if d := doc.AnchorMs - time.Now().UnixMilli(); d > 1000 || d < -1000 {
		t.Fatalf("anchor %d ms off MAIN's time", d)
	}

	gone.Store(true)
	time.Sleep(200 * time.Millisecond) // past the last heartbeat MAIN answered
	first := readLeaseFile(t, a.LeaseFile)
	time.Sleep(500 * time.Millisecond)
	later := readLeaseFile(t, a.LeaseFile)
	wrote, anchored := later.WroteAtMs-first.WroteAtMs, later.AnchorMs-first.AnchorMs
	if wrote < 350 {
		t.Fatalf("rewritten %d ms later over 500 ms with MAIN gone", wrote)
	}
	if d := anchored - wrote; d > 50 || d < -50 {
		t.Fatalf("the anchor moved %d ms while %d ms passed", anchored, wrote)
	}
	if later.Exp != doc.Exp {
		t.Fatalf("exp %d", later.Exp)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	// Its last act: the clock, kept for the restart.
	back, err := loadRaw(st.path)
	if err != nil {
		t.Fatal(err)
	}
	if back.MainAnchor == nil || back.MainAnchor.MainMs < later.AnchorMs || back.MainSeenMs <= 0 {
		t.Fatalf("saved clock %d %+v, last written anchor %d", back.MainSeenMs, back.MainAnchor, later.AnchorMs)
	}
}

func TestARestartedAgentWritesTheLeaseStateBeforeMainAnswers(t *testing.T) {
	// An agent restarted while MAIN is gone never gets its hello answered: the
	// file must be written all the same, from the clock the state kept, or a
	// restart would be a way out of the fence.
	f, st := newFake(t)
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	st.MainURLs = []string{"http://" + l.Addr().String() + "/cluster/v1/"}
	l.Close() // connection refused
	st.Enrolled = true
	now := time.Now().Unix()
	holdLease(t, f, st, now, now+3600)
	st.MainSeenMs = now*1000 - 3_600_000
	st.MainAnchor = &ClockMark{MainMs: now*1000 - 60_000, MonoNs: monotonicNs() - int64(10*time.Second), BootID: bootID()}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Interval: 50 * time.Millisecond, Logf: t.Logf,
		LeaseFile: filepath.Join(filepath.Dir(st.path), LeaseStateFile)}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := a.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v", err)
	}
	doc := readLeaseFile(t, a.LeaseFile)
	if doc.Exp != now+3600 {
		t.Fatalf("doc %+v", doc)
	}
	// The mark plus the 10 s before the restart (same boot) and the run.
	if want := now*1000 - 50_000; doc.AnchorMs < want || doc.AnchorMs > want+2000 {
		t.Fatalf("anchor %d, want about %d", doc.AnchorMs, want)
	}
}

func TestUnpublishRemovesTheLeaseState(t *testing.T) {
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "xc_agent/test"), LeaseFile: filepath.Join(t.TempDir(), LeaseStateFile), Logf: t.Logf}
	a.publishLease()
	a.Unpublish()
	if _, err := os.Stat(a.LeaseFile); !os.IsNotExist(err) {
		t.Fatalf("lease state left behind a node MAIN stopped: %v", err)
	}
}

func TestTheLeaseReportGivesTheWindowOnMainsClock(t *testing.T) {
	f, st := newFake(t)
	iat := int64(1800000000)
	exp := iat + 7200
	holdLease(t, f, st, iat, exp)
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(iat+3600, 0)
	out, err := LeaseReport(st.path, now)
	if err != nil || !strings.Contains(out, "no "+LeaseStateFile) {
		t.Fatalf("no file: %q %v", out, err)
	}
	file := filepath.Join(filepath.Dir(st.path), LeaseStateFile)
	write := func(anchorMs, wroteAtMs int64) {
		t.Helper()
		if err := writeLeaseState(file, LeaseState{Exp: exp, Iat: iat, Gen: 1, ServerID: 3, AnchorMs: anchorMs, WroteAtMs: wroteAtMs}); err != nil {
			t.Fatal(err)
		}
	}
	write(0, now.UnixMilli())
	if out, _ := LeaseReport(st.path, now); !strings.Contains(out, "never been heard") {
		t.Fatalf("no anchor: %q", out)
	}
	// MAIN's clock half an hour behind this machine's, written 10 s ago:
	// 7200 − 1800 − 10 s of the window left, as NodeLease reckons it.
	write((iat+1800)*1000, now.UnixMilli()-10_000)
	out, _ = LeaseReport(st.path, now)
	if !strings.Contains(out, "1h29m50s left on MAIN's clock") || strings.Contains(out, "the node serves") {
		t.Fatalf("report:\n%s", out)
	}
	// Past it on MAIN's clock, and the file is old enough that the fence does
	// not judge it.
	write((exp+60)*1000, now.UnixMilli()-120_000)
	out, _ = LeaseReport(st.path, now)
	if !strings.Contains(out, "expired 3m0s ago on MAIN's clock") || !strings.Contains(out, "2m0s ago") || !strings.Contains(out, "the node serves") {
		t.Fatalf("report:\n%s", out)
	}
}
