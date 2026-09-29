package clusteragent

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeFanoutConns is a fanout control socket that lists the connections it
// serves and drops one on DELETE, recording the drops.
type fakeFanoutConns struct {
	mu      sync.Mutex
	live    map[string]bool
	dropped []string
}

func newFakeFanoutConns(t *testing.T, uuids ...string) (string, *fakeFanoutConns) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "xf")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "c.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeFanoutConns{live: map[string]bool{}}
	for _, u := range uuids {
		f.live[u] = true
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/connections":
			out := []string{}
			for u := range f.live {
				out = append(out, u)
			}
			sort.Strings(out)
			json.NewEncoder(w).Encode(out)
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/connections/"):
			u := strings.TrimPrefix(r.URL.Path, "/connections/")
			f.dropped = append(f.dropped, u)
			if !f.live[u] {
				http.NotFound(w, r)
				return
			}
			delete(f.live, u)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	})}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return sock, f
}

func (f *fakeFanoutConns) drops() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]string(nil), f.dropped...)
	sort.Strings(out)
	return out
}

func (f *fakeFanoutConns) add(u string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.live[u] = true
}

// leaseFenceAgent is a mode-1 agent holding a lease that expires at exp, with
// MAIN's clock at mainMs and the replica's settings section as given.
func leaseFenceAgent(t *testing.T, sock string, exp, mainMs int64, settings map[string]any) *Agent {
	t.Helper()
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, FanoutCtl: sock, ReplicaDir: t.TempDir()}
	a.mode.Store(1)
	st.Lease = &Lease{Exp: exp, Iat: exp - 3600, Gen: 1, ServerID: 3}
	if mainMs > 0 {
		a.Client.setMainTime(mainMs)
	}
	if settings != nil {
		b, _ := json.Marshal(map[string]any{"etag": "e1", "data": settings})
		if err := os.WriteFile(filepath.Join(a.ReplicaDir, "settings.json"), b, 0o640); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

// Past the lease's exp plus lb_fence_drain_min on MAIN's clock, with the
// switch on, every viewer the fanout serves is dropped, each once; a viewer
// that attaches later is dropped at the next tick.
func TestALeasePastItsDrainDropsTheFanoutsViewers(t *testing.T) {
	sock, f := newFakeFanoutConns(t, "v1", "v2", "../x")
	now := time.Now().UnixMilli()
	exp := now/1000 - 10*60 - 5 // the default drain (10 min) is over
	a := leaseFenceAgent(t, sock, exp, now, map[string]any{"lb_lease_fence": "1", "lb_fence_drain_min": nil})
	if fenced, why := a.leaseFenced(); !fenced {
		t.Fatalf("not fenced: %s", why)
	}
	a.leaseFenceTick()
	if got := f.drops(); strings.Join(got, ",") != "v1,v2" {
		t.Fatalf("dropped %v, want v1,v2 (never a malformed uuid)", got)
	}
	a.leaseFenceTick()
	if got := f.drops(); len(got) != 2 {
		t.Fatalf("dropped again: %v", got)
	}
	f.add("v3")
	a.leaseFenceTick()
	if got := f.drops(); strings.Join(got, ",") != "v1,v2,v3" {
		t.Fatalf("a viewer that attached later: %v", got)
	}
}

// Every uncertainty serves, as NodeLease: nothing is dropped.
func TestALeaseFenceServesOnEveryUncertainty(t *testing.T) {
	now := time.Now().UnixMilli()
	past := now/1000 - 3600
	on := map[string]any{"lb_lease_fence": "1", "lb_fence_drain_min": "10"}
	cases := []struct {
		name     string
		exp      int64
		mainMs   int64
		settings map[string]any
		mode     int64
		want     string
	}{
		{"the switch off", past, now, map[string]any{"lb_lease_fence": "0"}, 1, "the switch is off"},
		{"no replica settings", past, now, nil, 1, "no replica settings"},
		{"mode 0", past, now, on, 0, "not a cluster node"},
		{"no lease", 0, now, on, 1, "no lease"},
		{"MAIN never heard", past, 0, on, 1, "MAIN's time never seen"},
		{"still draining", now/1000 - 9*60, now, on, 1, "serving or draining"},
		{"a longer drain", now/1000 - 11*60, now, map[string]any{"lb_lease_fence": float64(1), "lb_fence_drain_min": "20"}, 1, "serving or draining"},
		{"serving", now/1000 + 60, now, on, 1, "serving or draining"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sock, f := newFakeFanoutConns(t, "v1")
			a := leaseFenceAgent(t, sock, c.exp, c.mainMs, c.settings)
			a.mode.Store(c.mode)
			if c.exp == 0 {
				a.Client.State.Lease = nil
			}
			if fenced, why := a.leaseFenced(); fenced || why != c.want {
				t.Fatalf("fenced %v (%s), want serving (%s)", fenced, why, c.want)
			}
			a.leaseFenceTick()
			if got := f.drops(); len(got) != 0 {
				t.Fatalf("dropped %v", got)
			}
		})
	}
}

// A drain out of MAIN's bounds reads as its default, as ClusterSettings does.
func TestTheLeaseFenceSettingsAreReadAsMAINKeepsThem(t *testing.T) {
	a := leaseFenceAgent(t, "", 0, 0, map[string]any{"lb_lease_fence": "1", "lb_fence_drain_min": "61"})
	if on, drain, ok := a.leaseFenceSettings(); !on || drain != LeaseFenceDrainMinDefault || !ok {
		t.Fatalf("on %v drain %d ok %v", on, drain, ok)
	}
	os.WriteFile(filepath.Join(a.ReplicaDir, "settings.json"), []byte(`{"data":{"lb_lease_fence":"yes","lb_fence_drain_min":"0"}}`), 0o640)
	if on, drain, _ := a.leaseFenceSettings(); on || drain != 0 {
		t.Fatalf("on %v drain %d", on, drain)
	}
	os.WriteFile(filepath.Join(a.ReplicaDir, "settings.json"), []byte(`not json`), 0o640)
	if _, _, ok := a.leaseFenceSettings(); ok {
		t.Fatal("a broken settings file read as settings")
	}
}

// A fence MAIN commanded also drops the fanout viewers the registry does not
// hold (its CONNECTIONS flow off), which the fanout lists itself.
func TestACommandedFenceDropsTheFanoutsOwnViewersToo(t *testing.T) {
	sock, f := newFakeFanoutConns(t, "v1", "daemon9")
	_, a, _ := fenceAgent(t)
	a.FanoutCtl = sock
	a.controlExec(t.Context(), controlCmd(TypeFence, map[string]any{"reason": "admin", "drain_min": float64(0)}))
	if a.fenceTick(time.Now()) != FenceFenced {
		t.Fatal("not fenced")
	}
	if got := f.drops(); !strings.Contains(strings.Join(got, ","), "daemon9") {
		t.Fatalf("dropped %v: the fanout's own viewer was missed", got)
	}
}
