package gateway

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeAgent answers /v1/conn on a unix socket as xc_agent's registry does.
type fakeAgent struct {
	mu      sync.Mutex
	records map[string]map[string]any
	touched []string // "<uuid> <body>"
	puts    []string
	down    bool
}

func startAgent(t *testing.T, a *fakeAgent) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "agent.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.down {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/v1/conn/")
		uuid := strings.TrimSuffix(path, "/touch")
		if r.Method == http.MethodPut {
			var rec map[string]any
			if json.NewDecoder(r.Body).Decode(&rec) != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			a.records[uuid] = rec
			a.puts = append(a.puts, uuid)
			_ = json.NewEncoder(w).Encode(rec)
			return
		}
		rec := a.records[uuid]
		if rec == nil {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(path, "/touch") {
			b, _ := io.ReadAll(r.Body)
			a.touched = append(a.touched, uuid+" "+string(b))
		}
		_ = json.NewEncoder(w).Encode(rec)
	}))
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)
	return sock
}

func TestAgentConnsTouchAndPeek(t *testing.T) {
	a := &fakeAgent{records: map[string]map[string]any{"live": {"hls_end": 0}, "ended": {"hls_end": 1}, "endedstr": {"hls_end": "1"}, "zerostr": {"hls_end": "0"}}}
	c := NewAgentConns(startAgent(t, a))
	for uuid, want := range map[string]bool{"live": false, "ended": true, "endedstr": true, "zerostr": false, "missing": false} {
		ended, ok := c.Peek(uuid)
		if !ok || ended != want {
			t.Errorf("peek %s: ended %v ok %v; want %v", uuid, ended, ok, want)
		}
	}
	if len(a.touched) != 0 {
		t.Fatalf("peek touched: %v", a.touched)
	}
	if ended, ok := c.Touch("live", 1799999990); !ok || ended {
		t.Fatalf("touch: %v %v", ended, ok)
	}
	if len(a.touched) != 1 || a.touched[0] != `live {"hls_last_read":1799999990}` {
		t.Fatalf("touched: %v", a.touched)
	}
	a.down = true
	if _, ok := c.Touch("live", 1); ok {
		t.Fatal("an agent that fails: not ok")
	}
	if _, ok := NewAgentConns(filepath.Join(t.TempDir(), "none.sock")).Peek("live"); ok {
		t.Fatal("no agent: not ok")
	}
}

type fakeFiles struct {
	path      string
	offset    int64
	playlists map[string]string
}

func (f *fakeFiles) FedPlaylist(id string) string { return f.playlists[id] }

func (f *fakeFiles) Fed(id string) bool { return f.playlists[id] != "" }

func (f *fakeFiles) ServeFilePart(w http.ResponseWriter, _ *http.Request, path string, offset int64, contentType string) {
	f.path, f.offset = path, offset
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write([]byte("MINUTE"))
}

func TestServeCatchUpTouchesTheViewerAndServesTheMinute(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"cons", "archive/12"} {
		_ = os.MkdirAll(filepath.Join(dir, d), 0o755)
	}
	_ = os.WriteFile(filepath.Join(dir, "cons", testUUID), nil, 0o644)
	_ = os.WriteFile(filepath.Join(dir, "archive/12/2026-10-08:12-00.ts"), []byte("x"), 0o644)
	a := &fakeAgent{records: map[string]map[string]any{testUUID: {"hls_end": 0}}}
	sock := startAgent(t, a)
	path := writeServePolicy(t, dir, "segments", func(doc map[string]any) {
		doc["conn_store"] = "agent"
		doc["time_offset"] = 10
		doc["paths"].(map[string]any)["agent_sock"] = sock
	})
	files := &fakeFiles{}
	s := NewServer(path, http.NotFoundHandler(), files)
	ip := "198.51.100.7"
	rec := serveReq(s, "/hls/"+archiveTok("12_2026-10-08:12-00.ts_188", ip), ip)
	if rec.Code != 200 || rec.Body.String() != "MINUTE" || rec.Header().Get("Content-Type") != "video/mp2t" {
		t.Fatalf("catch-up: %d %q %v", rec.Code, rec.Body.String(), rec.Header())
	}
	if files.path != filepath.Join(dir, "archive/12/2026-10-08:12-00.ts") || files.offset != 188 {
		t.Fatalf("served %s from %d", files.path, files.offset)
	}
	if len(a.touched) != 1 || !strings.HasPrefix(a.touched[0], testUUID+` {"hls_last_read":`) {
		t.Fatalf("the viewer is heard as PHP's heartbeat hears it: %v", a.touched)
	}

	// Shadow judges without touching.
	a.touched = nil
	shadowPath := writeServePolicy(t, dir, "shadow", func(doc map[string]any) {
		doc["conn_store"] = "agent"
		doc["paths"].(map[string]any)["agent_sock"] = sock
	})
	sh := NewServer(shadowPath, http.NotFoundHandler(), files)
	req := httptest.NewRequest(http.MethodPost, "/shadow", nil)
	req.Header.Set("X-XC-Original-URI", "/hls/"+archiveTok("12_2026-10-08:12-00.ts_0", ip))
	req.Header.Set("X-XC-Client-IP", ip)
	sh.ServeHTTP(httptest.NewRecorder(), req)
	if len(a.touched) != 0 || sh.Stats()["segment serve archive"] != 1 {
		t.Fatalf("shadow: touched %v, stats %v", a.touched, sh.Stats())
	}

	// The session ended: PHP's 404.
	a.records[testUUID] = map[string]any{"hls_end": 1}
	if rec = serveReq(s, "/hls/"+archiveTok("12_2026-10-08:12-00.ts_0", ip), ip); rec.Code != 404 {
		t.Fatalf("ended: %d", rec.Code)
	}
}

func TestPHPNonEmpty(t *testing.T) {
	for raw, want := range map[string]bool{`0`: false, `1`: true, `"0"`: false, `""`: false, `"00"`: true, `null`: false, `false`: false, `true`: true, `[]`: false, `[0]`: true, ``: false} {
		if got := phpNonEmpty(json.RawMessage(raw)); got != want {
			t.Errorf("%q: %v; want %v", raw, got, want)
		}
	}
}
