package clusteragent

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestFanoutStreamStatusAndEnsure(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "ctl.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var ensured atomic.Bool
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/streams/7":
			_ = json.NewEncoder(w).Encode(map[string]any{"running": true, "has_data": true, "refs": 1, "since_data_ms": 10})
		case r.Method == http.MethodPost && r.URL.Path == "/streams/7/ensure":
			ensured.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{"running": true})
		default:
			http.NotFound(w, r)
		}
	})}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })

	a := &Agent{FanoutCtl: sock, Logf: t.Logf}
	running, hasData, ok := a.fanoutStreamStatus(context.Background(), "7")
	if !ok || !running || !hasData {
		t.Fatalf("status: running=%v hasData=%v ok=%v", running, hasData, ok)
	}
	a.fanoutEnsure(context.Background(), "7")
	if !ensured.Load() {
		t.Fatal("ensure was not POSTed to /streams/7/ensure")
	}
	// An unknown stream (404) resolves to ok=false.
	if _, _, ok := a.fanoutStreamStatus(context.Background(), "9"); ok {
		t.Fatal("unknown stream must be ok=false")
	}
}
