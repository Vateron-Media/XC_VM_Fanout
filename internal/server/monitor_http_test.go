package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/config"
)

func supervisingManager(t *testing.T, on bool) (*Manager, http.Handler) {
	t.Helper()
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	v := config.Defaults()
	v.Supervise = on
	m.ApplyConfig(v)
	m.EnableSupervision()
	t.Cleanup(func() { m.sup.ReleaseAll() })
	return m, m.ControlHandler()
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

const sleepSpec = `{"sources":[{"label":"src","cmd":"sleep 30"}],"policy":{"start_timeout_sec":30}}`

// TestHandOverRefusedWhileSuperviseOff: with supervise off a hand-over is a 501,
// which the panel reads as "run it yourself" — and nothing is started.
func TestHandOverRefusedWhileSuperviseOff(t *testing.T) {
	m, h := supervisingManager(t, false)
	if rec := do(h, http.MethodPut, "/monitor/7", sleepSpec); rec.Code != http.StatusNotImplemented {
		t.Fatalf("PUT with supervise off = %d, want 501", rec.Code)
	}
	if ids := m.sup.IDs(); len(ids) != 0 {
		t.Errorf("a refused hand-over is being supervised: %v", ids)
	}
}

// TestSuperviseIsLive: turning supervise on in config.json takes effect on the
// next reload, with no restart; turning it off stops NEW hand-overs but leaves
// the streams already handed over running — dropping them would kill every
// encoder on the node at once.
func TestSuperviseIsLive(t *testing.T) {
	m, h := supervisingManager(t, false)
	v := config.Defaults()
	v.Supervise = true
	m.ApplyConfig(v)
	if rec := do(h, http.MethodPut, "/monitor/7", sleepSpec); rec.Code != http.StatusNoContent {
		t.Fatalf("PUT after enabling = %d %s, want 204", rec.Code, rec.Body)
	}
	v.Supervise = false
	m.ApplyConfig(v)
	if rec := do(h, http.MethodPut, "/monitor/8", sleepSpec); rec.Code != http.StatusNotImplemented {
		t.Errorf("PUT after disabling = %d, want 501", rec.Code)
	}
	if !m.sup.State("7").Supervised {
		t.Error("disabling supervise dropped a stream that was already handed over")
	}
	// The panel can still see and release it.
	if rec := do(h, http.MethodGet, "/monitor/7", ""); rec.Code != http.StatusOK {
		t.Errorf("GET of a supervised stream = %d", rec.Code)
	}
	if rec := do(h, http.MethodDelete, "/monitor/7", ""); rec.Code != http.StatusNoContent {
		t.Errorf("DELETE = %d", rec.Code)
	}
}

// TestMonitorStatesIsOneCall: the panel's reconcile reads every supervised
// stream from one response, with the daemon pid it records as monitor_pid.
func TestMonitorStatesIsOneCall(t *testing.T) {
	_, h := supervisingManager(t, true)
	for _, id := range []string{"7", "9"} {
		if rec := do(h, http.MethodPut, "/monitor/"+id, sleepSpec); rec.Code != http.StatusNoContent {
			t.Fatalf("PUT %s = %d", id, rec.Code)
		}
	}
	rec := do(h, http.MethodGet, "/monitors/state", "")
	var body struct {
		Accepting bool `json:"accepting"`
		DaemonPID int  `json:"daemon_pid"`
		Streams   map[string]struct {
			Supervised bool `json:"supervised"`
			DaemonPID  int  `json:"daemon_pid"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad body %q: %v", rec.Body, err)
	}
	if !body.Accepting || body.DaemonPID <= 0 || len(body.Streams) != 2 {
		t.Fatalf("state = %+v, want accepting with both streams", body)
	}
	for id, st := range body.Streams {
		if !st.Supervised || st.DaemonPID <= 0 {
			t.Errorf("stream %s: %+v", id, st)
		}
	}
}
