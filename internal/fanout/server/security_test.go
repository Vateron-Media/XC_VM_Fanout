package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPullerPanicIsContainedToStream(t *testing.T) {
	s := &Stream{id: "7", running: true, pullGen: 1}
	func() {
		defer s.finishPuller(1)
		panic("hostile input")
	}()
	if s.running || s.cancel != nil {
		t.Fatal("panicked puller remained running")
	}
}

func TestControlBodyIsBounded(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, 0)
	m.EnableSupervision("/bin/true")
	body := append([]byte(`{"padding":"`), bytes.Repeat([]byte("x"), maxControlBody+1)...)
	body = append(body, []byte(`"}`)...)
	req := httptest.NewRequest(http.MethodPost, "/monitor/7", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	m.ControlHandler().ServeHTTP(rec, req)
	if rec.Code >= 200 && rec.Code < 300 {
		t.Fatalf("oversize control body accepted: %d", rec.Code)
	}
}
