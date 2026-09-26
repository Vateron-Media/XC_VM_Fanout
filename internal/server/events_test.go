package server

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/supervisor"
)

func view(running, confirmed bool, pid int, uptime int64) monitorStateView {
	return monitorStateView{State: supervisor.State{Supervised: true, Running: running, Confirmed: confirmed, PID: pid, UptimeMS: uptime}}
}

func TestEventsPublishOnlyTransitions(t *testing.T) {
	l := newEventLog()
	l.observe(map[string]monitorStateView{"1": view(true, false, 10, 0)})
	l.observe(map[string]monitorStateView{"1": view(true, false, 10, 250)}) // only uptime moved
	l.observe(map[string]monitorStateView{"1": view(true, true, 10, 500), "2": view(false, false, 0, 0)})
	out, _ := l.since(l.boot, 0)
	if out == nil || out.Reset || len(out.Events) != 3 || out.Seq != 3 {
		t.Fatalf("%+v", out)
	}
	if e := out.Events[1]; e.Stream != "1" || !e.State.Confirmed {
		t.Fatalf("second event %+v", e)
	}
	if out, wake := l.since(l.boot, 3); out != nil || wake == nil {
		t.Fatal("nothing new should wait")
	}
}

func TestEventsResetForANewOrLaggingConsumer(t *testing.T) {
	old := EventsRing
	EventsRing = 2
	defer func() { EventsRing = old }()
	l := newEventLog()
	for pid := 1; pid <= 5; pid++ {
		l.observe(map[string]monitorStateView{"7": view(true, true, pid, 0)})
	}
	for name, c := range map[string]struct {
		boot  string
		since uint64
	}{"other daemon life": {"feedface", 5}, "behind the ring": {l.boot, 1}, "ahead": {l.boot, 99}} {
		out, _ := l.since(c.boot, c.since)
		if out == nil || !out.Reset || out.Seq != 5 || len(out.Events) != 1 || out.Events[0].State.PID != 5 {
			t.Errorf("%s: %+v", name, out)
		}
	}
	if out, _ := l.since(l.boot, 4); out == nil || out.Reset || len(out.Events) != 1 {
		t.Fatalf("within the ring: %+v", out)
	}
	l.observe(map[string]monitorStateView{})
	if out, _ := l.since("", 0); len(out.Events) != 0 {
		t.Fatal("an unsupervised stream stays in the snapshot")
	}
}

func TestEventsLongPollWakesOnATransition(t *testing.T) {
	m := NewManager(1<<20, 1000, 2, 5, time.Second)
	m.events.observe(map[string]monitorStateView{"3": view(true, false, 1, 0)})
	go func() {
		time.Sleep(100 * time.Millisecond)
		m.events.observe(map[string]monitorStateView{"3": view(true, true, 1, 0)})
	}()
	start := time.Now()
	rec := httptest.NewRecorder()
	m.serveEvents(rec, httptest.NewRequest("GET", "/events?boot="+m.events.boot+"&since=1&wait=5", nil))
	var out eventsReply
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Events) != 1 || !out.Events[0].State.Confirmed {
		t.Fatalf("%s %v", rec.Body.String(), err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the poll was not woken")
	}
	rec = httptest.NewRecorder()
	m.serveEvents(rec, httptest.NewRequest("GET", "/events?boot="+m.events.boot+"&since=2&wait=0", nil))
	if json.Unmarshal(rec.Body.Bytes(), &out); out.Seq != 2 || len(out.Events) != 0 || out.Reset {
		t.Fatalf("empty poll: %s", rec.Body.String())
	}
}

func TestEventsPublishAViewersLastClose(t *testing.T) {
	m := &Manager{events: newEventLog()}
	st := &Stream{id: "9", mgr: m}
	st.addConn("abc")
	st.addConn("abc") // a second connection with the same uuid (a reconnect overlap)
	st.removeConn("abc")
	if out, _ := m.events.since(m.events.boot, 0); out != nil {
		t.Fatalf("published while a connection remains: %+v", out)
	}
	st.removeConn("abc")
	out, _ := m.events.since(m.events.boot, 0)
	if out == nil || len(out.Events) != 1 || out.Events[0].Type != "conn_close" || out.Events[0].Stream != "9" || out.Events[0].UUID != "abc" {
		t.Fatalf("%+v", out)
	}
	b, _ := json.Marshal(out.Events[0])
	if string(b) != `{"seq":1,"type":"conn_close","stream":"9","uuid":"abc"}` {
		t.Fatalf("wire form %s", b)
	}
	st.removeConn("abc") // unknown now: nothing more
	if out, _ := m.events.since(m.events.boot, 1); out != nil {
		t.Fatalf("%+v", out)
	}
}
