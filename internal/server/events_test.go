package server

import (
	"encoding/json"
	"net/http/httptest"
	"runtime"
	"strconv"
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

// The ring wraps in place: once full, each event replaces the oldest, and a
// consumer anywhere in it gets exactly the events after its seq; one behind
// it, ahead of it, or from another boot gets a reset.
func TestEventsRingWrapsKeepingItsSemantics(t *testing.T) {
	old := EventsRing
	EventsRing = 3
	defer func() { EventsRing = old }()
	l := newEventLog()
	for i := 1; i <= 10; i++ {
		l.connClosed("4", "u"+strconv.Itoa(i))
	}
	l.observe(map[string]monitorStateView{"4": view(true, true, 9, 0)}) // seq 11
	seqs := func(out *eventsReply) (s []uint64) {
		for _, e := range out.Events {
			s = append(s, e.Seq)
		}
		return s
	}
	for since, want := range map[uint64][]uint64{8: {9, 10, 11}, 9: {10, 11}, 10: {11}} {
		out, wake := l.since(l.boot, since)
		if out == nil || wake != nil || out.Reset || out.Seq != 11 || len(out.Events) != len(want) {
			t.Fatalf("since %d: %+v", since, out)
		}
		for i, s := range seqs(out) {
			if s != want[i] {
				t.Fatalf("since %d: seqs %v, want %v", since, seqs(out), want)
			}
		}
	}
	if out, _ := l.since(l.boot, 9); out.Events[0].Type != "conn_close" || out.Events[0].UUID != "u10" || out.Events[1].Type != "monitor" {
		t.Fatalf("wrong events %+v", out.Events)
	}
	if out, wake := l.since(l.boot, 11); out != nil || wake == nil {
		t.Fatal("a consumer at the head should wait")
	}
	for name, c := range map[string]struct {
		boot  string
		since uint64
	}{"behind the ring": {l.boot, 7}, "ahead": {l.boot, 12}, "other boot": {"feedface", 10}, "new": {"", 0}} {
		out, _ := l.since(c.boot, c.since)
		if out == nil || !out.Reset || out.Seq != 11 || len(out.Events) != 1 || out.Events[0].State.PID != 9 {
			t.Errorf("%s: %+v", name, out)
		}
	}
}

// Publishing into a full ring costs O(1): it used to copy the whole ring
// (4096 events, about 256 KB) on every conn_close and every transition.
func TestEventsAFullRingDoesNotCopyItself(t *testing.T) {
	l := newEventLog()
	for i := 0; i < EventsRing; i++ {
		l.connClosed("1", "fill")
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const n = 2000
	for i := 0; i < n; i++ {
		l.connClosed("1", "x")
	}
	runtime.ReadMemStats(&after)
	// A wake channel per event, not a ring: the old copy was n × 256 KB.
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 8<<20 {
		t.Fatalf("%d events into a full ring allocated %d bytes", n, grew)
	}
	if l.n != EventsRing {
		t.Fatalf("ring holds %d, want %d", l.n, EventsRing)
	}
	if out, _ := l.since(l.boot, l.seq-1); out == nil || len(out.Events) != 1 || out.Events[0].Seq != l.seq {
		t.Fatalf("%+v", out)
	}
}
