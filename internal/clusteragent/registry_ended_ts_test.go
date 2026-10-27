package clusteragent

import (
	"encoding/json"
	"os"
	"testing"
)

func TestEndedTSIsMirroredThenForgotten(t *testing.T) {
	r, s, _ := newTestRegistry(t)
	c := rec(7, "x", 100, 100)
	c["container"] = "ts"
	if err := r.Put("ts1", c); err != nil {
		t.Fatal(err)
	}
	ended := clone(c)
	ended["hls_end"] = 1
	s.fail = true
	if err := r.Put("ts1", ended); err == nil || r.Get("ts1") == nil {
		t.Fatal("a failed spool must leave the live record in place")
	}
	s.fail = false
	if err := r.Put("ts1", ended); err != nil {
		t.Fatal(err)
	}
	if r.Get("ts1") != nil {
		t.Fatal("ended TS retained")
	}
	last := s.events[len(s.events)-1]["d"].(map[string]any)["record"].(map[string]any)
	if num(last["hls_end"]) != 1 {
		t.Fatal("MAIN did not receive the end")
	}
	if len(r.readAt) != 0 || len(r.sentAt) != 0 {
		t.Fatal("per-viewer bookkeeping retained")
	}
}

func TestTSMainCloseForgetsButHLSKeepsItsEndedRecord(t *testing.T) {
	r, _, _ := newTestRegistry(t)
	ts := rec(7, "x", 100, 100)
	ts["container"] = "ts"
	r.Put("ts1", ts)
	r.Put("hls1", rec(7, "x", 100, 100))
	r.Close("ts1", false)
	r.Close("hls1", false)
	r.Close("missing", false)
	if r.Get("ts1") != nil {
		t.Fatal("MAIN's TS close retained the record")
	}
	if r.Get("hls1") == nil || num(r.Get("hls1")["hls_end"]) != 1 {
		t.Fatal("HLS ended record lost")
	}
}

func TestRestoreAndSeedDropEndedTSButKeepOpenViewersAndHLS(t *testing.T) {
	r, s, _ := newTestRegistry(t)
	open := rec(7, "x", 100, 100)
	open["container"] = "ts"
	ended := clone(open)
	ended["hls_end"] = 1
	hls := rec(7, "x", 100, 100)
	hls["hls_end"] = 1
	data, _ := json.Marshal(map[string]any{"open": open, "ended": ended, "hls": hls})
	if err := os.WriteFile(r.snap, data, 0600); err != nil {
		t.Fatal(err)
	}
	back := NewRegistry(r.snap, s.emit, t.Logf)
	if back.Get("ended") != nil || back.Get("open") == nil || back.Get("hls") == nil {
		t.Fatal("wrong restored records")
	}
	open["uuid"], ended["uuid"], hls["uuid"] = "open", "ended", "hls"
	if n := back.Seed([]map[string]any{open, ended, hls}, true); n != 2 {
		t.Fatalf("seeded %d", n)
	}
	if back.Get("ended") != nil || back.Get("open") == nil || back.Get("hls") == nil {
		t.Fatal("wrong seeded records")
	}
}
