package clusteragent

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// readP1Events reads every event spooled under <spoolDir>/p1.
func readP1Events(t *testing.T, spoolDir string) []map[string]any {
	t.Helper()
	ents, _ := os.ReadDir(filepath.Join(spoolDir, "p1"))
	var evs []map[string]any
	for _, e := range ents {
		b, _ := os.ReadFile(filepath.Join(spoolDir, "p1", e.Name()))
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line == "" {
				continue
			}
			var ev map[string]any
			if json.Unmarshal([]byte(line), &ev) == nil {
				evs = append(evs, ev)
			}
		}
	}
	return evs
}

func eventRows(ev map[string]any) []any {
	d, _ := ev["d"].(map[string]any)
	rows, _ := d["rows"].([]any)
	return rows
}

func streamLogLine(streamID int, action, source, tm string) string {
	j, _ := json.Marshal(map[string]any{"stream_id": streamID, "action": action, "source": source, "time": tm})
	return base64.StdEncoding.EncodeToString(j) + "\n"
}

func newDrainAgent(t *testing.T) *Agent {
	t.Helper()
	_, st := newFake(t) // ServerID = 3
	a := &Agent{Client: NewClient(st, "t"), SpoolDir: t.TempDir(), TmpRoot: t.TempDir(), Logf: t.Logf}
	if err := os.MkdirAll(a.logsTmpDir(), 0o750); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestDrainStreamLogs(t *testing.T) {
	a := newDrainAgent(t)
	body := streamLogLine(100, "START", "http://h/live/u/secret/1", "1700000000") +
		streamLogLine(101, "STOP", "src2", "1700000001") +
		streamLogLine(0, "SKIP", "ignored", "1700000002") // stream_id 0 -> skipped
	logFile := filepath.Join(a.logsTmpDir(), "stream_log.log")
	if err := os.WriteFile(logFile, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := a.drainStreamLogs(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(logFile); err == nil {
		t.Fatal("stream_log.log must be unlinked after draining")
	}
	evs := readP1Events(t, a.SpoolDir)
	if len(evs) != 1 || evs[0]["type"] != "log.stream" {
		t.Fatalf("events = %+v", evs)
	}
	rows := eventRows(evs[0])
	if len(rows) != 2 {
		t.Fatalf("want 2 rows (stream_id 0 skipped), got %d", len(rows))
	}
	r0 := rows[0].(map[string]any)
	if r0["server_id"] != float64(3) {
		t.Fatalf("server_id = %v, want 3", r0["server_id"])
	}
	if r0["source"] != "http://h/live/***/***/1" {
		t.Fatalf("source not redacted: %v", r0["source"])
	}
}

func TestLogEventsChunk(t *testing.T) {
	rows := make([]map[string]any, 1001)
	for i := range rows {
		rows[i] = map[string]any{"stream_id": i, "action": "A", "source": "s", "date": "1"}
	}
	evs := logEvents("stream", streamLogCols, rows)
	if len(evs) != 2 {
		t.Fatalf("want 2 events (1000+1), got %d", len(evs))
	}
	if n := len(evs[0]["d"].(map[string]any)["rows"].([]map[string]any)); n != 1000 {
		t.Fatalf("first event rows = %d, want 1000", n)
	}
}

func TestLogEventsSpoolBytes(t *testing.T) {
	big := strings.Repeat("x", 400*1024)
	rows := []map[string]any{
		{"stream_id": 1, "action": "A", "source": big, "date": "1"},
		{"stream_id": 2, "action": "A", "source": big, "date": "1"},
		{"stream_id": 3, "action": "A", "source": big, "date": "1"},
	}
	evs := logEvents("stream", streamLogCols, rows)
	if len(evs) != 2 {
		t.Fatalf("want 2 events (byte cap split), got %d", len(evs))
	}
	if n := len(evs[0]["d"].(map[string]any)["rows"].([]map[string]any)); n != 2 {
		t.Fatalf("first event rows = %d, want 2", n)
	}
}
