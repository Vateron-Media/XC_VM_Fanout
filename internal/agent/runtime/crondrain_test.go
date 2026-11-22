package clusteragent

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	a := &Agent{Client: NewClient(st, "t"), SpoolDir: t.TempDir(), TmpRoot: t.TempDir(), ReplicaDir: t.TempDir(), Logf: t.Logf}
	a.flows.Store(FlowLogs) // a serve-only LB spools logs to MAIN; the chores are gated on this
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

func b64line(m map[string]any) string {
	j, _ := json.Marshal(m)
	return base64.StdEncoding.EncodeToString(j) + "\n"
}

func TestDrainLinesLogs(t *testing.T) {
	a := newDrainAgent(t)
	logFile := filepath.Join(a.logsTmpDir(), "client_request.log")
	// a pre-existing claim is drained first
	if err := os.WriteFile(logFile+".import", []byte(b64line(map[string]any{"stream_id": 9, "action": "OLD", "user_ip": "9.9.9.9", "time": "1"})), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logFile, []byte(b64line(map[string]any{"stream_id": 5, "user_id": 7, "action": "PLAY", "query_string": "q", "user_agent": "UA", "user_ip": "1.2.3.4", "extra_data": "x", "time": "1700"})), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := a.drainLinesLogs(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(logFile); err == nil {
		t.Fatal("client_request.log must be claimed+drained")
	}
	if _, err := os.Stat(logFile + ".import"); err == nil {
		t.Fatal(".import must be drained+removed")
	}
	var found map[string]any
	for _, ev := range readP1Events(t, a.SpoolDir) {
		if ev["type"] != "log.client" {
			t.Fatalf("unexpected event type %v", ev["type"])
		}
		for _, r := range eventRows(ev) {
			row := r.(map[string]any)
			if row["client_status"] == "PLAY" {
				found = row
			}
		}
	}
	if found == nil {
		t.Fatal("the PLAY row did not reach a log.client event")
	}
	if found["ip"] != "1.2.3.4" || found["date"] != "1700" || found["user_id"] != "7" {
		t.Fatalf("key remap wrong: %+v", found) // user_ip→ip, time→date, every value stringified
	}
}

func validActivity() map[string]any {
	return map[string]any{"server_id": 3, "user_id": 7, "stream_id": 5, "user_ip": "1.2.3.4", "container": "ts", "date_start": 1700}
}

func TestDrainActivity(t *testing.T) {
	a := newDrainAgent(t)
	logFile := filepath.Join(a.logsTmpDir(), "activity")
	body := b64line(validActivity()) +
		b64line(map[string]any{"server_id": 3, "user_id": 7, "stream_id": 5}) // missing user_ip -> invalid, dropped
	if err := os.WriteFile(logFile, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := a.drainActivity(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(logFile); err == nil {
		t.Fatal("activity must be drained")
	}
	rows := 0
	for _, ev := range readP1Events(t, a.SpoolDir) {
		if ev["type"] != "log.activity" {
			t.Fatalf("type %v", ev["type"])
		}
		rows += len(eventRows(ev))
	}
	if rows != 1 {
		t.Fatalf("want 1 valid row (invalid dropped), got %d", rows)
	}
}

func TestDrainActivityRefusalKeeps(t *testing.T) {
	a := newDrainAgent(t)
	logFile := filepath.Join(a.logsTmpDir(), "activity")
	if err := os.WriteFile(logFile, []byte(b64line(validActivity())), 0o640); err != nil {
		t.Fatal(err)
	}
	orig := activitySpool
	activitySpool = func(a *Agent, rows []map[string]any) error { return errors.New("refused") }
	if err := a.drainActivity(); err != nil {
		t.Fatal(err)
	}
	activitySpool = orig
	if _, err := os.Stat(logFile + ".import"); err != nil {
		t.Fatal("a refused batch must be kept as .import")
	}
	if _, err := os.Stat(logFile); err == nil {
		t.Fatal("the original must have been claimed (renamed)")
	}
	// recovery: the sink works now -> the kept claim drains and is removed
	if err := a.drainActivity(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(logFile + ".import"); err == nil {
		t.Fatal(".import must drain on recovery")
	}
	if len(readP1Events(t, a.SpoolDir)) == 0 {
		t.Fatal("recovery spooled nothing")
	}
}

func TestDrainErrorsStream(t *testing.T) {
	run := func(save int) (unlinked bool, rows int) {
		a := newDrainAgent(t)
		a.node.streams = t.TempDir()
		floodSettings(t, a.ReplicaDir, map[string]any{"stream_logs_save": save})
		ef := filepath.Join(a.node.streams, "5.errors")
		if err := os.WriteFile(ef, []byte("boom\ndeprecated thing\n\n"), 0o640); err != nil { // 'deprecated' is ignore-listed, blank skipped
			t.Fatal(err)
		}
		if err := a.drainErrors(); err != nil {
			t.Fatal(err)
		}
		_, statErr := os.Stat(ef)
		for _, ev := range readP1Events(t, a.SpoolDir) {
			if ev["type"] == "log.stream_error" {
				rows += len(eventRows(ev))
			}
		}
		return os.IsNotExist(statErr), rows
	}
	if u, r := run(1); !u || r != 1 {
		t.Fatalf("save on: unlinked=%v rows=%d (want true,1)", u, r)
	}
	if u, r := run(0); !u || r != 0 {
		t.Fatalf("save off: unlinked=%v rows=%d (want true,0 — file consumed, nothing spooled)", u, r)
	}
}

func TestDrainErrorsPanel(t *testing.T) {
	a := newDrainAgent(t)
	floodSettings(t, a.ReplicaDir, map[string]any{})
	logFile := filepath.Join(a.logsTmpDir(), "error_log.log")
	dup := map[string]any{"type": "E_WARNING", "message": "boom", "file": "x.php", "line": 10}
	body := b64line(dup) + b64line(dup) + // identical -> dedup to 1
		b64line(map[string]any{"message": "MySQL server has gone away"}) // transient -> skipped
	if err := os.WriteFile(logFile, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := a.drainErrors(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(logFile); err == nil {
		t.Fatal("error_log.log must be unlinked after a successful panel spool")
	}
	rows := 0
	for _, ev := range readP1Events(t, a.SpoolDir) {
		if ev["type"] == "log.panel_error" {
			rows += len(eventRows(ev))
		}
	}
	if rows != 1 {
		t.Fatalf("want 1 panel row (dedup + transient skip), got %d", rows)
	}
}

func TestDrainErrorsPanelKeepsOnFailure(t *testing.T) {
	a := newDrainAgent(t)
	floodSettings(t, a.ReplicaDir, map[string]any{})
	logFile := filepath.Join(a.logsTmpDir(), "error_log.log")
	if err := os.WriteFile(logFile, []byte(b64line(map[string]any{"message": "boom"})), 0o640); err != nil {
		t.Fatal(err)
	}
	orig := errPanelSpool
	errPanelSpool = func(a *Agent, rows []map[string]any) error { return errors.New("refused") }
	defer func() { errPanelSpool = orig }()
	if err := a.drainErrors(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(logFile); err != nil {
		t.Fatal("error_log.log must be KEPT when the panel spool fails")
	}
}

func TestRunChoresTickDrainsAndMarks(t *testing.T) {
	a := newDrainAgent(t)
	a.NodeCronsFile = filepath.Join(t.TempDir(), "node_crons")
	if err := os.WriteFile(filepath.Join(a.logsTmpDir(), "stream_log.log"), []byte(streamLogLine(100, "A", "s", "1")), 0o640); err != nil {
		t.Fatal(err)
	}
	a.runChoresTick()
	if _, err := os.Stat(filepath.Join(a.logsTmpDir(), "stream_log.log")); err == nil {
		t.Fatal("a chores tick must drain stream_log.log")
	}
	if len(readP1Events(t, a.SpoolDir)) == 0 {
		t.Fatal("a chores tick must spool the drained rows")
	}
	fi, err := os.Stat(a.NodeCronsFile)
	if err != nil {
		t.Fatal("a chores tick must write the node_crons marker")
	}
	if time.Since(fi.ModTime()) > 5*time.Second {
		t.Fatal("marker must be fresh")
	}
	// a second tick with no input still refreshes the marker's mtime
	ageFile(t, a.NodeCronsFile, 120)
	a.runChoresTick()
	fi2, _ := os.Stat(a.NodeCronsFile)
	if time.Since(fi2.ModTime()) > 5*time.Second {
		t.Fatal("an idle tick must still refresh the marker")
	}
}

func TestChoresGatedOnLogsFlow(t *testing.T) {
	a := newDrainAgent(t)
	a.NodeCronsFile = filepath.Join(t.TempDir(), "node_crons")
	logFile := filepath.Join(a.logsTmpDir(), "stream_log.log")

	a.flows.Store(0) // LOGS off -> not a log-owning node
	if err := os.WriteFile(logFile, []byte(streamLogLine(100, "A", "s", "1")), 0o640); err != nil {
		t.Fatal(err)
	}
	a.runChoresTick()
	if _, err := os.Stat(logFile); err != nil {
		t.Fatal("LOGS off must not drain the log file")
	}
	if _, err := os.Stat(a.NodeCronsFile); err == nil {
		t.Fatal("LOGS off must not write the ownership marker")
	}
	if len(a.ownedCrons()) != 0 {
		t.Fatalf("LOGS off: owns nothing, got %v", a.ownedCrons())
	}

	a.flows.Store(FlowLogs) // LOGS on -> owns + drains + marks
	a.runChoresTick()
	if _, err := os.Stat(logFile); err == nil {
		t.Fatal("LOGS on must drain the log file")
	}
	if _, err := os.Stat(a.NodeCronsFile); err != nil {
		t.Fatal("LOGS on must write the ownership marker")
	}
	if got := a.ownedCrons(); len(got) != 5 {
		t.Fatalf("LOGS on: owns the 5 ported crons, got %v", got)
	}
}

// The marker must be refreshed far more often than the PHP side's
// NODE_CRONS_FRESH_SEC (30s) window, else the gate judges a live agent stale.
func TestChoreTouchCadenceUnderFreshWindow(t *testing.T) {
	if choreTouchInterval > 10*time.Second {
		t.Fatalf("choreTouchInterval %s must stay well under the 30s freshness window", choreTouchInterval)
	}
	if choreTouchInterval >= choreRunInterval {
		t.Fatal("the marker touch must be more frequent than the drain/sweep run")
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
