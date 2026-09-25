package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// eventsMain is MAIN's `events` op in miniature: a cursor per lane, P0
// gap-checked (409 USEQ_GAP), a batch applied together with its cursor, and
// optionally a reply lost after the batch was applied.
type eventsMain struct {
	*fakeMain
	cursor   map[string]int64
	applied  map[string][]string // event "d.n" values, in order
	loseNext bool
	calls    int
}

func newEventsMain(t *testing.T) (*eventsMain, *Agent) {
	f, st := newFake(t)
	m := &eventsMain{fakeMain: f, cursor: map[string]int64{}, applied: map[string][]string{}}
	f.answer = m.answer
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf, SpoolDir: t.TempDir()}
	return m, a
}

func (m *eventsMain) answer(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
	m.calls++
	body, _ := io.ReadAll(r.Body)
	plain, err := cc.Unbox(m.keys.EncUp, reqCtx, body)
	if err != nil {
		http.Error(w, "bad box", 400)
		return
	}
	var req struct {
		Lane   string `json:"lane"`
		First  int64  `json:"first_useq"`
		Events []struct {
			Type string `json:"type"`
			D    struct {
				N     string `json:"n"`
				Count int    `json:"count"`
			} `json:"d"`
		} `json:"events"`
	}
	json.Unmarshal(plain, &req)
	cur := m.cursor[req.Lane]
	last := req.First + int64(len(req.Events)) - 1
	if req.Lane == "p0" && last > cur && req.First != cur+1 {
		doc, _ := json.Marshal(map[string]any{"v": 1, "typ": "xcvm-denial", "reason": "USEQ_GAP", "node": m.uuid, "req_nonce": hex.EncodeToString(nonce), "expected_useq": cur + 1})
		w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.panel, cc.PanelSigInput("den", doc))))
		w.WriteHeader(409)
		w.Write(doc)
		return
	}
	for i, e := range req.Events {
		if req.First+int64(i) > cur {
			v := e.D.N
			if e.Type == "skip" {
				v = fmt.Sprintf("skip:%d", e.D.Count)
			}
			m.applied[req.Lane] = append(m.applied[req.Lane], v)
		}
	}
	if last > cur {
		m.cursor[req.Lane] = last
	}
	if m.loseNext {
		m.loseNext = false
		http.Error(w, "gateway timeout", 504)
		return
	}
	ts := uint64(time.Now().UnixMilli())
	rn := make([]byte, 16)
	resCtx, _ := cc.ResponseContext(reqCtx, 200, octet, ts, rn)
	out, _ := json.Marshal(map[string]any{"useq": m.cursor[req.Lane], "applied": len(req.Events), "dropped": 0, "main_time_ms": ts})
	sealed, _ := cc.Box(m.keys.EncDown, resCtx, out)
	w.Header().Set("Content-Type", octet)
	w.Header().Set(cc.HTs, strconv.FormatUint(ts, 10))
	w.Header().Set(cc.HNonce, hex.EncodeToString(rn))
	w.Header().Set(cc.HSig, hex.EncodeToString(cc.MAC(m.keys.MacDown, resCtx, sealed)))
	w.Write(sealed)
}

// spool writes events as the PHP side does: one file per write, in name order.
func spool(t *testing.T, dir, lane string, seq int, ns ...string) {
	t.Helper()
	os.MkdirAll(filepath.Join(dir, lane), 0o750)
	var b strings.Builder
	for _, n := range ns {
		fmt.Fprintf(&b, `{"type":"stream.state","t":1,"d":{"n":%q}}`+"\n", n)
	}
	name := fmt.Sprintf("%019d-1-0000.ndjson", seq)
	if err := os.WriteFile(filepath.Join(dir, lane, name), []byte(b.String()), 0o640); err != nil {
		t.Fatal(err)
	}
}

func laneFor(a *Agent, name string) *laneSpool {
	for _, l := range Lanes {
		if l.Name == name {
			return &laneSpool{lane: l, dir: filepath.Join(a.SpoolDir, name), state: filepath.Join(a.SpoolDir, name+".inflight")}
		}
	}
	return nil
}

func drain(t *testing.T, a *Agent, ls *laneSpool, next int64) int64 {
	t.Helper()
	for i := 0; i < 20; i++ {
		n, err := a.shipOnce(context.Background(), ls, next)
		if err != nil {
			t.Logf("ship: %v", err)
		}
		next = n
		left, _ := ls.spooled()
		if _, statErr := os.Stat(ls.state); len(left) == 0 && os.IsNotExist(statErr) {
			return next
		}
	}
	t.Fatal("the spool did not drain")
	return 0
}

func TestEventsGoInOrderAndOnceAcrossALostReply(t *testing.T) {
	m, a := newEventsMain(t)
	spool(t, a.SpoolDir, "p0", 1, "a", "b")
	spool(t, a.SpoolDir, "p0", 2, "c")
	m.loseNext = true // MAIN applies the batch, the reply is lost
	ls := laneFor(a, "p0")
	next := drain(t, a, ls, 1)
	spool(t, a.SpoolDir, "p0", 3, "d")
	next = drain(t, a, ls, next)
	if got := strings.Join(m.applied["p0"], ","); got != "a,b,c,d" || next != 5 || m.cursor["p0"] != 4 {
		t.Fatalf("applied %s, next %d, cursor %d", got, next, m.cursor["p0"])
	}
}

func TestEventsResumeFromTheInflightRecord(t *testing.T) {
	m, a := newEventsMain(t)
	ls := laneFor(a, "p0")

	// Sent before a crash and applied: MAIN's cursor (from hello) covers it.
	spool(t, a.SpoolDir, "p0", 1, "a", "b")
	ls.saveInflight(&inflight{First: 1, Count: 2, Files: []string{fmt.Sprintf("%019d-1-0000.ndjson", 1)}})
	m.cursor["p0"] = 2
	drain(t, a, ls, 3)
	if m.calls != 0 {
		t.Fatalf("an applied batch was sent again (%d calls)", m.calls)
	}

	// Numbered but never applied, and MAIN's cursor went back: renumbered.
	spool(t, a.SpoolDir, "p0", 2, "c")
	ls.saveInflight(&inflight{First: 9, Count: 1, Files: []string{fmt.Sprintf("%019d-1-0000.ndjson", 2)}})
	next := drain(t, a, ls, 3)
	if got := strings.Join(m.applied["p0"], ","); got != "c" || next != 4 {
		t.Fatalf("applied %s, next %d", got, next)
	}
}

func TestEventsRewindOnAGap(t *testing.T) {
	m, a := newEventsMain(t)
	m.cursor["p0"] = 2 // MAIN's cursor went back (a restored database); the agent was at 7
	spool(t, a.SpoolDir, "p0", 1, "x")
	if next := drain(t, a, laneFor(a, "p0"), 7); next != 4 || strings.Join(m.applied["p0"], ",") != "x" {
		t.Fatalf("next %d, applied %v", next, m.applied["p0"])
	}
}

func TestP1DropsTheOldestPastItsCapAndSaysSo(t *testing.T) {
	m, a := newEventsMain(t)
	ls := laneFor(a, "p1")
	ls.lane.Cap = 100
	for i := 1; i <= 4; i++ {
		spool(t, a.SpoolDir, "p1", i, fmt.Sprintf("l%d", i)) // 44 bytes each
	}
	drain(t, a, ls, 1)
	if got := strings.Join(m.applied["p1"], ","); got != "skip:2,l3,l4" {
		t.Fatalf("applied %s", got)
	}
}

func TestSpoolIgnoresPartialAndForeignFiles(t *testing.T) {
	_, a := newEventsMain(t)
	dir := filepath.Join(a.SpoolDir, "p0")
	os.MkdirAll(dir, 0o750)
	os.WriteFile(filepath.Join(dir, ".0000000000000000001-1-0000.ndjson.tmp"), []byte(`{"type":"stream.state","d":{}}`), 0o640)
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o640)
	os.WriteFile(filepath.Join(dir, "0000000000000000002-1-0000.ndjson"), []byte("garbage\n{\"type\":\"stream.state\",\"d\":{}}\n"), 0o640)
	files, events, err := laneFor(a, "p0").collect()
	if err != nil || len(files) != 1 || len(events) != 1 {
		t.Fatalf("files %v, events %d, err %v", files, len(events), err)
	}
}

// TestInteropEvents runs the event lanes end to end against the panel's real
// PHP: the LB-side StreamStateWriter and LogSink spool redacted events (the
// flows file as this agent wrote it), the agent sends them, and MAIN's
// ClusterApi applies them to the node's own row and log table. Opt-in, as the
// other interop tests.
func TestInteropEvents(t *testing.T) {
	panel := os.Getenv("XCVM_PANEL_DIR")
	if panel == "" {
		t.Skip("XCVM_PANEL_DIR not set")
	}
	php, err := exec.LookPath("php")
	if err != nil {
		t.Skip("php not found")
	}
	dir := t.TempDir()
	harness, _ := filepath.Abs("testdata/panel")
	port := freePort(t)
	env := append(os.Environ(), "XCVM_PANEL_DIR="+panel, "XCVM_INTEROP_DB="+filepath.Join(dir, "main.sqlite"), fmt.Sprintf("XCVM_INTEROP_PORT=%d", port))
	runPHP := func(script string, args ...string) string {
		cmd := exec.Command(php, append([]string{filepath.Join(harness, script)}, args...)...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", script, args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	code := runPHP("enrol_code.php")
	srv := exec.Command(php, "-S", fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(harness, "router.php"))
	srv.Env = env
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Process.Kill()
	waitPort(t, port)
	old := EnrolPoll
	EnrolPoll = 100 * time.Millisecond
	defer func() { EnrolPoll = old }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	statePath := filepath.Join(dir, "agent.json")
	sasCh, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- EnrolByCode(ctx, statePath, code, "xc_agent/interop", false, func(s string) { sasCh <- s })
	}()
	runPHP("approve.php", <-sasCh)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	st, _ := LoadState(statePath)
	a := &Agent{Client: NewClient(st, "xc_agent/interop"), Version: "0.0.0-interop", Logf: t.Logf,
		FlowsFile: filepath.Join(dir, "flows.json"), SpoolDir: filepath.Join(dir, "spool")}
	runPHP("events.php", "flows", "12") // LOGS | STREAMS
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if a.cursor("p0") != 0 || a.cursor("p1") != 0 {
		t.Fatalf("cursors from hello: %d %d", a.cursor("p0"), a.cursor("p1"))
	}

	runPHP("events.php", "write", a.FlowsFile, a.SpoolDir)
	for _, lane := range []string{"p0", "p1"} {
		if files, _ := laneFor(a, lane).spooled(); len(files) != 1 {
			t.Fatalf("%s: %d spool files (the PHP side wrote to MAIN directly?)", lane, len(files))
		}
	}
	oldLanes := Lanes
	Lanes = []Lane{{Name: "p0", Interval: 50 * time.Millisecond}, {Name: "p1", Interval: 50 * time.Millisecond, Cap: 1 << 20}}
	defer func() { Lanes = oldLanes }()
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	for _, l := range Lanes {
		go a.RunEvents(lctx, l)
	}
	var got struct {
		Pid    int              `json:"pid"`
		Source string           `json:"source"`
		Logs   []map[string]any `json:"logs"`
		P0, P1 int
	}
	for i := 0; i < 100; i++ {
		json.Unmarshal([]byte(runPHP("events.php", "state")), &got)
		if got.P0 == 1 && got.P1 == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got.Pid != 4242 || strings.Contains(got.Source, "pw") || got.P0 != 1 || got.P1 != 1 || len(got.Logs) != 1 {
		t.Fatalf("MAIN holds %+v", got)
	}
	if strings.Contains(fmt.Sprint(got.Logs[0]["source"]), "pw") || fmt.Sprint(got.Logs[0]["server_id"]) != "7" {
		t.Fatalf("log row %v", got.Logs[0])
	}
	for _, lane := range []string{"p0", "p1"} {
		if files, _ := laneFor(a, lane).spooled(); len(files) != 0 {
			t.Fatalf("%s: spool not drained", lane)
		}
	}
}

func TestP0CompactsPastItsSizeToTheLatestStatePerKey(t *testing.T) {
	_, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	dir := filepath.Join(a.SpoolDir, "p0")
	os.MkdirAll(dir, 0o750)
	write := func(seq int, lines ...string) {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("%019d-1-0000.ndjson", seq)), []byte(strings.Join(lines, "\n")+"\n"), 0o640)
	}
	write(1, `{"type":"stream.state","t":1,"d":{"stream_id":5,"server_id":7,"fields":{"pid":1,"stream_status":2}}}`,
		`{"type":"recording.state","t":1,"d":{"id":3,"status":1}}`)
	write(2, `{"type":"stream.state","t":2,"d":{"stream_id":5,"server_id":7,"fields":{"pid":2}}}`,
		`{"type":"future.thing","t":2,"d":{"x":1}}`,
		`{"type":"recording.state","t":2,"d":{"id":3,"status":2}}`)
	ls.lane.Compact = 1 << 30
	if n, _ := ls.compact(); n != 0 {
		t.Fatal("compacted below its size")
	}
	ls.lane.Compact = 10
	n, err := ls.compact()
	if err != nil || n != 2 {
		t.Fatalf("folded %d, %v", n, err)
	}
	files, _ := ls.spooled()
	if len(files) != 1 || files[0].Name() != fmt.Sprintf("%019d-1-0000.ndjson", 1) {
		t.Fatalf("files %v", files)
	}
	evs, _, _ := readSpoolFile(filepath.Join(dir, files[0].Name()))
	var got []string
	for _, e := range evs {
		got = append(got, string(e))
	}
	want := []string{
		`{"d":{"fields":{"pid":2,"stream_status":2},"server_id":7,"stream_id":5},"t":2,"type":"stream.state"}`,
		`{"d":{"x":1},"t":2,"type":"future.thing"}`,
		`{"d":{"id":3,"status":2},"t":2,"type":"recording.state"}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("compacted:\n%s", strings.Join(got, "\n"))
	}
}
