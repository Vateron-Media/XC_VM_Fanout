package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// Rules of ADR 0004's agent contracts pinned one by one, so that dropping
// any of them fails a test.

func TestLaneNextFollowsTheIntervalRules(t *testing.T) {
	bulk := &Denial{Status: 503, Reason: "RATE_LIMITED", Lane: "bulk", RetryAfterMs: 1000}
	p0 := &Denial{Status: 503, Reason: "RATE_LIMITED", Lane: "p0", RetryAfterMs: 300}
	db := &Denial{Status: 503, Reason: "DB"}
	starting := &Denial{Status: 503, Reason: "STARTING", RetryAfterMs: 5000}
	type step struct {
		served bool
		err    error
		busy   bool
		cur    time.Duration // the interval after the step
		min    time.Duration // the wait
		max    time.Duration
	}
	s := time.Second
	cases := []struct {
		name   string
		normal time.Duration
		steps  []step
	}{
		{"nothing to send waits the interval", 5 * s, []step{{false, nil, false, 5 * s, 5 * s, 5 * s}}},
		{"a served batch halves a stretched interval, and the next waits it", 5 * s, []step{
			{false, bulk, true, 10 * s, 10 * s, 10 * s},
			{false, bulk, true, 20 * s, 20 * s, 20 * s},
			{true, nil, false, 10 * s, 10 * s, 10 * s},
			{true, nil, false, 5 * s, 5 * s, 5 * s},
		}},
		{"another failure keeps the usual backoff and leaves the interval", 5 * s, []step{
			{false, bulk, true, 10 * s, 10 * s, 10 * s},
			{false, db, false, 10 * s, 10 * s, 10 * s}, // twice the normal 5 s
			{false, starting, false, 10 * s, 10 * s, 10 * s},
			{true, nil, false, 5 * s, 5 * s, 5 * s},
		}},
		{"the usual backoff: 1 s at least, MAIN's wait if longer", 200 * time.Millisecond, []step{
			{false, db, false, 200 * time.Millisecond, s, s},
			{false, starting, false, 200 * time.Millisecond, 4500 * time.Millisecond, 5500 * time.Millisecond},
		}},
		{"a p0 refusal waits retry_after_ms and leaves the interval", 200 * time.Millisecond, []step{
			{false, p0, true, 200 * time.Millisecond, 300 * time.Millisecond, 330 * time.Millisecond},
		}},
		{"a batch served in the same round as a refusal", 5 * s, []step{
			{false, bulk, true, 10 * s, 10 * s, 10 * s},
			{true, bulk, true, 10 * s, 10 * s, 10 * s}, // halved to 5, doubled again
		}},
	}
	for _, c := range cases {
		iv := newLaneInterval(c.normal)
		for i, st := range c.steps {
			w, busy := iv.next(st.served, st.err)
			if busy != st.busy || iv.cur != st.cur || w < st.min || w > st.max {
				t.Fatalf("%s, step %d: wait %s busy %v interval %s; want %s–%s busy %v interval %s", c.name, i, w, busy, iv.cur, st.min, st.max, st.busy, st.cur)
			}
		}
	}
}

func TestP2SendsOneBatchPerInterval(t *testing.T) {
	m, a, _, _ := newTouchAgent(t)
	a.publish(p2Reply)
	for i := 0; i < MaxBatchEvents+500; i++ {
		uuid := "t" + itoa(i)
		a.Registry.Put(uuid, rec(7, "10.0.0.1", 100, 100))
		a.Registry.Touch(uuid, 101)
	}
	served, err := a.sendTouchBatch(context.Background())
	if err != nil || !served {
		t.Fatalf("served %v: %v", served, err)
	}
	m.mu.Lock()
	n := len(m.batches)
	m.mu.Unlock()
	if n != 1 || len(m.sent()) != MaxBatchEvents {
		t.Fatalf("%d request(s), %d touches", n, len(m.sent()))
	}
	// The loop: the rest go an interval later, not back to back.
	old := TouchLoop
	TouchLoop = 300 * time.Millisecond
	defer func() { TouchLoop = old }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.RunTouches(ctx); close(done) }()
	for len(m.sent()) < MaxBatchEvents+500 {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	m.mu.Lock()
	gap := m.at[len(m.at)-1].Sub(m.at[0])
	m.mu.Unlock()
	if gap < 250*time.Millisecond {
		t.Fatalf("the second batch followed the first after %s", gap)
	}
}

func TestHeartbeatsNeverWaitForTheReplica(t *testing.T) {
	m, st, _ := newRekeyMain(t)
	c := NewClient(st, "xc_agent/test")
	var mu sync.Mutex
	var beats []time.Time
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a := &Agent{Client: c, Version: "t", Interval: 50 * time.Millisecond, Logf: t.Logf, ReplicaDir: t.TempDir(), Telemetry: func() map[string]any {
		mu.Lock()
		defer mu.Unlock()
		beats = append(beats, time.Now())
		switch len(beats) {
		case 2:
			// Every token expires: the heartbeat is refused and the agent re-keys.
			m.mu.Lock()
			m.keys = nil
			m.mu.Unlock()
		case 6:
			cancel()
		}
		return nil
	}}
	// A sync holds the replica for the whole run (a long cluster:apply).
	a.replicaMu.Lock()
	go func() {
		<-ctx.Done()
		a.replicaMu.Unlock()
	}()
	start := time.Now()
	err := a.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(beats) < 6 || time.Since(start) > 10*time.Second {
		t.Fatalf("%d heartbeats in %s", len(beats), time.Since(start))
	}
	for i := 1; i < len(beats); i++ {
		if gap := beats[i].Sub(beats[i-1]); gap > MaxHeartbeatGap {
			t.Fatalf("heartbeats %d apart around the re-key", gap)
		}
	}
}

func TestTheConfigFlowAppliesOnceFlowsJSONHoldsIt(t *testing.T) {
	dir := t.TempDir()
	flows := filepath.Join(dir, "cluster", "flows.json")
	os.MkdirAll(filepath.Dir(flows), 0o750)
	a := &Agent{Logf: t.Logf, ReplicaDir: t.TempDir(), FlowsFile: flows}
	kicks := a.kickChans()
	kicked := func() bool {
		select {
		case <-kicks.apply:
			return true
		default:
			return false
		}
	}
	a.publish(&Reply{State: "active", Mode: 1, Flows: 0})
	// The write of CONFIG on fails: nothing changed hands, no apply.
	os.Rename(filepath.Dir(flows), filepath.Join(dir, "away"))
	a.publish(&Reply{State: "active", Mode: 1, Flows: FlowConfig})
	if kicked() {
		t.Fatal("applied while flows.json still says CONFIG off")
	}
	// Written at the next reply: now it applies.
	os.Rename(filepath.Join(dir, "away"), filepath.Dir(flows))
	a.publish(&Reply{State: "active", Mode: 1, Flows: FlowConfig})
	if !kicked() {
		t.Fatal("no apply once flows.json says CONFIG on")
	}
	a.publish(&Reply{State: "active", Mode: 1, Flows: FlowConfig})
	if kicked() {
		t.Fatal("applied again for the same bit")
	}
}

func TestRunReplicaAppliesWhenTheConfigFlowChanges(t *testing.T) {
	m, c := newReplayMain(t)
	m.answers["config"] = map[string]any{"blocklist": map[string]any{"seq": 0, "unchanged": true}}
	applied := make(chan struct{}, 8)
	a := &Agent{Client: c, Logf: t.Logf, ReplicaDir: t.TempDir(), Apply: func(context.Context) error { applied <- struct{}{}; return nil }}
	old, oldD := ReplicaPoll, ApplyDebounce
	ReplicaPoll, ApplyDebounce = time.Hour, 0
	defer func() { ReplicaPoll, ApplyDebounce = old, oldD }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.RunReplica(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	wait := func(what string) {
		t.Helper()
		select {
		case <-applied:
		case <-time.After(5 * time.Second):
			t.Fatalf("no apply %s", what)
		}
	}
	wait("after the first sync")
	a.publish(&Reply{State: "active", Mode: 1, Flows: 0})
	a.publish(&Reply{State: "active", Mode: 1, Flows: FlowConfig})
	wait("when CONFIG went on")
	a.publish(&Reply{State: "active", Mode: 1, Flows: 0})
	wait("when CONFIG went off")
	if m.count("config") != 1 {
		t.Fatalf("a flows change asked MAIN %d times", m.count("config"))
	}
}

// recordRaw signs and seals a payload given as bytes, as MAIN's PHP encodes
// it (json_encode leaves <, > and & as they are).
func (m *replicaMain) recordRaw(t *testing.T, payload []byte) string {
	body := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	body = append(append(body, payload...), ed25519.Sign(m.panel, cc.PanelSigInput("rep", payload))...)
	sealed, err := cc.Seal(m.boxPub, "replica", m.uuid, body)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(sealed)
}

func TestWholeSectionJSONHoldsTheDataExactlyAsSigned(t *testing.T) {
	m, a := newReplicaMain(t)
	etag := etagOf("settings")
	data := `{"server_name":"A <b> & \/c","z":"é"}`
	payload := `{"v":1,"section":"settings","node":"` + m.uuid + `","gen":1,"etag":"` + etag + `","data":` + data + `}`
	m.whole = append(m.whole, map[string]any{"settings": map[string]any{"etag": etag, "sealed": m.recordRaw(t, []byte(payload))}})
	if err := a.SyncReplica(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "settings.json"))
	if want := `{"data":` + data + `,"etag":"` + etag + `"}`; string(b) != want {
		t.Fatalf("settings.json\n got %s\nwant %s", b, want)
	}
}

func TestTheRecordIsWrittenBeforeItsJSON(t *testing.T) {
	m, a := newReplicaMain(t)
	os.MkdirAll(a.ReplicaDir, 0o750)
	// servers.rep cannot be written (a directory stands there).
	os.MkdirAll(filepath.Join(a.ReplicaDir, "servers.rep", "x"), 0o750)
	m.whole = append(m.whole, map[string]any{"servers": m.wholeSection(t, "servers", m.uuid, etagOf("sv"), map[string]any{"servers": []any{}})})
	if err := a.SyncReplica(context.Background()); err == nil {
		t.Fatal("a section whose record was not written was taken")
	}
	if _, err := os.Stat(filepath.Join(a.ReplicaDir, "servers.json")); err == nil {
		t.Fatal("servers.json written without its record")
	}
	if LoadReplicaState(a.ReplicaDir).WholeEtags["servers"] != "" {
		t.Fatal("the ETag of a section not stored was kept")
	}
}

func TestReplicaStateIsPrivate(t *testing.T) {
	m, a := newReplicaMain(t)
	m.whole = append(m.whole, map[string]any{"secrets": m.wholeSection(t, "secrets", m.uuid, etagOf("s"), secretsData)})
	if err := a.SyncReplica(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(filepath.Join(a.ReplicaDir, "state.json")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("state.json: %v", err)
	}
}

func TestStoredRecordsAreCheckedAtStartAndWhenTheKeysChange(t *testing.T) {
	m, a := newReplicaMain(t)
	ctx := context.Background()
	m.whole = append(m.whole, map[string]any{"servers": m.wholeSection(t, "servers", m.uuid, etagOf("sv"), map[string]any{"servers": []any{}})})
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	have := func() any { return m.asked[len(m.asked)-1]["have"].(map[string]any)["servers"] }
	// The same keys: the ETag held is named.
	if err := a.SyncReplica(ctx); err != nil || have() != etagOf("sv") {
		t.Fatalf("asked with %v: %v", have(), err)
	}
	// The node's box key changed (a re-enrolment): the next sync asks for it
	// again, with no call from the heartbeat loop.
	a.Client.State.NodeBoxSk, _, _ = cc.NewX25519()
	if err := a.SyncReplica(ctx); err != nil || have() != "" {
		t.Fatalf("after a key change asked with %v: %v", have(), err)
	}
	// A restarted agent over a replica stored under other keys does the same
	// at its first sync.
	m2, a2 := newReplicaMain(t)
	a2.ReplicaDir = a.ReplicaDir
	st := LoadReplicaState(a.ReplicaDir)
	st.setEtag("servers", etagOf("sv"))
	a.saveReplicaState(a.ReplicaDir, st)
	if err := a2.SyncReplica(ctx); err != nil || m2.asked[0]["have"].(map[string]any)["servers"] != "" {
		t.Fatalf("at start asked with %v: %v", m2.asked[0]["have"], err)
	}
}

func TestEnrolByCodeEmptiesTheKnownGoodSets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.json")
	uuid := "0f8fad5b-d9cb-469f-a165-70867728950e"
	kg, err := Keygen(path, uuid)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := loadRaw(path)
	st.KnownGoodURLs = []URLSet{{PolicyVer: 4, MainURLs: []string{"https://previous-main/cluster/v1/"}}}
	st.Save()
	pub, panel, _ := ed25519.GenerateKey(nil)
	ephPub, _ := hex.DecodeString(kg.EphPub)
	doc, _ := json.Marshal(map[string]any{"node_uuid": uuid, "server_id": 3, "epoch": 1, "token_sealed": base64.StdEncoding.EncodeToString(mintFor(t, panel, uuid, ephPub)),
		"cluster": map[string]any{"policy": map[string]any{"policy_ver": 6, "main_urls": []string{"https://main/cluster/v1/"}}}})
	h := http.Header{}
	h.Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(panel, cc.PanelSigInput("pre", doc))))
	cl := &codeClient{code: &Code{ServerID: 3}, base: "https://main/cluster/v1/", panelPub: pub}
	if err := cl.install(st, doc, h, nil); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	if strings.Contains(string(b), "known_good_urls") || strings.Contains(string(b), "previous-main") {
		t.Fatalf("enrolment by code kept the previous sets: %s", b)
	}
}

func TestAdoptingAPolicyDoesNotMakeItsSetKnownGood(t *testing.T) {
	_, st := newFake(t)
	if ok, err := st.adoptPolicy(&Policy{PolicyVer: 3, Transport: "auto", MainURLs: []string{"https://main/cluster/v1/"}}, false); !ok || err != nil {
		t.Fatalf("not adopted: %v", err)
	}
	if len(st.KnownGoodURLs) != 0 {
		t.Fatalf("adopting recorded %v", st.KnownGoodURLs)
	}
}

func TestP0HasItsOwnConnectionAndBulkNeverUsesIt(t *testing.T) {
	if tr := newP0Transport(); tr.MaxConnsPerHost != 1 || tr.MaxIdleConnsPerHost != 1 {
		t.Fatalf("P0 transport: %d conns, %d idle", tr.MaxConnsPerHost, tr.MaxIdleConnsPerHost)
	}
	m, c := newReplayMain(t)
	if c.P0HTTP == nil || c.P0HTTP == c.HTTP || c.P0HTTP.Transport == c.HTTP.Transport {
		t.Fatal("P0 shares the bulk client")
	}
	p0 := &countingTransport{next: newP0Transport()}
	c.P0HTTP = &http.Client{Timeout: 10 * time.Second, Transport: p0}
	a := &Agent{Client: c, Logf: t.Logf, SpoolDir: t.TempDir(), ReplicaDir: t.TempDir()}
	m.answers["events"] = map[string]any{"useq": 1, "applied": 1}
	spool(t, a.SpoolDir, "p1", 1, "x")
	if n, served, err := a.shipOnce(context.Background(), laneFor(a, "p1"), 1); err != nil || !served || n != 2 {
		t.Fatalf("P1: %d %v %v", n, served, err)
	}
	a.SyncReplica(context.Background())
	if p0.n.Load() != 0 {
		t.Fatalf("%d bulk request(s) over P0's connection", p0.n.Load())
	}
	spool(t, a.SpoolDir, "p0", 1, "y")
	if _, served, err := a.shipOnce(context.Background(), laneFor(a, "p0"), 1); err != nil || !served || p0.n.Load() != 1 {
		t.Fatalf("P0 over its connection: %d request(s), %v %v", p0.n.Load(), served, err)
	}
}

func TestNoLaterP0BatchGoesBeforeARefusedOne(t *testing.T) {
	m, c := newReplayMain(t)
	a := &Agent{Client: c, Logf: t.Logf, SpoolDir: t.TempDir()}
	a.cursorP0.Store(1)
	spool(t, a.SpoolDir, "p0", 1, "a")
	m.answers["events"] = map[string]any{"useq": 0, "applied": 1}
	m.queue("events", 2, 503, "RATE_LIMITED", laneBusy("p0", 250, "events"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.RunEvents(ctx, Lanes[0]); close(done) }()
	for m.count("events") < 1 && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	spool(t, a.SpoolDir, "p0", 2, "b") // spooled while the first batch waits
	for m.count("events") < 4 && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	m.mu.Lock()
	defer m.mu.Unlock()
	names := func(b map[string]any) string {
		var out []string
		for _, e := range b["events"].([]any) {
			out = append(out, e.(map[string]any)["d"].(map[string]any)["n"].(string))
		}
		return strings.Join(out, ",")
	}
	b := m.bodies["events"]
	for i := 0; i < 3; i++ {
		if names(b[i]) != "a" || b[i]["first_useq"] != float64(1) {
			t.Fatalf("request %d: %s at %v", i, names(b[i]), b[i]["first_useq"])
		}
	}
	if names(b[3]) != "b" || b[3]["first_useq"] != float64(2) {
		t.Fatalf("the later batch: %s at %v", names(b[3]), b[3]["first_useq"])
	}
}

func TestRunReplicaNeverLogsTheSecretsSection(t *testing.T) {
	m, a := newReplicaMain(t)
	logs := &logSink{}
	a.Logf = logs.logf(t)
	etag := etagOf("secrets")
	sec := m.wholeSection(t, "secrets", "11111111-1111-4111-8111-111111111111", etag, secretsData)
	m.whole = append(m.whole, map[string]any{"secrets": sec})
	old := ReplicaPoll
	ReplicaPoll = time.Hour
	defer func() { ReplicaPoll = old }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.RunReplica(ctx); close(done) }()
	for !logs.has("cluster: replica:") {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	logs.mu.Lock()
	all := strings.Join(logs.lines, "\n")
	logs.mu.Unlock()
	if !strings.Contains(all, "secrets") {
		t.Fatalf("the error does not name the section: %s", all)
	}
	if leak := secretsLeak(all, sec["sealed"].(string), etag); leak != "" {
		t.Fatalf("logged %q", leak)
	}
}

func TestASnapshotGivenUpWhileMainIsBusyIsNotAnError(t *testing.T) {
	m, c := newReplayMain(t)
	logs := &logSink{}
	a := &Agent{Client: c, Logf: logs.logf(t)}
	reg, _, _ := newTestRegistry(t)
	a.Registry = reg
	reg.Seed([]map[string]any{{"uuid": "a", "user_id": 1}}, true)
	old := SnapshotBusyRetries
	SnapshotBusyRetries = 1
	defer func() { SnapshotBusyRetries = old }()
	m.queue("conn_snapshot", 2, 503, "RATE_LIMITED", laneBusy("bulk", 1000, "conn_snapshot"))
	a.snapshot(context.Background())
	if !logs.has("MAIN stayed busy") || logs.has("RATE_LIMITED") {
		t.Fatalf("logged %v", logs.lines)
	}
}

// MAIN refuses the lane twice as busy (the interval stretched from 300 ms to
// 1.2 s), then serves it: the next batch goes after the halved interval,
// about 600 ms, not the stretched 1.2 s.
func TestP1HalvesItsIntervalAfterAServedBatch(t *testing.T) {
	m, c := newReplayMain(t)
	a := &Agent{Client: c, Logf: t.Logf, SpoolDir: t.TempDir()}
	a.cursorP1.Store(1)
	spool(t, a.SpoolDir, "p1", 1, "x")
	m.answers["events"] = map[string]any{"useq": 0, "applied": 1}
	m.queue("events", 2, 503, "RATE_LIMITED", laneBusy("bulk", 1000, "events"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.RunEvents(ctx, Lane{Name: "p1", Interval: 300 * time.Millisecond}); close(done) }()
	for m.count("events") < 3 && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	spool(t, a.SpoolDir, "p1", 2, "y")
	for m.count("events") < 4 && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	m.mu.Lock()
	gap := m.stamps["events"][3] - m.stamps["events"][2]
	m.mu.Unlock()
	if gap < 500 || gap >= 1100 {
		t.Fatalf("the next P1 batch went %d ms after a served one, want about 600", gap)
	}
}

func TestP2HalvesItsIntervalAfterAServedBatch(t *testing.T) {
	m, a, _, _ := newTouchAgent(t)
	a.publish(p2Reply)
	a.Registry.Put("h1", rec(7, "10.0.0.1", 100, 100))
	a.Registry.Touch("h1", 101)
	refusals := 2
	m.mu.Lock()
	m.status, m.reason, m.extra = 503, "RATE_LIMITED", laneBusy("bulk", 1000, "events")()
	m.mu.Unlock()
	old := TouchLoop
	TouchLoop = 300 * time.Millisecond
	defer func() { TouchLoop = old }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.RunTouches(ctx); close(done) }()
	count := func() int { m.mu.Lock(); defer m.mu.Unlock(); return len(m.batches) }
	for count() < 1 && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	// The second refusal too, then served.
	for refusals--; refusals > 0; refusals-- {
		m.mu.Lock()
		m.status, m.reason, m.extra = 503, "RATE_LIMITED", laneBusy("bulk", 1000, "events")()
		m.mu.Unlock()
		for count() < 2 && ctx.Err() == nil {
			time.Sleep(5 * time.Millisecond)
		}
	}
	for count() < 3 && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	// Another viewer's touch, due at once.
	a.Registry.Put("h2", rec(8, "10.0.0.2", 100, 100))
	a.Registry.Touch("h2", 102)
	for count() < 4 && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	m.mu.Lock()
	gap := m.at[3].Sub(m.at[2])
	m.mu.Unlock()
	if gap < 500*time.Millisecond || gap >= 1100*time.Millisecond {
		t.Fatalf("the next P2 batch went %s after a served one, want about 600 ms", gap)
	}
}
