package clusteragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// interopNode enrols an agent by code against MAIN's real PHP ClusterApi
// (XCVM_PANEL_DIR, as TestInteropWithPanel) and returns it with a runner for
// the harness scripts. The agent has a spool, a registry and a local socket,
// none of them running yet.
func interopNode(t *testing.T) (*Agent, func(script string, args ...string) string, context.Context) {
	a, runPHP, ctx, _ := interopNodeEnv(t)
	return a, runPHP, ctx
}

// interopNodeEnv is interopNode, plus a function that restarts MAIN's php -S
// with extra environment (settings the harness reads from it).
func interopNodeEnv(t *testing.T) (*Agent, func(script string, args ...string) string, context.Context, func(extra ...string)) {
	t.Helper()
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
	var srv *exec.Cmd
	dumpRouterLog(t, filepath.Join(dir, "main.sqlite"))
	restart := func(extra ...string) {
		if srv != nil {
			srv.Process.Kill()
			srv.Wait()
		}
		srv = exec.Command(php, "-S", fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(harness, "router.php"))
		srv.Env = append(append([]string{}, env...), extra...)
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
		waitPort(t, port)
	}
	restart()
	t.Cleanup(func() { srv.Process.Kill() })
	old := EnrolPoll
	EnrolPoll = 100 * time.Millisecond
	t.Cleanup(func() { EnrolPoll = old })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
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
	sockDir, _ := os.MkdirTemp("", "xc")
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	spool := filepath.Join(dir, "spool")
	a := &Agent{Client: NewClient(st, "xc_agent/interop"), Version: "0.0.0-interop", Logf: t.Logf,
		FlowsFile: filepath.Join(dir, "flows.json"), SpoolDir: spool, SocketPath: filepath.Join(sockDir, "a.sock")}
	a.Registry = NewRegistry(filepath.Join(dir, "registry.snap"), func(ev []map[string]any) error { return spoolP0(spool, ev) }, t.Logf)
	a.Registry.Admit = a.admit
	return a, runPHP, ctx, restart
}

func serveSocket(t *testing.T, ctx context.Context, a *Agent) {
	t.Helper()
	go a.ServeSocket(ctx, a.SocketPath)
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(a.SocketPath); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the local socket never came up")
}

// TestInteropAdmission: the node's PHP registers limited viewers with the
// X-XCVM-Admission header it builds (AgentConnections::admission), the agent
// asks MAIN's real conn_admit, and PHP reads the agent's answer.
// The lease MAIN signs travels in the approval an enrol code gets, and the node
// keeps it: PHP's LeaseService mints the bytes, the extension's `lea` tag signs
// them and Go verifies them. It is the one place the two languages meet over
// this document.
func TestInteropTheApprovedEnrolCarriesALease(t *testing.T) {
	a, _, _ := interopNode(t)
	st := a.Client.State
	if st.Lease == nil {
		t.Fatalf("no lease after enrolling by code: refused=%q", st.LeaseRefused)
	}
	if st.LeaseRefused != "" {
		t.Fatalf("refusal recorded: %q", st.LeaseRefused)
	}
	if st.Lease.ServerID != st.ServerID || st.Lease.Gen == 0 {
		t.Fatalf("lease %+v for server %d", st.Lease, st.ServerID)
	}
	if !cc.VerifyPanel(st.PanelSignPub, "lea", st.Lease.Payload, st.Lease.Sig) {
		t.Fatal("the stored bytes do not verify under the panel key MAIN sent")
	}
	// MAIN's cap, applied by the extension side and not by the agent.
	if w := st.Lease.Exp - st.Lease.Iat; w <= 0 || w > MaxLeaseSec {
		t.Fatalf("window %d s", w)
	}
}

func TestInteropAdmission(t *testing.T) {
	a, runPHP, ctx := interopNode(t)
	runPHP("admission.php", "lines")
	runPHP("events.php", "flows", "64") // CONNECTIONS
	r, err := a.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r.OfflineAdmission != OfflineLocal || a.OfflineAdmission() != OfflineLocal {
		t.Fatalf("hello carried offline_admission %q", r.OfflineAdmission)
	}
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	serveSocket(t, lctx, a)

	var out struct {
		Header bool `json:"header"`
		Result any  `json:"result"`
	}
	// A valid line: MAIN admits, the viewer is stored.
	json.Unmarshal([]byte(runPHP("admission.php", "node", a.FlowsFile, a.SocketPath, "70", "okviewer")), &out)
	if !out.Header || out.Result != true || a.Registry.Get("okviewer") == nil {
		t.Fatalf("valid line: %+v, stored %v", out, a.Registry.Get("okviewer"))
	}
	// An expired line: MAIN refuses, PHP reads the reason, nothing is stored.
	json.Unmarshal([]byte(runPHP("admission.php", "node", a.FlowsFile, a.SocketPath, "71", "expviewer")), &out)
	if !out.Header || out.Result != "EXPIRED" || a.Registry.Get("expviewer") != nil {
		t.Fatalf("expired line: %+v", out)
	}
	// An unknown line likewise.
	json.Unmarshal([]byte(runPHP("admission.php", "node", a.FlowsFile, a.SocketPath, "79", "noline")), &out)
	if out.Result != "UNKNOWN_LINE" {
		t.Fatalf("unknown line: %+v", out)
	}
	// CONNECTIONS off on MAIN while the agent still thinks it is on: a signed
	// FLOW_OFF, which admits.
	runPHP("events.php", "flows", "0")
	json.Unmarshal([]byte(runPHP("admission.php", "node", a.FlowsFile, a.SocketPath, "71", "flowoff")), &out)
	if out.Result != true {
		t.Fatalf("FLOW_OFF must admit: %+v", out)
	}
}

// TestInteropP2Touches: with CONNECTIONS on, MAIN lists conn.touch in
// p2_types; a touch goes on P2 and, without a cluster bus, lands in MAIN's
// store; the P0 upsert of the open carried the viewer there first.
func TestInteropP2Touches(t *testing.T) {
	a, runPHP, ctx := interopNode(t)
	runPHP("events.php", "flows", "64")
	a.Registry.P2 = a.p2Touch.Load
	r, err := a.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !a.p2Wanted(r) || !a.p2Touch.Load() {
		t.Fatalf("hello's p2_types %v did not turn P2 on", r.P2Types)
	}
	oldLanes := Lanes
	Lanes = []Lane{{Name: "p0", Interval: 50 * time.Millisecond}}
	defer func() { Lanes = oldLanes }()
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	go a.RunEvents(lctx, Lanes[0])
	a.Registry.Put("hlsv", map[string]any{"user_id": 7, "stream_id": 100, "server_id": 7, "user_ip": "10.0.0.9", "user_agent": "VLC", "container": "hls", "pid": nil, "date_start": 1800000000, "hls_last_read": 1800000000, "hls_end": 0})
	for i := 0; i < 100 && runPHP("touch.php", "hlsv") != "1800000000"; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	if got := runPHP("touch.php", "hlsv"); got != "1800000000" {
		t.Fatalf("the open never reached MAIN: %s", got)
	}
	a.Registry.Touch("hlsv", 1800000042)
	if err := a.sendTouches(ctx); err != nil {
		t.Fatal(err)
	}
	if got := runPHP("touch.php", "hlsv"); got != "1800000042" {
		t.Fatalf("MAIN's store after the P2 touch: %s", got)
	}
}

// TestInteropHTTPSRequiredRecovery: the drill of HttpsRequiredRecoveryTest
// against the real ClusterApi (over plain HTTP, as php -S serves it): under
// https_required every op but the challenge is refused with a signed
// HTTPS_REQUIRED; the agent takes the policy from the signed challenge over
// HTTP, and is back once the admin picks auto again.
func TestInteropHTTPSRequiredRecovery(t *testing.T) {
	a, _, ctx, restart := interopNodeEnv(t)
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	plain := a.Client.State.MainURLs[0]
	restart("XCVM_INTEROP_TRANSPORT=https_required", "XCVM_INTEROP_POLICY_VER=2")
	_, err := a.Heartbeat(ctx)
	var d *Denial
	if !errors.As(err, &d) || d.Reason != "HTTPS_REQUIRED" || d.Status != 403 {
		t.Fatalf("a heartbeat over HTTP under https_required: %v", err)
	}
	a.httpsTrouble(err)
	if err := a.PolicyOverHTTP(ctx); err != nil {
		t.Fatal(err)
	}
	st := a.Client.State
	if st.policyVer() != 2 || st.Transport != "https_required" || !strings.HasPrefix(st.MainURLs[0], "https://main.invalid:1/") {
		t.Fatalf("policy from the challenge: v%d %s %v", st.PolicyVer, st.Transport, st.MainURLs)
	}
	if got := st.plainURLs(); len(got) == 0 || got[0] != plain {
		t.Fatalf("the plain-HTTP URL was forgotten: %v", got)
	}
	// HTTPS does not work (main.invalid): the heartbeats fail.
	if _, err := a.Heartbeat(ctx); err == nil {
		t.Fatal("a heartbeat went through without HTTPS")
	}
	// The admin switches back to auto (v3): the next challenge over HTTP
	// brings the node back.
	restart("XCVM_INTEROP_TRANSPORT=auto", "XCVM_INTEROP_POLICY_VER=3")
	if err := a.PolicyOverHTTP(ctx); err != nil {
		t.Fatal(err)
	}
	if st.policyVer() != 3 || st.Transport != "auto" {
		t.Fatalf("after the switch: v%d %s", st.PolicyVer, st.Transport)
	}
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatalf("heartbeat after the switch: %v", err)
	}
	// An older policy from a replayed challenge is never adopted.
	restart("XCVM_INTEROP_TRANSPORT=https_required", "XCVM_INTEROP_POLICY_VER=2")
	if err := a.PolicyOverHTTP(ctx); err != nil {
		t.Fatal(err)
	}
	if st.policyVer() != 3 || st.Transport != "auto" {
		t.Fatalf("adopted an older policy: v%d %s", st.PolicyVer, st.Transport)
	}
}

// TestInteropIngestLaneRefusals: MAIN's real ingest permits, on a cluster bus
// (a redis-server of the test's) with one permit per lane. While a request
// MAIN is serving holds a lane's permit, the agent's config (bulk) and P0
// events are refused with the lane named; the agent counts the refusal,
// logs no error, and its next try after MAIN's wait is served.
func TestInteropIngestLaneRefusals(t *testing.T) {
	redis, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not found")
	}
	a, runPHP, ctx, restart := interopNodeEnv(t)
	sockDir, _ := os.MkdirTemp("", "xcbus")
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	bus := filepath.Join(sockDir, "bus.sock")
	rs := exec.Command(redis, "--port", "0", "--unixsocket", bus, "--save", "", "--appendonly", "no")
	if err := rs.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rs.Process.Kill(); rs.Wait() })
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(bus); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	restart("XCVM_INTEROP_BUS=" + bus)
	logs := &logSink{}
	a.Logf = logs.logf(t)
	if _, err := a.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		for deadline := time.Now().Add(20 * time.Second); !ok(); time.Sleep(20 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
		}
	}

	// Bulk: config is refused while bulk's one permit is held.
	held := runPHP("permit.php", bus, "hold", "bulk")
	a.ReplicaDir = filepath.Join(t.TempDir(), "replica")
	old := ReplicaPoll
	ReplicaPoll = time.Hour
	t.Cleanup(func() { ReplicaPoll = old })
	rctx, stopReplica := context.WithCancel(ctx)
	replicaDone := make(chan struct{})
	go func() { a.RunReplica(rctx); close(replicaDone) }()
	waitFor("config's lane refusal", func() bool { return a.BusyRefusals() >= 1 })
	runPHP("permit.php", bus, "release", "bulk", held)
	waitFor("the replica after the busy wait", func() bool { return LoadReplicaState(a.ReplicaDir).SettingsEtag != "" })
	stopReplica()
	<-replicaDone

	// P0: a batch is refused while P0's one permit is held, then sent again.
	before := a.BusyRefusals()
	held = runPHP("permit.php", bus, "hold", "p0")
	spool(t, a.SpoolDir, "p0", 1, "a")
	ectx, stopEvents := context.WithCancel(ctx)
	eventsDone := make(chan struct{})
	go func() { a.RunEvents(ectx, Lanes[0]); close(eventsDone) }()
	waitFor("P0's lane refusal", func() bool { return a.BusyRefusals() > before })
	if _, err := os.Stat(filepath.Join(a.SpoolDir, "p0.inflight")); err != nil {
		t.Fatalf("the refused batch is not kept in flight: %v", err)
	}
	runPHP("permit.php", bus, "release", "p0", held)
	waitFor("the P0 batch after the busy wait", func() bool {
		_, err := os.Stat(filepath.Join(a.SpoolDir, "p0.inflight"))
		return os.IsNotExist(err)
	})
	stopEvents()
	<-eventsDone
	var state struct {
		P0 int64 `json:"p0"`
	}
	if err := json.Unmarshal([]byte(runPHP("events.php", "state")), &state); err != nil || state.P0 < 1 {
		t.Fatalf("MAIN's P0 cursor %d: %v", state.P0, err)
	}
	// Logged as an error, a refusal would read "MAIN refused (503 RATE_LIMITED)".
	if logs.has("RATE_LIMITED") || logs.has("cluster: replica:") {
		t.Fatalf("a busy refusal was logged as an error: %v", logs.lines)
	}
}

// TestInteropStreams: the R2 streams section against MAIN's real streams op
// (StreamReplica), with the node's STREAMS flow on and the feature said at
// hello: a new node's full pass, a delta that adds, changes and removes a
// stream, a resync that finds a change no delta carries, and a full pass
// again after every stream changed at once.
func TestInteropStreams(t *testing.T) {
	a, runPHP, ctx := interopNode(t)
	a.ReplicaDir = filepath.Join(t.TempDir(), "replica")
	runPHP("events.php", "flows", "8") // STREAMS
	if _, err := a.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	stored := func() string {
		var ids []string
		for _, id := range heldStreams(a.ReplicaDir) {
			ids = append(ids, fmt.Sprint(id))
		}
		return strings.Join(ids, ",")
	}
	sync := func(what string) {
		t.Helper()
		if err := a.SyncStreams(ctx); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	record := func(id int64) string {
		t.Helper()
		rep, err := os.ReadFile(streamFile(a, id, ".rep"))
		if err != nil {
			t.Fatalf("stream %d: %v", id, err)
		}
		doc, err := a.openStream(rep, id)
		if err != nil {
			t.Fatalf("stream %d does not verify: %v", id, err)
		}
		if storedStreamEtag(a.ReplicaDir, id) != doc.Etag {
			t.Fatalf("stream %d: .json and .rep disagree", id)
		}
		return string(doc.Data)
	}
	head := func() int64 {
		var n int64
		fmt.Sscan(runPHP("stream.php", "head"), &n)
		return n
	}

	// A new node: the full pass takes stream 100 (assigned, archived and
	// recorded on server 7), and the cursor becomes MAIN's head.
	sync("full pass")
	if stored() != "100" || !strings.Contains(record(100), `"stream_display_name":`) {
		t.Fatalf("after the pass: %q", stored())
	}
	if since := streamsSince(a.ReplicaDir); since < 1 || since != head() {
		t.Fatalf("cursor %d, MAIN's head %d", since, head())
	}
	for _, f := range []string{streamFile(a, 100, ".rep"), streamFile(a, 100, ".json")} {
		if fi, err := os.Stat(f); err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("%s: %v", f, err)
		}
	}

	// Deltas: a stream added, one edited, then one taken off the node.
	runPHP("stream.php", "add", "200")
	runPHP("stream.php", "edit", "100", "Edited")
	sync("delta")
	if stored() != "100,200" || !strings.Contains(record(100), `"stream_display_name":"Edited"`) || streamsSince(a.ReplicaDir) != head() {
		t.Fatalf("after the delta: %q", stored())
	}
	runPHP("stream.php", "drop", "200")
	sync("removal")
	if stored() != "100" || streamsSince(a.ReplicaDir) != head() {
		t.Fatalf("after the removal: %q", stored())
	}
	if _, err := os.Stat(streamFile(a, 200, ".rep")); !os.IsNotExist(err) {
		t.Fatal("the removed stream's record kept")
	}

	// A change no version carries: only the resync finds it, cursor unchanged.
	runPHP("stream.php", "quiet", "100", "Quiet")
	cursor := streamsSince(a.ReplicaDir)
	sync("idle delta")
	if strings.Contains(record(100), `"Quiet"`) {
		t.Fatal("a delta carried a change without a version")
	}
	st := LoadReplicaState(a.ReplicaDir)
	st.StreamsResyncAt = 0
	a.saveStreamsState(a.ReplicaDir, st)
	sync("resync")
	if !strings.Contains(record(100), `"stream_display_name":"Quiet"`) || streamsSince(a.ReplicaDir) != cursor {
		t.Fatalf("after the resync: cursor %d", streamsSince(a.ReplicaDir))
	}

	// Every stream at once (the floor raised): MAIN answers full, the node
	// walks again and takes the new head.
	runPHP("stream.php", "reset")
	sync("full again")
	if stored() != "100" || streamsSince(a.ReplicaDir) != head() || streamsSince(a.ReplicaDir) <= cursor {
		t.Fatalf("after the reset: cursor %d, head %d", streamsSince(a.ReplicaDir), head())
	}
}

// TestInteropDataPlane: the relay half of the data plane end to end, against
// the panel's real code (Phase 8). MAIN's streams op mints node 7's tickets
// for stream 100 (relayed from MAIN, reading a file MAIN holds) into the
// record; the agent keeps them apart from it; its loopback proxy signs each
// upstream connect, which MAIN's RelayGuard admits once (a replay is
// refused), and reads the file through MAIN's FileTicketServer chunk by
// chunk, each checked against MAIN's digest, and refuses a tampered chunk.
func TestInteropDataPlane(t *testing.T) {
	a, runPHP, ctx := interopNode(t)
	a.ReplicaDir = filepath.Join(t.TempDir(), "replica")
	runPHP("events.php", "flows", fmt.Sprint(FlowStreams|16|FlowDataplane))
	size := cc.FileChunk + 1234
	path := runPHP("dataplane.php", "setup", fmt.Sprint(size))
	want, err := os.ReadFile(path)
	if err != nil || len(want) != size {
		t.Fatalf("the file MAIN holds: %v", err)
	}
	if _, err := a.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatalf("full pass: %v", err)
	}
	if a.tickets().relay(100) == "" {
		t.Fatal("no relay ticket for stream 100")
	}
	// The ref the node's PHP puts in its URL (DataPlane::ref) is the one MAIN minted.
	h := sha256.New()
	h.Write([]byte("xcvm-file-ref-v1"))
	h.Write(cc.U32(1))
	h.Write(cc.U32(uint32(len(path))))
	h.Write([]byte(path))
	ref := hex.EncodeToString(h.Sum(nil))[:32]
	if a.tickets().file(ref) == "" {
		t.Fatalf("no file ticket for ref %s", ref)
	}
	// The record's file keeps its ETag: the tickets live apart.
	rep, _ := os.ReadFile(streamFile(a, 100, ".rep"))
	if doc, err := a.openStream(rep, 100); err != nil || storedStreamEtag(a.ReplicaDir, 100) != doc.Etag {
		t.Fatalf("stream 100's record: %v", err)
	}

	var seen []http.Header
	var tamper atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/live":
			seen = append(seen, r.Header.Clone())
			if runPHP("dataplane.php", "relay", r.Header.Get("X-XCVM-Relay"), r.Header.Get("X-XCVM-Relay-Auth"), r.URL.RequestURI()) != "7" {
				http.NotFound(w, r)
				return
			}
			io.WriteString(w, "TS-FROM-MAIN")
		case "/xfile":
			var out struct {
				Status int    `json:"status"`
				Digest string `json:"digest"`
				Body   []byte `json:"body"`
			}
			json.Unmarshal([]byte(runPHP("dataplane.php", "xfile", r.Header.Get("X-XCVM-File"), r.Header.Get("X-XCVM-File-Auth"), r.URL.RequestURI(), r.URL.Query().Get("o"), r.URL.Query().Get("n"))), &out)
			if tamper.Load() && r.URL.Query().Get("o") != "0" && len(out.Body) > 0 {
				out.Body[0] ^= 1
			}
			w.Header().Set("X-XCVM-File-Digest", out.Digest)
			w.WriteHeader(out.Status)
			w.Write(out.Body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	host, port, _ := net.SplitHostPort(u.Host)
	p, _ := strconv.ParseInt(port, 10, 64)
	proxy := a.NewRelayProxy("interop-loopback-key-0123")
	proxy.servers = func() (*serverRoutes, error) {
		return &serverRoutes{self: 7, mainSid: 1, byID: map[int64]serverRoute{1: {ip: host, port: p}, 7: {ip: "127.0.0.2", port: 1}}, nodes: map[int64]routeNode{}}, nil
	}
	get := func(path string) (int, []byte) {
		rec := httptest.NewRecorder()
		func() {
			defer func() {
				if v := recover(); v != nil && v != http.ErrAbortHandler {
					panic(v)
				}
			}()
			proxy.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		}()
		return rec.Code, rec.Body.Bytes()
	}

	if code, body := get("/relay/interop-loopback-key-0123/100.ts"); code != 200 || string(body) != "TS-FROM-MAIN" {
		t.Fatalf("relay: %d %q", code, body)
	}
	if len(seen) != 1 {
		t.Fatalf("%d connects", len(seen))
	}
	// A replay of the headers a sniffer copied: MAIN has spent the nonce.
	if out := runPHP("dataplane.php", "relay", seen[0].Get("X-XCVM-Relay"), seen[0].Get("X-XCVM-Relay-Auth"), "/admin/live?stream=100&extension=ts"); out != "refused" {
		t.Fatalf("a replayed relay auth: %s", out)
	}
	if code, body := get("/xfile/interop-loopback-key-0123/" + ref + ".mkv"); code != 200 || !bytes.Equal(body, want) {
		t.Fatalf("xfile: %d, %d bytes", code, len(body))
	}
	tamper.Store(true)
	if _, body := get("/xfile/interop-loopback-key-0123/" + ref + ".mkv"); len(body) != cc.FileChunk {
		t.Fatalf("a tampered chunk: %d bytes passed on", len(body))
	}
}

// TestInteropNodeLeaseJudgesTheAgentsFile: the lease MAIN's real LeaseService
// signed, kept by the agent and anchored on MAIN's clock from a real reply, is
// judged by the node's real Core\Cluster\NodeLease from the file the agent
// writes — serving while MAIN's clock is short of the exp, draining past it and
// fenced past the drain, as the agent's clock carries MAIN's time forward with
// MAIN gone.
func TestInteropNodeLeaseJudgesTheAgentsFile(t *testing.T) {
	a, runPHP, ctx := interopNode(t)
	runPHP("events.php", "flows", "0") // mode 1: a cluster node
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	st := a.Client.State
	if st.Lease == nil {
		t.Fatalf("no lease: %q", st.LeaseRefused)
	}
	a.LeaseFile = filepath.Join(filepath.Dir(st.path), LeaseStateFile)
	verdict := func(fence, drainMin int) (v struct {
		State      string `json:"state"`
		Exp        int64  `json:"exp"`
		DrainUntil int64  `json:"drain_until"`
		Anchor     int64  `json:"anchor"`
		Gen        uint64 `json:"gen"`
		Why        string `json:"why"`
	}) {
		t.Helper()
		out := runPHP("lease.php", a.FlowsFile, a.LeaseFile, fmt.Sprint(fence), fmt.Sprint(drainMin))
		if err := json.Unmarshal([]byte(out), &v); err != nil {
			t.Fatalf("%s: %v", out, err)
		}
		return v
	}

	a.publishLease()
	v := verdict(1, 10)
	if v.State != "serving" || v.Why != "" || v.Exp != st.Lease.Exp || v.Gen != st.Lease.Gen {
		t.Fatalf("a live lease: %+v, held %+v", v, st.Lease)
	}
	if d := v.Anchor - time.Now().Unix(); d < -5 || d > 5 {
		t.Fatalf("PHP reckons MAIN's clock %d s off", d)
	}
	if v.DrainUntil != v.Exp+600 {
		t.Fatalf("drain until %d for exp %d", v.DrainUntil, v.Exp)
	}

	// MAIN gone, and the time passing on the agent's monotonic clock only: the
	// wall clock this machine keeps is not asked.
	pass := func(to int64) {
		t.Helper()
		a.Client.clock.mu.Lock()
		shift := (to - a.Client.clock.nowLocked(monotonicNs())) * int64(time.Millisecond)
		a.Client.clock.mono = func() int64 { return monotonicNs() + shift }
		a.Client.clock.mu.Unlock()
		a.publishLease()
	}
	pass(st.Lease.Exp*1000 + 5_000)
	if v := verdict(1, 10); v.State != "draining" {
		t.Fatalf("5 s past the exp: %+v", v)
	}
	if v := verdict(0, 10); v.State != "serving" || v.Why != "the switch is off" {
		t.Fatalf("switch off: %+v", v)
	}
	pass((st.Lease.Exp+600)*1000 + 5_000)
	if v := verdict(1, 10); v.State != "fenced" {
		t.Fatalf("past the drain: %+v", v)
	}
	// And a word from MAIN again puts its clock back where MAIN says it is.
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	a.publishLease()
	if v := verdict(1, 10); v.State != "serving" {
		t.Fatalf("MAIN heard again: %+v", v)
	}
}
