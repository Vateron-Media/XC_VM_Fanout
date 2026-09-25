package clusteragent

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
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

func TestSocketPassesOnlyAllowedOpsToMain(t *testing.T) {
	f, st := newFake(t)
	var gotPath string
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, _ []byte) {
		gotPath = r.URL.Path
		ts := uint64(time.Now().UnixMilli())
		rn := make([]byte, 16)
		resCtx, _ := cc.ResponseContext(reqCtx, 200, octet, ts, rn)
		body, _ := cc.Box(f.keys.EncDown, resCtx, []byte(`{"stream_id":42,"main_time_ms":`+strconv.FormatUint(ts, 10)+`}`))
		w.Header().Set("Content-Type", octet)
		w.Header().Set(cc.HTs, strconv.FormatUint(ts, 10))
		w.Header().Set(cc.HNonce, hex.EncodeToString(rn))
		w.Header().Set(cc.HSig, hex.EncodeToString(cc.MAC(f.keys.MacDown, resCtx, body)))
		w.Write(body)
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
	sock := filepath.Join(t.TempDir(), "agent.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.ServeSocket(ctx, sock)
	hc := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	var err error
	for i := 0; i < 50; i++ {
		if _, err = net.Dial("unix", sock); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	post := func(path, body string) (int, string) {
		res, err := hc.Post("http://agent"+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	if code, body := post("/v1/main/recording_complete", `{"recording_id":1}`); code != 200 || !strings.Contains(body, `"stream_id":42`) || gotPath != "/cluster/v1/recording_complete" {
		t.Fatalf("allowed op: %d %s (MAIN saw %s)", code, body, gotPath)
	}
	for _, path := range []string{"/v1/main/token_refresh", "/v1/main/heartbeat", "/v1/other"} {
		if code, _ := post(path, `{}`); code != 403 {
			t.Errorf("%s: %d, want 403", path, code)
		}
	}
	if code, _ := post("/v1/main/recording_complete", `not json`); code != 400 {
		t.Errorf("bad body: %d", code)
	}
}

// TestInteropContent finishes a recording as RecordCommand does on a node with
// CONTENT and STREAMS on, against the panel's real PHP: the VOD id comes from
// MAIN through this agent's socket (PHP's AgentClient), then the conversion's
// end and an archive worker's pid go as events. Opt-in, as the other interop
// tests.
func TestInteropContent(t *testing.T) {
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
	// A short socket path: unix sockets are limited to ~108 bytes.
	sockDir, _ := os.MkdirTemp("", "xa")
	defer os.RemoveAll(sockDir)
	a := &Agent{Client: NewClient(st, "xc_agent/interop"), Version: "0.0.0-interop", Logf: t.Logf,
		FlowsFile: filepath.Join(dir, "flows.json"), SpoolDir: filepath.Join(dir, "spool"), SocketPath: filepath.Join(sockDir, "a.sock")}
	runPHP("events.php", "flows", "24") // STREAMS | CONTENT
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	oldLanes := Lanes
	Lanes = []Lane{{Name: "p0", Interval: 50 * time.Millisecond}}
	defer func() { Lanes = oldLanes }()
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	go a.ServeSocket(lctx, a.SocketPath)
	go a.RunEvents(lctx, Lanes[0])
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(a.SocketPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	var ids []int
	json.Unmarshal([]byte(runPHP("content.php", "record", a.FlowsFile, a.SpoolDir, a.SocketPath)), &ids)
	if len(ids) != 2 || ids[0] <= 100 || ids[0] != ids[1] {
		t.Fatalf("VOD ids through the socket: %v", ids)
	}
	var got struct {
		Status     int `json:"status"`
		CreatedID  int `json:"created_id"`
		Attached   int `json:"attached"`
		ArchivePid int `json:"archive_pid"`
	}
	for i := 0; i < 100; i++ {
		json.Unmarshal([]byte(runPHP("content.php", "state")), &got)
		if got.Status == 2 && got.ArchivePid == 4242 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got.Status != 2 || got.CreatedID != ids[0] || got.Attached != 1 || got.ArchivePid != 4242 {
		t.Fatalf("MAIN holds %+v (VOD %d)", got, ids[0])
	}
}
