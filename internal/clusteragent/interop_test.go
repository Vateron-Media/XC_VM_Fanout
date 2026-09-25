package clusteragent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// TestInteropWithPanel runs this agent against MAIN's real PHP ClusterApi
// (served by `php -S` from a panel checkout, with the panel's test fake of
// xcvm_core and a SQLite file): enrolment, hello, heartbeat, and a token
// refresh including a retried one. Opt-in:
//
//	XCVM_PANEL_DIR=/path/to/XC_VM go test ./internal/clusteragent -run Interop -v
func TestInteropWithPanel(t *testing.T) {
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

	// The install flow, as LbInstallFlow::provisionCluster runs it over SSH:
	// keygen on the node, epoch 1 minted by MAIN, probe, install.
	uuid := "3b0c1d2e-4f5a-4b6c-8d7e-9f0a1b2c3d4e"
	statePath := filepath.Join(dir, "state.json")
	keys, err := Keygen(statePath, uuid)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(php, filepath.Join(harness, "enrol.php"), uuid, keys.SignPub, keys.BoxPub, keys.EphPub)
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("enrol.php: %v\n%s", err, out)
	}
	var first struct {
		TokenSealed  string `json:"token_sealed"`
		PanelSignPub string `json:"panel_sign_pub"`
		Cluster      struct {
			ServerID int64 `json:"server_id"`
			Policy   struct {
				PolicyVer int      `json:"policy_ver"`
				MainURLs  []string `json:"main_urls"`
			} `json:"policy"`
		} `json:"cluster"`
	}
	if err := json.Unmarshal(out, &first); err != nil {
		t.Fatalf("enrol output: %v\n%s", err, out)
	}
	sealed, _ := base64.StdEncoding.DecodeString(first.TokenSealed)
	panelPub, _ := base64.StdEncoding.DecodeString(first.PanelSignPub)

	srv := exec.Command(php, "-S", fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(harness, "router.php"))
	srv.Env = env
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Process.Kill()
	waitPort(t, port)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := Probe(ctx, panelPub, first.Cluster.Policy.MainURLs); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if _, err := Probe(ctx, make([]byte, 32), first.Cluster.Policy.MainURLs); err == nil {
		t.Fatal("probe accepted a health document under the wrong panel key")
	}
	if err := Install(statePath, InstallData{ServerID: first.Cluster.ServerID, PanelSignPub: panelPub, MainURLs: first.Cluster.Policy.MainURLs, PolicyVer: first.Cluster.Policy.PolicyVer, Epoch: 1, TokenSealed: sealed}); err != nil {
		t.Fatalf("install: %v", err)
	}
	st := &State{MainURLs: first.Cluster.Policy.MainURLs}

	loaded, err := LoadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(loaded, "xc_agent/interop")
	if _, ok := c.Current(); !ok {
		t.Fatal("epoch 1 did not open with the agent's key")
	}
	if _, err := c.Health(ctx, st.MainURLs[0]); err != nil {
		t.Fatalf("health: %v", err)
	}
	a := &Agent{Client: c, Version: "0.0.0-interop", Logf: t.Logf}
	r, err := a.Start(ctx)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if r.State != "active" || !loaded.Enrolled {
		t.Fatalf("after start: %+v enrolled=%v", r, loaded.Enrolled)
	}
	var hb Reply
	if err := c.Call(ctx, "heartbeat", map[string]any{"telemetry": map[string]int{"cpu": 1}}, &hb, false); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	// A refresh whose reply is lost: the agent had persisted the key first, so
	// its retry presents the same key and gets the very same token back.
	lostSk, lostPub, _ := cc.NewX25519()
	s1, _ := c.current()
	var lost struct {
		TokenSealed string `json:"token_sealed"`
		Epoch       uint64 `json:"epoch"`
	}
	if err := c.call(ctx, s1, "token_refresh", map[string]string{"eph_pub": base64.StdEncoding.EncodeToString(lostPub)}, &lost, true); err != nil || lost.Epoch != 2 {
		t.Fatalf("first refresh: %v %+v", err, lost)
	}
	loaded.PendingEphSk = lostSk
	tok2, err := c.Refresh(ctx)
	if err != nil {
		t.Fatalf("retried refresh: %v", err)
	}
	if tok2.Epoch != 2 || loaded.Epochs[0].Epoch != 2 || base64.StdEncoding.EncodeToString(loaded.Epochs[0].TokenSealed) != lost.TokenSealed {
		t.Fatalf("the retry did not return the same token: epoch %d", tok2.Epoch)
	}
	if loaded.PendingEphSk != nil {
		t.Fatal("pending key kept after success")
	}
	if err := c.Call(ctx, "heartbeat", map[string]any{}, &hb, false); err != nil {
		t.Fatalf("heartbeat on epoch 2: %v", err)
	}
	tok3, err := c.Refresh(ctx)
	if err != nil || tok3.Epoch != 3 {
		t.Fatalf("second refresh: %v %+v", err, tok3)
	}

	// A request under an unknown epoch gets a panel-signed refusal about it.
	s, _ := c.current()
	s.epoch = 99
	err = c.call(ctx, s, "heartbeat", map[string]any{}, nil, false)
	var d *Denial
	if !errors.As(err, &d) || d.Reason != "TOKEN_EXPIRED" {
		t.Fatalf("want a signed TOKEN_EXPIRED, got %v", err)
	}
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitPort(t *testing.T, port int) {
	for i := 0; i < 100; i++ {
		if res, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", port)); err == nil {
			res.Body.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("php -S did not start")
}
