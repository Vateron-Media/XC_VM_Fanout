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
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// TestInteropWithPanel runs this agent against MAIN's real PHP ClusterApi
// (served by `php -S` from a panel checkout, with the panel's test fake of
// xcvm_core and a SQLite file): enrolment, hello, heartbeat, and a token
// refresh including a retried one, and a re-key after every token expired;
// the policy version it dials and its audit, as MAIN records them; the
// replica's blocklist and whole sections, secrets included. Opt-in:
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
	// A policy version other than 0, so MAIN recording it proves the agent said it.
	dbPath := filepath.Join(dir, "main.sqlite")
	env := append(os.Environ(), "XCVM_PANEL_DIR="+panel, "XCVM_INTEROP_DB="+dbPath, fmt.Sprintf("XCVM_INTEROP_PORT=%d", port), "XCVM_INTEROP_POLICY_VER=5")
	// What MAIN keeps for the node: the policy it dials, the port, the audit.
	mainNode := func() (row struct {
		PolicyVer json.Number `json:"policy_ver"`
		MainPort  json.Number `json:"main_port"`
		Audit     *string     `json:"audit"`
		Features  *string     `json:"features"`
		// RelayDownSince is nil while the relay proxy holds its port.
		RelayDownSince *json.Number `json:"relay_down_since"`
		RelayError     *string      `json:"relay_error"`
		DigestN1       *string      `json:"digest_n1"`
	}) {
		t.Helper()
		cmd := exec.Command(php, filepath.Join(harness, "node.php"))
		cmd.Env = env
		out, err := cmd.Output()
		if err != nil || json.Unmarshal(out, &row) != nil {
			t.Fatalf("node.php: %v\n%s", err, out)
		}
		return row
	}

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
		TokenSealed  string          `json:"token_sealed"`
		PanelSignPub string          `json:"panel_sign_pub"`
		Lease        json.RawMessage `json:"lease"`
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
	dumpRouterLog(t, dbPath)
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
	if err := Install(statePath, InstallData{ServerID: first.Cluster.ServerID, PanelSignPub: panelPub, MainURLs: first.Cluster.Policy.MainURLs, PolicyVer: first.Cluster.Policy.PolicyVer, Epoch: 1, TokenSealed: sealed, Lease: first.Lease}); err != nil {
		t.Fatalf("install: %v", err)
	}
	// The lease MAIN's own LeaseService signed, verified here against the panel
	// key: the one place the two languages meet over these bytes.
	installed, err := loadRaw(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if installed.Lease == nil {
		t.Fatalf("the install data's lease was not kept: %s", installed.LeaseRefused)
	}
	if installed.Lease.ServerID != first.Cluster.ServerID || installed.Lease.Gen == 0 || installed.LeaseRefused != "" {
		t.Fatalf("lease from MAIN: %+v refused=%q", installed.Lease, installed.LeaseRefused)
	}
	firstLeaseIat := installed.Lease.Iat
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
	// Hello said which policy the agent dials, and MAIN recorded it with the
	// port it was reached on.
	if first.Cluster.Policy.PolicyVer != 5 {
		t.Fatalf("installed policy %d", first.Cluster.Policy.PolicyVer)
	}
	if row := mainNode(); row.PolicyVer.String() != "5" || row.MainPort.String() != fmt.Sprint(port) {
		t.Fatalf("MAIN recorded policy_ver %s, main_port %s", row.PolicyVer, row.MainPort)
	}
	// The node's audit.json rides the heartbeat and MAIN keeps it.
	audit := `{"settings_misses":{"allowed_ips_admin":3},"sql_connects":2,"redis_connects":1,"sites":{"sql src/Core/Database/Database.php:120":2},"connects_since":1790000000}`
	if err := os.WriteFile(filepath.Join(dir, "audit.json"), []byte(audit), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatalf("heartbeat with audit: %v", err)
	}
	if row := mainNode(); row.Audit == nil || !strings.Contains(*row.Audit, `"allowed_ips_admin":3`) || !strings.Contains(*row.Audit, `"sql_connects":2`) || !strings.Contains(*row.Audit, `"connects_since":1790000000`) {
		t.Fatalf("MAIN kept audit %v", row.Audit)
	}
	// A relay proxy that cannot bind its port: MAIN keeps it with the error,
	// then clears it once the port is the agent's.
	a.RelayAddr, a.RelayKeyDir = RelayProxyAddr, t.TempDir()
	a.relayBind.down(time.Now().Add(-time.Minute), 3, errors.New("listen tcp 127.0.0.1:31290: bind: address already in use"))
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatalf("heartbeat with relay down: %v", err)
	}
	if row := mainNode(); row.RelayDownSince == nil || row.RelayError == nil || !strings.Contains(*row.RelayError, "address already in use") {
		t.Fatalf("MAIN kept relay_down_since %v, relay_error %v", row.RelayDownSince, row.RelayError)
	} else if since, _ := row.RelayDownSince.Int64(); since > time.Now().Unix()-50 || since < time.Now().Unix()-70 {
		t.Fatalf("relay_down_since %d, want about a minute ago", since)
	}
	a.relayBind.up()
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatalf("heartbeat with relay bound: %v", err)
	}
	if row := mainNode(); row.RelayDownSince != nil || row.RelayError != nil {
		t.Fatalf("MAIN still keeps relay_down_since %v, relay_error %v", row.RelayDownSince, row.RelayError)
	}
	// The owners whose chunk digest named no request: none yet, then one.
	if row := mainNode(); row.DigestN1 == nil || *row.DigestN1 != "[]" {
		t.Fatalf("MAIN kept digest_n1 %v, want []", row.DigestN1)
	}
	a.takeDigestN1(9)
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatalf("heartbeat with an N-1 owner: %v", err)
	}
	if row := mainNode(); row.DigestN1 == nil || *row.DigestN1 != "[9]" {
		t.Fatalf("MAIN kept digest_n1 %v, want [9]", row.DigestN1)
	}
	a.RelayAddr, a.RelayKeyDir = "", ""
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
	// Both refreshes carried a lease of MAIN's, the retried one included; the
	// node holds the last it was sent, never an older copy.
	if loaded.Lease == nil || loaded.LeaseRefused != "" {
		t.Fatalf("no lease after the refreshes: refused=%q", loaded.LeaseRefused)
	}
	if loaded.Lease.Iat < firstLeaseIat {
		t.Fatalf("the refresh replaced the lease with an older one: %d < %d", loaded.Lease.Iat, firstLeaseIat)
	}

	var d *Denial
	// Every token expires (an outage past exp): the heartbeat is refused, and
	// the agent re-keys with a challenge. This node was installed without the
	// panel box key, as nodes enrolled before re-key existed were, so it takes
	// the key from the signed health document first.
	expire := exec.Command(php, filepath.Join(harness, "expire.php"))
	expire.Env = env
	if out, err := expire.CombinedOutput(); err != nil {
		t.Fatalf("expire.php: %v\n%s", err, out)
	}
	err = c.Call(ctx, "heartbeat", map[string]any{}, &hb, false)
	if !needsRekey(err) {
		t.Fatalf("want a re-key signal after expiry, got %v", err)
	}
	if len(loaded.PanelBoxPub) != 0 {
		t.Fatal("the box key should not be known yet")
	}
	// Epoch 3 was minted but never used, so MAIN numbers from the node's
	// current epoch (2); the old epoch 3 row is gone with its z.
	tok4, err := c.Rekey(ctx, a.identity())
	if err != nil {
		t.Fatalf("rekey: %v", err)
	}
	if tok4.Epoch != 3 || len(loaded.Epochs) != 1 || loaded.Epochs[0].Epoch != 3 || len(loaded.PanelBoxPub) != 32 {
		t.Fatalf("after rekey: epoch %d, %d epochs held, box key %d bytes", tok4.Epoch, len(loaded.Epochs), len(loaded.PanelBoxPub))
	}
	if err := c.Call(ctx, "heartbeat", map[string]any{}, &hb, false); err != nil {
		t.Fatalf("heartbeat after rekey: %v", err)
	}
	reloaded, err := LoadState(statePath)
	if err != nil || len(reloaded.Epochs) != 1 || reloaded.Epochs[0].Epoch != 3 {
		t.Fatalf("the re-keyed epoch was not persisted: %v", err)
	}
	// The replica (config op): the whole blocklist once, then deltas, each
	// stored only after it opens for this node and verifies.
	if _, err := os.Stat(filepath.Join(panel, "src/Domain/Cluster/ReplicaBuilder.php")); err == nil {
		block := func(args ...string) {
			cmd := exec.Command(php, append([]string{filepath.Join(harness, "block.php")}, args...)...)
			cmd.Env = env
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("block.php: %v\n%s", err, out)
			}
		}
		a.ReplicaDir = filepath.Join(dir, "replica")
		block("203.0.113.1")
		if err := a.SyncReplica(ctx); err != nil {
			t.Fatalf("replica: %v", err)
		}
		sealedRep, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "blocklist.rep"))
		payload, err := c.OpenRecord(sealedRep, "rep")
		if err != nil || !strings.Contains(string(payload), `"ip":["203.0.113.1"]`) {
			t.Fatalf("stored section: %v %s", err, payload)
		}
		block("203.0.113.2")
		block("203.0.113.1", "del")
		if err := a.SyncReplica(ctx); err != nil {
			t.Fatalf("replica delta: %v", err)
		}
		deltas := ReplicaDeltas(a.ReplicaDir)
		if len(deltas) != 1 {
			t.Fatalf("deltas %v", deltas)
		}
		sealedBlk, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "blocklist.d", deltas[0]))
		payload, err = c.OpenRecord(sealedBlk, "blk")
		if err != nil || !strings.Contains(string(payload), `"add":["203.0.113.2"],"remove":["203.0.113.1"]`) {
			t.Fatalf("stored delta: %v %s", err, payload)
		}
		if st := LoadReplicaState(a.ReplicaDir); st.BlocklistSeq != 3 || len(st.BlocklistEtag) != 64 {
			t.Fatalf("replica state %+v", st)
		}
		// The settings section came with the first sync: allowlisted keys only.
		if b, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "settings.json")); !strings.Contains(string(b), `"server_name":"Interop"`) || strings.Contains(string(b), "secret") {
			t.Fatalf("settings.json %s", b)
		}
		// Every whole section came too, each stored as MAIN signed it for
		// this node: secrets private to xc_vm, with MAIN's values.
		held := LoadReplicaState(a.ReplicaDir)
		// A panel before the twelfth Phase 7 increment serves neither
		// catalogue section; one with it must serve both.
		_, catalogue := os.Stat(filepath.Join(panel, "src/Core/Cluster/ReplicaStreamCache.php"))
		for _, name := range WholeSections {
			if (name == "bouquets" || name == "categories") && catalogue != nil {
				continue
			}
			rep, err := os.ReadFile(filepath.Join(a.ReplicaDir, name+".rep"))
			if err != nil {
				t.Fatalf("%s.rep: %v", name, err)
			}
			doc, err := a.openWhole(rep, name, held.etag(name))
			if err != nil || len(held.etag(name)) != 64 {
				t.Fatalf("%s: %v (ETag %q)", name, err, held.etag(name))
			}
			stored := readJSON(t, filepath.Join(a.ReplicaDir, name+".json"))
			want, _ := json.Marshal(json.RawMessage(doc.Data))
			got, _ := json.Marshal(stored["data"])
			var w, g any
			json.Unmarshal(want, &w)
			json.Unmarshal(got, &g)
			if fmt.Sprint(w) != fmt.Sprint(g) || stored["etag"] != held.etag(name) {
				t.Fatalf("%s.json is not the signed data", name)
			}
		}
		for _, f := range []string{"secrets.rep", "secrets.json"} {
			if fi, err := os.Stat(filepath.Join(a.ReplicaDir, f)); err != nil || fi.Mode().Perm() != 0o600 {
				t.Fatalf("%s: %v", f, err)
			}
		}
		secrets := readJSON(t, filepath.Join(a.ReplicaDir, "secrets.json"))["data"].(map[string]any)
		if secrets["live_streaming_pass"].(map[string]any)["current"] != "InteropStreamPass" || secrets["openssl_extra"].(map[string]any)["current"] != "test-openssl-extra" {
			t.Fatalf("secrets %v", secrets)
		}
		if b, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "crontab.json")); !strings.Contains(string(b), `"filename":"cache"`) || !strings.Contains(string(b), `"filename":"users"`) || strings.Contains(string(b), `"epg"`) {
			t.Fatalf("crontab.json %s", b)
		}
		if b, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "cluster.json")); !strings.Contains(string(b), `"policy_ver":5`) {
			t.Fatalf("cluster.json %s", b)
		}
		// Named with their ETags, MAIN answers them unchanged.
		if err := a.SyncReplica(ctx); err != nil {
			t.Fatalf("replica again: %v", err)
		}
		if again := LoadReplicaState(a.ReplicaDir); fmt.Sprint(again.WholeEtags) != fmt.Sprint(held.WholeEtags) || again.SettingsEtag != held.SettingsEtag {
			t.Fatal("the held ETags moved on an unchanged reply")
		}
		// A record sealed to this node but under another tag does not verify.
		if _, err := c.OpenRecord(sealedBlk, "rep"); err == nil {
			t.Fatal("a blk record verified as rep")
		}
		// Bouquets too large for one reply come in parts, from a panel that stages them.
		if src, _ := os.ReadFile(filepath.Join(panel, "src/Domain/Cluster/ReplicaBuilder.php")); strings.Contains(string(src), "PART_BYTES") {
			cmd := exec.Command(php, filepath.Join(harness, "bouquet.php"))
			cmd.Env = env
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("bouquet.php: %v\n%s", err, out)
			}
			if err := a.SyncReplica(ctx); err != nil {
				t.Fatalf("replica in parts: %v", err)
			}
			now := LoadReplicaState(a.ReplicaDir)
			after := now.etag("bouquets")
			b, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "bouquets.json"))
			if after == held.etag("bouquets") || len(b) < 4<<20 || !strings.Contains(string(b), `"bouquet_name":"Everything"`) || !strings.Contains(string(b), `"etag":"`+after+`"`) {
				t.Fatalf("bouquets in parts: ETag %s (was %s), %d bytes", after, held.etag("bouquets"), len(b))
			}
			if left, _ := filepath.Glob(filepath.Join(dir, "xfer", "*")); len(left) != 0 {
				t.Fatalf("MAIN kept its stage after the last part: %v", left)
			}
		}
	}

	// A second attempt within the minute is refused, signed and about this request.
	_, err = c.Rekey(ctx, a.identity())
	if !errors.As(err, &d) || d.Reason != "RATE_LIMITED" || retryAfterMs(d) <= 0 {
		t.Fatalf("want a signed RATE_LIMITED, got %v", err)
	}

	// A request under an unknown epoch gets a panel-signed refusal about it.
	s, _ := c.current()
	s.epoch = 99
	err = c.call(ctx, s, "heartbeat", map[string]any{}, nil, false)
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

// dumpRouterLog prints the harness router's log (its requests, and the text of
// any throwable MAIN's code raised) when the test fails. Without it a fatal in
// the panel reads as a bare "HTTP 500" on the agent's side: php -S does not pass
// the router script's STDERR through.
func dumpRouterLog(t *testing.T, dbPath string) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		if b, err := os.ReadFile(dbPath + ".log"); err == nil && len(b) > 0 {
			t.Logf("MAIN harness log:\n%s", b)
		}
	})
}
