package clusteragent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// The whole sections (ADR 0004, Phase 7, fifth to seventh increments).

func etagOf(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

// wholeSection is a reply part for a whole section, as MAIN's ReplicaBuilder
// sends it.
func (m *replicaMain) wholeSection(t *testing.T, name, node, etag string, data any) map[string]any {
	return map[string]any{"etag": etag, "sealed": m.record(t, m.panel, "rep", map[string]any{
		"v": 1, "section": name, "node": node, "gen": 1, "etag": etag, "iat": time.Now().Unix(), "data": data,
	})}
}

func readJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWholeSectionsAreStoredByETagAndApplied(t *testing.T) {
	m, a := newReplicaMain(t)
	ctx := context.Background()
	applied := 0
	a.Apply = func(context.Context) error { applied++; return nil }
	ApplyDebounce = 0
	t.Cleanup(func() { ApplyDebounce = time.Second })
	data := map[string]any{
		"servers": map[string]any{"servers": []any{map[string]any{"id": 1}}, "nodes": []any{}},
		"node":    map[string]any{"id": 3, "http_broadcast_port": 80},
		"crontab": map[string]any{"jobs": []any{map[string]any{"filename": "cache", "time": "* * * * *"}}},
		"cluster": map[string]any{"policy_ver": 4, "main_urls": []any{"https://main/cluster/v1/"}},
	}
	parts := map[string]any{}
	for name, d := range data {
		parts[name] = m.wholeSection(t, name, m.uuid, etagOf(name), d)
	}
	m.whole = append(m.whole, parts)
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	if applied != 1 {
		t.Fatalf("cluster:apply ran %d times for one sync", applied)
	}
	st := LoadReplicaState(a.ReplicaDir)
	for name, d := range data {
		rep, _ := os.ReadFile(filepath.Join(a.ReplicaDir, name+".rep"))
		sealed, _ := base64.StdEncoding.DecodeString(parts[name].(map[string]any)["sealed"].(string))
		if !bytes.Equal(rep, sealed) {
			t.Fatalf("%s.rep is not the record as received", name)
		}
		doc := readJSON(t, filepath.Join(a.ReplicaDir, name+".json"))
		want, _ := json.Marshal(d)
		got, _ := json.Marshal(doc["data"])
		if doc["etag"] != etagOf(name) || string(got) != string(want) || st.WholeEtags[name] != etagOf(name) {
			t.Fatalf("%s.json %v, held %q", name, doc, st.WholeEtags[name])
		}
		if fi, _ := os.Stat(filepath.Join(a.ReplicaDir, name+".json")); fi.Mode().Perm() != 0o640 {
			t.Fatalf("%s.json mode %v", name, fi.Mode().Perm())
		}
	}
	// The next call names every section with the ETag held, "" for none.
	m.whole = append(m.whole, map[string]any{"servers": map[string]any{"unchanged": true}})
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	have := m.asked[len(m.asked)-1]["have"].(map[string]any)
	for _, name := range WholeSections {
		want := ""
		if _, ok := data[name]; ok {
			want = etagOf(name)
		}
		if have[name] != want {
			t.Fatalf("have[%s] = %v, want %q", name, have[name], want)
		}
	}
	// Unchanged, or left out ("not served"): kept, and nothing applied.
	if applied != 1 || LoadReplicaState(a.ReplicaDir).WholeEtags["crontab"] != etagOf("crontab") {
		t.Fatalf("applied %d after an unchanged reply", applied)
	}
	// A record for another node, another section, another ETag or another
	// generation of this node is refused and keeps what is held.
	bad := []map[string]any{
		{"node": m.wholeSection(t, "node", "11111111-1111-4111-8111-111111111111", etagOf("x"), map[string]any{})},
		{"node": m.wholeSection(t, "crontab", m.uuid, etagOf("x"), map[string]any{})},
		{"node": map[string]any{"etag": etagOf("y"), "sealed": m.wholeSection(t, "node", m.uuid, etagOf("x"), map[string]any{})["sealed"]}},
		{"node": map[string]any{"etag": etagOf("x"), "sealed": m.record(t, m.panel, "rep", map[string]any{"v": 1, "section": "node", "node": m.uuid, "gen": 2, "etag": etagOf("x"), "data": map[string]any{}})}},
	}
	for i, b := range bad {
		m.whole = append(m.whole, b)
		if err := a.SyncReplica(ctx); err == nil {
			t.Fatalf("bad section %d stored", i)
		}
		if LoadReplicaState(a.ReplicaDir).WholeEtags["node"] != etagOf("node") {
			t.Fatalf("bad section %d replaced the held ETag", i)
		}
	}
}

// secretsData is a secrets section as MAIN's ReplicaSections builds it.
var secretsData = map[string]any{
	"live_streaming_pass": map[string]any{"current": "S3cretStreamPass", "kid": "0123456789abcdef", "previous": nil, "previous_valid_until": nil},
	"openssl_extra":       map[string]any{"current": "OpensslExtraCurrentValue", "kid": "fedcba9876543210", "previous": "OpensslExtraPreviousValue", "previous_valid_until": 1900000000},
}

func secretsLeak(text string, sealed string, etag string) string {
	for _, s := range []string{"S3cretStreamPass", "OpensslExtraCurrentValue", "OpensslExtraPreviousValue", "0123456789abcdef", "fedcba9876543210", etag, sealed[:24]} {
		if strings.Contains(text, s) {
			return s
		}
	}
	return ""
}

func TestSecretsAreStoredPrivatelyAndNeverLogged(t *testing.T) {
	m, a := newReplicaMain(t)
	logs := &logSink{}
	a.Logf = logs.logf(t)
	ctx := context.Background()
	etag := etagOf("secrets")
	sec := m.wholeSection(t, "secrets", m.uuid, etag, secretsData)
	sealed := sec["sealed"].(string)
	var errs []string

	// Refused ones first: another node's, another ETag, another panel key.
	_, other, _ := ed25519.GenerateKey(nil)
	for _, bad := range []map[string]any{
		m.wholeSection(t, "secrets", "11111111-1111-4111-8111-111111111111", etag, secretsData),
		{"etag": etagOf("other"), "sealed": sealed},
		{"etag": etag, "sealed": m.record(t, other, "rep", map[string]any{"v": 1, "section": "secrets", "node": m.uuid, "gen": 1, "etag": etag, "data": secretsData})},
		{"etag": etag, "sealed": "%%%"},
	} {
		m.whole = append(m.whole, map[string]any{"secrets": bad})
		err := a.SyncReplica(ctx)
		if err == nil {
			t.Fatal("a bad secrets section was stored")
		}
		errs = append(errs, err.Error())
		if _, err := os.Stat(filepath.Join(a.ReplicaDir, "secrets.json")); err == nil {
			t.Fatal("a refused secrets section was written")
		}
	}
	m.whole = append(m.whole, map[string]any{"secrets": sec, "settings": m.wholeSection(t, "settings", m.uuid, etagOf("settings"), map[string]any{"server_name": "x"})})
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"secrets.rep", "secrets.json"} {
		fi, err := os.Stat(filepath.Join(a.ReplicaDir, f))
		if err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("%s: %v", f, err)
		}
	}
	doc := readJSON(t, filepath.Join(a.ReplicaDir, "secrets.json"))
	got, _ := json.Marshal(doc["data"])
	want, _ := json.Marshal(secretsData)
	if string(got) != string(want) || doc["etag"] != etag || LoadReplicaState(a.ReplicaDir).WholeEtags["secrets"] != etag {
		t.Fatalf("secrets.json %s", got)
	}
	// Named with its ETag from then on.
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	if m.asked[len(m.asked)-1]["have"].(map[string]any)["secrets"] != etag {
		t.Fatal("the secrets ETag is not named in have")
	}
	// A stored record that no longer verifies is named by its section only.
	st := a.Client.State
	st.NodeBoxSk, _, _ = cc.NewX25519()
	a.recheckReplica()

	logs.mu.Lock()
	all := strings.Join(logs.lines, "\n") + "\n" + strings.Join(errs, "\n")
	logs.mu.Unlock()
	if leak := secretsLeak(all, sealed, etag); leak != "" {
		t.Fatalf("logged %q:\n%s", leak, all)
	}
	if !strings.Contains(all, "secrets") {
		t.Fatalf("no error names the section:\n%s", all)
	}
}

func TestA503DBToConfigKeepsEverythingHeld(t *testing.T) {
	m, a := newReplicaMain(t)
	ctx := context.Background()
	applied := 0
	a.Apply = func(context.Context) error { applied++; return nil }
	ApplyDebounce = 0
	t.Cleanup(func() { ApplyDebounce = time.Second })
	m.next = append(m.next, m.section(t, m.uuid, 5, etagOf("bl")))
	m.whole = append(m.whole, map[string]any{
		"secrets":  m.wholeSection(t, "secrets", m.uuid, etagOf("s"), secretsData),
		"settings": m.wholeSection(t, "settings", m.uuid, etagOf("st"), map[string]any{"server_name": "x"}),
	})
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		var b strings.Builder
		for _, f := range []string{"state.json", "secrets.rep", "secrets.json", "settings.rep", "settings.json", "blocklist.rep", "blocklist.json"} {
			c, _ := os.ReadFile(filepath.Join(a.ReplicaDir, f))
			b.Write(c)
		}
		return b.String()
	}
	before, n := snapshot(), applied
	m.deny = "DB"
	err := a.SyncReplica(ctx)
	var d *Denial
	if !errors.As(err, &d) || d.Reason != "DB" {
		t.Fatalf("got %v", err)
	}
	if snapshot() != before || applied != n {
		t.Fatal("a 503 DB changed what the node holds, or applied")
	}
	if _, ok := busyWait(err); ok {
		t.Fatal("a 503 DB taken as busy: it waits for the next poll")
	}
}

func TestStoredRecordsThatNoLongerVerifyAreFetchedAgain(t *testing.T) {
	m, a := newReplicaMain(t)
	ctx := context.Background()
	m.next = append(m.next, m.section(t, m.uuid, 5, etagOf("bl")), m.delta(t, m.panel, 6))
	m.whole = append(m.whole, map[string]any{
		"settings": m.wholeSection(t, "settings", m.uuid, etagOf("st"), map[string]any{"server_name": "x"}),
		"servers":  m.wholeSection(t, "servers", m.uuid, etagOf("sv"), map[string]any{"servers": []any{}}),
	})
	for i := 0; i < 2; i++ {
		if err := a.SyncReplica(ctx); err != nil {
			t.Fatal(err)
		}
	}
	held := LoadReplicaState(a.ReplicaDir)
	if held.BlocklistSeq != 6 || held.SettingsEtag == "" || held.WholeEtags["servers"] == "" {
		t.Fatalf("held %+v", held)
	}
	// The keys they were stored under: nothing changes.
	stamp, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "state.json"))
	a.recheckReplica()
	if now, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "state.json")); string(now) != string(stamp) {
		t.Fatal("records that verify were reset")
	}
	cases := []struct {
		name   string
		change func(st *State)
	}{
		{"a new box key (re-enrolment)", func(st *State) { st.NodeBoxSk, _, _ = cc.NewX25519() }},
		{"a new panel key", func(st *State) {
			pub, _, _ := ed25519.GenerateKey(nil)
			st.PanelSignPub = pub
		}},
	}
	for _, c := range cases {
		os.WriteFile(filepath.Join(a.ReplicaDir, "state.json"), stamp, 0o640)
		st := a.Client.State
		keepSk, keepPub := st.NodeBoxSk, st.PanelSignPub
		c.change(st)
		a.recheckReplica()
		got := LoadReplicaState(a.ReplicaDir)
		if got.SettingsEtag != "" || got.WholeEtags["servers"] != "" || got.BlocklistEtag != "" || got.BlocklistSeq != 0 {
			t.Fatalf("%s: held %+v", c.name, got)
		}
		st.NodeBoxSk, st.PanelSignPub = keepSk, keepPub
	}
	// The next call asks for all of them again.
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	last := m.asked[len(m.asked)-1]
	have := last["have"].(map[string]any)
	if last["blocklist_since"] != float64(0) || have["blocklist"] != "" || have["settings"] != "" || have["servers"] != "" {
		t.Fatalf("asked %v", last)
	}
}

func TestConfigChangedSyncsAtOnceAndAcks(t *testing.T) {
	_, c := newReplayMain(t)
	a := &Agent{Client: c, Logf: t.Logf, ReplicaDir: t.TempDir(), Exec: func(context.Context, *Command, WireCommand) (bool, []byte) {
		t.Fatal("config.changed handed to cluster:exec")
		return false, nil
	}}
	if f := a.features(); strings.Join(f, ",") != "hls_reaper,config_changed" {
		t.Fatalf("features %v", f)
	}
	if f := (&Agent{Client: c, ReplicaDir: a.ReplicaDir}).features(); strings.Join(f, ",") != "hls_reaper" {
		t.Fatalf("features without commands %v", f)
	}
	run := a.localExec(a.Exec)
	for i := 0; i < 3; i++ { // coalesced: one sync pending
		ok, res := run(context.Background(), &Command{Type: "config.changed", Args: map[string]any{"sections": []any{"servers"}}}, WireCommand{})
		if !ok || string(res) != `{"result":true}` {
			t.Fatalf("acked %v %s", ok, res)
		}
	}
	if len(a.kickChans().sync) != 1 {
		t.Fatal("no sync started")
	}
}

func TestHelloSaysConfigChanged(t *testing.T) {
	m, c := newReplayMain(t)
	c.State.Enrolled = true
	a := &Agent{Client: c, Logf: t.Logf, ReplicaDir: t.TempDir(), Exec: func(context.Context, *Command, WireCommand) (bool, []byte) { return true, nil }}
	if _, err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	f, _ := json.Marshal(m.bodies["hello"][0]["features"])
	if string(f) != `["hls_reaper","config_changed"]` {
		t.Fatalf("hello features %s", f)
	}
}

func TestApplyExitCodes(t *testing.T) {
	dir := t.TempDir()
	script := func(code int, out string) string {
		p := filepath.Join(dir, "apply"+itoa(code)+".sh")
		os.WriteFile(p, []byte("echo '"+out+"'\nexit "+itoa(code)+"\n"), 0o700)
		return p
	}
	ctx := context.Background()
	if err := ApplyViaPHP("/bin/sh", script(0, "{}"), 10*time.Second)(ctx); err != nil {
		t.Fatalf("exit 0: %v", err)
	}
	if err := ApplyViaPHP("/bin/sh", script(2, "cluster:apply: no replica to apply"), 10*time.Second)(ctx); !errors.Is(err, ErrNothingToApply) {
		t.Fatalf("exit 2: %v", err)
	}
	err := ApplyViaPHP("/bin/sh", script(3, `{"secrets":{"mode":"failed"}}`), 10*time.Second)(ctx)
	if err == nil || !strings.Contains(err.Error(), `{"secrets":{"mode":"failed"}}`) {
		t.Fatalf("exit 3: %v", err)
	}
	// Nothing to apply is not logged; a failed part is, with its output.
	for _, c := range []struct {
		code   int
		logged bool
	}{{2, false}, {3, true}} {
		logs := &logSink{}
		a := &Agent{Logf: logs.logf(t), Apply: ApplyViaPHP("/bin/sh", script(c.code, `{"secrets":{"mode":"failed"}}`), 10*time.Second)}
		a.runApply(ctx)
		if logs.has("cluster: replica: apply") != c.logged {
			t.Fatalf("exit %d logged %v", c.code, logs.lines)
		}
	}
}

func TestApplyIsDebouncedAndFollowsTheConfigFlow(t *testing.T) {
	ApplyDebounce = 300 * time.Millisecond
	t.Cleanup(func() { ApplyDebounce = time.Second })
	var at []time.Time
	a := &Agent{Logf: t.Logf, ReplicaDir: t.TempDir(), Apply: func(context.Context) error { at = append(at, time.Now()); return nil }}
	a.runApply(context.Background())
	a.runApply(context.Background())
	if len(at) != 2 || at[1].Sub(at[0]) < 290*time.Millisecond {
		t.Fatalf("applies %v", at)
	}
	// The CONFIG bit changing, either way, asks for an apply; the first
	// flows seen and other bits do not.
	kicks := a.kickChans()
	for i, c := range []struct {
		flows int
		kick  bool
	}{{0, false}, {2, false}, {2 | FlowConfig, true}, {FlowConfig, false}, {0, true}} {
		a.publish(&Reply{State: "active", Mode: 1, Flows: c.flows})
		got := false
		select {
		case <-kicks.apply:
			got = true
		default:
		}
		if got != c.kick {
			t.Fatalf("step %d (flows %d): apply asked %v", i, c.flows, got)
		}
	}
}

func TestRunReplicaAppliesOnceAfterTheFirstSync(t *testing.T) {
	m, c := newReplayMain(t)
	m.answers["config"] = map[string]any{"blocklist": map[string]any{"seq": 0, "unchanged": true}}
	applied := make(chan struct{}, 4)
	a := &Agent{Client: c, Logf: t.Logf, ReplicaDir: t.TempDir(), Apply: func(context.Context) error { applied <- struct{}{}; return nil }}
	old := ReplicaPoll
	ReplicaPoll = time.Hour
	defer func() { ReplicaPoll = old }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.RunReplica(ctx); close(done) }()
	select {
	case <-applied:
	case <-time.After(5 * time.Second):
		t.Fatal("no apply after the first sync")
	}
	// config.changed: a sync at once.
	a.ConfigChanged()
	for deadline := time.Now().Add(5 * time.Second); m.count("config") < 2; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("config.changed started no sync")
		}
	}
	cancel()
	<-done
	if len(applied) != 0 {
		t.Fatal("applied again with nothing changed")
	}
}

func TestAgentJSONKeepsItsNamesAndEncodings(t *testing.T) {
	_, st := newFake(t)
	st.NodeBoxSk, _, _ = cc.NewX25519()
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(st.path)
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("agent.json mode %v", fi.Mode().Perm())
	}
	doc := readJSON(t, st.path)
	if doc["node_uuid"] != st.NodeUUID || strings.ToLower(st.NodeUUID) != st.NodeUUID {
		t.Fatalf("node_uuid %v", doc["node_uuid"])
	}
	for key, want := range map[string][]byte{"node_box_sk": st.NodeBoxSk, "panel_sign_pub": st.PanelSignPub} {
		s, _ := doc[key].(string)
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil || !bytes.Equal(b, want) || len(b) != 32 || !strings.HasSuffix(s, "=") {
			t.Fatalf("%s = %q", key, s)
		}
	}
}
