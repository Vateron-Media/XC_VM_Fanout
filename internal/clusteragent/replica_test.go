package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// replicaMain answers config with whatever the test queued next.
type replicaMain struct {
	*fakeMain
	boxPub   []byte
	next     []map[string]any
	settings []map[string]any // sent with the next replies, one each
	asked    []map[string]any
}

func newReplicaMain(t *testing.T) (*replicaMain, *Agent) {
	f, st := newFake(t)
	sk, pub, _ := cc.NewX25519()
	st.NodeBoxSk = sk
	m := &replicaMain{fakeMain: f, boxPub: pub}
	f.answer = m.answer
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	return m, &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf, ReplicaDir: t.TempDir()}
}

func (m *replicaMain) answer(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
	body, _ := io.ReadAll(r.Body)
	plain, _ := cc.Unbox(m.keys.EncUp, reqCtx, body)
	var req map[string]any
	json.Unmarshal(plain, &req)
	m.asked = append(m.asked, req)
	out := map[string]any{"blocklist": map[string]any{"seq": 0, "more": false}}
	if len(m.next) > 0 {
		out = map[string]any{"blocklist": m.next[0]}
		m.next = m.next[1:]
	}
	if len(m.settings) > 0 {
		out["settings"] = m.settings[0]
		m.settings = m.settings[1:]
	}
	ts := uint64(time.Now().UnixMilli())
	rn := make([]byte, 16)
	resCtx, _ := cc.ResponseContext(reqCtx, 200, octet, ts, rn)
	b, _ := json.Marshal(out)
	sealed, _ := cc.Box(m.keys.EncDown, resCtx, b)
	w.Header().Set("Content-Type", octet)
	w.Header().Set(cc.HTs, strconv.FormatUint(ts, 10))
	w.Header().Set(cc.HNonce, hex.EncodeToString(rn))
	w.Header().Set(cc.HSig, hex.EncodeToString(cc.MAC(m.keys.MacDown, resCtx, sealed)))
	w.Write(sealed)
}

// record signs and seals a replica record as MAIN's ReplicaBuilder does.
func (m *replicaMain) record(t *testing.T, key ed25519.PrivateKey, tag string, doc map[string]any) string {
	payload, _ := json.Marshal(doc)
	body := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	body = append(append(body, payload...), ed25519.Sign(key, cc.PanelSigInput(tag, payload))...)
	sealed, err := cc.Seal(m.boxPub, "replica", m.uuid, body)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(sealed)
}

func (m *replicaMain) section(t *testing.T, node string, seq int64, etag string) map[string]any {
	return map[string]any{"seq": seq, "section": map[string]any{"etag": etag, "sealed": m.record(t, m.panel, "rep", map[string]any{
		"v": 1, "section": "blocklist", "node": node, "gen": 1, "etag": etag, "seq": seq, "data": map[string]any{"ip": []string{"203.0.113.1"}},
	})}}
}

func (m *replicaMain) delta(t *testing.T, key ed25519.PrivateKey, seq int64) map[string]any {
	return map[string]any{"seq": seq, "delta": m.record(t, key, "blk", map[string]any{"v": 1, "seq": seq, "add": []string{"203.0.113.2"}, "remove": []string{}})}
}

func TestReplicaStoresOnlyWhatOpensAndVerifies(t *testing.T) {
	m, a := newReplicaMain(t)
	ctx := context.Background()
	applied := 0
	a.Apply = func(context.Context) error { applied++; return nil }
	etag := hex.EncodeToString(make([]byte, 32))

	// Another node's section is refused, and nothing is kept.
	m.next = append(m.next, m.section(t, "11111111-1111-4111-8111-111111111111", 5, etag))
	if err := a.SyncReplica(ctx); err == nil {
		t.Fatal("stored another node's section")
	}
	if _, err := os.Stat(filepath.Join(a.ReplicaDir, "blocklist.rep")); err == nil {
		t.Fatal("a refused section was written")
	}

	m.next = append(m.next, m.section(t, m.uuid, 5, etag))
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	if st := LoadReplicaState(a.ReplicaDir); st.BlocklistSeq != 5 || st.BlocklistEtag != etag {
		t.Fatalf("state %+v", st)
	}

	// A delta signed by someone else, or not the one announced, is refused.
	_, other, _ := ed25519.GenerateKey(nil)
	bad := m.delta(t, other, 6)
	wrong := m.delta(t, m.panel, 7)
	wrong["seq"] = 8
	m.next = append(m.next, bad, wrong, m.delta(t, m.panel, 6))
	for i := 0; i < 2; i++ {
		if err := a.SyncReplica(ctx); err == nil {
			t.Fatalf("refused delta %d was accepted", i)
		}
	}
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	if d := ReplicaDeltas(a.ReplicaDir); len(d) != 1 || LoadReplicaState(a.ReplicaDir).BlocklistSeq != 6 {
		t.Fatalf("deltas %v", d)
	}
	if got := m.asked[len(m.asked)-1]["blocklist_since"]; got != float64(5) {
		t.Fatalf("asked since %v", got)
	}
	// The section with its delta applied, for PHP, and cluster:apply run.
	var mat struct {
		Seq  int64 `json:"seq"`
		Etag string
		Data struct {
			IP []string `json:"ip"`
		} `json:"data"`
	}
	b, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "blocklist.json"))
	if err := json.Unmarshal(b, &mat); err != nil || mat.Seq != 6 || mat.Etag != etag || strings.Join(mat.Data.IP, ",") != "203.0.113.1,203.0.113.2" || applied != 2 {
		t.Fatalf("materialised %s (applied %d)", b, applied)
	}

	// A new section replaces the deltas; one the node holds is not sent again.
	m.next = append(m.next, m.section(t, m.uuid, 9, etag), map[string]any{"seq": 10, "unchanged": true})
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	if d := ReplicaDeltas(a.ReplicaDir); len(d) != 0 {
		t.Fatalf("deltas kept past a section: %v", d)
	}
	ReplicaFullEvery = 0 // the daily reload: since 0, with the ETag held
	t.Cleanup(func() { ReplicaFullEvery = 24 * time.Hour })
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	last := m.asked[len(m.asked)-1]
	if last["blocklist_since"] != float64(0) || last["have"].(map[string]any)["blocklist"] != etag || LoadReplicaState(a.ReplicaDir).BlocklistSeq != 10 {
		t.Fatalf("daily reload asked %v", last)
	}
}

func TestReplicaKeepsTheSettingsSectionForPHP(t *testing.T) {
	m, a := newReplicaMain(t)
	ctx := context.Background()
	applied := 0
	a.Apply = func(context.Context) error { applied++; return nil }
	etag := hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	section := func(node string) map[string]any {
		return map[string]any{"etag": etag, "sealed": m.record(t, m.panel, "rep", map[string]any{
			"v": 1, "section": "settings", "node": node, "gen": 1, "etag": etag, "data": map[string]any{"seg_time": "6"},
		})}
	}

	m.settings = append(m.settings, section("11111111-1111-4111-8111-111111111111"))
	if err := a.SyncReplica(ctx); err == nil {
		t.Fatal("kept another node's settings")
	}
	m.settings = append(m.settings, section(m.uuid))
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "settings.json"))
	if string(b) != `{"data":{"seg_time":"6"},"etag":"`+etag+`"}` || applied != 1 || LoadReplicaState(a.ReplicaDir).SettingsEtag != etag {
		t.Fatalf("settings.json %s (applied %d)", b, applied)
	}
	// The next request names the ETag held; unchanged applies nothing.
	m.settings = append(m.settings, map[string]any{"unchanged": true})
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	if have := m.asked[len(m.asked)-1]["have"].(map[string]any)["settings"]; have != etag || applied != 1 {
		t.Fatalf("asked with %v, applied %d", have, applied)
	}
}
