package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// The digest vector PHP's ConnectionDigestTest pins too.
func TestDigestMatchesThePanels(t *testing.T) {
	conns := map[string]map[string]any{
		"a": {"user_id": float64(7), "hls_end": float64(0)},
		"b": {"user_id": "7"},
		"c": {"user_id": nil, "hmac_id": float64(3), "hmac_identifier": "dev1"},
		"d": {"user_id": float64(9), "hls_end": float64(1)}, // ended: not counted
	}
	if got := digestOf(conns); got != (ConnDigest{Count: 3, Users: 2, Xor64: "07b06e765aad73d6"}) {
		t.Fatalf("digest %+v", got)
	}
	if got := digestOf(nil); got != (ConnDigest{Xor64: "0000000000000000"}) {
		t.Fatalf("empty %+v", got)
	}
}

func TestSeedLoadsRecordsWithoutEvents(t *testing.T) {
	r, s, _ := newTestRegistry(t)
	r.Put("old", rec(1, "x", 1, 1))
	n := len(s.events)
	a := &Agent{Registry: r, Logf: t.Logf}
	rw := httptest.NewRecorder()
	a.socketHandler().ServeHTTP(rw, httptest.NewRequest("POST", "/v1/conn/seed", strings.NewReader(`{"reset":true,"records":[{"uuid":"s1","user_id":7,"hls_end":0},{"uuid":"../x","user_id":8},{"uuid":"s2","user_id":8,"hls_end":0}]}`)))
	if rw.Code != 200 || !strings.Contains(rw.Body.String(), `"seeded":2`) {
		t.Fatalf("seed %d %s", rw.Code, rw.Body)
	}
	if r.Get("old") != nil || r.Get("s1") == nil || r.Get("s2") == nil || len(s.events) != n {
		t.Fatalf("after seed: old %v, events %d→%d", r.Get("old"), n, len(s.events))
	}
	if d := r.Digest(); d.Count != 2 || d.Users != 2 {
		t.Fatalf("digest after seed %+v", d)
	}
	// A seed touch is not re-sent to MAIN as a change: MAIN holds it already.
	c := r.Get("s1")
	r.Put("s1", c)
	if len(s.events) != n {
		t.Fatalf("an unchanged seeded record was mirrored: %s", s.types())
	}
}

// snapMain answers heartbeat (asking for a snapshot once) and conn_snapshot.
type snapMain struct {
	*fakeMain
	digest  map[string]any
	chunks  [][]any
	lasts   []bool
	snapIDs map[string]bool
	gapAt   int // a seq refused with SNAP_GAP (-1: none)
}

func (m *snapMain) answer(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
	body, _ := io.ReadAll(r.Body)
	plain, err := cc.Unbox(m.keys.EncUp, reqCtx, body)
	if err != nil {
		http.Error(w, "bad box", 400)
		return
	}
	var req map[string]any
	json.Unmarshal(plain, &req)
	out := map[string]any{}
	switch {
	case strings.HasSuffix(r.URL.Path, "/heartbeat"):
		m.digest, _ = req["conn_digest"].(map[string]any)
		out = map[string]any{"state": "active", "mode": 1, "flows": 74, "want_conn_snapshot": true}
	case strings.HasSuffix(r.URL.Path, "/conn_snapshot"):
		seq := int(req["seq"].(float64))
		if seq == m.gapAt {
			doc, _ := json.Marshal(map[string]any{"v": 1, "typ": "xcvm-denial", "reason": "SNAP_GAP", "node": m.uuid, "req_nonce": hex.EncodeToString(nonce), "expected_seq": 0})
			w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.panel, cc.PanelSigInput("den", doc))))
			w.WriteHeader(409)
			w.Write(doc)
			return
		}
		m.snapIDs[req["snap_id"].(string)] = true
		recs, _ := req["records"].([]any)
		m.chunks = append(m.chunks, recs)
		m.lasts = append(m.lasts, req["last"] == true)
		out = map[string]any{"done": req["last"] == true, "applied": len(recs)}
	}
	ts := uint64(time.Now().UnixMilli())
	rn := make([]byte, 16)
	resCtx, _ := cc.ResponseContext(reqCtx, 200, octet, ts, rn)
	doc, _ := json.Marshal(out)
	sealed, _ := cc.Box(m.keys.EncDown, resCtx, doc)
	w.Header().Set("Content-Type", octet)
	w.Header().Set(cc.HTs, strconv.FormatUint(ts, 10))
	w.Header().Set(cc.HNonce, hex.EncodeToString(rn))
	w.Header().Set(cc.HSig, hex.EncodeToString(cc.MAC(m.keys.MacDown, resCtx, sealed)))
	w.Write(sealed)
}

func TestHeartbeatCarriesTheDigestAndSnapshotGoesInChunks(t *testing.T) {
	f, st := newFake(t)
	m := &snapMain{fakeMain: f, snapIDs: map[string]bool{}, gapAt: -1}
	f.answer = m.answer
	srv := httptest.NewServer(f)
	defer srv.Close()
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	r, _, _ := newTestRegistry(t)
	for i := 0; i < 5; i++ {
		r.Put("c"+strconv.Itoa(i), rec(7+i%2, "x", i, i))
	}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf, Registry: r}
	ctx := context.Background()

	// Flows not known yet: no digest.
	rep, err := a.Heartbeat(ctx)
	if err != nil || !rep.WantConnSnapshot {
		t.Fatalf("heartbeat %v %+v", err, rep)
	}
	if m.digest != nil {
		t.Fatalf("a digest before CONNECTIONS was known: %v", m.digest)
	}
	// The reply switched CONNECTIONS on (74): the next one carries it.
	a.Heartbeat(ctx)
	if m.digest == nil || m.digest["count"] != float64(5) || m.digest["users"] != float64(2) || len(m.digest["xor64"].(string)) != 16 {
		t.Fatalf("digest %v", m.digest)
	}

	old := SnapshotChunk
	SnapshotChunk = 2
	defer func() { SnapshotChunk = old }()
	if err := a.SendSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if len(m.chunks) != 3 || len(m.chunks[0]) != 2 || len(m.chunks[2]) != 1 || m.lasts[0] || m.lasts[1] || !m.lasts[2] || len(m.snapIDs) != 1 {
		t.Fatalf("chunks %v, lasts %v, ids %v", m.chunks, m.lasts, m.snapIDs)
	}

	// An empty registry still sends one (last) chunk: MAIN then removes what it holds.
	m.chunks, m.lasts = nil, nil
	a.Registry = NewRegistry(t.TempDir()+"/r.snap", func([]map[string]any) error { return nil }, t.Logf)
	if err := a.SendSnapshot(ctx); err != nil || len(m.chunks) != 1 || len(m.chunks[0]) != 0 || !m.lasts[0] {
		t.Fatalf("empty: %v %v %v", err, m.chunks, m.lasts)
	}

	// A chunk MAIN refuses ends the snapshot with the refusal.
	m.gapAt = 0
	var d *Denial
	if err := a.SendSnapshot(ctx); err == nil || !errors.As(err, &d) || d.Reason != "SNAP_GAP" {
		t.Fatalf("gap: %v", err)
	}
}
