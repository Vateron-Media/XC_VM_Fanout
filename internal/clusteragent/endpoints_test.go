package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// policy_ver in hello and heartbeat (ADR 0004, "Releasing an old port early
// (Phase 2, sixth increment)") and the known-good URL sets ("MAIN endpoint
// changes (Phase 3, second increment)").

func TestPolicyVerIsTheAdoptedPolicysOnHelloAndEveryHeartbeat(t *testing.T) {
	m, c := newReplayMain(t)
	c.State.Enrolled, c.State.PolicyVer = true, 7
	url := c.State.MainURLs[0]
	a := &Agent{Client: c, Logf: t.Logf}
	ctx := context.Background()
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// A heartbeat reply's newer version is only seen, never adopted.
	m.answers["heartbeat"] = map[string]any{"state": "active", "mode": 1, "policy_ver": 9}
	for i := 0; i < 2; i++ {
		if _, err := a.Heartbeat(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// The hello that fetches it adopts it: the next request carries it.
	m.answers["hello"] = map[string]any{"state": "active", "mode": 1, "policy": map[string]any{"policy_ver": 9, "transport": "auto", "main_urls": []string{url}}}
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	want := map[string][]float64{"hello": {7, 7}, "heartbeat": {7, 7, 9}}
	for op, vers := range want {
		for i, v := range vers {
			if got := m.bodies[op][i]["policy_ver"]; got != v {
				t.Fatalf("%s %d: policy_ver %v, want %v", op, i, got, v)
			}
		}
	}
	// A JSON integer on the wire.
	b, _ := json.Marshal(map[string]any{"policy_ver": c.State.policyVer()})
	if string(b) != `{"policy_ver":9}` {
		t.Fatalf("encoded %s", b)
	}
}

func TestKnownGoodURLSetsFollowTheContract(t *testing.T) {
	A := []string{"https://a/cluster/v1/", "http://a/cluster/v1/"}
	B := []string{"https://b/cluster/v1/"}
	C := []string{"https://c/cluster/v1/"}
	D := []string{"https://d/cluster/v1/"}
	type step struct {
		ver  int
		urls []string
		base string // the URL that answered
		// the sets held after, newest first: "ver:first URL's host"
		want    string
		written bool
	}
	cases := []struct {
		name  string
		steps []step
	}{
		{"recorded on an answer at a current URL, not re-written when confirmed", []step{
			{1, A, A[1], "1:a", true},
			{1, A, A[0], "1:a", false},
		}},
		{"an answer at a URL the policy does not list records nothing", []step{
			{1, A, B[0], "", false},
		}},
		{"the same URLs under a new version replace the set", []step{
			{1, A, A[0], "1:a", true},
			{2, A, A[0], "2:a", true},
		}},
		{"order matters: the same URLs reordered are another set", []step{
			{1, A, A[0], "1:a", true},
			{2, []string{A[1], A[0]}, A[0], "2:a 1:a", true},
		}},
		{"three kept, the oldest dropped, a set seen again moves to the front", []step{
			{1, A, A[0], "1:a", true},
			{2, B, B[0], "2:b 1:a", true},
			{3, C, C[0], "3:c 2:b 1:a", true},
			{4, D, D[0], "4:d 3:c 2:b", true},
			{5, B, B[0], "5:b 4:d 3:c", true},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, st := newFake(t)
			for i, s := range c.steps {
				st.PolicyVer, st.MainURLs = s.ver, s.urls
				os.Remove(st.path)
				if _, err := st.confirmURLs(s.base); err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, k := range st.KnownGoodURLs {
					host := strings.SplitN(strings.SplitN(k.MainURLs[0], "//", 2)[1], "/", 2)[0]
					got = append(got, strings.Join([]string{itoa(k.PolicyVer), host}, ":"))
				}
				_, err := os.Stat(st.path)
				if strings.Join(got, " ") != s.want || (err == nil) != s.written {
					t.Fatalf("step %d: sets %v (written %v), want %q (written %v)", i, got, err == nil, s.want, s.written)
				}
			}
		})
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func TestFallbackURLsAndTheDialOrder(t *testing.T) {
	cur := []string{"https://new/cluster/v1/", "http://new/cluster/v1/"}
	sets := []URLSet{
		{PolicyVer: 5, MainURLs: []string{"https://mid/cluster/v1/", "http://mid/cluster/v1/", "https://new/cluster/v1/"}},
		{PolicyVer: 3, MainURLs: []string{"http://old/cluster/v1/", "https://mid/cluster/v1/", "https://old/cluster/v1/"}},
	}
	cases := []struct {
		name      string
		transport string
		failed    []string
		want      []string
	}{
		{"newest set first, each URL once, none the policy lists", "auto", nil, []string{
			"https://new/cluster/v1/", "http://new/cluster/v1/",
			"https://mid/cluster/v1/", "http://mid/cluster/v1/", "http://old/cluster/v1/", "https://old/cluster/v1/",
		}},
		{"https_required dials only the https:// fallbacks", "https_required", nil, []string{
			"https://new/cluster/v1/", "http://new/cluster/v1/",
			"https://mid/cluster/v1/", "https://old/cluster/v1/",
		}},
		{"unreachable ones last: the current, then the fallbacks", "auto", []string{"https://new/cluster/v1/", "https://mid/cluster/v1/"}, []string{
			"http://new/cluster/v1/", "http://mid/cluster/v1/", "http://old/cluster/v1/", "https://old/cluster/v1/",
			"https://new/cluster/v1/", "https://mid/cluster/v1/",
		}},
	}
	for _, c := range cases {
		_, st := newFake(t)
		st.MainURLs, st.Transport, st.KnownGoodURLs = cur, c.transport, sets
		cl := NewClient(st, "xc_agent/test")
		for _, u := range c.failed {
			cl.reached(context.Background(), u, context.DeadlineExceeded)
		}
		if got := cl.urls(); !slices.Equal(got, c.want) {
			t.Fatalf("%s:\n got %v\nwant %v", c.name, got, c.want)
		}
	}
}

func TestOnlyAnAuthenticatedAnswerOnTheCurrentSetIsKnownGood(t *testing.T) {
	m, c := newReplayMain(t)
	cur := c.State.MainURLs[0]
	ctx := context.Background()
	call := func() error { return c.Call(ctx, "heartbeat", map[string]any{}, nil, false) }

	// HTTPS_REQUIRED reached MAIN, which refuses ops there: not known-good.
	m.queue("heartbeat", 1, 403, "HTTPS_REQUIRED", nil)
	call()
	// A denial that names no node or request can be replayed: not one either.
	m.refuse["heartbeat"] = append(m.refuse["heartbeat"], func(w http.ResponseWriter, nonce []byte) {
		m.fakeMain.refuse(w, 503, nonce, "DB", map[string]any{"node": nil, "req_nonce": nil})
	})
	call()
	// Nor is a reply whose MAC does not verify.
	m.refuse["heartbeat"] = append(m.refuse["heartbeat"], func(w http.ResponseWriter, nonce []byte) {
		w.Header().Set("Content-Type", octet)
		w.Write([]byte("not a reply"))
	})
	call()
	if len(c.State.KnownGoodURLs) != 0 {
		t.Fatalf("recorded %v", c.State.KnownGoodURLs)
	}
	// A signed denial naming this request is an answer, as is a MAC'd reply.
	m.queue("heartbeat", 1, 503, "STARTING", nil)
	call()
	if len(c.State.KnownGoodURLs) != 1 || c.State.KnownGoodURLs[0].MainURLs[0] != cur {
		t.Fatalf("after a signed denial: %v", c.State.KnownGoodURLs)
	}
	back, _ := loadRaw(c.State.path)
	if len(back.KnownGoodURLs) != 1 {
		t.Fatalf("state file holds %v", back.KnownGoodURLs)
	}

	// A new policy whose URL does not answer: the old set is dialled after
	// it, and an answer there records nothing and asks for a hello.
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	c.State.PolicyVer, c.State.MainURLs = 2, []string{dead.URL + "/cluster/v1/"}
	if err := call(); err != nil {
		t.Fatal(err)
	}
	if len(c.State.KnownGoodURLs) != 1 || c.State.KnownGoodURLs[0].PolicyVer != 0 || !c.fellBack.Load() {
		t.Fatalf("after a fallback answer: %v, fell back %v", c.State.KnownGoodURLs, c.fellBack.Load())
	}
	a := &Agent{Client: c, Logf: t.Logf}
	a.helloing.Store(true) // the hello itself is not under test here
	a.fallbackHello(ctx)
	if a.fallbackHelloAt.IsZero() || a.fallbackHelloVer != 2 || c.fellBack.Load() {
		t.Fatal("no hello after a fallback answer")
	}
	at := a.fallbackHelloAt
	c.fellBack.Store(true)
	a.fallbackHello(ctx)
	if a.fallbackHelloAt != at {
		t.Fatal("a hello per heartbeat while the fallback answers")
	}
}

// mintFor seals a first token to ephPub as MAIN does.
func mintFor(t *testing.T, panel ed25519.PrivateKey, uuid string, ephPub []byte) []byte {
	now := time.Now().Unix()
	doc, _ := json.Marshal(map[string]any{
		"v": 1, "typ": "xcvm-token", "node_uuid": uuid, "server_id": 3, "gen": 1, "epoch": 1,
		"iat": now, "nbf": now - 120, "exp": now + 4500, "kid": "00000000", "rotation_min": 60,
		"grace_min": 15, "refresh_at": now + 1800, "token": hex.EncodeToString(make([]byte, 32)),
	})
	body := binary.BigEndian.AppendUint32(nil, uint32(len(doc)))
	body = append(append(body, doc...), ed25519.Sign(panel, cc.PanelSigInput("tok", doc))...)
	sealed, err := cc.Seal(ephPub, "token", uuid, body)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

func TestInstallEmptiesTheKnownGoodSets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.json")
	uuid := "0f8fad5b-d9cb-469f-a165-70867728950e"
	kg, err := Keygen(path, uuid)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := loadRaw(path)
	st.KnownGoodURLs = []URLSet{{PolicyVer: 4, MainURLs: []string{"https://previous-main/cluster/v1/"}}}
	st.Save()
	_, panel, _ := ed25519.GenerateKey(nil)
	ephPub, _ := hex.DecodeString(kg.EphPub)
	if err := Install(path, InstallData{ServerID: 3, PanelSignPub: panel.Public().(ed25519.PublicKey), MainURLs: []string{"https://main/cluster/v1/"}, PolicyVer: 6, Epoch: 1, TokenSealed: mintFor(t, panel, uuid, ephPub)}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	if strings.Contains(string(b), "known_good_urls") {
		t.Fatalf("install kept the previous sets: %s", b)
	}
	// The wire form of a set, once recorded.
	st, _ = loadRaw(path)
	st.confirmURLs("https://main/cluster/v1/")
	b, _ = os.ReadFile(path)
	var doc struct {
		Known []map[string]any `json:"known_good_urls"`
	}
	json.Unmarshal(b, &doc)
	if len(doc.Known) != 1 || doc.Known[0]["policy_ver"] != float64(6) || doc.Known[0]["main_urls"].([]any)[0] != "https://main/cluster/v1/" {
		t.Fatalf("known_good_urls %s", b)
	}
}

func TestAHeartbeatCarriesTheNodesOwnClock(t *testing.T) {
	m, c := newReplayMain(t)
	c.State.Enrolled = true
	a := &Agent{Client: c, Logf: t.Logf}
	ctx := context.Background()
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// This node's clock runs ten minutes ahead of MAIN's.
	at := time.Now().Add(10 * time.Minute)
	c.now = func() time.Time { return at }
	c.offsetMs.Store(-600000)
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if got := m.bodies["heartbeat"][0]["local_ms"]; got != float64(at.UnixMilli()) {
		t.Fatalf("local_ms %v, want the node's clock %d", got, at.UnixMilli())
	}
	if got := m.stamps["heartbeat"][0]; got != at.UnixMilli()-600000 {
		t.Fatalf("stamped %d, want MAIN's time %d", got, at.UnixMilli()-600000)
	}
}

func TestAHeartbeatCarriesTheLanesLagAndTheURLsThatFail(t *testing.T) {
	m, c := newReplayMain(t)
	c.State.Enrolled = true
	spool := t.TempDir()
	a := &Agent{Client: c, Logf: t.Logf, SpoolDir: spool}
	ctx := context.Background()
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// One P1 file spooled three minutes ago; P0 empty.
	if err := os.MkdirAll(filepath.Join(spool, "p1"), 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(spool, "p1", "1.ndjson")
	if err := os.WriteFile(file, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-3 * time.Minute)
	if err := os.Chtimes(file, old, old); err != nil {
		t.Fatal(err)
	}
	// A URL that first failed a minute ago and has not answered since.
	now := time.Now()
	c.now = func() time.Time { return now }
	c.mu.Lock()
	c.failed = map[string]time.Time{"https://down.example:8443": now.Add(-time.Minute).Add(URLRetry)}
	c.mu.Unlock()
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	body := m.bodies["heartbeat"][0]
	lanes, _ := body["lanes"].(map[string]any)
	p0, _ := lanes["p0"].(map[string]any)
	p1, _ := lanes["p1"].(map[string]any)
	if p0["files"] != float64(0) || p0["lag_ms"] != float64(0) {
		t.Fatalf("p0 %v, want empty", p0)
	}
	if lag, _ := p1["lag_ms"].(float64); p1["files"] != float64(1) || lag < 180000 || lag > 190000 {
		t.Fatalf("p1 %v, want one file three minutes old", p1)
	}
	urls, _ := body["unreachable"].([]any)
	if len(urls) != 1 || urls[0].(map[string]any)["url"] != "https://down.example:8443" || urls[0].(map[string]any)["for_ms"] != float64(60000) {
		t.Fatalf("unreachable %v, want the URL that failed a minute ago", urls)
	}
}
