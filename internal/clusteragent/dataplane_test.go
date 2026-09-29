package clusteragent

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// A parent verifying a child's relay or file request must see each nonce once.
// MAIN has the cluster bus for that; a load balancer serving as a parent has
// only its agent, so the window lives in the agent (plan, section 6.6).
func TestNonceWindowSpendsEachOnce(t *testing.T) {
	c := newNonceCache()
	at := time.Unix(1800000000, 0)
	c.now = func() time.Time { return at }

	if !c.fresh("a") || c.fresh("a") {
		t.Fatal("a nonce was accepted twice")
	}
	if !c.fresh("b") {
		t.Fatal("another nonce was refused")
	}

	// One window on: the previous bucket still answers for what it holds.
	at = at.Add(NonceWindow)
	if c.fresh("a") {
		t.Fatal("a nonce from the previous window was accepted")
	}
	if !c.fresh("c") {
		t.Fatal("a new nonce in the new window was refused")
	}

	// Two windows on: nothing older survives, and the cache has not grown.
	at = at.Add(2 * NonceWindow)
	if !c.fresh("a") {
		t.Fatal("a nonce two windows old is not a replay any signature could use")
	}
	if len(c.live)+len(c.previous) > 2 {
		t.Fatalf("buckets hold %d+%d entries after a rotation", len(c.live), len(c.previous))
	}
}

func TestNonceWindowRefusesRatherThanGrow(t *testing.T) {
	c := newNonceCache()
	at := time.Unix(1800000000, 0)
	c.now = func() time.Time { return at }
	for i := 0; i < MaxNonces; i++ {
		c.live[strconv.Itoa(i)] = struct{}{}
	}
	// A parent under a flood refuses the request (which retries) instead of
	// growing until the agent dies.
	if c.fresh("one more") {
		t.Fatal("a full window accepted a nonce")
	}
}

// The owner of a file vouches for what it served with its node key, which the
// agent holds and the node's PHP does not. The document must be byte-identical
// to the panel's FileDigest (sorted keys), or the fetcher's verification fails.
func TestFileDigestIsSignedWithTheNodeKey(t *testing.T) {
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	// MAIN's clock, as the agent measured it: 1800000000, while this node's
	// runs 50 s behind. The digest is stamped on MAIN's clock.
	a.Client.now = func() time.Time { return time.Unix(1800000000-50, 0) }
	a.Client.setMainTime(1800000000 * 1000)

	rec := httptest.NewRecorder()
	// The caller's own iat (PHP's time()) is not what gets signed.
	body := `{"tid":"tid_0123456789","owner_sid":3,"size":1234,"sha256":"` + strings.Repeat("ab", 32) + `","iat":1700000000}`
	a.dataPlaneHandler(rec, httptest.NewRequest(http.MethodPost, "/v1/file_digest", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("file_digest: %d %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Header string `json:"header"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(out.Header, ".")
	if len(parts) != 2 {
		t.Fatalf("header %q", out.Header)
	}
	doc, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	// The panel's document, key for key and in its order.
	want := `{"iat":1800000000,"owner_sid":3,"sha256":"` + strings.Repeat("ab", 32) + `","size":1234,"tid":"tid_0123456789","typ":"xcvm-file-digest","v":1}`
	if string(doc) != want {
		t.Fatalf("document\n got %s\nwant %s", doc, want)
	}
	pub := st.SignKey().Public().(ed25519.PublicKey)
	if !cc.VerifyNode(pub, "digest", doc, sig) {
		t.Fatal("the signature does not verify under the node key with purpose digest")
	}
	if cc.VerifyNode(pub, "relay", doc, sig) {
		t.Fatal("the purpose is not bound")
	}
}

// A chunk's digest names the nonce of the request it answers, as the
// owner's PHP passes it; one without (older PHP) is signed as before.
func TestAChunksDigestNamesTheRequestsNonce(t *testing.T) {
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	a.Client.now = func() time.Time { return time.Unix(1800000000, 0) }
	for nonce, want := range map[string]string{
		strings.Repeat("cd", 16): `{"iat":1800000000,"nonce":"` + strings.Repeat("cd", 16) + `","offset":0,"owner_sid":3,"sha256":"` + strings.Repeat("ab", 32) + `","size":1,"tid":"t0123456789","total":1,"typ":"xcvm-file-digest","v":1}`,
		"":                       `{"iat":1800000000,"offset":0,"owner_sid":3,"sha256":"` + strings.Repeat("ab", 32) + `","size":1,"tid":"t0123456789","total":1,"typ":"xcvm-file-digest","v":1}`,
	} {
		body := `{"tid":"t0123456789","owner_sid":3,"size":1,"sha256":"` + strings.Repeat("ab", 32) + `","offset":0,"total":1`
		if nonce != "" {
			body += `,"nonce":"` + nonce + `"`
		}
		rec := httptest.NewRecorder()
		a.dataPlaneHandler(rec, httptest.NewRequest(http.MethodPost, "/v1/file_digest", strings.NewReader(body+"}")))
		var out struct {
			Header string `json:"header"`
		}
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
			t.Fatalf("nonce %q: %d %s", nonce, rec.Code, rec.Body.String())
		}
		doc, _ := base64.RawURLEncoding.DecodeString(strings.Split(out.Header, ".")[0])
		if string(doc) != want {
			t.Errorf("nonce %q: document\n got %s\nwant %s", nonce, doc, want)
		}
	}
}

func TestDataPlaneRefusesWhatItCannotSign(t *testing.T) {
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}

	for name, body := range map[string]string{
		"no sha256": `{"tid":"t0123456789","owner_sid":3,"size":1}`,
		"short sha": `{"tid":"t0123456789","owner_sid":3,"size":1,"sha256":"ab"}`,
		"no owner":  `{"tid":"t0123456789","owner_sid":0,"size":1,"sha256":"` + strings.Repeat("ab", 32) + `"}`,
		"no ticket": `{"tid":"","owner_sid":3,"size":1,"sha256":"` + strings.Repeat("ab", 32) + `"}`,
		"bad size":  `{"tid":"t0123456789","owner_sid":3,"size":-1,"sha256":"` + strings.Repeat("ab", 32) + `"}`,
		"not json":  `nope`,
		// The node signs digests as itself only (it is server 3).
		"another owner": `{"tid":"t0123456789","owner_sid":5,"size":1,"sha256":"` + strings.Repeat("ab", 32) + `"}`,
		// A nonce names the request a chunk answers: a chunk's only, 32 lowercase hex.
		"a nonce on a whole file": `{"tid":"t0123456789","owner_sid":3,"size":1,"sha256":"` + strings.Repeat("ab", 32) + `","nonce":"` + strings.Repeat("cd", 16) + `"}`,
		"a short nonce":           `{"tid":"t0123456789","owner_sid":3,"size":1,"sha256":"` + strings.Repeat("ab", 32) + `","offset":0,"total":1,"nonce":"cdcd"}`,
		"an upper-case nonce":     `{"tid":"t0123456789","owner_sid":3,"size":1,"sha256":"` + strings.Repeat("ab", 32) + `","offset":0,"total":1,"nonce":"` + strings.Repeat("CD", 16) + `"}`,
	} {
		rec := httptest.NewRecorder()
		a.dataPlaneHandler(rec, httptest.NewRequest(http.MethodPost, "/v1/file_digest", strings.NewReader(body)))
		if rec.Code == http.StatusOK {
			t.Errorf("%s: signed anyway", name)
		}
		if name == "another owner" && rec.Code != http.StatusForbidden {
			t.Errorf("%s: %d, want 403", name, rec.Code)
		}
	}

	for name, body := range map[string]string{
		"not hex":   `{"nonce":"zz"}`,
		"too short": `{"nonce":"abcd"}`,
		"missing":   `{}`,
	} {
		rec := httptest.NewRecorder()
		a.dataPlaneHandler(rec, httptest.NewRequest(http.MethodPost, "/v1/nonce", strings.NewReader(body)))
		if rec.Code == http.StatusOK {
			t.Errorf("%s: accepted as a nonce", name)
		}
	}
}
