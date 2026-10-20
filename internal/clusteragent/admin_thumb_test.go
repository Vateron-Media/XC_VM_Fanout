package clusteragent

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/viewertoken"
)

// thumbRig: a node (ServerID 3) with a gateway policy (viewer key + streams path
// + a redirect base for server 9) and a replica, to drive serveAdminThumb via a
// uitoken (the simplest admission for a handler test).
type thumbRig struct {
	a        *Agent
	vkey     []byte
	vctx     []byte
	jpgDir   string
	nowS     int64
	clientIP string
}

func newThumbRig(t *testing.T) *thumbRig {
	t.Helper()
	_, st := newFake(t) // ServerID 3
	dir := t.TempDir()
	jpg := t.TempDir()
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: dir}
	now := time.Now().Unix()
	a.Client.setMainTime(now * 1000)
	vkey, vctx := []byte("0123456789abcdef0123456789abcdef"), []byte("ctx")
	policy := fmt.Sprintf(`{"ip_subnet_match":false,"keys":{"viewer":[{"hex":%q,"until":null}],"shared":[],"context":[{"hex":%q,"until":null}],"accept_legacy_cbc":false},"paths":{"streams":%q},"redirect":{"9":["https://owner9.example/"]}}`,
		hex.EncodeToString(vkey), hex.EncodeToString(vctx), jpg+string(os.PathSeparator))
	pf := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(pf, []byte(policy), 0o644); err != nil {
		t.Fatal(err)
	}
	a.GatewayPolicyFile = pf
	return &thumbRig{a: a, vkey: vkey, vctx: vctx, jpgDir: jpg, nowS: now, clientIP: "203.0.113.5"}
}

func (rg *thumbRig) writeMeta(t *testing.T, id int, live, directSource, vframes int) {
	t.Helper()
	sd := streamsDir(rg.a.ReplicaDir)
	if err := os.MkdirAll(sd, 0o755); err != nil {
		t.Fatal(err)
	}
	js := fmt.Sprintf(`{"data":{"stream":{"vframes_server_id":%d,"direct_source":%d},"type":{"live":%d}}}`, vframes, directSource, live)
	if err := os.WriteFile(filepath.Join(sd, fmt.Sprintf("%d.json", id)), []byte(js), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (rg *thumbRig) req(t *testing.T, stream int) *http.Request {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"stream_id": stream, "ip": rg.clientIP, "expires": int64(2_000_000_000)})
	tok := viewertoken.Seal(b, rg.vkey, rg.vctx, make([]byte, viewertoken.NonceLen))
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/admin/thumb?stream=%d&uitoken=%s&aid=0&expires=0", stream, tok), nil)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	return r
}

func TestAdminThumbOwnerServesJpg(t *testing.T) {
	rg := newThumbRig(t)
	rg.writeMeta(t, 42, 1, 0, 3) // live, direct_source 0, owner = this node (3)
	if err := os.WriteFile(filepath.Join(rg.jpgDir, "42_.jpg"), []byte("JPEGDATA"), 0o644); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	rg.a.serveAdminThumb(rec, rg.req(t, 42))
	if rec.Code != 200 || rec.Body.String() != "JPEGDATA" || rec.Header().Get("Content-Type") != "image/jpg" {
		t.Fatalf("owner thumb: code=%d ct=%q body=%q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}

func TestAdminThumbStale404(t *testing.T) {
	rg := newThumbRig(t)
	rg.writeMeta(t, 42, 1, 0, 3)
	p := filepath.Join(rg.jpgDir, "42_.jpg")
	_ = os.WriteFile(p, []byte("OLD"), 0o644)
	old := time.Now().Add(-90 * time.Second)
	_ = os.Chtimes(p, old, old)
	rec := httptest.NewRecorder()
	rg.a.serveAdminThumb(rec, rg.req(t, 42))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("stale thumb: code=%d, want 404", rec.Code)
	}
}

func TestAdminThumbNonOwner302(t *testing.T) {
	rg := newThumbRig(t)
	rg.writeMeta(t, 42, 1, 0, 9) // owner is server 9, not this node
	rec := httptest.NewRecorder()
	rg.a.serveAdminThumb(rec, rg.req(t, 42))
	if rec.Code != http.StatusFound {
		t.Fatalf("non-owner thumb: code=%d, want 302", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc == "" || loc[:len("https://owner9.example/admin/thumb")] != "https://owner9.example/admin/thumb" {
		t.Fatalf("non-owner redirect Location=%q", loc)
	}
}

func TestAdminThumbNotLive404(t *testing.T) {
	rg := newThumbRig(t)
	rg.writeMeta(t, 42, 0, 0, 3) // not a live stream → StreamSource::streamRow(live=true) null
	rec := httptest.NewRecorder()
	rg.a.serveAdminThumb(rec, rg.req(t, 42))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("non-live thumb: code=%d, want 404", rec.Code)
	}
}

func TestAdminThumbNoAdmission404(t *testing.T) {
	rg := newThumbRig(t)
	rg.writeMeta(t, 42, 1, 0, 3)
	r := httptest.NewRequest(http.MethodGet, "/admin/thumb?stream=42", nil) // no uitoken, no relay
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	rec := httptest.NewRecorder()
	rg.a.serveAdminThumb(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unadmitted thumb: code=%d, want 404", rec.Code)
	}
}
