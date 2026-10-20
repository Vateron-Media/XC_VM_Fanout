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

type vodRig struct {
	a        *Agent
	vkey     []byte
	vctx     []byte
	vodDir   string
	clientIP string
}

func newVodRig(t *testing.T) *vodRig {
	t.Helper()
	_, st := newFake(t)
	dir := t.TempDir()
	vod := t.TempDir()
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: dir}
	a.Client.setMainTime(time.Now().Unix() * 1000)
	vkey, vctx := []byte("0123456789abcdef0123456789abcdef"), []byte("ctx")
	policy := fmt.Sprintf(`{"ip_subnet_match":false,"keys":{"viewer":[{"hex":%q,"until":null}],"shared":[],"context":[{"hex":%q,"until":null}],"accept_legacy_cbc":false},"paths":{"vod":%q}}`,
		hex.EncodeToString(vkey), hex.EncodeToString(vctx), vod+string(os.PathSeparator))
	pf := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(pf, []byte(policy), 0o644); err != nil {
		t.Fatal(err)
	}
	a.GatewayPolicyFile = pf
	// replica record carrying target_container (the MIME source).
	sd := streamsDir(dir)
	_ = os.MkdirAll(sd, 0o755)
	_ = os.WriteFile(filepath.Join(sd, "42.json"), []byte(`{"data":{"stream":{"target_container":"mp4"}}}`), 0o644)
	return &vodRig{a: a, vkey: vkey, vctx: vctx, vodDir: vod, clientIP: "203.0.113.5"}
}

func (rg *vodRig) writeFile(t *testing.T, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(rg.vodDir, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// req builds a uitoken-admitted /admin/vod request; container is the token's ext.
func (rg *vodRig) req(t *testing.T, stream int, container string, rangeHdr string) *http.Request {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"stream_id": stream, "ip": rg.clientIP, "expires": int64(2_000_000_000), "container": container})
	tok := viewertoken.Seal(b, rg.vkey, rg.vctx, make([]byte, viewertoken.NonceLen))
	r := httptest.NewRequest(http.MethodGet, "/admin/vod?uitoken="+tok, nil)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	if rangeHdr != "" {
		r.Header.Set("Range", rangeHdr)
	}
	return r
}

func TestAdminVodFullBody(t *testing.T) {
	rg := newVodRig(t)
	rg.writeFile(t, "42.mp4", []byte("MOVIEBYTES"))
	rec := httptest.NewRecorder()
	rg.a.serveAdminVOD(rec, rg.req(t, 42, "mp4", ""))
	if rec.Code != 200 || rec.Body.String() != "MOVIEBYTES" {
		t.Fatalf("full body: code=%d body=%q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "video/mp4" {
		t.Fatalf("Content-Type=%q, want video/mp4 (from target_container)", ct)
	}
}

func TestAdminVodRange(t *testing.T) {
	rg := newVodRig(t)
	rg.writeFile(t, "42.mp4", []byte("0123456789"))
	rec := httptest.NewRecorder()
	rg.a.serveAdminVOD(rec, rg.req(t, 42, "mp4", "bytes=2-5"))
	if rec.Code != http.StatusPartialContent || rec.Body.String() != "2345" {
		t.Fatalf("range: code=%d body=%q, want 206 \"2345\"", rec.Code, rec.Body.String())
	}
}

func TestAdminVodBadRange(t *testing.T) {
	rg := newVodRig(t)
	rg.writeFile(t, "42.mp4", []byte("0123456789"))
	rec := httptest.NewRecorder()
	rg.a.serveAdminVOD(rec, rg.req(t, 42, "mp4", "bytes=999-1000"))
	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("bad range: code=%d, want 416", rec.Code)
	}
}

func TestAdminVodNoTraversalExt(t *testing.T) {
	rg := newVodRig(t)
	rg.writeFile(t, "42.mp4", []byte("x"))
	rec := httptest.NewRecorder()
	rg.a.serveAdminVOD(rec, rg.req(t, 42, "mp4/../../etc", "")) // bad container ext
	if rec.Code != http.StatusNotFound {
		t.Fatalf("traversal ext: code=%d, want 404", rec.Code)
	}
}

func TestAdminVodAdmissionRequired(t *testing.T) {
	rg := newVodRig(t)
	rg.writeFile(t, "42.mp4", []byte("x"))
	r := httptest.NewRequest(http.MethodGet, "/admin/vod?stream=42.mp4", nil) // no uitoken, no relay
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	rec := httptest.NewRecorder()
	rg.a.serveAdminVOD(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unadmitted vod: code=%d, want 404", rec.Code)
	}
}
