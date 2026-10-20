package clusteragent

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/viewertoken"
)

type tsRig struct {
	a          *Agent
	panel      ed25519.PrivateKey
	child      ed25519.PrivateKey
	vkey, vctx []byte
	archiveDir string // ARCHIVE_PATH/<id>
	ts         int64  // the start second the fixtures are written at
	clientIP   string // 203.0.113.50 (the dataplane-off server IP, for the password arm)
	nowMs      int64
}

func newTSRig(t *testing.T) *tsRig {
	t.Helper()
	f, st := newFake(t) // ServerID 3
	dir := t.TempDir()
	arch := t.TempDir()
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: dir}
	now := time.Now().Unix()
	a.Client.setMainTime(now * 1000)

	cpub, cpriv, _ := ed25519.GenerateKey(nil)
	// servers.json: child node (relay) + a dataplane-off server (password).
	servers := fmt.Sprintf(`{"data":{"servers":[{"id":5,"server_ip":"203.0.113.50","private_ip":""}],"nodes":[`+
		`{"sid":7,"gen":1,"state":"active","ed_pub":%q,"box_pub":""},{"sid":5,"gen":1,"state":"active","ed_pub":"","box_pub":"","dataplane":false}]}}`,
		base64.StdEncoding.EncodeToString(cpub))
	_ = os.WriteFile(filepath.Join(dir, "servers.json"), []byte(servers), 0o644)
	writeSecrets(t, dir, "s3cr3t")
	// replica stream record so streamMeta(42) is ok.
	sd := streamsDir(dir)
	_ = os.MkdirAll(sd, 0o755)
	_ = os.WriteFile(filepath.Join(sd, "42.json"), []byte(`{"data":{"stream":{"direct_source":0},"type":{"live":1},"server":{"on_demand":0}}}`), 0o644)
	// policy: viewer key + archive path + UTC timezone.
	vkey, vctx := []byte("0123456789abcdef0123456789abcdef"), []byte("ctx")
	policy := fmt.Sprintf(`{"ip_subnet_match":false,"keys":{"viewer":[{"hex":%q,"until":null}],"shared":[],"context":[{"hex":%q,"until":null}],"accept_legacy_cbc":false},"paths":{"archive":%q},"default_timezone":"UTC"}`,
		hex.EncodeToString(vkey), hex.EncodeToString(vctx), arch+string(os.PathSeparator))
	pf := filepath.Join(dir, "policy.json")
	_ = os.WriteFile(pf, []byte(policy), 0o644)
	a.GatewayPolicyFile = pf

	archiveDir := filepath.Join(arch, "42")
	_ = os.MkdirAll(archiveDir, 0o755)
	// a round start minute so the archive filenames line up
	ts := (now / 60) * 60
	return &tsRig{a: a, panel: f.panel, child: cpriv, vkey: vkey, vctx: vctx, archiveDir: archiveDir, ts: ts, clientIP: "203.0.113.50", nowMs: now * 1000}
}

func (rg *tsRig) writeMinutes(t *testing.T, contents ...string) {
	t.Helper()
	for i, c := range contents {
		name := time.Unix(rg.ts+int64(i)*60, 0).UTC().Format("2006-01-02:15-04") + ".ts"
		if err := os.WriteFile(filepath.Join(rg.archiveDir, name), []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func (rg *tsRig) minuteName(i int) string {
	return time.Unix(rg.ts+int64(i)*60, 0).UTC().Format("2006-01-02:15-04") + ".ts"
}

func (rg *tsRig) uitokenReq(t *testing.T, stream int, duration int, extra string) *http.Request {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"stream_id": stream, "ip": rg.clientIP, "expires": int64(2_000_000_000),
		"start": strconv.FormatInt(rg.ts, 10), "duration": strconv.Itoa(duration)})
	tok := viewertoken.Seal(b, rg.vkey, rg.vctx, make([]byte, viewertoken.NonceLen))
	u := "/admin/timeshift?uitoken=" + tok + extra
	r := httptest.NewRequest(http.MethodGet, u, nil)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	return r
}

func TestTimeshiftM3U8Playlist(t *testing.T) {
	rg := newTSRig(t)
	rg.writeMinutes(t, "AAAA", "BBBB", "CCCC")
	rec := httptest.NewRecorder()
	rg.a.serveAdminTimeshift(rec, rg.uitokenReq(t, 42, 3, ""))
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/x-mpegurl" {
		t.Fatalf("m3u8: code=%d ct=%q", rec.Code, rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "#EXTM3U") || !strings.Contains(body, "#EXT-X-PLAYLIST-TYPE:VOD") || !strings.HasSuffix(body, "#EXT-X-ENDLIST") {
		t.Fatalf("playlist shape:\n%s", body)
	}
	for i := 0; i < 3; i++ {
		if !strings.Contains(body, "segment="+rg.minuteName(i)+"&uitoken=") {
			t.Fatalf("playlist missing minute %d (%s):\n%s", i, rg.minuteName(i), body)
		}
	}
	if strings.Count(body, "#EXTINF:60.0,") != 3 {
		t.Fatalf("want 3 EXTINF, got:\n%s", body)
	}
}

func TestTimeshiftSegmentServe(t *testing.T) {
	rg := newTSRig(t)
	rg.writeMinutes(t, "AAAA", "BBBB", "CCCC")
	rec := httptest.NewRecorder()
	rg.a.serveAdminTimeshift(rec, rg.uitokenReq(t, 42, 3, "&segment="+rg.minuteName(1)))
	if rec.Code != 200 || rec.Body.String() != "BBBB" || rec.Header().Get("Content-Type") != "video/mp2t" {
		t.Fatalf("segment: code=%d ct=%q body=%q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}

func TestTimeshiftSegmentNoTraversal(t *testing.T) {
	rg := newTSRig(t)
	rg.writeMinutes(t, "AAAA")
	rec := httptest.NewRecorder()
	rg.a.serveAdminTimeshift(rec, rg.uitokenReq(t, 42, 1, "&segment=..%2f..%2fsecrets.json"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("traversal segment: code=%d, want 404", rec.Code)
	}
}

func TestTimeshiftMissingFirst404(t *testing.T) {
	rg := newTSRig(t)
	// no archive files written
	rec := httptest.NewRecorder()
	rg.a.serveAdminTimeshift(rec, rg.uitokenReq(t, 42, 3, ""))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing archive: code=%d, want 404", rec.Code)
	}
}

func TestTimeshiftTSByteRangePassword(t *testing.T) {
	rg := newTSRig(t)
	rg.writeMinutes(t, "AAAA", "BBBB", "CCCC") // concatenated = AAAABBBBCCCC (12 bytes)
	u := fmt.Sprintf("/admin/timeshift?stream=42&extension=ts&start=%d&duration=3&password=s3cr3t", rg.ts)
	r := httptest.NewRequest(http.MethodGet, u, nil)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	r.Header.Set("Range", "bytes=2-9")
	rec := httptest.NewRecorder()
	rg.a.serveAdminTimeshift(rec, r)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("ts range: code=%d, want 206", rec.Code)
	}
	if rec.Body.String() != "AABBBBCC" {
		t.Fatalf("ts range body=%q, want AABBBBCC", rec.Body.String())
	}
	if rec.Header().Get("Content-Range") != "bytes 2-9/12" {
		t.Fatalf("Content-Range=%q", rec.Header().Get("Content-Range"))
	}
}

func TestTimeshiftTSFullBodyPassword(t *testing.T) {
	rg := newTSRig(t)
	rg.writeMinutes(t, "AAAA", "BBBB", "CCCC")
	u := fmt.Sprintf("/admin/timeshift?stream=42&extension=ts&start=%d&duration=3&password=s3cr3t", rg.ts)
	r := httptest.NewRequest(http.MethodGet, u, nil)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	rec := httptest.NewRecorder()
	rg.a.serveAdminTimeshift(rec, r)
	if rec.Code != 200 || rec.Body.String() != "AAAABBBBCCCC" {
		t.Fatalf("ts full: code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestTimeshiftRelayM3U8Rejected(t *testing.T) {
	rg := newTSRig(t)
	rg.writeMinutes(t, "AAAA", "BBBB")
	now := rg.nowMs / 1000
	doc := map[string]any{"v": 1, "typ": "xcvm-relay", "tid": "r0123456789", "iat": now - 60, "exp": now + 3600,
		"parent_sid": 3, "stream_id": 42, "child_sid": 7, "child_gen": 1}
	b, _ := json.Marshal(doc)
	wire := cc.JoinSigned(b, ed25519.Sign(rg.panel, cc.PanelSigInput("rly", b)))
	target := fmt.Sprintf("/admin/timeshift?stream=42&extension=m3u8&start=%d&duration=2", rg.ts)
	hdr, err := cc.RelayAuthHeader(rg.child, wire, "GET", target, rg.nowMs, freshNonce(t))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("X-XCVM-Relay", wire)
	r.Header.Set("X-XCVM-Relay-Auth", hdr)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	rec := httptest.NewRecorder()
	rg.a.serveAdminTimeshift(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("relay+m3u8: code=%d, want 404 (relay pulls TS only)", rec.Code)
	}
}
