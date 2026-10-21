package clusteragent

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// --- Task 3: TS arm (continuous relay, prebuffer, odstart, relay-seal) ---

func TestAdminLiveTSRelays(t *testing.T) {
	body := []byte("TS-BYTES-0123456789")
	lf := &liveFanout{id: "42", running: true, hasData: true, liveBody: body}
	rg := newLiveRig(t, lf)
	rec := httptest.NewRecorder()
	rg.a.serveAdminLive(rec, rg.relayReq(t, "/admin/live?stream=42&extension=ts"))
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "video/mp2t" {
		t.Fatalf("ts relay: code=%d ct=%q", rec.Code, rec.Header().Get("Content-Type"))
	}
	if !bytes.Equal(rec.Body.Bytes(), body) {
		t.Fatalf("ts body=%q, want %q", rec.Body.String(), body)
	}
	// admin is a source read, not a counted viewer: no uuid (c=) reaches /live.
	if q := lf.lastLiveQuery(); strings.Contains(q, "c=") {
		t.Fatalf("admin /live carried a viewer uuid (c=): %q", q)
	}
	if q := lf.lastLiveQuery(); q != "prebuffer=0" {
		t.Fatalf("default prebuffer: query=%q, want prebuffer=0", q)
	}
}

func TestAdminLiveRelayM3u8Is404(t *testing.T) {
	lf := &liveFanout{id: "42", running: true, hasData: true}
	rg := newLiveRig(t, lf)
	rec := httptest.NewRecorder()
	rg.a.serveAdminLive(rec, rg.relayReq(t, "/admin/live?stream=42&extension=m3u8"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("relay+m3u8: code=%d, want 404 (relay pulls TS only)", rec.Code)
	}
}

func TestAdminLiveTSSealed(t *testing.T) {
	body := bytes.Repeat([]byte("X"), 3*relayFrameMax+7) // multi-frame
	lf := &liveFanout{id: "42", running: true, hasData: true, liveBody: body}
	rg := newLiveRig(t, lf)
	sessionKey := bytes.Repeat([]byte{4}, 32)
	sealed, err := cc.Seal(rg.boxPub, "relay", relaySealContext(3, 42), sessionKey)
	if err != nil {
		t.Fatal(err)
	}
	rk := base64.RawURLEncoding.EncodeToString(sealed)
	rec := httptest.NewRecorder()
	rg.a.serveAdminLive(rec, rg.relayReq(t, "/admin/live?stream=42&extension=ts&rk="+rk))
	if rec.Code != 200 || rec.Header().Get(relaySealHeader) != relaySealVersion {
		t.Fatalf("sealed ts: code=%d seal=%q", rec.Code, rec.Header().Get(relaySealHeader))
	}
	var out bytes.Buffer
	if err := copyRelayFrames(&out, rec.Body, sessionKey); err != nil {
		t.Fatalf("decode sealed body: %v", err)
	}
	if !bytes.Equal(out.Bytes(), body) {
		t.Fatalf("sealed body mismatch: %d vs %d bytes", out.Len(), len(body))
	}
}

func TestAdminLiveOdstart(t *testing.T) {
	lf := &liveFanout{id: "42", running: true, hasData: true}
	rg := newLiveRig(t, lf)
	rec := httptest.NewRecorder()
	rg.a.serveAdminLive(rec, rg.relayReq(t, "/admin/live?stream=42&extension=ts&odstart"))
	if rec.Code != 200 || rec.Body.String() != "1" {
		t.Fatalf("odstart ready: code=%d body=%q, want 1", rec.Code, rec.Body.String())
	}

	lf2 := &liveFanout{id: "42"} // down, not startable
	rg2 := newLiveRig(t, lf2)
	rg2.writeStream(t, 0) // not on-demand
	rec2 := httptest.NewRecorder()
	rg2.a.serveAdminLive(rec2, rg2.relayReq(t, "/admin/live?stream=42&extension=ts&odstart"))
	if rec2.Code != 200 || rec2.Body.String() != "0" {
		t.Fatalf("odstart not-ready: code=%d body=%q, want 0", rec2.Code, rec2.Body.String())
	}
}

func TestAdminLivePrebufferHeader(t *testing.T) {
	lf := &liveFanout{id: "42", running: true, hasData: true, liveBody: []byte("B")}
	rg := newLiveRig(t, lf)
	r := rg.relayReq(t, "/admin/live?stream=42&extension=ts")
	r.Header.Set("X-XC_VM-PREBUFFER", "1")
	rec := httptest.NewRecorder()
	rg.a.serveAdminLive(rec, r)
	if rec.Code != 200 {
		t.Fatalf("prebuffer header: code=%d", rec.Code)
	}
	if q := lf.lastLiveQuery(); strings.Contains(q, "prebuffer=0") {
		t.Fatalf("a prebuffer request must not force prebuffer=0 (query=%q)", q)
	}
}

func TestAdminLiveNoAdmission404(t *testing.T) {
	lf := &liveFanout{id: "42", running: true, hasData: true}
	rg := newLiveRig(t, lf)
	r := httptest.NewRequest(http.MethodGet, "/admin/live?stream=42&extension=ts", nil)
	r.Header.Set("X-XC-Client-IP", "198.51.100.9") // no ticket, no token, not an allowed server IP
	rec := httptest.NewRecorder()
	rg.a.serveAdminLive(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("no admission: code=%d, want 404", rec.Code)
	}
}
