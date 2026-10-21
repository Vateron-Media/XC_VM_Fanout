package clusteragent

import (
	"net/http/httptest"
	"testing"
)

// thumb.php:24 / vod.php:23 / timeshift.php:23 set Access-Control-Allow-Origin: *
// on every response, like live.php. The admin preview (player.php) loads these
// cross-origin, so the Go ports must set it too (parity with /admin/live).

func TestAdminThumbCORS(t *testing.T) {
	rg := newThumbRig(t)
	rec := httptest.NewRecorder()
	rg.a.serveAdminThumb(rec, rg.req(t, 42))
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("thumb ACAO=%q, want *", rec.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestAdminVodCORS(t *testing.T) {
	rg := newVodRig(t)
	rec := httptest.NewRecorder()
	rg.a.serveAdminVOD(rec, rg.req(t, 42, "mp4", ""))
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("vod ACAO=%q, want *", rec.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestAdminTimeshiftCORS(t *testing.T) {
	rg := newTSRig(t)
	rg.writeMinutes(t, "AAAA", "BBBB", "CCCC")
	rec := httptest.NewRecorder()
	rg.a.serveAdminTimeshift(rec, rg.uitokenReq(t, 42, 3, ""))
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("timeshift ACAO=%q, want *", rec.Header().Get("Access-Control-Allow-Origin"))
	}
}
