package clusteragent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/viewertoken"
)

// A uitoken with no container must not let the client's stream-param extension
// choose the VOD file: PHP derives the ext solely from the token. With an empty
// container the request is refused (no client-controlled path input).
func TestAdminVodUitokenIgnoresParamExt(t *testing.T) {
	rg := newVodRig(t)
	rg.writeFile(t, "42.mp4", []byte("MOVIE"))
	b, _ := json.Marshal(map[string]any{"stream_id": 42, "ip": rg.clientIP, "expires": int64(2_000_000_000)}) // no container
	tok := viewertoken.Seal(b, rg.vkey, rg.vctx, make([]byte, viewertoken.NonceLen))
	r := httptest.NewRequest(http.MethodGet, "/admin/vod?uitoken="+tok+"&stream=42.mp4", nil)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	rec := httptest.NewRecorder()
	rg.a.serveAdminVOD(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("empty-container uitoken used the client's .mp4 param: code=%d, want 404", rec.Code)
	}
}

// verifyLoopbackToken must require exactly a 32-byte key (LoopbackToken::key), so
// a malformed key file (e.g. a trailing newline) can't silently diverge from PHP.
func TestLoopbackTokenRequires32ByteKey(t *testing.T) {
	key33 := []byte("0123456789abcdef0123456789abcdef\n") // 33 bytes
	tok := mintLoopback(key33, 42, 2_000_000_000)
	if verifyLoopbackToken(key33, 42, tok, 1000) {
		t.Fatal("a 33-byte key was accepted; PHP requires exactly 32")
	}
}
