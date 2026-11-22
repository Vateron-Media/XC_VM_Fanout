package nativesrc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A hostile byte-range offset near int64 max — a hostile #EXT-X-BYTERANGE offset near int64 max, with an origin that
// answers 200 (ignoring Range), overflowed Offset+Length negative and panicked
// slicing the body. Must be refused, never crash.
func TestHostileByteRangeDoesNotPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			_, _ = w.Write(make([]byte, 4096)) // 200, whole body, ignoring Range
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n" +
			"#EXT-X-BYTERANGE:100@9223372036854775800\n#EXTINF:2.0,\nall.ts\n"))
	}))
	defer srv.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PANICKED on a hostile byte range (C2): %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/x.m3u8", Options{})
	if err != nil {
		return // refused at open is fine
	}
	defer rc.Close()
	_, _ = io.ReadAll(rc) // must not panic
}
