package clusteragent

import (
	"io"
	"net/http"
	"strconv"
)

// recorder.go ports the PHP DVR recorder (Cli/Commands/RecordCommand.php,
// `console.php record`): it captures one due recording to an .mp4 VOD. It pulls
// the node's own stream by calling the ported /admin/live (live) or
// /admin/timeshift (catch-up) handlers in-process, admitted with a freshly
// minted lb1- loopback token, writes ARCHIVE_PATH/<recording_id>.ts, then asks
// MAIN (recording_complete) for the VOD id, runs ffmpeg -c copy into
// VOD_PATH/<vod>.mp4, and reports state as P0 recording.state spool events. The
// scheduler that finds due recordings and starts one worker per is slice 3.

// issueLoopbackToken mints the lb1-<exp>-<mac> token the recorder presents to
// its own /admin over loopback — the issue side of verifyLoopbackToken, reusing
// loopbackMAC. Empty when the key is not 32 bytes (LoopbackToken::key).
func issueLoopbackToken(key []byte, streamID int64, exp int64) string {
	if len(key) != 32 {
		return ""
	}
	return "lb1-" + strconv.FormatInt(exp, 10) + "-" + loopbackMAC(key, streamID, exp)
}

// recordSink is a minimal http.ResponseWriter that streams an /admin handler's
// body to w and records its status, so the recorder can call the ported
// handlers in-process (no HTTP hop) and tell a served body (200) from an
// admission/serve failure (404).
type recordSink struct {
	w      io.Writer
	status int
	hdr    http.Header
}

func newRecordSink(w io.Writer) *recordSink {
	return &recordSink{w: w, hdr: http.Header{}}
}

func (s *recordSink) Header() http.Header { return s.hdr }

func (s *recordSink) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
}

func (s *recordSink) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK // an implicit 200, as net/http does on first write
	}
	return s.w.Write(b)
}
