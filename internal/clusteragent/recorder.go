package clusteragent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
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
	if s.status != http.StatusOK {
		return len(b), nil // discard a non-200 body (e.g. the 404 text) — PHP's fopen fails on a 4xx, writing nothing
	}
	return s.w.Write(b)
}

// recording is one scheduled DVR recording (RECORDING_FIELDS from the replica),
// resolved by the slice-3 scheduler and handed to the worker.
type recording struct {
	ID         int64
	StreamID   int64
	Start, End int64 // unix seconds
	Archive    bool  // a catch-up recording (pull /admin/timeshift) vs a live one
	StreamIcon string
	SourceID   int64 // the node scheduled to record it (recordings.source_id)
	Status     int   // recordings.status (1 recording, 2 done; the scheduler skips those)
}

// captureRecording captures rec's bytes to dst by calling the node's own ported
// /admin handler in-process, admitted with a freshly minted lb1- loopback token
// (RecordCommand's loopback source pull). Live → /admin/live bounded by rec.End
// (the Manager's /live is a continuous in-memory stream, so one ctx-deadline
// pass captures the window — no PHP broken-pipe reconnect). Catch-up →
// /admin/timeshift over [start, start+duration), one bounded pass. Returns the
// serve status (404 = admission/serve refused, nothing captured).
func (a *Agent) captureRecording(ctx context.Context, rec recording, dst io.Writer) int {
	var key []byte
	if a.LoopbackKeyFile != "" {
		key, _ = os.ReadFile(a.LoopbackKeyFile)
	}
	// The 32-byte lb1- key (config/cluster/loopback.key) MUST be provisioned by the
	// install flow: with no/invalid key the token is "" and every recording 404s
	// here (there is no live_streaming_pass fallback, unlike RecordCommand::loopbackPass).
	tok := issueLoopbackToken(key, rec.StreamID, time.Now().Unix()+86400) // LoopbackToken::TTL (24h), now-based — NOT tied to rec.End, else a past-programme catch-up mints an expired token.
	id := strconv.FormatInt(rec.StreamID, 10)
	var target string
	if rec.Archive {
		dur := (rec.End - rec.Start) / 60
		target = "/admin/timeshift?stream=" + id + "&extension=ts&start=" + strconv.FormatInt(rec.Start, 10) +
			"&duration=" + strconv.FormatInt(dur, 10) + "&password=" + tok
	} else {
		target = "/admin/live?stream=" + id + "&extension=ts&password=" + tok
	}
	cctx := ctx
	if !rec.Archive {
		var cancel context.CancelFunc
		cctx, cancel = context.WithDeadline(ctx, time.Unix(rec.End, 0))
		defer cancel()
	}
	r, err := http.NewRequestWithContext(cctx, http.MethodGet, target, nil)
	if err != nil {
		return 0
	}
	r.Header.Set("X-XC-Client-IP", "127.0.0.1")
	sink := newRecordSink(dst)
	if rec.Archive {
		a.serveAdminTimeshift(sink, r)
	} else {
		a.serveAdminLive(sink, r)
	}
	return sink.status
}

// recorderFfmpeg is the ffmpeg binary the recorder runs for the TS→MP4 copy;
// the node's real path is wired by config (slice 3). recorderComplete and
// recorderRun are the two external calls (MAIN op, ffmpeg), package vars so
// tests fake them.
var (
	recorderFfmpeg = "ffmpeg"

	// icon is forwarded as-is; MAIN's RecordingFinalizer regex-validates it
	// (s:<sid>:/images/<md5>.jpg) and drops a raw URL, so DVR VODs currently get no
	// cover art. Porting RecordCommand::downloadAndSaveImage needs a new images path
	// in adminPolicy — a follow-up.
	recorderComplete = func(a *Agent, ctx context.Context, recID int64, icon string) (int64, error) {
		var out struct {
			StreamID int64 `json:"stream_id"`
		}
		err := a.Client.Call(ctx, "recording_complete", map[string]any{"recording_id": recID, "stream_icon": icon}, &out, false)
		return out.StreamID, err
	}

	recorderRun = func(ctx context.Context, name string, args ...string) error {
		return exec.CommandContext(ctx, name, args...).Run()
	}
)

// finalizeRecording ports RecordCommand::processRecording: ask MAIN to create
// the VOD row (recording_complete → its stream id), then ffmpeg -c copy the
// captured .ts into VOD_PATH/<vod>.mp4. A VOD id ≤ 0 (MAIN made none) or an
// ffmpeg that produces no file is a failure.
func (a *Agent) finalizeRecording(ctx context.Context, rec recording, tsPath string) (int64, error) {
	vodID, err := recorderComplete(a, ctx, rec.ID, rec.StreamIcon)
	if err != nil {
		return 0, err
	}
	if vodID <= 0 {
		return 0, errors.New("recorder: MAIN created no VOD row")
	}
	pol, ok := a.adminPolicy()
	if !ok {
		return 0, errors.New("recorder: no gateway policy")
	}
	out := filepath.Join(pol.vodPath, strconv.FormatInt(vodID, 10)+".mp4")
	if err := recorderRun(ctx, recorderFfmpeg, "-i", tsPath, "-c:v", "copy", "-c:a", "copy", out); err != nil {
		return 0, err
	}
	if st, err := os.Stat(out); err != nil || st.Size() <= 0 {
		return 0, errors.New("recorder: ffmpeg produced no mp4")
	}
	return vodID, nil
}

// spoolRecordingState reports a recording's status to MAIN as a P0 recording.state
// event (ContentSink::recordingState on a CONTENT-flow node): 1 recording, 2 done,
// 3 failed.
func (a *Agent) spoolRecordingState(id int64, status int) {
	_ = spoolP0(a.SpoolDir, []map[string]any{{"type": "recording.state", "d": map[string]any{"id": id, "status": status}}})
}

// runRecording is the worker's body (RecordCommand::execute): a programme that
// is not airing and not an archive job fails without a VOD; otherwise it marks
// the recording started, captures to ARCHIVE_PATH/<id>.ts, and on a non-empty
// capture finalizes it into a VOD (done), else fails. The .ts is always removed.
//
// Two deliberate narrowings vs RecordCommand: (1) an archive (catch-up) recording
// assumes its minute files are already written — the slice-3 scheduler starts an
// archive worker only once now >= End+65 (RecordCommand's in-worker end+65 wait),
// so the worker does not wait; (2) a live source that drops mid-window finalizes
// whatever was captured as done, without PHP's broken-pipe reconnect (the
// Manager's /live is a continuous in-memory stream).
func (a *Agent) runRecording(ctx context.Context, rec recording) {
	pol, ok := a.adminPolicy()
	if !ok {
		return
	}
	tsPath := filepath.Join(pol.archivePath, strconv.FormatInt(rec.ID, 10)+".ts")
	now := time.Now().Unix()
	if !rec.Archive && (rec.Start-60 > now || now > rec.End) {
		a.spoolRecordingState(rec.ID, 3) // not currently airing (RecordCommand lines 79–84)
		_ = os.Remove(tsPath)
		return
	}
	a.spoolRecordingState(rec.ID, 1)
	f, err := os.Create(tsPath)
	if err != nil {
		a.spoolRecordingState(rec.ID, 3)
		return
	}
	st := a.captureRecording(ctx, rec, f)
	_ = f.Close()
	fi, serr := os.Stat(tsPath)
	if st != http.StatusOK || serr != nil || fi.Size() == 0 {
		a.spoolRecordingState(rec.ID, 3)
		_ = os.Remove(tsPath)
		return
	}
	if _, err := a.finalizeRecording(ctx, rec, tsPath); err != nil {
		a.spoolRecordingState(rec.ID, 3)
		_ = os.Remove(tsPath)
		return
	}
	a.spoolRecordingState(rec.ID, 2)
	_ = os.Remove(tsPath)
}
