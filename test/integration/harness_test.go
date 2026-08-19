//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// origin returns the base URL of the fake source service (docker-compose sets
// $ORIGIN=http://origin:8080).
func origin() string {
	if v := os.Getenv("ORIGIN"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func ffmpegBin() string {
	if v := os.Getenv("FFMPEG"); v != "" {
		return v
	}
	return "ffmpeg"
}

// daemon is one running xc_fanout process with its own socket directory.
type daemon struct {
	t         *testing.T
	dir       string
	sock      string // client socket (nginx-facing)
	ctl       string // control socket (PHP-only); "" when disabled
	ingestDir string
	cmd       *exec.Cmd
	cancel    context.CancelFunc
}

// startDaemon launches the daemon with the given extra flags and waits until its
// client socket answers /healthz. It registers cleanup on t.
func startDaemon(t *testing.T, extraFlags ...string) *daemon {
	t.Helper()
	bin := os.Getenv("XC_FANOUT_BIN")
	if bin == "" {
		bin = "xc_fanout"
	}
	dir := t.TempDir()
	d := &daemon{
		t:         t,
		dir:       dir,
		sock:      filepath.Join(dir, "http.sock"),
		ctl:       filepath.Join(dir, "control.sock"),
		ingestDir: filepath.Join(dir, "ingest"),
	}

	args := []string{"-sock", d.sock, "-ctl", d.ctl, "-ingestdir", d.ingestDir}
	args = append(args, extraFlags...)
	// If the caller opted out of the control API, drop -ctl.
	if hasFlag(extraFlags, "-noctl") {
		d.ctl = ""
		args = stripFlag(args, "-ctl", true)
		args = stripFlag(args, "-noctl", false)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdout = &prefixWriter{t: t, p: "[daemon] "}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start daemon: %v", err)
	}
	d.cmd = cmd
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := d.health(); err == nil {
			return d
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("daemon did not become healthy on %s", d.sock)
	return nil
}

func hasFlag(flags []string, name string) bool {
	for _, f := range flags {
		if f == name {
			return true
		}
	}
	return false
}

// stripFlag removes name from args; if hasValue, also removes the token after it.
func stripFlag(args []string, name string, hasValue bool) []string {
	out := args[:0:0]
	for i := 0; i < len(args); i++ {
		if args[i] == name {
			if hasValue {
				i++
			}
			continue
		}
		out = append(out, args[i])
	}
	return out
}

// unixClient builds an http.Client bound to a single unix socket. Requests use
// the host "unix" (ignored) and the real path, e.g. GET http://unix/live/<id>.
func unixClient(sock string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
	}
}

func (d *daemon) client() *http.Client { return unixClient(d.sock) }
func (d *daemon) control() *http.Client {
	if d.ctl == "" {
		d.t.Fatal("control API is disabled for this daemon")
	}
	return unixClient(d.ctl)
}

func (d *daemon) health() error {
	resp, err := d.client().Get("http://unix/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("healthz: %d", resp.StatusCode)
	}
	return nil
}

// ---- control-surface helpers ----------------------------------------------

type streamConfig struct {
	URLs   []string `json:"urls"`
	UA     string   `json:"ua,omitempty"`
	Proxy  string   `json:"proxy,omitempty"`
	Cookie string   `json:"cookie,omitempty"`
	Ffmpeg string   `json:"ffmpeg,omitempty"`
	Chunk  int      `json:"chunk,omitempty"`
	Key    string   `json:"key,omitempty"`
	IV     string   `json:"iv,omitempty"`
}

type streamStatus struct {
	Running     bool  `json:"running"`
	Refs        int   `json:"refs"`
	HasData     bool  `json:"has_data"`
	SinceDataMs int64 `json:"since_data_ms"`
}

func (d *daemon) putStream(id string, cfg streamConfig) {
	d.t.Helper()
	body, _ := json.Marshal(cfg)
	req, _ := http.NewRequest(http.MethodPut, "http://unix/streams/"+id, bytes.NewReader(body))
	resp, err := d.control().Do(req)
	if err != nil {
		d.t.Fatalf("put stream %s: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		d.t.Fatalf("put stream %s: status %d (%s)", id, resp.StatusCode, strings.TrimSpace(string(b)))
	}
}

func (d *daemon) status(id string) (streamStatus, int) {
	d.t.Helper()
	resp, err := d.control().Get("http://unix/streams/" + id)
	if err != nil {
		d.t.Fatalf("status %s: %v", id, err)
	}
	defer resp.Body.Close()
	var st streamStatus
	if resp.StatusCode == 200 {
		_ = json.NewDecoder(resp.Body).Decode(&st)
	}
	return st, resp.StatusCode
}

func (d *daemon) probe(id string, waitMs int) (streamStatus, int) {
	d.t.Helper()
	resp, err := d.control().Get(fmt.Sprintf("http://unix/probe/%s?wait=%d", id, waitMs))
	if err != nil {
		d.t.Fatalf("probe %s: %v", id, err)
	}
	defer resp.Body.Close()
	var st streamStatus
	if resp.StatusCode == 200 {
		_ = json.NewDecoder(resp.Body).Decode(&st)
	}
	return st, resp.StatusCode
}

func (d *daemon) deleteStream(id string) {
	d.t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, "http://unix/streams/"+id, nil)
	resp, err := d.control().Do(req)
	if err != nil {
		d.t.Fatalf("delete %s: %v", id, err)
	}
	resp.Body.Close()
}

func (d *daemon) connections() []string {
	d.t.Helper()
	resp, err := d.control().Get("http://unix/connections")
	if err != nil {
		d.t.Fatalf("connections: %v", err)
	}
	defer resp.Body.Close()
	var out []string
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func (d *daemon) rates() map[string]float64 {
	d.t.Helper()
	resp, err := d.control().Get("http://unix/rates")
	if err != nil {
		d.t.Fatalf("rates: %v", err)
	}
	defer resp.Body.Close()
	out := map[string]float64{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

// setIngest switches id into push mode and returns the socket path producers
// must connect to.
func (d *daemon) setIngest(id string, cfg streamConfig) string {
	d.t.Helper()
	body, _ := json.Marshal(cfg)
	req, _ := http.NewRequest(http.MethodPut, "http://unix/ingest/"+id, bytes.NewReader(body))
	resp, err := d.control().Do(req)
	if err != nil {
		d.t.Fatalf("ingest %s: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		d.t.Fatalf("ingest %s: status %d", id, resp.StatusCode)
	}
	var out struct {
		Socket string `json:"socket"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.Socket
}

// ---- client-surface helpers -----------------------------------------------

// liveConn holds an open /live/<id> stream so the test can read from it and, by
// choosing when to close it, control the viewer lifecycle.
type liveConn struct {
	resp   *http.Response
	cancel context.CancelFunc
}

func (lc *liveConn) Close() {
	if lc.resp != nil {
		lc.resp.Body.Close()
	}
	if lc.cancel != nil {
		lc.cancel()
	}
}

// openLive starts a /live/<id> request with optional query (e.g. "c=uuid&prebuffer=5").
func (d *daemon) openLive(id, query string) (*liveConn, error) {
	ctx, cancel := context.WithCancel(context.Background())
	url := "http://unix/live/" + id
	if query != "" {
		url += "?" + query
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := d.client().Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("live %s: status %d", id, resp.StatusCode)
	}
	return &liveConn{resp: resp, cancel: cancel}, nil
}

// readLive opens /live/<id> and reads at least minBytes within timeout, then
// returns them. Fatal on failure. Leaves nothing open.
func (d *daemon) readLive(id, query string, minBytes int, timeout time.Duration) []byte {
	d.t.Helper()
	lc, err := d.openLive(id, query)
	if err != nil {
		d.t.Fatalf("open live %s: %v", id, err)
	}
	defer lc.Close()
	b, err := readAtLeast(lc.resp.Body, minBytes, timeout)
	if err != nil {
		d.t.Fatalf("read live %s: got %d/%d bytes: %v", id, len(b), minBytes, err)
	}
	return b
}

// readAtLeast reads until it has n bytes or timeout elapses.
func readAtLeast(r io.Reader, n int, timeout time.Duration) ([]byte, error) {
	type res struct {
		b   []byte
		err error
	}
	ch := make(chan res, 1)
	go func() {
		buf := make([]byte, 0, n)
		tmp := make([]byte, 4096)
		for len(buf) < n {
			m, err := r.Read(tmp)
			buf = append(buf, tmp[:m]...)
			if err != nil {
				ch <- res{buf, err}
				return
			}
		}
		ch <- res{buf, nil}
	}()
	select {
	case rr := <-ch:
		return rr.b, rr.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout after %s", timeout)
	}
}

// ---- HLS helpers ----------------------------------------------------------

func (d *daemon) hlsPlaylist(id string) (string, int) {
	d.t.Helper()
	resp, err := d.client().Get("http://unix/hls/" + id + "/index.m3u8")
	if err != nil {
		d.t.Fatalf("hls playlist %s: %v", id, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode
}

func (d *daemon) hlsSegment(id, name string) ([]byte, int) {
	d.t.Helper()
	resp, err := d.client().Get("http://unix/hls/" + id + "/" + name)
	if err != nil {
		d.t.Fatalf("hls segment %s/%s: %v", id, name, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode
}

// firstSegmentName returns the first "<seq>.ts" URI from a media playlist.
func firstSegmentName(playlist string) (string, bool) {
	sc := bufio.NewScanner(strings.NewReader(playlist))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		return line, true
	}
	return "", false
}

// waitHLS polls the daemon playlist until a segment is available or timeout.
func (d *daemon) waitHLS(id string, timeout time.Duration) (playlist, segName string) {
	d.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pl, code := d.hlsPlaylist(id)
		if code == 200 {
			if name, ok := firstSegmentName(pl); ok {
				return pl, name
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	d.t.Fatalf("hls %s produced no segment within %s", id, timeout)
	return "", ""
}

// ---- source helpers -------------------------------------------------------

// tsURL / hlsURL build source URLs on the origin service.
func tsURL(ch string) string  { return origin() + "/ts/" + ch }
func hlsURL(ch string) string { return origin() + "/hls/" + ch + "/index.m3u8" }
func deadURL() string         { return origin() + "/ts/dead" }
func stallURL() string        { return origin() + "/ts/stall" }

// originStats reads the fan-out invariant counters from the origin service.
func originStats(t *testing.T) (active, total int64) {
	t.Helper()
	resp, err := http.Get(origin() + "/stats")
	if err != nil {
		t.Fatalf("origin stats: %v", err)
	}
	defer resp.Body.Close()
	var s struct {
		Active int64 `json:"ts_active"`
		Total  int64 `json:"ts_total"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&s)
	return s.Active, s.Total
}

// ---- assertions -----------------------------------------------------------

// assertTS checks the buffer looks like MPEG-TS: first byte is the 0x47 sync and
// it repeats one packet (188 bytes) later.
func assertTS(t *testing.T, b []byte) {
	t.Helper()
	if len(b) < 188*2 {
		t.Fatalf("too few bytes for a TS check: %d", len(b))
	}
	// Find the first sync byte, then verify the 188-byte cadence holds.
	start := bytes.IndexByte(b, 0x47)
	if start < 0 {
		t.Fatalf("no TS sync byte 0x47 in %d bytes", len(b))
	}
	good := 0
	for off := start; off+188 < len(b); off += 188 {
		if b[off] == 0x47 {
			good++
		}
	}
	if good < 3 {
		t.Fatalf("bytes do not follow the 188-byte TS cadence (good=%d)", good)
	}
}

// prefixWriter tags daemon output lines so failures show which process spoke.
type prefixWriter struct {
	t *testing.T
	p string
}

func (w *prefixWriter) Write(b []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		w.t.Logf("%s%s", w.p, line)
	}
	return len(b), nil
}

// mustAtoi is a tiny helper for parsing segment sequence numbers in tests.
func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(strings.TrimSuffix(s, ".ts"))
	if err != nil {
		t.Fatalf("atoi %q: %v", s, err)
	}
	return n
}
