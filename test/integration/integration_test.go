//go:build integration

// Package integration exercises a real xc_fanout process end-to-end against the
// fake `origin` source, one scenario per test. Each test spawns its own daemon
// with the flags the scenario needs (grace, write-timeout, launch mode, …),
// talks to the client surface over its unix socket and to the control surface
// over the control socket, and pulls the source over TCP from $ORIGIN.
package integration

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// ---- source acquisition ---------------------------------------------------

// TestPullDirectTS_FanoutSingleUpstream: a video/mp2t source is streamed
// straight through (no ffmpeg on the daemon side), and N viewers on one channel
// are still a single upstream pull.
func TestPullDirectTS_FanoutSingleUpstream(t *testing.T) {
	d := startDaemon(t, "-grace", "30")
	const id = "direct"
	d.putStream(id, streamConfig{URLs: []string{tsURL("ch1")}})

	v1, err := d.openLive(id, "")
	if err != nil {
		t.Fatalf("viewer 1: %v", err)
	}
	defer v1.Close()
	v2, err := d.openLive(id, "")
	if err != nil {
		t.Fatalf("viewer 2: %v", err)
	}
	defer v2.Close()

	// Both viewers get valid MPEG-TS.
	b1, err := readAtLeast(v1.resp.Body, 188*100, 8*time.Second)
	if err != nil {
		t.Fatalf("viewer 1 read: %v", err)
	}
	assertTS(t, b1)
	b2, err := readAtLeast(v2.resp.Body, 188*100, 8*time.Second)
	if err != nil {
		t.Fatalf("viewer 2 read: %v", err)
	}
	assertTS(t, b2)

	if st, _ := d.status(id); st.Refs != 2 || !st.Running || !st.HasData {
		t.Fatalf("status = %+v, want refs=2 running has_data", st)
	}
	// The fan-out invariant: two viewers, one upstream connection to the source.
	waitOriginActive(t, 1, 3*time.Second)
}

// TestPullHLSSource_Remux: a non-mp2t (HLS) source must be remuxed to MPEG-TS by
// the daemon's ffmpeg before it reaches a live-TS viewer.
func TestPullHLSSource_Remux(t *testing.T) {
	d := startDaemon(t, "-grace", "30")
	const id = "remux"
	d.putStream(id, streamConfig{URLs: []string{hlsURL("ch1")}, Ffmpeg: ffmpegBin()})

	b := d.readLive(id, "", 188*100, 20*time.Second) // ffmpeg cold-start has headroom
	assertTS(t, b)
	if st, _ := d.status(id); !st.HasData {
		t.Fatalf("status = %+v, want has_data", st)
	}
}

// TestFailover: the first URL is dead (404); the daemon must fall through to the
// next candidate.
func TestFailover(t *testing.T) {
	d := startDaemon(t, "-grace", "30")
	const id = "failover"
	d.putStream(id, streamConfig{URLs: []string{deadURL(), tsURL("ch1")}})

	b := d.readLive(id, "", 188*100, 10*time.Second)
	assertTS(t, b)
}

// TestOffAir: a source that connects but never sends data must read as running
// with has_data=false — the daemon's "not on air" signal.
func TestOffAir(t *testing.T) {
	d := startDaemon(t, "-grace", "30")
	const id = "offair"
	d.putStream(id, streamConfig{URLs: []string{stallURL()}})

	st, code := d.probe(id, 1500)
	if code != 200 {
		t.Fatalf("probe status code = %d", code)
	}
	if !st.Running {
		t.Fatalf("probe %+v, want running=true (puller warmed)", st)
	}
	if st.HasData || st.SinceDataMs != -1 {
		t.Fatalf("probe %+v, want has_data=false since_data_ms=-1", st)
	}
}

// ---- delivery formats -----------------------------------------------------

// TestDaemonHLS_Plaintext: the in-memory HLS surface produces a playlist and a
// valid MPEG-TS segment.
func TestDaemonHLS_Plaintext(t *testing.T) {
	d := startDaemon(t, "-grace", "30", "-hlstarget", "2", "-hlswindow", "4")
	const id = "hls"
	d.putStream(id, streamConfig{URLs: []string{tsURL("ch1")}})

	// Polling the playlist itself warms the puller (touch()); wait for a segment.
	_, seg := d.waitHLS(id, 25*time.Second)
	data, code := d.hlsSegment(id, seg)
	if code != 200 {
		t.Fatalf("segment %s code = %d", seg, code)
	}
	assertTS(t, data)
	_ = mustAtoi(t, seg) // sequence is numeric
}

// TestDaemonHLS_Encrypted: with a key/iv set, segments come back AES-128-CBC
// encrypted and decrypt (same key+iv, PKCS#7) back to valid MPEG-TS.
func TestDaemonHLS_Encrypted(t *testing.T) {
	d := startDaemon(t, "-grace", "30", "-hlstarget", "2", "-hlswindow", "4")
	const id = "hlsenc"
	const keyHex = "00112233445566778899aabbccddeeff"
	const ivHex = "0102030405060708090a0b0c0d0e0f10"
	d.putStream(id, streamConfig{URLs: []string{tsURL("ch1")}, Key: keyHex, IV: ivHex})

	_, seg := d.waitHLS(id, 25*time.Second)
	enc, code := d.hlsSegment(id, seg)
	if code != 200 {
		t.Fatalf("segment %s code = %d", seg, code)
	}
	if len(enc)%aes.BlockSize != 0 || len(enc) == 0 {
		t.Fatalf("ciphertext len %d is not a block multiple", len(enc))
	}
	plain := decryptCBC(t, enc, keyHex, ivHex)
	assertTS(t, plain)
}

// TestPrebuffer: a viewer asking for history on join must receive data (the
// retained tail), not just the live edge.
func TestPrebuffer(t *testing.T) {
	d := startDaemon(t, "-grace", "30", "-prebuffer-max", "15")
	const id = "prebuf"
	d.putStream(id, streamConfig{URLs: []string{tsURL("ch1")}})

	// Warm the stream so history accumulates.
	warm := d.readLive(id, "", 188*100, 8*time.Second)
	assertTS(t, warm)
	time.Sleep(2 * time.Second)

	// A fresh viewer with prebuffer should get a burst of buffered TS quickly.
	lc, err := d.openLive(id, "prebuffer=10")
	if err != nil {
		t.Fatalf("prebuffer viewer: %v", err)
	}
	defer lc.Close()
	b, err := readAtLeast(lc.resp.Body, 188*200, 5*time.Second)
	if err != nil {
		t.Fatalf("prebuffer read: %v", err)
	}
	assertTS(t, b)
}

// ---- control / lifecycle --------------------------------------------------

// TestConnectionsAndRates: a viewer identified by ?c=<uuid> shows up in
// /connections and, once bytes flow, in /rates.
func TestConnectionsAndRates(t *testing.T) {
	d := startDaemon(t, "-grace", "30")
	const id = "conns"
	const uuid = "uuid-conns-1"
	d.putStream(id, streamConfig{URLs: []string{tsURL("ch1")}})

	lc, err := d.openLive(id, "c="+uuid)
	if err != nil {
		t.Fatalf("viewer: %v", err)
	}
	defer lc.Close()
	// Drain bytes in the background so rate accounting advances.
	go io.Copy(io.Discard, lc.resp.Body)

	if !waitConnection(t, d, uuid, 5*time.Second) {
		t.Fatalf("uuid %s never appeared in /connections", uuid)
	}
	// Give the rate window a moment of delivered bytes.
	time.Sleep(1500 * time.Millisecond)
	if _, ok := d.rates()[uuid]; !ok {
		t.Fatalf("uuid %s missing from /rates", uuid)
	}
}

// TestGraceReaper: after the last viewer leaves and the grace window elapses,
// the puller stops and the upstream pull is released.
func TestGraceReaper(t *testing.T) {
	d := startDaemon(t, "-grace", "2")
	const id = "grace"
	d.putStream(id, streamConfig{URLs: []string{tsURL("ch1")}})

	lc, err := d.openLive(id, "")
	if err != nil {
		t.Fatalf("viewer: %v", err)
	}
	b, err := readAtLeast(lc.resp.Body, 188*50, 8*time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertTS(t, b)
	if st, _ := d.status(id); !st.Running {
		t.Fatalf("status = %+v, want running while viewing", st)
	}
	lc.Close() // last viewer leaves

	// Within a few grace windows the reaper must stop the puller.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if st, _ := d.status(id); !st.Running && st.Refs == 0 {
			waitOriginActive(t, 0, 3*time.Second)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("puller did not idle-stop after grace")
}

// TestDeleteTeardown: DELETE removes the stream; status and live then 404.
func TestDeleteTeardown(t *testing.T) {
	d := startDaemon(t, "-grace", "30")
	const id = "teardown"
	d.putStream(id, streamConfig{URLs: []string{tsURL("ch1")}})
	_ = d.readLive(id, "", 188*50, 8*time.Second)

	d.deleteStream(id)
	if _, code := d.status(id); code != 404 {
		t.Fatalf("status after delete = %d, want 404", code)
	}
	if _, err := d.openLive(id, ""); err == nil {
		t.Fatal("live after delete should 404")
	}
}

// TestStalledViewerEviction: a viewer that stops draining its socket is dropped
// after -write-timeout, and its uuid disappears from /connections.
func TestStalledViewerEviction(t *testing.T) {
	d := startDaemon(t, "-grace", "30", "-write-timeout", "2")
	const id = "stall"
	const uuid = "uuid-stall-1"
	d.putStream(id, streamConfig{URLs: []string{tsURL("ch1")}})

	lc, err := d.openLive(id, "c="+uuid)
	if err != nil {
		t.Fatalf("viewer: %v", err)
	}
	defer lc.Close()

	// Read a little so the connection is alive and registered, then stop reading.
	if _, err := io.ReadFull(lc.resp.Body, make([]byte, 4096)); err != nil {
		t.Fatalf("initial read: %v", err)
	}
	if !waitConnection(t, d, uuid, 5*time.Second) {
		t.Fatalf("uuid %s never registered", uuid)
	}

	// Now we hold the body open but never read again: the daemon's send buffer
	// fills, the next write blocks past -write-timeout, and the viewer is evicted.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if !hasConnection(d, uuid) {
			return // evicted — success
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("stalled viewer was not evicted within the deadline")
}

// ---- push (ingest) mode ---------------------------------------------------

// TestIngestPush: a producer pushes MPEG-TS into the per-stream ingest socket
// and a live-TS viewer receives it.
func TestIngestPush(t *testing.T) {
	d := startDaemon(t, "-grace", "30")
	const id = "push"
	sock := d.setIngest(id, streamConfig{})
	if sock == "" {
		t.Fatal("ingest returned empty socket path")
	}

	stop := feedIngest(t, sock)
	defer stop()

	b := d.readLive(id, "", 188*100, 12*time.Second)
	assertTS(t, b)
}

// ---- launch (test) mode ---------------------------------------------------

// TestLaunchModeSource: -id/-source feeds one stream from a URL at launch,
// without the control API being used.
func TestLaunchModeSource(t *testing.T) {
	d := startDaemon(t, "-id", "launchsrc", "-source", tsURL("ch1"))
	b := d.readLive("launchsrc", "", 188*100, 10*time.Second)
	assertTS(t, b)
}

// TestLaunchModeFile: -id/-in feeds one stream from a local file at launch. The
// file is published into the retention window; a viewer joining with prebuffer
// picks up the buffered tail.
func TestLaunchModeFile(t *testing.T) {
	sample := makeSampleTS(t, 10)
	d := startDaemon(t, "-id", "launchfile", "-in", sample, "-prebuffer-max", "20")
	b := d.readLive("launchfile", "prebuffer=20", 188*50, 10*time.Second)
	assertTS(t, b)
}

// ---- helpers specific to these tests --------------------------------------

func waitOriginActive(t *testing.T, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got int64
	for time.Now().Before(deadline) {
		got, _ = originStats(t)
		if got == want {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("origin ts_active = %d, want %d", got, want)
}

func hasConnection(d *daemon, uuid string) bool {
	for _, u := range d.connections() {
		if u == uuid {
			return true
		}
	}
	return false
}

func waitConnection(t *testing.T, d *daemon, uuid string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if hasConnection(d, uuid) {
			return true
		}
		time.Sleep(150 * time.Millisecond)
	}
	return false
}

func decryptCBC(t *testing.T, ct []byte, keyHex, ivHex string) []byte {
	t.Helper()
	key, _ := hex.DecodeString(keyHex)
	iv, _ := hex.DecodeString(ivHex)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("aes key: %v", err)
	}
	out := make([]byte, len(ct))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, ct)
	// Strip PKCS#7 padding.
	if n := len(out); n > 0 {
		pad := int(out[n-1])
		if pad > 0 && pad <= aes.BlockSize && pad <= n {
			out = out[:n-pad]
		}
	}
	return out
}

// feedIngest starts an ffmpeg that loops a synthetic source as real-time MPEG-TS
// into the daemon's ingest unix socket, returning a stop function.
func feedIngest(t *testing.T, sock string) func() {
	t.Helper()
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial ingest socket: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, ffmpegBin(),
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-re", "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=25",
		"-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
		"-g", "25", "-pix_fmt", "yuv420p",
		"-f", "mpegts", "pipe:1",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		conn.Close()
		t.Fatalf("ingest ffmpeg pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		conn.Close()
		t.Fatalf("ingest ffmpeg start: %v", err)
	}
	go func() { _, _ = io.Copy(conn, stdout) }()
	return func() {
		cancel()
		conn.Close()
		_ = cmd.Wait()
	}
}

// makeSampleTS writes a seconds-long synthetic MPEG-TS file to a temp path.
func makeSampleTS(t *testing.T, seconds int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sample.ts")
	cmd := exec.Command(ffmpegBin(),
		"-y", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc2=size=320x240:rate=25",
		"-c:v", "libx264", "-preset", "veryfast", "-g", "25", "-pix_fmt", "yuv420p",
		"-t", strconv.Itoa(seconds), "-f", "mpegts", path,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make sample ts: %v\n%s", err, out)
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() == 0 {
		t.Fatalf("sample ts not produced: %v", err)
	}
	return path
}
