package puller

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

func serveTS(ct string, body []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", ct)
		_, _ = w.Write(body)
	}))
}

func TestProbeClassifiesContentType(t *testing.T) {
	tsSrv := serveTS("video/mp2t", []byte("x"))
	defer tsSrv.Close()
	isTS, body, err := probe(context.Background(), mustClient(t, Source{}), Source{}, tsSrv.URL)
	if err != nil {
		t.Fatalf("probe mp2t: %v", err)
	}
	body.Close()
	if !isTS {
		t.Fatal("video/mp2t must be classified as direct TS")
	}

	otherSrv := serveTS("video/mp4", []byte("x"))
	defer otherSrv.Close()
	isTS, body, err = probe(context.Background(), mustClient(t, Source{}), Source{}, otherSrv.URL)
	if err != nil {
		t.Fatalf("probe mp4: %v", err)
	}
	body.Close()
	if isTS {
		t.Fatal("video/mp4 must not be classified as direct TS")
	}
}

// TestSourceTLSVerification: Insecure skips upstream cert verification (a
// self-signed HTTPS source is accepted), while Insecure=false rejects it.
func TestSourceTLSVerification(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write([]byte("x"))
	}))
	defer srv.Close()

	// Insecure (the daemon default): the self-signed cert is accepted.
	isTS, body, err := probe(context.Background(), mustClient(t, Source{Insecure: true}), Source{Insecure: true}, srv.URL)
	if err != nil {
		t.Fatalf("insecure probe of a self-signed TLS source failed: %v", err)
	}
	body.Close()
	if !isTS {
		t.Fatal("video/mp2t must classify as direct TS")
	}

	// Verification on: the self-signed cert must be rejected.
	if _, _, err := probe(context.Background(), mustClient(t, Source{Insecure: false}), Source{Insecure: false}, srv.URL); err == nil {
		t.Fatal("secure probe must reject a self-signed certificate")
	}
}

func TestDirectPullStreamsBytes(t *testing.T) {
	payload := tsfixture.Concat(
		tsfixture.PAT(0x100),
		tsfixture.PMT(0x100, 0x101),
		tsfixture.Keyframe(0x101, 0),
	)
	srv := serveTS("video/mp2t", payload)
	defer srv.Close()

	var got []byte
	err := pullOnce(context.Background(), mustClient(t, Source{}), Source{URLs: []string{srv.URL}}, 12032,
		func(b []byte) { got = append(got, b...) })
	if err != nil && err != io.EOF {
		t.Fatalf("pullOnce: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("direct pull delivered %d bytes, want %d", len(got), len(payload))
	}
}

func TestFfmpegBranchRemuxes(t *testing.T) {
	dir := t.TempDir()
	payload := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	payloadPath := filepath.Join(dir, "p.ts")
	if err := os.WriteFile(payloadPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	// A stand-in ffmpeg that ignores its args and emits the payload on stdout.
	fake := filepath.Join(dir, "fakeffmpeg")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\ncat "+payloadPath+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	srv := serveTS("video/mp4", []byte("not a TS stream"))
	defer srv.Close()

	var got []byte
	err := pullOnce(context.Background(), mustClient(t, Source{}),
		Source{URLs: []string{srv.URL}, FfmpegBin: fake}, 12032,
		func(b []byte) { got = append(got, b...) })
	if err != nil && err != io.EOF {
		t.Fatalf("pullOnce (ffmpeg): %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ffmpeg branch delivered %d bytes, want %d", len(got), len(payload))
	}
}

// TestFfmpegExitSurfaced: when ffmpeg exits non-zero after closing stdout
// cleanly, runFfmpeg must surface the failure (not let it reach Run as a plain
// EOF that looks like a normal source end), and carry ffmpeg's stderr for the log.
func TestFfmpegExitSurfaced(t *testing.T) {
	dir := t.TempDir()
	// A stand-in ffmpeg that writes a diagnostic to stderr and exits 1 with no
	// stdout — i.e. a source ffmpeg could not open/decode.
	fake := filepath.Join(dir, "fakeffmpeg")
	script := "#!/bin/sh\necho 'boom: could not open source' >&2\nexit 1\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	srv := serveTS("video/mp4", []byte("not a TS stream"))
	defer srv.Close()

	err := pullOnce(context.Background(), mustClient(t, Source{}),
		Source{URLs: []string{srv.URL}, FfmpegBin: fake, Label: "t"}, 12032, func([]byte) {})
	if err == nil || err == io.EOF {
		t.Fatalf("ffmpeg exit 1 must surface as an error, got %v", err)
	}
	if !strings.Contains(err.Error(), "ffmpeg") {
		t.Fatalf("error should identify ffmpeg, got %q", err.Error())
	}
}

// TestFfmpegCancelNotSurfaced: when the context is cancelled (stream stop /
// shutdown), the ffmpeg kill must NOT be reported as a fault.
func TestFfmpegCancelNotSurfaced(t *testing.T) {
	dir := t.TempDir()
	// A stand-in ffmpeg that runs until killed. `exec` replaces the shell so the
	// context kill lands on sleep directly (otherwise the grandchild outlives the
	// killed shell, holding the stdout pipe open until it exits on its own).
	fake := filepath.Join(dir, "fakeffmpeg")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	srv := serveTS("video/mp4", []byte("not a TS stream"))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	err := pullOnce(ctx, mustClient(t, Source{}), Source{URLs: []string{srv.URL}, FfmpegBin: fake}, 12032, func([]byte) {})
	// The contract: a cancel-induced ffmpeg kill must never be reported as an
	// "ffmpeg: …" fault (whatever benign EOF/read error the pipe returns is fine).
	if err != nil && strings.Contains(err.Error(), "ffmpeg:") {
		t.Fatalf("cancel surfaced an ffmpeg fault: %q", err.Error())
	}
}

// TestFfmpegColdStartArgs pins ADR 0003 Phase C1a: the remux ffmpeg must bound
// input analysis (probesize/analyzeduration) and enable HTTP reconnect, and
// those must be INPUT options — i.e. precede -i — or ffmpeg ignores them and the
// cold-start win is lost.
func TestFfmpegColdStartArgs(t *testing.T) {
	dir := t.TempDir()
	payload := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	payloadPath := filepath.Join(dir, "p.ts")
	if err := os.WriteFile(payloadPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	argPath := filepath.Join(dir, "args.txt")
	// A stand-in ffmpeg that records its argv (one per line) then emits the payload.
	fake := filepath.Join(dir, "fakeffmpeg")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > " + argPath + "\ncat " + payloadPath + "\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	srv := serveTS("video/mp4", []byte("not a TS stream"))
	defer srv.Close()

	if err := pullOnce(context.Background(), mustClient(t, Source{}),
		Source{URLs: []string{srv.URL}, FfmpegBin: fake}, 12032, func([]byte) {}); err != nil && err != io.EOF {
		t.Fatalf("pullOnce (ffmpeg): %v", err)
	}

	raw, err := os.ReadFile(argPath)
	if err != nil {
		t.Fatalf("read recorded args: %v", err)
	}
	args := strings.Split(strings.TrimSpace(string(raw)), "\n")
	idx := func(flag string) int {
		for i, a := range args {
			if a == flag {
				return i
			}
		}
		return -1
	}

	iPos := idx("-i")
	if iPos < 0 {
		t.Fatalf("ffmpeg args missing -i: %v", args)
	}
	for _, flag := range []string{"-probesize", "-analyzeduration", "-reconnect", "-reconnect_streamed", "-reconnect_delay_max"} {
		p := idx(flag)
		if p < 0 {
			t.Errorf("cold-start flag %s missing from ffmpeg args", flag)
			continue
		}
		if p > iPos {
			t.Errorf("cold-start flag %s at %d must precede -i at %d (else ffmpeg ignores it)", flag, p, iPos)
		}
	}
	// Bounded well under ffmpeg's 5MB/5s defaults or the cold-start win is lost.
	if p := idx("-probesize"); p >= 0 && p+1 < len(args) && args[p+1] != "1000000" {
		t.Errorf("probesize = %s, want 1000000", args[p+1])
	}
	if p := idx("-analyzeduration"); p >= 0 && p+1 < len(args) && args[p+1] != "1000000" {
		t.Errorf("analyzeduration = %s, want 1000000", args[p+1])
	}
}

// mustClient builds the shared per-puller HTTP client the way Run does, so the
// tests exercise probe/pullOnce through the same transport bounds production uses.
func mustClient(t *testing.T, src Source) *http.Client {
	t.Helper()
	c, err := httpClient(src)
	if err != nil {
		t.Fatalf("httpClient: %v", err)
	}
	t.Cleanup(c.CloseIdleConnections)
	return c
}
