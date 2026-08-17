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
	isTS, body, err := probe(context.Background(), Source{}, tsSrv.URL)
	if err != nil {
		t.Fatalf("probe mp2t: %v", err)
	}
	body.Close()
	if !isTS {
		t.Fatal("video/mp2t must be classified as direct TS")
	}

	otherSrv := serveTS("video/mp4", []byte("x"))
	defer otherSrv.Close()
	isTS, body, err = probe(context.Background(), Source{}, otherSrv.URL)
	if err != nil {
		t.Fatalf("probe mp4: %v", err)
	}
	body.Close()
	if isTS {
		t.Fatal("video/mp4 must not be classified as direct TS")
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
	err := pullOnce(context.Background(), Source{URLs: []string{srv.URL}}, 12032,
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
	err := pullOnce(context.Background(),
		Source{URLs: []string{srv.URL}, FfmpegBin: fake}, 12032,
		func(b []byte) { got = append(got, b...) })
	if err != nil && err != io.EOF {
		t.Fatalf("pullOnce (ffmpeg): %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ffmpeg branch delivered %d bytes, want %d", len(got), len(payload))
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

	if err := pullOnce(context.Background(),
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
