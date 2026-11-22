// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// argRecorder writes a stand-in ffmpeg that dumps its argv (one per line) to
// argPath and exits. Every other ffmpeg test uses a stand-in that IGNORES argv,
// which is exactly why a whole class of per-scheme argument bugs was invisible.
func argRecorder(t *testing.T) (bin string, args func() []string) {
	t.Helper()
	dir := t.TempDir()
	bin = filepath.Join(dir, "fakeffmpeg")
	argPath := filepath.Join(dir, "args.txt")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > " + argPath + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, func() []string {
		t.Helper()
		raw, err := os.ReadFile(argPath)
		if err != nil {
			t.Fatalf("stand-in ffmpeg recorded no args: %v", err)
		}
		return strings.Split(strings.TrimSpace(string(raw)), "\n")
	}
}

func hasArg(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

// httpOnlyFfmpegOpts are AVOptions that only the http/https protocol declares.
// ffmpeg treats an input option no protocol consumed as FATAL ("Option
// user_agent not found." / "Error opening input files: Option not found",
// exit 8), so passing any of them with a udp/rtp/file input kills the child
// before a single byte reaches stdout.
var httpOnlyFfmpegOpts = []string{
	"-user_agent", "-reconnect", "-reconnect_streamed", "-reconnect_delay_max",
	"-headers", "-http_proxy",
}

// alwaysFfmpegOpts are AVFormatContext options, valid for any input, and must
// survive the gating — they are the ADR 0003 cold-start bounds.
var alwaysFfmpegOpts = []string{"-probesize", "-analyzeduration"}

// TestFfmpegHTTPOnlyOptionsGatedOnScheme: the remux child must only be given
// HTTP protocol options when the input actually IS http/https. An operator who
// pins backend=ffmpeg on a udp://, rtp:// or file:// source otherwise gets an
// ffmpeg that dies in under 100ms, forever.
func TestFfmpegHTTPOnlyOptionsGatedOnScheme(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantHTTP bool
	}{
		{"udp", "udp://239.0.0.1:1234", false},
		{"rtp", "rtp://239.0.0.1:1234", false},
		{"file-url", "file:///media/x.ts", false},
		{"bare-path", "/media/x.ts", false},
		{"relative-path", "./x.ts", false},
		{"http", "http://origin.invalid/live.ts", true},
		{"https", "https://origin.invalid/live.ts", true},
		{"http-uppercase-scheme", "HTTP://origin.invalid/live.ts", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bin, args := argRecorder(t)
			src := Source{
				URLs:      []string{tc.raw},
				FfmpegBin: bin,
				Backend:   BackendFfmpeg,
				Cookie:    "sid=abc",                // forces -headers
				Proxy:     "127.0.0.1:3128",         // forces -http_proxy
				Headers:   []string{"X-Token: shh"}, // forces -headers
				Label:     "t",
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// backend=ffmpeg with no response goes straight to runFfmpeg for any
			// scheme — the operator's documented kill-switch.
			_ = convert(ctx, src, tc.raw, nil, 12032, func([]byte) {})

			got := args()
			iPos := -1
			for i, a := range got {
				if a == "-i" {
					iPos = i
					break
				}
			}
			if iPos < 0 || iPos+1 >= len(got) {
				t.Fatalf("ffmpeg args missing -i <url>: %v", got)
			}
			if got[iPos+1] != tc.raw {
				t.Errorf("-i = %q, want the configured URL %q", got[iPos+1], tc.raw)
			}
			for _, flag := range alwaysFfmpegOpts {
				if !hasArg(got, flag) {
					t.Errorf("%s must be passed for every scheme (it is an AVFormatContext option); args=%v", flag, got)
				}
			}
			for _, flag := range httpOnlyFfmpegOpts {
				if has := hasArg(got, flag); has != tc.wantHTTP {
					if tc.wantHTTP {
						t.Errorf("%s missing for an http source — the HTTP path still needs it", flag)
					} else {
						t.Errorf("%s passed with a %s input: ffmpeg exits 8 (\"Option not found\") before emitting a byte", flag, tc.name)
					}
				}
			}
		})
	}
}

// TestRealFfmpegServesNonHTTPSource runs the REAL ffmpeg over the argv the
// daemon actually builds, for a non-HTTP source. This is the end of the chain
// the stand-in cannot check: whether ffmpeg accepts the options at all.
func TestRealFfmpegServesNonHTTPSource(t *testing.T) {
	ffbin, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("no real ffmpeg on PATH")
	}
	dir := t.TempDir()
	tsPath := filepath.Join(dir, "src.ts")
	// Let ffmpeg itself make a TS it will certainly accept back.
	gen := exec.Command(ffbin, "-nostdin", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=size=64x64:rate=5:duration=2",
		"-c:v", "mpeg2video", "-f", "mpegts", "-y", tsPath)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Skipf("could not synthesise a TS fixture with this ffmpeg build: %v: %s", err, out)
	}

	for _, raw := range []string{tsPath, "file://" + tsPath} {
		t.Run(raw, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var got []byte
			err := convert(ctx, Source{
				URLs:      []string{raw},
				FfmpegBin: ffbin,
				Backend:   BackendFfmpeg,
				Cookie:    "sid=abc",
				Proxy:     "127.0.0.1:3128",
				Headers:   []string{"X-Token: shh"},
				Label:     "t",
			}, raw, nil, 12032, func(b []byte) { got = append(got, b...) })
			if err != nil && err != io.EOF {
				t.Fatalf("real ffmpeg refused a %s source: %v", raw, err)
			}
			if len(got) == 0 {
				t.Fatalf("real ffmpeg delivered 0 bytes for %s — the channel is off air", raw)
			}
			if got[0] != 0x47 {
				t.Fatalf("first byte 0x%02x, want a TS sync byte", got[0])
			}
		})
	}
}
