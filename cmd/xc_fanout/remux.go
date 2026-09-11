package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/nativesrc"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/remux"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/supervisor"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsseg"
)

// remuxUsage is printed for -h and for a malformed command line.
const remuxUsage = `usage: xc_fanout remux -i <url> [options] <playlist.m3u8>

Copy a live MPEG-TS source to on-disk HLS and the daemon's ingest socket, with
no ffmpeg: the native equivalent of

  ffmpeg -i <url> -c copy -f tee "[f=hls:…]<playlist>|[f=mpegts]unix:<sock>"

XC_VM composes this command (StreamProcess::buildNativeLive) and the daemon's
supervisor runs it. Exit status: 0 stopped, 1 source failed, 2 bad usage,
3 source cannot be served natively (use the ffmpeg fallback).

options:
`

// runRemux is `xc_fanout remux …`. The flags use ffmpeg's names wherever ffmpeg
// has one, because the panel builds this line beside its ffmpeg lines and an
// operator reading either should recognise the other.
func runRemux(args []string) int {
	fs := flag.NewFlagSet("remux", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), remuxUsage)
		fs.PrintDefaults()
	}
	in := fs.String("i", "", "source URL: http(s) MPEG-TS or HLS with TS segments, udp://, rtp://")
	ua := fs.String("user_agent", "", "source User-Agent")
	cookies := fs.String("cookies", "", "source Cookie header")
	proxy := fs.String("http_proxy", "", "source HTTP proxy (http://host:port or host:port)")
	headers := fs.String("headers", "", `extra source request headers, "Key: value" lines separated by CRLF or LF`)
	insecure := fs.Bool("insecure", false, "skip the source's TLS certificate verification (the panel's fanout_source_insecure)")
	ingestSock := fs.String("ingest", "", "daemon ingest socket, unix:<path>; empty = no daemon feed")
	hlsTime := fs.Int("hls_time", 10, "target segment duration, seconds")
	hlsInit := fs.Float64("hls_init_time", 0, "first segment's target duration, seconds (0 = hls_time)")
	listSize := fs.Int("hls_list_size", 6, "segments listed in the playlist")
	delThreshold := fs.Int("hls_delete_threshold", 1, "segments kept on disk after leaving the playlist")
	segName := fs.String("hls_segment_filename", "", "segment path pattern with one %d, e.g. /…/<id>_%d.ts")
	progressPath := fs.String("progress", "", "append ffmpeg-style progress reports to this file")
	idle := fs.Int("idle_timeout", 0, "seconds with no source bytes before the source counts as gone (0 = 8)")
	loglevel := fs.String("loglevel", "error", "error, warning or info")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *in == "" || fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "remux: need -i <url> and exactly one output playlist")
		fs.Usage()
		return 2
	}
	playlist := fs.Arg(0)
	pattern := *segName
	if pattern == "" {
		// ffmpeg's own default: the playlist's name with its extension replaced.
		pattern = strings.TrimSuffix(playlist, ".m3u8") + "%d.ts"
	}
	sock := ""
	if *ingestSock != "" {
		if !strings.HasPrefix(*ingestSock, "unix:") {
			fmt.Fprintf(os.Stderr, "remux: -ingest must be unix:<path>, got %q\n", *ingestSock)
			return 2
		}
		sock = strings.TrimPrefix(*ingestSock, "unix:")
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	logf := func(format string, a ...any) { logger.Printf("[remux] "+format, a...) }
	quiet := func(string, ...any) {}
	// notef is what the stream's log gets at ANY level below quiet: the source
	// summary and how the run ended, a handful of lines per process. The panel
	// points our stderr at <id>.errors, which is where an operator looks when a
	// channel misbehaves, and an empty file there answers nothing.
	notef := logf
	infof, warnf := quiet, quiet
	switch strings.ToLower(*loglevel) {
	case "info", "verbose", "debug":
		infof, warnf = logf, logf
	case "warning":
		warnf = logf
	}
	if *loglevel == "quiet" {
		logger.SetOutput(io.Discard)
	}

	cfg := remux.Config{
		Input: *in,
		Source: nativesrc.Options{
			UserAgent: *ua,
			Cookie:    *cookies,
			Proxy:     strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(*proxy, "http://"), "https://"), "/"),
			Insecure:  *insecure,
			Headers:   splitHeaders(*headers),
		},
		Seg: tsseg.Config{
			Playlist:   playlist,
			SegPattern: pattern,
			TargetSec:  *hlsTime,
			InitSec:    *hlsInit,
			ListSize:   *listSize,
			KeepExtra:  *delThreshold,
			Logf:       warnf,
		},
		IngestSock:   sock,
		ProgressPath: *progressPath,
		IdleTimeout:  time.Duration(*idle) * time.Second,
		Logf:         infof,
		Notef:        notef,
	}

	// The supervisor ends this process with SIGKILL to its group, but an
	// operator running it by hand stops it with ^C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	notef("xc_fanout %s native remuxer, pid %d", buildVersion(), os.Getpid())
	infof("starting: %s -> %s", remux.Redact(*in), playlist)
	err := remux.Run(ctx, cfg)
	switch {
	case err == nil:
		notef("stopped")
		return 0
	case errors.Is(err, remux.ErrUnsupported):
		notef("%v; handing this source back to the panel's fallback command", err)
		return supervisor.ExitUnsupported
	default:
		notef("source failed: %v", err)
		return 1
	}
}

// splitHeaders turns ffmpeg's -headers value (CRLF-separated, usually with a
// trailing CRLF) into "Key: value" lines.
func splitHeaders(v string) []string {
	var out []string
	for _, line := range strings.Split(strings.ReplaceAll(v, "\r\n", "\n"), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out = append(out, line)
		}
	}
	return out
}
