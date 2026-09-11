// Package remux is `xc_fanout remux`: the native replacement for the ffmpeg
// process XC_VM runs for a copy-only live stream.
//
// The panel composes this command line exactly as it composes an ffmpeg one
// (StreamProcess::buildNativeLive next to buildLive) and hands it to the
// daemon's supervisor, which runs and watches it like any other encoder. It
// produces the same two outputs the panel's ffmpeg tee does:
//
//   - the on-disk HLS the rest of the panel reads (<id>_.m3u8 + <id>_<n>.ts,
//     for the tv_archive worker, loopback children and the start checks), cut
//     by internal/tsseg — P2PTV's passthrough segmenter;
//   - the MPEG-TS feed into the daemon's ingest socket, from which the daemon
//     fans the stream out to viewers.
//
// The source is read by internal/nativesrc (P2PTV's source layer): MPEG-TS over
// http(s), HLS with TS segments, udp/rtp. Bytes pass through unchanged — no
// demux of the codec payload, no re-encode — which is the whole point: a
// channel that only needs copying costs a goroutine's worth of CPU instead of
// an ffmpeg.
//
// A source it cannot serve (fMP4 or encrypted HLS, rtmp/srt, no detectable
// keyframes) ends the run with ErrUnsupported, which the command turns into
// supervisor.ExitUnsupported so the supervisor can switch that source to the
// ffmpeg fallback the panel supplied. Anything else — the upstream going away —
// is an ordinary failure, retried and failed over like ffmpeg's.
package remux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/nativesrc"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsseg"
)

// ErrUnsupported reports that the source cannot be served natively at all.
var ErrUnsupported = errors.New("remux: source cannot be served natively")

// ErrSourceEnded reports that a live source closed its stream. For a live
// channel that is a failure like any other: the supervisor restarts it.
var ErrSourceEnded = errors.New("remux: source ended")

// Config is one run.
type Config struct {
	Input  string            // source URL
	Source nativesrc.Options // how the source is reached
	Seg    tsseg.Config      // where segments go
	// IngestSock is the daemon's per-stream ingest socket ("" = no daemon feed).
	IngestSock string
	// ProgressPath receives ffmpeg-style progress blocks ("" = none), which the
	// panel's streams cron reads for the stream's speed and frame rate.
	ProgressPath string
	// IdleTimeout ends a source that delivers no bytes for this long, so a
	// half-open upstream becomes a failure the supervisor acts on rather than a
	// silent hang. 0 = nativesrc.DefaultSourceIdleTimeout.
	IdleTimeout time.Duration
	Logf        func(format string, args ...any) // nil = silent
}

// Run reads the source and produces both outputs until ctx is cancelled (nil)
// or the source fails (an error; ErrUnsupported when it can never work).
func Run(ctx context.Context, cfg Config) error {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = nativesrc.DefaultSourceIdleTimeout
	}
	if err := checkScheme(cfg.Input); err != nil {
		return err
	}
	if cfg.Seg.Logf == nil {
		cfg.Seg.Logf = cfg.Logf
	}
	seg, err := tsseg.New(cfg.Seg)
	if err != nil {
		return err
	}
	defer seg.Close()

	src, err := nativesrc.Open(ctx, cfg.Input, cfg.Source)
	if err != nil {
		return classify(ctx, err)
	}
	src = nativesrc.WrapIdleTimeout(src, cfg.IdleTimeout)
	defer src.Close()

	var feed *ingestWriter
	if cfg.IngestSock != "" {
		feed = newIngestWriter(cfg.IngestSock, cfg.Logf)
		go feed.run(ctx)
	}
	var prog *progress
	if cfg.ProgressPath != "" {
		prog = newProgress(cfg.ProgressPath)
	}

	err = pump(ctx, src, seg, feed, prog)
	if feed != nil {
		feed.close()
	}
	return classify(ctx, err)
}

// pump moves packets from the source to the outputs: every packet to the
// segmenter, and each read's worth of packets to the daemon as one chunk.
func pump(ctx context.Context, src io.Reader, seg *tsseg.Segmenter, feed *ingestWriter, prog *progress) error {
	var (
		al      aligner
		buf     = make([]byte, 64<<10)
		lastRep time.Time
	)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			var ferr error
			out := al.push(buf[:n], func(pkt []byte) {
				if ferr == nil {
					ferr = seg.Feed(pkt)
				}
			})
			if ferr != nil {
				return ferr
			}
			if len(out) > 0 {
				if feed != nil {
					feed.send(out)
				}
				if prog != nil {
					prog.add(len(out))
				}
			}
			if prog != nil {
				if now := time.Now(); now.Sub(lastRep) >= time.Second {
					lastRep = now
					prog.report(seg.Stats(), false)
				}
			}
		}
		if rerr != nil {
			if prog != nil {
				prog.report(seg.Stats(), true)
			}
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(rerr, io.EOF) {
				return ErrSourceEnded
			}
			return rerr
		}
	}
}

// classify turns an error into one of the run's outcomes.
func classify(ctx context.Context, err error) error {
	switch {
	case err == nil || ctx.Err() != nil:
		return nil
	case nativesrc.IsFormat(err), errors.Is(err, tsseg.ErrNoKeyframe):
		return fmt.Errorf("%w: %v", ErrUnsupported, err)
	}
	return err
}

// checkScheme refuses up front what nativesrc cannot read, so the fallback is
// reached without a network round trip. A bare path is refused too: nativesrc
// would hand a local MP4 through as raw bytes.
func checkScheme(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" {
		return fmt.Errorf("%w: input %q is not a URL", ErrUnsupported, raw)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "udp", "rtp":
		return nil
	}
	return fmt.Errorf("%w: scheme %q", ErrUnsupported, u.Scheme)
}

// aligner turns arbitrary reads into whole 188-byte packets, resynchronising
// on a lost sync byte. A source's bytes are not guaranteed to arrive aligned —
// an RTP header, a truncated datagram, a segment boundary with junk in it — and
// the daemon's ingest and the segmenter both assume alignment.
//
// The rule is P2PTV's (readTSPacket): while in sync, a packet that starts with
// 0x47 is taken as it is; once sync is lost, a candidate 0x47 must be confirmed
// by the next packet's before it is believed, because a lone 0x47 inside a
// payload is not a sync byte.
type aligner struct {
	carry  []byte
	out    []byte
	synced bool
}

// push appends b, calls onPkt for each whole packet found, and returns those
// packets contiguously (valid until the next push).
func (a *aligner) push(b []byte, onPkt func([]byte)) []byte {
	a.carry = append(a.carry, b...)
	a.out = a.out[:0]
	i := 0
	for i+tspes.PacketSize <= len(a.carry) {
		if a.carry[i] != 0x47 {
			a.synced = false
			i++
			continue
		}
		if !a.synced {
			next := i + tspes.PacketSize
			if next >= len(a.carry) {
				break // confirm once the next packet's first byte arrives
			}
			if a.carry[next] != 0x47 {
				i++
				continue
			}
			a.synced = true
		}
		pkt := a.carry[i : i+tspes.PacketSize]
		onPkt(pkt)
		a.out = append(a.out, pkt...)
		i += tspes.PacketSize
	}
	// Keep the unconsumed tail (at most one partial packet plus unsynced bytes).
	a.carry = append(a.carry[:0], a.carry[i:]...)
	return a.out
}
