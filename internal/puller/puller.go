// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package puller acquires a live source and feeds it into a publish callback as
// MPEG-TS, reconnecting with backoff. It ports the source-selection logic of the
// legacy ProxyCommand::getActiveStream: a source served as video/mp2t is
// streamed directly; anything else (HLS/other) is remuxed to MPEG-TS by ffmpeg.
package puller

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/ingest"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/nativesrc"
)

// tailBuffer keeps only the last max bytes written to it — a bounded sink for a
// child process's stderr, so a chatty ffmpeg can never grow memory without bound
// while we still keep the most recent lines to explain why it exited.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(string(t.buf))
}

// Source describes where and how to pull a live stream.
type Source struct {
	URLs      []string // candidate source URLs, tried in order
	UserAgent string
	Proxy     string // "host:port" HTTP proxy, optional
	Cookie    string // optional Cookie header value
	FfmpegBin string // ffmpeg path; "ffmpeg" if empty
	Label     string // stream id, for debug logging only (no effect on behaviour)
	Insecure  bool   // skip upstream TLS certificate verification (see -source-insecure)
	// Headers are extra request headers as raw "Key: value" lines, for an
	// upstream that needs more than a User-Agent and a Cookie to answer. A
	// source fetched without the headers it was configured with is a DIFFERENT
	// source — it may 403, or serve something else entirely — so these travel
	// down every path: the probe, the native reader and the ffmpeg child.
	Headers []string
	// Backend selects how a NON-mp2t source becomes MPEG-TS: BackendAuto,
	// BackendFfmpeg or BackendNative. Empty means BackendAuto. A direct mp2t
	// source ignores it entirely — that path never needed a converter.
	Backend string
	// OnPath, when set, is called with a short label for the route each connect
	// attempt settles on (see the Path* constants). It is how the daemon can
	// report whether a stream is ACTUALLY running native or on ffmpeg, rather
	// than only what was configured — the question the source backend setting
	// exists to answer and which nothing else can answer after the fact.
	// Called from the pull goroutine; keep it cheap and non-blocking.
	OnPath func(path string)
}

// The routes a connect attempt can settle on, as reported through
// Source.OnPath. Short and stable: they show up in the debug snapshot and in
// GET /streams/<id>, and operators compare them against the configured backend.
const (
	PathDirectMP2T = "direct-mp2t"     // served as video/mp2t; no converter involved
	PathNative     = "native"          // converted in-process by nativesrc
	PathFfmpegPin  = "ffmpeg-pinned"   // backend=ffmpeg, native never tried
	PathFfmpegBack = "ffmpeg-fallback" // native declined the source; ffmpeg took it
)

// reportPath tells the owner which route this attempt took, if it is listening.
func (s Source) reportPath(p string) {
	if s.OnPath != nil {
		s.OnPath(p)
	}
}

// How a non-mp2t source is converted. The panel sets this globally through the
// config file and may override it per stream, so one troublesome channel can be
// pinned to ffmpeg without changing the node.
const (
	// BackendAuto tries the native reader and falls back to ffmpeg for anything
	// it declines. Strictly safer than ffmpeg-always: a declined source runs the
	// exact pipeline it ran before.
	BackendAuto = "auto"
	// BackendFfmpeg always spawns ffmpeg — the pre-0.12 behaviour, kept as the
	// kill-switch.
	BackendFfmpeg = "ffmpeg"
	// BackendNative refuses to fall back: a source the native reader declines
	// fails instead of growing an ffmpeg child. The panel applies the same
	// meaning to its own streams — on a native node they are produced by
	// `xc_fanout remux` with no ffmpeg fallback — so a declined source there is
	// a dead channel rather than a slightly more expensive one.
	BackendNative = "native"
)

func (s Source) ua() string {
	if s.UserAgent == "" {
		return "Mozilla/5.0"
	}
	return s.UserAgent
}

// Run pulls src into publish until ctx is cancelled, reconnecting with capped
// exponential backoff whenever the source ends or errors.
func Run(ctx context.Context, src Source, chunkSize int, publish func([]byte)) {
	dlog.Logf("puller", "id=%s start; urls=%v proxy=%q", src.Label, src.URLs, src.Proxy)

	// One client — and so one connection pool — for the whole puller, not one per
	// probe. A fresh http.Transport per attempt meant no connection was ever
	// reused (a full TCP + TLS handshake on every reconnect) and, worse, each
	// abandoned transport kept whatever it had pooled: a source that ended
	// cleanly had its connection returned to that pool, where — a zero-value
	// transport having no idle timeout — it stayed open with its reader goroutine
	// forever, unreachable because the transport itself was garbage. A source
	// reconnecting on the 8 s backoff ceiling leaked a socket and two goroutines
	// every 8 s. CloseIdleConnections on the way out returns the rest.
	client, cerr := httpClient(src)
	if cerr != nil {
		log.Printf("puller: id=%s %v", src.Label, cerr)
		return
	}
	defer client.CloseIdleConnections()

	backoff := defaults.PullBackoffInitial
	for ctx.Err() == nil {
		start := time.Now()
		err := pullOnce(ctx, client, src, chunkSize, publish)
		if err != nil && ctx.Err() == nil {
			log.Printf("puller: id=%s %v (retry in %s)", src.Label, err, backoff)
		} else if ctx.Err() == nil {
			// A pullOnce that returned nil/EOF means the source ended cleanly; the
			// daemon still reconnects (live sources are not supposed to end).
			dlog.Logf("puller", "id=%s source ended after %s (retry in %s)", src.Label, time.Since(start).Round(time.Millisecond), backoff)
		}
		if ctx.Err() != nil {
			dlog.Logf("puller", "id=%s stop (context cancelled)", src.Label)
			return
		}
		select {
		case <-ctx.Done():
			dlog.Logf("puller", "id=%s stop (context cancelled)", src.Label)
			return
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff, time.Since(start))
	}
}

// backoffResetAfter is how long a pull must have run for its failure to count
// as a new incident rather than another retry of the last one.
const backoffResetAfter = time.Minute

// nextBackoff is the wait before the next attempt, after one that ran for
// ranFor. It doubles up to PullBackoffMax while failures follow each other, and
// starts over after a long healthy run — doubling forever put every reconnect
// at the full 8 s after a stream's third blip in its lifetime.
func nextBackoff(cur, ranFor time.Duration) time.Duration {
	if ranFor > backoffResetAfter {
		return defaults.PullBackoffInitial
	}
	if cur < defaults.PullBackoffMax {
		return cur * 2
	}
	return cur
}

// ffmpegStallBound is how long the ffmpeg path may deliver nothing before it is
// treated as stalled: the native path's bound, and room for a segment-at-a-time
// HLS source, which ffmpeg also reads in bursts.
func ffmpegStallBound(raw string) time.Duration {
	if strings.Contains(strings.ToLower(raw), ".m3u8") {
		return 3 * nativesrc.DefaultSourceIdleTimeout
	}
	return nativesrc.DefaultSourceIdleTimeout
}

// pullOnce tries each URL once: mp2t is streamed directly, anything else goes
// through convert(). Returns when the chosen source ends or errors.
func pullOnce(ctx context.Context, client *http.Client, src Source, chunkSize int, publish func([]byte)) error {
	var lastErr error
	for _, raw := range src.URLs {
		// A scheme an HTTP GET cannot speak must not be probed with one. Every
		// udp://, rtp:// and file:// source used to fail here with "unsupported
		// protocol scheme", be counted as a probe failure and be skipped, so the
		// native reader's support for them (and ffmpeg's) was unreachable through
		// the daemon no matter what the operator configured.
		if nativeOnlyScheme(raw) {
			dlog.Logf("puller", "id=%s non-http source, skipping probe: %s", src.Label, raw)
			return convert(ctx, src, raw, nil, chunkSize, publish)
		}
		resp, err := probe(ctx, client, src, raw)
		if err != nil {
			dlog.Logf("puller", "id=%s probe failed: %s: %v", src.Label, raw, err)
			lastErr = err
			continue
		}
		dlog.Logf("puller", "id=%s probe %s: HTTP %d content-type=%q", src.Label, raw, resp.StatusCode, resp.Header.Get("Content-Type"))
		if isMP2T(resp) {
			src.reportPath(PathDirectMP2T)
			// Bound a stall: without this a source that goes half-open (stops
			// sending but never closes the socket) blocked ingest.Copy's Read
			// forever. No error means no reconnect, so the channel was silently
			// dead while the panel still saw a running stream with a live puller.
			body := nativesrc.WrapIdleTimeout(resp.Body, nativesrc.DefaultSourceIdleTimeout)
			defer body.Close()
			dlog.Logf("puller", "id=%s connected direct mpegts: %s", src.Label, raw)
			return ingest.Copy(body, chunkSize, publish)
		}
		// Hand the OPEN response to convert rather than closing it and fetching
		// the same URL a second time. See nativesrc.AdoptHTTP.
		return convert(ctx, src, raw, resp, chunkSize, publish)
	}
	if lastErr == nil {
		lastErr = errors.New("no source urls")
	}
	return lastErr
}

// nativeOnlyScheme reports whether raw names a source that must bypass the HTTP
// probe. It mirrors nativesrc.Open's own dispatch, deliberately: anything else,
// a malformed URL included, keeps going through probe, so a bad entry is still
// skipped in favour of the next URL instead of being handed to a converter.
func nativeOnlyScheme(raw string) bool {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "./") || strings.HasPrefix(raw, "../") {
		return true
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "udp", "rtp", "file":
		return true
	}
	return false
}

// applyHeaders stamps raw "Key: value" lines onto a request. A line without a
// colon, or with an empty name, is skipped rather than guessed at: a malformed
// entry in a panel's per-stream settings must not become a malformed request.
func applyHeaders(req *http.Request, lines []string) {
	for _, line := range lines {
		name, value, ok := strings.Cut(line, ":")
		name = strings.TrimSpace(name)
		if !ok || name == "" {
			continue
		}
		req.Header.Set(name, strings.TrimSpace(value))
	}
}

// isMP2T reports whether a response is served as MPEG-TS.
func isMP2T(resp *http.Response) bool {
	return strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "video/mp2t")
}

func httpClient(src Source) (*http.Client, error) {
	// Upstream TLS verification is opt-in (src.Insecure, from -source-insecure).
	// The panel commonly pulls sources with self-signed or mismatched certs, so
	// the daemon defaults to skipping verification — but a deployment that pulls
	// only trusted HTTPS origins can turn it on.
	//
	// The timeouts bound everything EXCEPT the body: connect, TLS and the wait for
	// response headers each get a deadline, and idle pooled connections expire. A
	// zero-value Transport has none of these, so a source that accepted a
	// connection and then went quiet held its puller open indefinitely. The body
	// itself stays unbounded — it is a live stream, and there is no Client.Timeout
	// for the same reason.
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: src.Insecure},
		DialContext:           (&net.Dialer{Timeout: defaults.PullDialTimeout, KeepAlive: defaults.PullKeepAlive}).DialContext,
		TLSHandshakeTimeout:   defaults.PullTLSTimeout,
		ResponseHeaderTimeout: defaults.PullHeaderTimeout,
		IdleConnTimeout:       defaults.PullIdleConnTimeout,
	}
	if src.Proxy != "" {
		pu, err := url.Parse("http://" + src.Proxy)
		if err != nil {
			return nil, err
		}
		tr.Proxy = http.ProxyURL(pu)
	}
	return &http.Client{Transport: tr}, nil // no client timeout: this is a long-lived stream
}

// probe opens the URL. On success the whole response, headers AND an unread,
// still-open body, is returned for the caller to classify (isMP2T), stream, or
// hand to nativesrc.AdoptHTTP. Returning the response rather than just the body
// is what lets a non-mp2t source be adopted instead of re-fetched.
func probe(ctx context.Context, c *http.Client, src Source, raw string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", src.ua())
	if src.Cookie != "" {
		req.Header.Set("Cookie", src.Cookie)
	}
	applyHeaders(req, src.Headers)
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("%s: HTTP %d", raw, resp.StatusCode)
	}
	return resp, nil
}

// convert turns a non-mp2t source into MPEG-TS and feeds it in, natively when
// the backend allows and the native reader will take the source, else with
// ffmpeg. The native reader refuses loudly (nativesrc.ErrUnsupported) rather
// than half-serving, which is what makes the fallback safe: a source it declines
// gets exactly the ffmpeg pipeline it got before.
//
// resp, when non-nil, is an already-open response for raw that convert takes
// ownership of: the native reader adopts it instead of fetching the URL a second
// time, and every path that does not use it closes it.
func convert(ctx context.Context, src Source, raw string, resp *http.Response, chunkSize int, publish func([]byte)) error {
	opt := nativesrc.Options{
		UserAgent: src.ua(),
		Cookie:    src.Cookie,
		Proxy:     src.Proxy,
		Insecure:  src.Insecure,
		Headers:   src.Headers,
	}

	if src.Backend == BackendFfmpeg {
		closeBody(resp)
		src.reportPath(PathFfmpegPin)
		dlog.Logf("puller", "id=%s connected via ffmpeg remux (backend=ffmpeg): %s", src.Label, raw)
		return runFfmpeg(ctx, src, raw, chunkSize, publish)
	}

	// AdoptHTTP closes the body itself on every refusal, so the ffmpeg fallback
	// below never leaks it.
	var rc io.ReadCloser
	var err error
	if resp != nil {
		rc, err = nativesrc.AdoptHTTP(ctx, resp, opt)
	} else {
		rc, err = nativesrc.Open(ctx, raw, opt)
	}
	if err == nil {
		// Bound a stall, as the direct-MPEG-TS path does: a frozen upstream (a
		// live playlist that stops updating, a half-open connection) otherwise
		// left ingest.Copy blocked forever, with no retry and no failover. The
		// bound is the source's own when it is longer — a live HLS pull is
		// silent for a whole segment between bursts.
		rc = nativesrc.WrapIdleTimeout(rc, nativesrc.IdleBound(rc, nativesrc.DefaultSourceIdleTimeout))
		defer rc.Close()
		src.reportPath(PathNative)
		dlog.Logf("puller", "id=%s connected native (no ffmpeg child): %s", src.Label, raw)
		return ingest.Copy(rc, chunkSize, publish)
	}

	if src.Backend == BackendNative {
		// The operator asked for native only — surfacing the refusal is the
		// point, so Run() backs off and retries rather than silently doing the
		// thing they turned off.
		return fmt.Errorf("native source declined (backend=native, no fallback): %w", err)
	}
	src.reportPath(PathFfmpegBack)
	dlog.Logf("puller", "id=%s native declined (%v); falling back to ffmpeg: %s", src.Label, err, raw)
	return runFfmpeg(ctx, src, raw, chunkSize, publish)
}

// closeBody discards a response the chosen path will not read.
func closeBody(resp *http.Response) {
	if resp != nil {
		_ = resp.Body.Close()
	}
}

// ffmpegHeaderBlock folds the cookie and any extra headers into the single
// CRLF-terminated block ffmpeg's -headers option expects, or "" when there is
// nothing to send.
func ffmpegHeaderBlock(src Source) string {
	var b strings.Builder
	if src.Cookie != "" {
		b.WriteString("Cookie: " + src.Cookie + "\r\n")
	}
	for _, line := range src.Headers {
		name, value, ok := strings.Cut(line, ":")
		name = strings.TrimSpace(name)
		if !ok || name == "" {
			continue
		}
		b.WriteString(name + ": " + strings.TrimSpace(value) + "\r\n")
	}
	return b.String()
}

// runFfmpeg remuxes a non-mp2t source to MPEG-TS on stdout and feeds it in.
// Mirrors the ffmpeg invocation in ProxyCommand.php.
func runFfmpeg(ctx context.Context, src Source, raw string, chunkSize int, publish func([]byte)) error {
	bin := src.FfmpegBin
	if bin == "" {
		bin = "ffmpeg"
	}
	args := []string{
		// -loglevel error (not quiet): ffmpeg only speaks up on a genuine error,
		// which we capture from stderr and log — the mpegts byte stream is on
		// stdout, a separate pipe, so this never pollutes it.
		"-copyts", "-vsync", "0", "-nostats", "-nostdin", "-hide_banner",
		"-loglevel", "error", "-y", "-user_agent", src.ua(),
		// Cold-start bounds (ADR 0003, Phase C1a): cap input analysis so the first
		// mpegts bytes appear quickly on a cold on-demand join, instead of ffmpeg
		// spending its default 5s/5MB probing the source. 1s/1MB still identifies
		// the PAT/PMT + codecs a live TS/HLS source presents. HTTP reconnect (as
		// the panel's own ffmpeg uses) rides out a transient fetch hiccup during
		// warm-up without dropping the pull. Input options — must precede -i.
		"-probesize", defaults.PullFfmpegProbeSize, "-analyzeduration", defaults.PullFfmpegAnalyzeDuration,
		"-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
	}
	// ffmpeg takes ONE -headers value, so the cookie and any extra headers have
	// to be folded into a single CRLF-separated block. Passing -headers twice
	// silently keeps only the last, which would drop whichever the panel cared
	// about more.
	if hdr := ffmpegHeaderBlock(src); hdr != "" {
		args = append(args, "-headers", hdr)
	}
	if src.Proxy != "" {
		args = append(args, "-http_proxy", "http://"+src.Proxy)
	}
	args = append(args,
		"-i", raw, "-map", "0", "-c", "copy",
		"-mpegts_flags", "+initial_discontinuity", "-pat_period", "2",
		"-f", "mpegts", "-",
	)

	// Its own context, so a stalled ffmpeg can be ended without stopping the
	// stream: see the stall bound below.
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	cmd := exec.CommandContext(cctx, bin, args...)
	stderr := &tailBuffer{max: 4096}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	// Bound a stall. A half-open upstream or a frozen live playlist leaves
	// ffmpeg alive and silent — its -reconnect options only act on an error it
	// can see — and ingest.Copy then blocked forever: no retry, no failover,
	// while viewers who reconnected kept the stream referenced. Close the pipe
	// after the stall bound, then end the process before waiting on it.
	copyErr := ingest.Copy(nativesrc.WrapIdleTimeout(stdout, ffmpegStallBound(raw)), chunkSize, publish)
	ccancel()
	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		return copyErr // we cancelled it (stream stop/shutdown) — not a fault
	}
	// A context.Canceled from Wait is our own doing, not an ffmpeg fault: ccancel()
	// above always fires before Wait, and when ffmpeg ends cleanly (exit 0) at the
	// same instant, Cmd.Wait races its watchdog and can report that cancel instead
	// of the clean exit. A real ffmpeg failure surfaces as a non-zero *ExitError,
	// never as context.Canceled, so it is still caught below.
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		if tail := stderr.String(); tail != "" {
			dlog.Logf("puller", "id=%s ffmpeg exited (%v): %s", src.Label, waitErr, tail)
		} else {
			dlog.Logf("puller", "id=%s ffmpeg exited: %v", src.Label, waitErr)
		}
		// A non-zero ffmpeg exit that closed stdout cleanly would otherwise reach
		// Run() as a plain EOF and look like a normal source end; surface the real
		// cause so it is logged and backed off on, not silently retried as "ended".
		if copyErr == nil || copyErr == io.EOF {
			return fmt.Errorf("ffmpeg: %w", waitErr)
		}
	}
	return copyErr
}
