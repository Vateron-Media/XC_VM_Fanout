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
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/ingest"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/nativesrc"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/redact"
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
	dlog.Logf("puller", "id=%s start; urls=%v proxy=%q", src.Label, redact.URLs(src.URLs), proxyForLog(src.Proxy))

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
		// How long the attempt itself ran, measured before the wait below can be
		// folded into it: a pull that streamed for 55s and failed is not a
		// healthy minute just because an 8s backoff was added to it.
		ran := time.Since(start)
		// A pull that stayed up is a NEW incident, not one more retry of the
		// last one, so its first reconnect is the fast one. Deciding this only
		// after the wait armed the reset for the NEXT failure instead: a stream
		// that flapped in the morning, climbed to the 8s ceiling and then
		// streamed cleanly for hours still opened its next incident with the
		// full 8s of dead air — exactly what the reset was added to remove.
		if ran > backoffResetAfter {
			backoff = defaults.PullBackoffInitial
		}
		if ctx.Err() == nil {
			if err != nil && !errors.Is(err, io.EOF) {
				log.Printf("puller: id=%s %v (retry in %s)", src.Label, err, backoff)
			} else {
				// A source that simply ended (an upstream rotating its encoder, a
				// playlist that ran out) is not a fault. ingest.Copy returns the
				// reader's error and never nil, so a clean end arrives here as
				// io.EOF — which is why the old "err == nil" branch below was
				// unreachable and every clean end was logged as the bare error
				// "EOF", once per reconnect, forever. The daemon still
				// reconnects: live sources are not supposed to end, so say so in
				// the operator log, just not as a failure.
				log.Printf("puller: id=%s source ended after %s (retry in %s)", src.Label, ran.Round(time.Millisecond), backoff)
			}
		}
		if ctx.Err() != nil {
			dlog.Logf("puller", "id=%s stop (context cancelled)", src.Label)
			return
		}
		if !waitBackoff(ctx, backoff) {
			dlog.Logf("puller", "id=%s stop (context cancelled)", src.Label)
			return
		}
		// And double for the attempt after this one. ran is the attempt's own
		// duration, so the reset above and this one agree: a source that just
		// came back holds the initial wait for one more try before climbing.
		backoff = nextBackoff(backoff, ran)
	}
}

// waitBackoff waits d before the next attempt, cut short when ctx ends; it
// reports whether the wait ran to completion. A var so a test can read the
// reconnect loop's decisions without spending the seconds they take.
var waitBackoff = func(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// backoffResetAfter is how long a pull must have run for its failure to count
// as a new incident rather than another retry of the last one. A var so a test
// can compress a healthy run.
var backoffResetAfter = time.Minute

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

// ffmpegWaitDelay bounds how long cmd.Wait may spend after the child has been
// told to go: long enough for an ffmpeg that is flushing on SIGKILL, short
// enough that a wedged descendant costs one reconnect rather than the stream.
const ffmpegWaitDelay = 5 * time.Second

// ffmpegStallBound is how long the ffmpeg path may deliver nothing before it is
// treated as stalled: the native path's bound for a continuous source, and room
// for a segment-at-a-time HLS source, which ffmpeg also reads in bursts. It is
// the ffmpeg-path twin of nativesrc.hlsIdleBound.
func ffmpegStallBound(hls bool) time.Duration {
	if hls {
		return 3 * nativesrc.DefaultSourceIdleTimeout
	}
	return nativesrc.DefaultSourceIdleTimeout
}

// isHLSSource answers the only question the stall bound depends on: will ffmpeg
// read this source a segment at a time, and so be legitimately silent between
// bursts? Three independent signals, any one of which settles it:
//
//   - a ".m3u8" in the configured URL;
//   - the content-type we were actually served, for the very common playlist
//     under an arbitrary path extension (nativesrc.AdoptHTTP classifies by
//     content-type and by sniffing, never by extension);
//   - the native reader's own refusal, which names an HLS source whenever it
//     declines one — and those refusals are precisely why the ffmpeg fallback
//     gets HLS sources at all.
//
// Guessing from the URL text alone gave an extensionless HLS source the 8s
// continuous bound and killed it between two healthy segments.
func isHLSSource(raw string, resp *http.Response, refusal error) bool {
	if strings.Contains(strings.ToLower(raw), ".m3u8") {
		return true
	}
	if resp != nil && strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "mpegurl") {
		return true
	}
	return isHLSRefusal(refusal)
}

// hlsRefusals are every sentinel nativesrc.servable() can refuse a playlist
// with. They are listed in one place because the list is the contract: each one
// means "this IS an HLS source, just not one I will serve", so each one has to
// earn the segment-at-a-time stall bound on the fallback. Two of the four were
// added to nativesrc after the classifier was written and nobody came back
// here, so a byte-range or packed-audio source under an extensionless path was
// still given the 8s continuous bound.
var hlsRefusals = []error{
	nativesrc.ErrHLSIsFMP4,
	nativesrc.ErrHLSEncrypted,
	nativesrc.ErrHLSByteRange,
	nativesrc.ErrHLSNotTS,
}

// isHLSRefusal reports whether err is the native reader declining an HLS source
// for what it IS. hls.go wraps these before closing the pipe with them, so the
// test has to be errors.Is, never equality.
func isHLSRefusal(err error) bool {
	if err == nil {
		return false
	}
	for _, s := range hlsRefusals {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
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
			dlog.Logf("puller", "id=%s non-http source, skipping probe: %s", src.Label, redact.URL(raw))
			// A native-only URL is a CANDIDATE like any other. This branch used
			// to return the attempt's error, so a stream whose first URL was
			// udp://, rtp:// or a path never reached its configured backups:
			// Run always restarts pullOnce at URLs[0], so a node with no route
			// to the group, or a missing file, stayed off air forever with a
			// healthy HTTP backup sitting untouched in its config — while the
			// same config with an HTTP primary failed over on the first probe.
			//
			// Fall through only when the attempt produced NOTHING, which is
			// this path's equivalent of a failed probe. A source that was on
			// air and then ended keeps the stream: rotating on that would
			// migrate a channel off its primary after one hiccup hours into a
			// healthy run, and Run's backoff returns to URLs[0] anyway.
			delivered := false
			err := convert(ctx, src, raw, nil, chunkSize, func(b []byte) {
				delivered = true
				publish(b)
			})
			if err == nil || delivered || ctx.Err() != nil {
				return err
			}
			dlog.Logf("puller", "id=%s source delivered nothing: %s: %v", src.Label, redact.URL(raw), err)
			lastErr = err
			continue
		}
		resp, err := probe(ctx, client, src, raw)
		if err != nil {
			dlog.Logf("puller", "id=%s probe failed: %s: %v", src.Label, redact.URL(raw), err)
			lastErr = err
			continue
		}
		dlog.Logf("puller", "id=%s probe %s: HTTP %d content-type=%q", src.Label, redact.URL(raw), resp.StatusCode, resp.Header.Get("Content-Type"))
		if isMP2T(resp) {
			src.reportPath(PathDirectMP2T)
			// Bound a stall: without this a source that goes half-open (stops
			// sending but never closes the socket) blocked ingest.Copy's Read
			// forever. No error means no reconnect, so the channel was silently
			// dead while the panel still saw a running stream with a live puller.
			body := nativesrc.WrapIdleTimeout(resp.Body, nativesrc.DefaultSourceIdleTimeout)
			defer body.Close()
			dlog.Logf("puller", "id=%s connected direct mpegts: %s", src.Label, redact.URL(raw))
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

// isHTTPScheme reports whether raw is an http/https URL, i.e. whether ffmpeg
// will open it with the HTTP protocol and so accept that protocol's options.
// Anything else — udp, rtp, file, a bare path, a URL that will not even parse —
// answers false, because the cost of being wrong that way is one missing header
// and the cost of being wrong the other way is a child that refuses to start.
func isHTTPScheme(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
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
		// Host is the one header net/http will not take from the header map: it
		// sends URL.Host and Request.Host is the only override. ffmpeg's
		// -headers block DOES honour a Host line, so leaving this out made the
		// probe and the native reader ask a different vhost than the ffmpeg
		// fallback — and the probe could 404 a URL out of the candidate list
		// before ffmpeg was ever given it.
		if strings.EqualFold(name, "Host") {
			req.Host = strings.TrimSpace(value)
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
	pu, err := proxyURL(src.Proxy)
	if err != nil {
		return nil, err
	}
	if pu != nil {
		tr.Proxy = http.ProxyURL(pu)
	}
	return &http.Client{Transport: tr}, nil // no client timeout: this is a long-lived stream
}

// proxyURL turns the configured proxy value into the address the transport and
// the ffmpeg child both dial, or nil when no proxy is configured.
//
// The panel's field is free text, documented as host:port, and the daemon used
// to prefix "http://" unconditionally. "http://10.0.0.5:3128" — the value an
// operator naturally types — then PARSES, as the host "http:" with the path
// "//10.0.0.5:3128": every probe died with `dial tcp: lookup http:: no such
// host`, an error naming neither the proxy nor the stream, and ffmpeg was
// handed `-http_proxy http://http://10.0.0.5:3128` besides. Accept the scheme
// when it is there, add it when it is not, and keep only the address.
func proxyURL(raw string) (*url.URL, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return nil, nil
	}
	if low := strings.ToLower(v); !strings.HasPrefix(low, "http://") && !strings.HasPrefix(low, "https://") {
		v = "http://" + v
	}
	u, err := url.Parse(v)
	if err != nil {
		return nil, fmt.Errorf("proxy %q: %w", redact.URL(raw), err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("proxy %q has no host:port", redact.URL(raw))
	}
	// A proxy is an address to dial, never a document to fetch: anything after
	// the host is at best noise and at worst part of the dialled name.
	u.Path, u.RawPath, u.RawQuery, u.Fragment = "", "", "", ""
	return u, nil
}

// probe opens the URL. On success the whole response, headers AND an unread,
// still-open body, is returned for the caller to classify (isMP2T), stream, or
// hand to nativesrc.AdoptHTTP. Returning the response rather than just the body
// is what lets a non-mp2t source be adopted instead of re-fetched.
func probe(ctx context.Context, c *http.Client, src Source, raw string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, redactRequestError(err)
	}
	req.Header.Set("User-Agent", src.ua())
	if src.Cookie != "" {
		req.Header.Set("Cookie", src.Cookie)
	}
	applyHeaders(req, src.Headers)
	resp, err := c.Do(req)
	if err != nil {
		return nil, redactRequestError(err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("%s: HTTP %d", redact.URL(raw), resp.StatusCode)
	}
	return resp, nil
}

// redactRequestError masks the credentials net/http itself put in an error:
// every failure out of Client.Do is a *url.Error carrying the request URL
// verbatim ("Get \"http://host/live/user/pass/1.ts\": dial tcp …"), and that
// error is what Run prints on every reconnect.
func redactRequestError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		ue.URL = redact.URL(ue.URL)
	}
	return err
}

// proxyForLog is the configured proxy as a log line may carry it. A proxy value
// holds a password as often as a source URL does, and the shared redactor can
// only see userinfo in something shaped like a URL — "user:pass@host:3128" on
// its own has no scheme for it to find the authority behind.
func proxyForLog(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	if u, err := proxyURL(raw); err == nil && u != nil {
		return redact.URL(u.String())
	}
	return redact.URL(raw)
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
		// Classify BEFORE the body goes: resp's content-type is the only thing
		// that can tell an extensionless playlist from a continuous source.
		stall := ffmpegStallBound(isHLSSource(raw, resp, nil))
		closeBody(resp)
		src.reportPath(PathFfmpegPin)
		dlog.Logf("puller", "id=%s connected via ffmpeg remux (backend=ffmpeg): %s", src.Label, redact.URL(raw))
		return runFfmpeg(ctx, src, raw, stall, chunkSize, publish)
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
		src.reportPath(PathNative)
		dlog.Logf("puller", "id=%s connected native (no ffmpeg child): %s", src.Label, redact.URL(raw))
		copyErr := ingest.Copy(rc, chunkSize, publish)
		// Closed here rather than deferred: the fallback below outlives this
		// block, and the wrapper is a goroutine and a ticker that stop only on
		// Close — one left watching a dead pipe for the whole of an ffmpeg
		// session is the leak the stdout bound was already fixed for.
		rc.Close()
		// The native reader refuses a source for what it IS at two different
		// moments. AdoptHTTP/Open refuses before a byte moves, and that refusal
		// is handled below. The HLS puller refuses AFTER the pull has started —
		// a segment whose body turns out not to be MPEG-TS, a live playlist
		// that changes flavour mid-life — and that one arrives here instead, as
		// the pipe's error out of ingest.Copy. It used to be returned to Run(),
		// which only logs and reconnects, so such a source looped connect →
		// refuse → back off forever with nothing published, and nothing
		// recorded that it had been format-refused at all. Give it the same
		// fallback the refusal at open gets: the rest of this attempt runs on
		// ffmpeg, and the next reconnect tries native again, exactly as it does
		// for a source refused at open.
		if ctx.Err() != nil || src.Backend == BackendNative || !nativesrc.IsFormat(copyErr) {
			return copyErr
		}
		src.reportPath(PathFfmpegBack)
		dlog.Logf("puller", "id=%s native refused the source mid-pull (%v); falling back to ffmpeg: %s", src.Label, copyErr, redact.URL(raw))
		return runFfmpeg(ctx, src, raw, ffmpegStallBound(isHLSSource(raw, resp, copyErr)), chunkSize, publish)
	}

	if src.Backend == BackendNative {
		// The operator asked for native only — surfacing the refusal is the
		// point, so Run() backs off and retries rather than silently doing the
		// thing they turned off.
		return fmt.Errorf("native source declined (backend=native, no fallback): %w", err)
	}
	src.reportPath(PathFfmpegBack)
	dlog.Logf("puller", "id=%s native declined (%v); falling back to ffmpeg: %s", src.Label, err, redact.URL(raw))
	// The refusal itself is a classification: every one of nativesrc's HLS
	// refusals says "this IS an HLS source, just not one I will serve", which is
	// the case the ffmpeg fallback exists for and the case that needs the wider
	// bound. See hlsRefusals.
	return runFfmpeg(ctx, src, raw, ffmpegStallBound(isHLSSource(raw, resp, err)), chunkSize, publish)
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
// Mirrors the ffmpeg invocation in ProxyCommand.php. stall is how long the child
// may deliver nothing before it is treated as stalled; the caller picks it,
// because only the caller knows whether this source arrives in bursts.
func runFfmpeg(ctx context.Context, src Source, raw string, stall time.Duration, chunkSize int, publish func([]byte)) error {
	bin := src.FfmpegBin
	if bin == "" {
		bin = "ffmpeg"
	}
	args := []string{
		// -loglevel error (not quiet): ffmpeg only speaks up on a genuine error,
		// which we capture from stderr and log — the mpegts byte stream is on
		// stdout, a separate pipe, so this never pollutes it.
		"-copyts", "-vsync", "0", "-nostats", "-nostdin", "-hide_banner",
		"-loglevel", "error", "-y",
		// Cold-start bounds (ADR 0003, Phase C1a): cap input analysis so the first
		// mpegts bytes appear quickly on a cold on-demand join, instead of ffmpeg
		// spending its default 5s/5MB probing the source. 1s/1MB still identifies
		// the PAT/PMT + codecs a live TS/HLS source presents. These are
		// AVFormatContext options, so they are valid for every input. Input
		// options — must precede -i.
		"-probesize", defaults.PullFfmpegProbeSize, "-analyzeduration", defaults.PullFfmpegAnalyzeDuration,
	}
	// Everything below is declared by the http/https PROTOCOL, not by ffmpeg
	// itself, and ffmpeg treats an input option no protocol consumed as fatal:
	// `-user_agent ... -i udp://…` prints "Option user_agent not found." and
	// exits 8 with nothing on stdout. Passing them unconditionally made the
	// ffmpeg backend dead for every udp://, rtp:// and file:// source — the
	// documented kill-switch and the auto fallback both died in under 100ms and
	// were retried forever. Gate them on the scheme actually being HTTP.
	if isHTTPScheme(raw) {
		args = append(args, "-user_agent", src.ua(),
			// HTTP reconnect (as the panel's own ffmpeg uses) rides out a
			// transient fetch hiccup during warm-up without dropping the pull.
			"-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5")
		// ffmpeg takes ONE -headers value, so the cookie and any extra headers
		// have to be folded into a single CRLF-separated block. Passing -headers
		// twice silently keeps only the last, which would drop whichever the
		// panel cared about more.
		if hdr := ffmpegHeaderBlock(src); hdr != "" {
			args = append(args, "-headers", hdr)
		}
		// The same normalised address the probe dials — see proxyURL. A value
		// too broken to be one is dropped here rather than passed on: Run has
		// already refused to start with it, so this can only be a direct caller.
		if pu, err := proxyURL(src.Proxy); err == nil && pu != nil {
			args = append(args, "-http_proxy", pu.String())
		}
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
	// Give the command its OWN process group and kill the whole group, not just
	// the direct child. FfmpegBin is operator-supplied and a wrapper script (a
	// ulimit shim, cpulimit, a logging wrapper) is an ordinary deployment: then
	// the daemon's child is the shell and ffmpeg is its grandchild. Killing only
	// the shell left ffmpeg orphaned, still holding the inherited stdout pipe and
	// still pulling the provider — so ingest.Copy below never saw EOF, runFfmpeg
	// never returned, and every stopped stream leaked a puller goroutine and a
	// source connection that nothing would ever close. Same shape as
	// internal/supervisor's encoder launcher, for the same reason.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				// The group is already gone — the ordinary case when the source
				// ended and ffmpeg exited before our own cancel reached it.
				// os/exec turns any other error from here into Wait's result, so
				// say it the way it expects and keep a clean end clean.
				return os.ErrProcessDone
			}
			return cmd.Process.Kill()
		}
		return nil
	}
	// And a backstop for anything the group kill cannot reach. cmd.Stderr is a
	// tailBuffer, not an *os.File, so os/exec makes its own pipe and a copy
	// goroutine, and Wait blocks until every write end is closed — which a
	// descendant that gave itself a new session still holds. With no WaitDelay
	// that wait is unbounded; with one, Wait gives up and the puller reconnects.
	cmd.WaitDelay = ffmpegWaitDelay
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
	//
	// The wrapper is a goroutine and a ticker, and it stops only when it is
	// Closed or its bound fires. Passed inline it was never closed, so every
	// attempt — the ordinary clean end included — left one watching a dead pipe
	// for the whole bound (8s, 24s for an HLS source). Close it on the way out,
	// AFTER cmd.Wait: closing the read end while the child is still writing
	// would hand ffmpeg an EPIPE and turn a clean end into a fault.
	stdoutBounded := nativesrc.WrapIdleTimeout(stdout, stall)
	defer stdoutBounded.Close()
	copyErr := ingest.Copy(stdoutBounded, chunkSize, publish)
	ccancel()
	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		return copyErr // we cancelled it (stream stop/shutdown) — not a fault
	}
	// A context.Canceled out of Wait is our own teardown, never ffmpeg's doing.
	// os/exec reports ctx.Err() whenever the Cancel func above says it
	// successfully interrupted the command, and a process-group kill says that
	// even when the child has already exited and is only waiting to be reaped —
	// so the ordinary clean source end (ffmpeg exits 0, ccancel fires, Wait
	// reaps) would otherwise surface as a phantom "ffmpeg: context canceled"
	// fault and be backed off on. ccancel() always runs before Wait and parent
	// cancellation is handled just above, so there is no other way to get here;
	// a real ffmpeg failure is a non-zero *ExitError and is still caught below.
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
