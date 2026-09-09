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
	// Backend selects how a NON-mp2t source becomes MPEG-TS: BackendAuto,
	// BackendFfmpeg or BackendNative. Empty means BackendAuto. A direct mp2t
	// source ignores it entirely — that path never needed a converter.
	Backend string
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
	// BackendNative refuses to fall back. For finding out what is actually
	// eligible on a node; not for production, where a declined source means a
	// dead channel instead of a slightly more expensive one.
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
		if backoff < defaults.PullBackoffMax {
			backoff *= 2
		}
	}
}

// pullOnce tries each URL once: mp2t is streamed directly, anything else goes
// through convert(). Returns when the chosen source ends or errors.
func pullOnce(ctx context.Context, client *http.Client, src Source, chunkSize int, publish func([]byte)) error {
	var lastErr error
	for _, raw := range src.URLs {
		isTS, body, err := probe(ctx, client, src, raw)
		if err != nil {
			dlog.Logf("puller", "id=%s probe failed: %s: %v", src.Label, raw, err)
			lastErr = err
			continue
		}
		if isTS {
			defer body.Close()
			dlog.Logf("puller", "id=%s connected direct mpegts: %s", src.Label, raw)
			return ingest.Copy(body, chunkSize, publish)
		}
		body.Close()
		return convert(ctx, src, raw, chunkSize, publish)
	}
	if lastErr == nil {
		lastErr = errors.New("no source urls")
	}
	return lastErr
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

// probe opens the URL and reports whether it is served as MPEG-TS. On success
// the returned body is left open for the caller to stream or close.
func probe(ctx context.Context, c *http.Client, src Source, raw string) (bool, io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return false, nil, err
	}
	req.Header.Set("User-Agent", src.ua())
	if src.Cookie != "" {
		req.Header.Set("Cookie", src.Cookie)
	}
	resp, err := c.Do(req)
	if err != nil {
		return false, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return false, nil, fmt.Errorf("%s: HTTP %d", raw, resp.StatusCode)
	}
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	return strings.Contains(ct, "video/mp2t"), resp.Body, nil
}

// convert turns a non-mp2t source into MPEG-TS and feeds it in, natively when
// the backend allows and the native reader will take the source, else with
// ffmpeg. The native reader refuses loudly (nativesrc.ErrUnsupported) rather
// than half-serving, which is what makes the fallback safe: a source it declines
// gets exactly the ffmpeg pipeline it got before.
func convert(ctx context.Context, src Source, raw string, chunkSize int, publish func([]byte)) error {
	if src.Backend == BackendFfmpeg {
		dlog.Logf("puller", "id=%s connected via ffmpeg remux (backend=ffmpeg): %s", src.Label, raw)
		return runFfmpeg(ctx, src, raw, chunkSize, publish)
	}

	rc, err := nativesrc.Open(ctx, raw, nativesrc.Options{
		UserAgent: src.ua(),
		Cookie:    src.Cookie,
		Proxy:     src.Proxy,
		Insecure:  src.Insecure,
	})
	if err == nil {
		defer rc.Close()
		dlog.Logf("puller", "id=%s connected native (no ffmpeg child): %s", src.Label, raw)
		return ingest.Copy(rc, chunkSize, publish)
	}

	if src.Backend == BackendNative {
		// The operator asked for native only — surfacing the refusal is the
		// point, so Run() backs off and retries rather than silently doing the
		// thing they turned off.
		return fmt.Errorf("native source declined (backend=native, no fallback): %w", err)
	}
	dlog.Logf("puller", "id=%s native declined (%v); falling back to ffmpeg: %s", src.Label, err, raw)
	return runFfmpeg(ctx, src, raw, chunkSize, publish)
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
	if src.Cookie != "" {
		args = append(args, "-headers", "Cookie: "+src.Cookie+"\r\n")
	}
	if src.Proxy != "" {
		args = append(args, "-http_proxy", "http://"+src.Proxy)
	}
	args = append(args,
		"-i", raw, "-map", "0", "-c", "copy",
		"-mpegts_flags", "+initial_discontinuity", "-pat_period", "2",
		"-f", "mpegts", "-",
	)

	cmd := exec.CommandContext(ctx, bin, args...)
	stderr := &tailBuffer{max: 4096}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	copyErr := ingest.Copy(stdout, chunkSize, publish)
	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		return copyErr // we cancelled it (stream stop/shutdown) — not a fault
	}
	if waitErr != nil {
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
