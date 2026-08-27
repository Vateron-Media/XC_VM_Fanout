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
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/ingest"
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
}

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
	backoff := defaults.PullBackoffInitial
	for ctx.Err() == nil {
		start := time.Now()
		err := pullOnce(ctx, src, chunkSize, publish)
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

// pullOnce tries each URL once: mp2t is streamed directly, otherwise ffmpeg
// remuxes it. Returns when the chosen source ends or errors.
func pullOnce(ctx context.Context, src Source, chunkSize int, publish func([]byte)) error {
	var lastErr error
	for _, raw := range src.URLs {
		isTS, body, err := probe(ctx, src, raw)
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
		dlog.Logf("puller", "id=%s connected via ffmpeg remux: %s", src.Label, raw)
		return runFfmpeg(ctx, src, raw, chunkSize, publish)
	}
	if lastErr == nil {
		lastErr = errors.New("no source urls")
	}
	return lastErr
}

func httpClient(src Source) (*http.Client, error) {
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
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
func probe(ctx context.Context, src Source, raw string) (bool, io.ReadCloser, error) {
	c, err := httpClient(src)
	if err != nil {
		return false, nil, err
	}
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
