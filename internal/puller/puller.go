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
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/ingest"
)

// Source describes where and how to pull a live stream.
type Source struct {
	URLs      []string // candidate source URLs, tried in order
	UserAgent string
	Proxy     string // "host:port" HTTP proxy, optional
	Cookie    string // optional Cookie header value
	FfmpegBin string // ffmpeg path; "ffmpeg" if empty
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
	backoff := time.Second
	for ctx.Err() == nil {
		if err := pullOnce(ctx, src, chunkSize, publish); err != nil && ctx.Err() == nil {
			log.Printf("puller: %v (retry in %s)", err, backoff)
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
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
			lastErr = err
			continue
		}
		if isTS {
			defer body.Close()
			return ingest.Copy(body, chunkSize, publish)
		}
		body.Close()
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
		"-copyts", "-vsync", "0", "-nostats", "-nostdin", "-hide_banner",
		"-loglevel", "quiet", "-y", "-user_agent", src.ua(),
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
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	copyErr := ingest.Copy(stdout, chunkSize, publish)
	_ = cmd.Wait()
	return copyErr
}
