// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Options are the per-source knobs the daemon already carries for a pull, so a
// stream reaches its source the same way whether it goes down the native path or
// the ffmpeg one. The upstream project used package-level HTTP clients with
// ProxyFromEnvironment and default TLS verification; neither is usable here — a
// panel source routinely needs its OWN proxy, and source_insecure defaults to
// true precisely because those upstreams commonly present self-signed or
// mismatched certificates.
type Options struct {
	UserAgent string // sent unless the caller set one in Header
	Cookie    string // value of the Cookie header, if any
	Proxy     string // "host:port" HTTP proxy, optional
	Insecure  bool   // skip upstream TLS certificate verification
	// Headers are extra request headers as raw "Key: value" lines. They apply
	// to every fetch this package makes for the source — the playlist AND each
	// segment — because an upstream that gates on a header gates on all of it.
	Headers []string
}

// Timeouts. Bounded fetches (playlists, segments) get a whole-request deadline;
// a continuous stream body must not, or a healthy live source would be cut, so
// there the deadlines cover only reaching the first byte.
const (
	pullRequestTimeout = 30 * time.Second // one playlist or one segment
	streamHeaderWait   = 12 * time.Second // response headers on a live body
	tlsWait            = 10 * time.Second
	idleConnLife       = 90 * time.Second
)

// ErrUnsupported is returned for any source this package will not take. The
// caller treats it as "run ffmpeg instead" — it is the fallback signal, and
// every refusal must funnel through it rather than through a partial read.
var ErrUnsupported = ErrUnsupportedSource

// ErrHLSIsFMP4 marks an HLS source whose segments are fragmented MP4 rather than
// MPEG-TS. Repackaging those to TS needs a real demuxer, so it is a refusal here
// — but a distinct one, because it says something specific about the upstream
// and is worth seeing in a log rather than a generic "unsupported". A format
// refusal (IsFormat).
var ErrHLSIsFMP4 = fmt.Errorf("%w: hls source carries fmp4 segments", ErrFormat)

// ErrHLSEncrypted marks an HLS source whose segments are encrypted
// (#EXT-X-KEY with a METHOD other than NONE). This package passes segment bytes
// through unread, so an encrypted segment would reach viewers as ciphertext —
// noise with a valid-looking content type. ffmpeg decrypts AES-128 HLS, so this
// is a format refusal (IsFormat) that the fallback can serve.
var ErrHLSEncrypted = fmt.Errorf("%w: hls segments are encrypted", ErrFormat)

// transport builds the shared transport shape. dialWait separates the two
// callers: a bounded fetch can afford to wait a little longer to connect than a
// live body, which should fail fast so the puller can rotate to the next URL.
func (o Options) transport(dialWait time.Duration) *http.Transport {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: o.Insecure},
		DialContext: (&net.Dialer{
			Timeout:   dialWait,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   tlsWait,
		ResponseHeaderTimeout: streamHeaderWait,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          32,
		IdleConnTimeout:       idleConnLife,
	}
	if o.Proxy != "" {
		if pu, err := url.Parse("http://" + o.Proxy); err == nil {
			tr.Proxy = http.ProxyURL(pu)
		}
	}
	return tr
}

// apply stamps the source's identity onto a request.
func (o Options) apply(req *http.Request) {
	if o.UserAgent != "" && req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", o.UserAgent)
	}
	if o.Cookie != "" && req.Header.Get("Cookie") == "" {
		req.Header.Set("Cookie", o.Cookie)
	}
	// Configured headers are set last and unconditionally: they are the most
	// specific thing anyone said about this source, so they win over the
	// defaults above rather than being skipped because a default got there
	// first. A line without a colon is skipped, not guessed at.
	for _, line := range o.Headers {
		name, value, ok := strings.Cut(line, ":")
		name = strings.TrimSpace(name)
		if !ok || name == "" {
			continue
		}
		req.Header.Set(name, strings.TrimSpace(value))
	}
}
