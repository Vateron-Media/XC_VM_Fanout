// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/redact"
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

// ErrHLSNotTS marks an HLS source whose segments are not MPEG-TS at all. Packed
// audio is the one that turns up in practice — RFC 8216 lets a playlist carry
// ID3 + ADTS directly in .aac/.ac3/.mp3 segments, which is how radio channels
// ship — and an HTML error page served in a segment's place is the other. This
// package copies segment bytes through unread, so either would reach viewers as
// 188-byte slices of something that is not TS: no PAT, no PMT, no keyframe,
// nothing playable. ffmpeg handles packed audio, so this is a format refusal
// (IsFormat) that the fallback can serve.
var ErrHLSNotTS = fmt.Errorf("%w: hls segments are not mpeg-ts", ErrFormat)

// ErrHLSByteRange marks an HLS source whose segments are #EXT-X-BYTERANGE slices
// of one larger resource rather than whole files. This package GETs a segment
// URI and passes the whole response through, so such a playlist would fetch the
// entire resource for its first entry and then skip every later one that names
// it again — and once that resource grows past the runaway limit, fetch nothing
// at all. ffmpeg sends a Range header per segment, so this is a format refusal
// (IsFormat) that the fallback can serve.
var ErrHLSByteRange = fmt.Errorf("%w: hls segments are byte ranges of one resource", ErrFormat)

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
	switch pu, err := o.proxyURL(); {
	case err != nil:
		// Fail CLOSED. The parse error used to be dropped, which left a source
		// that an operator had put behind a proxy connecting DIRECTLY from the
		// node's own IP — no error, no log line, and no proxy, which is the one
		// outcome a proxy is configured to prevent. checkProxy refuses the fetch
		// before it starts; this is the belt to that braces, for any path that
		// builds a transport without asking first.
		tr.Proxy = func(*http.Request) (*url.URL, error) { return nil, err }
	case pu != nil:
		tr.Proxy = http.ProxyURL(pu)
	}
	return tr
}

// proxyURL resolves the configured proxy. The panel sends a bare "host:port",
// which needs the scheme prefixed; the remux flag and hand-written configs carry
// "http://host:port", which must NOT be prefixed again — "http://http://host"
// either fails to parse or aims at a host called "http".
func (o Options) proxyURL() (*url.URL, error) {
	raw := strings.TrimSpace(o.Proxy)
	if raw == "" {
		return nil, nil
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		// A *url.Error repeats the whole URL — credentials included — in its
		// message, so only the reason is safe to put in a log.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("proxy %s: %v", redact.URL(raw), err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("proxy %s: no host", redact.URL(raw))
	}
	return u, nil
}

// checkProxy refuses a fetch whose proxy cannot be used, so a misconfigured
// proxy is an error the operator sees rather than a direct connection nobody
// notices. Callers that speak HTTP check it before their first request.
func (o Options) checkProxy() error {
	_, err := o.proxyURL()
	return err
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
