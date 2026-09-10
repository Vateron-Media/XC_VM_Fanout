// Package nativesrc acquires a live source and yields MPEG-TS bytes, in-process,
// for the sources that need no transcoding — replacing the per-stream ffmpeg
// remux child for the common IPTV case.
//
// Ported from the P2PTV project's pkg/remux source layer (same author), reduced
// to the two files Fanout needs (source dispatch + HLS pull) and adapted to
// Fanout's per-source knobs: the package-level HTTP clients became per-source
// ones so a stream's proxy, cookie, User-Agent and TLS-verification setting all
// apply, exactly as they do on the direct-mp2t path.
//
// What it handles, and what it refuses:
//
//   - http(s) serving MPEG-TS  → streamed through unchanged
//   - http(s) serving m3u8     → segments pulled and concatenated (live or VOD)
//   - udp:// and rtp://        → read straight off the socket
//   - fMP4/CMAF HLS            → ErrHLSIsFMP4 (caller falls back to ffmpeg)
//   - anything else            → ErrUnsupported
//
// Refusing loudly is the whole contract: the caller runs ffmpeg for whatever
// this package will not take, so an unsupported source degrades to today's
// behaviour rather than to garbage on the wire.
package nativesrc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultSourceIdleTimeout bounds how long a source read may stall with NO bytes
// before we treat it as a network stall (a half-open connection) and close it so
// the read errors out and the puller reconnects. Mirrors the ffmpeg path's
// -rw_timeout (8s). A live source that delivers nothing for eight seconds is not
// slow, it is gone; the cost of being wrong is one reconnect, which viewers ride
// out on the buffer they already have.
const DefaultSourceIdleTimeout = 8 * time.Second

// idleTimeoutReader closes the wrapped source if no Read makes progress within
// `idle`. A live source that goes half-open — stops sending bytes but never
// closes the socket, the classic dropped upstream — would otherwise block the
// read forever: no error, so no reconnect, so a channel that is silently dead
// until something downstream happens to notice.
type idleTimeoutReader struct {
	rc   io.ReadCloser
	idle time.Duration
	last atomic.Int64 // unixnano of the last byte received
	done chan struct{}
	once sync.Once
}

// WrapIdleTimeout wraps rc so a no-bytes stall longer than idle becomes a read
// error. idle <= 0 (or a nil rc) returns rc unchanged.
func WrapIdleTimeout(rc io.ReadCloser, idle time.Duration) io.ReadCloser {
	if rc == nil || idle <= 0 {
		return rc
	}
	r := &idleTimeoutReader{rc: rc, idle: idle, done: make(chan struct{})}
	r.last.Store(time.Now().UnixNano())
	go r.watch()
	return r
}

func (r *idleTimeoutReader) watch() {
	tick := r.idle / 3
	if tick < time.Second {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-t.C:
			if time.Since(time.Unix(0, r.last.Load())) > r.idle {
				_ = r.rc.Close() // unblock a stuck Read: it returns an error
				return
			}
		}
	}
}

func (r *idleTimeoutReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if n > 0 {
		r.last.Store(time.Now().UnixNano())
	}
	return n, err
}

func (r *idleTimeoutReader) Close() error {
	r.once.Do(func() { close(r.done) })
	return r.rc.Close()
}

// ErrUnsupportedSource is returned by Open when the URL scheme or content-type
// isn't something the native reader can take. The caller falls back to ffmpeg.
var ErrUnsupportedSource = errors.New("remux: unsupported source")

// Open returns a streaming io.ReadCloser of MPEG-TS bytes for the given URL:
//
//	http(s):// — an MPEG-TS body (by content-type, or by sniffing the sync
//	             bytes), or an m3u8 playlist whose segments are MPEG-TS.
//	udp:// / rtp:// — pulls MPEG-TS off the socket (multicast or unicast).
//	file:// or a bare path — read from disk.
//
// The returned reader honours ctx: Close() during a read aborts the transfer.
func Open(ctx context.Context, rawURL string, opt Options) (io.ReadCloser, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, ErrUnsupportedSource
	}
	// Bare path → file.
	if strings.HasPrefix(rawURL, "/") || strings.HasPrefix(rawURL, "./") || strings.HasPrefix(rawURL, "../") {
		return os.Open(rawURL)
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: parse: %v", ErrUnsupportedSource, err)
	}
	switch strings.ToLower(u.Scheme) {
	case "file":
		return os.Open(u.Path)
	case "http", "https":
		// HLS playlists are routed through the pull client, which returns a pipe
		// whose reader yields concatenated MPEG-TS bytes from the live segment
		// window. An m3u8 served under an arbitrary path extension is still
		// classified by the sniff in AdoptHTTP.
		if strings.HasSuffix(strings.ToLower(u.Path), ".m3u8") {
			return OpenHLSPull(ctx, u.String(), opt)
		}
		return openHTTP(ctx, u, opt)
	case "udp", "rtp":
		return openUDP(ctx, u)
	}
	return nil, fmt.Errorf("%w: scheme %q", ErrUnsupportedSource, u.Scheme)
}

// newStreamClient opens a CONTINUOUS live source body. Unlike the pull client it
// has NO whole-request Timeout — the body streams indefinitely and a Timeout
// would cut a healthy live stream; ctx handles cancellation. The dial and
// response-header deadlines exist to fail FAST on a dead host instead of waiting
// out the OS connect timeout before the caller can try the next URL. Redirects
// are still followed: providers commonly 302 first.
func newStreamClient(opt Options) *http.Client {
	return &http.Client{Transport: opt.transport(8 * time.Second)}
}

// clientBoundReadCloser ties a per-request http.Client's connection pool to the
// lifetime of the body it produced.
//
// Building a client inline and letting it fall out of scope leaves the transport
// unreachable but not idle: when the body closes, its connection is handed back
// to that orphaned pool, where it stays open — with its reader goroutine — until
// IdleConnTimeout expires it 90s later. A source reconnecting on the puller's
// backoff stranded a socket and its goroutines on every attempt. This is the same
// leak internal/puller documents fixing for its own client, reintroduced here by
// the per-open client; closing the body now drains its pool with it.
type clientBoundReadCloser struct {
	io.ReadCloser
	client *http.Client
}

func (c *clientBoundReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.client.CloseIdleConnections()
	return err
}

func openHTTP(ctx context.Context, u *url.URL, opt Options) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	opt.apply(req)
	client := newStreamClient(opt)
	resp, err := client.Do(req)
	if err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	rc, err := AdoptHTTP(ctx, resp, opt)
	if err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	return &clientBoundReadCloser{ReadCloser: rc, client: client}, nil
}

// AdoptHTTP takes ownership of an ALREADY-OPEN response and yields MPEG-TS bytes
// from it, or an error (having closed the body). It is the classification half of
// openHTTP, exported so a caller that has already fetched the URL can hand the
// response straight over instead of discarding it and fetching again.
//
// internal/puller does exactly that: it probes every source URL with a GET to
// read the content-type, and used to close that body and let this package open a
// second connection to the same URL — two round trips per connect, the first
// one's connection unusable for the pool because it was closed undrained. The
// caller keeps ownership of the http.Client the response came from; only the body
// transfers.
func AdoptHTTP(ctx context.Context, resp *http.Response, opt Options) (io.ReadCloser, error) {
	if resp.StatusCode/100 != 2 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: http %d", ErrUnsupportedSource, resp.StatusCode)
	}
	final := resp.Request.URL // honour redirects when resolving segment URIs

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	switch {
	case strings.Contains(ct, "mpegurl"):
		return adoptHLS(ctx, final, opt, nil, resp.Body)
	case strings.Contains(ct, "mp2t"):
		return WrapIdleTimeout(resp.Body, DefaultSourceIdleTimeout), nil
	}

	// Anything else has to PROVE what it is before we hand it on. Upstream this
	// returned the body unchecked, which was safe there because a TS demuxer sat
	// behind it; here the bytes go straight to viewers, so an MP4 (or an HTML
	// error page) served with a generic content-type would be fanned out as if it
	// were video. Sniff instead of trusting the header — IPTV upstreams routinely
	// serve real TS, and the odd playlist, as application/octet-stream, so the
	// header alone is neither sufficient nor necessary.
	head := make([]byte, tsSyncProbePackets*tsPacketSize)
	n, err := io.ReadFull(resp.Body, head)
	head = head[:n]
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: %s: read: %v", ErrUnsupportedSource, redact(final), err)
	}
	switch {
	case looksLikeTS(head):
		body := &prefixedReadCloser{r: io.MultiReader(bytes.NewReader(head), resp.Body), c: resp.Body}
		return WrapIdleTimeout(body, DefaultSourceIdleTimeout), nil
	case looksLikePlaylist(head):
		return adoptHLS(ctx, final, opt, head, resp.Body)
	}
	_ = resp.Body.Close()
	return nil, fmt.Errorf("%w: %s: not an mpegts stream or a playlist", ErrUnsupportedSource, redact(final))
}

// adoptHLS finishes reading a playlist whose first bytes have already been
// consumed and hands the manifest to the HLS puller, so it is not fetched a
// second time. Always closes body.
func adoptHLS(ctx context.Context, base *url.URL, opt Options, head []byte, body io.ReadCloser) (io.ReadCloser, error) {
	defer body.Close()
	rest, err := io.ReadAll(io.LimitReader(body, maxPlaylistBytes-int64(len(head))))
	if err != nil {
		return nil, fmt.Errorf("%w: %s: read playlist: %v", ErrUnsupportedSource, redact(base), err)
	}
	manifest := make([]byte, 0, len(head)+len(rest))
	manifest = append(append(manifest, head...), rest...)
	return openHLSPullWith(ctx, base, opt, manifest)
}

func redact(u *url.URL) string {
	if u == nil {
		return "source"
	}
	return u.Redacted()
}

// tsSyncProbePackets is how many consecutive packet boundaries must carry the
// sync byte before we accept a body as MPEG-TS. One 0x47 is a coincidence; four
// at exactly 188-byte spacing is the format.
const tsSyncProbePackets = 4

// tsPacketSize is the MPEG-TS packet length.
const tsPacketSize = 188

// looksLikeTS reports whether head carries the sync byte at every 188-byte
// boundary it covers, over at least one whole packet.
func looksLikeTS(head []byte) bool {
	if len(head) < tsPacketSize {
		return false
	}
	for off := 0; off < len(head); off += tsPacketSize {
		if head[off] != 0x47 {
			return false
		}
	}
	return true
}

// looksLikePlaylist reports whether head opens an m3u8, tolerating a UTF-8 BOM
// and leading whitespace. An upstream that serves playlists as text/plain or
// octet-stream would otherwise be bounced to ffmpeg for no reason.
func looksLikePlaylist(head []byte) bool {
	return bytes.HasPrefix(bytes.TrimLeft(head, "\xef\xbb\xbf \t\r\n"), []byte("#EXTM3U"))
}

// prefixedReadCloser replays already-consumed bytes ahead of the live body while
// still closing the underlying connection.
type prefixedReadCloser struct {
	r io.Reader
	c io.Closer
}

func (p *prefixedReadCloser) Read(b []byte) (int, error) { return p.r.Read(b) }
func (p *prefixedReadCloser) Close() error               { return p.c.Close() }

func openUDP(ctx context.Context, u *url.URL) (io.ReadCloser, error) {
	host := u.Host
	if host == "" {
		return nil, fmt.Errorf("%w: empty udp host", ErrUnsupportedSource)
	}
	ip, port, err := net.SplitHostPort(host)
	if err != nil {
		return nil, fmt.Errorf("%w: host: %v", ErrUnsupportedSource, err)
	}
	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(ip, port))
	if err != nil {
		return nil, err
	}
	var conn *net.UDPConn
	if addr.IP.IsMulticast() {
		conn, err = net.ListenMulticastUDP("udp", nil, addr)
	} else {
		conn, err = net.ListenUDP("udp", addr)
	}
	if err != nil {
		return nil, err
	}
	// UDP packets are not MPEG-TS-aligned per se; many ingests carry 7
	// TS packets per UDP datagram. The demuxer reads in 188-byte
	// chunks anyway, so we just hand back the raw conn wrapped in a
	// ReadCloser that respects ctx cancellation.
	r := &udpReader{conn: conn, ctx: ctx, done: make(chan struct{})}
	go r.watch()
	return r, nil
}

// udpReader adapts a datagram socket to io.Reader.
//
// It MUST stage whole datagrams internally. A UDP read is all-or-nothing: the
// kernel copies as much as fits into the supplied buffer and DISCARDS the rest
// of the datagram, returning no error (recvfrom without MSG_TRUNC). The TS
// demuxer reads with io.ReadFull(r, d.pkt[:]) where d.pkt is [188]byte, and a
// standard MPEG-TS-over-UDP datagram carries 7×188 = 1316 bytes — so passing
// the caller's 188-byte buffer straight to conn.Read kept the first TS packet
// of every datagram and silently dropped the other six. PAT/PMT then landed
// only ~1/7 of the time and video PES never reassembled: udp:// sources on the
// native fMP4 path either stalled with "no decodable frames" or produced
// shredded segments. (The TS-passthrough path escaped this only by accident,
// because it wraps the reader in a bufio.Reader.)
type udpReader struct {
	conn *net.UDPConn
	ctx  context.Context

	// done releases the ctx watcher when this reader closes. Without it the
	// watcher blocked on ctx.Done() for the whole STREAM's lifetime rather than
	// this reader's, so every reconnect of a udp source stranded a goroutine and
	// this reader's 64 KiB staging buffer until the stream itself was torn down.
	done chan struct{}
	once sync.Once

	buf []byte // staging for one whole datagram
	off int    // next unread byte in buf
	n   int    // bytes held in buf
	err error  // deferred error, reported after buffered bytes drain
}

// maxUDPDatagram is the largest payload a single UDP datagram can carry.
// Sized to the theoretical maximum rather than the usual 1316-byte TS payload
// so an oversized or jumbo-framed source can never be truncated either.
const maxUDPDatagram = 65536

func (r *udpReader) Read(p []byte) (int, error) {
	// Drain whatever is still staged before touching the socket.
	if r.off < r.n {
		c := copy(p, r.buf[r.off:r.n])
		r.off += c
		return c, nil
	}
	// Buffer empty: surface any error the previous datagram carried.
	if r.err != nil {
		err := r.err
		r.err = nil
		return 0, err
	}
	if r.buf == nil {
		r.buf = make([]byte, maxUDPDatagram)
	}
	// 60s read deadline is the upper bound between live datagrams —
	// if the source vanishes we want Read to return rather than block
	// the demuxer forever.
	_ = r.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	n, err := r.conn.Read(r.buf)
	r.off, r.n = 0, n
	if n <= 0 {
		r.n = 0
		return 0, err
	}
	// Bytes arrived alongside an error: hand over the data first and report
	// the error on the next call, as io.Reader requires.
	if err != nil {
		r.err = err
	}
	c := copy(p, r.buf[r.off:r.n])
	r.off += c
	return c, nil
}

func (r *udpReader) Close() error {
	r.once.Do(func() { close(r.done) })
	return r.conn.Close()
}

func (r *udpReader) watch() {
	select {
	case <-r.ctx.Done():
		_ = r.conn.Close()
	case <-r.done:
	}
}
