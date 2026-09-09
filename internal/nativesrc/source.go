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
//   - fMP4/CMAF HLS            → ErrUnsupported (caller falls back to ffmpeg)
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

// DefaultSourceIdleTimeout bounds how long a native source read may stall
// with NO bytes before we treat it as a network stall (half-open connection)
// and close it so the read errors out → the supervisor reconnects. Mirrors
// the ffmpeg path's -rw_timeout (8s). A reconnect must only ever be driven by
// the SOURCE dropping / stalling — never by output (segment) staleness while
// bytes are still flowing.
const DefaultSourceIdleTimeout = 8 * time.Second

// idleTimeoutReader closes the wrapped source if no Read makes progress
// within `idle`. A live source that goes half-open (stops sending bytes but
// never closes the socket) would otherwise block the read forever; closing it
// turns the stall into a read error the caller can act on (reconnect).
// ErrSourceRateFloor wraps the read error produced when the rate-floor
// watchdog closes a source for sustained-low throughput, so the supervisor
// (and channel_health_events) can tell a rate-floor failover apart from a
// generic source drop / stall. errors.Is(err, ErrSourceRateFloor) is true on
// the read error the closed source returns.
var ErrSourceRateFloor = errors.New("remux: source below rate floor")

type idleTimeoutReader struct {
	rc    io.ReadCloser
	idle  time.Duration
	last  atomic.Int64 // unixnano of last byte received
	total atomic.Int64 // cumulative bytes read (for the rate floor)
	// Rate floor (opt-in, all zero = disabled): close the source when the read
	// throughput stays below the EFFECTIVE floor for `grace` consecutive
	// `window`s. The floor is max(minBps, ratio×baseline):
	//   - minBps  : an ABSOLUTE bits/s floor (a source below it is ~dead).
	//   - ratio   : a RELATIVE floor vs the source's OWN self-calibrated healthy
	//               baseline (peak-hold with slow decay), so "dropped to a
	//               fraction of what it was delivering" trips without needing a
	//               trustworthy external nominal (ffprobe on live TS only sees
	//               the audio PID). The relative half arms only after a short
	//               warmup so startup ramp-up never trips it.
	// This catches a source that keeps trickling bytes (idle/no-bytes check
	// never fires) but delivers a fraction of a real feed — a token URL that
	// throttles or reconnect-churns. Closing turns it into an ErrSourceRateFloor
	// read error the supervisor acts on via the SAME failover path as a stall.
	// LONG window + multi-window grace are deliberate so a transient dip never
	// churns a healthy channel (the historical SIC/TVI failover-loop came from
	// an aggressive PREFLIGHT probe, not this).
	minBps  int64
	ratio   float64
	window  time.Duration
	grace   int
	publish *atomic.Int64 // optional: latest measured window bitrate (bps), for the heartbeat
	tripped atomic.Bool   // set when the rate floor (not idle) closed the source
	done    chan struct{}
	once    sync.Once
}

// RateFloorOpts configures the sustained-low-throughput watchdog. All fields
// optional: MinBPS<=0 and Ratio<=0 disable the floor; a non-nil Publish still
// measures and exports the window bitrate (so the heartbeat gets a real rate
// even when no floor is armed); Window<=0 disables both the floor and Publish.
type RateFloorOpts struct {
	MinBPS  int64         // absolute bits/s floor (0 = off)
	Ratio   float64       // relative floor vs self-calibrated baseline, 0<r<1 (0 = off)
	Window  time.Duration // measurement window
	Grace   int           // consecutive low windows before closing
	Publish *atomic.Int64 // optional sink for the latest measured window bitrate (bps)
}

// WrapIdleTimeout wraps rc so a no-bytes stall longer than idle becomes a read
// error. idle <= 0 (or nil rc) returns rc unchanged.
func WrapIdleTimeout(rc io.ReadCloser, idle time.Duration) io.ReadCloser {
	return WrapIdleTimeoutWithRateFloorOpts(rc, idle, RateFloorOpts{})
}

// WrapIdleTimeoutWithRateFloor is the legacy absolute-only entrypoint kept for
// callers that don't measure/relative-floor (origin). Equivalent to Opts with
// only MinBPS/Window/Grace set.
func WrapIdleTimeoutWithRateFloor(rc io.ReadCloser, idle time.Duration, minBps int64, window time.Duration, grace int) io.ReadCloser {
	return WrapIdleTimeoutWithRateFloorOpts(rc, idle, RateFloorOpts{MinBPS: minBps, Window: window, Grace: grace})
}

// WrapIdleTimeoutWithRateFloorOpts wraps rc with the idle watchdog plus the
// configurable rate floor / measurement in o. Returns rc unchanged when there
// is nothing to do (nil rc, or no idle AND no window to measure on).
func WrapIdleTimeoutWithRateFloorOpts(rc io.ReadCloser, idle time.Duration, o RateFloorOpts) io.ReadCloser {
	measuring := o.Window > 0 && (o.MinBPS > 0 || (o.Ratio > 0 && o.Ratio < 1) || o.Publish != nil)
	if rc == nil || (idle <= 0 && !measuring) {
		return rc
	}
	grace := o.Grace
	if grace < 1 {
		grace = 1
	}
	ratio := o.Ratio
	if ratio <= 0 || ratio >= 1 {
		ratio = 0
	}
	r := &idleTimeoutReader{
		rc: rc, idle: idle,
		minBps: o.MinBPS, ratio: ratio, window: o.Window, grace: grace, publish: o.Publish,
		done: make(chan struct{}),
	}
	r.last.Store(time.Now().UnixNano())
	go r.watch()
	return r
}

func (r *idleTimeoutReader) watch() {
	tick := r.idle / 3
	if r.idle <= 0 || tick < time.Second {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	measuring := r.window > 0 && (r.minBps > 0 || r.ratio > 0 || r.publish != nil)
	winDeadline := time.Now().Add(r.window)
	winStart := r.total.Load()
	lowCount := 0
	windowsSeen := 0
	var baseline int64 // self-calibrated healthy rate: peak-hold w/ slow decay
	// Relative floor only arms after warmup windows so a stream still ramping
	// up / filling its first buffers never trips it. The absolute floor is NOT
	// warmup-gated (its multi-window grace already covers a slow start).
	const warmupWindows = 2
	for {
		select {
		case <-r.done:
			return
		case <-t.C:
			if r.idle > 0 && time.Since(time.Unix(0, r.last.Load())) > r.idle {
				_ = r.rc.Close() // unblock a stuck Read → it returns an error
				return
			}
			if measuring && !time.Now().Before(winDeadline) {
				delta := r.total.Load() - winStart
				secs := int64(r.window / time.Second)
				if secs < 1 {
					secs = 1
				}
				windowBps := (delta * 8) / secs
				if r.publish != nil {
					r.publish.Store(windowBps) // export measured rate for the heartbeat
				}
				windowsSeen++
				// Peak-hold with slow decay (×0.95/window): a transient burst
				// can't permanently inflate the baseline, and a sustained drop
				// lets it decay so windowBps eventually falls below ratio×baseline.
				// Only accumulate AFTER warmup so a startup burst (HLS-pull
				// concatenating a buffered segment window, TCP catch-up after
				// connect) never seeds an inflated baseline that would then false-
				// trip healthy steady-state windows.
				if windowsSeen > warmupWindows {
					if decayed := baseline - baseline/20; windowBps > decayed {
						baseline = windowBps
					} else {
						baseline = decayed
					}
				}
				floor := r.minBps
				if r.ratio > 0 && windowsSeen > warmupWindows && baseline > 0 {
					if rel := int64(float64(baseline) * r.ratio); rel > floor {
						floor = rel
					}
				}
				if floor > 0 && windowBps < floor {
					lowCount++
					if lowCount >= r.grace {
						r.tripped.Store(true)
						_ = r.rc.Close() // sustained thin source → ErrSourceRateFloor → failover
						return
					}
				} else {
					lowCount = 0
				}
				winStart = r.total.Load()
				winDeadline = time.Now().Add(r.window)
			}
		}
	}
}

func (r *idleTimeoutReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if n > 0 {
		r.last.Store(time.Now().UnixNano())
		r.total.Add(int64(n))
	}
	// When the rate-floor watchdog closed the source, surface a distinguishable
	// error so the supervisor labels the failover (vs a generic stall/drop).
	// %v (not %w) on the underlying error is deliberate: if the close races and
	// the blocked Read returns io.EOF, %w would make the trip satisfy
	// errors.Is(io.EOF) and the TS readers (which check io.EOF first) would treat
	// it as a CLEAN stream end — silently dropping the trip. %v keeps the
	// ErrSourceRateFloor chain but not the io.EOF chain, so it's always a failure.
	if err != nil && r.tripped.Load() {
		return n, fmt.Errorf("%w: %v", ErrSourceRateFloor, err)
	}
	return n, err
}

func (r *idleTimeoutReader) Close() error {
	r.once.Do(func() { close(r.done) })
	return r.rc.Close()
}

// ErrUnsupportedSource is returned by OpenSource when the URL scheme
// or content-type isn't something the native remuxer can read. The
// caller falls back to ffmpeg.
var ErrUnsupportedSource = errors.New("remux: unsupported source")

// OpenSource returns a streaming io.ReadCloser of MPEG-TS bytes for
// the given URL. Supported schemes today:
//
//	http(s):// — direct MPEG-TS streamed body (Content-Type starting
//	             with "video/mp2t" or path ending .ts). HLS playlists
//	             return ErrUnsupportedSource — use ffmpeg for those.
//	udp:// / rtp:// — pulls multicast MPEG-TS off the network. Local
//	             dev only; ops typically run a relay in front.
//	file:// or bare path — read from disk.
//
// The returned reader honours ctx — Close() during read aborts the
// transfer. Headers are applied to HTTP requests only.
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
		// HLS playlists are routed through the pull client, which
		// returns a pipe whose reader yields concatenated MPEG-TS
		// bytes from the live segment window. m3u8 served with an
		// arbitrary path extension is still classified by the
		// content-type sniff in openHTTP.
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

func openHTTP(ctx context.Context, u *url.URL, opt Options) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	opt.apply(req)
	resp, err := newStreamClient(opt).Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: http %d", ErrUnsupportedSource, resp.StatusCode)
	}
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	// Late HLS detection: server may have served a playlist despite
	// the URL not ending .m3u8. Close the body and re-route through
	// the pull client.
	if strings.Contains(ct, "mpegurl") {
		_ = resp.Body.Close()
		return OpenHLSPull(ctx, u.String(), opt)
	}
	if strings.Contains(ct, "mp2t") {
		return resp.Body, nil
	}
	// Anything else has to PROVE it is MPEG-TS before we hand it on. Upstream
	// this returned the body unchecked, which was safe there because a TS
	// demuxer sat behind it; here the bytes go straight to viewers, so an MP4
	// (or an HTML error page) served with a generic content-type would be
	// fanned out as if it were video. Sniff instead of trusting the header —
	// IPTV upstreams routinely serve real TS as application/octet-stream, so
	// the header alone is neither sufficient nor necessary.
	head, err := peekTS(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: %s: %v", ErrUnsupportedSource, u.Redacted(), err)
	}
	return &prefixedReadCloser{r: io.MultiReader(bytes.NewReader(head), resp.Body), c: resp.Body}, nil
}

// tsSyncProbePackets is how many consecutive packet boundaries must carry the
// sync byte before we accept a body as MPEG-TS. One 0x47 is a coincidence; four
// at exactly 188-byte spacing is the format.
const tsSyncProbePackets = 4

// peekTS reads enough of a body to confirm it is MPEG-TS and returns what it
// consumed, so the caller can replay those bytes to the real reader.
func peekTS(r io.Reader) ([]byte, error) {
	head := make([]byte, tsSyncProbePackets*tsPacketSize)
	n, err := io.ReadFull(r, head)
	head = head[:n]
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("read: %w", err)
	}
	if n < tsPacketSize {
		return nil, fmt.Errorf("only %d bytes, not an mpegts stream", n)
	}
	for off := 0; off+1 <= n; off += tsPacketSize {
		if head[off] != 0x47 {
			return nil, fmt.Errorf("no mpegts sync byte at offset %d (0x%02x)", off, head[off])
		}
	}
	return head, nil
}

// tsPacketSize is the MPEG-TS packet length.
const tsPacketSize = 188

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
	r := &udpReader{conn: conn, ctx: ctx}
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
func (r *udpReader) Close() error { return r.conn.Close() }
func (r *udpReader) watch() {
	<-r.ctx.Done()
	_ = r.conn.Close()
}
