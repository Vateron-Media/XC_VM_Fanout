// Package server exposes the fan-out hubs and in-memory HLS over HTTP, owns the
// id→Stream registry, and drives per-stream on-demand lifecycle (start the
// source puller on the first viewer, stop it after the last one leaves).
//
// Two HTTP surfaces:
//   - client  (nginx-facing): GET /live/<id>, /hls/<id>/index.m3u8, /hls/<id>/<seq>.ts
//   - control (PHP-only):     PUT/DELETE /streams/<id>
package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/config"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/hlscrypt"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/hub"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/ingest"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// Operational defaults live in internal/defaults; these aliases keep the local
// names the hot paths read by. See that package for the rationale behind each.
const defaultChunk = defaults.IngestChunk
const defaultWriteTimeout = defaults.WriteTimeout

// Stream bundles the fan-out for one source plus its on-demand lifecycle state.
// The Hub owns the single TS cache; HLS is cut from that cache on demand (see
// Hub.HLSPlaylist / HLSSegment), not buffered a second time.
type Stream struct {
	Hub *hub.Hub

	id string // stream id, set at creation; for debug logging

	mgr *Manager // owning manager, for the viewer-gated buffer restore/gate

	mu       sync.Mutex
	cfg      *puller.Source // nil = externally fed (launch mode / ingest); set = daemon pulls
	chunk    int
	grace    time.Duration
	running  bool
	cancel   context.CancelFunc
	refs     int  // live TS viewers currently connected
	buffered bool // true = ring at full prebuffer/HLS; false = gated to the idle floor

	ingestLn   net.Listener // non-nil = push-fed: the producer (ffmpeg tee) connects here
	ingestSock string       // path of the ingest listener socket (for cleanup)

	// ingestConns are the producer connections accepted on ingestLn. Tracked so a
	// teardown can close them: closing the listener alone leaves an already-connected
	// producer feeding a Stream that is no longer in the registry — an orphan whose
	// ring is unreachable and never freed.
	ingestMu    sync.Mutex
	ingestConns map[net.Conn]struct{}

	lastData   atomic.Int64 // UnixNano of the last non-empty Publish (0 = never); off-air signal
	lastAccess atomic.Int64 // UnixNano of the last viewer touch (TS attach or HLS request)

	connMu sync.Mutex           // guards conns (map + each connStat's refs/since)
	conns  map[string]*connStat // active live-TS viewer uuids (from the ?c= param)

	encMu  sync.RWMutex // guards hlsKey/hlsIV
	hlsKey []byte       // AES-128 key for encrypted HLS segments (nil = plain)
	hlsIV  []byte       // AES-128-CBC IV

	segMu    sync.Mutex        // guards segCache/segOrder
	segCache map[int]*segEntry // seq → the bytes served for it (post-encryption)
	segOrder []int             // seqs in insertion order, for eviction
}

// segEntry is one cached HLS segment. ready is closed once data is final, so the
// first requester assembles-and-encrypts while every other viewer asking for the
// same seq waits for that one result instead of repeating the work — the join of
// a channel's whole HLS audience onto a fresh segment is exactly simultaneous, so
// without this single-flight the cache would still let the herd through.
type segEntry struct {
	ready chan struct{}
	data  []byte
}

// setEnc configures per-segment HLS encryption from hex key/iv (16 bytes each);
// empty/invalid clears it (plain HLS). TS fan-out is always plain.
func (s *Stream) setEnc(keyHex, ivHex string) {
	var k, iv []byte
	if keyHex != "" && ivHex != "" {
		if kk, e1 := hex.DecodeString(keyHex); e1 == nil && len(kk) == 16 {
			if vv, e2 := hex.DecodeString(ivHex); e2 == nil && len(vv) == 16 {
				k, iv = kk, vv
			}
		}
	}
	s.encMu.Lock()
	s.hlsKey, s.hlsIV = k, iv
	s.encMu.Unlock()
	s.dropSegCache() // cached segments carry the OLD key's ciphertext
}

// encryptSegment returns the segment encrypted for HLS when a key is set, else
// the plain bytes unchanged.
func (s *Stream) encryptSegment(data []byte) []byte {
	s.encMu.RLock()
	k, iv := s.hlsKey, s.hlsIV
	s.encMu.RUnlock()
	if len(k) == 0 {
		return data
	}
	if enc := hlscrypt.EncryptCBC(data, k, iv); enc != nil {
		return enc
	}
	return data
}

// hlsSegment returns the bytes to serve for HLS segment seq — assembled from the
// ring and, when the stream has a key, AES-encrypted — reusing a recent result
// when one is cached. Every viewer of a channel fetches byte-identical segments,
// so doing this per request meant each one paid a full copy of the segment out of
// the ring plus a full AES pass over it; at a 2 MB segment that is several ms of
// CPU and ~7 MB of garbage per viewer per segment, scaling linearly with the
// audience. Returns nil when seq is unknown or has aged out of the ring.
//
// A miss is NOT cached: seq may simply not have closed yet, and a nil pinned in
// the map would then hide the segment once it does.
func (s *Stream) hlsSegment(seq int) []byte {
	s.segMu.Lock()
	if e := s.segCache[seq]; e != nil {
		s.segMu.Unlock()
		<-e.ready
		return e.data
	}
	e := &segEntry{ready: make(chan struct{})}
	if s.segCache == nil {
		s.segCache = make(map[int]*segEntry)
	}
	s.segCache[seq] = e
	s.segOrder = append(s.segOrder, seq)
	for len(s.segOrder) > defaults.HLSSegCacheEntries {
		delete(s.segCache, s.segOrder[0])
		s.segOrder = append(s.segOrder[:0], s.segOrder[1:]...)
	}
	s.segMu.Unlock()

	if raw := s.Hub.HLSSegment(seq); raw != nil {
		e.data = s.encryptSegment(raw)
	}
	close(e.ready)

	if e.data == nil {
		s.segMu.Lock()
		if s.segCache[seq] == e {
			delete(s.segCache, seq)
			for i, q := range s.segOrder {
				if q == seq {
					s.segOrder = append(s.segOrder[:i], s.segOrder[i+1:]...)
					break
				}
			}
		}
		s.segMu.Unlock()
	}
	return e.data
}

// dropSegCache releases the cached segments. Called when the key changes (the
// ciphertext is stale) and when the stream is gated idle — a channel nobody
// watches should not hold segment copies on top of its ring.
func (s *Stream) dropSegCache() {
	s.segMu.Lock()
	s.segCache, s.segOrder = nil, nil
	s.segMu.Unlock()
}

// connStat tracks one live-TS viewer uuid: an active-connection refcount, the
// attach time, and the bytes delivered so far. refs/since are guarded by the
// stream's connMu; bytes is atomic so serveLive can account each write on the
// hot path without taking the lock. Used for both disconnect reconciliation
// (refs) and per-viewer transfer telemetry (bytes/since → KB/s, P4).
type connStat struct {
	refs  int
	since time.Time
	bytes atomic.Int64
}

// addConn records an active live-TS viewer by its connection uuid (passed by
// live.php via the X-Accel URL) and returns its connStat so the caller can
// account delivered bytes. PHP marks these lines_live rows pid=0 and the
// fanout_sync daemon reconciles them against this set — closing rows whose uuid
// is no longer connected here, since PHP can't see the disconnect under X-Accel.
func (s *Stream) addConn(uuid string) *connStat {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conns == nil {
		s.conns = make(map[string]*connStat)
	}
	cs := s.conns[uuid]
	if cs == nil {
		cs = &connStat{since: time.Now()}
		s.conns[uuid] = cs
	}
	cs.refs++
	return cs
}

func (s *Stream) removeConn(uuid string) {
	s.connMu.Lock()
	if s.conns != nil {
		if cs := s.conns[uuid]; cs != nil {
			cs.refs--
			if cs.refs <= 0 {
				delete(s.conns, uuid)
			}
		}
	}
	s.connMu.Unlock()
}

func (s *Stream) connUUIDs() []string {
	s.connMu.Lock()
	out := make([]string, 0, len(s.conns))
	for u := range s.conns {
		out = append(out, u)
	}
	s.connMu.Unlock()
	return out
}

// connRates returns, per active viewer uuid, the average delivery rate in KB/s
// since the connection attached (bytes / elapsed / 1024). This is the daemon-side
// replacement for the legacy chase-read loop's DIVERGENCE_TMP_PATH speed file:
// fanout_sync compares it to the stream's expected bitrate and records the
// divergence for daemon-served viewers, whose rate PHP can no longer measure
// itself (it left the byte path at X-Accel hand-off).
func (s *Stream) connRates() map[string]int {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	now := time.Now()
	out := make(map[string]int, len(s.conns))
	for u, cs := range s.conns {
		elapsed := now.Sub(cs.since).Seconds()
		if elapsed <= 0 {
			continue
		}
		out[u] = int(float64(cs.bytes.Load()) / elapsed / 1024.0)
	}
	return out
}

// startIngestLocked puts the stream in push-fed mode: it listens on sockPath and
// feeds each producer connection's mpegts bytes into the Stream. The producer
// (the stream's ffmpeg `-f tee … unix:<sockPath>` output) connects; the daemon
// accepts and reads. Idempotent. Caller holds s.mu.
func (s *Stream) startIngestLocked(sockPath string, chunk int) error {
	if s.ingestLn != nil {
		return nil // already listening
	}
	if dir := filepath.Dir(sockPath); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	_ = os.Remove(sockPath)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return err
	}
	_ = os.Chmod(sockPath, 0o660)
	if chunk > 0 {
		s.chunk = chunk
	}
	s.ingestLn = ln
	s.ingestSock = sockPath
	ch := s.chunk
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed (stopIngestLocked)
			}
			dlog.Logf("ingest", "id=%s producer connected on %s", s.id, sockPath)
			s.addIngestConn(conn)
			go func(c net.Conn) {
				defer s.removeIngestConn(c)
				defer c.Close()
				err := ingest.Copy(c, ch, s.Publish)
				dlog.Logf("ingest", "id=%s producer disconnected: %v", s.id, err)
			}(conn)
		}
	}()
	return nil
}

func (s *Stream) addIngestConn(c net.Conn) {
	s.ingestMu.Lock()
	if s.ingestConns == nil {
		s.ingestConns = make(map[net.Conn]struct{})
	}
	s.ingestConns[c] = struct{}{}
	s.ingestMu.Unlock()
}

func (s *Stream) removeIngestConn(c net.Conn) {
	s.ingestMu.Lock()
	delete(s.ingestConns, c)
	s.ingestMu.Unlock()
}

// closeIngestConns hangs up on every connected producer. Takes only ingestMu, so
// it is safe to call with s.mu held (the accept path never takes s.mu).
func (s *Stream) closeIngestConns() int {
	s.ingestMu.Lock()
	n := len(s.ingestConns)
	for c := range s.ingestConns {
		_ = c.Close()
	}
	s.ingestConns = nil
	s.ingestMu.Unlock()
	return n
}

// stopIngestLocked closes the ingest listener, hangs up on every connected
// producer and removes the socket. Closing the listener alone was not enough: a
// producer already connected (the stream's ffmpeg tee, which the panel may stop a
// moment later, or never) went on feeding a Stream that Unregister had just taken
// out of the registry — an orphan holding a full ring that nothing could reach or
// free. Caller holds s.mu.
func (s *Stream) stopIngestLocked() {
	if s.ingestLn != nil {
		_ = s.ingestLn.Close()
		s.ingestLn = nil
	}
	if n := s.closeIngestConns(); n > 0 {
		dlog.Logf("ingest", "id=%s closed %d in-flight producer connection(s)", s.id, n)
	}
	if s.ingestSock != "" {
		_ = os.Remove(s.ingestSock)
		s.ingestSock = ""
	}
}

// Publish feeds one packet-aligned chunk into the fan-out (which also folds it
// into the single TS cache HLS is cut from) and records data liveness (for
// off-air detection via status()).
func (s *Stream) Publish(chunk []byte) {
	if len(chunk) > 0 {
		s.lastData.Store(time.Now().UnixNano())
	}
	s.Hub.Publish(chunk)
}

// setConfig registers/updates the pull config; if viewers are already waiting it
// starts the puller immediately.
func (s *Stream) setConfig(src puller.Source, chunk int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := src
	s.cfg = &c
	if chunk > 0 {
		s.chunk = chunk
	}
	dlog.Logf("ctl", "id=%s registered pull config: urls=%v proxy=%q (refs=%d)", s.id, src.URLs, src.Proxy, s.refs)
	if s.refs > 0 {
		s.startLocked()
	}
}

func (s *Stream) startLocked() {
	if s.running || s.cfg == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.running = true
	cfg, chunk := *s.cfg, s.chunk
	cfg.Label = s.id
	dlog.Logf("stream", "id=%s puller starting (refs=%d)", s.id, s.refs)
	go puller.Run(ctx, cfg, chunk, s.Publish)
}

func (s *Stream) stopLocked() {
	if s.running && s.cancel != nil {
		dlog.Logf("stream", "id=%s puller stopping", s.id)
		s.cancel()
	}
	s.running = false
}

// attach registers a new live TS viewer and starts the puller on the first
// viewer of a control-managed stream.
func (s *Stream) attach() {
	s.lastAccess.Store(time.Now().UnixNano())
	s.mu.Lock()
	s.refs++
	s.ensureBufferedLocked()
	s.startLocked()
	s.mu.Unlock()
}

// detach drops a live TS viewer. The reaper stops the puller once refs reach 0
// and no HLS access has touched the stream for the grace window.
//
// It marks lastAccess: attach() stamps it on arrival and nothing moves it during
// the session, so without this the mark is as old as the session was long — and
// the moment the last viewer left, `now - lastAccess` already exceeded both
// grace_sec and idle_buffer_grace_sec. The puller was killed and the ring
// collapsed on the very next reaper tick, for every session longer than the grace
// itself, i.e. all of them: a channel change tore down the source ffmpeg and cold-
// started it seconds later, and the gate fired (and forced a heap release) on
// nearly every sweep. The grace windows only mean anything if the clock starts
// when the audience actually leaves.
func (s *Stream) detach() {
	s.lastAccess.Store(time.Now().UnixNano())
	s.mu.Lock()
	s.refs--
	s.mu.Unlock()
}

// touch marks recent interest (an HLS playlist/segment request) and starts the
// puller if a control-managed stream isn't running yet. HLS is poll-based, so it
// holds no ref — the reaper keeps the puller alive via lastAccess instead, so an
// HLS-only audience no longer lets the puller idle-stop under them.
func (s *Stream) touch() {
	s.lastAccess.Store(time.Now().UnixNano())
	s.mu.Lock()
	s.ensureBufferedLocked()
	s.startLocked()
	s.mu.Unlock()
}

// ensureBufferedLocked pumps the ring back to the full prebuffer + HLS view if
// the stream was gated down to the idle floor. Called when a viewer returns (TS
// attach or HLS touch) so the audience gets the configured buffer depth again —
// the ring then refills over the next prebuffer window. Caller holds s.mu.
func (s *Stream) ensureBufferedLocked() {
	if s.buffered || s.mgr == nil {
		return
	}
	s.buffered = true
	s.mgr.applyBufferLocked(s)
	dlog.Logf("buffer", "id=%s ring restored to full (viewer returned)", s.id)
}

// idleStopLocked stops the puller when it is control-managed, has no live
// viewers, and hasn't been accessed within the grace window. Caller holds s.mu.
func (s *Stream) idleStopLocked(now time.Time) {
	if !s.running || s.cfg == nil || s.refs > 0 {
		return
	}
	if idle := now.Sub(time.Unix(0, s.lastAccess.Load())); idle >= s.grace {
		dlog.Logf("reaper", "id=%s idle-stop (no viewers, idle %s ≥ grace %s)", s.id, idle.Round(time.Second), s.grace)
		s.stopLocked()
	}
}

// gateIdleBufferLocked collapses an unwatched stream's ring to the idle floor
// (dropping the HLS view) so idle channels cost near-nothing in RAM. A stream is
// unwatched when it has no live TS viewers and nothing has touched it (TS attach
// or HLS request) within the idle-buffer grace window. Restored the instant a
// viewer returns (ensureBufferedLocked). Returns true when it actually gated this
// call, so the reaper can force the freed heap back to the OS. No-op (false) when
// the gate is disabled (grace <= 0) or the stream is already gated. Caller holds
// s.mu.
func (s *Stream) gateIdleBufferLocked(now time.Time) bool {
	if s.mgr == nil || !s.buffered || s.refs > 0 {
		return false
	}
	graceNS := s.mgr.idleBufferGraceNS.Load()
	if graceNS <= 0 {
		return false
	}
	if now.UnixNano()-s.lastAccess.Load() < graceNS {
		return false
	}
	s.buffered = false
	s.mgr.applyBufferLocked(s)
	s.dropSegCache()
	dlog.Logf("buffer", "id=%s ring gated to idle fraction (%.2f, no viewers)", s.id, s.mgr.idleRatio())
	return true
}

// streamStatus is the control-API GET payload: enough for the PHP auth endpoint
// to decide off-air (a running stream with stale/no data) and for ops.
type streamStatus struct {
	Running     bool  `json:"running"`
	Refs        int   `json:"refs"`
	HasData     bool  `json:"has_data"`
	SinceDataMs int64 `json:"since_data_ms"` // ms since last non-empty publish; -1 if never
}

func (s *Stream) status() streamStatus {
	s.mu.Lock()
	running, refs := s.running, s.refs
	s.mu.Unlock()
	ld := s.lastData.Load()
	st := streamStatus{Running: running, Refs: refs, HasData: ld != 0, SinceDataMs: -1}
	if ld != 0 {
		st.SinceDataMs = time.Since(time.Unix(0, ld)).Milliseconds()
	}
	return st
}

// Manager holds the live streams keyed by id.
type Manager struct {
	mu      sync.Mutex
	streams map[string]*Stream
	maxGOP  int
	// maxPrebufMS/writeTimeout/hlsTargetMS/hlsWindow/idleBuffer* are read off m.mu
	// on hot paths (the client handler, the reaper's buffer gate, the attach/touch
	// buffer restore), so they are atomic — ApplyConfig retunes them live without a
	// lock. grace is read only under m.mu (stream creation), so it stays a plain
	// field guarded by it.
	maxPrebufMS     atomic.Int64 // the buffer/ring size (ms of TS history) + ceiling for a per-viewer burst
	defaultPrebufMS atomic.Int64 // per-viewer join burst (ms) fallback ONLY when the panel passes no ?prebuffer=
	hlsTargetMS     atomic.Int64 // HLS target segment duration (ms); HLS is cut from the ring, not sized by it
	hlsWindow       atomic.Int64 // HLS segments listed in the playlist (display cap)
	grace           time.Duration
	writeTimeout    atomic.Int64 // per-write deadline for live-TS viewers (nanoseconds)

	// idleBufferGraceNS is the no-viewer window (ns) before an unwatched stream's
	// ring collapses, 0 = gate off; idleBufferRatioBits is the fraction of the
	// buffer kept while gated (math.Float64bits, read on the hot path).
	idleBufferGraceNS   atomic.Int64
	idleBufferRatioBits atomic.Uint64

	// viewerIdleNS drops a live-TS viewer that has received nothing for this long
	// (0 = never); read on the connect path. gatedSinceScavenge is set by the
	// reaper when it collapses a ring, so the memory scavenger knows there is real
	// garbage to hand back and can bypass its rate floor for it.
	viewerIdleNS       atomic.Int64
	gatedSinceScavenge atomic.Bool

	ingestDir string // base dir for per-stream push-fed ingest sockets

	ffmpegBin string       // ffmpeg path for the "send message" drawtext overlay
	fontPath  string       // font file for the overlay text
	signals   *signalStore // pending per-uuid "send message" overlays

	// defaultChunk is the source read size stamped onto a stream at creation
	// (read under m.mu). sourceInsecure is read off m.mu when registering a pull,
	// so it is atomic. Both are retunable live via ApplyConfig (new streams/pulls
	// pick up the change).
	defaultChunk   int
	sourceInsecure atomic.Bool  // default for puller.Source.Insecure on registered sources
	sourceBackend  atomic.Value // string: how a non-mp2t source becomes MPEG-TS
}

// SetSourceInsecure sets whether pull sources skip upstream TLS verification
// (applied to every control-registered source). See the -source-insecure flag.
func (m *Manager) SetSourceInsecure(v bool) { m.sourceInsecure.Store(v) }

// SetIngestDir sets the directory for per-stream ingest sockets (non-proxy tee).
func (m *Manager) SetIngestDir(dir string) { m.ingestDir = dir }

// SetOverlay configures the ffmpeg binary + font used to burn an admin
// "send message" text banner onto a signalled viewer's stream. Empty values
// disable the overlay (a queued signal is then dropped, never breaking playback).
func (m *Manager) SetOverlay(ffmpegBin, fontPath string) {
	m.ffmpegBin = ffmpegBin
	m.fontPath = fontPath
}

// SetWriteTimeout overrides the per-write deadline for live-TS viewers (0 keeps
// the default). A stalled write past this window drops the viewer.
func (m *Manager) SetWriteTimeout(d time.Duration) {
	if d > 0 {
		m.writeTimeout.Store(int64(d))
	}
}

// NewManager configures hub join size, the client-prebuffer ceiling (ms of live
// TS history retained per stream), HLS segment target/window, and the idle-stop
// grace period for control-managed streams.
func NewManager(maxGOP int, maxPrebufMS int64, hlsTarget float64, hlsWindow int, grace time.Duration) *Manager {
	m := &Manager{
		streams:      make(map[string]*Stream),
		maxGOP:       maxGOP,
		grace:        grace,
		defaultChunk: defaultChunk,
		signals:      newSignalStore(),
	}
	m.maxPrebufMS.Store(maxPrebufMS)
	m.defaultPrebufMS.Store(int64(defaults.CfgDefaultPrebufferSec) * 1000) // fallback only; panel is authoritative
	m.hlsTargetMS.Store(int64(hlsTarget * 1000))
	m.hlsWindow.Store(int64(hlsWindow))
	m.writeTimeout.Store(int64(defaultWriteTimeout))
	m.sourceInsecure.Store(true)
	m.sourceBackend.Store(defaults.CfgSourceBackend)
	m.idleBufferGraceNS.Store(int64(time.Duration(defaults.CfgIdleBufferGraceSec) * time.Second))
	m.idleBufferRatioBits.Store(math.Float64bits(defaults.CfgIdleBufferRatio))
	m.viewerIdleNS.Store(int64(time.Duration(defaults.CfgViewerIdleTimeoutSec) * time.Second))
	return m
}

// idleRatio returns the configured fraction of the buffer kept while gated.
func (m *Manager) idleRatio() float64 { return math.Float64frombits(m.idleBufferRatioBits.Load()) }

// resolvePrebufMS decides a viewer's join-burst depth (ms). The panel is
// authoritative: `param` is the ?prebuffer= it passed (client or restreamer
// value) and is honored as-is when present, including an explicit "0". A blank
// param (the request carried none) falls back to the daemon default. Clamped to
// the ring (maxPrebufMS).
func (m *Manager) resolvePrebufMS(param string) int64 {
	prebufMS := m.defaultPrebufMS.Load()
	if param != "" {
		if sec, err := strconv.Atoi(param); err == nil && sec >= 0 {
			prebufMS = int64(sec) * 1000
		}
	}
	if mp := m.maxPrebufMS.Load(); prebufMS > mp {
		prebufMS = mp
	}
	return prebufMS
}

// ApplyConfig live-applies operator tuning (from the polled config file) to the
// running daemon. New streams pick up the new values at creation; every existing
// stream's prebuffer ring and HLS window are reconfigured in place, so lowering
// them frees memory within one poll — no restart, no viewer drop. Safe to call
// from the config-poll goroutine while streams are serving.
func (m *Manager) ApplyConfig(v config.Values) {
	m.maxPrebufMS.Store(int64(v.PrebufferMaxSec) * 1000)
	m.defaultPrebufMS.Store(int64(v.DefaultPrebufferSec) * 1000)
	m.hlsTargetMS.Store(int64(v.HLSTargetSec * 1000))
	m.hlsWindow.Store(int64(v.HLSWindow))
	m.writeTimeout.Store(int64(time.Duration(v.WriteTimeoutSec) * time.Second))
	m.sourceInsecure.Store(v.SourceInsecure)
	m.sourceBackend.Store(v.SourceBackend)
	m.idleBufferGraceNS.Store(int64(time.Duration(v.IdleBufferGraceSec) * time.Second))
	m.idleBufferRatioBits.Store(math.Float64bits(v.IdleBufferRatio))
	m.viewerIdleNS.Store(int64(time.Duration(v.ViewerIdleTimeoutSec) * time.Second))

	// Update the new-stream defaults and snapshot the live set under m.mu, then
	// reconfigure each stream outside the lock (each takes its own hub/seg lock;
	// holding m.mu across all of them would block GetOrCreate needlessly).
	// maxGOP/defaultChunk apply to streams created after this point (existing
	// hubs keep the join cap they were built with); prebuffer/HLS retune live.
	m.mu.Lock()
	m.grace = time.Duration(v.GraceSec) * time.Second
	m.maxGOP = v.MaxGOPBytes
	m.defaultChunk = v.ChunkBytes
	streams := make([]*Stream, 0, len(m.streams))
	for _, st := range m.streams {
		streams = append(streams, st)
	}
	m.mu.Unlock()

	// Reconfigure each stream to the depth its current audience warrants: a
	// fully-buffered (watched) stream to the new full prebuffer/HLS, a gated
	// (idle) one to the new idle floor — so a config change never un-gates an
	// unwatched stream. st.mu orders before the hub lock everywhere.
	for _, st := range streams {
		st.mu.Lock()
		m.applyBufferLocked(st)
		st.mu.Unlock()
	}
}

// applyBufferLocked (re)tunes st.Hub from the live config and its gate state. The
// buffer (ring) + HLS view are the same for watched and idle; the gate only flips
// the ring to the idle fraction. HLS keeps being cut from the ring either way, so
// an idle stream still serves a (shorter) playlist. Caller holds st.mu.
func (m *Manager) applyBufferLocked(st *Stream) {
	st.Hub.Configure(m.maxPrebufMS.Load(), m.hlsTargetMS.Load(), int(m.hlsWindow.Load()))
	st.Hub.SetIdleRatio(m.idleRatio())
	st.Hub.SetGated(!st.buffered)
}

// StartReaper runs the idle-stop sweep until ctx is cancelled: control-managed
// streams with no live viewers and no HLS access within the grace window get
// their puller stopped. This is the single idle-stop path for both TS and HLS
// audiences. Call once from main; tests that don't need reaping omit it.
func (m *Manager) StartReaper(ctx context.Context) {
	interval := m.grace / 2
	if interval < time.Second {
		interval = time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				m.mu.Lock()
				streams := make([]*Stream, 0, len(m.streams))
				for _, st := range m.streams {
					streams = append(streams, st)
				}
				m.mu.Unlock()
				gated := false
				for _, st := range streams {
					st.mu.Lock()
					st.idleStopLocked(now)
					if st.gateIdleBufferLocked(now) {
						gated = true
					}
					st.mu.Unlock()
				}
				// A gate collapsed at least one ring: the dropped GOP bytes are now
				// GC garbage, but Go hands freed pages back to the OS only lazily,
				// so RSS would sit flat for minutes. Hand that fact to the memory
				// scavenger rather than forcing a full stop-the-world GC here: this
				// sweep runs every grace/2 (5 s at the default grace), and calling
				// FreeOSMemory from it put the daemon in near-continuous full
				// collections, re-faulting the pages it had just returned. The
				// scavenger owns the release, and this flag lets it skip its rate
				// floor because the garbage is known-real.
				if gated {
					m.gatedSinceScavenge.Store(true)
				}
			}
		}
	}()
}

// StartMemoryScavenger periodically returns idle heap to the OS. The Go runtime
// frees dropped GOP/snapshot memory to its own heap promptly but hands the pages
// back to the OS only lazily (the background scavenger paces itself over minutes
// to hours), so after a viewer burst subsides the process RSS sits at its
// high-water mark indefinitely. This sweep checks how much freed-but-unreturned
// heap the runtime is holding and, when it exceeds threshold, forces the release
// so idle RSS tracks the working set instead. Call once from main.
//
// Two things keep the cure from being worse than the disease:
//
//   - The check reads runtime/metrics, which does NOT stop the world.
//     runtime.ReadMemStats does, and it ran on every sweep — a global pause every
//     20 s, charged to every stream, to answer a question about none of them.
//   - A forced release is a full GC plus a page-return sweep. On a busy daemon the
//     idle-heap threshold is met almost continuously, so releases are floored at
//     minGap apart; otherwise the process lives in back-to-back collections and
//     thrashes the pages it just gave back. A ring collapse (the reaper's idle
//     gate) is known-real garbage and skips the floor.
func (m *Manager) StartMemoryScavenger(ctx context.Context, interval time.Duration, threshold uint64) {
	m.startMemoryScavenger(ctx, interval, threshold, defaults.MemScavengeMinGap)
}

func (m *Manager) startMemoryScavenger(ctx context.Context, interval time.Duration, threshold uint64, minGap time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		// heap/free is memory the runtime holds free and STILL BACKED by physical
		// pages — i.e. exactly what a release would hand back. (It is disjoint from
		// heap/released, the part already returned, so this is the runtime/metrics
		// spelling of the old HeapIdle−HeapReleased, not a term of it.)
		samples := []metrics.Sample{{Name: "/memory/classes/heap/free:bytes"}}
		var lastRelease time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				metrics.Read(samples)
				retained := samples[0].Value.Uint64()
				if retained < threshold {
					continue
				}
				gated := m.gatedSinceScavenge.Swap(false)
				if !gated && !lastRelease.IsZero() && now.Sub(lastRelease) < minGap {
					continue // rate floor: not worth another full GC yet
				}
				lastRelease = now
				debug.FreeOSMemory()
				dlog.Logf("mem", "scavenged: returned ~%dMB idle heap to OS (gate=%v)", retained>>20, gated)
			}
		}
	}()
}

// StartDebugStats logs a compact per-stream state snapshot every `every` while
// debug mode is on: for each stream, whether its puller is running (or it is
// push-fed via ingest), how many live-TS viewers hold a ref, the hub subscriber
// count, tracked viewer uuids, and the age of the last data (a growing data_age
// on a running stream is the off-air signal). This is the "what is the daemon
// doing right now" view. No-op when debug is off or every <= 0, so it costs
// nothing in normal operation. Call once from main.
func (m *Manager) StartDebugStats(ctx context.Context, every time.Duration) {
	if !dlog.On() || every <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.mu.Lock()
				streams := make([]*Stream, 0, len(m.streams))
				for _, st := range m.streams {
					streams = append(streams, st)
				}
				m.mu.Unlock()
				if len(streams) == 0 {
					dlog.Logf("stats", "no streams registered")
					continue
				}
				for _, st := range streams {
					st.mu.Lock()
					running, refs, ingesting := st.running, st.refs, st.ingestLn != nil
					st.mu.Unlock()
					st.connMu.Lock()
					conns := len(st.conns)
					st.connMu.Unlock()
					dataAge := "never"
					if ld := st.lastData.Load(); ld != 0 {
						dataAge = time.Since(time.Unix(0, ld)).Round(time.Millisecond).String()
					}
					// nokf > 0 means the source carries no random_access_indicator: the
					// ring is being cut on the byte cap instead of on keyframes, and
					// HLS cannot produce segments for it at all.
					dlog.Logf("stats", "id=%s running=%v ingest=%v refs=%d subs=%d conns=%d data_age=%s nokf=%d",
						st.id, running, ingesting, refs, st.Hub.Count(), conns, dataAge, st.Hub.NoKeyframeCuts())
				}
			}
		}
	}()
}

// Get returns the stream for id, or nil.
func (m *Manager) Get(id string) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[id]
}

// GetOrCreate returns the stream for id, creating it if necessary.
func (m *Manager) GetOrCreate(id string) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.streams[id]
	if st == nil {
		st = &Stream{
			id:       id,
			mgr:      m,
			Hub:      hub.New(m.maxGOP, m.maxPrebufMS.Load()),
			chunk:    m.defaultChunk,
			grace:    m.grace,
			buffered: true,
		}
		// Size the single cache (the buffer) and the HLS view cut from it. A newly
		// created stream starts fully buffered; the reaper gates it to the idle
		// fraction if it draws no audience.
		st.Hub.Configure(m.maxPrebufMS.Load(), m.hlsTargetMS.Load(), int(m.hlsWindow.Load()))
		st.Hub.SetIdleRatio(m.idleRatio())
		m.streams[id] = st
		dlog.Logf("stream", "id=%s created", id)
	}
	return st
}

// Register sets a stream's pull config (control API). The node-wide source
// backend applies unless the registration pinned one for this stream.
func (m *Manager) Register(id string, src puller.Source, chunk int) {
	src.Insecure = m.sourceInsecure.Load()
	if src.Backend == "" {
		src.Backend = m.backend()
	}
	m.GetOrCreate(id).setConfig(src, chunk)
}

// backend returns the node-wide source backend.
func (m *Manager) backend() string {
	if v, ok := m.sourceBackend.Load().(string); ok && v != "" {
		return v
	}
	return defaults.CfgSourceBackend
}

// RunPinned feeds a stream for the whole process lifetime (launch/test mode,
// -id/-source): it stores the pull config, starts the puller immediately bound
// to ctx, and marks the stream running with a permanent pin ref so status and
// the debug snapshot reflect it and the reaper never idle-stops it. Unlike a
// control-managed stream it does not wait for a viewer. Cancelling ctx (SIGINT/
// SIGTERM) stops the puller and clears running.
func (m *Manager) RunPinned(ctx context.Context, id string, src puller.Source, chunk int) {
	src.Insecure = m.sourceInsecure.Load()
	if src.Backend == "" {
		src.Backend = m.backend()
	}
	src.Label = id
	st := m.GetOrCreate(id)
	st.mu.Lock()
	c := src
	st.cfg = &c
	if chunk > 0 {
		st.chunk = chunk
	}
	st.running = true
	st.refs++ // permanent pin: no viewer needed, and the reaper leaves refs>0 alone
	cfg, ch := *st.cfg, st.chunk
	st.mu.Unlock()
	st.lastAccess.Store(time.Now().UnixNano())
	dlog.Logf("stream", "id=%s launch puller starting (pinned)", id)
	go func() {
		puller.Run(ctx, cfg, ch, st.Publish)
		st.mu.Lock()
		st.running = false
		st.mu.Unlock()
		dlog.Logf("stream", "id=%s launch puller stopped", id)
	}()
}

// RegisterIngest puts a stream in push-fed mode: it listens on
// <ingestDir>/<id>.sock for the producer (the stream's ffmpeg tee) and fans out
// whatever it pushes. Returns the socket path the producer must connect to.
func (m *Manager) RegisterIngest(id string, chunk int) (string, error) {
	if m.ingestDir == "" {
		return "", errors.New("ingest dir not set")
	}
	sock := filepath.Join(m.ingestDir, id+".sock")
	st := m.GetOrCreate(id)
	st.mu.Lock()
	err := st.startIngestLocked(sock, chunk)
	st.mu.Unlock()
	if err != nil {
		dlog.Logf("ingest", "id=%s listen failed on %s: %v", id, sock, err)
		return "", err
	}
	dlog.Logf("ingest", "id=%s listening on %s", id, sock)
	return sock, nil
}

// Unregister stops and removes a control-managed stream (pull or ingest).
//
// It also drops the viewers still attached. They are about to be served by
// nothing — the puller is stopped and the producer hung up — and once the stream
// leaves the registry they are invisible to /connections, so PHP closes their
// lines_live rows while their handler goroutines sit forever on a hub that will
// never publish again, pinning the Stream, its hub and its whole ring. Closing
// the subscribers lets each serveLive return and run its deferred cleanup, so the
// viewer reconnects (and re-authorises) instead of freezing on an orphan.
func (m *Manager) Unregister(id string) {
	if st := m.Get(id); st != nil {
		st.mu.Lock()
		st.cfg = nil
		st.stopLocked()
		st.stopIngestLocked()
		st.mu.Unlock()
		if n := st.Hub.CloseAll(); n > 0 {
			dlog.Logf("ctl", "id=%s dropped %d attached viewer(s) on teardown", id, n)
		}
		st.dropSegCache()
	}
	m.mu.Lock()
	delete(m.streams, id)
	m.mu.Unlock()
	dlog.Logf("ctl", "id=%s unregistered and removed", id)
}

// ClientHandler routes the nginx-facing surface: live TS, HLS, health.
func (m *Manager) ClientHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/live/", m.serveLive)
	mux.HandleFunc("/hls/", m.serveHLS)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

// ControlHandler routes the PHP-only control surface.
func (m *Manager) ControlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/streams/", m.serveControl)
	mux.HandleFunc("/ingest/", m.serveIngest)
	mux.HandleFunc("/probe/", m.serveProbe)
	mux.HandleFunc("/connections", m.serveConnections)
	mux.HandleFunc("/rates", m.serveRates)
	mux.HandleFunc("/signal/", m.serveSignal)
	return mux
}

// serveConnections returns every currently-connected live-TS viewer uuid across
// all streams (the ?c= values), for the fanout_sync reconciler.
func (m *Manager) serveConnections(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	streams := make([]*Stream, 0, len(m.streams))
	for _, st := range m.streams {
		streams = append(streams, st)
	}
	m.mu.Unlock()

	uuids := make([]string, 0)
	for _, st := range streams {
		uuids = append(uuids, st.connUUIDs()...)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(uuids)
}

// serveRates returns { "<uuid>": <avgKBs>, ... } across all streams — each
// active live-TS viewer's average delivery rate (KB/s) since it attached. The
// fanout_sync reconciler turns this into lines_live.divergence for daemon-served
// viewers, restoring the transfer telemetry the legacy chase-read loop wrote to
// DIVERGENCE_TMP_PATH before the byte path left PHP (ADR 0003, P4). On the rare
// chance a uuid is live on more than one stream, the higher rate wins.
func (m *Manager) serveRates(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	streams := make([]*Stream, 0, len(m.streams))
	for _, st := range m.streams {
		streams = append(streams, st)
	}
	m.mu.Unlock()

	rates := make(map[string]int)
	for _, st := range streams {
		for u, kbps := range st.connRates() {
			// Include every active viewer, a genuine 0 KB/s (stalled/just-fed)
			// among them — a stalled reading is exactly the divergence signal we
			// want to surface, so keep it. On a cross-stream uuid clash the higher
			// rate wins.
			if v, ok := rates[u]; !ok || kbps > v {
				rates[u] = kbps
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rates)
}

// serveSignal queues an admin "send message" text overlay for one viewer uuid
// (POST /signal/<uuid>). PHP's signal_send action calls this; the overlay is
// applied one-shot to that viewer's next HLS segment (and TS window), then
// cleared — reproducing the legacy admin "send message" feature now that clients are
// daemon-only. The overlay is best-effort: no ffmpeg/font ⇒ the signal is a
// no-op, never a broken stream.
func (m *Manager) serveSignal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	uuid := strings.TrimPrefix(r.URL.Path, "/signal/")
	if uuid == "" {
		http.Error(w, "missing uuid", http.StatusBadRequest)
		return
	}
	var body struct {
		Message  string `json:"message"`
		FontSize int    `json:"font_size"`
		Color    string `json:"font_color"`
		XYOffset string `json:"xy_offset"` // "<x>x<y>" or empty (⇒ random, like legacy)
		TTL      int    `json:"ttl"`       // seconds a viewer has to pick it up; 0 = default
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Message == "" {
		http.Error(w, "bad signal", http.StatusBadRequest)
		return
	}
	if body.FontSize <= 0 {
		body.FontSize = 20
	}
	ttl := body.TTL
	if ttl <= 0 {
		ttl = 30
	}
	x, y := parseXY(body.XYOffset)
	m.signals.set(uuid, pendingSignal{
		text:     body.Message,
		fontSize: body.FontSize,
		color:    body.Color,
		x:        x,
		y:        y,
		expires:  time.Now().Add(time.Duration(ttl) * time.Second),
	})
	dlog.Logf("signal", "queued overlay uuid=%s ttl=%ds msg=%q", uuid, ttl, body.Message)
	w.WriteHeader(http.StatusNoContent)
}

// serveProbe supports off-air detection (ADR 0003, Phase C): it prewarms a
// registered stream (starts the puller for a pull-fed/proxy stream, like a
// viewer would) and waits up to `?wait=<ms>` for the source to produce data,
// then returns the status. PHP calls this after registering a proxy source and
// shows the not-on-air page when `has_data` is still false — matching the legacy
// startProxy path, whose viewer would otherwise hang on a dead source. The
// prewarmed puller keeps running (touch bumped lastAccess) so the real viewer's
// connection attaches to it; the reaper stops it if no viewer arrives.
func (m *Manager) serveProbe(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/probe/")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	st := m.Get(id)
	if st == nil {
		http.NotFound(w, r)
		return
	}

	waitMs := defaults.ProbeDefaultWaitMS
	if q := r.URL.Query().Get("wait"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v >= 0 {
			waitMs = v
		}
	}
	if waitMs > defaults.ProbeMaxWaitMS {
		waitMs = defaults.ProbeMaxWaitMS
	}

	st.touch() // start the puller (pull-fed) + bump lastAccess
	dlog.Logf("ctl", "id=%s probe: prewarming, waiting up to %dms for data", id, waitMs)
	deadline := time.Now().Add(time.Duration(waitMs) * time.Millisecond)
	for st.lastData.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(defaults.ProbePollInterval)
	}

	status := st.status()
	dlog.Logf("ctl", "id=%s probe result: has_data=%v since_data_ms=%d", id, status.HasData, status.SinceDataMs)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

// serveIngest is the push-fed control surface: PUT /ingest/<id> starts (or
// confirms) the per-stream ingest listener and returns its socket path; DELETE
// tears the stream down. Called by StreamProcess before it launches the stream's
// ffmpeg tee.
func (m *Manager) serveIngest(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/ingest/")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		var c struct {
			Chunk int    `json:"chunk"`
			Key   string `json:"key"` // hex AES-128 key for encrypted HLS (optional)
			IV    string `json:"iv"`  // hex AES-128-CBC IV
		}
		_ = json.NewDecoder(r.Body).Decode(&c) // body optional
		sock, err := m.RegisterIngest(id, c.Chunk)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		m.GetOrCreate(id).setEnc(c.Key, c.IV)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"socket": sock})
	case http.MethodDelete:
		m.Unregister(id)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type streamConfig struct {
	URLs   []string `json:"urls"`
	UA     string   `json:"ua"`
	Proxy  string   `json:"proxy"`
	Cookie string   `json:"cookie"`
	Ffmpeg string   `json:"ffmpeg"`
	Chunk  int      `json:"chunk"`
	Key    string   `json:"key"` // hex AES-128 key for encrypted HLS (optional)
	IV     string   `json:"iv"`  // hex AES-128-CBC IV
	// Backend pins how THIS stream's non-mp2t source is converted, overriding
	// source_backend from the config file: "auto", "ffmpeg" or "native". Empty
	// (the usual case) takes the node-wide setting, so the panel only has to
	// send it for a channel that needs pinning.
	Backend string `json:"backend"`
}

// normalizeBackend keeps an unknown per-stream backend from pinning a channel to
// something that does not exist: it falls through to the node-wide setting,
// exactly as an omitted field does. A typo must never take a channel off air.
func normalizeBackend(b string) string {
	switch b {
	case puller.BackendAuto, puller.BackendFfmpeg, puller.BackendNative:
		return b
	}
	return ""
}

func (m *Manager) serveControl(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/streams/")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		var c streamConfig
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil || len(c.URLs) == 0 {
			http.Error(w, "bad config", http.StatusBadRequest)
			return
		}
		m.Register(id, puller.Source{
			URLs:      c.URLs,
			UserAgent: c.UA,
			Proxy:     c.Proxy,
			Cookie:    c.Cookie,
			FfmpegBin: c.Ffmpeg,
			Backend:   normalizeBackend(c.Backend),
		}, c.Chunk)
		m.GetOrCreate(id).setEnc(c.Key, c.IV)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		// Status for the PHP auth endpoint (off-air detection) and ops.
		st := m.Get(id)
		if st == nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st.status())
	case http.MethodDelete:
		m.Unregister(id)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *Manager) serveLive(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/live/")
	st := m.Get(id)
	if id == "" || st == nil {
		http.NotFound(w, r)
		return
	}

	// Join burst: a keyframe-aligned history burst sent on connect so the viewer
	// starts with a real buffer instead of ~1 GOP. The PANEL is authoritative — it
	// knows client vs restreamer and passes the right value in ?prebuffer= (its
	// client_prebuffer / restreamer_prebuffer setting), which we honor AS IS,
	// including an explicit 0 (= current GOP). The daemon's defaultPrebufMS is only
	// a fallback for a request that carries no prebuffer param at all. Clamped to
	// the ring (maxPrebufMS).
	prebufMS := m.resolvePrebufMS(r.URL.Query().Get("prebuffer"))

	sub, snap := st.Hub.Subscribe(prebufMS)
	defer st.Hub.Unsubscribe(sub)
	st.attach()
	defer st.detach()

	// Track this viewer by its connection uuid (from live.php's X-Accel URL) so
	// fanout_sync can detect its disconnect and close the lines_live row, and so
	// delivered bytes are accounted for the per-viewer rate telemetry (P4).
	uuid := r.URL.Query().Get("c")
	codec := r.URL.Query().Get("vc") // video codec, for a possible "send message" overlay
	var cs *connStat
	if uuid != "" {
		cs = st.addConn(uuid)
		defer st.removeConn(uuid)
	}

	// Debug: narrate this viewer's whole live-TS session — attach, then a single
	// disconnect line with the cause (client closed / hub dropped it as too slow /
	// write stalled past the timeout), how long it lasted and how much it got.
	start := time.Now()
	reason := "client closed"
	dlog.Logf("viewer", "id=%s live attach uuid=%s prebuffer=%dms subs=%d", id, uuid, prebufMS, st.Hub.Count())
	defer func() {
		var sent int64
		if cs != nil {
			sent = cs.bytes.Load()
		}
		dlog.Logf("viewer", "id=%s live detach uuid=%s reason=%q dur=%s sent=%dKB", id, uuid, reason, time.Since(start).Round(time.Millisecond), sent/1024)
	}()

	w.Header().Set("Content-Type", "video/mp2t")
	w.Header().Set("Cache-Control", "no-store")

	// Bound every write with a deadline. A viewer that stops draining its socket
	// without cleanly closing the connection (a backgrounded/force-switched
	// player, a dropped mobile link) fills its OS send buffer and blocks the
	// write forever — neither sub.Done() (the hub drops the slow subscriber) nor
	// r.Context().Done() can interrupt an in-flight Write. The deadline turns that
	// stall into an error so the deferred detach/removeConn run and fanout_sync
	// can close the lines_live row, instead of a ghost connection lingering on the
	// stream the viewer already left.
	rc := http.NewResponseController(w)
	write := func(b []byte) error {
		if err := rc.SetWriteDeadline(time.Now().Add(time.Duration(m.writeTimeout.Load()))); err != nil {
			// Deadlines unsupported (shouldn't happen for a real conn) — fall back
			// to a plain write rather than aborting the viewer.
			n, werr := w.Write(b)
			if cs != nil && n > 0 {
				cs.bytes.Add(int64(n))
			}
			return werr
		}
		n, err := w.Write(b)
		if cs != nil && n > 0 {
			cs.bytes.Add(int64(n))
		}
		if err != nil {
			return err
		}
		return rc.Flush()
	}

	// Guard against a viewer that receives nothing at all. The per-write deadline
	// above can only fire while there are bytes to write, so it does not cover the
	// other half of the problem: a stream that goes off-air stops producing chunks,
	// nothing is ever written, and a client whose socket is half-open (a dropped
	// mobile link that sent no FIN nor RST) is never noticed by anyone — not by us,
	// and not by nginx, which is equally idle. That connection sat in the select
	// below forever, holding its uuid in /connections as a ghost the reconciler
	// could never clear. Polled on a coarse ticker rather than a per-chunk timer
	// reset: quarter-timeout precision is plenty, and the hot path stays a
	// timestamp store.
	var idleC <-chan time.Time
	idleTimeout := time.Duration(m.viewerIdleNS.Load())
	if idleTimeout > 0 {
		tick := idleTimeout / 4
		if tick <= 0 {
			tick = idleTimeout // never hand NewTicker a zero interval
		}
		tk := time.NewTicker(tick)
		defer tk.Stop()
		idleC = tk.C
	}
	lastChunk := time.Now()

	// Write the join burst, then return its (pooled) buffer at once — the viewer
	// holds no reference to it past this write, so it can be reused by the next
	// connect instead of lingering as garbage for the whole session.
	var snapErr error
	if len(snap) > 0 {
		snapErr = write(snap)
	}
	hub.ReleaseSnapshot(snap)
	snap = nil
	if snapErr != nil {
		reason = writeFailReason(snapErr)
		return
	}
	lastChunk = time.Now()
	for {
		// Admin "send message" overlay (rare): if one is queued for this viewer,
		// burn it onto a short window of the stream via a transient ffmpeg, then
		// fall back to the raw fan-out. peek keeps the hot path a single map read.
		if uuid != "" && m.signals.peek(uuid) {
			if sig, ok := m.signals.take(uuid); ok {
				dlog.Logf("signal", "id=%s uuid=%s applying overlay to live TS window", id, uuid)
				if !m.overlayTSWindow(st, sub, write, sig, codec) {
					reason = "client closed (during overlay)"
					return
				}
				// The overlay goroutine drained sub.C() for the window, so no chunk
				// reached the idle check meanwhile — don't count that as silence.
				lastChunk = time.Now()
			}
		}
		select {
		case b := <-sub.C():
			lastChunk = time.Now()
			if err := write(b); err != nil {
				reason = writeFailReason(err)
				return
			}
		case now := <-idleC:
			if now.Sub(lastChunk) < idleTimeout {
				continue
			}
			reason = "no data for " + idleTimeout.String() + " (source off-air; dropped)"
			return
		case <-sub.Done():
			reason = "dropped: too slow (hub buffer full)"
			return
		case <-r.Context().Done():
			reason = "client closed"
			return
		}
	}
}

// writeFailReason labels why a live-TS write failed: a stall past the per-write
// deadline (a backgrounded/half-open player whose socket buffer filled) reads
// differently from a plain broken pipe, and telling them apart is the whole
// point of watching a stuck viewer in debug mode.
func writeFailReason(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "write stalled past timeout (dropped)"
	}
	return "write failed: " + err.Error()
}

// serveHLS handles /hls/<id>/index.m3u8 and /hls/<id>/<seq>.ts.
func (m *Manager) serveHLS(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/hls/")
	id, file, ok := strings.Cut(rest, "/")
	if !ok || id == "" {
		http.NotFound(w, r)
		return
	}
	st := m.Get(id)
	if st == nil {
		http.NotFound(w, r)
		return
	}
	// HLS is poll-based and holds no ref; touch() keeps the puller alive (and
	// starts it for an HLS-only audience) via the reaper's lastAccess window.
	st.touch()

	switch {
	case file == "index.m3u8":
		pl := st.Hub.HLSPlaylist()
		if pl == "" {
			dlog.Logf("hls", "id=%s playlist requested but no segments yet (warming up / off-air)", id)
			http.Error(w, "no segments yet", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(pl))

	case strings.HasSuffix(file, ".ts"):
		seq, err := strconv.Atoi(strings.TrimSuffix(file, ".ts"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		// Admin "send message" overlay for this viewer (one-shot): burn the text
		// banner into this one segment, then the signal is cleared. Applied before
		// encryption so the client still decrypts normally. ?c=<uuid> identifies
		// the viewer, ?vc=<codec> its video codec (from segment.php's token). These
		// bytes are this viewer's alone, so this path bypasses the shared cache
		// (both ways: it neither reads a cached segment nor poisons one).
		uuid := r.URL.Query().Get("c")
		if uuid != "" && m.signals.peek(uuid) {
			if sig, ok := m.signals.take(uuid); ok {
				data := st.Hub.HLSSegment(seq)
				if data == nil {
					http.NotFound(w, r)
					return
				}
				data = st.encryptSegment(m.overlaySegment(data, sig, r.URL.Query().Get("vc")))
				w.Header().Set("Content-Type", "video/mp2t")
				_, _ = w.Write(data)
				return
			}
		}
		// Assembled from the ring and encrypted ONCE per segment, then shared by
		// every viewer asking for it.
		data := st.hlsSegment(seq)
		if data == nil {
			dlog.Logf("hls", "id=%s segment %d not found (rolled out of window or never existed)", id, seq)
			http.NotFound(w, r)
			return
		}
		dlog.Logf("hls", "id=%s segment %d served (%dKB)", id, seq, len(data)/1024)
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(data)

	default:
		http.NotFound(w, r)
	}
}
