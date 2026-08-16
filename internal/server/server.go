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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Vateron-Media/xc_fanout/internal/hlscrypt"
	"github.com/Vateron-Media/xc_fanout/internal/hlsseg"
	"github.com/Vateron-Media/xc_fanout/internal/hub"
	"github.com/Vateron-Media/xc_fanout/internal/ingest"
	"github.com/Vateron-Media/xc_fanout/internal/puller"
)

const defaultChunk = 12032

// Stream bundles the TS fan-out (Hub) and in-memory HLS (Seg) for one source,
// plus its on-demand lifecycle state.
type Stream struct {
	Hub *hub.Hub
	Seg *hlsseg.Segmenter

	mu      sync.Mutex
	cfg     *puller.Source // nil = externally fed (launch mode / ingest); set = daemon pulls
	chunk   int
	grace   time.Duration
	running bool
	cancel  context.CancelFunc
	refs    int // live TS viewers currently connected

	ingestLn   net.Listener // non-nil = push-fed: the producer (ffmpeg tee) connects here
	ingestSock string       // path of the ingest listener socket (for cleanup)

	lastData   atomic.Int64 // UnixNano of the last non-empty Publish (0 = never); off-air signal
	lastAccess atomic.Int64 // UnixNano of the last viewer touch (TS attach or HLS request)

	connMu sync.Mutex     // guards conns
	conns  map[string]int // active live-TS viewer uuids (from the ?c= param) → refcount

	encMu  sync.RWMutex // guards hlsKey/hlsIV
	hlsKey []byte       // AES-128 key for encrypted HLS segments (nil = plain)
	hlsIV  []byte       // AES-128-CBC IV
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

// addConn records an active live-TS viewer by its connection uuid (passed by
// live.php via the X-Accel URL). PHP marks these lines_live rows pid=0 and the
// fanout_sync daemon reconciles them against this set — closing rows whose uuid
// is no longer connected here, since PHP can't see the disconnect under X-Accel.
func (s *Stream) addConn(uuid string) {
	s.connMu.Lock()
	if s.conns == nil {
		s.conns = make(map[string]int)
	}
	s.conns[uuid]++
	s.connMu.Unlock()
}

func (s *Stream) removeConn(uuid string) {
	s.connMu.Lock()
	if s.conns != nil {
		if s.conns[uuid] <= 1 {
			delete(s.conns, uuid)
		} else {
			s.conns[uuid]--
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
			go func(c net.Conn) {
				defer c.Close()
				_ = ingest.Copy(c, ch, s.Publish)
			}(conn)
		}
	}()
	return nil
}

// stopIngestLocked closes the ingest listener and removes its socket. In-flight
// producer connections end when the producer (ffmpeg) itself stops. Caller holds
// s.mu.
func (s *Stream) stopIngestLocked() {
	if s.ingestLn != nil {
		_ = s.ingestLn.Close()
		s.ingestLn = nil
	}
	if s.ingestSock != "" {
		_ = os.Remove(s.ingestSock)
		s.ingestSock = ""
	}
}

// Publish feeds one packet-aligned chunk into both the TS fan-out and the HLS
// segmenter, and records data liveness (for off-air detection via status()).
func (s *Stream) Publish(chunk []byte) {
	if len(chunk) > 0 {
		s.lastData.Store(time.Now().UnixNano())
	}
	s.Hub.Publish(chunk)
	s.Seg.Feed(chunk)
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
	go puller.Run(ctx, cfg, chunk, s.Publish)
}

func (s *Stream) stopLocked() {
	if s.running && s.cancel != nil {
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
	s.startLocked()
	s.mu.Unlock()
}

// detach drops a live TS viewer. The reaper stops the puller once refs reach 0
// and no HLS access has touched the stream for the grace window.
func (s *Stream) detach() {
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
	s.startLocked()
	s.mu.Unlock()
}

// idleStopLocked stops the puller when it is control-managed, has no live
// viewers, and hasn't been accessed within the grace window. Caller holds s.mu.
func (s *Stream) idleStopLocked(now time.Time) {
	if !s.running || s.cfg == nil || s.refs > 0 {
		return
	}
	if now.Sub(time.Unix(0, s.lastAccess.Load())) >= s.grace {
		s.stopLocked()
	}
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
	mu        sync.Mutex
	streams     map[string]*Stream
	maxGOP      int
	maxPrebufMS int64 // ceiling for per-viewer client prebuffer (ms of TS history)
	hlsTarget   float64
	hlsWindow   int
	grace       time.Duration
	ingestDir   string // base dir for per-stream push-fed ingest sockets
}

// SetIngestDir sets the directory for per-stream ingest sockets (non-proxy tee).
func (m *Manager) SetIngestDir(dir string) { m.ingestDir = dir }

// NewManager configures hub join size, the client-prebuffer ceiling (ms of live
// TS history retained per stream), HLS segment target/window, and the idle-stop
// grace period for control-managed streams.
func NewManager(maxGOP int, maxPrebufMS int64, hlsTarget float64, hlsWindow int, grace time.Duration) *Manager {
	return &Manager{
		streams:     make(map[string]*Stream),
		maxGOP:      maxGOP,
		maxPrebufMS: maxPrebufMS,
		hlsTarget:   hlsTarget,
		hlsWindow:   hlsWindow,
		grace:       grace,
	}
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
				for _, st := range streams {
					st.mu.Lock()
					st.idleStopLocked(now)
					st.mu.Unlock()
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
			Hub:   hub.New(m.maxGOP, m.maxPrebufMS),
			Seg:   hlsseg.New(m.hlsTarget, m.hlsWindow),
			chunk: defaultChunk,
			grace: m.grace,
		}
		m.streams[id] = st
	}
	return st
}

// Register sets a stream's pull config (control API).
func (m *Manager) Register(id string, src puller.Source, chunk int) {
	m.GetOrCreate(id).setConfig(src, chunk)
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
		return "", err
	}
	return sock, nil
}

// Unregister stops and removes a control-managed stream (pull or ingest).
func (m *Manager) Unregister(id string) {
	if st := m.Get(id); st != nil {
		st.mu.Lock()
		st.cfg = nil
		st.stopLocked()
		st.stopIngestLocked()
		st.mu.Unlock()
	}
	m.mu.Lock()
	delete(m.streams, id)
	m.mu.Unlock()
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

	waitMs := 5000
	if q := r.URL.Query().Get("wait"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v >= 0 {
			waitMs = v
		}
	}
	if waitMs > 30000 {
		waitMs = 30000
	}

	st.touch() // start the puller (pull-fed) + bump lastAccess
	deadline := time.Now().Add(time.Duration(waitMs) * time.Millisecond)
	for st.lastData.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st.status())
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

	// client_prebuffer (seconds, from live.php's X-Accel URL) → a keyframe-aligned
	// history burst on join, clamped to the daemon's retention ceiling. Absent/0
	// keeps the minimal current-GOP clean join.
	var prebufMS int64
	if q := r.URL.Query().Get("prebuffer"); q != "" {
		if sec, err := strconv.Atoi(q); err == nil && sec > 0 {
			prebufMS = int64(sec) * 1000
			if prebufMS > m.maxPrebufMS {
				prebufMS = m.maxPrebufMS
			}
		}
	}

	sub, snap := st.Hub.Subscribe(prebufMS)
	defer st.Hub.Unsubscribe(sub)
	st.attach()
	defer st.detach()

	// Track this viewer by its connection uuid (from live.php's X-Accel URL) so
	// fanout_sync can detect its disconnect and close the lines_live row.
	if uuid := r.URL.Query().Get("c"); uuid != "" {
		st.addConn(uuid)
		defer st.removeConn(uuid)
	}

	w.Header().Set("Content-Type", "video/mp2t")
	w.Header().Set("Cache-Control", "no-store")
	flusher, _ := w.(http.Flusher)

	if len(snap) > 0 {
		if _, err := w.Write(snap); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	for {
		select {
		case b := <-sub.C():
			if _, err := w.Write(b); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		case <-sub.Done():
			return
		case <-r.Context().Done():
			return
		}
	}
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
		pl := st.Seg.Playlist()
		if pl == "" {
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
		data := st.Seg.Segment(seq)
		if data == nil {
			http.NotFound(w, r)
			return
		}
		data = st.encryptSegment(data) // encrypted HLS when a key is set; else plain
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(data)

	default:
		http.NotFound(w, r)
	}
}
