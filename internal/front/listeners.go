package front

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
)

// soReusePort is SO_REUSEPORT on Linux (the node's only platform); the stdlib
// syscall package does not export it. Value 15 is stable across the arches
// xc_node ships on (amd64, arm64).
const soReusePort = 0x0F

// ListenSpec is one public port the front listens on. TLS true means HTTPS,
// terminated with the reloading certbot certificate; false is plain HTTP.
type ListenSpec struct {
	Port int
	TLS  bool
}

type listenerEntry struct {
	spec ListenSpec
	srv  *http.Server
	ln   net.Listener
}

// Manager owns the set of listeners the front serves on and reconciles them to
// a desired set when MAIN changes the node's ports — starting new listeners and
// draining removed ones gracefully, the way `nginx -s reload` does. One shared
// handler and one shared TLS config back every listener.
type Manager struct {
	handler http.Handler
	tlsCfg  *tls.Config
	drain   time.Duration

	mu      sync.Mutex
	entries map[int]*listenerEntry
}

// NewManager builds a manager serving h on every listener, with tlsCfg for the
// TLS ports (shared, so a cert reload covers them all) and drain as the graceful
// shutdown deadline for a removed port.
func NewManager(h http.Handler, tlsCfg *tls.Config, drain time.Duration) *Manager {
	return &Manager{handler: h, tlsCfg: tlsCfg, drain: drain, entries: map[int]*listenerEntry{}}
}

// reusePort sets SO_REUSEPORT on the socket, matching nginx's `reuseport` and
// letting the front bind a port nginx still holds for a zero-drop handover.
func reusePort(_, _ string, c syscall.RawConn) error {
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePort, 1)
	}); err != nil {
		return err
	}
	return serr
}

// Reconcile brings the live listeners to desired: start each new port, drain
// each removed one, and skip — not abort on — a port that fails to bind (its
// error is returned, the rest keep serving). An empty desired set is refused so
// the front never ends up listening on nothing (the last-good set stays).
func (m *Manager) Reconcile(desired []ListenSpec) error {
	m.mu.Lock()

	want := make(map[int]ListenSpec, len(desired))
	for _, s := range desired {
		want[s.Port] = s // dedup by port
	}
	if len(want) == 0 {
		m.mu.Unlock()
		return errors.New("front: refusing an empty listen set (keeping last-good)")
	}

	var errs []error
	// Remove a port that is gone, or restart one whose spec changed (e.g. a TLS
	// flip): close its listener now — under the lock, so it stops accepting at
	// once and a restart never splits traffic with its replacement — and drain
	// it in the background, below.
	var removed []*listenerEntry
	for port, e := range m.entries {
		if w, ok := want[port]; !ok || w != e.spec {
			_ = e.ln.Close()
			removed = append(removed, e)
			delete(m.entries, port)
		}
	}
	for port, s := range want {
		if _, ok := m.entries[port]; ok {
			continue
		}
		if err := m.start(s); err != nil {
			errs = append(errs, fmt.Errorf("port %d: %w", port, err))
		}
	}
	m.mu.Unlock()
	// Drain removed listeners outside the lock, so a long-lived stream on a port
	// going away never holds up management ops (Ports, a concurrent Reconcile, or
	// Shutdown). Each drain is bounded by m.drain and self-terminates.
	for _, e := range removed {
		m.drainInBackground(e)
	}
	return errors.Join(errs...)
}

// start binds one spec and serves it; the caller holds m.mu.
func (m *Manager) start(s ListenSpec) error {
	lc := net.ListenConfig{Control: reusePort}
	ln, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return err
	}
	if s.TLS {
		ln = tls.NewListener(ln, m.tlsCfg)
	}
	srv := &http.Server{Handler: m.handler, ReadHeaderTimeout: defaults.HTTPReadHeaderTimeout}
	m.entries[s.Port] = &listenerEntry{spec: s, srv: srv, ln: ln}
	go srv.Serve(ln) //nolint:errcheck // Serve returns ErrServerClosed on Shutdown
	return nil
}

// drainInBackground gracefully shuts a removed listener down off the lock,
// bounded by m.drain; its listener is already closed so it accepts nothing new,
// and its in-flight responses finish (or hit the deadline).
func (m *Manager) drainInBackground(e *listenerEntry) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), m.drain)
		defer cancel()
		_ = e.srv.Shutdown(ctx)
	}()
}

// Ports returns the live listen set, ordered by port.
func (m *Manager) Ports() []ListenSpec {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ListenSpec, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, e.spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Port < out[j].Port })
	return out
}

// Shutdown drains every listener (used on daemon shutdown), concurrently and
// off the lock, bounded by ctx.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	entries := make([]*listenerEntry, 0, len(m.entries))
	for port, e := range m.entries {
		entries = append(entries, e)
		delete(m.entries, port)
	}
	m.mu.Unlock()

	errCh := make(chan error, len(entries))
	var wg sync.WaitGroup
	for _, e := range entries {
		wg.Add(1)
		go func(e *listenerEntry) {
			defer wg.Done()
			errCh <- e.srv.Shutdown(ctx)
		}(e)
	}
	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
