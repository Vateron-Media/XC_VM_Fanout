package front

import (
	"context"
	"log"
	"os"
	"time"
)

// ServeManaged binds the node's listen set — parsed from the two nginx ports
// includes (ports/http.conf, ports/https.conf) the node maintains — and serves
// the front on every port, re-binding when MAIN changes the set. The initial
// bind's error is returned, so the daemon logs it and leaves nginx serving
// (opt-in, non-fatal, like the gateway socket); a later rebind error is logged
// and the good listeners keep serving. The returned stop ends the watch and
// drains every listener.
func (s *Server) ServeManaged(certs *CertReloader, httpPortsPath, httpsPortsPath string, poll, drain time.Duration) (*Manager, func(), error) {
	mgr := NewManager(s, certs.TLSConfig(), drain)
	h, _ := os.ReadFile(httpPortsPath)
	hs, _ := os.ReadFile(httpsPortsPath)
	specs, err := ParseListenSet(string(h), string(hs))
	if err != nil {
		return nil, nil, err
	}
	// A partial bind (one port taken, the rest fine) must not orphan the started
	// listeners: proceed if any came up, matching the watcher's behaviour and the
	// spec's "one bad port is skipped, the rest keep serving". Only a total
	// failure is fatal.
	if err := mgr.Reconcile(specs); err != nil {
		if len(mgr.Ports()) == 0 {
			return nil, nil, err
		}
		log.Printf("front: some listen ports failed to bind: %v (serving the rest)", err)
	}
	stopWatch := WatchPorts(httpPortsPath, httpsPortsPath, poll, func(set []ListenSpec) {
		if err := mgr.Reconcile(set); err != nil {
			log.Printf("front: port reconcile: %v", err)
		}
	})
	stop := func() {
		stopWatch()
		ctx, cancel := context.WithTimeout(context.Background(), drain)
		defer cancel()
		_ = mgr.Shutdown(ctx)
	}
	return mgr, stop, nil
}
