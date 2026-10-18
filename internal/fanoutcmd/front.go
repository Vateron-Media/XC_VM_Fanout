package fanoutcmd

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/front"
)

const (
	frontPortPoll = 2 * time.Second  // how often the front re-reads the node's listen files
	frontDrain    = 10 * time.Second // graceful drain for a removed port and on shutdown
)

// serveFront starts the public front for the single-binary LB: it binds the
// node's full listen set — the ports/http.conf + ports/https.conf the node
// maintains — terminates TLS with certbot's certificate on the TLS ports,
// answers the serve routes from the in-process gateway, serves /images and
// ACME, and reverse-proxies the rest to nginx during the transition. It
// re-binds when MAIN changes the ports. Opt-in and non-fatal — a bad cert or an
// unparsable port set is returned as an error so the caller logs it and leaves
// nginx serving. The returned stop ends the watch and drains every listener.
func serveFront(gw http.Handler, nginxURL, webroot, certPath, keyPath, realipPaths, images, httpPortsPath, httpsPortsPath string) (func(), error) {
	certs, err := front.NewCertReloader(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("tls certificate: %w", err)
	}
	u, err := url.Parse(nginxURL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("nginx backend url %q is not usable", nginxURL)
	}
	f := front.NewServer(gw, u, webroot, front.LoadTrustedProxies(splitCSV(realipPaths)...)).WithImages(images)
	mgr, stop, err := f.ServeManaged(certs, httpPortsPath, httpsPortsPath, frontPortPoll, frontDrain)
	if err != nil {
		return nil, err
	}
	log.Printf("front: serving the node listen set %v (serve-only; proxying the rest to %s)", mgr.Ports(), nginxURL)
	return stop, nil
}
