// Package netguard keeps source HTTP fetches on public HTTP(S) destinations.
package netguard

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"
)

const MaxRedirects = 3

type Transport struct {
	Base         http.RoundTripper
	BlockPrivate bool

	mu              sync.Mutex
	proxyTransports map[string]*http.Transport
	lookupIP        func(context.Context, string) ([]net.IP, error)
}

func (t *Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if err := validateURL(r.URL); err != nil {
		return nil, err
	}
	if t.BlockPrivate {
		if proxyBase, ok := base.(*http.Transport); ok && proxyBase.Proxy != nil {
			proxyURL, err := proxyBase.Proxy(r)
			if err != nil {
				return nil, err
			}
			if proxyURL != nil {
				ips, err := t.resolvePublic(r.Context(), r.URL.Hostname())
				if err != nil {
					return nil, err
				}
				var lastErr error
				for i, ip := range ips {
					var response *http.Response
					switch {
					case r.URL.Scheme == "http" && (proxyURL.Scheme == "http" || proxyURL.Scheme == "https"):
						request, _ := pinProxyRequest(r, ip)
						path := request.URL.EscapedPath()
						if path == "" {
							path = "/"
						}
						request.URL.Opaque = "//" + request.URL.Host + path
						response, err = t.forwardProxyTransport(proxyBase, proxyURL).RoundTrip(request)
					case proxyURL.Scheme == "socks5" || proxyURL.Scheme == "socks5h":
						request, key := pinProxyRequest(r, ip)
						response, err = t.socksTransport(proxyBase, proxyURL.String()+"\x00"+key, r.URL.Hostname()).RoundTrip(request)
					default:
						target := pinnedAddress(r.URL, ip)
						key := proxyURL.String() + "\x00" + target
						var transport *http.Transport
						transport, err = t.proxyTransport(proxyBase, key, proxyURL, target)
						if err == nil {
							response, err = transport.RoundTrip(r)
						}
					}
					if err == nil {
						response.Request = r
						if i+1 < len(ips) && (response.StatusCode == http.StatusBadGateway || response.StatusCode == http.StatusGatewayTimeout) {
							response.Body.Close()
							continue
						}
						return response, nil
					}
					lastErr = err
					if r.Body != nil || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
						break
					}
				}
				return nil, lastErr
			}
		}
		if err := CheckURL(r.Context(), r.URL, true); err != nil {
			return nil, err
		}
	}
	return base.RoundTrip(r)
}

func (t *Transport) resolvePublic(ctx context.Context, host string) ([]net.IP, error) {
	if t.lookupIP != nil {
		ips, err := t.lookupIP(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			if !public(ip) {
				return nil, fmt.Errorf("source host resolves to a non-public address: %s", ip)
			}
		}
		if len(ips) == 0 {
			return nil, errors.New("source host has no address")
		}
		return ips, nil
	}
	return publicIPs(ctx, host)
}

func pinProxyRequest(r *http.Request, ip net.IP) (*http.Request, string) {
	host := ip.String()
	if ip.To4() == nil {
		host = "[" + host + "]"
	}
	if port := r.URL.Port(); port != "" {
		host = net.JoinHostPort(ip.String(), port)
	}
	u := *r.URL
	u.Host = host
	request := r.Clone(r.Context())
	request.URL = &u
	if request.Host == "" {
		request.Host = r.URL.Host
	}
	return request, r.URL.Scheme + "\x00" + host + "\x00" + r.URL.Hostname()
}

func (t *Transport) socksTransport(base *http.Transport, key, serverName string) *http.Transport {
	t.mu.Lock()
	defer t.mu.Unlock()
	if transport := t.proxyTransports[key]; transport != nil {
		return transport
	}
	transport := base.Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
	}
	transport.TLSClientConfig.ServerName = serverName
	if t.proxyTransports == nil {
		t.proxyTransports = make(map[string]*http.Transport)
	}
	t.proxyTransports[key] = transport
	return transport
}

func (t *Transport) forwardProxyTransport(base *http.Transport, proxyURL *url.URL) *http.Transport {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := "forward\x00" + proxyURL.String()
	if transport := t.proxyTransports[key]; transport != nil {
		return transport
	}
	transport := base.Clone()
	transport.Proxy = http.ProxyURL(proxyURL)
	if t.proxyTransports == nil {
		t.proxyTransports = make(map[string]*http.Transport)
	}
	t.proxyTransports[key] = transport
	return transport
}

func pinnedAddress(u *url.URL, ip net.IP) string {
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(ip.String(), port)
}

func (t *Transport) proxyTransport(base *http.Transport, key string, proxyURL *url.URL, target string) (*http.Transport, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if transport := t.proxyTransports[key]; transport != nil {
		return transport, nil
	}
	if proxyURL.Scheme != "http" && proxyURL.Scheme != "https" {
		return nil, fmt.Errorf("source guard does not support %s proxies", proxyURL.Scheme)
	}
	transport := base.Clone()
	transport.Proxy = nil
	transport.DialContext = proxyTunnelDial(base, proxyURL, target)
	if t.proxyTransports == nil {
		t.proxyTransports = make(map[string]*http.Transport)
	}
	t.proxyTransports[key] = transport
	return transport, nil
}

func proxyTunnelDial(base *http.Transport, proxyURL *url.URL, target string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		port := proxyURL.Port()
		if port == "" {
			if proxyURL.Scheme == "https" {
				port = "443"
			} else {
				port = "80"
			}
		}
		dial := base.DialContext
		if dial == nil {
			dial = (&net.Dialer{}).DialContext
		}
		conn, err := dial(ctx, network, net.JoinHostPort(proxyURL.Hostname(), port))
		if err != nil {
			return nil, err
		}
		if proxyURL.Scheme == "https" {
			tlsConn := tls.Client(conn, &tls.Config{ServerName: proxyURL.Hostname()})
			if base.TLSHandshakeTimeout > 0 {
				_ = conn.SetDeadline(time.Now().Add(base.TLSHandshakeTimeout))
			}
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			conn = tlsConn
			_ = conn.SetDeadline(time.Time{})
		}
		if base.ResponseHeaderTimeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(base.ResponseHeaderTimeout))
		}
		req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Opaque: target}, Host: target, Header: make(http.Header)}
		if proxyURL.User != nil {
			password, _ := proxyURL.User.Password()
			token := base64.StdEncoding.EncodeToString([]byte(proxyURL.User.Username() + ":" + password))
			req.Header.Set("Proxy-Authorization", "Basic "+token)
		}
		if err := req.Write(conn); err != nil {
			conn.Close()
			return nil, err
		}
		reader := bufio.NewReader(conn)
		resp, err := http.ReadResponse(reader, req)
		if err != nil {
			conn.Close()
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			conn.Close()
			return nil, fmt.Errorf("proxy CONNECT: %s", resp.Status)
		}
		_ = conn.SetDeadline(time.Time{})
		return &bufferedConn{Conn: conn, reader: reader}, nil
	}
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func (t *Transport) CloseIdleConnections() {
	if closer, ok := t.Base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, transport := range t.proxyTransports {
		transport.CloseIdleConnections()
	}
}

// PublicDialContext makes the DNS answer checked here the address that is
// actually dialled, closing the check-then-resolve DNS rebinding gap.
func PublicDialContext(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := publicIPs(ctx, host)
		if err != nil {
			return nil, err
		}
		var last error
		for _, ip := range ips {
			conn, err := dial(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
		}
		return nil, last
	}
}

func CheckURL(ctx context.Context, u *url.URL, blockPrivate bool) error {
	if err := validateURL(u); err != nil {
		return err
	}
	if !blockPrivate {
		return nil
	}
	_, err := publicIPs(ctx, u.Hostname())
	return err
}

func validateURL(u *url.URL) error {
	if u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return errors.New("source URL must use http or https")
	}
	return nil
}

func publicIPs(ctx context.Context, host string) ([]net.IP, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve source host: %w", err)
	}
	if len(addrs) == 0 {
		return nil, errors.New("source host has no address")
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		if !public(a.IP) {
			return nil, fmt.Errorf("source host resolves to a non-public address: %s", a.IP)
		}
		ips = append(ips, a.IP)
	}
	return ips, nil
}

func public(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	for _, prefix := range specialUsePrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsUnspecified()
}

var specialUsePrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"),
	netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.175.48.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2002::/16"),
}

func CheckRedirect(blockPrivate bool) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) > MaxRedirects {
			return errors.New("too many source redirects")
		}
		if len(via) > 0 {
			prev := via[len(via)-1]
			if !strings.EqualFold(prev.URL.Host, req.URL.Host) {
				userAgent := req.Header.Get("User-Agent")
				byteRange := req.Header.Get("Range")
				req.Header = make(http.Header)
				if userAgent != "" {
					req.Header.Set("User-Agent", userAgent)
				}
				if byteRange != "" {
					req.Header.Set("Range", byteRange)
				}
				req.Host = ""
			}
			if prev.URL.Scheme == "https" && req.URL.Scheme != "https" {
				return errors.New("source redirect downgrades TLS")
			}
		}
		return CheckURL(req.Context(), req.URL, blockPrivate)
	}
}
