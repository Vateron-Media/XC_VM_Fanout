// Package netguard keeps source HTTP fetches on public HTTP(S) destinations.
package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const MaxRedirects = 3

type Transport struct {
	Base         http.RoundTripper
	BlockPrivate bool
}

func (t Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if err := CheckURL(r.Context(), r.URL, t.BlockPrivate); err != nil {
		return nil, err
	}
	return base.RoundTrip(r)
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
	if u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return errors.New("source URL must use http or https")
	}
	if !blockPrivate {
		return nil
	}
	_, err := publicIPs(ctx, u.Hostname())
	return err
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
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsUnspecified()
}

func CheckRedirect(blockPrivate bool) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) > MaxRedirects {
			return errors.New("too many source redirects")
		}
		if len(via) > 0 {
			prev := via[len(via)-1]
			if !strings.EqualFold(prev.URL.Host, req.URL.Host) {
				req.Header.Del("Authorization")
				req.Header.Del("Cookie")
				req.Header.Del("Proxy-Authorization")
			}
			if prev.URL.Scheme == "https" && req.URL.Scheme != "https" {
				return errors.New("source redirect downgrades TLS")
			}
		}
		return CheckURL(req.Context(), req.URL, blockPrivate)
	}
}
