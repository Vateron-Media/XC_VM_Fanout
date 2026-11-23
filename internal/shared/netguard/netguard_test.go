package netguard

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPublicDialRefusesPrivateAddressBeforeDial(t *testing.T) {
	called := false
	dial := PublicDialContext(func(context.Context, string, string) (net.Conn, error) {
		called = true
		return nil, errors.New("unexpected dial")
	})
	if _, err := dial(context.Background(), "tcp", "127.0.0.1:80"); err == nil {
		t.Fatal("private address accepted")
	}
	if called {
		t.Fatal("base dialer was called for a private address")
	}
}

func TestPrivateAndUnsafeDestinationsAreRefused(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1/x", "http://169.254.169.254/x", "http://100.64.0.1/x", "file:///etc/passwd"} {
		u, _ := url.Parse(raw)
		if err := CheckURL(context.Background(), u, true); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

func TestRedirectPolicyRejectsDowngradeAndStripsCredentials(t *testing.T) {
	prev, _ := http.NewRequest(http.MethodGet, "https://origin.example/x", nil)
	next, _ := http.NewRequest(http.MethodGet, "http://other.example/x", nil)
	next.Header.Set("Authorization", "secret")
	next.Header.Set("Cookie", "secret")
	next.Header.Set("X-Provider-Token", "secret")
	next.Header.Set("User-Agent", "fanout-test")
	next.Header.Set("Range", "bytes=67108864-")
	if err := CheckRedirect(false)(next, []*http.Request{prev}); err == nil {
		t.Fatal("accepted an HTTPS to HTTP redirect")
	}
	if next.Header.Get("Authorization") != "" || next.Header.Get("Cookie") != "" || next.Header.Get("X-Provider-Token") != "" {
		t.Fatal("cross-host redirect kept credentials")
	}
	if next.Header.Get("User-Agent") != "fanout-test" {
		t.Fatal("cross-host redirect removed User-Agent")
	}
	if next.Header.Get("Range") != "bytes=67108864-" {
		t.Fatal("cross-host redirect removed byte range")
	}
}

func TestProxyReceivesPinnedIPWithOriginalHostAndSNI(t *testing.T) {
	var connectTargets []string
	var requestHost, serverName string
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestHost = r.Host
		w.WriteHeader(http.StatusNoContent)
	}))
	origin.StartTLS()
	defer origin.Close()
	origin.TLS.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		serverName = hello.ServerName
		return nil, nil
	}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectTargets = append(connectTargets, r.Host)
		if r.Host == "8.8.8.8:443" {
			http.Error(w, "try next", http.StatusBadGateway)
			return
		}
		upstream, err := net.Dial("tcp", origin.Listener.Addr().String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		client, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			upstream.Close()
			return
		}
		_, _ = client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		go func() {
			_, _ = io.Copy(upstream, client)
			upstream.Close()
		}()
		_, _ = io.Copy(client, upstream)
		client.Close()
	}))
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)
	guard := &Transport{
		Base: &http.Transport{
			Proxy:           http.ProxyURL(proxyURL),
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		BlockPrivate: true,
		lookupIP: func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("8.8.8.8"), net.ParseIP("1.1.1.1")}, nil
		},
	}
	client := &http.Client{Transport: guard}
	resp, err := client.Get("https://provider.example/live.ts")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	client.CloseIdleConnections()
	if len(connectTargets) != 2 || connectTargets[0] != "8.8.8.8:443" || connectTargets[1] != "1.1.1.1:443" ||
		requestHost != "provider.example" || serverName != "provider.example" {
		t.Fatalf("proxy targets %v, Host %q, SNI %q", connectTargets, requestHost, serverName)
	}
}

func TestHTTPForwardProxyGetsPinnedAuthorityAndPreservesOriginIdentity(t *testing.T) {
	var proxyMethod, proxyAuthority, upstreamHost string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHost = r.Host
		w.WriteHeader(http.StatusNoContent)
	}))
	defer origin.Close()
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	proxyDone := make(chan error, 1)
	go func() {
		conn, err := proxy.Accept()
		if err != nil {
			proxyDone <- err
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		line, err := reader.ReadString('\n')
		if err != nil {
			proxyDone <- err
			return
		}
		parts := strings.SplitN(strings.TrimSpace(line), " ", 3)
		if len(parts) != 3 {
			proxyDone <- fmt.Errorf("invalid proxy request line: %q", line)
			return
		}
		target, err := url.Parse(parts[1])
		if err != nil {
			proxyDone <- err
			return
		}
		proxyMethod, proxyAuthority = parts[0], target.Host
		var host string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				proxyDone <- err
				return
			}
			if line == "\r\n" {
				break
			}
			if strings.HasPrefix(strings.ToLower(line), "host:") {
				host = strings.TrimSpace(line[5:])
			}
		}
		upstream, err := net.Dial("tcp", origin.Listener.Addr().String())
		if err == nil {
			_, err = fmt.Fprintf(upstream, "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", parts[0], target.RequestURI(), host)
		}
		if err == nil {
			_, err = io.Copy(conn, upstream)
		}
		if upstream != nil {
			upstream.Close()
		}
		proxyDone <- err
	}()
	proxyURL, _ := url.Parse("http://" + proxy.Addr().String())
	var selectedHosts []string
	dialer := &net.Dialer{}
	guard := &Transport{
		Base: &http.Transport{
			Proxy: func(r *http.Request) (*url.URL, error) {
				selectedHosts = append(selectedHosts, r.URL.Host)
				if r.URL.Hostname() == "provider.example" {
					return proxyURL, nil
				}
				return nil, nil
			},
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				if address != proxy.Addr().String() {
					return nil, fmt.Errorf("unexpected direct dial to %s", address)
				}
				return dialer.DialContext(ctx, network, address)
			},
		},
		BlockPrivate: true,
		lookupIP: func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		},
	}
	resp, err := (&http.Client{Transport: guard}).Get("http://provider.example/live.ts")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if err := <-proxyDone; err != nil {
		t.Fatal(err)
	}
	if len(selectedHosts) != 1 || selectedHosts[0] != "provider.example" {
		t.Fatalf("proxy selector hosts = %v", selectedHosts)
	}
	if proxyMethod != http.MethodGet || proxyAuthority != "8.8.8.8" {
		t.Fatalf("proxy received method %q and authority %q", proxyMethod, proxyAuthority)
	}
	if upstreamHost != "provider.example" {
		t.Fatalf("upstream Host = %q", upstreamHost)
	}
	if resp.Request.URL.Host != "provider.example" {
		t.Fatalf("response request URL host = %q", resp.Request.URL.Host)
	}
}

func TestSocksProxyPinsIPAndPreservesHostAndSNI(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://provider.example:8443/live.ts", nil)
	pinned, key := pinProxyRequest(req, net.ParseIP("8.8.8.8"))
	if pinned.URL.Host != "8.8.8.8:8443" || pinned.Host != "provider.example:8443" {
		t.Fatalf("pinned URL host %q and Host %q", pinned.URL.Host, pinned.Host)
	}
	guard := &Transport{}
	transport := guard.socksTransport(&http.Transport{}, key, req.URL.Hostname())
	if transport.TLSClientConfig.ServerName != "provider.example" {
		t.Fatalf("TLS ServerName = %q", transport.TLSClientConfig.ServerName)
	}
}

func TestSocksRoundTripRestoresOriginalRequestURL(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	target := make(chan string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			target <- "accept: " + err.Error()
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		greeting := make([]byte, 3)
		if _, err = io.ReadFull(reader, greeting); err != nil {
			target <- "greeting: " + err.Error()
			return
		}
		_, _ = conn.Write([]byte{5, 0})
		header := make([]byte, 4)
		if _, err = io.ReadFull(reader, header); err != nil {
			target <- "request: " + err.Error()
			return
		}
		address := make([]byte, 4)
		if _, err = io.ReadFull(reader, address); err != nil {
			target <- "address: " + err.Error()
			return
		}
		port := make([]byte, 2)
		_, _ = io.ReadFull(reader, port)
		target <- net.IP(address).String()
		_, _ = conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
		if _, err = http.ReadRequest(reader); err == nil {
			_, _ = conn.Write([]byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"))
		}
	}()
	proxyURL, _ := url.Parse("socks5://" + listener.Addr().String())
	guard := &Transport{
		Base:         &http.Transport{Proxy: http.ProxyURL(proxyURL)},
		BlockPrivate: true,
		lookupIP: func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		},
	}
	resp, err := (&http.Client{Transport: guard}).Get("http://provider.example/live.ts")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := <-target; got != "8.8.8.8" {
		t.Fatalf("SOCKS target = %q", got)
	}
	if resp.Request.URL.Host != "provider.example" {
		t.Fatalf("response request URL host = %q", resp.Request.URL.Host)
	}
}

func TestHTTPSProxyHandshakeIsBounded(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			time.Sleep(time.Second)
		}
	}()
	proxyURL, _ := url.Parse("https://" + listener.Addr().String())
	guard := &Transport{
		Base: &http.Transport{
			Proxy:               http.ProxyURL(proxyURL),
			TLSHandshakeTimeout: 30 * time.Millisecond,
		},
		BlockPrivate: true,
		lookupIP: func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		},
	}
	started := time.Now()
	if _, err := (&http.Client{Transport: guard}).Get("https://provider.example/live.ts"); err == nil {
		t.Fatal("stalled HTTPS proxy handshake succeeded")
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("HTTPS proxy handshake ignored its timeout")
	}
}
