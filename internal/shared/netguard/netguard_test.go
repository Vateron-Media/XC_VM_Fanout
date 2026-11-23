package netguard

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
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
	var connectTarget, requestHost, serverName string
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
		connectTarget = r.Host
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
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		},
	}
	client := &http.Client{Transport: guard}
	resp, err := client.Get("https://provider.example/live.ts")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	client.CloseIdleConnections()
	if connectTarget != "8.8.8.8:443" || requestHost != "provider.example" || serverName != "provider.example" {
		t.Fatalf("proxy target %q, Host %q, SNI %q", connectTarget, requestHost, serverName)
	}
}
