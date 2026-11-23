package netguard

import (
	"context"
	"errors"
	"net"
	"net/http"
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
	for _, raw := range []string{"http://127.0.0.1/x", "http://169.254.169.254/x", "file:///etc/passwd"} {
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
	if err := CheckRedirect(false)(next, []*http.Request{prev}); err == nil {
		t.Fatal("accepted an HTTPS to HTTP redirect")
	}
	if next.Header.Get("Authorization") != "" || next.Header.Get("Cookie") != "" {
		t.Fatal("cross-host redirect kept credentials")
	}
}
