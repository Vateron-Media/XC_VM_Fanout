package front

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestParseListenSetPrimaryAndAdds(t *testing.T) {
	got, err := ParseListenSet("listen 80 reuseport;\nlisten 8080 reuseport;", "listen 443 ssl reuseport;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []ListenSpec{{Port: 80, TLS: false}, {Port: 8080, TLS: false}, {Port: 443, TLS: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParseListenSetIgnoresNoise(t *testing.T) {
	httpConf := "# a comment\n\n  listen 80 reuseport;\nserver_name _;\n"
	got, err := ParseListenSet(httpConf, "   listen   443   ssl;  # tls\n")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []ListenSpec{{Port: 80, TLS: false}, {Port: 443, TLS: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParseListenSetEmptyIsError(t *testing.T) {
	if _, err := ParseListenSet("# nothing\n", ""); err == nil {
		t.Fatalf("expected an error when no listen directives are present")
	}
}

func TestWatchPortsFiresOnChange(t *testing.T) {
	dir := t.TempDir()
	httpPath := filepath.Join(dir, "http.conf")
	httpsPath := filepath.Join(dir, "https.conf")
	if err := os.WriteFile(httpPath, []byte("listen 80 reuseport;"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(httpsPath, []byte("listen 443 ssl reuseport;"), 0o644); err != nil {
		t.Fatal(err)
	}
	applied := make(chan []ListenSpec, 8)
	stop := WatchPorts(httpPath, httpsPath, 20*time.Millisecond, func(s []ListenSpec) { applied <- s })
	defer stop()

	// Initial apply carries the starting set.
	select {
	case s := <-applied:
		if len(s) != 2 {
			t.Fatalf("initial apply got %v, want 2 ports", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no initial apply")
	}

	// Add an https port; the watcher must fire with the new set.
	if err := os.WriteFile(httpsPath, []byte("listen 443 ssl reuseport;\nlisten 8443 ssl reuseport;"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case s := <-applied:
			if hasPort(s, 8443) {
				return // success
			}
		case <-deadline:
			t.Fatal("watcher never fired with the added port 8443")
		}
	}
}

func hasPort(s []ListenSpec, port int) bool {
	for _, x := range s {
		if x.Port == port {
			return true
		}
	}
	return false
}
