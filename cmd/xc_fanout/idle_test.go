// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package main

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// TestListenUnixClosesIdleConnections: a kept-alive connection no request
// comes on is closed after unixIdleTimeout, while a request held longer than
// that (the agent's /events long-poll, a live-TS response) is not cut.
//
// Without an idle timeout every connection a client left open and idle held
// an fd and a goroutine in the daemon for its whole life.
func TestListenUnixClosesIdleConnections(t *testing.T) {
	old := unixIdleTimeout
	unixIdleTimeout = 150 * time.Millisecond
	defer func() { unixIdleTimeout = old }()

	path := filepath.Join(t.TempDir(), "ctl.sock")
	srv, cleanup, err := listenUnix(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			time.Sleep(3 * unixIdleTimeout) // held past the idle timeout
		}
		_, _ = io.WriteString(w, "ok")
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close(); cleanup() })

	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	br := bufio.NewReader(c)
	get := func(p string) {
		t.Helper()
		if _, err := io.WriteString(c, "GET "+p+" HTTP/1.1\r\nHost: fanout\r\n\r\n"); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		res, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if string(b) != "ok" {
			t.Fatalf("%s: %q", p, b)
		}
	}
	get("/slow") // longer than the idle timeout, and answered
	get("/fast") // the same connection is still kept after it

	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("an idle connection was not closed by the daemon: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("closed after %s, want about %s", d, unixIdleTimeout)
	}
}
