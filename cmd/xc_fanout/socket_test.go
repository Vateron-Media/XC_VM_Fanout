// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// sockGet does one GET over a unix socket and returns the body, or an error if
// nothing is listening there. It is how nginx reaches the daemon.
func sockGet(path string) (string, error) {
	c := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "unix", path)
			},
		},
		Timeout: 3 * time.Second,
	}
	resp, err := c.Get("http://unix/who")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

// nameHandler answers every request with a fixed name, so a test can tell which
// daemon is actually behind a socket path.
func nameHandler(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, name)
	})
}

// TestListenUnixRefusesALiveDaemonsSocket: a second daemon aimed at a socket a
// RUNNING one already serves must refuse, not unlink it and bind its own.
//
// Binding a unix socket means unlinking whatever is at the path first, which is
// how the daemon recovers from its own unclean exit. Done blind, it took the
// live socket of the daemon nginx was talking to: from then on every /live and
// /hls request reached an empty registry, and when the impostor stopped it took
// the path with it. A node-wide outage started by a second `systemctl start`.
func TestListenUnixRefusesALiveDaemonsSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "http.sock")

	srvA, cleanupA, err := listenUnix(path, nameHandler("A"))
	if err != nil {
		t.Fatalf("daemon A could not bind %s: %v", path, err)
	}
	t.Cleanup(func() { _ = srvA.Close(); cleanupA() })
	if got, err := sockGet(path); err != nil || got != "A" {
		t.Fatalf("daemon A not reachable on its own socket: body=%q err=%v", got, err)
	}

	srvB, cleanupB, err := listenUnix(path, nameHandler("B"))
	if err == nil {
		t.Error("second daemon bound a socket a live daemon is already serving; it should have refused")
		_ = srvB.Close()
		cleanupB()
	} else if !strings.Contains(err.Error(), path) {
		t.Errorf("refusal does not name the socket: %v", err)
	}

	// Whatever happened above, nginx must still reach daemon A.
	if got, err := sockGet(path); err != nil || got != "A" {
		t.Fatalf("daemon A lost its socket to the second launch: body=%q err=%v", got, err)
	}
}

// TestListenUnixReplacesAStaleSocket: the self-heal must survive the guard. A
// socket file left behind by a SIGKILLed daemon has nobody listening on it, and
// the next launch has to unlink it and bind — otherwise one unclean exit wedges
// the node until an operator deletes the file by hand.
func TestListenUnixReplacesAStaleSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "http.sock")

	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false) // as a SIGKILL leaves it
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stale socket file not left behind: %v", err)
	}

	srv, cleanup, err := listenUnix(path, nameHandler("A"))
	if err != nil {
		t.Fatalf("a stale socket file wedged the daemon: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close(); cleanup() })
	if got, err := sockGet(path); err != nil || got != "A" {
		t.Fatalf("daemon not reachable after replacing a stale socket: body=%q err=%v", got, err)
	}
}

// TestCleanupLeavesAReplacementsSocketAlone: the cleanup an exiting daemon runs
// must only remove the socket IT created. Shutdown is given two seconds; a
// daemon still inside it when its replacement binds the same path used to
// unlink the replacement's socket on its way out — a restart that ends with
// nginx talking to nothing even though a healthy daemon is running.
func TestCleanupLeavesAReplacementsSocketAlone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "http.sock")

	srvOld, cleanupOld, err := listenUnix(path, nameHandler("old"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := sockGet(path); err != nil || got != "old" { // it is serving, as on a live node
		t.Fatalf("daemon not reachable on its own socket: body=%q err=%v", got, err)
	}
	// The old daemon's exit, in main's order: Shutdown (which closes the listener,
	// and the listener unlinks the socket it created) …
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srvOld.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("Shutdown did not unlink the socket it bound: %v", err)
	}

	srvNew, cleanupNew, err := listenUnix(path, nameHandler("new"))
	if err != nil {
		t.Fatalf("replacement could not bind: %v", err)
	}
	t.Cleanup(func() { _ = srvNew.Close(); cleanupNew() })

	cleanupOld() // … and then the cleanup, arriving after the replacement bound

	if got, err := sockGet(path); err != nil || got != "new" {
		t.Fatalf("the exiting daemon deleted its replacement's socket: body=%q err=%v", got, err)
	}
}

// TestStrayPositionalArgumentStartsNoDaemon: `xc_fanout version` — or the panel's
// `xc_fanout remux …` line handed to a binary from before the native remuxer —
// must not start a daemon.
//
// flag.Parse stops at the first positional word and nothing looked at what was
// left, so such a launch became a FULL daemon on the default -sock path, which
// then took the running daemon's socket. The panel composes `remux` lines only
// after checking /monitor's feature list, but an operator typing a subcommand
// that does not exist gets the same outage.
func TestStrayPositionalArgumentStartsNoDaemon(t *testing.T) {
	bin := buildDaemon(t)
	dir := t.TempDir()
	sock := filepath.Join(dir, "http.sock")

	// The daemon nginx is talking to.
	stopA := startDaemon(t, bin, sock)
	defer stopA()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-sock", sock, "-config", "", "version")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("`xc_fanout version` never exited — it started a daemon; output:\n%s", out)
	}
	if err == nil {
		t.Errorf("`xc_fanout version` exited 0; want a usage refusal. output:\n%s", out)
	} else if code := cmd.ProcessState.ExitCode(); code != 2 {
		t.Errorf("`xc_fanout version` exit status = %d, want 2 (bad usage). output:\n%s", code, out)
	}
	if strings.Contains(string(out), "listening on unix:") {
		t.Errorf("`xc_fanout version` started a daemon and bound a socket:\n%s", out)
	}

	if got, err := sockGet(sock); err != nil {
		t.Fatalf("the running daemon lost its client socket to `xc_fanout version`: body=%q err=%v", got, err)
	}
}

// buildDaemon builds the xc_fanout binary under test.
func buildDaemon(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go toolchain to build the daemon with")
	}
	bin := filepath.Join(t.TempDir(), "xc_fanout")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// startDaemon launches a real daemon on sock and waits until it answers there.
func startDaemon(t *testing.T, bin, sock string) func() {
	t.Helper()
	cmd := exec.Command(bin, "-sock", sock, "-config", "")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stop := func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := sockGet(sock); err == nil {
			return stop
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	t.Fatalf("daemon never answered on %s", sock)
	return stop
}
