// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestAdoptHTTPBoundsTheClassifyingRead pins a hang that took a channel off the
// air with nothing to show for it. The daemon's probe client carries no
// Client.Timeout and ResponseHeaderTimeout covers only the headers, so an
// upstream that answers 200 with a generic content-type, flushes, and then goes
// silent — an overloaded origin, or udpxy pointed at a dead multicast group —
// left AdoptHTTP blocked in the sniff forever. The stall wrapper was applied by
// the caller only AFTER AdoptHTTP returned, and ctx is the stream's lifetime, so
// nothing ever fired: the stream showed as running with a live puller, delivered
// no bytes, and never rotated to its backup URLs.
func TestAdoptHTTPBoundsTheClassifyingRead(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the source stall bound")
	}
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-release // headers are out, the body never follows
	}))
	defer srv.Close()
	defer close(release)

	resp, err := http.Get(srv.URL + "/live")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		rc, e := AdoptHTTP(context.Background(), resp, Options{})
		if rc != nil {
			_ = rc.Close()
		}
		done <- e
	}()

	select {
	case e := <-done:
		if e == nil {
			t.Fatal("AdoptHTTP accepted a body that never sent a byte")
		}
	case <-time.After(25 * time.Second):
		t.Fatal("AdoptHTTP never returned on a silent body: the puller has no reconnect " +
			"and no failover, so the stream is dead while reporting healthy")
	}
}

// TestAdoptHTTPBoundsAPlaylistTrickle: the stall bound alone cannot catch a slow
// loris, because a byte a second keeps resetting it. A playlist is a BOUNDED
// object, so reading one gets an absolute deadline as well — otherwise an
// upstream trickling towards the 4 MiB cap wedges the same classify step for as
// long as it likes.
func TestAdoptHTTPBoundsAPlaylistTrickle(t *testing.T) {
	old := playlistReadDeadline
	playlistReadDeadline = 500 * time.Millisecond
	defer func() { playlistReadDeadline = old }()

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n")
		w.(http.Flusher).Flush()
		for {
			select {
			case <-release:
				return
			case <-time.After(50 * time.Millisecond):
				_, _ = io.WriteString(w, "#") // never a whole entry, never idle
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer srv.Close()
	defer close(release)

	resp, err := http.Get(srv.URL + "/index.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		rc, e := AdoptHTTP(context.Background(), resp, Options{})
		if rc != nil {
			_ = rc.Close()
		}
		done <- e
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("a trickling playlist was read without an absolute deadline: the classify " +
			"step never finished, so the source never failed and never failed over")
	}
}
