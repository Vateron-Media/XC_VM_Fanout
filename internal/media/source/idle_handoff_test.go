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

// hasIdleWatcher reports whether rc (or anything it wraps) still carries a stall
// watcher of its own.
func hasIdleWatcher(rc any) bool {
	switch v := rc.(type) {
	case *idleTimeoutReader:
		return true
	case *prefixedReadCloser:
		return hasIdleWatcher(v.c)
	case *clientBoundReadCloser:
		return hasIdleWatcher(v.ReadCloser)
	}
	return false
}

// TestAdoptHTTPHandsTheStallBoundToTheCaller: classification is bounded, the
// live body is the caller's to bound. Every caller already wraps what it gets
// (remux with -idle_timeout, the puller with IdleBound), so a second watcher
// left inside is not a safety net — it is a second bound that always fires
// first, and a second goroutine and ticker per source.
func TestAdoptHTTPHandsTheStallBoundToTheCaller(t *testing.T) {
	body := tsSegment(0)
	for _, ct := range []string{"video/mp2t", "application/octet-stream"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", ct)
			_, _ = w.Write(body)
		}))
		rc, err := Open(context.Background(), srv.URL+"/live", Options{})
		if err != nil {
			srv.Close()
			t.Fatalf("content-type %q: Open: %v", ct, err)
		}
		if hasIdleWatcher(rc) {
			t.Errorf("content-type %q: the adopted body still carries its own stall watcher, "+
				"so the caller's bound can only ever shorten it", ct)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		srv.Close()
		if len(got) != len(body) {
			t.Errorf("content-type %q: read %d bytes, want %d", ct, len(got), len(body))
		}
	}
}

// TestALongerCallerBoundSurvivesAdoption pins the operator-visible half.
//
// `xc_fanout remux -idle_timeout 30` documents 30s with no source bytes before
// the source counts as gone. AdoptHTTP wrapped every TS body in its own 8s
// bound first, so a bursty upstream — a provider that flushes a GOP at a time,
// or one whose origin hiccups — was cut at eight seconds no matter what the
// operator configured, and the supervisor restart-looped a channel that was
// about to deliver.
func TestALongerCallerBoundSurvivesAdoption(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the 8s bound AdoptHTTP used to impose")
	}
	const pause = 11500 * time.Millisecond // past the 8s bound and its tick
	body := tsSegment(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(body)
		w.(http.Flusher).Flush()
		time.Sleep(pause) // a bursty source: silent, but not gone
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/live.ts", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Exactly what remux.Run does with -idle_timeout 30.
	rc = WrapIdleTimeout(rc, IdleBound(rc, 30*time.Second))
	defer rc.Close()

	start := time.Now()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("a source that paused %s under a 30s bound failed after %s: %v",
			pause, time.Since(start).Round(100*time.Millisecond), err)
	}
	if len(got) != 2*len(body) {
		t.Fatalf("read %d bytes, want both bursts (%d)", len(got), 2*len(body))
	}
}
