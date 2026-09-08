package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// TestServeLiveDropsIdleViewer covers the other half of the ghost problem from
// TestServeLiveDropsStalledClient. The per-write deadline only fires while there
// are bytes to write; it says nothing about a stream that has gone off-air. Then
// nothing is ever written, so nothing ever times out — and a client whose socket
// is half-open (a dropped mobile link that sent no FIN nor RST) is invisible to
// us and to nginx alike. That viewer sat in the serve loop forever, holding its
// uuid in /connections as a ghost the reconciler could never clear.
func TestServeLiveDropsIdleViewer(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	mgr.viewerIdleNS.Store(int64(400 * time.Millisecond))
	st := mgr.GetOrCreate("5")
	feedStream(st) // a clean join snapshot exists, then the source goes silent

	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.Get(ts.URL + "/live/5?c=idle-ghost&prebuffer=0")
		if err != nil {
			return
		}
		buf := make([]byte, 4096)
		for {
			if _, err := resp.Body.Read(buf); err != nil {
				break
			}
		}
		resp.Body.Close()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(st.connUUIDs()) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if len(st.connUUIDs()) == 0 {
		t.Fatal("viewer never registered")
	}

	// Nothing is published from here on: the stream is off-air.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("viewer receiving no data was never dropped — ghost connection on an off-air stream")
	}
	if n := len(st.connUUIDs()); n != 0 {
		t.Errorf("/connections still reports %v after the idle drop", st.connUUIDs())
	}
}

// TestServeLiveKeepsFedViewer: the idle drop must only fire on a genuinely
// silent stream. A viewer that keeps receiving data stays connected however long
// it watches.
func TestServeLiveKeepsFedViewer(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	mgr.viewerIdleNS.Store(int64(300 * time.Millisecond))
	st := mgr.GetOrCreate("5")
	feedStream(st)

	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()

	dropped := make(chan struct{})
	go func() {
		defer close(dropped)
		resp, err := http.Get(ts.URL + "/live/5?c=healthy&prebuffer=0")
		if err != nil {
			return
		}
		buf := make([]byte, 64*1024)
		for {
			if _, err := resp.Body.Read(buf); err != nil {
				break
			}
		}
		resp.Body.Close()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(st.connUUIDs()) == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	// Feed for well over three idle windows.
	stop := time.After(1200 * time.Millisecond)
	for {
		select {
		case <-dropped:
			t.Fatal("a viewer that is still receiving data was dropped as idle")
		case <-stop:
			if len(st.connUUIDs()) != 1 {
				t.Fatalf("healthy viewer lost: /connections = %v", st.connUUIDs())
			}
			return
		default:
			st.Publish(tsfixture.Fill(0x101))
			time.Sleep(20 * time.Millisecond)
		}
	}
}
