// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// publishSecond publishes one keyframe-opened block carrying the ring clock at
// sec seconds, padded to pad+1 packets so the ring holds bytes a viewer takes
// time to drain (a ring of bare keyframes is a few hundred bytes and proves
// nothing about a reader following it).
func publishSecond(st *Stream, sec int64, pad int) {
	buf := tsfixture.KeyframePCR(0x101, sec*90000, sec*90000)
	for i := 0; i < pad; i++ {
		buf = append(buf, tsfixture.Fill(0x101)...)
	}
	st.Publish(buf)
}

// joinResult is what one zapping viewer got: the bytes it received before the
// daemon closed the response, and any client-side error.
type joinResult struct {
	got int64
	err error
}

// TestServeLiveJoinSurvivesIdleSweep: a reaper sweep landing between a viewer's
// join and its attach must not pull the ring out from under it.
//
// serveLive used to place the cursor (Hub.Join) BEFORE taking a ref, so between
// the two the stream still looked unwatched: refs==0 with a stale lastAccess is
// exactly what the reaper acts on. A sweep in that window gates the ring down to
// the idle floor (or idle-stops and flushes it), and the block the cursor points
// at is pruned — so the viewer's very first Follow reports behind and a fresh
// zap on a fast link is dropped as "fell behind the ring". The ref must be taken
// first: refs>0 blocks both the gate and the idle-stop for as long as the viewer
// is there.
func TestServeLiveJoinSurvivesIdleSweep(t *testing.T) {
	const (
		ringSec  = 40       // seconds of history the ring holds
		padPkts  = 100      // packets of filler per second, so a block is ~19 KB
		wantRead = 64 << 10 // bytes a viewer must receive before we call it served
		rounds   = 20
		joiners  = 12
	)

	mgr := NewManager(1<<20, ringSec*1000, 6, 6, time.Hour)
	mgr.viewerIdleNS.Store(0)      // no idle drop: a short session here means "behind"
	mgr.idleBufferGraceNS.Store(1) // the gate is eligible the moment refs hit 0
	st := mgr.GetOrCreate("5")
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))

	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()

	// The reaper, sweeping as fast as it can: the real gate on the real lock, so
	// the window between join and attach is hit rather than simulated.
	stop := make(chan struct{})
	var sweeper sync.WaitGroup
	sweeper.Add(1)
	go func() {
		defer sweeper.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			st.mu.Lock()
			st.gateIdleBufferLocked(time.Now())
			st.mu.Unlock()
			runtime.Gosched()
		}
	}()
	defer func() { close(stop); sweeper.Wait() }()

	sec := int64(0)
	for round := 0; round < rounds; round++ {
		// Hold st.mu so the sweep cannot run: restore the ring to full depth, refill
		// it, and let a burst of viewers queue on the very lock the sweep wants. When
		// it is released the sweep takes its turn among them — which is exactly the
		// interleaving an unlucky zap meets on a channel whose last viewer just left.
		res := make([]joinResult, joiners)
		var wg sync.WaitGroup
		st.mu.Lock()
		st.ensureBufferedLocked()
		for i := 0; i < ringSec; i++ {
			sec++
			publishSecond(st, sec, padPkts)
		}
		for v := 0; v < joiners; v++ {
			wg.Add(1)
			go func(v int) {
				defer wg.Done()
				resp, err := http.Get(ts.URL + "/live/5?prebuffer=35&c=zap")
				if err != nil {
					res[v].err = err
					return
				}
				defer resp.Body.Close()
				buf := make([]byte, 32<<10)
				for res[v].got < wantRead {
					n, err := resp.Body.Read(buf)
					res[v].got += int64(n)
					if err != nil {
						res[v].err = err
						break
					}
				}
			}(v)
		}
		time.Sleep(20 * time.Millisecond) // let the viewers reach the lock
		st.mu.Unlock()
		wg.Wait()

		for v := range res {
			if res[v].got < wantRead {
				t.Fatalf("round %d viewer %d got %d bytes (the join burst is ~%d KB), err=%v: "+
					"a reaper sweep between Hub.Join and attach collapsed the ring under a joining viewer",
					round, v, res[v].got, ringSec*(padPkts+1)*188/1024, res[v].err)
			}
		}
	}
}
