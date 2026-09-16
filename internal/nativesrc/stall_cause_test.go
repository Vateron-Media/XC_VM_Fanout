// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// TestStallErrorNamesTheWatchdog: when the bound fires the wrapper closes the
// source, and the read error the consumer then gets is whatever the close
// happened to produce — "io: read/write on closed pipe" for an HLS pull, "use of
// closed network connection" for an http TS body. puller.Run logs that verbatim
// ("puller: id=42 io: read/write on closed pipe (retry in 8s)") and remux writes
// it to <id>.errors, so an operator chasing a channel that keeps reconnecting
// cannot tell a frozen upstream — the thing the watchdog detected — from a bug
// on this side of the socket.
func TestStallErrorNamesTheWatchdog(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a stall bound")
	}
	pr, pw := io.Pipe()
	defer pw.Close()
	rc := WrapIdleTimeout(pr, time.Second)
	defer rc.Close()

	_, err := io.ReadAll(rc) // nothing is ever written: the source is frozen
	if err == nil {
		t.Fatal("a frozen source read clean")
	}
	if !strings.Contains(err.Error(), "source idle for 1s") {
		t.Fatalf("stall surfaced as %q, which says nothing about the stall watchdog", err)
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("stall error %v no longer carries the underlying cause", err)
	}
}

// TestCloseByTheConsumerIsNotReportedAsAStall: the flag must mean the watchdog
// fired, not merely that the source is closed. A viewer going away closes the
// reader too, and blaming the upstream for that would be a new wrong log line in
// place of an unhelpful one.
func TestCloseByTheConsumerIsNotReportedAsAStall(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	rc := WrapIdleTimeout(pr, time.Minute)
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := rc.Read(make([]byte, 16)); err == nil {
		t.Fatal("read after close succeeded")
	} else if strings.Contains(err.Error(), "source idle") {
		t.Fatalf("a consumer's own close was reported as an upstream stall: %v", err)
	}
}

// TestCleanEOFIsUntouched: io.Copy compares against io.EOF by identity, so a
// source that simply ended must keep ending cleanly.
func TestCleanEOFIsUntouched(t *testing.T) {
	rc := WrapIdleTimeout(io.NopCloser(strings.NewReader("done")), time.Minute)
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll = %v, want a clean end", err)
	}
	if string(b) != "done" {
		t.Fatalf("read %q, want done", b)
	}
}
