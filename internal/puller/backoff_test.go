// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
)

// TestBackoffStartsOverAfterAHealthyRun: failures in a row back off to the cap;
// a failure after a long healthy run is a new incident and retries at once.
func TestBackoffStartsOverAfterAHealthyRun(t *testing.T) {
	b := defaults.PullBackoffInitial
	for i := 0; i < 6; i++ {
		b = nextBackoff(b, time.Second) // failing straight away, again and again
	}
	if b != defaults.PullBackoffMax {
		t.Fatalf("after repeated quick failures the backoff is %s, want the %s cap", b, defaults.PullBackoffMax)
	}
	if b = nextBackoff(b, 2*time.Hour); b != defaults.PullBackoffInitial {
		t.Fatalf("after a two-hour healthy run the backoff is %s, want it back to %s", b, defaults.PullBackoffInitial)
	}
}
