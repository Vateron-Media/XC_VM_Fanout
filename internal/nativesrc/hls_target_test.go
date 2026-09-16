// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"fmt"
	"testing"
	"time"
)

// TestTargetDurationIsClamped pins both ends of a hostile or broken
// #EXT-X-TARGETDURATION, because every timing decision the pull makes is derived
// from it.
//
// A huge value overflows time.Duration: the poll wait goes NEGATIVE, so the
// timer fires immediately and the puller refetches the manifest in a tight loop
// against the upstream — one stream, one provider account, thousands of requests
// a second. An upstream that writes milliseconds (6000) instead puts the polls 50
// minutes apart with a five-hour stall bound, so the channel plays its first
// segments and then sits frozen for hours with the idle watchdog asleep.
func TestTargetDurationIsClamped(t *testing.T) {
	for _, c := range []struct{ name, tag string }{
		{"int64 max", "9223372036854775807"},
		{"overflows the duration", "18446744074"},
		{"milliseconds, not seconds", "6000"},
		{"zero", "0"},
		{"negative", "-5"},
		{"not a number", "abc"},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := fmt.Sprintf("#EXTM3U\n#EXT-X-TARGETDURATION:%s\n#EXTINF:1.0,\ns0.ts\n", c.tag)
			pl, err := parseHLSPlaylist([]byte(body), nil)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if pl.TargetDuration < 1 || pl.TargetDuration > 60 {
				t.Errorf("TargetDuration = %d, want a sane number of seconds", pl.TargetDuration)
			}
			if w := hlsPollWait(pl); w < time.Second || w > 60*time.Second {
				t.Errorf("poll wait = %s: the manifest is refetched in a tight loop (or never)", w)
			}
			if b := hlsIdleBound(pl); b < DefaultSourceIdleTimeout || b > 3*60*time.Second {
				t.Errorf("idle bound = %s: a frozen source is never noticed", b)
			}
		})
	}
}

// TestTargetDurationIsKeptWhenSane: clamping must not move a real playlist's
// cadence — the poll wait is half the target duration, and the stall bound three
// times it.
func TestTargetDurationIsKeptWhenSane(t *testing.T) {
	pl, err := parseHLSPlaylist([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXTINF:10.0,\ns0.ts\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pl.TargetDuration != 10 {
		t.Fatalf("TargetDuration = %d, want 10 untouched", pl.TargetDuration)
	}
	if w := hlsPollWait(pl); w != 5*time.Second {
		t.Errorf("poll wait = %s, want 5s", w)
	}
	if b := hlsIdleBound(pl); b != 30*time.Second {
		t.Errorf("idle bound = %s, want 30s", b)
	}
}

// TestNoTargetDurationKeepsTheFloor: a playlist without the tag is not a broken
// value — it keeps the 1s poll floor and the default stall bound it has always
// had, rather than being given a made-up cadence.
func TestNoTargetDurationKeepsTheFloor(t *testing.T) {
	pl, err := parseHLSPlaylist([]byte("#EXTM3U\n#EXTINF:1.0,\ns0.ts\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pl.TargetDuration != 0 {
		t.Fatalf("TargetDuration = %d, want 0 when the tag is absent", pl.TargetDuration)
	}
	if w := hlsPollWait(pl); w != time.Second {
		t.Errorf("poll wait = %s, want the 1s floor", w)
	}
	if b := hlsIdleBound(pl); b != DefaultSourceIdleTimeout {
		t.Errorf("idle bound = %s, want the default", b)
	}
}
