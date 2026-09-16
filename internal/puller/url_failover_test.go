// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// deadFfmpeg is a stand-in that cannot open the source either — what the real
// ffmpeg does for a missing file or a multicast group with no route.
func deadFfmpeg(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "fakeffmpeg")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho 'cannot open source' >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

// TestNativeOnlyURLFallsThroughToBackup: a udp/rtp/file primary that delivers
// nothing must fall through to the next configured URL, exactly as a failed
// HTTP probe does.
//
// Source.URLs is documented as "candidate source URLs, tried in order", but the
// native-only branch RETURNED the error instead of recording it and continuing.
// Since Run always restarts pullOnce at URLs[0], the operator's healthy HTTP
// backup was never contacted even once, for the life of the stream — while the
// same config with an HTTP primary would have failed over on the first attempt.
func TestNativeOnlyURLFallsThroughToBackup(t *testing.T) {
	// A bare path is a native-only source (nativeOnlyScheme matches a leading
	// "/"), and this one does not exist: the native reader fails and so does the
	// ffmpeg fallback — the "missing file" / "no route to the group" case.
	missing := filepath.Join(t.TempDir(), "no-such-source.ts")

	payload := tsfixture.Concat(
		tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101), tsfixture.Keyframe(0x101, 0),
	)
	backup := serveTS("video/mp2t", payload)
	defer backup.Close()

	var got []byte
	var paths []string
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := pullOnce(ctx, mustClient(t, Source{}), Source{
		URLs:      []string{missing, backup.URL},
		FfmpegBin: deadFfmpeg(t),
		Label:     "t",
		OnPath:    func(p string) { paths = append(paths, p) },
	}, 12032, func(b []byte) { got = append(got, b...) })

	if err != nil && err != io.EOF {
		t.Logf("pullOnce returned %v (the primary's failure is expected to be logged)", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("delivered %d bytes, want the backup's %d: a dead native-only primary hid a healthy backup (routes=%v, err=%v)",
			len(got), len(payload), paths, err)
	}
	if len(paths) == 0 || paths[len(paths)-1] != PathDirectMP2T {
		t.Errorf("routes = %v, want the backup served direct mp2t", paths)
	}
}

// TestNativeOnlyURLThatStreamedDoesNotRotate is the other half of the contract:
// a primary that WAS on air and then ended must not hand the stream to a backup
// on its way out. Rotating on any error would migrate a channel off its primary
// after a single hiccup hours into a healthy run; Run's backoff already brings
// it back to URLs[0].
func TestNativeOnlyURLThatStreamedDoesNotRotate(t *testing.T) {
	payload := tsfixture.Concat(
		tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101), tsfixture.Keyframe(0x101, 0),
	)
	primary := filepath.Join(t.TempDir(), "live.ts")
	if err := os.WriteFile(primary, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	var backupHits atomic.Int32
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backupHits.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(payload)
	}))
	defer backup.Close()

	var got []byte
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := pullOnce(ctx, mustClient(t, Source{}), Source{
		URLs:      []string{primary, backup.URL},
		FfmpegBin: deadFfmpeg(t),
		Label:     "t",
	}, 12032, func(b []byte) { got = append(got, b...) })

	if err != nil && err != io.EOF {
		t.Fatalf("pullOnce over a readable file source: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("delivered %d bytes from the primary, want %d", len(got), len(payload))
	}
	if n := backupHits.Load(); n != 0 {
		t.Fatalf("the backup was contacted %d time(s) although the primary streamed fine", n)
	}
}
