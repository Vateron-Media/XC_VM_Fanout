// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
)

// thePassword is the provider account password an XC source URL carries in its
// path. Nothing the daemon writes to a log may contain it.
const thePassword = "s3cr3t-pw"

// TestSourceCredentialsAreNotLogged: an XC source URL is
// http://host:8080/live/<user>/<pass>/<id>.ts, so every line that prints the
// URL prints the account password — and a provider answering 403 makes the
// puller print one on every backoff cycle, forever, into the log support reads
// and ships off the box.
func TestSourceCredentialsAreNotLogged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()
	raw := srv.URL + "/live/joe/" + thePassword + "/1234.ts"

	// The error the puller logs on a refusing provider.
	if _, err := probe(context.Background(), mustClient(t, Source{}), Source{}, raw); err == nil {
		t.Fatal("a 403 source must fail the probe")
	} else if strings.Contains(err.Error(), thePassword) {
		t.Errorf("probe error carries the account password: %q", err)
	}

	// And the one it logs when the source cannot be reached at all: net/http
	// wraps the whole URL in *url.Error.
	dead := "http://127.0.0.1:1/live/joe/" + thePassword + "/1234.ts"
	if _, err := probe(context.Background(), mustClient(t, Source{}), Source{}, dead); err == nil {
		t.Fatal("a closed port must fail the probe")
	} else if strings.Contains(err.Error(), thePassword) {
		t.Errorf("transport error carries the account password: %q", err)
	}

	// Now the whole loop, with the debug narration on: nothing it writes may
	// carry the password, from the start line's URL list onwards.
	dlog.Enable(true)
	t.Cleanup(func() { dlog.Enable(false) })
	sink := captureLog(t)
	recordWaits(t, 1)
	Run(context.Background(), Source{
		URLs:  []string{raw, dead},
		Proxy: "joe:" + thePassword + "@127.0.0.1:1", // refused at once, not a 10s dial
		Label: "1234",
	}, 12032, func([]byte) {})

	out := sink.String()
	if strings.Contains(out, thePassword) {
		t.Errorf("the puller log carries the account password:\n%s", out)
	}
	// Redaction has to leave the line worth reading: the stream and the host
	// are why it is logged at all.
	if !strings.Contains(out, "id=1234") || !strings.Contains(out, "127.0.0.1") {
		t.Errorf("the redacted log no longer says which stream and host failed:\n%s", out)
	}
}

// TestFfmpegStderrTailIsNotLoggedVerbatim: the child's stderr is untrusted
// text, and ffmpeg quotes the whole input URL on any failure to open it —
// "Error opening input file http://host/live/<user>/<pass>/1234.ts." — so an XC
// source pinned to (or falling back to) ffmpeg against a provider answering 403
// wrote the account password into the daemon log on every reconnect, through
// the one line that carries it. That is the same log support reads and ships
// off the box, and the same failure the URL redaction was done for.
func TestFfmpegStderrTailIsNotLoggedVerbatim(t *testing.T) {
	raw := "http://127.0.0.1:18081/live/joe/" + thePassword + "/1234.ts"
	dir := t.TempDir()
	bin := filepath.Join(dir, "fakeffmpeg")
	// Exactly what ffmpeg n7.1.5 writes for a 403, measured against a local
	// refusing server.
	script := "#!/bin/sh\n" +
		"echo 'Error opening input: Server returned 403 Forbidden (access denied)' >&2\n" +
		"echo 'Error opening input file " + raw + ".' >&2\n" +
		"exit 1\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	dlog.Enable(true)
	t.Cleanup(func() { dlog.Enable(false) })
	sink := captureLog(t)

	if err := runFfmpeg(context.Background(), Source{FfmpegBin: bin, Label: "1234"},
		raw, 8*time.Second, 12032, func([]byte) {}); err == nil {
		t.Fatal("a stand-in ffmpeg that exits 1 must surface an error")
	}

	out := sink.String()
	if strings.Contains(out, thePassword) {
		t.Errorf("ffmpeg's stderr tail put the account password in the log:\n%s", out)
	}
	// And the line still has to be worth logging: the reason and the host are
	// why it is written at all.
	for _, want := range []string{"403 Forbidden", "127.0.0.1:18081", "id=1234"} {
		if !strings.Contains(out, want) {
			t.Errorf("the scrubbed tail no longer says %q:\n%s", want, out)
		}
	}
}
