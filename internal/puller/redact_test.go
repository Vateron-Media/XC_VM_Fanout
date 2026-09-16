// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
