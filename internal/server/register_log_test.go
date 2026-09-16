// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// credSource is a registration carrying the account both ways an XC panel hands
// it over — as path segments and as userinfo — plus a proxy with its own.
func credSource() puller.Source {
	return puller.Source{
		URLs: []string{
			"http://box.example:8080/live/joe/s3cr3tpass/1234.ts",
			"http://joe:s3cr3tpass@backup.example/stream.ts",
		},
		Proxy: "http://proxyuser:pr0xyp4ss@proxy.example:3128",
	}
}

// TestRegisterDoesNotLogSourceCredentials: the panel re-registers a stream on
// every viewer request (live.php calls FanoutClient::register each time, HLS
// playlist polls included), and setConfig logged urls=%v proxy=%q raw. With
// debug on, the provider account and the proxy password were written to the
// journal hundreds of times a minute — into logs that support reads and that get
// shipped off the box. Mask them, and keep enough (host, stream id) for the line
// to still say which source was registered.
func TestRegisterDoesNotLogSourceCredentials(t *testing.T) {
	buf := captureDebugLog(t)

	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.Register("7", credSource(), 0)

	out := buf.String()
	if !strings.Contains(out, "registered pull config") {
		t.Fatalf("no registration line was logged at all: %q", out)
	}
	for _, secret := range []string{"s3cr3tpass", "pr0xyp4ss"} {
		if strings.Contains(out, secret) {
			t.Errorf("registration log leaks the credential %q:\n%s", secret, out)
		}
	}
	// Redaction that hides which source was registered would make the line
	// useless for the failure it is written for.
	for _, keep := range []string{"box.example:8080", "1234.ts", "proxy.example:3128"} {
		if !strings.Contains(out, keep) {
			t.Errorf("registration log no longer says %q, so it cannot identify the source:\n%s", keep, out)
		}
	}
}

// TestRegisterLogsOnlyRealChanges: the same re-registration storm is also pure
// noise — one line per viewer request for a config that did not move. Log a new
// registration and a genuine source edit; say nothing for a repeat.
func TestRegisterLogsOnlyRealChanges(t *testing.T) {
	buf := captureDebugLog(t)

	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.Register("7", credSource(), 0)
	for i := 0; i < 5; i++ {
		m.Register("7", credSource(), 0) // the panel, on every viewer request
	}
	if n := strings.Count(buf.String(), "registered pull config"); n != 1 {
		t.Errorf("%d registration lines for one config and five identical repeats, want 1", n)
	}

	edited := credSource()
	edited.URLs = []string{"http://other.example:8080/live/joe/s3cr3tpass/999.ts"}
	m.Register("7", edited, 0)
	if n := strings.Count(buf.String(), "registered pull config"); n != 2 {
		t.Errorf("%d registration lines after a real source edit, want 2", n)
	}
	if strings.Contains(buf.String(), "s3cr3tpass") {
		t.Errorf("the edited registration leaks the credential:\n%s", buf.String())
	}
}
