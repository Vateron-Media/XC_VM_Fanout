package clusteragent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// certbotDomains reads the node replica section's domain_name (CSV), keeping only
// valid hostnames — ports CertbotCommand::hostNames (trim+basename, reject IPs
// and non-hostnames), de-duplicated in first-seen order.
func TestCertbotDomainsFromNode(t *testing.T) {
	dir := t.TempDir()
	node := filepath.Join(dir, "node.json")
	os.WriteFile(node, []byte(`{"etag":"e","data":{"id":7,"domain_name":"lb1.example.com, 203.0.113.9 , bad_host, lb1.example.com ,LB2.Example.com"}}`), 0o644)
	got := certbotDomains(node)
	want := []string{"lb1.example.com", "LB2.Example.com"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("certbotDomains = %v, want %v (IP + underscore dropped, de-duped, trimmed)", got, want)
	}

	empty := filepath.Join(dir, "empty.json")
	os.WriteFile(empty, []byte(`{"etag":"e","data":{"id":7,"domain_name":""}}`), 0o644)
	if got := certbotDomains(empty); len(got) != 0 {
		t.Fatalf("empty domain_name: got %v, want []", got)
	}
	if got := certbotDomains(filepath.Join(dir, "absent.json")); len(got) != 0 {
		t.Fatalf("absent node.json: got %v, want []", got)
	}
}

// runCertbot runs certbot through the xcvm-noderoot helper and reads the
// outcome from its output: exit 0 is a change; "not yet due for renewal" is
// success with no change; any other non-zero is an error. A fake helper stands
// in for `sudo -n xcvm-noderoot`.
func TestRunCertbotOutcomes(t *testing.T) {
	dir := t.TempDir()
	oldSudo, oldHelper := certbotSudo, certbotHelper
	certbotSudo = ""
	defer func() { certbotSudo, certbotHelper = oldSudo, oldHelper }()
	ctx := context.Background()

	argvLog := filepath.Join(dir, "argv")
	ok := filepath.Join(dir, "helper_ok")
	os.WriteFile(ok, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > "+argvLog+"\nexit 0\n"), 0o755)
	certbotHelper = ok
	changed, err := runCertbot(ctx, []string{"a.example"})
	if !changed || err != nil {
		t.Fatalf("exit 0: changed=%v err=%v, want true,nil", changed, err)
	}
	// The agent invokes the helper as `xcvm-noderoot certbot <domain>…`; the
	// certonly/--webroot argv and fixed paths live in the helper itself.
	if b, _ := os.ReadFile(argvLog); !strings.Contains(string(b), "certbot") || !strings.Contains(string(b), "a.example") {
		t.Fatalf("helper argv missing certbot/<domain>: %s", b)
	}

	notDue := filepath.Join(dir, "helper_notdue")
	os.WriteFile(notDue, []byte("#!/bin/sh\necho 'Certificate not yet due for renewal'\nexit 1\n"), 0o755)
	certbotHelper = notDue
	if changed, err := runCertbot(ctx, []string{"a.example"}); changed || err != nil {
		t.Fatalf("not-due: changed=%v err=%v, want false,nil", changed, err)
	}

	fail := filepath.Join(dir, "helper_fail")
	os.WriteFile(fail, []byte("#!/bin/sh\necho 'some validation error'\nexit 1\n"), 0o755)
	certbotHelper = fail
	if _, err := runCertbot(ctx, []string{"a.example"}); err == nil {
		t.Fatal("other non-zero exit must be an error")
	}

	// An existing node whose helper predates the certbot verb: `unknown verb` is
	// reported distinctly so the agent degrades to keeping its current cert
	// instead of logging a hard failure every cycle.
	oldVerb := filepath.Join(dir, "helper_oldverb")
	os.WriteFile(oldVerb, []byte("#!/bin/sh\necho 'xcvm-noderoot: unknown verb' >&2\nexit 1\n"), 0o755)
	certbotHelper = oldVerb
	if _, err := runCertbot(ctx, []string{"a.example"}); !errors.Is(err, errHelperNoCertbot) {
		t.Fatalf("an old helper (unknown verb) must be errHelperNoCertbot, got %v", err)
	}
}

// certbotRefresh degrades quietly when the helper has no certbot verb: no error
// bubbles up (the node keeps its current certificate until a server:install
// refreshes the root helper).
func TestCertbotRefreshSkipsAnOldHelper(t *testing.T) {
	dir := t.TempDir()
	oldSudo, oldHelper := certbotSudo, certbotHelper
	certbotSudo = ""
	defer func() { certbotSudo, certbotHelper = oldSudo, oldHelper }()

	oldVerb := filepath.Join(dir, "helper_oldverb")
	os.WriteFile(oldVerb, []byte("#!/bin/sh\necho 'xcvm-noderoot: unknown verb' >&2\nexit 1\n"), 0o755)
	certbotHelper = oldVerb

	node := filepath.Join(dir, "node.json")
	os.WriteFile(node, []byte(`{"data":{"domain_name":"lb1.example.com"}}`), 0o644)
	a := &Agent{CertbotWebroot: "/home/xc_vm/certbot-webroot", ReplicaDir: dir}
	if err := a.certbotRefresh(context.Background()); err != nil {
		t.Fatalf("an old helper must not surface as an error, got %v", err)
	}
}

// RunCertbot with no webroot (non-serve-only or still-PHP node) returns at once.
func TestRunCertbotEmptyWebrootNoop(t *testing.T) {
	done := make(chan struct{})
	go func() { (&Agent{}).RunCertbot(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunCertbot with no webroot must return immediately")
	}
}
