package clusteragent

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
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

// certbotArgs is the serve-only --webroot invocation of CertbotCommand.
func TestCertbotArgs(t *testing.T) {
	got := certbotArgs("/home/xc_vm/bin/", "/home/xc_vm/certbot-webroot", []string{"a.example", "b.example"})
	want := []string{
		"certonly", "--config-dir", "/home/xc_vm/bin/certbot/config",
		"--work-dir", "/home/xc_vm/bin/certbot/work", "--logs-dir", "/home/xc_vm/bin/certbot/logs",
		"--agree-tos", "--expand", "--non-interactive", "--register-unsafely-without-email",
		"--webroot", "-w", "/home/xc_vm/certbot-webroot", "-d", "a.example", "-d", "b.example",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("certbotArgs =\n%v\nwant\n%v", got, want)
	}
}

// runCertbot: exit 0 is a change; "not yet due for renewal" is success with no
// change; any other non-zero is an error. A fake certbot stands in for sudo.
func TestRunCertbotOutcomes(t *testing.T) {
	dir := t.TempDir()
	old := certbotSudo
	certbotSudo = ""
	defer func() { certbotSudo = old }()
	ctx := context.Background()

	argvLog := filepath.Join(dir, "argv")
	ok := filepath.Join(dir, "certbot_ok")
	os.WriteFile(ok, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > "+argvLog+"\nexit 0\n"), 0o755)
	changed, err := runCertbot(ctx, ok, "/home/xc_vm/bin/", "/home/xc_vm/certbot-webroot", []string{"a.example"})
	if !changed || err != nil {
		t.Fatalf("exit 0: changed=%v err=%v, want true,nil", changed, err)
	}
	if b, _ := os.ReadFile(argvLog); !strings.Contains(string(b), "--webroot") || !strings.Contains(string(b), "a.example") {
		t.Fatalf("certbot argv missing --webroot/-d: %s", b)
	}

	notDue := filepath.Join(dir, "certbot_notdue")
	os.WriteFile(notDue, []byte("#!/bin/sh\necho 'Certificate not yet due for renewal'\nexit 1\n"), 0o755)
	if changed, err := runCertbot(ctx, notDue, "/b/", "/wr", []string{"a.example"}); changed || err != nil {
		t.Fatalf("not-due: changed=%v err=%v, want false,nil", changed, err)
	}

	fail := filepath.Join(dir, "certbot_fail")
	os.WriteFile(fail, []byte("#!/bin/sh\necho 'some validation error'\nexit 1\n"), 0o755)
	if _, err := runCertbot(ctx, fail, "/b/", "/wr", []string{"a.example"}); err == nil {
		t.Fatal("other non-zero exit must be an error")
	}
}
