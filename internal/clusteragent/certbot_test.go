package clusteragent

import (
	"os"
	"path/filepath"
	"reflect"
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
