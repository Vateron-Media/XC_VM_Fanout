//go:build unix

package supervisor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDriverConfigValidatesTrustedMappings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drivers.json")
	if err := os.WriteFile(path, []byte(`{"acme-dash":"/usr/local/libexec/acme-dash"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	drivers, err := loadDriverConfig(path, uint32(os.Geteuid()))
	if err != nil || drivers["acme-dash"] != "/usr/local/libexec/acme-dash" {
		t.Fatalf("loadDriverConfig = %v, %v", drivers, err)
	}
	if err := os.Chmod(path, 0o620); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDriverConfig(path, uint32(os.Geteuid())); err == nil {
		t.Fatal("group-writable driver config accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"bad/id":"relative"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDriverConfig(path, uint32(os.Geteuid())); err == nil {
		t.Fatal("invalid ID and relative executable accepted")
	}
}

func TestDriverConfigRejectsNonRootOwner(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("test needs a non-root owner")
	}
	path := filepath.Join(t.TempDir(), "drivers.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDriverConfig(path); err == nil {
		t.Fatal("non-root-owned driver config accepted")
	}
}
