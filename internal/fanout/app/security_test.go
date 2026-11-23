//go:build unix

package fanoutcmd

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestControlSocketModeIsOwnerOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sock")
	srv, cleanup, err := listenUnixMode(path, 0o600, http.NewServeMux())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	defer srv.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("control socket mode = %o", got)
	}
}
