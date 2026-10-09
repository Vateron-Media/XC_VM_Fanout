// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/supervisor"
)

func TestReadSourceFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "5_.source")
	body := `{"i":"http://u:p@src/live.ts","user_agent":"VLC","cookies":"a=b","http_proxy":"10.0.0.1:3128","headers":"X-A: 1\r\n"}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	sf, err := readSourceFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := sourceFile{Input: "http://u:p@src/live.ts", UserAgent: "VLC", Cookies: "a=b", Proxy: "10.0.0.1:3128", Headers: "X-A: 1\r\n"}
	if sf != want {
		t.Fatalf("got %+v, want %+v", sf, want)
	}

	if err := os.WriteFile(path, []byte(`{"i":"http://u:secret@src`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSourceFile(path); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("bad JSON: err %v, want an error that does not echo the content", err)
	}
}

// TestRemuxTakesItsSourceFromTheFile: the input comes from the file (an rtmp://
// one is refused up front with ExitUnsupported, before any network), and -i
// beside -source_file is a usage error.
func TestRemuxTakesItsSourceFromTheFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "5_.source")
	if err := os.WriteFile(path, []byte(`{"i":"rtmp://src/live"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	playlist := filepath.Join(dir, "5_.m3u8")
	if got := runRemux([]string{"-loglevel", "quiet", "-source_file", path, playlist}); got != supervisor.ExitUnsupported {
		t.Errorf("rtmp source from the file: exit %d, want %d", got, supervisor.ExitUnsupported)
	}
	if got := runRemux([]string{"-loglevel", "quiet", "-i", "http://x/a.ts", "-source_file", path, playlist}); got != 2 {
		t.Errorf("-i with -source_file: exit %d, want 2", got)
	}
	if got := runRemux([]string{"-loglevel", "quiet", "-source_file", filepath.Join(dir, "missing"), playlist}); got != 2 {
		t.Errorf("missing source file: exit %d, want 2", got)
	}
}
