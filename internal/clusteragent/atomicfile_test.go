package clusteragent

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// dotFiles lists dir's dot files: writeFile's temporary files.
func dotFiles(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestWriteFileReplacesInPlace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	for _, w := range []fileWrite{{perm: 0o640}, {perm: 0o600, exact: true}, {perm: 0o640, noSync: true}, {perm: 0o640, mustSyncDir: true}, {perm: 0o600, exact: true, fixedTemp: true}} {
		os.WriteFile(path, []byte("old"), 0o644)
		if err := writeFile(path, []byte("new"), w); err != nil {
			t.Fatalf("%+v: %v", w, err)
		}
		if b, _ := os.ReadFile(path); string(b) != "new" {
			t.Fatalf("%+v: read %q", w, b)
		}
		if left := dotFiles(t, dir); left != nil {
			t.Fatalf("%+v: left %v", w, left)
		}
	}
}

func TestWriteFileMode(t *testing.T) {
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)
	dir := t.TempDir()
	cases := []struct {
		w    fileWrite
		want os.FileMode
	}{
		{fileWrite{perm: 0o640}, 0o600}, // less the umask, as os.WriteFile
		{fileWrite{perm: 0o640, exact: true}, 0o640},
		{fileWrite{perm: 0o600, exact: true, fixedTemp: true}, 0o600},
	}
	for i, c := range cases {
		path := filepath.Join(dir, "f"+string(rune('a'+i)))
		if err := writeFile(path, []byte("x"), c.w); err != nil {
			t.Fatal(err)
		}
		if fi, _ := os.Stat(path); fi.Mode().Perm() != c.want {
			t.Fatalf("%+v: mode %o, want %o", c.w, fi.Mode().Perm(), c.want)
		}
	}
}

func TestWriteFileTempNames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "7.json")
	// A crash's leftover of the fixed name is replaced, never renamed in as is.
	os.WriteFile(filepath.Join(dir, ".7.json.tmp"), []byte("stale"), 0o600)
	if err := writeFile(path, []byte("new"), fileWrite{perm: 0o600, exact: true, fixedTemp: true}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != "new" {
		t.Fatalf("read %q", b)
	}
	if left := dotFiles(t, dir); left != nil {
		t.Fatalf("left %v", left)
	}
	// A unique name: one that is there (another writer's) is left alone.
	other := filepath.Join(dir, ".7.json.tmp")
	os.WriteFile(other, []byte("theirs"), 0o600)
	if err := writeFile(path, []byte("newer"), fileWrite{perm: 0o600}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(other); string(b) != "theirs" {
		t.Fatalf("another writer's temporary file changed: %q", b)
	}
}

func TestWriteFileFailures(t *testing.T) {
	dir := t.TempDir()

	// The rename fails (the target is a directory): the error, no temporary
	// file left, the target as it was.
	target := filepath.Join(dir, "busy")
	os.Mkdir(target, 0o700)
	os.WriteFile(filepath.Join(target, "inside"), nil, 0o600)
	for _, w := range []fileWrite{{perm: 0o640}, {perm: 0o600, exact: true, fixedTemp: true}, {perm: 0o640, noSync: true}} {
		if err := writeFile(target, []byte("x"), w); err == nil {
			t.Fatalf("%+v: renamed over a directory", w)
		}
		if left := dotFiles(t, dir); left != nil {
			t.Fatalf("%+v: left %v", w, left)
		}
		if fi, err := os.Stat(target); err != nil || !fi.IsDir() {
			t.Fatalf("%+v: the target changed", w)
		}
	}

	// No directory: nothing created.
	if err := writeFile(filepath.Join(dir, "none", "f"), []byte("x"), fileWrite{perm: 0o640}); err == nil {
		t.Fatal("wrote into a missing directory")
	}

	// A link where the fixed temporary file goes is removed, never followed.
	victim := filepath.Join(dir, "victim")
	os.WriteFile(victim, []byte("keep"), 0o600)
	os.Symlink(victim, filepath.Join(dir, ".linked.tmp"))
	if err := writeFile(filepath.Join(dir, "linked"), []byte("x"), fileWrite{perm: 0o600, exact: true, fixedTemp: true}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "keep" {
		t.Fatalf("wrote through a link: %q", b)
	}

	// The directory's sync fails: returned when it must succeed (the file in
	// place), ignored when it is best effort.
	realSync := syncDir
	swap(t, &syncDir, func(d string) error {
		if d == dir {
			return errors.New("EIO")
		}
		return realSync(d)
	})
	path := filepath.Join(dir, "manifest")
	if err := writeFile(path, []byte("m"), fileWrite{perm: 0o640, mustSyncDir: true}); err == nil || err.Error() != "EIO" {
		t.Fatalf("the directory's sync did not fail the write: %v", err)
	}
	if b, _ := os.ReadFile(path); string(b) != "m" {
		t.Fatalf("read %q", b)
	}
	if err := writeFile(path, []byte("n"), fileWrite{perm: 0o640}); err != nil {
		t.Fatal(err)
	}
	if left := dotFiles(t, dir); left != nil {
		t.Fatalf("left %v", left)
	}
}
