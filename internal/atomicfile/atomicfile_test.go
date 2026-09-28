package atomicfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// leftovers lists dir's temporary files: dot files and *.tmp.
func leftovers(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".") || strings.HasSuffix(e.Name(), ".tmp") {
			out = append(out, e.Name())
		}
	}
	return out
}

var every = []Options{
	{Perm: 0o640},
	{Perm: 0o600, Exact: true},
	{Perm: 0o640, NoSync: true},
	{Perm: 0o600, Exact: true, Temp: Fixed},
	{Perm: 0o644, Temp: Suffix, NoSync: true},
	{Perm: 0o640, SyncDir: func(string) error { return nil }},
}

func TestWriteReplacesInPlace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	for _, o := range every {
		os.WriteFile(path, []byte("old"), 0o644)
		if err := Write(path, []byte("new"), o); err != nil {
			t.Fatalf("%+v: %v", o, err)
		}
		if b, _ := os.ReadFile(path); string(b) != "new" {
			t.Fatalf("%+v: read %q", o, b)
		}
		if left := leftovers(t, dir); left != nil {
			t.Fatalf("%+v: left %v", o, left)
		}
	}
}

func TestWriteMode(t *testing.T) {
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)
	dir := t.TempDir()
	cases := []struct {
		o    Options
		want os.FileMode
	}{
		{Options{Perm: 0o640}, 0o600}, // less the umask, as os.WriteFile
		{Options{Perm: 0o644, Temp: Suffix}, 0o600},
		{Options{Perm: 0o640, Exact: true}, 0o640},
		{Options{Perm: 0o600, Exact: true, Temp: Fixed}, 0o600},
	}
	for i, c := range cases {
		path := filepath.Join(dir, "f"+string(rune('a'+i)))
		if err := Write(path, []byte("x"), c.o); err != nil {
			t.Fatal(err)
		}
		if fi, _ := os.Stat(path); fi.Mode().Perm() != c.want {
			t.Fatalf("%+v: mode %o, want %o", c.o, fi.Mode().Perm(), c.want)
		}
	}
}

func TestTempNames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "7.json")
	for _, c := range []struct {
		temp Temp
		name string
	}{{Fixed, ".7.json.tmp"}, {Suffix, "7.json.tmp"}} {
		// A crash's leftover of the fixed name is replaced, never renamed in as is.
		os.WriteFile(filepath.Join(dir, c.name), []byte("stale"), 0o600)
		f, err := Create(path, Options{Perm: 0o600, Temp: c.temp})
		if err != nil {
			t.Fatal(err)
		}
		if f.TempPath() != filepath.Join(dir, c.name) {
			t.Fatalf("temporary file %s, want %s", f.TempPath(), c.name)
		}
		f.Write([]byte("new"))
		if err := f.Commit(); err != nil {
			t.Fatal(err)
		}
		if b, _ := os.ReadFile(path); string(b) != "new" {
			t.Fatalf("read %q", b)
		}
		if left := leftovers(t, dir); left != nil {
			t.Fatalf("left %v", left)
		}
	}
	// A unique name: one that is there (another writer's) is left alone.
	other := filepath.Join(dir, ".7.json.tmp")
	os.WriteFile(other, []byte("theirs"), 0o600)
	f, err := Create(path, Options{Perm: 0o600})
	if err != nil {
		t.Fatal(err)
	}
	tmp := f.TempPath()
	f.Abort()
	if name := filepath.Base(tmp); !strings.HasPrefix(name, ".7.json.") || !strings.HasSuffix(name, ".tmp") || name == ".7.json.tmp" || filepath.Dir(tmp) != dir {
		t.Fatalf("temporary file %s", tmp)
	}
	if err := Write(path, []byte("newer"), Options{Perm: 0o600}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(other); string(b) != "theirs" {
		t.Fatalf("another writer's temporary file changed: %q", b)
	}
}

// A streamed write: nothing at the path until Commit, nothing after Abort.
func TestCreateCommitAbort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "12_0.ts")
	f, err := Create(path, Options{Perm: 0o644, Temp: Suffix, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("seg"))
	if _, err := os.Stat(path); err == nil {
		t.Fatal("the target appeared before Commit")
	}
	f.Abort()
	if left := leftovers(t, dir); left != nil {
		t.Fatalf("left %v", left)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("an aborted write reached the target")
	}
	f, _ = Create(path, Options{Perm: 0o644, Temp: Suffix, NoSync: true})
	f.Write([]byte("seg"))
	if err := f.Commit(); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != "seg" {
		t.Fatalf("read %q", b)
	}
}

func TestWriteFailures(t *testing.T) {
	dir := t.TempDir()

	// The rename fails (the target is a directory): the error, no temporary
	// file left, the target as it was.
	target := filepath.Join(dir, "busy")
	os.Mkdir(target, 0o700)
	os.WriteFile(filepath.Join(target, "inside"), nil, 0o600)
	for _, o := range every {
		if err := Write(target, []byte("x"), o); err == nil {
			t.Fatalf("%+v: renamed over a directory", o)
		}
		if left := leftovers(t, dir); left != nil {
			t.Fatalf("%+v: left %v", o, left)
		}
		if fi, err := os.Stat(target); err != nil || !fi.IsDir() {
			t.Fatalf("%+v: the target changed", o)
		}
	}

	// No directory: nothing created.
	if err := Write(filepath.Join(dir, "none", "f"), []byte("x"), Options{Perm: 0o640}); err == nil {
		t.Fatal("wrote into a missing directory")
	}

	// A link where a fixed temporary file goes is removed, never followed.
	for _, c := range []struct {
		temp Temp
		link string
	}{{Fixed, ".linked.tmp"}, {Suffix, "linked.tmp"}} {
		victim := filepath.Join(dir, "victim")
		os.WriteFile(victim, []byte("keep"), 0o600)
		os.Symlink(victim, filepath.Join(dir, c.link))
		if err := Write(filepath.Join(dir, "linked"), []byte("x"), Options{Perm: 0o600, Temp: c.temp}); err != nil {
			t.Fatal(err)
		}
		if b, _ := os.ReadFile(victim); string(b) != "keep" {
			t.Fatalf("wrote through a link: %q", b)
		}
	}

	// The directory's sync fails: returned when SyncDir is given (the file
	// in place), never under NoSync.
	path := filepath.Join(dir, "manifest")
	fail := func(string) error { return errors.New("EIO") }
	if err := Write(path, []byte("m"), Options{Perm: 0o640, SyncDir: fail}); err == nil || err.Error() != "EIO" {
		t.Fatalf("the directory's sync did not fail the write: %v", err)
	}
	if b, _ := os.ReadFile(path); string(b) != "m" {
		t.Fatalf("read %q", b)
	}
	if err := Write(path, []byte("n"), Options{Perm: 0o640, NoSync: true, SyncDir: fail}); err != nil {
		t.Fatal(err)
	}
}
