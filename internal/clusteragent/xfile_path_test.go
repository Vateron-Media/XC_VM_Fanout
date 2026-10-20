package clusteragent

import (
	"bytes"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

func TestScanRootsParsing(t *testing.T) {
	def := []string{"/home/xc_vm/content", "/mnt", "/media"}
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"json array", `["/mnt/a","/media/b/"]`, []string{"/mnt/a", "/media/b"}},
		{"newline delimited", "/mnt/a\n/media/b\n", []string{"/mnt/a", "/media/b"}},
		{"empty -> defaults", "", def},
		{"whitespace -> defaults", "   \n  ", def},
		{"relative entry rejects all", `["/mnt/a","rel"]`, nil},
		{"dotdot rejects all", `["/mnt/../etc"]`, nil},
		{"double slash rejects all", `["/mnt//a"]`, nil},
	}
	for _, c := range cases {
		if got := scanRoots(c.raw); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: scanRoots(%q) = %v, want %v", c.name, c.raw, got, c.want)
		}
	}
}

func TestPathAllowed(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "mnt")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	ok := filepath.Join(root, "ok.ts")
	if err := os.WriteFile(ok, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A real file under the root, and the root itself, are allowed.
	if !pathAllowed(ok, []string{root}) {
		t.Fatal("a real file under the root was refused")
	}
	// A sibling directory sharing a name prefix is not "under" the root.
	sib := filepath.Join(dir, "mntfoo")
	os.MkdirAll(sib, 0o755)
	sf := filepath.Join(sib, "f.ts")
	os.WriteFile(sf, []byte("x"), 0o644)
	if pathAllowed(sf, []string{root}) {
		t.Fatal("/mntfoo matched as under /mnt")
	}
	// An intermediate-directory symlink that escapes the root is refused
	// (EvalSymlinks canonicalises before the prefix test).
	outside := filepath.Join(dir, "outside")
	os.MkdirAll(outside, 0o755)
	os.WriteFile(filepath.Join(outside, "x.ts"), []byte("x"), 0o644)
	os.Symlink(outside, filepath.Join(root, "esc"))
	if pathAllowed(filepath.Join(root, "esc", "x.ts"), []string{root}) {
		t.Fatal("an intermediate-symlink escape was allowed")
	}
}

func TestExtAllowed(t *testing.T) {
	for _, p := range []string{"/mnt/a.mkv", "/mnt/A.MKV", "/mnt/x.ts", "/mnt/y.m3u8", "/mnt/z.JPEG"} {
		if !extAllowed(p) {
			t.Errorf("%q should be allowed", p)
		}
	}
	for _, p := range []string{"/mnt/a.php", "/mnt/passwd", "/mnt/.env", "/mnt/a.so"} {
		if extAllowed(p) {
			t.Errorf("%q should be refused", p)
		}
	}
}

func TestOpenChunkSymlinkSafe(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real.ts")
	content := []byte("0123456789")
	if err := os.WriteFile(real, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// A normal real file serves its chunk.
	chunk, total, err := openChunk(real, 2, 5)
	if err != nil || total != 10 || !bytes.Equal(chunk, content[2:7]) {
		t.Fatalf("real file: chunk=%q total=%d err=%v", chunk, total, err)
	}
	// offset == total: empty chunk, no error (200 empty body upstream).
	if c, tot, err := openChunk(real, 10, 5); err != nil || tot != 10 || len(c) != 0 {
		t.Fatalf("offset==total: chunk=%q total=%d err=%v", c, tot, err)
	}
	// offset > total: errOffsetPastEnd, total is the file size (for Content-Range).
	if _, tot, err := openChunk(real, 11, 5); !errors.Is(err, errOffsetPastEnd) || tot != 10 {
		t.Fatalf("offset>total: total=%d err=%v, want errOffsetPastEnd + total 10", tot, err)
	}
	// A short final chunk returns only the bytes that exist.
	if c, _, err := openChunk(real, 8, 100); err != nil || !bytes.Equal(c, content[8:]) {
		t.Fatalf("short final chunk: %q err=%v", c, err)
	}

	// A symlink whose target is a real file INSIDE the same dir is refused
	// (O_NOFOLLOW on the final component) — the documented divergence from PHP.
	link := filepath.Join(dir, "link.ts")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openChunk(link, 0, 5); err == nil {
		t.Fatal("a symlinked final component was followed (want refused)")
	}
}

func TestResolveFilePath(t *testing.T) {
	_, st := newFake(t) // ServerID = 3
	sk, pub, err := cc.NewX25519()
	if err != nil {
		t.Fatal(err)
	}
	st.NodeBoxSk = sk
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}

	path := "/mnt/media/a.ts"
	ref := cc.FileRef(3, path)
	sealed, err := cc.Seal(pub, "file", ref, []byte(path))
	if err != nil {
		t.Fatal(err)
	}
	file := "n." + base64.RawURLEncoding.EncodeToString(sealed)

	if got, ok := a.resolveFilePath(file, ref); !ok || got != path {
		t.Fatalf("resolveFilePath = %q, %v; want %q, true", got, ok, path)
	}
	// A ref that does not match the unsealed path is refused, even though the
	// seal opens (sealed under that same wrong ref as context).
	badRef := cc.FileRef(3, "/mnt/media/other.ts")
	sealedBad, _ := cc.Seal(pub, "file", badRef, []byte(path))
	if _, ok := a.resolveFilePath("n."+base64.RawURLEncoding.EncodeToString(sealedBad), badRef); ok {
		t.Fatal("a ref not matching the unsealed path resolved")
	}
	// The m.-prefixed (MAIN-owned) branch is refused on the node owner.
	if _, ok := a.resolveFilePath("m."+base64.RawURLEncoding.EncodeToString(sealed), ref); ok {
		t.Fatal("an m.-prefixed file resolved on the node owner")
	}
}
