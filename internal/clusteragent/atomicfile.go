package clusteragent

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

// fileWrite says how writeFile puts a file in place. Every write goes to a
// temporary file in the target's directory, named with a leading dot and a
// .tmp suffix (the readers skip dot files), created exclusively and renamed
// in: a reader sees the old file or the new one, never half of one. A write
// that fails removes its temporary file and leaves the target as it was.
type fileWrite struct {
	// perm is the file's mode, less the umask unless exact.
	perm  os.FileMode
	exact bool
	// fixedTemp names the temporary file .<name>.tmp, as ADR 0004 names the
	// replica's, and removes one a crash left first. Otherwise it is
	// .<name>.<random>.tmp, so two writers of one file never share it.
	fixedTemp bool
	// noSync renames the file in with no fsync, as PHP's EventSpool writes
	// its spool files. Otherwise the file is fsynced before the rename and
	// its directory after, so a crash leaves the old file or the new one,
	// and files written in turn reach the disk in that order.
	noSync bool
	// mustSyncDir makes the directory's sync part of the write: its failure
	// is returned, the file renamed in. Otherwise that sync is best effort.
	mustSyncDir bool
}

// writeFile writes b to path atomically, as w says.
func writeFile(path string, b []byte, w fileWrite) error {
	dir := filepath.Dir(path)
	f, tmp, err := createTemp(dir, filepath.Base(path), w)
	if err != nil {
		return err
	}
	if err := fillTemp(f, b, w); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	switch {
	case w.noSync:
	case w.mustSyncDir:
		return syncDir(dir)
	default:
		if d, err := os.Open(dir); err == nil {
			d.Sync()
			d.Close()
		}
	}
	return nil
}

// createTemp creates writeFile's temporary file, never through a link or
// over a file already there.
func createTemp(dir, base string, w fileWrite) (*os.File, string, error) {
	const flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if w.fixedTemp {
		tmp := filepath.Join(dir, "."+base+".tmp")
		os.Remove(tmp)
		f, err := os.OpenFile(tmp, flags, w.perm)
		return f, tmp, err
	}
	for try := 0; ; try++ {
		var r [8]byte
		if _, err := rand.Read(r[:]); err != nil {
			return nil, "", err
		}
		tmp := filepath.Join(dir, "."+base+"."+hex.EncodeToString(r[:])+".tmp")
		f, err := os.OpenFile(tmp, flags, w.perm)
		if err == nil || !errors.Is(err, os.ErrExist) || try == 9 {
			return f, tmp, err
		}
	}
}

// fillTemp writes, syncs and closes writeFile's temporary file.
func fillTemp(f *os.File, b []byte, w fileWrite) error {
	if w.exact {
		if err := f.Chmod(w.perm); err != nil {
			f.Close()
			return err
		}
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if !w.noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}
