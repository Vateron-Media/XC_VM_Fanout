// Package atomicfile writes a file so that a reader sees the old file or the
// new one, never half of one: every write goes to a temporary file in the
// target's directory, created exclusively (never through a link or over a
// file already there) and renamed in. A write that fails removes its
// temporary file and leaves the target as it was.
package atomicfile

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

// Temp names the temporary file a write goes through.
type Temp int

const (
	// Random is .<name>.<16 hex>.tmp, so two writers of one file never
	// share it.
	Random Temp = iota
	// Fixed is .<name>.tmp, as ADR 0004 names the replica's files; one a
	// crash left is removed first.
	Fixed
	// Suffix is <name>.tmp, for a directory whose readers or sweepers
	// already know that name (the HLS segmenter's, the daemon config's);
	// one a crash left is removed first, as for Fixed.
	Suffix
)

// Options says how a file is put in place.
type Options struct {
	// Perm is the file's mode, less the umask unless Exact.
	Perm  os.FileMode
	Exact bool
	Temp  Temp
	// NoSync renames the file in with no fsync (the file's or its
	// directory's). Otherwise the file is fsynced before the rename and its
	// directory after, so a crash leaves the old file or the new one, and
	// files written in turn reach the disk in that order.
	NoSync bool
	// SyncDir, when set, syncs the directory after the rename and its error
	// is returned (the file renamed in). Otherwise that sync is best effort.
	SyncDir func(dir string) error
}

// File is a file being written: nothing changes at its path until Commit.
type File struct {
	f         *os.File
	path, tmp string
	o         Options
}

// Create opens the temporary file for path.
func Create(path string, o Options) (*File, error) {
	dir, base := filepath.Split(path)
	dir = filepath.Clean(dir)
	f, tmp, err := createTemp(dir, base, o)
	if err != nil {
		return nil, err
	}
	if o.Exact {
		if err := f.Chmod(o.Perm); err != nil {
			f.Close()
			os.Remove(tmp)
			return nil, err
		}
	}
	return &File{f: f, path: path, tmp: tmp, o: o}, nil
}

// TempPath is the temporary file's path.
func (f *File) TempPath() string { return f.tmp }

// Write writes to the temporary file.
func (f *File) Write(p []byte) (int, error) { return f.f.Write(p) }

// Abort closes and removes the temporary file; the target stays as it was.
func (f *File) Abort() {
	f.f.Close()
	os.Remove(f.tmp)
}

// Commit syncs (unless NoSync) and closes the temporary file and renames it
// over the target. On failure the temporary file is removed.
func (f *File) Commit() error {
	if !f.o.NoSync {
		if err := f.f.Sync(); err != nil {
			f.Abort()
			return err
		}
	}
	if err := f.f.Close(); err != nil {
		os.Remove(f.tmp)
		return err
	}
	if err := os.Rename(f.tmp, f.path); err != nil {
		os.Remove(f.tmp)
		return err
	}
	if f.o.NoSync {
		return nil
	}
	dir := filepath.Dir(f.path)
	if f.o.SyncDir != nil {
		return f.o.SyncDir(dir)
	}
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// Write writes b to path atomically, as o says.
func Write(path string, b []byte, o Options) error {
	f, err := Create(path, o)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Abort()
		return err
	}
	return f.Commit()
}

// createTemp creates the temporary file, never through a link or over a file
// already there.
func createTemp(dir, base string, o Options) (*os.File, string, error) {
	const flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	switch o.Temp {
	case Fixed, Suffix:
		tmp := filepath.Join(dir, "."+base+".tmp")
		if o.Temp == Suffix {
			tmp = filepath.Join(dir, base+".tmp")
		}
		os.Remove(tmp)
		f, err := os.OpenFile(tmp, flags, o.Perm)
		return f, tmp, err
	}
	for try := 0; ; try++ {
		var r [8]byte
		if _, err := rand.Read(r[:]); err != nil {
			return nil, "", err
		}
		tmp := filepath.Join(dir, "."+base+"."+hex.EncodeToString(r[:])+".tmp")
		f, err := os.OpenFile(tmp, flags, o.Perm)
		if err == nil || !errors.Is(err, os.ErrExist) || try == 9 {
			return f, tmp, err
		}
	}
}
