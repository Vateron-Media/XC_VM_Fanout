package clusteragent

import (
	"os"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/atomicfile"
)

// fileWrite says how writeFile puts a file in place (internal/atomicfile):
// through a dot-named temporary file in the target's directory (the readers
// skip dot files), created exclusively and renamed in.
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
	// its directory after.
	noSync bool
	// mustSyncDir makes the directory's sync (syncDir) part of the write:
	// its failure is returned, the file renamed in. Otherwise that sync is
	// best effort.
	mustSyncDir bool
}

// writeFile writes b to path atomically, as w says.
func writeFile(path string, b []byte, w fileWrite) error {
	o := atomicfile.Options{Perm: w.perm, Exact: w.exact, NoSync: w.noSync}
	if w.fixedTemp {
		o.Temp = atomicfile.Fixed
	}
	if w.mustSyncDir {
		o.SyncDir = syncDir
	}
	return atomicfile.Write(path, b, o)
}
