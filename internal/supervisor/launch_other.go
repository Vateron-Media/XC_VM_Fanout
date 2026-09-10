//go:build !unix

package supervisor

import (
	"context"
	"errors"
)

// shellLauncher is Unix-only: supervising an encoder needs process groups and
// signals (see launch_unix.go), and the daemon ships as a static Linux binary.
// This stub exists so the package still builds — and its tests, which supply
// their own in-process Launcher, still run — on a developer's non-Unix machine.
func shellLauncher(context.Context, string, string) (Process, error) {
	return nil, errors.New("supervisor: launching encoders is only supported on unix")
}
