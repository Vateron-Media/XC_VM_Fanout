//go:build linux

package clusteragent

import (
	"syscall"
	"unsafe"
)

// monotonicNs is CLOCK_MONOTONIC in nanoseconds, the clock PHP's hrtime()
// reads: spool files from PHP and from the agent sort by it together.
func monotonicNs() int64 {
	var ts syscall.Timespec
	if _, _, e := syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, 1, uintptr(unsafe.Pointer(&ts)), 0); e != 0 {
		return 0
	}
	return ts.Nano()
}
