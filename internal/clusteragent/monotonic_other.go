//go:build !linux

package clusteragent

import "time"

var bootTime = time.Now()

// monotonicNs stands in for CLOCK_MONOTONIC off Linux (development only).
func monotonicNs() int64 { return int64(time.Since(bootTime)) }
