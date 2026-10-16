// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Command xc_fanout is the native live-stream fan-out daemon (ADR 0002, P2–P3).
// The implementation lives in internal/fanoutcmd so the merged xc_node binary
// can dispatch to it; this wrapper is the standalone xc_fanout binary.
package main

import "github.com/Vateron-Media/XC_VM_Fanout/internal/fanoutcmd"

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() { fanoutcmd.Main(version) }
