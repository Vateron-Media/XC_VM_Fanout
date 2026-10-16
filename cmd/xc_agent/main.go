// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Command xc_agent is an LB node's end of the XC_VM cluster API (MAIN ↔ LB
// plan). The implementation lives in internal/agentcmd so the merged xc_node
// binary can dispatch to it; this wrapper is the standalone xc_agent binary.
package main

import "github.com/Vateron-Media/XC_VM_Fanout/internal/agentcmd"

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() { agentcmd.Main(version) }
