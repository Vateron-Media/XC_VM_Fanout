// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Command xc_node is the single load-balancer binary: the live fan-out daemon
// (internal/fanoutcmd) and the cluster agent (internal/agentcmd) in one static
// executable. It picks which to run by the name it is invoked under — so the
// symlinks xc_fanout and xc_agent keep working for PHP's process-name checks
// and run.sh — or by an explicit leading subcommand:
//
//	xc_node fanout [flags]     the fan-out daemon (== xc_fanout …)
//	xc_node agent  [command]   the cluster agent  (== xc_agent …)
//	xc_fanout …                (symlink) the fan-out daemon
//	xc_agent …                 (symlink) the cluster agent
//
// A leading subcommand is stripped before the daemon or agent parses its own
// flags from os.Args, so the two programs merge with no change to their flags.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/agentcmd"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/fanoutcmd"
)

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	// 1) By the name the binary is called under (xc_fanout / xc_agent symlinks).
	switch filepath.Base(os.Args[0]) {
	case "xc_fanout":
		fanoutcmd.Main(version)
		return
	case "xc_agent":
		agentcmd.Main(version)
		return
	}
	// 2) By an explicit leading subcommand; strip it so the inner program sees
	//    its own flags at os.Args[1:].
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "fanout":
			os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
			fanoutcmd.Main(version)
			return
		case "agent":
			os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
			agentcmd.Main(version)
			return
		case "version", "-version", "--version":
			fmt.Println(version)
			return
		}
	}
	fmt.Fprintln(os.Stderr, "xc_node: usage: xc_node <fanout|agent> …  (or invoke as the symlink xc_fanout / xc_agent)")
	os.Exit(2)
}
