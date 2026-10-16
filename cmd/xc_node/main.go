// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Command xc_node is the single load-balancer binary: the live fan-out daemon
// (internal/fanoutcmd) and the cluster agent (internal/agentcmd) in one static
// executable. It picks which to run by the name it is invoked under — so the
// panel's xc_fanout / xc_agent binaries, and the staged/trial/rollback copies
// it probes (.xc_fanout.new, .xc_agent.new, xc_agent.prev, xc_agent.trial),
// all dispatch correctly — or by an explicit leading subcommand:
//
//	xc_node fanout [flags]     the fan-out daemon (== xc_fanout …)
//	xc_node agent  [command]   the cluster agent  (== xc_agent …)
//	<name containing xc_fanout> …   the fan-out daemon (install name / symlink)
//	<name containing xc_agent>  …   the cluster agent  (install name / symlink)
//
// Dispatch is by substring, not exact match, because the panel's installer runs
// the binary under temp names (basename `.xc_fanout.new`, `.xc_agent.new`) to
// probe `-version` / `version` and to read the agent's `features:` line before
// it commits the install; an exact-name match would miss those and break the
// agent's reach-trial gating. A leading subcommand is stripped before the
// daemon or agent parses its own flags from os.Args, so the two merge with no
// change to their flags.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/agentcmd"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/fanoutcmd"
)

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

// route picks the command implied by the invocation name (its basename), or ""
// when the name selects neither. Substring, not exact match, so the installer's
// staged/trial/rollback copies (.xc_fanout.new, .xc_agent.new, xc_agent.prev,
// xc_agent.trial) dispatch like the plain xc_fanout / xc_agent names.
func route(base string) string {
	switch {
	case strings.Contains(base, "xc_fanout"):
		return "fanout"
	case strings.Contains(base, "xc_agent"):
		return "agent"
	}
	return ""
}

func main() {
	// 1) By the name the binary is called under.
	switch route(filepath.Base(os.Args[0])) {
	case "fanout":
		fanoutcmd.Main(version)
		return
	case "agent":
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
	fmt.Fprintln(os.Stderr, "xc_node: usage: xc_node <fanout|agent> …  (or invoke under a name containing xc_fanout / xc_agent)")
	os.Exit(2)
}
