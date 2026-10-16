// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout

package main

import "testing"

// The panel installs the merged binary under each tool's name and probes it
// under staged/trial copies; every such name must dispatch to the right
// command, or the agent's reach-trial gating (run under .xc_agent.new) breaks.
func TestRouteByInvocationName(t *testing.T) {
	cases := map[string]string{
		"xc_fanout":      "fanout",
		"xc_agent":       "agent",
		".xc_fanout.new": "fanout",
		".xc_agent.new":  "agent",
		"xc_agent.prev":  "agent",
		"xc_agent.trial": "agent",
		"xc_node":        "",
		"something-else": "",
	}
	for name, want := range cases {
		if got := route(name); got != want {
			t.Errorf("route(%q) = %q, want %q", name, got, want)
		}
	}
}
