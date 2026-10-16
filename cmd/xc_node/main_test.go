// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import "testing"

// The installer runs the binary under staged/trial/rollback names to probe it,
// so those must dispatch like the plain names; a bare xc_node name selects
// neither (it falls through to the explicit subcommand).
func TestRoute(t *testing.T) {
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
