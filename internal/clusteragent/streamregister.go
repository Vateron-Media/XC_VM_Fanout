// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package clusteragent

import "encoding/json"

// The runtime directive is what MAIN decides a serve-only LB should run for a
// stream, embedded in the stream's replicated record (XC_VM
// StreamRuntimeDirective / StreamRecords, spec "LB as a single Go binary"
// Phase 4a). The agent applies it verbatim and derives nothing: MAIN owns the
// restream-vs-transcode decision and how the source is built (D3).

// directiveSource is the node-agnostic pull config for a restream — MAIN's
// FanoutClient::buildSource minus the node-local ffmpeg path, which the agent
// fills from its own config.
type directiveSource struct {
	URLs   []string `json:"urls"`
	UA     string   `json:"ua"`
	Proxy  string   `json:"proxy"`
	Cookie string   `json:"cookie"`
}

// runtimeDirective is the record's `runtime` object. Kind is "proxy" for a
// direct-proxy restream (Source set) or "other" for a transcode/encode stream
// (Source nil, carried by Phase 4b).
type runtimeDirective struct {
	Kind   string           `json:"kind"`
	Source *directiveSource `json:"source"`
}

// parseRuntimeDirective reads the `runtime` directive from a stream record's
// signed `data`. ok is false when the record carries no runtime directive (an
// older MAIN, or a record from before the directive was added).
func parseRuntimeDirective(recordData []byte) (runtimeDirective, bool) {
	var wrap struct {
		Runtime *runtimeDirective `json:"runtime"`
	}
	if err := json.Unmarshal(recordData, &wrap); err != nil || wrap.Runtime == nil {
		return runtimeDirective{}, false
	}
	return *wrap.Runtime, true
}
