// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package clusteragent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// errFanoutTransient is returned when the fanout answers 503 to a register —
// a concurrent teardown beat it (server.go's serveControl). It is not a hard
// failure, but note the stream re-registers only when its record next changes
// or the fanout restarts (boot-change re-seed); there is no periodic reconcile
// yet, so a 503 with the fanout up is not self-healed until then. A two-way
// reconcile (register every held proxy, clear everything else) belongs with the
// serve-not-defer slice, where a stale/missing registration becomes serving-
// visible rather than inert.
var errFanoutTransient = errors.New("clusteragent: fanout register: transient (concurrent teardown)")

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

// parseRuntimeDirective reads the `runtime` directive from a stored stream
// record — the `{etag, ver, data}` envelope the agent writes (checkStream), so
// the directive sits at `.data.runtime`. ok is false when the record carries no
// runtime directive (an older MAIN, or a record from before it was added).
func parseRuntimeDirective(recordJSON []byte) (runtimeDirective, bool) {
	var wrap struct {
		Data struct {
			Runtime *runtimeDirective `json:"runtime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recordJSON, &wrap); err != nil || wrap.Data.Runtime == nil {
		return runtimeDirective{}, false
	}
	return *wrap.Data.Runtime, true
}

// streamRegisterBody is the PUT /streams/<id> body — the fanout's streamConfig
// (internal/server, server.go). The agent builds it from a proxy directive and
// its own node ffmpeg path; Chunk 0 lets the fanout apply its configured
// default.
type streamRegisterBody struct {
	URLs   []string `json:"urls"`
	UA     string   `json:"ua"`
	Proxy  string   `json:"proxy"`
	Cookie string   `json:"cookie"`
	Ffmpeg string   `json:"ffmpeg"`
	Chunk  int      `json:"chunk"`
}

// registerBody turns a proxy directive into the fanout register body, filling
// the node-local ffmpeg path the directive leaves out (D3). ok is false unless
// this is a proxy directive with at least one non-empty url — so a transcode
// (kind "other") or an empty-source stream is skipped, never registered as a
// config the fanout would reject.
func (d runtimeDirective) registerBody(ffmpegBin string) (streamRegisterBody, bool) {
	if d.Kind != "proxy" || d.Source == nil {
		return streamRegisterBody{}, false
	}
	var urls []string
	for _, u := range d.Source.URLs {
		if u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		return streamRegisterBody{}, false
	}
	return streamRegisterBody{
		URLs:   urls,
		UA:     d.Source.UA,
		Proxy:  d.Source.Proxy,
		Cookie: d.Source.Cookie,
		Ffmpeg: ffmpegBin,
	}, true
}

// registerStream hands a restream's pull config to the fanout — PUT
// /streams/<id> on the control socket, what PHP's FanoutClient::register did.
// A no-op when this node has no control socket configured.
func (a *Agent) registerStream(ctx context.Context, id int64, b streamRegisterBody) error {
	if a.FanoutCtl == "" {
		return nil
	}
	body, err := json.Marshal(b)
	if err != nil {
		return err
	}
	return a.fanoutStreamCall(ctx, http.MethodPut, id, body)
}

// unregisterStream removes a stream's registration from the fanout (DELETE
// /streams/<id>), so a stream that was torn down or is no longer a proxy stops
// being served. A no-op when this node has no control socket configured.
func (a *Agent) unregisterStream(ctx context.Context, id int64) error {
	if a.FanoutCtl == "" {
		return nil
	}
	return a.fanoutStreamCall(ctx, http.MethodDelete, id, nil)
}

// seedFanoutRecord hands one stored record's runtime directive to the fanout
// when serve-only seeding is on: a proxy directive registers the puller,
// anything else (a transcode, or no directive) unregisters it so a stream that
// stopped being a proxy is cleared. No-op unless SeedFanout. Errors are logged
// by id and swallowed — a down or busy fanout must never fail the streams sync.
// A failed call is re-attempted only when the record next changes or the fanout
// restarts (boot-change re-seed), NOT on every sync — there is no periodic
// reconcile yet (see errFanoutTransient). Only the id is logged, never the
// record or its source (which carries credentials).
func (a *Agent) seedFanoutRecord(id int64, recordJSON []byte) {
	if !a.SeedFanout {
		return
	}
	if d, ok := parseRuntimeDirective(recordJSON); ok {
		if body, reg := d.registerBody(a.FfmpegBin); reg {
			if err := a.registerStream(context.Background(), id, body); err != nil {
				a.logf("cluster: fanout register stream %d: %v", id, err)
			}
			return
		}
	}
	a.unseedFanoutRecord(id)
}

// seedHeldProxies registers every held proxy stream with the fanout, for when
// the fanout (re)starts and its in-memory registry is empty: with no PHP to
// re-register per request on a serve-only LB, the agent re-seeds. It only
// registers proxies — a restarted fanout holds nothing to unregister — and
// reads records straight from the replica. No-op unless SeedFanout with a
// replica; errors logged by id and swallowed.
func (a *Agent) seedHeldProxies(ctx context.Context) {
	if !a.SeedFanout || a.ReplicaDir == "" {
		return
	}
	// Serialize with the streams sync: apply() runs under streamsMu and may be
	// removing a record (and DELETEing its registration) concurrently. Without
	// the lock a stale register here could overtake that DELETE and leave an
	// unassigned stream registered on the fanout. Held across the PUTs — they
	// are local control-socket calls, each already bounded by a 5s timeout, and
	// a fanout (re)start is rare.
	a.streamsMu.Lock()
	defer a.streamsMu.Unlock()
	for _, id := range heldStreams(a.ReplicaDir) {
		recordJSON, err := os.ReadFile(filepath.Join(streamsDir(a.ReplicaDir), strconv.FormatInt(id, 10)+".json"))
		if err != nil {
			continue
		}
		d, ok := parseRuntimeDirective(recordJSON)
		if !ok {
			continue
		}
		if body, reg := d.registerBody(a.FfmpegBin); reg {
			if err := a.registerStream(ctx, id, body); err != nil {
				a.logf("cluster: fanout re-seed stream %d: %v", id, err)
			}
		}
	}
}

// unseedFanoutRecord removes a stream's fanout registration — on a removal, or
// a stream that stopped being a proxy. No-op unless SeedFanout; errors logged
// by id and swallowed.
func (a *Agent) unseedFanoutRecord(id int64) {
	if !a.SeedFanout {
		return
	}
	if err := a.unregisterStream(context.Background(), id); err != nil {
		a.logf("cluster: fanout unregister stream %d: %v", id, err)
	}
}

func (a *Agent) fanoutStreamCall(ctx context.Context, method string, id int64, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://fanout/streams/"+strconv.FormatInt(id, 10), r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := fanoutClient(a.FanoutCtl).Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusNoContent, http.StatusOK:
		return nil
	case http.StatusServiceUnavailable:
		return errFanoutTransient
	default:
		return fmt.Errorf("clusteragent: fanout %s /streams/%d: HTTP %d", method, id, res.StatusCode)
	}
}
