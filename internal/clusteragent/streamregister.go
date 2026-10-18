// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package clusteragent

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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
	// Spec is the portable supervisor.Spec for a kind:"transcode" directive
	// (4b): a JSON object whose node-local values are placeholders the agent
	// resolves. Stored raw and forwarded to /monitor/ after substitution.
	Spec json.RawMessage `json:"spec"`
}

// transcodeSpec returns the raw portable Spec of a transcode directive, or
// ok=false when this is not a transcode directive or carries no spec.
func (d runtimeDirective) transcodeSpec() (json.RawMessage, bool) {
	if d.Kind != "transcode" || len(d.Spec) == 0 {
		return nil, false
	}
	return d.Spec, true
}

// agentNode is this node's resolved values for the transcode-spec placeholders
// — its paths, ffmpeg binaries and server id, from the agent's config.
type agentNode struct {
	streams, logs, serverID, ffmpegCpu, ffmpegGpu, ffprobe, binary string
}

// SetNodePaths configures the transcode-spec placeholder values from the
// install's node layout (called by agentcmd after FfmpegBin is set, which
// doubles as the {ffmpeg_cpu} value). Leaving any empty makes a transcode
// directive that needs that placeholder skip — fail closed.
func (a *Agent) SetNodePaths(streams, logs, serverID, ffmpegGPU, ffprobe, binary string) {
	a.node = agentNode{
		streams: streams, logs: logs, serverID: serverID,
		ffmpegCpu: a.FfmpegBin, ffmpegGpu: ffmpegGPU, ffprobe: ffprobe, binary: binary,
	}
}

// residualToken matches a leftover placeholder after substitution. It excludes
// `%`-prefixed forms so ffmpeg's own `%{...}` expansion syntax (a drawtext
// `%{localtime}` overlay, say) is not misread as an unresolved token — which,
// being fail-closed, would silently keep the transcode off air.
var residualToken = regexp.MustCompile(`(?:^|[^%])(\{[a-z_]+\})`)

// resolveSpec substitutes the placeholders in a portable transcode spec (4b-i)
// with this node's values and the ingest socket. It fails closed: a placeholder
// present in the spec whose value is empty (incomplete node config), or any
// unknown token left over, is an error — a half-resolved spec must never reach
// the supervisor. The HLS key/IV are NOT spec placeholders: the fanout (not
// ffmpeg) encrypts, so the agent registers them via /ingest/ (setEnc), and the
// spec's `encrypt` flag — not a token — says whether to.
func resolveSpec(spec json.RawMessage, n agentNode, ingestSock string) ([]byte, error) {
	s := string(spec)
	for _, kv := range [][2]string{
		{"{ingest_sock}", ingestSock}, {"{streams}", n.streams}, {"{logs}", n.logs},
		{"{server_id}", n.serverID}, {"{ffmpeg_cpu}", n.ffmpegCpu}, {"{ffmpeg_gpu}", n.ffmpegGpu},
		{"{ffprobe}", n.ffprobe}, {"{binary}", n.binary},
	} {
		if strings.Contains(s, kv[0]) && kv[1] == "" {
			return nil, fmt.Errorf("clusteragent: transcode spec needs %s but this node has no value for it", kv[0])
		}
		s = strings.ReplaceAll(s, kv[0], kv[1])
	}
	if m := residualToken.FindStringSubmatch(s); m != nil {
		return nil, fmt.Errorf("clusteragent: transcode spec has an unknown placeholder %s", m[1])
	}
	return []byte(s), nil
}

// randHex16 is a fresh 16-byte value, hex-encoded — an HLS key or IV.
func randHex16() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// registerIngest registers an ingest socket for the stream with the local
// fanout (PUT /ingest/<id>, which also sets the HLS key/IV) and returns the
// socket path — the encoder's tee target, resolving {ingest_sock}.
func (a *Agent) registerIngest(ctx context.Context, id int64, keyHex, ivHex string) (string, error) {
	body, _ := json.Marshal(map[string]string{"key": keyHex, "iv": ivHex})
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://fanout/ingest/"+strconv.FormatInt(id, 10), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := fanoutClient(a.FanoutCtl).Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("clusteragent: fanout PUT /ingest/%d: HTTP %d", id, res.StatusCode)
	}
	var out struct {
		Socket string `json:"socket"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil || out.Socket == "" {
		return "", fmt.Errorf("clusteragent: fanout /ingest/%d returned no socket", id)
	}
	return out.Socket, nil
}

// superviseStream hands a transcode stream to the fanout's supervisor: register
// an ingest socket (with a fresh key/IV when the spec is encrypted), resolve the
// spec's placeholders, and PUT /monitor/<id>. Fails closed — an unresolved
// placeholder never reaches the supervisor. No-op when FanoutCtl is unset.
func (a *Agent) superviseStream(ctx context.Context, id int64, spec json.RawMessage) error {
	if a.FanoutCtl == "" {
		return nil
	}
	// The spec's `encrypt` flag (not a token) says whether this is encrypted HLS:
	// generate a fresh key/IV and register them with the ingest (the fanout, not
	// ffmpeg, encrypts). The key/IV never appear in the spec itself.
	var enc struct {
		Encrypt bool `json:"encrypt"`
	}
	_ = json.Unmarshal(spec, &enc)
	var keyHex, ivHex string
	if enc.Encrypt {
		var err error
		if keyHex, err = randHex16(); err != nil {
			return err
		}
		if ivHex, err = randHex16(); err != nil {
			return err
		}
	}
	sock, err := a.registerIngest(ctx, id, keyHex, ivHex)
	if err != nil {
		return err
	}
	resolved, err := resolveSpec(spec, a.node, sock)
	if err != nil {
		return err // fail closed: never hand a half-resolved spec to the supervisor
	}
	return a.fanoutMonitorCall(ctx, http.MethodPut, id, resolved)
}

// unsuperviseStream stops the supervisor for a stream (DELETE /monitor/<id>) —
// on a removal or a flip away from transcode. No-op when FanoutCtl is unset.
func (a *Agent) unsuperviseStream(ctx context.Context, id int64) error {
	if a.FanoutCtl == "" {
		return nil
	}
	return a.fanoutMonitorCall(ctx, http.MethodDelete, id, nil)
}

func (a *Agent) fanoutMonitorCall(ctx context.Context, method string, id int64, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://fanout/monitor/"+strconv.FormatInt(id, 10), r)
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
		return fmt.Errorf("clusteragent: fanout %s /monitor/%d: HTTP %d", method, id, res.StatusCode)
	}
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
// when serve-only seeding is on, routing by kind: a proxy directive registers
// the puller (and releases a supervisor it had from a prior transcode); a
// transcode directive supervises the encoder (applyTranscode); anything else
// (or no directive) clears both. No-op unless SeedFanout. Errors are logged by
// id and swallowed — a down or busy fanout must never fail the streams sync. A
// failed call is re-attempted only when the record next changes or the fanout
// restarts (boot-change re-seed), NOT on every sync — there is no periodic
// reconcile yet (see errFanoutTransient). Only the id is logged, never the
// record or its source/spec (which carry credentials). A flip proxy->transcode
// leaves the dormant puller registration in place (the supervised stream is
// ingest-fed); clearing it safely is the serve path's concern (4b-iii).
func (a *Agent) seedFanoutRecord(id int64, recordJSON []byte) {
	if !a.SeedFanout {
		return
	}
	if d, ok := parseRuntimeDirective(recordJSON); ok {
		if body, reg := d.registerBody(a.FfmpegBin); reg {
			a.releaseSupervision(id) // a flip transcode->proxy: stop the encoder
			if err := a.registerStream(context.Background(), id, body); err != nil {
				a.logf("cluster: fanout register stream %d: %v", id, err)
			}
			return
		}
		if spec, ok := d.transcodeSpec(); ok {
			a.applyTranscode(context.Background(), id, spec)
			return
		}
		if d.Kind == "loopback" {
			a.registerLoopback(id)
			return
		}
	}
	a.unseedFanoutRecord(id)
}

// loopbackBody is the puller registration for a loopback stream: its node-local
// relay URL (http://<RelayProxyAddr>/relay/<k>/<id>.ts), composed from the agent's
// own relay key. The puller fetches it as a plain mp2t source and the relay proxy
// signs the upstream connect to the parent (resolved per stream from the record)
// with MAIN's relay ticket — so no relay secret is ever shipped in the directive;
// MAIN cannot compose this URL because the key k is node-local. ok=false (fail
// closed) when the relay proxy is off (no RelayKeyDir) or the key cannot be
// loaded, so the stream is left unregistered rather than pulling a broken URL.
// Silent: callers decide whether a miss is logged (register) or skipped quietly
// (the 2-minute reconcile). The single composer every loopback call site shares,
// so registration, re-seed and reconcile agree on the exact URL.
func (a *Agent) loopbackBody(id int64) (streamRegisterBody, bool) {
	if a.RelayKeyDir == "" {
		return streamRegisterBody{}, false
	}
	k, err := LoadRelayKey(a.RelayKeyDir)
	if err != nil || k == "" {
		return streamRegisterBody{}, false
	}
	return streamRegisterBody{URLs: []string{"http://" + RelayProxyAddr + "/relay/" + k + "/" + strconv.FormatInt(id, 10) + ".ts"}}, true
}

// registerLoopback registers a loopback stream's relay URL with the puller, or
// fails closed (unregister) when the relay proxy is off or the key is
// unavailable, so the daemon is never left pulling a broken URL.
func (a *Agent) registerLoopback(id int64) {
	body, ok := a.loopbackBody(id)
	if !ok {
		a.logf("cluster: loopback stream %d: relay proxy off or key unavailable, not registering", id)
		a.unseedFanoutRecord(id)
		return
	}
	a.releaseSupervision(id) // a flip transcode->loopback: stop the encoder
	if err := a.registerStream(context.Background(), id, body); err != nil {
		a.logf("cluster: fanout register loopback %d: %v", id, err)
	}
}

// applyTranscode hands a transcode stream to the supervisor, but only when its
// portable spec is new or changed: PUT /monitor/ restarts the encoder, so an
// unchanged re-seed (a resync) must be a no-op. streamsMu is held by the caller.
func (a *Agent) applyTranscode(ctx context.Context, id int64, spec json.RawMessage) {
	if bytes.Equal(a.supervisedSpec[id], spec) {
		return
	}
	if err := a.superviseStream(ctx, id, spec); err != nil {
		a.logf("cluster: fanout supervise stream %d: %v", id, err)
		return // unmarked, so the next change or resync retries
	}
	if a.supervisedSpec == nil {
		a.supervisedSpec = map[int64][]byte{}
	}
	a.supervisedSpec[id] = append([]byte(nil), spec...)
}

// releaseSupervision stops the supervisor for a stream the agent had supervised
// (a flip away from transcode, or a removal) and forgets it. No-op if it was
// never supervised. streamsMu is held by the caller.
func (a *Agent) releaseSupervision(id int64) {
	if _, ok := a.supervisedSpec[id]; !ok {
		return
	}
	if err := a.unsuperviseStream(context.Background(), id); err != nil {
		a.logf("cluster: fanout release stream %d: %v", id, err)
		return // keep the entry so a later seed retries the release
	}
	delete(a.supervisedSpec, id)
}

// seedHeldProxies re-seeds every held stream with the fanout, for when the
// fanout (re)starts and its in-memory registry is empty: with no PHP to
// re-register per request on a serve-only LB, the agent re-seeds. It registers
// proxies (PUT /streams/) and re-supervises transcodes (PUT /monitor/, which
// lets the supervisor adopt an encoder that outlived the daemon — adopt.go). It
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
	// The fanout forgot every stream, so forget what we thought was supervised
	// and re-apply all: a transcode re-PUT makes the supervisor adopt the encoder
	// that outlived the daemon rather than leaving it unwatched.
	a.supervisedSpec = nil
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
			continue
		}
		if spec, ok := d.transcodeSpec(); ok {
			a.applyTranscode(ctx, id, spec)
			continue
		}
		if d.Kind == "loopback" {
			if body, ok := a.loopbackBody(id); ok {
				if err := a.registerStream(ctx, id, body); err != nil {
					a.logf("cluster: fanout re-seed loopback %d: %v", id, err)
				}
			}
		}
	}
}

// fanoutRegisteredIDs is the ids the fanout currently holds a pull config for
// (GET /streams). Used by the reconcile to clear stale registrations.
func (a *Agent) fanoutRegisteredIDs(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://fanout/streams", nil)
	if err != nil {
		return nil, err
	}
	res, err := fanoutClient(a.FanoutCtl).Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clusteragent: fanout GET /streams: HTTP %d", res.StatusCode)
	}
	var out struct {
		Streams []string `json:"streams"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Streams, nil
}

// reconcileFanout converges the fanout's registered set to exactly the held
// proxies: it registers every held proxy the fanout lacks and unregisters every
// id the fanout holds that is no longer a held proxy, leaving matches alone.
// This closes the gap that pure event-driven seeding leaves — a failed
// register/unregister, or a bulk drain, that no later record change would
// retry. No-op unless SeedFanout with a control socket; holds streamsMu (like
// seedHeldProxies) so it cannot interleave with apply; errors logged by id.
func (a *Agent) reconcileFanout(ctx context.Context) {
	if !a.SeedFanout || a.FanoutCtl == "" || a.ReplicaDir == "" {
		return
	}
	a.streamsMu.Lock()
	defer a.streamsMu.Unlock()

	want := map[int64]streamRegisterBody{}
	for _, id := range heldStreams(a.ReplicaDir) {
		recordJSON, err := os.ReadFile(filepath.Join(streamsDir(a.ReplicaDir), strconv.FormatInt(id, 10)+".json"))
		if err != nil {
			continue
		}
		if d, ok := parseRuntimeDirective(recordJSON); ok {
			if body, reg := d.registerBody(a.FfmpegBin); reg {
				want[id] = body
			} else if d.Kind == "loopback" {
				// A loopback is a /streams puller registration too, so the reconcile
				// must keep it (not tear it down as a non-proxy). Fail-closed when the
				// key is unavailable → left out of want → unregistered, as intended.
				if body, ok := a.loopbackBody(id); ok {
					want[id] = body
				}
			}
		}
	}

	have, err := a.fanoutRegisteredIDs(ctx)
	if err != nil {
		a.logf("cluster: fanout reconcile: list: %v", err)
		return
	}
	haveSet := map[int64]bool{}
	for _, s := range have {
		if id, e := strconv.ParseInt(s, 10, 64); e == nil {
			haveSet[id] = true
		}
	}

	for id, body := range want {
		if !haveSet[id] {
			if err := a.registerStream(ctx, id, body); err != nil {
				a.logf("cluster: fanout reconcile: register %d: %v", id, err)
			}
		}
	}
	for id := range haveSet {
		if _, ok := want[id]; !ok {
			if err := a.unregisterStream(ctx, id); err != nil {
				a.logf("cluster: fanout reconcile: unregister %d: %v", id, err)
			}
		}
	}
}

// unseedFanoutRecord removes a stream's fanout registration — on a removal, or
// a stream that stopped being a proxy/transcode. Releases the supervisor first
// (if it was a transcode), then unregisters the puller. No-op unless SeedFanout;
// errors logged by id and swallowed.
func (a *Agent) unseedFanoutRecord(id int64) {
	if !a.SeedFanout {
		return
	}
	a.releaseSupervision(id) // stop the encoder if this stream was a transcode
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
