package clusteragent

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// admin_live.go ports Public/admin/live.php: the cross-node live relay source.
// A relay/uitoken/password-admitted request (slice-1 admitAdmin) is served from
// the in-process fanout Manager over the control socket — continuous TS by
// proxying GET /live/<id> (the Manager follows the ring's GOPs itself; we omit
// the viewer uuid so no viewer connection or limit is recorded), and admin-HLS
// by rewriting GET /hls/<id>/index.m3u8 with a named segment proxied from
// /hls/<id>/<seq>.ts. This substitutes the Go-native in-memory live/HLS surface
// for live.php's disk <id>_N.ts follow loop (same bytes, same framing).

// adminHLSWait/Poll bound the wait for the fanout's live HLS playlist to have
// segments (the gateway's waitHLSPlaylist analog). Vars so tests can shrink them.
var (
	adminHLSWait = 2 * time.Second
	adminHLSPoll = 50 * time.Millisecond
)

// fanoutRequest issues method+path on the fanout control socket (http://fanout).
func (a *Agent) fanoutRequest(ctx context.Context, method, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://fanout"+path, nil)
	if err != nil {
		return nil, err
	}
	return fanoutClient(a.FanoutCtl).Do(req)
}

// adminLiveReady is live.php's readiness gate (lines 107–178) over the fanout
// control socket: the stream on air with data (ts) or a playlist with segments
// (hls), after an on-demand start when it is down. A stream the fanout does not
// know → false (live.php's null StreamSource row). A down, NOT on-demand stream
// → false (never force-started). A down on-demand stream is ensured
// (Manager.EnsureRunning blocks until onAir or its timeout), then re-checked.
func (a *Agent) adminLiveReady(ctx context.Context, id string, wantHLS bool) bool {
	running, hasData, ok := a.fanoutStreamStatus(ctx, id)
	if !ok {
		return false
	}
	if !running {
		if !a.streamOnDemand(id) {
			return false
		}
		a.fanoutEnsure(ctx, id)
		if running, hasData, ok = a.fanoutStreamStatus(ctx, id); !ok || !running {
			return false
		}
	}
	if wantHLS {
		_, pok := a.adminHLSPlaylist(ctx, id)
		return pok
	}
	return hasData
}

// adminHLSPlaylist fetches the fanout's live HLS playlist for id over the control
// socket, waiting (bounded) for it to exist with at least one segment. false
// until ready. It is both the hls readiness check and the source the admin-HLS
// rewrite (serveAdminLive) builds on.
func (a *Agent) adminHLSPlaylist(ctx context.Context, id string) ([]byte, bool) {
	deadline := time.Now().Add(adminHLSWait)
	for {
		if body, ok := a.fanoutPlaylistOnce(ctx, id); ok {
			return body, true
		}
		if !time.Now().Before(deadline) {
			return nil, false
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(adminHLSPoll):
		}
	}
}

func (a *Agent) fanoutPlaylistOnce(ctx context.Context, id string) ([]byte, bool) {
	resp, err := a.fanoutRequest(ctx, http.MethodGet, "/hls/"+id+"/index.m3u8")
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || !hlsHasSegment(body) {
		return nil, false
	}
	return body, true
}

// hlsHasSegment reports whether a playlist carries at least one media segment.
func hlsHasSegment(playlist []byte) bool {
	for _, line := range strings.Split(string(playlist), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") && strings.HasSuffix(line, ".ts") {
			return true
		}
	}
	return false
}

// serveAdminLive is the /admin/live dispatcher (live.php lines 37–342): admit
// (slice-1 admitAdmin, password allowed), force m3u8 for a uitoken, refuse a
// relay pulling m3u8, require the stream's replica record, run the readiness
// gate (answering odstart as bare "1"/"0"), then serve the TS relay or the
// admin HLS.
func (a *Agent) serveAdminLive(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	adm, ok := a.admitAdmin(parsePositiveInt(q.Get("stream")), r, r.Header.Get("X-XC-Client-IP"), true, a.Client.MainNowMs())
	if !ok {
		http.NotFound(w, r)
		return
	}
	ext := q.Get("extension")
	if adm.kind == admitUitoken {
		ext = "m3u8" // the panel's uitoken path always serves the playlist
	}
	if adm.kind == admitRelayKind && ext == "m3u8" {
		http.NotFound(w, r) // a relay pulls TS only; the playlist's segment URLs carry the password
		return
	}
	id := strconv.FormatInt(adm.streamID, 10)
	if _, ok := a.streamMeta(id); !ok {
		http.NotFound(w, r) // StreamSource row required
		return
	}
	wantHLS := ext == "m3u8"
	ready := a.adminLiveReady(r.Context(), id, wantHLS)
	if q.Has("odstart") {
		if ready {
			_, _ = io.WriteString(w, "1")
		} else {
			_, _ = io.WriteString(w, "0")
		}
		return
	}
	if !ready {
		http.NotFound(w, r)
		return
	}
	if wantHLS {
		a.serveAdminLiveHLS(w, r, adm, id)
		return
	}
	a.serveAdminLiveTS(w, r, adm, id)
}

// serveAdminLiveTS proxies the Manager's continuous live TS (GET /live/<id>) to
// the admitted caller — live.php's default (ts) arm. The viewer uuid (c) is
// omitted so the Manager records no connection and spools no limit: admin is a
// source read, never a counted viewer. A RELAY admission carrying ?rk= gets the
// body AES-256-GCM-framed (streamRelaySealed) under X-XCVM-Relay-Seal: v1.
func (a *Agent) serveAdminLiveTS(w http.ResponseWriter, r *http.Request, adm adminAdmission, id string) {
	// prebuffer: requested (query flag or X-XC_VM-PREBUFFER header) → the
	// Manager's default join burst (param omitted); else prebuffer=0 (current
	// GOP) — faithful to live.php's "prebuffer present → seg_time burst, else 0".
	path := "/live/" + id
	if !adminPrebufferRequested(r) {
		path += "?prebuffer=0"
	}
	resp, err := a.fanoutRequest(r.Context(), http.MethodGet, path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		http.NotFound(w, r)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "video/mp2t")
	h.Set("X-Accel-Buffering", "no")
	if adm.kind == admitRelayKind {
		if rk := r.URL.Query().Get(relaySealParam); rk != "" {
			key, err := a.openLiveSealKey(rk, adm.streamID)
			if err != nil {
				http.NotFound(w, r)
				return
			}
			h.Set(relaySealHeader, relaySealVersion)
			w.WriteHeader(http.StatusOK)
			_ = streamRelaySealed(w, resp.Body, key)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	flushCopy(w, resp.Body)
}

// adminPrebufferRequested is live.php's prebuffer trigger: the prebuffer query
// param or an X-XC_VM-PREBUFFER request header.
func adminPrebufferRequested(r *http.Request) bool {
	return r.URL.Query().Has("prebuffer") || r.Header.Get("X-XC_VM-PREBUFFER") != ""
}

// serveAdminLiveHLS is live.php's m3u8 arm (uitoken/password): with no segment,
// the fanout's live playlist rewritten so each segment URL points back at
// /admin/live (carrying the uitoken, or the stream+password); with a segment,
// that one HLS sequence proxied from the Manager (GET /hls/<id>/<seq>.ts). The
// segment value is an integer sequence only — anything else is refused, so no
// path but a numeric seq ever reaches the fanout.
func (a *Agent) serveAdminLiveHLS(w http.ResponseWriter, r *http.Request, adm adminAdmission, id string) {
	q := r.URL.Query()
	if seg := q.Get("segment"); seg != "" {
		if !isAllDigits(seg) {
			http.NotFound(w, r)
			return
		}
		resp, err := a.fanoutRequest(r.Context(), http.MethodGet, "/hls/"+id+"/"+seg+".ts")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		flushCopy(w, resp.Body)
		return
	}
	pl, ok := a.adminHLSPlaylist(r.Context(), id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	var uitoken, password string
	if adm.kind == admitUitoken {
		uitoken = q.Get("uitoken")
	} else {
		password = a.secret("live_streaming_pass")
	}
	out := rewriteAdminHLS(pl, adm.streamID, uitoken, password)
	h := w.Header()
	h.Set("Content-Type", "application/vnd.apple.mpegurl")
	h.Set("Content-Length", strconv.Itoa(len(out)))
	_, _ = w.Write(out)
}

// rewriteAdminHLS is StreamUtils::generateAdminHLS: each media segment line
// (<seq>.ts) becomes a /admin/live URL carrying the uitoken, or the stream +
// password (the slice-2 timeshift m3u8 shape); every #EXT tag passes through.
func rewriteAdminHLS(playlist []byte, id int64, uitoken, password string) []byte {
	lines := strings.Split(string(playlist), "\n")
	for i, line := range lines {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "#") || !strings.HasSuffix(t, ".ts") {
			continue
		}
		seq := strings.TrimSuffix(t, ".ts")
		if uitoken != "" {
			lines[i] = "/admin/live?extension=m3u8&segment=" + seq + "&uitoken=" + uitoken
		} else {
			lines[i] = "/admin/live?extension=m3u8&stream=" + strconv.FormatInt(id, 10) + "&segment=" + seq + "&password=" + password
		}
	}
	return []byte(strings.Join(lines, "\n"))
}

// isAllDigits reports whether s is a non-empty run of ASCII digits (an HLS seq).
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
