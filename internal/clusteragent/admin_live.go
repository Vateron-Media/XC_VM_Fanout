package clusteragent

import (
	"context"
	"io"
	"net/http"
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
