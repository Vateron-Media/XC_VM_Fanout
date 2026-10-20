package clusteragent

import (
	"crypto/hmac"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
)

// serveRtmpNotify answers nginx_rtmp's /stream/rtmp auth-notify (GET; 200 =
// admit, 404 = deny), the Go port of rtmp.php's LB arm. The front routes the
// public callback here and loopback-guards the real peer; this handler trusts
// its socket caller (the agent's unix-socket peer is the front, not the client).
func (a *Agent) serveRtmpNotify(w http.ResponseWriter, r *http.Request) {
	n, ok := parseRtmpNotify(r.URL.RawQuery)
	if !ok {
		a.rtmpDeny(w, "parse")
		return
	}
	// The panel's own ffmpeg FLV output publishing into nginx_rtmp is admitted
	// without auth (addr 127.0.0.1 + publish).
	if n.Addr == "127.0.0.1" && n.Call == "publish" {
		a.rtmpMeter("publish_loopback")
		w.WriteHeader(http.StatusOK)
		return
	}
	switch {
	case n.Call == "play_done":
		a.closeRtmpRecord(n.ClientID)
		a.rtmpMeter("play_done")
		w.WriteHeader(http.StatusOK)
	case n.Call == "publish":
		pw := r.URL.Query().Get("password")
		if a.passMatches(a.secret("live_streaming_pass"), pw) || a.allowRTMP(n.Addr, "push", pw) {
			a.rtmpMeter("publish")
			w.WriteHeader(http.StatusOK)
			return
		}
		a.rtmpDeny(w, "publish gate")
	default: // play (viewer)
		a.rtmpPlay(w, r, n)
	}
}

func (a *Agent) rtmpPlay(w http.ResponseWriter, r *http.Request, n *rtmpNotify) {
	q := r.URL.Query()
	// Relay bypass: the stream password, or the allowed-RTMP pull list (an IP in
	// the list with pull=1 and its per-entry password, rtmp.php:69). A relay
	// carries no line and opens no record.
	if a.passMatches(a.secret("live_streaming_pass"), q.Get("password")) || a.allowRTMP(n.Addr, "pull", q.Get("password")) {
		if running, _, held := a.fanoutStreamStatus(r.Context(), n.Name); held && !running {
			a.fanoutEnsure(r.Context(), n.Name)
		}
		a.rtmpMeter("play_relay")
		w.WriteHeader(http.StatusOK)
		return
	}
	// A line: tcurl + app are required, and a credential may not be an array.
	if q.Get("tcurl") == "" || q.Get("app") == "" {
		a.rtmpDeny(w, "no tcurl/app")
		return
	}
	creds, ok := rtmpCreds(q)
	if !ok {
		a.rtmpDeny(w, "cred array")
		return
	}
	streamID, _ := strconv.ParseInt(n.Name, 10, 64)
	uuid := rtmpUUID(a.Client.State.ServerID, n.ClientID)
	reply := a.rtmpAsk(r.Context(), streamID, n.Addr, rtmpRestreamDetect(r), uuid, creds)
	if !reply.OK {
		if reply.Reason == "AUTH_FAILED" {
			a.rtmpFlood(n.Addr)
		}
		a.rtmpDeny(w, "line "+reply.Reason)
		return
	}
	// The stream and this node's row: the fanout's liveness stands in for
	// StreamSource::joined. A line viewer of a stream this node does not hold is
	// refused; an idle held stream is brought up on demand.
	running, _, held := a.fanoutStreamStatus(r.Context(), n.Name)
	if !held {
		a.rtmpDeny(w, "not held")
		return
	}
	if !running {
		a.fanoutEnsure(r.Context(), n.Name)
	}
	if a.openRtmpRecord(r.Context(), n, reply, streamID) {
		a.rtmpMeter("play")
		w.WriteHeader(http.StatusOK)
		return
	}
	a.rtmpDeny(w, "record")
}

// rtmpCreds builds the line credentials from the query (token, or
// username+password), refusing (ok=false) when a credential is sent more than
// once — the port of rtmp.php's array-credential refusal (stricter than PHP's
// take-last on a plain duplicate, which nginx_rtmp never sends).
func rtmpCreds(q url.Values) (map[string]string, bool) {
	if _, has := q["token"]; has {
		if len(q["token"]) > 1 {
			return nil, false
		}
		return map[string]string{"token": q.Get("token")}, true
	}
	if len(q["username"]) > 1 || len(q["password"]) > 1 {
		return nil, false
	}
	return map[string]string{"username": q.Get("username"), "password": q.Get("password")}, true
}

// rtmpFloodMax caps the per-IP failed-auth counter map so it cannot grow
// without bound (the counter is best-effort observability; the ban is deferred).
const rtmpFloodMax = 10000

func rtmpRestreamDetect(r *http.Request) bool {
	// rtmp.php keys on the header's presence, any value.
	return len(r.Header.Values("X-XC_VM-DETECT")) > 0
}

// passMatches ports ViewerKey::passMatches for the LB: a constant-time compare
// of the given password against the known stream password. An empty given never
// matches; an empty known (no passHash fallback on an LB) never matches.
func (a *Agent) passMatches(known, given string) bool {
	if given == "" || known == "" {
		return false
	}
	return hmac.Equal([]byte(known), []byte(given))
}

// secret reads a value from the replica's secrets section (secrets.json, 0600),
// e.g. live_streaming_pass; "" when absent.
func (a *Agent) secret(key string) string {
	if a.ReplicaDir == "" {
		return ""
	}
	b, err := os.ReadFile(filepath.Join(a.ReplicaDir, "secrets.json"))
	if err != nil {
		return ""
	}
	var doc struct {
		Data map[string]any `json:"data"`
	}
	if json.Unmarshal(b, &doc) != nil {
		return ""
	}
	s, _ := doc.Data[key].(string)
	return s
}

// rtmpDeny logs the reason, meters it, and answers 404 (nginx_rtmp's deny). The
// real reason stays in the agent log; the wire is only 200/404 (improvement #1).
func (a *Agent) rtmpDeny(w http.ResponseWriter, reason string) {
	a.rtmpMeter("deny:" + reason)
	if a.Logf != nil {
		a.Logf("rtmp: deny (%s)", reason)
	}
	w.WriteHeader(http.StatusNotFound)
}

func (a *Agent) rtmpMeter(label string) {
	a.rtmpStatsMu.Lock()
	if a.rtmpStats == nil {
		a.rtmpStats = map[string]uint64{}
	}
	a.rtmpStats[label]++
	a.rtmpStatsMu.Unlock()
}

// rtmpFlood ticks a per-IP failed-auth counter (the panel's BruteforceGuard
// records the failure; the file-based ban is deferred — see the design).
func (a *Agent) rtmpFlood(ip string) {
	a.rtmpFloodMu.Lock()
	if a.rtmpFloodN == nil {
		a.rtmpFloodN = map[string]int{}
	}
	if _, seen := a.rtmpFloodN[ip]; !seen && len(a.rtmpFloodN) >= rtmpFloodMax {
		a.rtmpFloodN = map[string]int{} // bound: reset rather than grow without limit
	}
	a.rtmpFloodN[ip]++
	n := a.rtmpFloodN[ip]
	a.rtmpFloodMu.Unlock()
	if a.Logf != nil {
		a.Logf("rtmp: auth failed from %s (%d)", ip, n)
	}
}

func (a *Agent) rtmpFloodCount(ip string) int {
	a.rtmpFloodMu.Lock()
	defer a.rtmpFloodMu.Unlock()
	return a.rtmpFloodN[ip]
}
