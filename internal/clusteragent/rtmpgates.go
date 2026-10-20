package clusteragent

import (
	"crypto/hmac"
	"encoding/json"
	"os"
	"path/filepath"
)

// rtmpAllow is one allowed-RTMP row (rtmp_ips), as it rides in blocklist.json's
// data.rtmp: push/pull are 0/1 ints on disk (the PHP consumer re-casts to bool).
type rtmpAllow struct {
	Password string
	Push     int64
	Pull     int64
}

// allowedRTMP looks an IP up in the allowed-RTMP list MAIN ships inside
// blocklist.json (data.rtmp), the port of BlocklistService::getAllowedRTMP for
// the serve-only LB. The list is cached and rebuilt when blocklist.json's
// mod-time changes (materialise rewrites the file whenever the section's ETag
// changes). A missing or unparseable file yields an empty list (fail-safe).
func (a *Agent) allowedRTMP(ip string) (rtmpAllow, bool) {
	if a.ReplicaDir == "" {
		return rtmpAllow{}, false
	}
	a.rtmpAllowMu.Lock()
	defer a.rtmpAllowMu.Unlock()
	path := filepath.Join(a.ReplicaDir, "blocklist.json")
	fi, err := os.Stat(path)
	if err != nil {
		a.rtmpAllowCache = nil
		return rtmpAllow{}, false
	}
	if a.rtmpAllowCache == nil || !fi.ModTime().Equal(a.rtmpAllowMod) {
		a.rtmpAllowCache = map[string]rtmpAllow{}
		a.rtmpAllowMod = fi.ModTime()
		if b, err := os.ReadFile(path); err == nil {
			var doc struct {
				Data struct {
					Rtmp []struct {
						IP       string `json:"ip"`
						Password string `json:"password"`
						Push     int64  `json:"push"`
						Pull     int64  `json:"pull"`
					} `json:"rtmp"`
				} `json:"data"`
			}
			if json.Unmarshal(b, &doc) == nil {
				for _, r := range doc.Data.Rtmp {
					// rows are IP literals — MAIN IP-validates rtmp_ips at write time.
					// ponytail: no gethostbyname; add resolution only if MAIN ever permits hostname rows.
					a.rtmpAllowCache[r.IP] = rtmpAllow{Password: r.Password, Push: r.Push, Pull: r.Pull}
				}
			}
		}
	}
	e, ok := a.rtmpAllowCache[ip]
	return e, ok
}

// secretMatches ports AuthService::secretMatches: an empty known secret never
// matches; a non-empty one is a constant-time equal against the given value.
// (The "empty admits on IP+flag alone" rule lives in the caller, as rtmp.php.)
func secretMatches(known, given string) bool {
	return known != "" && hmac.Equal([]byte(known), []byte(given))
}

// allowRTMP reports whether the peer IP is on the allowed-RTMP list for the
// given direction ("push" for a publish, "pull" for a relay-play) and clears
// its per-entry password — the OR-arm of rtmp.php:69 (pull) / rtmp.php:183
// (push). An empty entry password admits on IP+flag alone.
func (a *Agent) allowRTMP(ip, want, given string) bool {
	e, ok := a.allowedRTMP(ip)
	if !ok {
		return false
	}
	flag := e.Push
	if want == "pull" {
		flag = e.Pull
	}
	return flag == 1 && (e.Password == "" || secretMatches(e.Password, given))
}

// streamOnDemand reports whether the stream's per-node record marks it on-demand
// (data.server.on_demand == 1), the gate rtmp.php applies before starting a
// held-but-idle stream. A missing/unreadable record, a null data.server
// (an archive/thumbnail-only hold), or on_demand != 1 → false (→ 404).
func (a *Agent) streamOnDemand(id string) bool {
	b, err := os.ReadFile(filepath.Join(streamsDir(a.ReplicaDir), id+".json"))
	if err != nil {
		return false
	}
	var w struct {
		Data struct {
			Server *struct {
				OnDemand int64 `json:"on_demand"`
			} `json:"server"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &w) != nil {
		return false
	}
	return w.Data.Server != nil && w.Data.Server.OnDemand == 1
}
