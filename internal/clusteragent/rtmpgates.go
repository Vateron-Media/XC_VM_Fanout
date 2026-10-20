package clusteragent

import (
	"crypto/hmac"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// floodBlocked reports whether the peer IP carries a block_<ip> flood marker —
// the RTMP mirror of the gateway's FloodProtectionStage / judge.go BootstrapRefuses
// (clusteragent does not import gateway, so the trivial check is re-implemented).
// A non-IP addr or an unset FloodDir is never blocked.
func (a *Agent) floodBlocked(ip string) bool {
	if a.FloodDir == "" || net.ParseIP(ip) == nil {
		return false
	}
	_, err := os.Stat(filepath.Join(a.FloodDir, "block_"+ip))
	return err == nil
}

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

// floodTick ports BruteforceGuard::checkFlood for the RTMP deny set: a per-IP
// {requests,last_request} counter under FloodDir, written atomically (a random
// temp, so it coexists with php-fpm's LOCK_EX writes of the same file during the
// transition). Over the limit it touches block_<ip> and spools MAIN a P0
// security.block_ip (unless the IP is already blocklisted). flood_limit 0/unset,
// an allowed IP, a flood_ips_exclude IP, a non-IP, or no FloodDir → no-op.
func (a *Agent) floodTick(ip string) {
	data := a.replicaSettings()
	lim, _ := settingInt(data["flood_limit"])
	if lim == 0 {
		return
	}
	if a.FloodDir == "" || net.ParseIP(ip) == nil {
		return
	}
	if a.allowedIPs()[ip] {
		return
	}
	for _, ex := range strings.Split(floodStr(data, "flood_ips_exclude"), ",") {
		if ex != "" && ex == ip {
			return
		}
	}
	ipFile := filepath.Join(a.FloodDir, ip)
	now := time.Now().Unix()
	b, err := os.ReadFile(ipFile)
	if err != nil {
		a.floodWrite(ipFile, 0, now) // first sighting: requests 0
		return
	}
	var row struct {
		Requests    int64 `json:"requests"`
		LastRequest int64 `json:"last_request"`
	}
	if json.Unmarshal(b, &row) != nil {
		a.floodWrite(ipFile, 0, now)
		return
	}
	secs, _ := settingInt(data["flood_seconds"])
	if now-row.LastRequest <= secs { // within the window
		row.Requests++
		if lim > row.Requests {
			a.floodWrite(ipFile, row.Requests, now)
			return
		}
		// requests >= flood_limit → block
		if !a.blockedIPs()[ip] {
			_ = spoolP0(a.SpoolDir, []map[string]any{{"type": "security.block_ip", "d": map[string]any{"ip": ip, "reason": "FLOOD ATTACK"}}})
		}
		_ = os.WriteFile(filepath.Join(a.FloodDir, "block_"+ip), nil, 0o640)
		_ = os.Remove(ipFile)
		return
	}
	a.floodWrite(ipFile, 0, now) // window expired: reset to requests 0
}

func (a *Agent) floodWrite(ipFile string, requests, last int64) {
	b, _ := json.Marshal(map[string]any{"requests": requests, "last_request": last})
	_ = writeFile(ipFile, b, fileWrite{perm: 0o640}) // random temp → safe under concurrency
}

func floodStr(data map[string]any, key string) string {
	s, _ := data[key].(string)
	return s
}

// allowedIPs ports ServerRepository::getAllowedIPs: 127.0.0.1, plus every
// server's server_ip, private_ip, valid-IP domain_name entries and whitelist_ips
// (a JSON-array-encoded string — null/""/malformed is skipped), plus the CSV
// allowed_ips_admin. The node's own server_ip is covered by the server loop.
func (a *Agent) allowedIPs() map[string]bool {
	set := map[string]bool{"127.0.0.1": true}
	if a.ReplicaDir != "" {
		if b, err := os.ReadFile(filepath.Join(a.ReplicaDir, "servers.json")); err == nil {
			var doc struct {
				Data struct {
					Servers []struct {
						ServerIP   string `json:"server_ip"`
						PrivateIP  string `json:"private_ip"`
						Whitelist  string `json:"whitelist_ips"`
						DomainName string `json:"domain_name"`
					} `json:"servers"`
				} `json:"data"`
			}
			if json.Unmarshal(b, &doc) == nil {
				for _, s := range doc.Data.Servers {
					if s.ServerIP != "" {
						set[s.ServerIP] = true
					}
					if s.PrivateIP != "" {
						set[s.PrivateIP] = true
					}
					for _, d := range strings.Split(s.DomainName, ",") {
						if net.ParseIP(d) != nil {
							set[d] = true
						}
					}
					if s.Whitelist != "" {
						var ips []string
						if json.Unmarshal([]byte(s.Whitelist), &ips) == nil {
							for _, ip := range ips {
								set[ip] = true
							}
						}
					}
				}
			}
		}
	}
	for _, ip := range strings.Split(floodStr(a.replicaSettings(), "allowed_ips_admin"), ",") {
		if ip != "" {
			set[ip] = true
		}
	}
	return set
}

// blockedIPs is the materialised blocklist IP set (blocklist.json data.ip), the
// port of BlocklistService::getBlockedIPs — the guard that keeps floodTick from
// re-spooling a P0 for an IP MAIN already has blocklisted.
func (a *Agent) blockedIPs() map[string]bool {
	set := map[string]bool{}
	if a.ReplicaDir == "" {
		return set
	}
	b, err := os.ReadFile(filepath.Join(a.ReplicaDir, "blocklist.json"))
	if err != nil {
		return set
	}
	var doc struct {
		Data struct {
			IP []string `json:"ip"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &doc) == nil {
		for _, ip := range doc.Data.IP {
			set[ip] = true
		}
	}
	return set
}
