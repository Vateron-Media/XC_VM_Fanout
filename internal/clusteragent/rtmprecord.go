package clusteragent

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"time"
)

// rtmpUUID names an RTMP viewer's connection record from nginx_rtmp's client id
// (which play_done closes it by): md5(server_id "/" client_id), the panel's
// ConnectionTracker::rtmpUuid. nginx numbers its clients from 1 per process, so
// the server id is part of it; the md5 only names the record, it protects nothing.
func rtmpUUID(serverID int64, clientID string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%d/%s", serverID, clientID))))
}

// rtmpAdmissionHeader builds the X-XCVM-Admission header the agent's admit gate
// reads, exactly as the panel's AgentConnections::admission does: empty (no gate,
// a plain register) when the line is unlimited or the node is not active; else
// the line, limit, stream and addr, plus MAIN's adm claim and mint when valid so
// the agent admits without a second conn_admit call.
func (a *Agent) rtmpAdmissionHeader(reply rtmpAuthReply, rec map[string]any, streamID int64) string {
	max := reply.User.MaxConnections
	if max <= 0 {
		return ""
	}
	if s, _ := a.state.Load().(string); s != "active" {
		return ""
	}
	doc := map[string]any{
		"line_id":         reply.User.ID,
		"stream_id":       streamID,
		"max_connections": max,
		"ip":              rec["user_ip"],
		"ua":              rec["user_agent"],
	}
	mainNow := a.Client.MainNowMs() / 1000
	if t := reply.Token; t != nil && t.Adm != nil && t.Adm.Exp >= mainNow && t.Adm.Sid == a.Client.State.ServerID {
		doc["adm"] = map[string]any{"exp": t.Adm.Exp, "sid": t.Adm.Sid}
	}
	if m, ok := rec["mint"].(string); ok && m != "" {
		doc["mint"] = m
	}
	b, _ := json.Marshal(doc)
	return string(b)
}

// openRtmpRecord opens the RTMP viewer's connection record in the registry,
// gated by the agent's admission (the panel's ConnectionTracker::openRecord +
// AgentConnections::register). Returns false — the caller 404s — when the
// admission refuses the viewer, or there is no registry to track it.
func (a *Agent) openRtmpRecord(ctx context.Context, n *rtmpNotify, reply rtmpAuthReply, streamID int64) bool {
	if a.Registry == nil {
		return false
	}
	uuid := rtmpUUID(a.Client.State.ServerID, n.ClientID)
	nowSec := a.Client.MainNowMs() / 1000
	rec := map[string]any{
		"uuid":               uuid,
		"user_id":            reply.User.ID,
		"stream_id":          streamID,
		"server_id":          a.Client.State.ServerID,
		"proxy_id":           0,
		"user_agent":         "",
		"user_ip":            n.Addr,
		"container":          "rtmp",
		"pid":                n.ClientID,
		"date_start":         nowSec,
		"geoip_country_code": reply.CountryCode,
		"isp":                reply.User.ConISPName,
		"hls_end":            0,
		"hls_last_read":      nowSec,
		"identity":           reply.User.ID,
	}
	if t := reply.Token; t != nil && t.AdmUUID != "" && t.Prf != nil {
		rec["mint"] = fmt.Sprintf("%s.%d.%s", t.AdmUUID, t.Prf.Iat, t.Prf.P)
	}
	if header := a.rtmpAdmissionHeader(reply, rec, streamID); header != "" {
		if reason := a.admit(ctx, time.Now(), uuid, rec, header); reason != "" {
			if a.Logf != nil {
				a.Logf("rtmp: admission refused %s: %s", uuid, reason)
			}
			return false
		}
	}
	if err := a.Registry.Put(uuid, rec); err != nil {
		if a.Logf != nil {
			a.Logf("rtmp: record write %s: %v", uuid, err)
		}
		return false
	}
	return true
}

// closeRtmpRecord ends the RTMP viewer's record on play_done, the panel's
// ConnectionLimiter::closeRTMP → ShutdownHandler::closeInRegistry: an upsert
// with hls_end=1, guarded by the stored record's pid matching the client id so
// a play_done for an unknown or mismatched client closes nothing.
func (a *Agent) closeRtmpRecord(clientID string) {
	if a.Registry == nil {
		return
	}
	uuid := rtmpUUID(a.Client.State.ServerID, clientID)
	rec := a.Registry.Get(uuid)
	if rec == nil || fmt.Sprint(rec["pid"]) != clientID {
		return
	}
	ended := make(map[string]any, len(rec)+1)
	for k, v := range rec {
		ended[k] = v
	}
	ended["hls_end"] = 1
	ended["hls_last_read"] = a.Client.MainNowMs() / 1000
	_ = a.Registry.Put(uuid, ended)
}
