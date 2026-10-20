package clusteragent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// rtmpUser, rtmpMint and rtmpAuthReply mirror MAIN's rtmp_auth reply
// (ClusterApi::rtmpAuth): the line, its country, and MAIN's admission mint.
type rtmpUser struct {
	ID             int64  `json:"id"`
	MaxConnections int64  `json:"max_connections"`
	PairID         int64  `json:"pair_id"`
	ConISPName     string `json:"con_isp_name"`
	IsRestreamer   bool   `json:"is_restreamer"`
	ExpDate        int64  `json:"exp_date"`
}

// rtmpMint is MAIN's admission mint on the rtmp_auth reply: the reserved uuid,
// the admission claim adm {exp,sid}, and the proof prf {iat,p} — the node mirrors
// adm + a mint string (adm_uuid.prf.iat.prf.p) into the viewer record so the
// agent admits without a second conn_admit call.
type rtmpAdm struct {
	Exp int64 `json:"exp"`
	Sid int64 `json:"sid"`
}

type rtmpPrf struct {
	Iat int64  `json:"iat"`
	P   string `json:"p"`
}

type rtmpMint struct {
	UUID    string   `json:"uuid"`
	Adm     *rtmpAdm `json:"adm"`
	AdmUUID string   `json:"adm_uuid"`
	Prf     *rtmpPrf `json:"prf"`
}

type rtmpAuthReply struct {
	OK          bool      `json:"ok"`
	User        rtmpUser  `json:"user"`
	CountryCode string    `json:"country_code"`
	Token       *rtmpMint `json:"token"`
	Reason      string    `json:"reason"`
}

// rtmpLastYesTTL is how long MAIN's last yes may stand in while MAIN is
// unreachable (the panel's RtmpOffline::TTL).
const rtmpLastYesTTL = 600 * time.Second

type rtmpYesEntry struct {
	reply rtmpAuthReply
	at    time.Time
}

// rtmpLastYes keeps MAIN's last rtmp_auth yes per (stream, ip, restream, creds)
// in memory. The agent is long-lived (the panel's RtmpOffline used per-request
// disk files only because php-fpm is not); no credential is persisted anywhere.
type rtmpLastYes struct {
	mu  sync.Mutex
	m   map[string]rtmpYesEntry
	now func() time.Time
}

func (c *rtmpLastYes) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *rtmpLastYes) keep(key string, r rtmpAuthReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]rtmpYesEntry{}
	}
	c.m[key] = rtmpYesEntry{reply: r, at: c.clock()}
}

func (c *rtmpLastYes) forget(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, key)
}

// answer returns the cached yes while it is fresh (within TTL) and the line has
// not expired on MAIN's clock (nowMs); otherwise nil, dropping a stale entry.
func (c *rtmpLastYes) answer(key string, nowMs int64) *rtmpAuthReply {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok {
		return nil
	}
	if c.clock().Sub(e.at) > rtmpLastYesTTL {
		delete(c.m, key)
		return nil
	}
	if e.reply.User.ExpDate > 0 && nowMs/1000 >= e.reply.User.ExpDate {
		return nil
	}
	r := e.reply
	return &r
}

// rtmpYesKey keys the cache by the stream, address, restream flag and the
// credentials (sorted), as the panel keys RtmpOffline.
func rtmpYesKey(streamID int64, ip string, restream bool, creds map[string]string) string {
	keys := make([]string, 0, len(creds))
	for k := range creds {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "%d|%s|%t|", streamID, ip, restream)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(creds[k])
		b.WriteByte('&')
	}
	return b.String()
}

// lastYes returns the cached yes for key, but only while the node could START a
// new viewer: the offline policy is not "deny", the node is active, and the
// lease is not refusing new sessions (the panel's RtmpOffline::answer gates).
func (a *Agent) lastYes(key string) *rtmpAuthReply {
	if a.OfflineAdmission() == OfflineDeny {
		return nil
	}
	if s, _ := a.state.Load().(string); s != "active" {
		return nil
	}
	if a.refusesNewSessions() {
		return nil
	}
	return a.rtmpYes.answer(key, a.Client.MainNowMs())
}

// rtmpAsk asks MAIN's rtmp_auth for the viewer line (the panel's
// RtmpOffline::ask, LB arm). A MAIN answer is returned as-is — an ok is cached,
// a non-ok forgets the cache. A verified refusal (a *Denial) forgets and
// returns REFUSED. An unreachable MAIN returns the cached yes, or NO_ANSWER.
func (a *Agent) rtmpAsk(ctx context.Context, streamID int64, ip string, restream bool, uuid string, creds map[string]string) rtmpAuthReply {
	key := rtmpYesKey(streamID, ip, restream, creds)
	payload := map[string]any{"stream_id": streamID, "ip": ip, "restream": restream, "uuid": uuid}
	for k, v := range creds {
		payload[k] = v
	}
	var out rtmpAuthReply
	err := a.Client.Call(ctx, "rtmp_auth", payload, &out, false)
	if err == nil {
		if out.OK {
			a.rtmpYes.keep(key, out)
		} else {
			a.rtmpYes.forget(key)
		}
		return out
	}
	var d *Denial
	if errors.As(err, &d) {
		a.rtmpYes.forget(key)
		return rtmpAuthReply{OK: false, Reason: "REFUSED"}
	}
	if r := a.lastYes(key); r != nil {
		return *r
	}
	return rtmpAuthReply{OK: false, Reason: "NO_ANSWER"}
}
