package clusteragent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Admission (plan, section 8, "Global max_connections and kills", steps 4 to
// 6; ADR 0004, ninth Phase 6 increment): a new viewer with a limited token is
// registered with an X-XCVM-Admission header on PUT /v1/conn/{uuid}, a
// compact JSON object built by the node's PHP (AgentConnections::admission):
//
//	{adm?: {exp, sid}, line_id | hmac_id + identifier, stream_id, max_connections, ip, ua, mint?}
//
//   - adm: MAIN admitted the viewer when it minted the token, until exp (MAIN's
//     unix seconds). While exp is not past on MAIN's clock the viewer is
//     admitted with no WAN call.
//   - otherwise the agent asks MAIN's conn_admit on the ctl lane, within
//     AdmitWait of the PUT's arrival. MAIN's answer decides; a verified
//     NOT_ACTIVE, FLOW_OFF or BAD_REQUEST admits (admission does not apply);
//     anything else — no answer, a transport error, STARTING, RATE_LIMITED,
//     DB, an older MAIN's UNKNOWN_OP — applies the offline policy.
//   - mint: MAIN's proof that it minted the viewer's token (<uuid>.<iat>.<p>,
//     ADR 0004, "The line a node names"), which the node's PHP also keeps in
//     the record. It is copied into conn_admit as it is: MAIN reserves and
//     cuts for a viewer whose mint it verifies, and under its `enforce`
//     binding admits one without it with neither. A malformed one is left out.
//   - the offline policy is MAIN's lb_offline_admission, delivered in every
//     hello and heartbeat reply (offline_admission) and kept in the state
//     file: allow admits, deny refuses (OFFLINE), local counts the viewer's
//     owner's open records in the registry, leaving out this uuid and the
//     same device, and refuses (LIMIT) at max_connections. It never ends a
//     record.
//
// A refusal is HTTP 403 {"admit": false, "reason": "<REASON>"}: nothing is
// stored and no event is spooled. A missing or malformed header is a plain
// register, as before admission existed.

// AdmissionHeader carries a new viewer's admission request on PUT /v1/conn/{uuid}.
const AdmissionHeader = "X-XCVM-Admission"

// AdmitWait is how long a register may wait for MAIN's conn_admit, from the
// PUT's arrival (PHP waits 2.5 s for the whole register).
var AdmitWait = 1500 * time.Millisecond

// The offline policies (lb_offline_admission).
const (
	OfflineLocal = "local"
	OfflineAllow = "allow"
	OfflineDeny  = "deny"
)

var admitReason = regexp.MustCompile(`^[A-Z_]{1,32}$`)

// admitMint is the form of a mint proof: a connection uuid, MAIN's unix
// seconds and 16 bytes of MAC as hex.
var admitMint = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}\.[0-9]{1,12}\.[0-9a-f]{32}$`)

// admissionReq is a parsed, valid X-XCVM-Admission header.
type admissionReq struct {
	adm        bool // a well-formed adm claim is present
	admExp     int64
	lineID     int64
	hmacID     int64
	identifier string
	streamID   int64
	max        int64
	ip, ua     string
	mint       string // MAIN's proof of the token's mint, "" without one
}

// jsonInt reads a JSON integer (json.Number without a fraction or exponent).
func jsonInt(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok || strings.ContainsAny(string(n), ".eE") {
		return 0, false
	}
	i, err := n.Int64()
	return i, err == nil
}

// parseAdmission reads the header; nil means a plain register: no header,
// not a JSON object, no single valid identity, or a max_connections that is
// missing, not an int or below 1. An adm that is not {exp: int, sid: int}
// is ignored.
func parseAdmission(h string) *admissionReq {
	if strings.TrimSpace(h) == "" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(h))
	dec.UseNumber()
	var doc map[string]any
	if dec.Decode(&doc) != nil || doc == nil || dec.More() {
		return nil
	}
	req := &admissionReq{}
	line, lineOK := jsonInt(doc["line_id"])
	hmac, hmacOK := jsonInt(doc["hmac_id"])
	ident, identOK := doc["identifier"].(string)
	lineOK = lineOK && line > 0
	hmacOK = hmacOK && hmac > 0 && identOK
	switch {
	case lineOK && !hmacOK:
		req.lineID = line
	case hmacOK && !lineOK:
		req.hmacID, req.identifier = hmac, ident
	default:
		return nil
	}
	max, ok := jsonInt(doc["max_connections"])
	if !ok || max < 1 {
		return nil
	}
	req.max = max
	req.streamID, _ = jsonInt(doc["stream_id"])
	req.ip, _ = doc["ip"].(string)
	req.ua, _ = doc["ua"].(string)
	if m, ok := doc["mint"].(string); ok && admitMint.MatchString(m) {
		req.mint = m
	}
	if adm, ok := doc["adm"].(map[string]any); ok {
		exp, ok1 := jsonInt(adm["exp"])
		_, ok2 := jsonInt(adm["sid"])
		if ok1 && ok2 {
			req.adm, req.admExp = true, exp
		}
	}
	return req
}

// admitCache holds conn_admit's admitting answers until their exp (MAIN ms),
// so a uuid registered again meanwhile needs no second call.
type admitCache struct {
	mu  sync.Mutex
	exp map[string]int64
}

// admitCacheMax bounds the cache; expired entries go first when it is full.
const admitCacheMax = 20000

func (c *admitCache) get(uuid string, nowMs int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	exp, ok := c.exp[uuid]
	return ok && exp >= nowMs
}

func (c *admitCache) put(uuid string, expMs, nowMs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.exp == nil {
		c.exp = map[string]int64{}
	}
	if len(c.exp) >= admitCacheMax {
		for k, v := range c.exp {
			if v < nowMs {
				delete(c.exp, k)
			}
		}
		if len(c.exp) >= admitCacheMax {
			return
		}
	}
	c.exp[uuid] = expMs
}

// OfflineAdmission is the offline policy the agent holds now.
func (a *Agent) OfflineAdmission() string {
	st := a.Client.State
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.OfflineAdmission == "" {
		return OfflineLocal
	}
	return st.OfflineAdmission
}

// setOfflineAdmission keeps a reply's offline_admission; a value other than
// local, allow or deny is ignored and the one held is kept.
func (a *Agent) setOfflineAdmission(v string) {
	if v != OfflineLocal && v != OfflineAllow && v != OfflineDeny {
		return
	}
	st := a.Client.State
	st.mu.Lock()
	if st.OfflineAdmission == v {
		st.mu.Unlock()
		return
	}
	st.OfflineAdmission = v
	err := st.saveLocked()
	st.mu.Unlock()
	if err != nil {
		a.logf("cluster: saving the offline admission policy: %v", err)
		return
	}
	a.logf("cluster: offline admission %s", v)
}

// admit decides a new viewer's register; it returns "" to admit, else the
// refusal's reason. arrived is when the PUT arrived.
func (a *Agent) admit(ctx context.Context, arrived time.Time, uuid string, rec map[string]any, header string) string {
	req := parseAdmission(header)
	if req == nil {
		return ""
	}
	now := a.Client.MainNowMs()
	if req.adm && req.admExp*1000 >= now {
		return ""
	}
	if a.admits.get(uuid, now) {
		return ""
	}
	if s, _ := a.state.Load().(string); s != "active" || a.flows.Load()&FlowConnections == 0 {
		// Admission does not apply: MAIN mints no claim for such a node and
		// answers its conn_admit NOT_ACTIVE or FLOW_OFF.
		return ""
	}
	payload := map[string]any{"uuid": uuid, "stream_id": req.streamID, "ip": req.ip, "ua": req.ua}
	if req.lineID > 0 {
		payload["line_id"] = req.lineID
	} else {
		payload["hmac_id"], payload["identifier"] = req.hmacID, req.identifier
	}
	if req.mint != "" {
		payload["mint"] = req.mint
	}
	cctx, cancel := context.WithDeadline(ctx, arrived.Add(AdmitWait))
	defer cancel()
	var out struct {
		Admit  bool   `json:"admit"`
		Exp    int64  `json:"exp"`
		Reason string `json:"reason"`
	}
	err := a.Client.Call(cctx, "conn_admit", payload, &out, false)
	if err == nil {
		if out.Admit {
			if out.Exp > 0 {
				a.admits.put(uuid, out.Exp*1000, a.Client.MainNowMs())
			}
			return ""
		}
		if !admitReason.MatchString(out.Reason) {
			return "REFUSED"
		}
		return out.Reason
	}
	var d *Denial
	if errors.As(err, &d) {
		switch d.Reason {
		case "NOT_ACTIVE", "FLOW_OFF", "BAD_REQUEST":
			return "" // MAIN answered: admission does not apply here
		}
	}
	return a.offlineAdmit(uuid, rec, req)
}

// offlineAdmit applies the offline policy to a viewer MAIN did not decide.
func (a *Agent) offlineAdmit(uuid string, rec map[string]any, req *admissionReq) string {
	switch a.OfflineAdmission() {
	case OfflineAllow:
		return ""
	case OfflineDeny:
		return "OFFLINE"
	}
	if a.Registry == nil {
		return ""
	}
	if int64(a.Registry.openOf(owner(rec), uuid, rec["user_ip"], rec["user_agent"])) >= req.max {
		return "LIMIT"
	}
	return ""
}

// openOf counts the open records (hls_end not set) of an owner, leaving out
// uuid and the records of the same device (the same user_ip and user_agent).
func (r *Registry) openOf(own, uuid string, ip, ua any) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for k, c := range r.conns {
		if k == uuid || num(c["hls_end"]) != 0 || owner(c) != own {
			continue
		}
		if fmt.Sprint(c["user_ip"]) == fmt.Sprint(ip) && fmt.Sprint(c["user_agent"]) == fmt.Sprint(ua) {
			continue
		}
		n++
	}
	return n
}
