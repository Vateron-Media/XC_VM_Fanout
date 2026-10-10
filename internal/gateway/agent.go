package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// agentTimeout bounds one call to the node's agent; past it, PHP answers.
const agentTimeout = time.Second

// admitTimeout is AgentConnections::ADMIT_TIMEOUT: a new viewer's register,
// which may wait for the agent's conn_admit to MAIN (1.5 s) and then its
// offline policy.
const admitTimeout = 2500 * time.Millisecond

// admissionHeaderName is the agent's X-XCVM-Admission (clusteragent.AdmissionHeader).
const admissionHeaderName = "X-XCVM-Admission"

// AgentConns reads and touches the viewers' records in the node's agent
// (xc_agent's /v1/conn on its local socket), the store a node with the
// CONNECTIONS flow keeps them in, as the panel's AgentConnections does.
type AgentConns struct {
	client *http.Client
	admit  *http.Client // the same socket, with a new viewer's longer wait
}

// NewAgentConns talks to the agent on the unix socket at sock.
func NewAgentConns(sock string) *AgentConns {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
		MaxIdleConnsPerHost: 8,
	}
	return &AgentConns{client: &http.Client{Timeout: agentTimeout, Transport: tr}, admit: &http.Client{Timeout: admitTimeout, Transport: tr}}
}

// Touch is ConnectionTracker::heartbeat on a CONNECTIONS node: the record's
// hls_last_read set to lastRead, and whether the record says the session
// ended. A missing record is not ended (PHP serves it); ok is false when the
// agent did not answer, so PHP decides.
func (a *AgentConns) Touch(uuid string, lastRead int64) (ended, ok bool) {
	return a.call(http.MethodPost, uuid+"/touch", fmt.Sprintf(`{"hls_last_read":%d}`, lastRead))
}

// Peek is Touch without the write, for shadow, which only judges.
func (a *AgentConns) Peek(uuid string) (ended, ok bool) {
	return a.call(http.MethodGet, uuid, "")
}

func (a *AgentConns) call(method, path, body string) (ended, ok bool) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://agent/v1/conn/"+path, rd)
	if err != nil {
		return false, false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return false, false
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return false, true
	case http.StatusOK:
	default:
		return false, false
	}
	var rec map[string]json.RawMessage
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&rec) != nil {
		return false, false
	}
	return phpNonEmpty(rec["hls_end"]), true
}

// phpNonEmpty is PHP's !empty() on a decoded JSON value: not null, false, 0,
// "", "0" or an empty array.
func phpNonEmpty(raw json.RawMessage) bool {
	switch string(raw) {
	case "", "null", "false", "0", `""`, `"0"`, "[]", "{}":
		return false
	case "true", "1":
		return true
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return false
	}
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case float64:
		return x != 0
	case string:
		return x != "" && x != "0"
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	}
	return true
}

// Get is AgentConnections::get: the viewer's record, whether there is one,
// and whether the agent answered.
func (a *AgentConns) Get(uuid string) (rec map[string]json.RawMessage, found, ok bool) {
	req, err := http.NewRequest(http.MethodGet, "http://agent/v1/conn/"+uuid, nil)
	if err != nil {
		return nil, false, false
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, false, false
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return nil, false, true
	case http.StatusOK:
	default:
		return nil, false, false
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&rec) != nil || rec == nil {
		return nil, false, false
	}
	return rec, true, true
}

// Put is AgentConnections::put: the record replaced (no admission header: a
// refresh of a viewer the agent already holds). True when the agent stored it.
func (a *AgentConns) Put(uuid string, rec map[string]json.RawMessage) bool {
	stored, _ := a.put(a.client, uuid, rec, "")
	return stored
}

// Register is AgentConnections::register: a new viewer's record stored,
// admitted by the agent when admission (its X-XCVM-Admission) is given.
// refused: the agent answered 403 {"admit": false} and stored nothing.
// Neither: it did not answer, and PHP decides.
func (a *AgentConns) Register(uuid string, rec map[string]json.RawMessage, admission string) (stored, refused bool) {
	if admission == "" {
		return a.put(a.client, uuid, rec, "")
	}
	return a.put(a.admit, uuid, rec, admission)
}

func (a *AgentConns) put(client *http.Client, uuid string, rec map[string]json.RawMessage, admission string) (stored, refused bool) {
	b, err := json.Marshal(rec)
	if err != nil {
		return false, false
	}
	req, err := http.NewRequest(http.MethodPut, "http://agent/v1/conn/"+uuid, strings.NewReader(string(b)))
	if err != nil {
		return false, false
	}
	req.Header.Set("Content-Type", "application/json")
	if admission != "" {
		req.Header.Set(admissionHeaderName, admission)
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, false
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden && admission != "" {
		var out map[string]json.RawMessage
		if json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&out) == nil && string(out["admit"]) == "false" {
			return false, true
		}
		return false, false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK, false
}
