package clusteragent

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/redact"
)

// Fanout events (plan, Phase 5): the node's xc_fanout publishes its supervised
// streams' monitor transitions on GET /events (control socket). While the
// STREAMS flow is on, the agent follows that feed and spools each transition
// as a P0 `stream.monitor` event, with the source redacted; MAIN derives the
// stream's row from it as PHP's reconcile does (StreamProcess::
// supervisedRowUpdate), within a second instead of the reconcile's cadence.
//
// Once the feed answers, the agent says so in flows.json ("features":
// ["fanout_events"]) and the node's PHP reconcile stops writing the same
// state; it keeps releasing streams nothing should produce.

// FlowStreams is the STREAMS flow bit (MAIN's NodeRegistry::FLOW_STREAMS).
const FlowStreams = 8

// FanoutWait is how long the fanout holds an /events poll with nothing to say.
var FanoutWait = 20 * time.Second

// FanoutIdle is how often the agent looks again while it does not follow the feed.
var FanoutIdle = 2 * time.Second

type fanoutEvent struct {
	Seq    uint64          `json:"seq"`
	Type   string          `json:"type"`
	Stream string          `json:"stream"`
	State  json.RawMessage `json:"state"`
}

type fanoutReply struct {
	Boot   string        `json:"boot"`
	Seq    uint64        `json:"seq"`
	Reset  bool          `json:"reset"`
	Events []fanoutEvent `json:"events"`
}

func fanoutClient(sock string) *http.Client {
	return &http.Client{
		Timeout: FanoutWait + 10*time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}},
	}
}

// RunFanoutEvents follows the fanout's feed until ctx ends.
func (a *Agent) RunFanoutEvents(ctx context.Context) {
	hc := fanoutClient(a.FanoutCtl)
	boot, seq := "", uint64(0)
	backoff := time.Second
	for ctx.Err() == nil {
		if a.flows.Load()&FlowStreams == 0 {
			a.setFanoutLive(false)
			if !sleep(ctx, FanoutIdle) {
				return
			}
			continue
		}
		out, err := fanoutPoll(ctx, hc, boot, seq)
		if err != nil {
			a.setFanoutLive(false)
			if ctx.Err() != nil {
				return
			}
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, time.Minute)
			continue
		}
		backoff = time.Second
		if len(out.Events) > 0 {
			if err := a.spoolMonitor(out); err != nil {
				a.logf("cluster: fanout events: %v", err)
				a.setFanoutLive(false)
				sleep(ctx, backoff)
				continue // not advanced: the same events come again
			}
		}
		boot, seq = out.Boot, out.Seq
		a.setFanoutLive(true)
	}
}

func fanoutPoll(ctx context.Context, hc *http.Client, boot string, seq uint64) (*fanoutReply, error) {
	q := url.Values{"boot": {boot}, "since": {strconv.FormatUint(seq, 10)}, "wait": {strconv.Itoa(int(FanoutWait / time.Second))}}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://fanout/events?"+q.Encode(), nil)
	res, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fanout /events: HTTP %d", res.StatusCode) // an older daemon: 404
	}
	var out fanoutReply
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// spoolMonitor writes the transitions as one P0 spool file, as PHP's
// EventSpool does (written aside, renamed in, named by CLOCK_MONOTONIC).
func (a *Agent) spoolMonitor(out *fanoutReply) error {
	var body []byte
	now := time.Now().UnixMilli()
	for _, e := range out.Events {
		id, err := strconv.Atoi(e.Stream)
		if e.Type != "monitor" || err != nil || id <= 0 {
			continue
		}
		var state map[string]any
		if json.Unmarshal(e.State, &state) != nil {
			continue
		}
		if src, ok := state["source"].(string); ok {
			state["source"] = redact.URL(src)
		}
		delete(state, "last_error") // free text from ffmpeg; may quote a URL
		line, err := json.Marshal(map[string]any{"type": "stream.monitor", "t": now, "d": map[string]any{"stream_id": id, "state": state}})
		if err != nil {
			return err
		}
		body = append(append(body, line...), '\n')
	}
	if len(body) == 0 {
		return nil
	}
	dir := filepath.Join(a.SpoolDir, "p0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	name := fmt.Sprintf("%019d-agent-%04x.ndjson", monotonicNs(), rand.Intn(0x10000))
	tmp := filepath.Join(dir, "."+name+".tmp")
	if err := os.WriteFile(tmp, body, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, name))
}

func (a *Agent) setFanoutLive(on bool) {
	if a.fanoutLive.Swap(on) != on {
		a.logf("cluster: fanout events %s", map[bool]string{true: "followed", false: "not followed"}[on])
	}
}
