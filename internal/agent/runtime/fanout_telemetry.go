package clusteragent

import "os"

// FanoutStatus is FanoutClient::status for a PHP-free node: the daemon's state
// read from its own control socket, shaped for servers.watchdog_data.fanout.
//   - socket: the control socket file exists (the binary started at all);
//   - running: the socket answered GET /connections (the daemon is up);
//   - connections: live viewers, nil when the daemon is unreachable;
//   - memory: always nil — FanoutClient::memory (the panel's per-stream ring
//     view) is PHP-only, and ReleaseCanary::fanoutRunning + capacity policies
//     read running/connections, not memory.
func (a *Agent) FanoutStatus() map[string]any {
	st := map[string]any{"socket": false, "running": false, "connections": nil, "memory": nil}
	if a.FanoutCtl == "" {
		return st
	}
	if _, err := os.Stat(a.FanoutCtl); err == nil {
		st["socket"] = true
	}
	if live, err := fanoutConnections(a.FanoutCtl); err == nil {
		st["running"] = true
		st["connections"] = len(live)
	}
	return st
}

// TelemetryWithFanout wraps a telemetry sampler so a serve-only node reports its
// own daemon status under local.fanout. HeartbeatService::toWatchdogData writes
// local.fanout into servers.watchdog_data when the agent sends it, else keeps the
// value a PHP watchdog last wrote — which, on a PHP-free node, freezes at
// running:false, so ReleaseCanary::fanoutRunning never vouches the serve-only
// canary and capacity policies have no node-reported fanout count (XC_VM #478).
// Only installed on a serve-only node; a PHP node writes local.json (fanout
// included) itself and is untouched. Any other local fields the sampler produced
// are preserved.
func (a *Agent) TelemetryWithFanout(base func() map[string]any) func() map[string]any {
	return func() map[string]any {
		out := base()
		if out == nil {
			out = map[string]any{}
		}
		local, _ := out["local"].(map[string]any)
		if local == nil {
			local = map[string]any{}
		}
		local["fanout"] = a.FanoutStatus()
		out["local"] = local
		return out
	}
}
