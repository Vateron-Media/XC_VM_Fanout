package clusteragent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// FlowLogs is NodeFlows::LOGS — the node spools its logs to MAIN (a serve-only
// LB). The chores only run, and the crontab is only filtered, while it is on;
// on a node that inserts locally (MAIN, LOGS off) the agent owns nothing.
const FlowLogs = 4

const (
	// choreTouchInterval keeps the node_crons marker fresh well under the PHP
	// side's NODE_CRONS_FRESH_SEC (30s), so a live agent is never judged stale.
	choreTouchInterval = 5 * time.Second
	// choreRunInterval is the drain/sweep cadence (what the PHP crontab used).
	choreRunInterval = time.Minute
)

// RunChores is the node-local cron loop: it drains + sweeps on the ~minute
// cadence the PHP crontab used, but touches the node_crons liveness marker much
// more often (choreTouchInterval) so the node-side crontab gate never mistakes a
// live agent for a dead one.
func (a *Agent) RunChores(ctx context.Context) {
	a.runChores()
	a.touchNodeCrons()
	t := time.NewTicker(choreTouchInterval)
	defer t.Stop()
	elapsed := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.touchNodeCrons()
			if elapsed += choreTouchInterval; elapsed >= choreRunInterval {
				elapsed = 0
				a.runChores()
			}
		}
	}
}

// runChores runs the log drains + the tmp sweep, only on a log-owning node (LOGS
// flow on). Off that posture the agent leaves the crons to PHP (which would
// SQL-insert, not spool), so the single-owner invariant holds relative to the flow.
func (a *Agent) runChores() {
	if a.flows.Load()&FlowLogs == 0 {
		return
	}
	for _, chore := range []func() error{a.drainStreamLogs, a.drainLinesLogs, a.drainActivity, a.drainErrors, a.sweepTmp} {
		if err := chore(); err != nil && a.Logf != nil {
			a.Logf("cluster: chore: %v", err)
		}
	}
}

// runChoresTick is one full cycle (work + marker) — used at startup and by tests.
func (a *Agent) runChoresTick() {
	a.runChores()
	a.touchNodeCrons()
}

// ownedCrons is the set of node crons the agent owns (and drains) — the log
// drains + tmp, only while the LOGS flow is on. Empty otherwise, so the node
// keeps its PHP crons. maxmind is absent: its license key is not shipped to a
// node, so it cannot be agent-owned and stays a PHP cron.
func (a *Agent) ownedCrons() []string {
	if a.flows.Load()&FlowLogs == 0 {
		return nil
	}
	return []string{"streams_logs", "lines_logs", "activity", "errors", "tmp"}
}

// touchNodeCrons rewrites the node_crons marker with the owned-cron set (JSON
// {"crons":[...]}); its mtime is the liveness signal. When the agent owns none
// (LOGS off), it writes nothing so the marker goes stale and the node keeps its
// PHP crons — never a gap with no owner.
func (a *Agent) touchNodeCrons() {
	owned := a.ownedCrons()
	if a.NodeCronsFile == "" || len(owned) == 0 {
		return
	}
	b, _ := json.Marshal(map[string]any{"crons": owned})
	_ = writeFile(a.NodeCronsFile, b, fileWrite{perm: 0o640, noSync: true})
}

// crondrain ports the node-local log-drain crons into the agent: read a
// node-local base64(json) log file, build the LogSink rows, and spool the same
// log.<kind> events LogSink::spool emits on the p1 lane. MAIN's EventIngest is
// unchanged. Each drain holds the same CronTrait lock (cronlock.go) PHP uses, so
// a still-scheduled PHP cron and the agent never drain one file at once.

// Node tmp-subdir helpers, derived from TmpRoot (TMP_PATH), per ConstantsInitializer.
func (a *Agent) logsTmpDir() string       { return filepath.Join(a.TmpRoot, "logs") }
func (a *Agent) cronsDir() string         { return filepath.Join(a.TmpRoot, "crons") }
func (a *Agent) floodTmpDir() string      { return filepath.Join(a.TmpRoot, "flood") }
func (a *Agent) signalsTmpDir() string    { return filepath.Join(a.TmpRoot, "signals") }
func (a *Agent) divergenceTmpDir() string { return filepath.Join(a.TmpRoot, "divergence") }
func (a *Agent) ministraTmpDir() string   { return filepath.Join(a.TmpRoot, "ministra") }

const (
	logChunk      = 1000    // LogSink::CHUNK
	logSpoolBytes = 1048576 // LogSink::SPOOL_BYTES (1 MiB of value-bytes)
)

var streamLogCols = []string{"stream_id", "server_id", "action", "source", "date"}

// logEvents ports LogSink::spool's batching: for each row keep only cols keys,
// redact string values, and emit a {"type":"log.<kind>","d":{"rows":[...]}} event
// when the batch reaches logChunk rows or before a row that would push the
// running value-byte total past logSpoolBytes.
func logEvents(kind string, cols []string, rows []map[string]any) []map[string]any {
	var events, batch []map[string]any
	var nbytes int
	flush := func() {
		events = append(events, map[string]any{"type": "log." + kind, "d": map[string]any{"rows": batch}})
		batch, nbytes = nil, 0
	}
	for _, row := range rows {
		kept := map[string]any{}
		for _, c := range cols {
			if v, ok := row[c]; ok {
				kept[c] = v
			}
		}
		kept = redactLogRow(kept)
		size := rowSize(kept)
		if len(batch) > 0 && (len(batch) >= logChunk || nbytes+size > logSpoolBytes) {
			flush()
		}
		batch = append(batch, kept)
		nbytes += size
	}
	if len(batch) > 0 {
		flush()
	}
	return events
}

// rowSize mirrors LogSink::spool's sizing: len() for strings, 8 otherwise.
func rowSize(row map[string]any) int {
	n := 0
	for _, v := range row {
		if s, ok := v.(string); ok {
			n += len(s)
		} else {
			n += 8
		}
	}
	return n
}

// readLogLines reads a base64(json) log file into decoded objects. A missing
// file yields (nil, false, nil); a present one yields (objects, true, nil) after
// skipping blank/undecodable/non-object lines.
func readLogLines(path string) ([]map[string]any, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var out []map[string]any
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		dec, err := base64.StdEncoding.DecodeString(line)
		if err != nil {
			continue
		}
		var m map[string]any
		if json.Unmarshal(dec, &m) != nil {
			continue
		}
		out = append(out, m)
	}
	return out, true, nil
}

// drainStreamLogs ports cron:streams_logs: stream_log.log → log.stream, read +
// unlink unconditionally (at-most-once, as PHP), under the StreamsLogsCronJob lock.
func (a *Agent) drainStreamLogs() error {
	rel, ok := acquireCronLock(cronLockPath(a.cronsDir(), `XcVm\Cli\CronJobs\StreamsLogsCronJob`))
	if !ok {
		return nil
	}
	defer rel()
	logFile := filepath.Join(a.logsTmpDir(), "stream_log.log")
	lines, present, err := readLogLines(logFile)
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	var rows []map[string]any
	for _, m := range lines {
		sid := logInt(m["stream_id"])
		if sid == 0 {
			continue
		}
		rows = append(rows, map[string]any{
			"stream_id": sid,
			"server_id": a.Client.State.ServerID,
			"action":    logStr(m["action"]),
			"source":    logStr(m["source"]),
			"date":      logStr(m["time"]),
		})
	}
	if len(rows) > 0 {
		if err := spoolP1(a.SpoolDir, logEvents("stream", streamLogCols, rows)); err != nil && a.Logf != nil {
			a.Logf("cluster: streams_logs spool: %v", err)
		}
	}
	return os.Remove(logFile) // unconditional, as StreamsLogsCronJob
}

// lines_logs: the spool keys (LinesLogsCronJob::KEYS) map positionally onto the
// client columns (action→client_status, user_ip→ip, time→date), every value
// (string), missing→"".
var linesLogKeys = []string{"stream_id", "user_id", "action", "query_string", "user_agent", "user_ip", "extra_data", "time"}
var clientLogCols = []string{"stream_id", "user_id", "client_status", "query_string", "user_agent", "ip", "extra_data", "date"}

// drainLinesLogs ports cron:lines_logs: client_request.log, rename-claimed to
// .import (a pre-existing claim first, at-least-once), → log.client.
func (a *Agent) drainLinesLogs() error {
	rel, ok := acquireCronLock(cronLockPath(a.cronsDir(), `XcVm\Cli\CronJobs\LinesLogsCronJob`))
	if !ok {
		return nil
	}
	defer rel()
	logFile := filepath.Join(a.logsTmpDir(), "client_request.log")
	claimed := logFile + ".import"
	if _, err := os.Stat(claimed); err == nil {
		if err := a.drainClientFile(claimed); err != nil {
			return err
		}
	}
	if _, err := os.Stat(logFile); err == nil {
		if os.Rename(logFile, claimed) == nil {
			return a.drainClientFile(claimed)
		}
	}
	return nil
}

// drainClientFile spools one claimed client_request spool and unlinks it.
func (a *Agent) drainClientFile(path string) error {
	lines, present, err := readLogLines(path)
	if err != nil || !present {
		return err
	}
	rows := make([]map[string]any, 0, len(lines))
	for _, m := range lines {
		row := map[string]any{}
		for i, col := range clientLogCols {
			row[col] = logStr(m[linesLogKeys[i]])
		}
		rows = append(rows, row)
	}
	if len(rows) > 0 {
		if err := spoolP1(a.SpoolDir, logEvents("client", clientLogCols, rows)); err != nil && a.Logf != nil {
			a.Logf("cluster: lines_logs spool: %v", err)
		}
	}
	return os.Remove(path)
}

var activityLogCols = []string{"server_id", "proxy_id", "user_id", "isp", "external_device", "stream_id", "date_start", "user_agent", "user_ip", "date_end", "container", "geoip_country_code", "divergence", "hmac_id", "hmac_identifier"}

// activitySpool is the activity batch sink; a package var so a test can force a
// refusal (the keep-on-refusal path).
var activitySpool = func(a *Agent, rows []map[string]any) error {
	return spoolP1(a.SpoolDir, logEvents("activity", activityLogCols, rows))
}

// drainActivity ports cron:activity: the activity spool, rename-claimed, with
// keep-on-refusal. A pre-existing refused claim is drained first and, if refused
// again, left in place (re-claiming would overwrite it).
func (a *Agent) drainActivity() error {
	rel, ok := acquireCronLock(cronLockPath(a.cronsDir(), `XcVm\Cli\CronJobs\ActivityCronJob`))
	if !ok {
		return nil
	}
	defer rel()
	logFile := filepath.Join(a.logsTmpDir(), "activity")
	claimed := logFile + ".import"
	if _, err := os.Stat(claimed); err == nil {
		if !a.drainActivityFile(claimed) {
			return nil // refused claim stays; don't re-claim over it
		}
	}
	if _, err := os.Stat(logFile); err == nil {
		if os.Rename(logFile, claimed) == nil {
			a.drainActivityFile(claimed)
		}
	}
	return nil
}

// drainActivityFile spools a claimed activity file in IMPORT_BATCH/IMPORT_BYTES
// batches. Returns true when fully drained+unlinked; false when a batch was
// refused, leaving the file holding the refused batch and everything after it
// (the committed prefix dropped), as ActivityCronJob::keepFrom does.
func (a *Agent) drainActivityFile(path string) bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	lines := nonEmptyLines(string(raw))
	var batch []map[string]any
	nbytes := 0
	lastOK := -1 // index of the line that completed the last successful batch
	keep := func(from int) bool {
		rest := strings.Join(lines[from:], "\n")
		if len(lines[from:]) > 0 {
			rest += "\n"
		}
		tmp := path + ".rest"
		if os.WriteFile(tmp, []byte(rest), 0o640) == nil {
			_ = os.Rename(tmp, path)
		}
		return false
	}
	for i, ln := range lines {
		dec, err := base64.StdEncoding.DecodeString(ln)
		if err != nil {
			continue
		}
		var m map[string]any
		if json.Unmarshal(dec, &m) != nil || !activityValid(m) {
			continue
		}
		batch = append(batch, m)
		nbytes += len(ln)
		if len(batch) >= logChunk || nbytes >= 4194304 { // IMPORT_BATCH / IMPORT_BYTES
			if activitySpool(a, batch) != nil {
				return keep(lastOK + 1)
			}
			lastOK, batch, nbytes = i, nil, 0
		}
	}
	if len(batch) > 0 && activitySpool(a, batch) != nil {
		return keep(lastOK + 1)
	}
	_ = os.Remove(path)
	return true
}

// activityValid ports ActivityCronJob's row validation (PHP empty() semantics).
func activityValid(m map[string]any) bool {
	return !phpEmpty(m["server_id"]) &&
		(!phpEmpty(m["user_id"]) || !phpEmpty(m["hmac_id"])) &&
		!phpEmpty(m["stream_id"]) && !phpEmpty(m["user_ip"])
}

// phpEmpty mirrors PHP empty() for a JSON-decoded value.
func phpEmpty(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case string:
		return x == "" || x == "0"
	case float64:
		return x == 0
	case bool:
		return !x
	case []any:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	}
	return false
}

// nonEmptyLines splits on newlines and drops blank/whitespace-only lines.
func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

var streamErrorCols = []string{"stream_id", "server_id", "date", "error"}
var panelErrorCols = []string{"server_id", "type", "log_message", "log_extra", "line", "date", "file", "env", "version", "unique"}

// errIgnorePhrases is ErrorsCronJob's ignore list (lowercase; matched case-insensitively).
var errIgnorePhrases = []string{
	"the user-agent option is deprecated", "last message repeated", "deprecated",
	"packets poorly interleaved", "invalid timestamps", "timescale not set",
	"frame size not set", "non-monotonous dts in output stream", "invalid dts",
	"no trailing crlf", "failed to parse extradata", "truncated", "missing picture",
	"non-existing pps", "clipping", "out of range",
	"cannot use rename on non file protocol", "end of file", "stream ends prematurely",
}

// errPanelSpool is the panel_error batch sink; a package var so a test can force
// a refusal (the keep-on-failure path).
var errPanelSpool = func(a *Agent, rows []map[string]any) error {
	return spoolP1(a.SpoolDir, logEvents("panel_error", panelErrorCols, rows))
}

// drainErrors ports cron:errors: stream .errors files under STREAMS_PATH (→
// log.stream_error, gated by stream_logs_save; the files are unlinked
// regardless) and error_log.log (→ log.panel_error, deduped; unlinked only on a
// successful spool), under the ErrorsCronJob lock.
func (a *Agent) drainErrors() error {
	rel, ok := acquireCronLock(cronLockPath(a.cronsDir(), `XcVm\Cli\CronJobs\ErrorsCronJob`))
	if !ok {
		return nil
	}
	defer rel()
	save := !phpEmpty(a.replicaSettings()["stream_logs_save"])

	var serows []map[string]any
	if dir := a.node.streams; dir != "" {
		ents, _ := os.ReadDir(dir)
		for _, e := range ents {
			if e.IsDir() || filepath.Ext(e.Name()) != ".errors" {
				continue
			}
			sid := leadingInt(strings.TrimSuffix(e.Name(), filepath.Ext(e.Name())))
			if sid <= 0 {
				continue
			}
			full := filepath.Join(dir, e.Name())
			b, _ := os.ReadFile(full)
			for _, ln := range splitErrLines(string(b)) {
				ln = strings.TrimSpace(ln)
				if ln == "" || errIgnored(ln) {
					continue
				}
				if save {
					serows = append(serows, map[string]any{"stream_id": int64(sid), "server_id": a.Client.State.ServerID, "date": time.Now().Unix(), "error": ln})
				}
			}
			_ = os.Remove(full) // unconditional, as ErrorsCronJob
		}
	}
	if save && len(serows) > 0 {
		if err := spoolP1(a.SpoolDir, logEvents("stream_error", streamErrorCols, serows)); err != nil && a.Logf != nil {
			a.Logf("cluster: stream_error spool: %v", err)
		}
	}

	logFile := filepath.Join(a.logsTmpDir(), "error_log.log")
	if rows := a.parsePanelLog(logFile); len(rows) > 0 {
		if errPanelSpool(a, rows) == nil {
			_ = os.Remove(logFile)
		}
	}
	return nil
}

// parsePanelLog ports ErrorsCronJob::parseLog: dedup by md5(server_id.type.
// message.extra.file.line), skip transient-mysql messages, sqlValue empties→null.
func (a *Agent) parsePanelLog(path string) []map[string]any {
	lines, present, err := readLogLines(path)
	if err != nil || !present {
		return nil
	}
	seen := map[string]bool{}
	var rows []map[string]any
	for _, m := range lines {
		msg := coalesceStr(m["log_message"], m["message"])
		extra := coalesceStr(m["log_extra"], m["extra"])
		typ := strOr(m["type"], "unknown")
		line := intOrDefault(m["line"], 0)
		date := intOrDefault(m["time"], time.Now().Unix())
		file := strOr(m["file"], "")
		env := strOr(m["env"], "cli") // php_sapi_name() of the draining process
		version := strOr(m["version"], "")
		sid := a.Client.State.ServerID
		if v, ok := m["server_id"]; ok && isNumeric(v) {
			sid = logInt(v)
		}
		lc := strings.ToLower(msg)
		if strings.Contains(lc, "server has gone away") || strings.Contains(lc, "socket error on read socket") || strings.Contains(lc, "connection lost") {
			continue
		}
		hash := md5hex(fmt.Sprintf("%d%s%s%s%s%d", sid, typ, msg, extra, file, line))
		if seen[hash] {
			continue
		}
		seen[hash] = true
		rows = append(rows, map[string]any{
			"server_id":   sid,
			"type":        emptyNull(typ),
			"log_message": emptyNull(msg),
			"log_extra":   emptyNull(extra),
			"line":        line,
			"date":        date,
			"file":        emptyNull(file),
			"env":         emptyNull(env),
			"version":     emptyNull(version),
			"unique":      hash,
		})
	}
	return rows
}

func errIgnored(s string) bool {
	lc := strings.ToLower(s)
	for _, p := range errIgnorePhrases {
		if strings.Contains(lc, p) {
			return true
		}
	}
	return false
}

func splitErrLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return strings.Split(s, "\n")
}

// leadingInt ports PHP (int)"<digits>..." for a filename stem.
func leadingInt(s string) int {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	n, _ := strconv.Atoi(s[:i])
	return n
}

func isNumeric(v any) bool {
	switch x := v.(type) {
	case float64:
		return true
	case string:
		_, err := strconv.ParseFloat(x, 64)
		return err == nil
	}
	return false
}

func coalesceStr(a, b any) string {
	if a != nil {
		return logStr(a)
	}
	if b != nil {
		return logStr(b)
	}
	return ""
}

func strOr(v any, def string) string {
	if v == nil {
		return def
	}
	return logStr(v)
}

func intOrDefault(v any, def int64) int64 {
	if v == nil {
		return def
	}
	return logInt(v)
}

func emptyNull(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// logInt ports PHP intval() for a JSON value.
func logInt(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

// logStr ports PHP (string) cast for a JSON value (number→decimal, null/absent→"").
func logStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "1"
		}
	}
	return ""
}
