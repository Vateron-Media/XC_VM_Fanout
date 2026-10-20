package clusteragent

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// crondrain ports the node-local log-drain crons into the agent: read a
// node-local base64(json) log file, build the LogSink rows, and spool the same
// log.<kind> events LogSink::spool emits on the p1 lane. MAIN's EventIngest is
// unchanged. Each drain holds the same CronTrait lock (cronlock.go) PHP uses, so
// a still-scheduled PHP cron and the agent never drain one file at once.

// Node tmp-subdir helpers, derived from TmpRoot (TMP_PATH), per ConstantsInitializer.
func (a *Agent) logsTmpDir() string      { return filepath.Join(a.TmpRoot, "logs") }
func (a *Agent) cronsDir() string        { return filepath.Join(a.TmpRoot, "crons") }
func (a *Agent) floodTmpDir() string     { return filepath.Join(a.TmpRoot, "flood") }
func (a *Agent) signalsTmpDir() string   { return filepath.Join(a.TmpRoot, "signals") }
func (a *Agent) divergenceTmpDir() string { return filepath.Join(a.TmpRoot, "divergence") }
func (a *Agent) ministraTmpDir() string  { return filepath.Join(a.TmpRoot, "ministra") }

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
