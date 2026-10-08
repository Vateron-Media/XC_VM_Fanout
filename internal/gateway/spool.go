package gateway

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"
)

// spoolStaleAfter is EventSpool::STALE_AFTER: an agent that has not touched
// flows.json for this long is not draining its spool.
const spoolStaleAfter = 120 * time.Second

// agentAlive is EventSpool::agentAlive.
func agentAlive(flows string, now time.Time) bool {
	fi, err := os.Stat(flows)
	return err == nil && now.Sub(fi.ModTime()) <= spoolStaleAfter
}

// connLimit is the conn.limit event StreamAuth::validateConnections spools on
// each playlist request of a line with a limit, for MAIN to enforce it.
func connLimit(r *Refresh, clientIP string) map[string]any {
	d := map[string]any{"uuid": r.UUID, "ip": clientIP, "user_agent": r.UserAgent}
	if r.HMAC != "" {
		hmac, _ := jsonInt(json.RawMessage(r.HMAC))
		d["hmac_id"], d["hmac_identifier"], d["max_connections"] = hmac, r.Identifier, r.MaxConns
	} else {
		d["user_id"] = r.UserID
	}
	return d
}

// spoolAppend is EventSpool::append of one event to a lane: one file,
// <hrtime>-<pid>-<rand>.ndjson, written aside and renamed in, so the agent
// (which sends the lane in name order) never reads half of it. hrtime is
// CLOCK_MONOTONIC, PHP's hrtime(true), so the gateway's files sort among the
// panel's by when they were written.
func spoolAppend(dir, lane, typ string, d map[string]any, now time.Time) error {
	laneDir := filepath.Join(dir, lane)
	if err := os.MkdirAll(laneDir, 0o750); err != nil {
		return err
	}
	var line bytes.Buffer
	enc := json.NewEncoder(&line)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(map[string]any{"type": typ, "t": now.UnixMilli(), "d": d}); err != nil {
		return err
	}
	var rnd [2]byte
	_, _ = rand.Read(rnd[:])
	name := fmt.Sprintf("%019d-%d-%04x.ndjson", monotonicNS(), os.Getpid(), binary.BigEndian.Uint16(rnd[:]))
	tmp, err := os.CreateTemp(laneDir, ".gw-*.tmp")
	if err != nil {
		return err
	}
	_, werr := tmp.Write(line.Bytes())
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("spool write: %v %v", werr, cerr)
	}
	_ = os.Chmod(tmp.Name(), 0o640)
	if err := os.Rename(tmp.Name(), filepath.Join(laneDir, name)); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return nil
}

// monotonicNS is CLOCK_MONOTONIC in nanoseconds (PHP's hrtime(true) on Linux).
func monotonicNS() int64 {
	var ts syscall.Timespec
	const clockMonotonic = 1
	if _, _, e := syscall.Syscall(syscall.SYS_CLOCK_GETTIME, clockMonotonic, uintptr(unsafe.Pointer(&ts)), 0); e != 0 {
		return time.Now().UnixNano()
	}
	return ts.Nano()
}
