package clusteragent

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
)

// The connect audit (ADR 0004, "Booting from the replica, and the settings
// misses (Phase 7, seventh increment)" and "The mode-2 refusal and the
// connect audit (Phase 7, eighth increment)"): the node's PHP keeps
// audit.json beside flows.json and local.json. At every heartbeat the agent
// reads it and, when it is at most MaxAuditBytes and a JSON object, sends it
// as the payload's `audit`, parsed and re-encoded like telemetry.local, and
// otherwise sends none (MAIN keeps what it has). The agent does not
// interpret it: MAIN checks it and ignores members it does not know.

// MaxAuditBytes bounds audit.json, measured on the file's bytes.
const MaxAuditBytes = 16384

// auditPath is audit.json beside the agent's state file.
func (a *Agent) auditPath() string {
	if a.Client == nil || a.Client.State == nil || a.Client.State.path == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(a.Client.State.path), "audit.json")
}

// readAudit returns audit.json's object, or nil when there is none to send.
func (a *Agent) readAudit() map[string]any {
	path := a.auditPath()
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxAuditBytes+1))
	if err != nil || len(b) > MaxAuditBytes {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber() // counts pass through exactly
	var doc map[string]any
	if dec.Decode(&doc) != nil || doc == nil {
		return nil
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil // trailing data: not one JSON object
	}
	return doc
}
