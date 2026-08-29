// Package config is the panel↔daemon tuning bridge, and the daemon's PRIMARY
// source of operator tuning (the CLI no longer carries these knobs — the panel
// never set them). An admin edits the values in the panel; the panel writes them
// to a small JSON file; the daemon polls that file and applies them at runtime —
// no restart, no viewer drop.
//
// The file is self-maintaining:
//
//   - Absent → the daemon writes it with the built-in defaults (defaults.Cfg*),
//     so a fresh node starts from a real, editable file instead of nothing.
//   - Missing a key → the daemon backfills that key with its default AND rewrites
//     the file. So the on-disk file always carries the daemon's full current
//     schema: an OLDER panel that writes only a subset never makes the daemon
//     throw, and a key a NEWER daemon added appears in the file (with its default)
//     for the panel to pick up.
//   - Malformed → the daemon keeps the defaults and does NOT overwrite the file
//     (it may be a torn mid-write or a hand-edit in progress); the next poll
//     retries. Streaming never breaks on a bad config.
package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
)

// Values is the resolved, validated tuning the daemon applies. The json tags are
// the on-disk schema (also what the panel writes).
type Values struct {
	PrebufferMaxSec    int     `json:"prebuffer_max_sec"`
	ClientPrebufferSec int     `json:"client_prebuffer_sec"`
	HLSTargetSec       float64 `json:"hls_target_sec"`
	HLSWindow          int     `json:"hls_window"`
	GraceSec           int     `json:"grace_sec"`
	WriteTimeoutSec    int     `json:"write_timeout_sec"`
	ChunkBytes         int     `json:"chunk_bytes"`
	MaxGOPBytes        int     `json:"max_gop_bytes"`
	SourceInsecure     bool    `json:"source_insecure"`
	IdleBufferSec      int     `json:"idle_buffer_sec"`
	IdleBufferGraceSec int     `json:"idle_buffer_grace_sec"`
	IdleHlsWindow      int     `json:"idle_hls_window"`
}

// Defaults is the built-in fallback (see defaults.Cfg*): what the daemon writes
// for a missing file and backfills for a missing key.
func Defaults() Values {
	return Values{
		PrebufferMaxSec:    defaults.CfgPrebufferMaxSec,
		ClientPrebufferSec: defaults.CfgClientPrebufferSec,
		HLSTargetSec:       defaults.CfgHLSTargetSec,
		HLSWindow:          defaults.CfgHLSWindow,
		GraceSec:           defaults.CfgGraceSec,
		WriteTimeoutSec:    defaults.CfgWriteTimeoutSec,
		ChunkBytes:         defaults.CfgChunkBytes,
		MaxGOPBytes:        defaults.CfgMaxGOPBytes,
		SourceInsecure:     defaults.CfgSourceInsecure,
		IdleBufferSec:      defaults.CfgIdleBufferSec,
		IdleBufferGraceSec: defaults.CfgIdleBufferGraceSec,
		IdleHlsWindow:      defaults.CfgIdleHlsWindow,
	}
}

// file mirrors the JSON for READING. Every field is a pointer so an absent key
// stays nil — that is how a missing key (to backfill) is told apart from one
// explicitly set to a zero value (e.g. prebuffer 0 = "current GOP only").
type file struct {
	PrebufferMaxSec    *int     `json:"prebuffer_max_sec"`
	ClientPrebufferSec *int     `json:"client_prebuffer_sec"`
	HLSTargetSec       *float64 `json:"hls_target_sec"`
	HLSWindow          *int     `json:"hls_window"`
	GraceSec           *int     `json:"grace_sec"`
	WriteTimeoutSec    *int     `json:"write_timeout_sec"`
	ChunkBytes         *int     `json:"chunk_bytes"`
	MaxGOPBytes        *int     `json:"max_gop_bytes"`
	SourceInsecure     *bool    `json:"source_insecure"`
	IdleBufferSec      *int     `json:"idle_buffer_sec"`
	IdleBufferGraceSec *int     `json:"idle_buffer_grace_sec"`
	IdleHlsWindow      *int     `json:"idle_hls_window"`
}

// Load reads path, returns the resolved (defaults-overlaid, clamped) values, and
// whether it (re)wrote the file — self-creating a missing file and backfilling a
// partial one. See the package doc for the full contract. A malformed file
// returns the defaults and the parse error, leaving the file untouched.
func Load(path string) (Values, bool, error) {
	d := Defaults()

	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			if werr := Save(path, d); werr != nil {
				return d, false, fmt.Errorf("create %s: %w", path, werr)
			}
			return d, true, nil
		}
		return d, false, err
	}

	var f file
	if err := json.Unmarshal(b, &f); err != nil {
		return d, false, fmt.Errorf("parse %s: %w", path, err)
	}

	v := d
	missing := false
	overlay := func(present bool) {
		if !present {
			missing = true
		}
	}
	if f.PrebufferMaxSec != nil {
		v.PrebufferMaxSec = *f.PrebufferMaxSec
	}
	overlay(f.PrebufferMaxSec != nil)
	if f.ClientPrebufferSec != nil {
		v.ClientPrebufferSec = *f.ClientPrebufferSec
	}
	overlay(f.ClientPrebufferSec != nil)
	if f.HLSTargetSec != nil {
		v.HLSTargetSec = *f.HLSTargetSec
	}
	overlay(f.HLSTargetSec != nil)
	if f.HLSWindow != nil {
		v.HLSWindow = *f.HLSWindow
	}
	overlay(f.HLSWindow != nil)
	if f.GraceSec != nil {
		v.GraceSec = *f.GraceSec
	}
	overlay(f.GraceSec != nil)
	if f.WriteTimeoutSec != nil {
		v.WriteTimeoutSec = *f.WriteTimeoutSec
	}
	overlay(f.WriteTimeoutSec != nil)
	if f.ChunkBytes != nil {
		v.ChunkBytes = *f.ChunkBytes
	}
	overlay(f.ChunkBytes != nil)
	if f.MaxGOPBytes != nil {
		v.MaxGOPBytes = *f.MaxGOPBytes
	}
	overlay(f.MaxGOPBytes != nil)
	if f.SourceInsecure != nil {
		v.SourceInsecure = *f.SourceInsecure
	}
	overlay(f.SourceInsecure != nil)
	if f.IdleBufferSec != nil {
		v.IdleBufferSec = *f.IdleBufferSec
	}
	overlay(f.IdleBufferSec != nil)
	if f.IdleBufferGraceSec != nil {
		v.IdleBufferGraceSec = *f.IdleBufferGraceSec
	}
	overlay(f.IdleBufferGraceSec != nil)
	if f.IdleHlsWindow != nil {
		v.IdleHlsWindow = *f.IdleHlsWindow
	}
	overlay(f.IdleHlsWindow != nil)

	v.clamp()

	wrote := false
	if missing {
		// Backfill the omitted keys so the file carries the full schema.
		if werr := Save(path, v); werr == nil {
			wrote = true
		}
	}
	return v, wrote, nil
}

// Save atomically writes v to path (temp file + rename) so a reader — the daemon
// polling, or a concurrent panel write — never sees a half-written file.
func Save(path string, v Values) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// clamp keeps every value inside a sane operating range so a typo in the panel
// (or a hand-edited file) can never push the daemon into a pathological state.
func (v *Values) clamp() {
	v.PrebufferMaxSec = clampInt(v.PrebufferMaxSec, 0, 120)
	v.ClientPrebufferSec = clampInt(v.ClientPrebufferSec, 0, 120)
	v.HLSWindow = clampInt(v.HLSWindow, 1, 20)
	v.GraceSec = clampInt(v.GraceSec, 1, 3600)
	v.WriteTimeoutSec = clampInt(v.WriteTimeoutSec, 1, 600)
	v.ChunkBytes = clampInt(v.ChunkBytes, 188, 4<<20)
	v.MaxGOPBytes = clampInt(v.MaxGOPBytes, 188, 256<<20)
	v.IdleBufferSec = clampInt(v.IdleBufferSec, 0, 60)
	v.IdleBufferGraceSec = clampInt(v.IdleBufferGraceSec, 0, 3600)
	v.IdleHlsWindow = clampInt(v.IdleHlsWindow, 0, 20)
	if v.HLSTargetSec < 1 {
		v.HLSTargetSec = 1
	} else if v.HLSTargetSec > 30 {
		v.HLSTargetSec = 30
	}
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
