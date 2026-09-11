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
	"strings"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
)

// Values is the resolved, validated tuning the daemon applies. The json tags are
// the on-disk schema (also what the panel writes).
type Values struct {
	PrebufferMaxSec int     `json:"prebuffer_max_sec"` // the buffer/ring size (ms of TS history), drives TS + HLS depth
	HLSTargetSec    float64 `json:"hls_target_sec"`
	HLSWindow       int     `json:"hls_window"` // HLS segments listed (display cap), not a ring driver
	GraceSec        int     `json:"grace_sec"`
	WriteTimeoutSec int     `json:"write_timeout_sec"`
	ChunkBytes      int     `json:"chunk_bytes"`
	MaxGOPBytes     int     `json:"max_gop_bytes"`
	SourceInsecure  bool    `json:"source_insecure"`
	// DefaultPrebufferSec is only a fallback per-viewer join burst for a request
	// that carries no ?prebuffer=; the panel is authoritative (client vs restreamer)
	// and passes the value per-request, which the daemon honors as-is (including 0).
	DefaultPrebufferSec int `json:"default_prebuffer_sec"`
	// IdleBufferGraceSec is the no-viewer window before the ring collapses (0 = gate
	// off); IdleBufferRatio is the fraction of the buffer kept while unwatched. HLS
	// keeps being cut from the reduced ring, so it stays openable.
	IdleBufferGraceSec int     `json:"idle_buffer_grace_sec"`
	IdleBufferRatio    float64 `json:"idle_buffer_ratio"`
	// ViewerIdleTimeoutSec drops a live-TS viewer that has received no data for
	// this long (0 = never). It is what bounds a ghost connection on an off-air
	// stream: the per-write deadline can only fire while there are bytes to write.
	ViewerIdleTimeoutSec int `json:"viewer_idle_timeout_sec"`
	// MemLimitMB is an explicit ceiling (MiB) for the Go soft memory limit;
	// 0 = derive it from the cgroup limit, else a share of the box's RAM.
	MemLimitMB int `json:"mem_limit_mb"`
	// SourceBackend is how a non-mp2t source becomes MPEG-TS: "auto" (native
	// where possible, ffmpeg otherwise), "ffmpeg" (always), or "native" (no
	// fallback — testing only).
	SourceBackend string `json:"source_backend"`
	// Supervise lets this node run and watch stream encoders on the panel's
	// behalf, replacing its per-stream PHP watchdog (docs/adr/0002). Off by
	// default: with it off the /monitor endpoints report that this node does not
	// do it, and the panel keeps running its own monitors. Enabling it changes
	// nothing on its own — a stream is only supervised once the panel hands it
	// over — so this is the node-level half of a two-sided opt-in.
	Supervise bool `json:"supervise"`
}

// Defaults is the built-in fallback (see defaults.Cfg*): what the daemon writes
// for a missing file and backfills for a missing key.
func Defaults() Values {
	return Values{
		PrebufferMaxSec:     defaults.CfgPrebufferMaxSec,
		HLSTargetSec:        defaults.CfgHLSTargetSec,
		HLSWindow:           defaults.CfgHLSWindow,
		GraceSec:            defaults.CfgGraceSec,
		WriteTimeoutSec:     defaults.CfgWriteTimeoutSec,
		ChunkBytes:          defaults.CfgChunkBytes,
		MaxGOPBytes:         defaults.CfgMaxGOPBytes,
		SourceInsecure:      defaults.CfgSourceInsecure,
		DefaultPrebufferSec: defaults.CfgDefaultPrebufferSec,
		IdleBufferGraceSec:  defaults.CfgIdleBufferGraceSec,
		IdleBufferRatio:     defaults.CfgIdleBufferRatio,

		ViewerIdleTimeoutSec: defaults.CfgViewerIdleTimeoutSec,
		MemLimitMB:           defaults.CfgMemLimitMB,
		SourceBackend:        defaults.CfgSourceBackend,
		Supervise:            defaults.CfgSupervise,
	}
}

// file mirrors the JSON for READING. Every field is a pointer so an absent key
// stays nil — that is how a missing key (to backfill) is told apart from one
// explicitly set to a zero value (e.g. prebuffer 0 = "current GOP only").
type file struct {
	PrebufferMaxSec     *int     `json:"prebuffer_max_sec"`
	HLSTargetSec        *float64 `json:"hls_target_sec"`
	HLSWindow           *int     `json:"hls_window"`
	GraceSec            *int     `json:"grace_sec"`
	WriteTimeoutSec     *int     `json:"write_timeout_sec"`
	ChunkBytes          *int     `json:"chunk_bytes"`
	MaxGOPBytes         *int     `json:"max_gop_bytes"`
	SourceInsecure      *bool    `json:"source_insecure"`
	DefaultPrebufferSec *int     `json:"default_prebuffer_sec"`
	IdleBufferGraceSec  *int     `json:"idle_buffer_grace_sec"`
	IdleBufferRatio     *float64 `json:"idle_buffer_ratio"`

	ViewerIdleTimeoutSec *int    `json:"viewer_idle_timeout_sec"`
	MemLimitMB           *int    `json:"mem_limit_mb"`
	SourceBackend        *string `json:"source_backend"`
	Supervise            *bool   `json:"supervise"`
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
	if f.DefaultPrebufferSec != nil {
		v.DefaultPrebufferSec = *f.DefaultPrebufferSec
	}
	overlay(f.DefaultPrebufferSec != nil)
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
	if f.IdleBufferGraceSec != nil {
		v.IdleBufferGraceSec = *f.IdleBufferGraceSec
	}
	overlay(f.IdleBufferGraceSec != nil)
	if f.IdleBufferRatio != nil {
		v.IdleBufferRatio = *f.IdleBufferRatio
	}
	overlay(f.IdleBufferRatio != nil)
	if f.ViewerIdleTimeoutSec != nil {
		v.ViewerIdleTimeoutSec = *f.ViewerIdleTimeoutSec
	}
	overlay(f.ViewerIdleTimeoutSec != nil)
	if f.MemLimitMB != nil {
		v.MemLimitMB = *f.MemLimitMB
	}
	overlay(f.MemLimitMB != nil)
	if f.SourceBackend != nil {
		v.SourceBackend = *f.SourceBackend
	}
	overlay(f.SourceBackend != nil)
	if f.Supervise != nil {
		v.Supervise = *f.Supervise
	}
	overlay(f.Supervise != nil)

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
	v.DefaultPrebufferSec = clampInt(v.DefaultPrebufferSec, 0, 120)
	v.HLSWindow = clampInt(v.HLSWindow, 1, 20)
	v.GraceSec = clampInt(v.GraceSec, 1, 3600)
	v.WriteTimeoutSec = clampInt(v.WriteTimeoutSec, 1, 600)
	v.ChunkBytes = clampInt(v.ChunkBytes, 188, 4<<20)
	v.MaxGOPBytes = clampInt(v.MaxGOPBytes, 188, 256<<20)
	v.IdleBufferGraceSec = clampInt(v.IdleBufferGraceSec, 0, 3600)
	// 0 disables the viewer idle-drop; anything above 0 is floored at 5 s so a
	// typo cannot start culling healthy viewers between two chunks.
	if v.ViewerIdleTimeoutSec != 0 {
		v.ViewerIdleTimeoutSec = clampInt(v.ViewerIdleTimeoutSec, 5, 3600)
	}
	v.MemLimitMB = clampInt(v.MemLimitMB, 0, 1<<20)
	// An unknown backend falls back to the safe default rather than throwing:
	// a typo in the panel must never stop streams from being pulled.
	//
	// Case and surrounding space are normalised before that judgement. "Native"
	// and " native" are not typos an operator can see — they look right in the
	// file and in the panel — but an exact-match test silently demoted them to
	// the default, which then behaved almost but not quite like what was asked
	// for. That is the worst kind of wrong: no error, no log, and a setting that
	// reads as honoured.
	v.SourceBackend = strings.ToLower(strings.TrimSpace(v.SourceBackend))
	switch v.SourceBackend {
	case "auto", "ffmpeg", "native":
	default:
		v.SourceBackend = defaults.CfgSourceBackend
	}
	if v.IdleBufferRatio < 0.1 {
		v.IdleBufferRatio = 0.1
	} else if v.IdleBufferRatio > 1 {
		v.IdleBufferRatio = 1
	}
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
