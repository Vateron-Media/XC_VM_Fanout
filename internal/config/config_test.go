package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestLoadMissingSelfCreates: an absent file is written with the defaults and
// reported as (defaults, wrote=true).
func TestLoadMissingSelfCreates(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	v, wrote, err := Load(p)
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if !wrote {
		t.Fatal("missing file should have been self-created (wrote=false)")
	}
	if v != Defaults() {
		t.Fatalf("self-create returned non-defaults: %+v", v)
	}
	// The file must now exist and reparse to the same values.
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("config not written: %v", err)
	}
	v2, wrote2, err := Load(p)
	if err != nil || wrote2 {
		t.Fatalf("second load should be clean no-op: wrote=%v err=%v", wrote2, err)
	}
	if v2 != v {
		t.Fatalf("reparse mismatch: %+v vs %+v", v2, v)
	}
}

// TestLoadPartialBackfills: a file missing keys keeps its present values, fills
// the rest from defaults, reports wrote=true, and the rewritten file now carries
// every key (so an older panel can never make the daemon throw on a version skew).
func TestLoadPartialBackfills(t *testing.T) {
	p := write(t, `{"prebuffer_max_sec":3,"hls_window":3}`)
	v, wrote, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if !wrote {
		t.Fatal("partial file should have been backfilled (wrote=false)")
	}
	if v.PrebufferMaxSec != 3 || v.HLSWindow != 3 {
		t.Fatalf("present keys not kept: %+v", v)
	}
	d := Defaults()
	if v.HLSTargetSec != d.HLSTargetSec || v.GraceSec != d.GraceSec || v.ChunkBytes != d.ChunkBytes || v.SourceInsecure != d.SourceInsecure {
		t.Fatalf("missing keys not defaulted: %+v", v)
	}
	// The file now has all eight keys.
	b, _ := os.ReadFile(p)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"prebuffer_max_sec", "hls_target_sec", "hls_window", "grace_sec", "write_timeout_sec", "chunk_bytes", "max_gop_bytes", "source_insecure"} {
		if _, ok := raw[k]; !ok {
			t.Fatalf("backfilled file missing key %q", k)
		}
	}
}

// TestLoadCompleteNoRewrite: a file that already carries every key is not
// rewritten (wrote=false) — the daemon does not fight a panel that owns the file.
func TestLoadCompleteNoRewrite(t *testing.T) {
	if err := Save(filepath.Join(t.TempDir(), "x"), Defaults()); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "config.json")
	if err := Save(p, Defaults()); err != nil {
		t.Fatal(err)
	}
	_, wrote, err := Load(p)
	if err != nil || wrote {
		t.Fatalf("complete file should be a no-op: wrote=%v err=%v", wrote, err)
	}
}

func TestLoadClampsOutOfRange(t *testing.T) {
	p := write(t, `{"prebuffer_max_sec":100000,"hls_window":0,"hls_target_sec":0.1,"grace_sec":0,"write_timeout_sec":99999,"chunk_bytes":1,"max_gop_bytes":1}`)
	v, _, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if v.PrebufferMaxSec != 120 || v.HLSWindow != 1 || v.HLSTargetSec != 1 || v.GraceSec != 1 || v.WriteTimeoutSec != 600 || v.ChunkBytes != 188 || v.MaxGOPBytes != 188 {
		t.Fatalf("clamp failed: %+v", v)
	}
}

func TestLoadZeroPrebufferIsValid(t *testing.T) {
	// 0 = "current GOP only" — a legitimate value, not clamped up.
	v, _, err := Load(write(t, `{"prebuffer_max_sec":0}`))
	if err != nil || v.PrebufferMaxSec != 0 {
		t.Fatalf("prebuffer 0 rejected: v=%+v err=%v", v, err)
	}
}

// TestLoadMalformedKeepsDefaultsNoOverwrite: a bad file yields defaults + an
// error and is left on disk untouched (never clobbered mid-edit).
func TestLoadMalformedKeepsDefaultsNoOverwrite(t *testing.T) {
	body := `{not json`
	p := write(t, body)
	v, wrote, err := Load(p)
	if err == nil {
		t.Fatal("malformed file should error")
	}
	if wrote {
		t.Fatal("malformed file must not be overwritten")
	}
	if v != Defaults() {
		t.Fatalf("malformed file changed defaults: %+v", v)
	}
	if b, _ := os.ReadFile(p); string(b) != body {
		t.Fatalf("malformed file was rewritten: %q", b)
	}
}
