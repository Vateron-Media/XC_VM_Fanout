package agentcmd

import (
	"os"
	"path/filepath"
	"testing"
)

// A serve-only node's Go agent materialises the whole replica itself, so
// console.php cluster:apply would only fill PHP caches a Go-only node never
// reads: its Apply hook must be nil. A normal node keeps ApplyViaPHP.
func TestApplyFuncServeOnlyIsNil(t *testing.T) {
	if applyFunc(true, "php", "console.php") != nil {
		t.Fatal("serve-only: Apply must be nil (no console.php cluster:apply)")
	}
	if applyFunc(false, "php", "console.php") == nil {
		t.Fatal("non-serve-only: Apply must keep ApplyViaPHP")
	}
}

// selfHealRepoFor enables the Go binary self-heal only on a serve-only node that
// is PHP-free (no console.php): while PHP is present it owns binary updates
// (fanout_binary), so the Go self-heal must stay dormant to avoid racing it.
func TestSelfHealRepoFor(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "console.php")
	if err := os.WriteFile(present, []byte("<?php"), 0o644); err != nil {
		t.Fatal(err)
	}
	absent := filepath.Join(dir, "gone.php")
	if got := selfHealRepoFor(true, present, "owner/repo"); got != "" {
		t.Fatalf("serve-only but PHP present: got %q, want '' (dormant while PHP updates)", got)
	}
	if got := selfHealRepoFor(false, absent, "owner/repo"); got != "" {
		t.Fatalf("not serve-only: got %q, want ''", got)
	}
	if got := selfHealRepoFor(true, absent, "owner/repo"); got != "owner/repo" {
		t.Fatalf("serve-only + PHP-free: got %q, want owner/repo", got)
	}
}

// ffmpegRepoFor gates the Go ffmpeg refresh the same way selfHealRepoFor gates
// self-heal: on only when the node is serve-only AND PHP-free (console.php gone).
// While PHP is present, cron:root_signals runs console.php ffmpeg, so the Go
// refresh stays dormant and the two never race to install a build.
func TestFfmpegRepoFor(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "console.php")
	if err := os.WriteFile(present, []byte("<?php"), 0o644); err != nil {
		t.Fatal(err)
	}
	absent := filepath.Join(dir, "gone.php")
	if got := ffmpegRepoFor(true, present, "owner/repo"); got != "" {
		t.Fatalf("serve-only but PHP present: got %q, want '' (dormant while PHP refreshes)", got)
	}
	if got := ffmpegRepoFor(false, absent, "owner/repo"); got != "" {
		t.Fatalf("not serve-only: got %q, want ''", got)
	}
	if got := ffmpegRepoFor(true, absent, "owner/repo"); got != "owner/repo" {
		t.Fatalf("serve-only + PHP-free: got %q, want owner/repo", got)
	}
}

// certbotWebrootFor gates the certbot loop like ffmpegRepoFor: on only for a
// serve-only node that is PHP-free, so it never races cron:certbot.
func TestCertbotWebrootFor(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "console.php")
	if err := os.WriteFile(present, []byte("<?php"), 0o644); err != nil {
		t.Fatal(err)
	}
	absent := filepath.Join(dir, "gone.php")
	if got := certbotWebrootFor(true, present, "/wr"); got != "" {
		t.Fatalf("serve-only but PHP present: got %q, want ''", got)
	}
	if got := certbotWebrootFor(false, absent, "/wr"); got != "" {
		t.Fatalf("not serve-only: got %q, want ''", got)
	}
	if got := certbotWebrootFor(true, absent, "/wr"); got != "/wr" {
		t.Fatalf("serve-only + PHP-free: got %q, want /wr", got)
	}
}
