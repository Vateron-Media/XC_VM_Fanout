package clusteragent

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// writeFfmpegTarGz builds an archive holding exactly ffmpeg, ffprobe and
// BUILD_INFO (plus a decoy the extractor must ignore), the two binaries being
// the given shell scripts.
func writeFfmpegTarGz(t *testing.T, path, ffmpeg, ffprobe string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()
	for _, e := range []struct {
		name, body string
		mode       int64
	}{
		{"ffmpeg", ffmpeg, 0o755},
		{"ffprobe", ffprobe, 0o755},
		{"BUILD_INFO", "label=7.1\n", 0o644},
		{"README", "ignore me\n", 0o644},
	} {
		if err := tw.WriteHeader(&tar.Header{Name: e.name, Mode: e.mode, Size: int64(len(e.body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(e.body)); err != nil {
			t.Fatal(err)
		}
	}
}

// placeFfmpeg extracts the three build files aside, proves both binaries run and
// the ffmpeg banner names the label, then swaps the build into <binDir>/<label>/
// atomically — keeping the installed build on any failure (FfmpegBuildsCommand::place).
func TestPlaceFfmpegAtomicAndProves(t *testing.T) {
	dir := t.TempDir() + "/"
	good := filepath.Join(t.TempDir(), "good.tar.gz")
	writeFfmpegTarGz(t, good, "#!/bin/sh\necho 'ffmpeg version 7.1-stub built'\n", "#!/bin/sh\necho 'ffprobe version 7.1-stub'\n")
	if err := placeFfmpeg(good, dir, "7.1"); err != nil {
		t.Fatalf("placeFfmpeg (good): %v", err)
	}
	if _, err := os.Stat(dir + "7.1/ffmpeg"); err != nil {
		t.Fatalf("build not installed: %v", err)
	}
	if _, err := os.Stat(dir + "7.1/BUILD_INFO"); err != nil {
		t.Fatalf("BUILD_INFO not installed: %v", err)
	}
	if _, err := os.Stat(dir + "7.1/README"); err == nil {
		t.Fatal("only ffmpeg/ffprobe/BUILD_INFO must be extracted, not README")
	}

	// A build whose ffmpeg exits non-zero must be rejected, the good one kept.
	bad := filepath.Join(t.TempDir(), "bad.tar.gz")
	writeFfmpegTarGz(t, bad, "#!/bin/sh\nexit 1\n", "#!/bin/sh\nexit 1\n")
	if err := placeFfmpeg(bad, dir, "7.1"); err == nil {
		t.Fatal("a build that does not run must be rejected")
	}
	if b, _ := os.ReadFile(dir + "7.1/ffmpeg"); string(b) != "#!/bin/sh\necho 'ffmpeg version 7.1-stub built'\n" {
		t.Fatal("the proven build must stay installed when a bad one is rejected")
	}

	// A build whose ffmpeg banner is a different version must be rejected too.
	wrong := filepath.Join(t.TempDir(), "wrong.tar.gz")
	writeFfmpegTarGz(t, wrong, "#!/bin/sh\necho 'ffmpeg version 7.10-other'\n", "#!/bin/sh\necho 'ffprobe version 7.10'\n")
	if err := placeFfmpeg(wrong, dir, "7.1"); err == nil {
		t.Fatal("a build whose ffmpeg is version 7.10 must not install as 7.1")
	}
}

// refreshLabel skips (no fetch) when the index already names the release and the
// build is present; the bogus URL proves no network call is made.
func TestRefreshLabelSkipsWhenIndexCurrent(t *testing.T) {
	dir := t.TempDir() + "/"
	os.MkdirAll(dir+"7.1", 0o755)
	os.WriteFile(dir+"7.1/ffmpeg", []byte("x"), 0o755)
	os.WriteFile(dir+"7.1/BUILD_INFO", []byte("label=7.1\n"), 0o644)
	if err := writeFfmpegIndex(dir+"ffmpeg_version.json", map[string]ffmpegIndexEntry{"7.1": {Version: "0.16.0", MD5: "d"}}); err != nil {
		t.Fatal(err)
	}
	changed, err := refreshLabel(context.Background(), nil, "http://127.0.0.1:0/bad", "http://127.0.0.1:0/bad/hashes.md5", "0.16.0", "7.1", "ubuntu_22", dir, false)
	if err != nil || changed {
		t.Fatalf("refreshLabel = %v, %v; want false, nil (up to date, no fetch)", changed, err)
	}
}
