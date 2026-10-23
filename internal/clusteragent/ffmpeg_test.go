package clusteragent

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ffmpegDistro ports ReleaseAsset::bundleFor: the XC_VM_FFMPEG asset distro tag
// for this node's /etc/os-release, "" for a distribution with no builds (the
// installed ones are then kept — FfmpegBuildsCommand's unsupported-distro path).
func TestFfmpegDistroMapsOsRelease(t *testing.T) {
	cases := []struct{ id, ver, want string }{
		{"ubuntu", "20.04", "ubuntu_20"},
		{"ubuntu", "22.04", "ubuntu_22"},
		{"ubuntu", "24.04", "ubuntu_24"},
		{"debian", "12", "debian_12"},
		{"debian", "13", "debian_13"},
		{"ubuntu", "18.04", ""}, // dropped support
		{"debian", "11", ""},    // dropped support
		{"rhel", "9", ""},       // unsupported family
		{"", "", ""},
	}
	for _, c := range cases {
		if got := ffmpegDistro(c.id, c.ver); got != c.want {
			t.Errorf("ffmpegDistro(%q, %q) = %q, want %q", c.id, c.ver, got, c.want)
		}
	}
}

// ffmpegAssetMD5 parses the release's hashes.md5 (md5sum format "<hex>  <name>",
// like expectedSHA for SHA256SUMS); fetchFfmpegArchive streams the asset through
// crypto/md5 and writes dest only on a match, leaving nothing on a mismatch
// (FfmpegBuildsCommand::install's corruption check).
func TestFfmpegAssetMD5AndFetch(t *testing.T) {
	blob := []byte("the-ffmpeg-tarball-bytes")
	sum := md5.Sum(blob)
	md5Hex := hex.EncodeToString(sum[:])
	asset := "ffmpeg_7.1_ubuntu_22.tar.gz"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/" + asset:
			w.Write(blob)
		case "/hashes.md5":
			io.WriteString(w, "00000000000000000000000000000000  other_asset.tar.gz\n"+md5Hex+"  "+asset+"\n")
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	ctx := context.Background()

	got, err := ffmpegAssetMD5(ctx, srv.Client(), srv.URL+"/hashes.md5", asset)
	if err != nil || got != md5Hex {
		t.Fatalf("ffmpegAssetMD5 = %q, %v; want %q", got, err, md5Hex)
	}
	if _, err := ffmpegAssetMD5(ctx, srv.Client(), srv.URL+"/hashes.md5", "absent.tar.gz"); err == nil {
		t.Fatal("an asset with no hashes.md5 line must error")
	}

	dest := filepath.Join(t.TempDir(), ".ffmpeg.new.tar.gz")
	if err := fetchFfmpegArchive(ctx, srv.Client(), srv.URL, asset, md5Hex, dest); err != nil {
		t.Fatalf("fetchFfmpegArchive (good md5): %v", err)
	}
	if b, _ := os.ReadFile(dest); string(b) != string(blob) {
		t.Fatalf("dest is not the downloaded archive: %q", b)
	}

	bad := filepath.Join(t.TempDir(), ".ffmpeg.new.tar.gz")
	if err := fetchFfmpegArchive(ctx, srv.Client(), srv.URL, asset, strings.Repeat("a", 32), bad); err == nil {
		t.Fatal("fetchFfmpegArchive must reject an md5 mismatch")
	}
	if _, err := os.Stat(bad); !os.IsNotExist(err) {
		t.Fatal("an md5-mismatched download must leave no file")
	}
}
