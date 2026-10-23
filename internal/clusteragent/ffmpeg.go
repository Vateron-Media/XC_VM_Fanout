package clusteragent

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// Node maintenance (P4): a PHP-free serve-only node refreshes its own ffmpeg
// builds, porting console.php's FfmpegBuildsCommand (which cron:root_signals
// runs daily). maxmind is dropped on a node (no licence key is shipped there and
// the gateway reads country_code from MAIN, never a local GeoLite2 DB); the
// cron:cleanup serve-only residue is already kept by the agent (audit.go,
// rtmpoffline.go). See docs/superpowers/plans/2026-10-23-lb-p4-node-maintenance.md.

// ffmpegLabels are the FFmpeg versions a node keeps a build of, one per label
// (FfmpegBuildsCommand::LABELS).
var ffmpegLabels = []string{"4.4", "7.1", "8.1"}

// ffmpegDistro ports ReleaseAsset::bundleFor (minus the ".tar.gz"): the
// XC_VM_FFMPEG asset distro tag for this node's /etc/os-release id+version,
// "" for a distribution with no builds — where the installed ones are kept.
func ffmpegDistro(id, version string) string {
	major, _ := strconv.Atoi(strings.SplitN(version, ".", 2)[0])
	switch {
	case id == "ubuntu" && (major == 20 || major == 22 || major == 24):
		return "ubuntu_" + strconv.Itoa(major)
	case id == "debian" && (major == 12 || major == 13):
		return "debian_" + strconv.Itoa(major)
	default:
		return ""
	}
}

// ffmpegBinDir is <home>/bin/ffmpeg_bin/, where each label's build lives as
// <label>/{ffmpeg,ffprobe,BUILD_INFO} (BIN_PATH . 'ffmpeg_bin/').
func ffmpegBinDir(home string) string {
	return strings.TrimRight(home, "/") + "/bin/ffmpeg_bin/"
}

// ffmpegAssetMD5 reads asset's hex MD5 from the release's hashes.md5 at hashesURL
// (md5sum format "<hex>  <name>", as expectedSHA reads SHA256SUMS). Ports
// FfmpegBuildsCommand's getAssetHash. "" + error when the asset has no line.
func ffmpegAssetMD5(ctx context.Context, client *http.Client, hashesURL, asset string) (string, error) {
	resp, err := ghGet(ctx, client, hashesURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(body), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[1] == asset && len(f[0]) == 32 {
			return f[0], nil
		}
	}
	return "", fmt.Errorf("clusteragent: %s has no hashes.md5 line", asset)
}

// fetchFfmpegArchive downloads baseURL/asset to dest, streaming it through MD5;
// dest is written (via a .part rename) only when the digest equals md5Hex, else
// nothing is left. The release ships only hashes.md5 over the same HTTPS, so
// this is a check against a corrupt or truncated download, not a signature
// (FfmpegBuildsCommand::install). Mirrors fetchAsset, md5 not sha256.
func fetchFfmpegArchive(ctx context.Context, client *http.Client, baseURL, asset, md5Hex, dest string) error {
	resp, err := ghGet(ctx, client, strings.TrimRight(baseURL, "/")+"/"+asset)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	part := dest + ".part"
	f, err := os.OpenFile(part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	h := md5.New()
	_, err = io.Copy(io.MultiWriter(f, h), resp.Body)
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(part)
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != md5Hex {
		os.Remove(part)
		return fmt.Errorf("clusteragent: %s MD5 %s, want %s", asset, got, md5Hex)
	}
	if err := os.Rename(part, dest); err != nil {
		os.Remove(part)
		return err
	}
	return nil
}
