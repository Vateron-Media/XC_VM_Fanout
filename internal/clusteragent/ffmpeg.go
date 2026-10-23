package clusteragent

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/md5"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"regexp"
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

// randHex is 8 hex chars for a temp build dir, like FfmpegBuildsCommand's bin2hex(random_bytes(4)).
func randHex() string {
	var b [4]byte
	_, _ = crand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// extractFfmpeg pulls exactly ffmpeg, ffprobe and BUILD_INFO out of the tar.gz
// into dir (whatever else the archive holds is ignored, like the PHP's
// `tar … ffmpeg ffprobe BUILD_INFO`). Only those three known names are ever
// written, so a crafted entry name cannot escape dir.
func extractFfmpeg(archive, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	want := map[string]bool{"ffmpeg": true, "ffprobe": true, "BUILD_INFO": true}
	found := map[string]bool{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		name := path.Base(strings.TrimRight(hdr.Name, "/"))
		if !want[name] || found[name] {
			continue
		}
		out, err := os.OpenFile(strings.TrimRight(dir, "/")+"/"+name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, io.LimitReader(tr, 256<<20))
		cerr := out.Close()
		if err != nil {
			return err
		}
		if cerr != nil {
			return cerr
		}
		found[name] = true
	}
	if len(found) < len(want) {
		return fmt.Errorf("the archive does not hold ffmpeg, ffprobe and BUILD_INFO")
	}
	return nil
}

// bannerMatchesLabel is FfmpegBuildsCommand's `^ffmpeg version n?<label>(?![0-9])`:
// a 4.4.5 banner counts as label 4.4, but 7.10 never passes as 7.1.
func bannerMatchesLabel(banner, label string) bool {
	return regexp.MustCompile(`^ffmpeg version n?` + regexp.QuoteMeta(label) + `([^0-9]|$)`).MatchString(banner)
}

// placeFfmpeg extracts the build aside, proves both binaries run on this node
// and the ffmpeg banner names the label, then swaps it into <binDir>/<label>/
// atomically. The installed build is kept on every failure (last-known-good);
// a crash leaves only a .new/.old temp dir, never a partial <label>/.
func placeFfmpeg(archive, binDir, label string) error {
	base := strings.TrimRight(binDir, "/") + "/"
	newDir := base + "." + label + ".new." + randHex()
	if err := extractFfmpeg(archive, newDir); err != nil {
		os.RemoveAll(newDir)
		return err
	}
	defer os.RemoveAll(newDir)
	for _, name := range []string{"ffmpeg", "ffprobe"} {
		p := newDir + "/" + name
		_ = os.Chmod(p, 0o755)
		if err := exec.Command(p, "-hide_banner", "-version").Run(); err != nil {
			return fmt.Errorf("%s does not run on this node: %w", name, err)
		}
	}
	out, _ := exec.Command(newDir+"/ffmpeg", "-hide_banner", "-version").Output()
	if !bannerMatchesLabel(string(out), label) {
		return fmt.Errorf("its ffmpeg is not FFmpeg %s", label)
	}
	dst := base + label
	oldDir := base + "." + label + ".old." + randHex()
	movedOld := false
	if _, err := os.Stat(dst); err == nil {
		if err := os.Rename(dst, oldDir); err != nil {
			return fmt.Errorf("cannot move the installed build aside: %w", err)
		}
		movedOld = true
	}
	if err := os.Rename(newDir, dst); err != nil {
		if movedOld {
			_ = os.Rename(oldDir, dst) // restore the last-known-good
		}
		return fmt.Errorf("cannot install the build: %w", err)
	}
	if movedOld {
		os.RemoveAll(oldDir)
	}
	return nil
}

// ffmpegIndexEntry is one label's line in ffmpeg_version.json.
type ffmpegIndexEntry struct {
	Version string `json:"version"`
	MD5     string `json:"md5"`
}

func ffmpegIndexPath(binDir string) string {
	return strings.TrimRight(binDir, "/") + "/" + "ffmpeg_version.json"
}

// readFfmpegIndex loads ffmpeg_version.json ({label:{version,md5}}); an absent
// or unreadable index is an empty map, so every label is then re-checked.
func readFfmpegIndex(p string) map[string]ffmpegIndexEntry {
	out := map[string]ffmpegIndexEntry{}
	b, err := os.ReadFile(p)
	if err != nil {
		return out
	}
	_ = json.Unmarshal(b, &out)
	return out
}

func writeFfmpegIndex(p string, idx map[string]ffmpegIndexEntry) error {
	b, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, append(b, '\n'), 0o644)
}

// refreshLabel brings one label up to release `version` for `distro`: it skips
// when the index already names it and the build is present (unless force), keeps
// the installed build when the release has no asset or its hashes.md5 cannot be
// read, and otherwise fetches, md5-verifies, installs and records it. Ports
// FfmpegBuildsCommand::label. Returns whether the build changed.
func refreshLabel(ctx context.Context, client *http.Client, baseURL, hashesURL, version, label, distro, binDir string, force bool) (bool, error) {
	base := strings.TrimRight(binDir, "/") + "/"
	labelDir := base + label
	idxPath := ffmpegIndexPath(binDir)
	idx := readFfmpegIndex(idxPath)
	if !force && idx[label].Version == version && fileExists(labelDir+"/ffmpeg") && fileExists(labelDir+"/BUILD_INFO") {
		return false, nil
	}
	asset := "ffmpeg_" + label + "_" + distro + ".tar.gz"
	md5Hex, err := ffmpegAssetMD5(ctx, client, hashesURL, asset)
	if err != nil {
		// No line for this label, or hashes.md5 unreadable: keep what is installed.
		return false, nil
	}
	if err := os.MkdirAll(base, 0o755); err != nil {
		return false, err
	}
	tmp := base + ".dl." + randHex() + ".tar.gz"
	if err := fetchFfmpegArchive(ctx, client, baseURL, asset, md5Hex, tmp); err != nil {
		return false, err
	}
	defer os.Remove(tmp)
	if err := placeFfmpeg(tmp, binDir, label); err != nil {
		return false, err
	}
	idx[label] = ffmpegIndexEntry{Version: version, MD5: md5Hex}
	return true, writeFfmpegIndex(idxPath, idx)
}
