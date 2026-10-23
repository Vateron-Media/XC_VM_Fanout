package clusteragent

import (
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
