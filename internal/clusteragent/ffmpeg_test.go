package clusteragent

import "testing"

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
