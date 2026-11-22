// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package clusteragent

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

var ffmpegBuildID = regexp.MustCompile(`^(\d+)\.(\d+)(?:-[a-z0-9]+)?$`)

type localFfmpegBuild struct {
	label        string
	major, minor uint64
}

// configuredNode resolves the MAIN-selected builds at apply time. The launcher
// flags are bootstrap fallbacks: run.sh cannot read the signed JSON replica and
// historically chose the newest binary for both CPU and GPU.
// Return a snapshot; do not mutate the Agent fields read by concurrent loops.
func (a *Agent) configuredNode() agentNode {
	n := a.node
	if n.ffmpegCpu == "" {
		n.ffmpegCpu = a.FfmpegBin
	}
	settings := a.replicaSettings()
	if settings == nil || a.FfmpegBinDir == "" {
		return n
	}
	cpu, cpuSet := settings["ffmpeg_cpu"].(string)
	gpu, gpuSet := settings["ffmpeg_gpu"].(string)
	if cpuSet {
		if p := resolveFfmpegBuild(a.FfmpegBinDir, cpu, "ffmpeg", true); p != "" {
			n.ffmpegCpu = p
		}
		if p := resolveFfmpegBuild(a.FfmpegBinDir, cpu, "ffprobe", true); p != "" {
			n.ffprobe = p
		}
	}
	if cpuSet || gpuSet {
		n.ffmpegGpu = n.ffmpegCpu
		if gpuSet && gpu != "" {
			if p := resolveFfmpegBuild(a.FfmpegBinDir, gpu, "ffmpeg", false); p != "" {
				n.ffmpegGpu = p
			}
		}
	}
	return n
}

// Match FfmpegPaths: exact vendor build first, then the same minor/major,
// then the newest installed version. Build ids never become arbitrary paths.
func resolveFfmpegBuild(base, label, binary string, newestFallback bool) string {
	match := ffmpegBuildID.FindStringSubmatch(label)
	if match == nil {
		return ""
	}
	major, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil {
		return ""
	}
	minor, err := strconv.ParseUint(match[2], 10, 64)
	if err != nil {
		return ""
	}
	executable := func(p string) bool {
		s, e := os.Stat(p)
		return e == nil && s.Mode().IsRegular() && s.Mode().Perm()&0111 != 0
	}
	if p := filepath.Join(base, label, binary); executable(p) {
		return p
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}
	var found []localFfmpegBuild
	for _, e := range entries {
		m := ffmpegBuildID.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		maj, e1 := strconv.ParseUint(m[1], 10, 64)
		min, e2 := strconv.ParseUint(m[2], 10, 64)
		if e1 != nil || e2 != nil || !executable(filepath.Join(base, e.Name(), binary)) {
			continue
		}
		found = append(found, localFfmpegBuild{e.Name(), maj, min})
	}
	sort.Slice(found, func(i, j int) bool {
		x, y := found[i], found[j]
		xm, ym := x.major == major, y.major == major
		if xm != ym {
			return xm
		}
		xn, yn := xm && x.minor == minor, ym && y.minor == minor
		if xn != yn {
			return xn
		}
		if x.major != y.major {
			return x.major > y.major
		}
		if x.minor != y.minor {
			return x.minor > y.minor
		}
		return x.label < y.label
	})
	if len(found) == 0 || (!newestFallback && found[0].major != major) {
		return ""
	}
	return filepath.Join(base, found[0].label, binary)
}
