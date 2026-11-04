// SPDX-License-Identifier: AGPL-3.0-or-later
package clusteragent

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func addSelectedBuild(t *testing.T, base, label string) {
	t.Helper()
	dir := filepath.Join(base, label)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ffmpeg", "ffprobe"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
			t.Fatal(err)
		}
	}
}
func writeSelectedBuild(t *testing.T, a *Agent, cpu, gpu string) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"data": map[string]any{"ffmpeg_cpu": cpu, "ffmpeg_gpu": gpu}})
	if err := os.WriteFile(filepath.Join(a.ReplicaDir, "settings.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}
func selectedBuildAgent(t *testing.T) *Agent {
	t.Helper()
	base := t.TempDir()
	for _, label := range []string{"4.1-sc2018", "7.1", "8.1"} {
		addSelectedBuild(t, base, label)
	}
	a := &Agent{FfmpegBinDir: base, ReplicaDir: t.TempDir(), FfmpegBin: filepath.Join(base, "8.1", "ffmpeg")}
	a.SetNodePaths("/s/", "/l/", "3", filepath.Join(base, "8.1", "ffmpeg"), filepath.Join(base, "8.1", "ffprobe"), "/node")
	return a
}
func TestConfiguredNodeUsesVendorBuildAndSeparateGPU(t *testing.T) {
	a := selectedBuildAgent(t)
	writeSelectedBuild(t, a, "4.1-sc2018", "7.1")
	n := a.configuredNode()
	if n.ffmpegCpu != filepath.Join(a.FfmpegBinDir, "4.1-sc2018", "ffmpeg") || n.ffmpegGpu != filepath.Join(a.FfmpegBinDir, "7.1", "ffmpeg") || n.ffprobe != filepath.Join(a.FfmpegBinDir, "4.1-sc2018", "ffprobe") {
		t.Fatalf("wrong paths: %+v", n)
	}
	if a.FfmpegBin != filepath.Join(a.FfmpegBinDir, "8.1", "ffmpeg") || a.node.ffmpegCpu != a.FfmpegBin {
		t.Fatal("resolution mutated concurrently read startup fields")
	}
}
func TestConfiguredNodeReadsChangedSettingsWithoutRestart(t *testing.T) {
	a := selectedBuildAgent(t)
	writeSelectedBuild(t, a, "7.1", "")
	first := a.configuredNode()
	writeSelectedBuild(t, a, "4.1-sc2018", "")
	next := a.configuredNode()
	if first.ffmpegCpu == next.ffmpegCpu || next.ffmpegGpu != next.ffmpegCpu || !strings.Contains(next.ffmpegCpu, "4.1-sc2018") {
		t.Fatalf("settings not followed: %+v => %+v", first, next)
	}
}
func TestConfiguredNodeKeepsBootstrapPathsWithoutReplica(t *testing.T) {
	a := selectedBuildAgent(t)
	if got := a.configuredNode(); got != a.node {
		t.Fatalf("unexpected bootstrap fallback: %+v", got)
	}
	if err := os.WriteFile(filepath.Join(a.ReplicaDir, "settings.json"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := a.configuredNode(); got != a.node {
		t.Fatalf("bad replica changed paths: %+v", got)
	}
}
func TestConfiguredNodeMissingGPUFallsBackToSelectedCPU(t *testing.T) {
	a := selectedBuildAgent(t)
	writeSelectedBuild(t, a, "4.1-sc2018", "99.1")
	n := a.configuredNode()
	if n.ffmpegGpu != n.ffmpegCpu {
		t.Fatalf("GPU incorrectly chose another major: %+v", n)
	}
}
func TestResolveFfmpegBuildMatchesNumericFallbackAndIgnoresBackups(t *testing.T) {
	base := t.TempDir()
	for _, label := range []string{"4.4", "7.1", "7.10", "8.1", "99.1.backup"} {
		addSelectedBuild(t, base, label)
	}
	tests := []struct {
		label, want string
		fallback    bool
	}{{"7.1-vendor", "7.1", true}, {"7.2", "7.10", true}, {"4.0", "4.4", true}, {"9.0", "8.1", true}, {"9.0", "", false}, {"../8.1", "", true}, {"8.1\n", "", true}}
	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			got := resolveFfmpegBuild(base, test.label, "ffmpeg", test.fallback)
			want := ""
			if test.want != "" {
				want = filepath.Join(base, test.want, "ffmpeg")
			}
			if got != want {
				t.Fatalf("got %q want %q", got, want)
			}
		})
	}
}
func TestSeedProxyUsesSelectedExternalBuild(t *testing.T) {
	a := selectedBuildAgent(t)
	st := newFanoutStub(t, http.StatusNoContent)
	a.FanoutCtl = st.sock
	a.SeedFanout = true
	writeSelectedBuild(t, a, "4.1-sc2018", "")
	record, err := os.ReadFile("testdata/runtime_directive_proxy.json")
	if err != nil {
		t.Fatal(err)
	}
	a.seedFanoutRecord(7, record)
	_, _, body := st.seen()
	var registration map[string]any
	if err := json.Unmarshal(body, &registration); err != nil {
		t.Fatal(err)
	}
	if registration["ffmpeg"] != filepath.Join(a.FfmpegBinDir, "4.1-sc2018", "ffmpeg") {
		t.Fatalf("wrong registered ffmpeg: %s", body)
	}
}
func TestSuperviseUsesSelectedCpuGpuAndProbe(t *testing.T) {
	a := selectedBuildAgent(t)
	writeSelectedBuild(t, a, "4.1-sc2018", "7.1")
	sock := filepath.Join(t.TempDir(), "ctl.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var body []byte
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/ingest/") {
			json.NewEncoder(w).Encode(map[string]string{"socket": "/i/7.sock"})
			return
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		body = b
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	a.FanoutCtl = sock
	spec := json.RawMessage(`{"cmd":"{ffmpeg_cpu} -nofix_dts -i x","gpu":"{ffmpeg_gpu}","probe":"{ffprobe}"}`)
	if err := a.superviseStream(context.Background(), 7, spec); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	var got map[string]string
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["cmd"] != filepath.Join(a.FfmpegBinDir, "4.1-sc2018", "ffmpeg")+" -nofix_dts -i x" || got["gpu"] != filepath.Join(a.FfmpegBinDir, "7.1", "ffmpeg") || got["probe"] != filepath.Join(a.FfmpegBinDir, "4.1-sc2018", "ffprobe") {
		t.Fatalf("wrong substituted command: %+v", got)
	}
}
