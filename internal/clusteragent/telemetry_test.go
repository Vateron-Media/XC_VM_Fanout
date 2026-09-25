package clusteragent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

const netDev = `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: %d 10 0 0 0 0 0 0 %d 10 0 0 0 0 0 0
  eth0: %d 100 1 0 0 0 0 0 %d 200 0 0 0 0 0 0
 bond0: 5 1 0 0 0 0 0 0 5 1 0 0 0 0 0 0
`

func TestSamplerReadsTheHostAsTheWatchdogDoes(t *testing.T) {
	dir := t.TempDir()
	proc, sysnet := filepath.Join(dir, "proc"), filepath.Join(dir, "sysnet")
	writeFiles(t, proc, map[string]string{
		"stat":                 "cpu  100 0 50 850 0 0 0 0 0 0\ncpu0 1 1 1 1\n",
		"cpuinfo":              "processor\t: 0\nmodel name\t: Test CPU\n\nprocessor\t: 1\nmodel name\t: Test CPU\n",
		"loadavg":              "0.50 0.25 0.10 1/100 42\n",
		"meminfo":              "MemTotal:       8000000 kB\nMemFree:         100 kB\nMemAvailable:   6000000 kB\n",
		"sys/kernel/osrelease": "6.1.0-test\n",
		"uptime":               "90061.5 1000.0\n",
		"net/dev":              fmt.Sprintf(netDev, 1, 1, 1000, 2000),
		"101/cmdline":          "/home/xc_vm/bin/ffmpeg_bin/ffmpeg\x00-i\x00x\x00",
		"102/cmdline":          "/home/xc_vm/bin/xc_fanout/xc_fanout\x00remux\x00",
		"103/cmdline":          "/home/xc_vm/bin/xc_fanout/xc_fanout\x00daemon\x00",
		"104/cmdline":          "php-fpm: pool xc_vm                        ",
		"99/cmdline":           "php-fpm: master process (/etc/php.conf)",
		"105/cmdline":          "php-fpm: pool xc_vm",
	})
	writeFiles(t, sysnet, map[string]string{"eth0/speed": "1000\n"})
	local := filepath.Join(dir, "local.json")
	writeFiles(t, dir, map[string]string{"local.json": `{"requests_per_second":7}`})
	s := &Sampler{Proc: proc, SysNet: sysnet, Home: dir, LocalFile: local}

	t0 := time.Now()
	s.Sample(t0)
	first := s.Latest()
	if _, ok := first["cpu"]; ok {
		t.Fatal("cpu needs two samples")
	}
	writeFiles(t, proc, map[string]string{
		"stat":    "cpu  130 0 70 900 0 0 0 0 0 0\n", // +30 user, +20 sys, +50 idle → 50 %
		"net/dev": fmt.Sprintf(netDev, 1, 1, 3000, 6000),
	})
	s.Sample(t0.Add(2 * time.Second))
	got := s.Latest()

	want := map[string]any{
		"cpu": 50.0, "cpu_cores": 2, "cpu_name": "Test CPU", "load": []float64{0.5, 0.25, 0.1},
		"mem_total_kb": int64(8000000), "mem_avail_kb": int64(6000000), "kernel": "6.1.0-test",
		"uptime_s": int64(90061), "stream_producers": 2, "php_pids": []int{104, 105},
		"interfaces": []string{"eth0"},
	}
	for k, v := range want {
		if !reflect.DeepEqual(got[k], v) {
			t.Errorf("%s = %#v, want %#v", k, got[k], v)
		}
	}
	net := got["net"].(map[string]NetRate)
	if _, ok := net["lo"]; ok {
		t.Error("lo is not reported")
	}
	if e := net["eth0"]; e.InBytes != 1000 || e.OutBytes != 2000 || e.RxTotal != 3000 || e.TxTotal != 6000 || e.Speed != 1000 {
		t.Errorf("eth0 %+v", e)
	}
	if l, _ := json.Marshal(got["local"]); string(l) != `{"requests_per_second":7}` {
		t.Errorf("local %s", l)
	}
	old := t0.Add(-time.Minute)
	os.Chtimes(local, old, old)
	s.Sample(t0.Add(3 * time.Second))
	if _, ok := s.Latest()["local"]; ok {
		t.Error("a stale local.json is not forwarded")
	}
}

func TestAgentPublishesFlows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flows.json")
	a := &Agent{FlowsFile: path, Logf: t.Logf}
	a.publish(&Reply{State: "active", Mode: 1, Flows: 1})
	b, err := os.ReadFile(path)
	if err != nil || string(b) != `{"flows":1,"mode":1,"state":"active"}` {
		t.Fatalf("%s %v", b, err)
	}
	a.Unpublish()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("flows kept after a stop")
	}
}

// TestInteropTelemetryShape feeds this host's real sample through MAIN's PHP
// (HeartbeatService::toWatchdogData) and checks the legacy watchdog fields
// come out filled. Opt-in, as the other interop tests.
func TestInteropTelemetryShape(t *testing.T) {
	panel := os.Getenv("XCVM_PANEL_DIR")
	if panel == "" {
		t.Skip("XCVM_PANEL_DIR not set")
	}
	php, err := exec.LookPath("php")
	if err != nil {
		t.Skip("php not found")
	}
	s := NewSampler(filepath.Join(t.TempDir(), "agent.json"))
	s.Home = t.TempDir()
	s.Sample(time.Now())
	time.Sleep(200 * time.Millisecond)
	s.Sample(time.Now())
	in, _ := json.Marshal(s.Latest())
	cmd := exec.Command(php, "testdata/panel/watchdog.php")
	cmd.Env = append(os.Environ(), "XCVM_PANEL_DIR="+panel)
	cmd.Stdin = bytes.NewReader(in)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("watchdog.php: %v", err)
	}
	var wd map[string]any
	if err := json.Unmarshal(out, &wd); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if wd["cpu_cores"].(float64) < 1 || wd["total_mem"].(float64) <= 0 || wd["kernel"] == "" || wd["uptime"] == "" || wd["total_disk_space"].(float64) <= 0 {
		t.Fatalf("watchdog data from this host's sample: %s", out)
	}
}
