package clusteragent

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Telemetry (plan, Phase 3): the node's host is sampled every second and the
// heartbeat carries the latest sample. MAIN turns it into the legacy
// servers.watchdog_data shape (HeartbeatService::toWatchdogData), so once the
// TELEMETRY flow is on the LB's watchdog, the stats part of cron:servers and
// network.py stop writing.
//
// What only PHP knows (nginx requests per second, the fanout daemon's
// status) the LB's watchdog keeps sampling and leaves in local.json beside
// the state; the sampler forwards it while it is fresh.

// TelemetryVersion is the telemetry document's format.
const TelemetryVersion = 1

type cpuTimes struct{ user, nice, sys, idle uint64 }

type netCounters struct{ rxBytes, rxPackets, rxErrs, txBytes, txPackets, txErrs uint64 }

// NetRate is one interface's traffic per second over the last sample.
type NetRate struct {
	InBytes    uint64 `json:"in_bytes"`
	InPackets  uint64 `json:"in_packets"`
	InErrors   uint64 `json:"in_errors"`
	OutBytes   uint64 `json:"out_bytes"`
	OutPackets uint64 `json:"out_packets"`
	OutErrors  uint64 `json:"out_errors"`
	RxTotal    uint64 `json:"rx_total"`
	TxTotal    uint64 `json:"tx_total"`
	Speed      int64  `json:"speed"`
}

// Sampler samples the host. Paths are fields so tests can point them at a
// fixture tree.
type Sampler struct {
	Proc      string // procfs mount, "/proc"
	SysNet    string // "/sys/class/net"
	Home      string // the deploy root whose filesystem is reported
	LocalFile string // PHP's local.json, "" for none

	mu      sync.Mutex
	prevCPU *cpuTimes
	prevNet map[string]netCounters
	prevAt  time.Time
	last    map[string]any
}

// NewSampler returns a sampler for this host, with local.json beside statePath.
func NewSampler(statePath string) *Sampler {
	return &Sampler{Proc: "/proc", SysNet: "/sys/class/net", Home: "/home/xc_vm", LocalFile: filepath.Join(filepath.Dir(statePath), "local.json")}
}

// Run samples every second until stop is closed.
func (s *Sampler) Run(stop <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	s.Sample(time.Now())
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			s.Sample(now)
		}
	}
}

// Latest is the newest sample (nil before the first).
func (s *Sampler) Latest() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// Sample takes one sample.
func (s *Sampler) Sample(now time.Time) {
	out := map[string]any{"v": TelemetryVersion, "at_ms": now.UnixMilli()}

	cpu, cpuOK := s.cpuTimes()
	cores, name := s.cpuInfo()
	out["cpu_cores"], out["cpu_name"] = cores, name
	if b, err := os.ReadFile(filepath.Join(s.Proc, "loadavg")); err == nil {
		if f := strings.Fields(string(b)); len(f) >= 3 {
			l := make([]float64, 3)
			for i := range l {
				l[i], _ = strconv.ParseFloat(f[i], 64)
			}
			out["load"] = l
		}
	}
	total, avail := s.memInfo()
	out["mem_total_kb"], out["mem_avail_kb"] = total, avail
	var st syscall.Statfs_t
	if syscall.Statfs(s.Home, &st) == nil {
		out["disk_total"] = st.Blocks * uint64(st.Bsize)
		out["disk_free"] = st.Bavail * uint64(st.Bsize)
	}
	if b, err := os.ReadFile(filepath.Join(s.Proc, "sys/kernel/osrelease")); err == nil {
		out["kernel"] = strings.TrimSpace(string(b))
	}
	if b, err := os.ReadFile(filepath.Join(s.Proc, "uptime")); err == nil {
		if f := strings.Fields(string(b)); len(f) > 0 {
			up, _ := strconv.ParseFloat(f[0], 64)
			out["uptime_s"] = int64(up)
		}
	}
	producers, phpPIDs, fpmMaster := s.processes()
	out["stream_producers"] = producers
	if len(phpPIDs) > 0 || fpmMaster {
		out["php_pids"] = phpPIDs
	}
	counters := s.netCounters()
	ifaces := make([]string, 0, len(counters))
	for name := range counters {
		if name != "lo" && !strings.HasPrefix(name, "bond") {
			ifaces = append(ifaces, name)
		}
	}
	sort.Strings(ifaces)
	out["interfaces"] = ifaces
	if local := s.local(now); local != nil {
		out["local"] = local
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	secs := now.Sub(s.prevAt).Seconds()
	if cpuOK && s.prevCPU != nil {
		du, dn, ds, di := cpu.user-s.prevCPU.user, cpu.nice-s.prevCPU.nice, cpu.sys-s.prevCPU.sys, cpu.idle-s.prevCPU.idle
		if sum := du + dn + ds + di; sum > 0 {
			// As the legacy watchdog: user + system over user, nice, system and idle.
			out["cpu"] = float64(int64(float64(du+ds)/float64(sum)*10000)) / 100
		}
	}
	if s.prevNet != nil && secs > 0 {
		net := map[string]NetRate{}
		for name, c := range counters {
			p, ok := s.prevNet[name]
			if !ok || name == "lo" {
				continue
			}
			rate := func(a, b uint64) uint64 {
				if a < b {
					return 0
				}
				return uint64(float64(a-b) / secs)
			}
			net[name] = NetRate{
				InBytes: rate(c.rxBytes, p.rxBytes), InPackets: rate(c.rxPackets, p.rxPackets), InErrors: rate(c.rxErrs, p.rxErrs),
				OutBytes: rate(c.txBytes, p.txBytes), OutPackets: rate(c.txPackets, p.txPackets), OutErrors: rate(c.txErrs, p.txErrs),
				RxTotal: c.rxBytes, TxTotal: c.txBytes, Speed: s.speed(name),
			}
		}
		out["net"] = net
	}
	if cpuOK {
		s.prevCPU = &cpu
	}
	s.prevNet, s.prevAt = counters, now
	s.last = out
}

func (s *Sampler) cpuTimes() (cpuTimes, bool) {
	f, err := os.Open(filepath.Join(s.Proc, "stat"))
	if err != nil {
		return cpuTimes{}, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return cpuTimes{}, false
	}
	x := strings.Fields(sc.Text())
	if len(x) < 5 || x[0] != "cpu" {
		return cpuTimes{}, false
	}
	n := func(i int) uint64 { v, _ := strconv.ParseUint(x[i], 10, 64); return v }
	return cpuTimes{user: n(1), nice: n(2), sys: n(3), idle: n(4)}, true
}

func (s *Sampler) cpuInfo() (int, string) {
	b, err := os.ReadFile(filepath.Join(s.Proc, "cpuinfo"))
	if err != nil {
		return 0, ""
	}
	cores, name := 0, ""
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "processor":
			cores++
		case "model name":
			if name == "" {
				name = strings.TrimSpace(v)
			}
		}
	}
	return cores, name
}

func (s *Sampler) memInfo() (total, avail int64) {
	b, err := os.ReadFile(filepath.Join(s.Proc, "meminfo"))
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		v, _ := strconv.ParseInt(f[1], 10, 64)
		switch f[0] {
		case "MemTotal:":
			total = v
		case "MemAvailable:":
			avail = v
		}
	}
	return total, avail
}

// processes counts stream producers (ffmpeg, `xc_fanout remux`) as
// ProcessManager::countStreamProducers does, and lists the xc_vm PHP-FPM
// workers (ProcessManager::phpFpmWorkerPIDs).
func (s *Sampler) processes() (producers int, php []int, fpmMaster bool) {
	entries, err := os.ReadDir(s.Proc)
	if err != nil {
		return 0, nil, false
	}
	php = []int{}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.Proc, e.Name(), "cmdline"))
		if err != nil || len(raw) == 0 {
			continue
		}
		argv := bytes.Split(bytes.TrimRight(raw, "\x00"), []byte{0})
		prog := filepath.Base(string(argv[0]))
		if prog == "ffmpeg" || (prog == "xc_fanout" && len(argv) > 1 && string(argv[1]) == "remux") {
			producers++
		}
		title := string(bytes.Join(argv, []byte(" ")))
		if strings.Contains(title, "php-fpm: pool xc_vm") {
			php = append(php, pid)
		} else if strings.Contains(title, "php-fpm: master process") {
			fpmMaster = true
		}
	}
	sort.Ints(php)
	return producers, php, fpmMaster
}

func (s *Sampler) netCounters() map[string]netCounters {
	out := map[string]netCounters{}
	b, err := os.ReadFile(filepath.Join(s.Proc, "net/dev"))
	if err != nil {
		return out
	}
	lines := strings.Split(string(b), "\n")
	for _, line := range lines[min(2, len(lines)):] {
		name, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		f := strings.Fields(rest)
		if len(f) < 16 {
			continue
		}
		n := func(i int) uint64 { v, _ := strconv.ParseUint(f[i], 10, 64); return v }
		out[strings.TrimSpace(name)] = netCounters{rxBytes: n(0), rxPackets: n(1), rxErrs: n(2), txBytes: n(8), txPackets: n(9), txErrs: n(10)}
	}
	return out
}

func (s *Sampler) speed(iface string) int64 {
	b, err := os.ReadFile(filepath.Join(s.SysNet, iface, "speed"))
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	return max(v, 0)
}

// local returns PHP's local.json when it was written in the last 10 s.
func (s *Sampler) local(now time.Time) map[string]any {
	if s.LocalFile == "" {
		return nil
	}
	fi, err := os.Stat(s.LocalFile)
	if err != nil || now.Sub(fi.ModTime()) > 10*time.Second || fi.Size() > 64<<10 {
		return nil
	}
	b, err := os.ReadFile(s.LocalFile)
	if err != nil {
		return nil
	}
	var doc map[string]any
	if json.Unmarshal(b, &doc) != nil {
		return nil
	}
	return doc
}
