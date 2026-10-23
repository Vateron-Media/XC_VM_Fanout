package clusteragent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Viewer IP-ban enforcement on a PHP-free serve-only node (P5c): a port of
// RootSignalsCronJob::syncSets. A normal LB drops banned viewer IPs at the kernel
// via the xcvm_block4/xcvm_block6 ipsets, synced every minute by cron:root_signals
// from the node's replica blocklist. With console.php gone that cron never runs,
// and the Go HTTP front has no app-layer IP-ban check (bans are applied to RTMP
// only), so the agent must own the ipsets itself or banned viewers reach the front.
//
// Scope rulings (see docs/superpowers/specs/2026-10-23-lb-p5-root-signals-go-design.md):
//   - Only runs when ipset is installed; the PHP per-address iptables fallback
//     (ownBlocks/blockip) is for ipset-less hosts and is out of scope here.
//   - No flood-file (block_<ip>) upkeep: the ipset kernel-drops an admin-banned IP
//     before RTMP ever sees it, so the PHP touch there is redundant; the RTMP flood
//     guard keeps its own files (rtmpgates.go).
//   - IP classification is faithful to PHP's intent (public addresses only; private,
//     reserved, loopback, link-local and the doc ranges skipped), via net.IP.

const (
	banSet4           = "xcvm_block4"     // RootSignalsCronJob::IPSETS
	banSet6           = "xcvm_block6"     //
	banSetMax         = 1048576           // IPSET_MAX
	banRangeMinPrefix = 16                // RANGE_MIN_PREFIX (IPv4 CIDR)
	banMarkerName     = "ban_sync_marker" // under ReplicaDir
	banResyncAfter    = 300 * time.Second // self-heal re-sync even when unchanged (PHP's 300s)
)

// banExec runs one ipset/iptables command (optional stdin) and returns its
// combined output. A test seam. The agent runs as root (noderoot.go), so no sudo.
var banExec = func(ctx context.Context, stdin, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	return cmd.CombinedOutput()
}

// banHaveIpset reports whether ipset is installed (PHP: `command -v ipset`). Seam.
var banHaveIpset = func() bool { _, err := exec.LookPath("ipset"); return err == nil }

// syncBans mirrors the replica's viewer-ban list into the xcvm_block4/6 ipsets and
// their INPUT DROP rules. No-op unless the agent owns root (NodeRootGo), a replica
// is present and ipset is installed. Debounced on the list's hash, with a periodic
// self-heal re-sync. Called each chore tick (crondrain.go).
func (a *Agent) syncBans(ctx context.Context) {
	if !a.NodeRootGo || a.ReplicaDir == "" || !banHaveIpset() {
		return
	}
	ips := a.banBlocklist()
	hash := banHash(ips)
	marker := filepath.Join(a.ReplicaDir, banMarkerName)
	if banMarkerFresh(marker, hash) {
		return
	}
	if err := a.applyBanSets(ctx, ips); err != nil {
		a.logf("cluster: ban sync: %v", err)
		return
	}
	_ = os.WriteFile(marker, []byte(hash+"\n"+strconv.FormatInt(time.Now().Unix(), 10)), 0o640)
}

// flushBans empties both ipsets (the node.root `flush` action) and drops the
// marker so the next syncBans tick re-fills from the replica.
func (a *Agent) flushBans(ctx context.Context) {
	if !banHaveIpset() {
		return
	}
	_, _ = banExec(ctx, "", "ipset", "flush", banSet4)
	_, _ = banExec(ctx, "", "ipset", "flush", banSet6)
	if a.ReplicaDir != "" {
		_ = os.Remove(filepath.Join(a.ReplicaDir, banMarkerName))
	}
}

// banBlocklist is the replica's blocked IPs, sorted and de-duped — the same source
// rtmpgates' blockedIPs reads (ReplicaDir/blocklist.json).
func (a *Agent) banBlocklist() []string {
	set := a.blockedIPs()
	ips := make([]string, 0, len(set))
	for ip := range set {
		ips = append(ips, ip)
	}
	sort.Strings(ips)
	return ips
}

// applyBanSets classifies the blocklist by family and refills each ipset.
func (a *Agent) applyBanSets(ctx context.Context, ips []string) error {
	var want4, want6 []string
	seen := map[string]bool{}
	for _, ip := range ips {
		set, member, ok := banClassify(ip)
		if !ok || seen[set+"|"+member] {
			continue
		}
		seen[set+"|"+member] = true
		if set == banSet4 {
			want4 = append(want4, member)
		} else {
			want6 = append(want6, member)
		}
	}
	if err := applyBanFamily(ctx, banSet4, "inet", "iptables", want4); err != nil {
		return err
	}
	return applyBanFamily(ctx, banSet6, "inet6", "ip6tables", want6)
}

// applyBanFamily refills one ipset with exactly `want` in a single atomic
// `ipset restore` (a fresh set built, swapped in, destroyed — never half-built),
// then ensures the INPUT rule that drops what the set holds (RootSignalsCronJob::
// restoreSet + the match-set rule).
func applyBanFamily(ctx context.Context, set, family, tool string, want []string) error {
	create := " hash:ip family " + family + " maxelem " + strconv.Itoa(banSetMax) + " -exist\n"
	var b strings.Builder
	b.WriteString("create " + set + create)
	b.WriteString("create " + set + "_new" + create)
	b.WriteString("flush " + set + "_new\n")
	for _, ip := range want {
		b.WriteString("add " + set + "_new " + ip + "\n")
	}
	b.WriteString("swap " + set + "_new " + set + "\n")
	b.WriteString("destroy " + set + "_new\n")
	if _, err := banExec(ctx, b.String(), "ipset", "restore"); err != nil {
		return err
	}
	rule := []string{"-m", "set", "--match-set", set, "src", "-j", "DROP"}
	// -C checks for the rule (idempotent); insert only when absent.
	if _, err := banExec(ctx, "", tool, append([]string{"-C", "INPUT"}, rule...)...); err != nil {
		if _, err := banExec(ctx, "", tool, append([]string{"-I", "INPUT"}, rule...)...); err != nil {
			return err
		}
	}
	return nil
}

// banClassify ports setEntry/blockTool/blockRange: the ipset an entry belongs in
// and the member to add, or ok=false for one never blocked. A plain public IPv4
// goes to block4, a plain public IPv6 to block6, and an IPv4 CIDR (prefix 16–31,
// whose network is public) to block4 as network/prefix. Everything else is skipped.
func banClassify(entry string) (set, member string, ok bool) {
	if ip := net.ParseIP(entry); ip != nil {
		if s := banTool(ip); s != "" {
			return s, entry, true
		}
		return "", "", false
	}
	if ip, ipnet, err := net.ParseCIDR(entry); err == nil && ip.To4() != nil {
		ones, _ := ipnet.Mask.Size()
		if ones >= banRangeMinPrefix && ones <= 31 && banTool(ipnet.IP) == banSet4 {
			return banSet4, ipnet.IP.String() + "/" + strconv.Itoa(ones), true
		}
	}
	return "", "", false
}

// banTool ports blockTool: the ipset for a public address, or "" for a private,
// reserved, loopback, link-local, unspecified, multicast or doc-range one.
func banTool(ip net.IP) string {
	if ip.IsLoopback() || ip.IsUnspecified() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || !ip.IsGlobalUnicast() {
		return ""
	}
	if ip.To4() != nil {
		return banSet4
	}
	// PHP skips the 2001:db8::/32 documentation range too (IsGlobalUnicast passes it).
	if strings.HasPrefix(strings.ToLower(ip.String()), "2001:db8") {
		return ""
	}
	return banSet6
}

// banHash fingerprints the (already sorted) blocklist so an unchanged list is not
// re-synced every tick (PHP hashes the sorted list the same way).
func banHash(sorted []string) string {
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:])
}

// banMarkerFresh reports whether the last sync wrote this same hash within the
// self-heal window — if so, the sync is skipped.
func banMarkerFresh(marker, hash string) bool {
	b, err := os.ReadFile(marker)
	if err != nil {
		return false
	}
	parts := strings.SplitN(strings.TrimSpace(string(b)), "\n", 2)
	if len(parts) != 2 || parts[0] != hash {
		return false
	}
	ts, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return false
	}
	return time.Since(time.Unix(ts, 0)) < banResyncAfter
}
