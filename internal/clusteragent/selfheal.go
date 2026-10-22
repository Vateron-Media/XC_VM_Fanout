package clusteragent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Binary self-heal (plan P2): a serve-only node keeps its own xc_node binary
// current with no PHP (replacing console.php fanout_binary + the root_signals
// self-heal). It resolves the release the fleet allows, downloads the arch
// asset from GitHub, SHA-256 verifies it, and installs — the fanout by atomic
// rename + supervisor respawn, its own binary by staging .xc_agent.new for
// run.sh's existing trial / .prev rollback. Faithful to FanoutBinaryCommand +
// ArtefactStage::installAgent; the trial→.prev rollback (run.sh) is unchanged.

// trimV drops a leading v/V, as PHP's ltrim($tag, 'vV').
func trimV(s string) string { return strings.TrimLeft(strings.TrimSpace(s), "vV") }

// compareVersions compares two numeric dotted versions (0.15.6 style, the
// release-tag format — tags match '[0-9]*'), part by part numerically, shorter
// padded with zeros: -1 if a<b, 0 if equal, 1 if a>b. It mirrors PHP
// version_compare for this format (numeric, so 0.15.10 > 0.15.9).
// ponytail: numeric dotted only; pre-release suffixes are not handled, which
// the tag convention never produces — revisit if tags gain -rc style suffixes.
func compareVersions(a, b string) int {
	pa, pb := strings.Split(trimV(a), "."), strings.Split(trimV(b), ".")
	for i := 0; i < len(pa) || i < len(pb); i++ {
		var x, y int
		if i < len(pa) {
			x, _ = strconv.Atoi(pa[i])
		}
		if i < len(pb) {
			y, _ = strconv.Atoi(pb[i])
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

// releaseFor is FanoutBinaryCommand::releaseFor: the tag this server installs
// from tags (newest first). Not held by the fleet canary → the newest. Held
// (a canary is set and it is not this server) → the newest tag at or below the
// pin, or "" while no pin is set.
func releaseFor(tags []string, canaryServer, serverID int64, pin string) string {
	held := canaryServer > 0 && canaryServer != serverID
	if !held {
		if len(tags) > 0 {
			return tags[0]
		}
		return ""
	}
	pin = trimV(pin)
	if pin == "" {
		return ""
	}
	for _, t := range tags {
		if compareVersions(t, pin) <= 0 {
			return t
		}
	}
	return ""
}

// ghRelease is a GitHub release object (only the fields the channel filter reads).
type ghRelease struct {
	TagName    string `json:"tag_name"`
	Prerelease bool   `json:"prerelease"`
	Draft      bool   `json:"draft"`
}

// listReleases GETs the GitHub releases API at apiURL and returns the tag names
// the channel allows, newest first: drafts are never returned; `stable` drops
// prereleases, `beta`/`dev` keep them (GitHubReleases' channel filter). apiURL
// is the full .../releases endpoint so tests can point it at a stub.
// ponytail: the `dev` channel's separate nightly repo (GitHubReleases devRepo)
// is not merged here — a dev-channel LB is a rare config; revisit if needed.
func listReleases(ctx context.Context, client *http.Client, apiURL, channel string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "XC_VM")
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clusteragent: releases: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	var rels []ghRelease
	if err := json.Unmarshal(body, &rels); err != nil {
		return nil, fmt.Errorf("clusteragent: releases: %w", err)
	}
	keepPre := channel == "beta" || channel == "dev"
	out := make([]string, 0, len(rels))
	for _, r := range rels {
		if r.Draft || (r.Prerelease && !keepPre) || r.TagName == "" {
			continue
		}
		out = append(out, r.TagName)
	}
	return out, nil
}

// getBody GETs url (GitHub UA/Accept) and returns its body, capped at 4 MiB for
// the sums file; the binary stream is read by fetchAsset instead.
func ghGet(ctx context.Context, client *http.Client, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "XC_VM")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("clusteragent: GET %s: HTTP %d", url, resp.StatusCode)
	}
	return resp, nil
}

// expectedSHA reads asset's hex SHA-256 from the SHA256SUMS at sumsURL (lines of
// "<hex>  <name>", sha256sum format), ports ReleaseAsset::expectedSha256 /
// sumFor. "" + error when the asset has no line.
func expectedSHA(ctx context.Context, client *http.Client, sumsURL, asset string) (string, error) {
	resp, err := ghGet(ctx, client, sumsURL)
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
		if len(f) == 2 && f[1] == asset {
			return f[0], nil
		}
	}
	return "", fmt.Errorf("clusteragent: %s has no SHA256SUMS line", asset)
}

// fetchAsset downloads baseURL/asset to dest, streaming it through SHA-256; it
// writes dest only when the digest equals sha256Hex, else leaves nothing. The
// download goes to dest+".part" and is renamed in on success (no half file at
// dest). dest is a staged name (e.g. .xc_agent.new) the caller then installs.
func fetchAsset(ctx context.Context, client *http.Client, baseURL, asset, sha256Hex, dest string) error {
	resp, err := ghGet(ctx, client, strings.TrimRight(baseURL, "/")+"/"+asset)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	part := dest + ".part"
	f, err := os.OpenFile(part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, h), resp.Body)
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(part)
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sha256Hex {
		os.Remove(part)
		return fmt.Errorf("clusteragent: %s SHA-256 %s, want %s", asset, got, sha256Hex)
	}
	if err := os.Rename(part, dest); err != nil {
		os.Remove(part)
		return err
	}
	return nil
}

// linkOrCopy makes dst a copy of src — a hard link when it can, else a byte
// copy (ArtefactStage's `@link || @copy`).
func linkOrCopy(src, dst string) error {
	if os.Link(src, dst) == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return err
}

// installFanout places the verified fanout binary (newPath) and forces the
// supervisor (run.sh) to respawn it: atomic rename onto <dir>/xc_fanout, then
// pkill the running daemon. No trial — the fanout is not self; run.sh respawns
// it. pkill is best effort (none running at first install).
func installFanout(dir, newPath string) error {
	target := strings.TrimRight(dir, "/") + "/xc_fanout"
	if err := os.Chmod(newPath, 0o755); err != nil {
		return err
	}
	if err := os.Rename(newPath, target); err != nil {
		return err
	}
	_ = exec.Command("pkill", "-u", "xc_vm", "-x", "xc_fanout").Run()
	return nil
}

// installAgentBinary places the agent's own verified new binary (newPath) onto
// target and stages it for run.sh's trial: it saves the current binary as
// target.prev (so a bad new one rolls back) and writes target.trial as
// "<unix> 0[ reach]". The .prev is NOT overwritten while a trial is already in
// flight — the last PROVEN binary stays the rollback target, never an unproven
// one (ArtefactStage::installAgent). `reach` is whether the new binary reports
// the `reached` feature (the caller probes it): then lasting is not enough, the
// trial ends only once it has reached MAIN. The agent never execs the new binary
// in-process — the supervisor + run.sh trial it.
func installAgentBinary(target, newPath string, reach bool) error {
	if err := os.Chmod(newPath, 0o755); err != nil {
		return err
	}
	_, trialErr := os.Stat(target + ".trial")
	_, prevErr := os.Stat(target + ".prev")
	onTrial := trialErr == nil && prevErr == nil
	if !onTrial {
		if _, err := os.Stat(target); err == nil {
			os.Remove(target + ".prev")
			if err := linkOrCopy(target, target+".prev"); err != nil {
				return err
			}
			os.Chmod(target+".prev", 0o755)
		}
	}
	if err := os.Rename(newPath, target); err != nil {
		return err
	}
	marker := strconv.FormatInt(time.Now().Unix(), 10) + " 0"
	if reach {
		marker += " reach"
	}
	return os.WriteFile(target+".trial", []byte(marker+"\n"), 0o644)
}

// targetRelease is the self-heal decision: the tag to install now, or "" — the
// release the fleet allows (releaseFor), unless it is not newer than installed
// (no downgrade, no reinstall of the same).
func targetRelease(tags []string, canary, server int64, pin, installed string) string {
	tag := releaseFor(tags, canary, server, pin)
	if tag == "" || compareVersions(tag, installed) <= 0 {
		return ""
	}
	return tag
}

// SelfHealEvery is how often a serve-only node checks for a newer binary (the
// root_signals fanout_binary cadence it replaces).
var SelfHealEvery = time.Hour

// probeBinary runs `<path> version` to confirm the downloaded binary runs on
// this node (ArtefactStage's `runs`), and reports whether it advertises the
// `reached` feature (then its trial waits for it to reach MAIN, not just last).
func probeBinary(ctx context.Context, path string) (reach, ok bool) {
	// `agent version`: cmd/xc_node routes that to the agent, whose version prints
	// the `features:` line (a bare `version` on the merged binary prints none).
	out, err := exec.CommandContext(ctx, path, "agent", "version").CombinedOutput()
	if err != nil {
		return false, false
	}
	return strings.Contains(string(out), FeatureReached), true
}

// selfHeal resolves, downloads, verifies and installs a newer node binary from
// GitHub, with no PHP — the Go port of `console.php fanout_binary`. A no-op when
// not configured (not a serve-only node). The same xc_node asset installs under
// both the fanout path (rename + respawn) and the agent's own (stage + run.sh
// trial / .prev rollback); canary/pin come from the replica settings.
func (a *Agent) selfHeal(ctx context.Context) error {
	if a.SelfHealRepo == "" {
		return nil
	}
	s := a.replicaSettings()
	canary, _ := settingInt(s["lb_binary_canary_server"])
	pin, _ := s["lb_release_pin"].(string)
	serverID, _ := strconv.ParseInt(a.node.serverID, 10, 64)
	api := "https://api.github.com/repos/" + a.SelfHealRepo + "/releases?per_page=100"
	tags, err := listReleases(ctx, http.DefaultClient, api, a.SelfHealChannel)
	if err != nil {
		return err
	}
	tag := targetRelease(tags, canary, serverID, pin, a.Version)
	if tag == "" {
		return nil
	}
	base := "https://github.com/" + a.SelfHealRepo + "/releases/download/" + tag + "/"
	asset := "xc_node-linux-" + a.SelfHealArch
	sha, err := expectedSHA(ctx, http.DefaultClient, base+"SHA256SUMS", asset)
	if err != nil {
		return err
	}
	// Download once; the merged binary runs as both tools by its name.
	tmp := filepath.Join(filepath.Dir(a.node.binary), ".xc_node.dl")
	if err := fetchAsset(ctx, http.DefaultClient, base, asset, sha, tmp); err != nil {
		return err
	}
	defer os.Remove(tmp)
	reach, ok := probeBinary(ctx, tmp)
	if !ok {
		return fmt.Errorf("clusteragent: self-heal: %s does not run on this node", tag)
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	fdir := filepath.Dir(a.node.binary) + "/"
	fnew := fdir + ".xc_fanout.new"
	if err := linkOrCopy(tmp, fnew); err != nil {
		return err
	}
	if err := installFanout(fdir, fnew); err != nil {
		return err
	}
	anew := filepath.Dir(self) + "/.xc_agent.new"
	if err := linkOrCopy(tmp, anew); err != nil {
		return err
	}
	a.logf("cluster: self-heal: installing %s", tag)
	return installAgentBinary(self, anew, reach)
}

// RunSelfHeal checks for a newer node binary on the self-heal cadence until ctx
// ends. A no-op (returns at once) when self-heal is not configured.
func (a *Agent) RunSelfHeal(ctx context.Context) {
	if a.SelfHealRepo == "" {
		return
	}
	t := time.NewTicker(SelfHealEvery)
	defer t.Stop()
	for {
		if err := a.selfHeal(ctx); err != nil && ctx.Err() == nil {
			a.logf("cluster: self-heal: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
