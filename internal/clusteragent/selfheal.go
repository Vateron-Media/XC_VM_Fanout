package clusteragent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
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
