package clusteragent

import (
	"slices"
	"strings"
)

// Known-good URL sets (plan, section 3: "Agents keep their last 3 known-good
// URL sets"; ADR 0004, "MAIN endpoint changes (Phase 3, second increment)"):
//
//   - A URL set is a policy's main_urls as the agent adopted it, in its
//     order, with its policy_ver. Only the current policy's set is recorded,
//     once a request to one of its URLs gets an authenticated answer: a reply
//     whose MAC verifies and whose BOX opens, or a panel-signed denial naming
//     this node and this request's nonce, except 403 HTTPS_REQUIRED (MAIN
//     refuses ops over that URL). Adopting a policy records nothing.
//   - The last KnownGoodSets sets are kept, newest first, in the state file
//     as known_good_urls. Two sets are the same when their main_urls are
//     equal element by element; the same set replaces the stored one (taking
//     the new policy_ver) and moves to the front. The state file is written
//     only when the list changes. Install empties it.
//   - The fallback URLs are the known-good sets' URLs the current policy does
//     not list, newest set first, each URL once; under https_required only
//     their https:// ones. They are dialled after the current URLs (urls).
//     An answer through one records nothing: the agent says hello and adopts
//     MAIN's policy by the usual rule.

// KnownGoodSets is how many known-good URL sets the agent keeps.
const KnownGoodSets = 3

// URLSet is one known-good set of MAIN URLs.
type URLSet struct {
	PolicyVer int      `json:"policy_ver"`
	MainURLs  []string `json:"main_urls"`
}

// confirmURLs records the current policy's URL set as known-good when base
// is one of its URLs, and reports whether it is. The state file is written
// only when the list changes.
func (st *State) confirmURLs(base string) (bool, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !slices.Contains(st.MainURLs, base) {
		return false, nil
	}
	set := URLSet{PolicyVer: st.PolicyVer, MainURLs: append([]string{}, st.MainURLs...)}
	if len(st.KnownGoodURLs) > 0 && st.KnownGoodURLs[0].PolicyVer == set.PolicyVer && slices.Equal(st.KnownGoodURLs[0].MainURLs, set.MainURLs) {
		return true, nil
	}
	out := []URLSet{set}
	for _, k := range st.KnownGoodURLs {
		if !slices.Equal(k.MainURLs, set.MainURLs) && len(out) < KnownGoodSets {
			out = append(out, k)
		}
	}
	st.KnownGoodURLs = out
	return true, st.saveLocked()
}

// fallbackURLsLocked are the known-good sets' URLs the current policy does
// not list, in dialling order; st.mu is held.
func (st *State) fallbackURLsLocked() []string {
	httpsOnly := st.Transport == "https_required"
	seen := map[string]bool{}
	for _, u := range st.MainURLs {
		seen[u] = true
	}
	var out []string
	for _, k := range st.KnownGoodURLs {
		for _, u := range k.MainURLs {
			if seen[u] || (httpsOnly && !strings.HasPrefix(strings.ToLower(u), "https://")) {
				continue
			}
			seen[u] = true
			out = append(out, u)
		}
	}
	return out
}

// answered notes that MAIN gave an authenticated answer at base: the current
// policy's set becomes known-good, and an answer through a fallback URL asks
// the agent to fetch MAIN's policy.
func (c *Client) answered(base string) {
	current, err := c.State.confirmURLs(base)
	if !current {
		c.fellBack.Store(true)
	}
	_ = err // kept in memory: the next write of the state file carries it
}
