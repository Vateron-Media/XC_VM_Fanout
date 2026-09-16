// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"strings"
	"testing"
)

// TestScrubTailKeepsTheLineReadable pins both halves of the contract: nothing
// the child quoted back at us may carry the account, and everything an operator
// reads the line for must survive.
func TestScrubTailKeepsTheLineReadable(t *testing.T) {
	src := Source{
		URLs:  []string{"http://prov:8080/live/joe/" + thePassword + "/1234.ts"},
		Proxy: "joe:" + thePassword + "@10.0.0.5:3128",
	}
	raw := src.URLs[0]
	cases := []struct {
		name, tail string
		wantKeep   []string
	}{
		{
			name:     "the 403 line ffmpeg writes",
			tail:     "Error opening input: Server returned 403 Forbidden (access denied)\nError opening input file " + raw + ".",
			wantKeep: []string{"403 Forbidden", "prov:8080", "1234.ts"},
		},
		{
			// The tail buffer keeps only the last 4 KiB, so a chatty child can
			// leave the line cut ahead of its scheme: nothing is left for the
			// generic URL scan to find, only the path.
			name:     "a tail cut ahead of the scheme",
			tail:     "joe/" + thePassword + "/1234.ts: Invalid data found when processing input",
			wantKeep: []string{"1234.ts", "Invalid data"},
		},
		{
			name:     "a redirect target the daemon never saw",
			tail:     "[http @ 0x1] Opening " + quote("http://edge2:8080/live/joe/"+thePassword+"/1234.ts") + " for reading",
			wantKeep: []string{"edge2:8080", "for reading"},
		},
		{
			name:     "the proxy it could not reach",
			tail:     "[tcp @ 0x1] Connection to tcp://10.0.0.5:3128 failed: Connection refused",
			wantKeep: []string{"10.0.0.5:3128", "Connection refused"},
		},
		{
			// And the proxy quoted back with its credential, both ways round:
			// as a URL, and bare, which is how the panel's field is written and
			// so how a child that never parsed it can echo it.
			name:     "the proxy credential quoted as a url",
			tail:     "[http @ 0x1] Opening " + quote("http://joe:"+thePassword+"@10.0.0.5:3128") + " for reading",
			wantKeep: []string{"10.0.0.5:3128", "for reading"},
		},
		{
			name:     "the proxy credential quoted bare",
			tail:     "[tcp @ 0x1] Connection to joe:" + thePassword + "@10.0.0.5:3128 failed: Connection refused",
			wantKeep: []string{"10.0.0.5:3128", "Connection refused"},
		},
		{
			name:     "a line with no url at all is left alone",
			tail:     "Invalid data found when processing input",
			wantKeep: []string{"Invalid data found when processing input"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scrubTail(src, raw, tc.tail)
			if strings.Contains(got, thePassword) {
				t.Errorf("scrubTail kept the account password: %q", got)
			}
			for _, want := range tc.wantKeep {
				if !strings.Contains(got, want) {
					t.Errorf("scrubTail dropped %q, which is why the line is logged at all: %q", want, got)
				}
			}
		})
	}
}

// quote wraps s the way ffmpeg quotes a URL it is opening.
func quote(s string) string { return "'" + s + "'" }

// TestScrubTailLeavesALineWithNoSecretAlone is the other half of the contract,
// and the half a "does it still contain the password?" test cannot see: a line
// carrying nothing to mask must come back byte for byte.
//
// The scrub substitutes strings derived from the source's own configuration
// into text the daemon did not write, so every derivation has to be exact. Two
// were not. proxyForLog NORMALISES as well as redacting — it adds the scheme
// the panel's bare "host:port" form leaves out — so the pair 10.0.0.5:3128 →
// http://10.0.0.5:3128 rewrote ffmpeg's own "Connection to tcp://10.0.0.5:3128
// failed" as "tcp://http://10.0.0.5:3128", mangling the one address the line
// exists to name. And an account segment was replaced wherever it appeared, so
// a provider username that is also an ordinary word turned "Option user_agent
// not found." into "Option xxxxx_agent not found." — an operator chasing a
// real ffmpeg error down a search engine with the wrong text.
func TestScrubTailLeavesALineWithNoSecretAlone(t *testing.T) {
	for _, tc := range []struct {
		name  string
		src   Source
		tails []string
	}{
		{
			// The panel's field is documented as host:port, so this is the
			// shape nearly every deployment has.
			name: "a proxy the operator typed without a scheme",
			src:  Source{URLs: []string{"http://prov:8080/live/joe/" + thePassword + "/1234.ts"}, Proxy: "10.0.0.5:3128"},
			tails: []string{
				"[tcp @ 0x1] Connection to tcp://10.0.0.5:3128 failed: Connection refused",
				"[http @ 0x1] Failed to open http://10.0.0.5:3128",
			},
		},
		{
			name: "an account segment that is also an ordinary word",
			src:  Source{URLs: []string{"http://prov:8080/live/user/passw0rd/1234.ts"}},
			tails: []string{
				"Option user_agent not found.",
				"[in#0 @ 0x1] Error opening input: Invalid data found when processing input",
			},
		},
		{
			name: "and one that is also a word ffmpeg quotes",
			src:  Source{URLs: []string{"http://prov:8080/live/test/abcd/1234.ts"}},
			tails: []string{
				"Unrecognized option 'test'.",
				"[abcd @ 0x1] no frame!",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, tail := range tc.tails {
				if got := scrubTail(tc.src, tc.src.URLs[0], tail); got != tail {
					t.Errorf("scrubTail rewrote a line that carries no secret:\n  in  %q\n  out %q", tail, got)
				}
			}
		})
	}
}
