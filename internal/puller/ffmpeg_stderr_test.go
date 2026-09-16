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
