// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package clusteragent

import (
	"os"
	"testing"
)

// The runtime directive is the cross-repo contract MAIN emits
// (StreamRuntimeDirective) and the agent applies. testdata/runtime_directive_
// proxy.json is the canonical shape, shared with the XC_VM side's fixture.
func TestParseRuntimeDirectiveProxy(t *testing.T) {
	data, err := os.ReadFile("testdata/runtime_directive_proxy.json")
	if err != nil {
		t.Fatal(err)
	}
	d, ok := parseRuntimeDirective(data)
	if !ok {
		t.Fatal("want ok for a record carrying a runtime directive")
	}
	if d.Kind != "proxy" {
		t.Fatalf("kind = %q, want proxy", d.Kind)
	}
	if d.Source == nil || len(d.Source.URLs) != 2 || d.Source.URLs[0] != "http://a/1.ts" {
		t.Fatalf("source = %+v", d.Source)
	}
	if d.Source.UA != "MyPlayer/1.0" || d.Source.Proxy != "1.2.3.4:8080" || d.Source.Cookie != "sid=xyz" {
		t.Fatalf("source fields = %+v", d.Source)
	}
}

func TestParseRuntimeDirectiveNoRuntime(t *testing.T) {
	if _, ok := parseRuntimeDirective([]byte(`{"stream":{"id":1}}`)); ok {
		t.Fatal("a record with no runtime directive must report ok=false")
	}
}

func TestParseRuntimeDirectiveOther(t *testing.T) {
	d, ok := parseRuntimeDirective([]byte(`{"runtime":{"kind":"other","source":null}}`))
	if !ok {
		t.Fatal("want ok")
	}
	if d.Kind != "other" || d.Source != nil {
		t.Fatalf("got kind=%q source=%+v", d.Kind, d.Source)
	}
}
