// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// Registered tells the gateway whether the node holds a pull config for a
// stream — so a serve-only LB serves-and-starts an idle registered stream but
// still fails closed on an unknown one.
func TestManagerRegistered(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)

	if m.Registered("7") {
		t.Fatal("an unknown id must not be registered")
	}
	m.GetOrCreate("7") // the stream exists now, but has no pull config
	if m.Registered("7") {
		t.Fatal("a created-but-unregistered stream must not be registered")
	}
	if !m.Register("7", puller.Source{URLs: []string{"http://a/1.ts"}}, 0) {
		t.Fatal("Register should succeed")
	}
	if !m.Registered("7") {
		t.Fatal("a registered stream must report registered")
	}
}
