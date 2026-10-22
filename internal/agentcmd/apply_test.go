package agentcmd

import "testing"

// A serve-only node's Go agent materialises the whole replica itself, so
// console.php cluster:apply would only fill PHP caches a Go-only node never
// reads: its Apply hook must be nil. A normal node keeps ApplyViaPHP.
func TestApplyFuncServeOnlyIsNil(t *testing.T) {
	if applyFunc(true, "php", "console.php") != nil {
		t.Fatal("serve-only: Apply must be nil (no console.php cluster:apply)")
	}
	if applyFunc(false, "php", "console.php") == nil {
		t.Fatal("non-serve-only: Apply must keep ApplyViaPHP")
	}
}
