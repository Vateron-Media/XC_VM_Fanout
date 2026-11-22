package clusteragent

import "testing"

// parseRtmpNotify is the Go port of the panel's StreamAuth::notifyArguments: it
// reads exactly addr/clientid/call/name from the raw query, and refuses (ok
// false → the caller 404s) only when one of those four appears twice with
// DIFFERING values. Other keys are ignored here (creds/tcurl live in the arms).
func TestParseRtmpNotify(t *testing.T) {
	n, ok := parseRtmpNotify("addr=203.0.113.9&clientid=42&call=play&name=100&tcurl=rtmp://h/live&app=live&password=p")
	if !ok || n.Addr != "203.0.113.9" || n.ClientID != "42" || n.Call != "play" || n.Name != "100" {
		t.Fatalf("parse = %+v ok=%v", n, ok)
	}

	// A same-value duplicate on a tracked key is fine.
	if _, ok := parseRtmpNotify("call=play&call=play"); !ok {
		t.Fatal("same-value duplicate must not refuse")
	}
	// A differing duplicate on a tracked key refuses.
	if _, ok := parseRtmpNotify("call=play&call=publish"); ok {
		t.Fatal("differing duplicate on a tracked key must refuse")
	}
	// A duplicate on a NON-tracked key is ignored (no refusal).
	if _, ok := parseRtmpNotify("name=100&token=a&token=b"); !ok {
		t.Fatal("a differing duplicate on a non-tracked key must not refuse")
	}
	// Keys and values are url-decoded.
	if n, ok := parseRtmpNotify("addr=%31%32%37.0.0.1&call=publish"); !ok || n.Addr != "127.0.0.1" {
		t.Fatalf("url-decode: %+v ok=%v", n, ok)
	}
	// Absent keys stay empty, still ok (the arms decide, not the parser).
	if n, ok := parseRtmpNotify(""); !ok || n.Addr != "" || n.Call != "" {
		t.Fatalf("empty query: %+v ok=%v", n, ok)
	}
}
