package clusteragent

import (
	"errors"
	"testing"
)

func TestRetirementTicketDoesNotCloseReusedUUIDInSameSecond(t *testing.T) {
	r, _, _ := newTestRegistry(t)
	old := rec(7, "x", 100, 100)
	old["hls_end"] = 1
	old["retire_token"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := r.Put("same", old); err != nil {
		t.Fatal(err)
	}
	fresh := clone(old)
	fresh["retire_token"] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := r.Put("same", fresh); err != nil {
		t.Fatal(err)
	}
	markers := 0
	r.RetirementMarker = func(map[string]any) error { markers++; return nil }
	r.Close("retire_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true)
	if r.Get("same") == nil || markers != 0 {
		t.Fatal("old retirement touched new lifecycle")
	}
	r.Close("retire_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", true)
	if r.Get("same") != nil || markers != 1 {
		t.Fatal("matching ended lifecycle not removed")
	}
	r.Close("retire_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", true)
	if markers != 1 {
		t.Fatal("retirement was not idempotent")
	}
}

func TestRetirementTicketNeverClosesAnOpenRecord(t *testing.T) {
	r, _, _ := newTestRegistry(t)
	c := rec(7, "x", 100, 100)
	c["retire_token"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := r.Put("same", c); err != nil {
		t.Fatal(err)
	}
	r.Close("retire_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true)
	if r.Get("same") == nil {
		t.Fatal("active lifecycle removed")
	}
}

func TestRetirementMarkerFailureRetainsRecordForRetry(t *testing.T) {
	r, _, _ := newTestRegistry(t)
	c := rec(7, "x", 100, 100)
	c["hls_end"] = 1
	c["retire_token"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := r.Put("same", c); err != nil {
		t.Fatal(err)
	}
	r.RetirementMarker = func(map[string]any) error { return errors.New("filesystem unavailable") }
	if err := r.Close("retire_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true); err == nil {
		t.Fatal("failed marker cleanup acknowledged")
	}
	if r.Get("same") == nil {
		t.Fatal("retry record was lost")
	}
	r.RetirementMarker = func(map[string]any) error { return nil }
	if err := r.Close("retire_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true); err != nil {
		t.Fatal(err)
	}
	if r.Get("same") != nil {
		t.Fatal("retry did not complete")
	}
}

func TestLegacyWorkerReopenRotatesRetirementTokenInSameSecond(t *testing.T) {
	r, _, _ := newTestRegistry(t)
	c := rec(7, "x", 100, 100)
	if err := r.Put("same", clone(c)); err != nil {
		t.Fatal(err)
	}
	token := r.Get("same")["retire_token"]
	if len(token.(string)) != 32 {
		t.Fatal("legacy worker did not get a token")
	}
	c["hls_end"] = 1
	if err := r.Put("same", clone(c)); err != nil {
		t.Fatal(err)
	}
	if r.Get("same")["retire_token"] != token {
		t.Fatal("closing changed lifecycle token")
	}
	c["hls_end"] = 0
	if err := r.Put("same", clone(c)); err != nil {
		t.Fatal(err)
	}
	if r.Get("same")["retire_token"] == token {
		t.Fatal("same-second reopen reused retirement token")
	}
}
