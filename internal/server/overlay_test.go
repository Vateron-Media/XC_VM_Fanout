package server

import (
	"testing"
	"time"
)

func TestSignalStoreOneShot(t *testing.T) {
	s := newSignalStore()
	s.set("u1", pendingSignal{text: "hi", fontSize: 20, expires: time.Now().Add(time.Minute)})
	if !s.peek("u1") {
		t.Fatal("peek should see the queued signal")
	}
	sig, ok := s.take("u1")
	if !ok || sig.text != "hi" {
		t.Fatalf("take = %+v, %v; want text=hi", sig, ok)
	}
	if _, ok := s.take("u1"); ok {
		t.Fatal("second take must be empty (one-shot)")
	}
	if s.peek("u1") {
		t.Fatal("peek after take should be false")
	}
	if _, ok := s.take(""); ok {
		t.Fatal("empty uuid must never match")
	}
}

func TestSignalStoreExpiry(t *testing.T) {
	s := newSignalStore()
	s.set("u2", pendingSignal{text: "old", expires: time.Now().Add(-time.Second)})
	if s.peek("u2") {
		t.Fatal("an expired signal must not peek")
	}
	if _, ok := s.take("u2"); ok {
		t.Fatal("an expired signal must not take")
	}
}

func TestParseXY(t *testing.T) {
	if x, y := parseXY("300x150"); x != 300 || y != 150 {
		t.Fatalf("parseXY(explicit) = %d,%d; want 300,150", x, y)
	}
	for i := 0; i < 100; i++ {
		for _, in := range []string{"", "junk", "10"} {
			x, y := parseXY(in)
			if x < 150 || x > 380 || y < 110 || y > 250 {
				t.Fatalf("parseXY(%q) random out of legacy range: %d,%d", in, x, y)
			}
		}
	}
}

func TestEscapeDrawtext(t *testing.T) {
	got := escapeDrawtext(`a:b'c\d%e`)
	want := `a\:b\'c\\d\%e`
	if got != want {
		t.Fatalf("escapeDrawtext = %q; want %q", got, want)
	}
}

func TestSanitizeColor(t *testing.T) {
	cases := map[string]string{
		"#FF0000":   "#FF0000",
		"white@0.8": "white@0.8",
		"';rm -rf/": "rmrf", // filtergraph-injection chars stripped
		"":          "white",
	}
	for in, want := range cases {
		if got := sanitizeColor(in); got != want {
			t.Fatalf("sanitizeColor(%q) = %q; want %q", in, got, want)
		}
	}
}
