package clusteragent

import "testing"

func TestRedactLog(t *testing.T) {
	cases := map[string]string{
		"user=alice&password=secret&x=1": "user=***&password=***&x=1",
		"rtsp://bob:pw@host/live":         "rtsp://***@host/live",
		"http://h/live/alice/secret/1.ts": "http://h/live/***/***/1.ts",
		"nothing to mask":                 "nothing to mask",
		"":                                "",
		`token=ab"cd`:                     `token=***"cd`, // value ends at the quote
	}
	for in, want := range cases {
		if got := redactLog(in); got != want {
			t.Fatalf("redactLog(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRedactLogRowStringsOnly(t *testing.T) {
	row := map[string]any{"q": "pass=x", "n": int64(7)}
	out := redactLogRow(row)
	if out["q"] != "pass=***" || out["n"] != int64(7) {
		t.Fatalf("row = %+v", out)
	}
}
