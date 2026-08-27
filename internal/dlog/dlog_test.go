package dlog

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// captureLog redirects the standard logger to a buffer for the duration of the
// test and restores it (and the debug toggle) afterwards.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() {
		log.SetOutput(old)
		Enable(false)
	})
	return &buf
}

func TestLogfSilentWhenDisabled(t *testing.T) {
	buf := captureLog(t)

	Enable(false)
	if On() {
		t.Fatal("On() reported true right after Enable(false)")
	}
	Logf("test", "must not appear %d", 42)
	if buf.Len() != 0 {
		t.Fatalf("Logf wrote %q while debug was off", buf.String())
	}
}

func TestLogfWritesTaggedLineWhenEnabled(t *testing.T) {
	buf := captureLog(t)

	Enable(true)
	if !On() {
		t.Fatal("On() reported false right after Enable(true)")
	}
	Logf("puller", "id=%s started", "abc")

	out := buf.String()
	if !strings.Contains(out, "[dbg puller") {
		t.Fatalf("output %q missing category tag", out)
	}
	if !strings.Contains(out, "id=abc started") {
		t.Fatalf("output %q missing formatted message", out)
	}
}

func TestEnableTogglesBackOff(t *testing.T) {
	buf := captureLog(t)

	Enable(true)
	Logf("a", "one")
	Enable(false)
	Logf("a", "two")

	out := buf.String()
	if !strings.Contains(out, "one") {
		t.Fatalf("first line lost: %q", out)
	}
	if strings.Contains(out, "two") {
		t.Fatalf("line written after Enable(false): %q", out)
	}
}
