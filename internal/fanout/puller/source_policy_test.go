package puller

import "testing"

func TestStrictSourceProtocols(t *testing.T) {
	for _, raw := range []string{"file:///etc/passwd", "/etc/passwd", "tcp://127.0.0.1:22", "udp://127.0.0.1:9"} {
		if err := validateSource(raw, true); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	for _, raw := range []string{"https://example.com/live.ts", "udp://239.0.0.1:1234", "rtp://[ff15::1]:1234"} {
		if err := validateSource(raw, true); err != nil {
			t.Errorf("refused %q: %v", raw, err)
		}
	}
}
