package clusteragent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// releaseFor ports FanoutBinaryCommand::releaseFor: the tag this server installs
// — newest unless the fleet canary holds it to a pin.
func TestReleaseFor(t *testing.T) {
	tags := []string{"0.16.0", "0.15.6", "0.15.5"} // newest first, as the API lists
	cases := []struct {
		name           string
		canary, server int64
		pin, want      string
	}{
		{"no canary -> newest", 0, 3, "", "0.16.0"},
		{"this server is the canary -> newest", 3, 3, "", "0.16.0"},
		{"held, no pin -> nothing", 3, 7, "", ""},
		{"held, pin -> newest at or below the pin", 3, 7, "0.15.6", "0.15.6"},
		{"held, pin below all -> nothing", 3, 7, "0.15.4", ""},
	}
	for _, c := range cases {
		if got := releaseFor(tags, c.canary, c.server, c.pin); got != c.want {
			t.Errorf("%s: releaseFor=%q want %q", c.name, got, c.want)
		}
	}
	// Numeric compare, not lexical: 0.15.10 > 0.15.9.
	if got := releaseFor([]string{"0.15.10", "0.15.9"}, 3, 7, "0.15.9"); got != "0.15.9" {
		t.Errorf("numeric compare: got %q want 0.15.9", got)
	}
}

func TestListReleasesChannelFilter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `[{"tag_name":"0.16.0","prerelease":true,"draft":false},`+
			`{"tag_name":"0.15.6","prerelease":false,"draft":false},`+
			`{"tag_name":"0.15.5d","prerelease":false,"draft":true},`+
			`{"tag_name":"0.15.5","prerelease":false,"draft":false}]`)
	}))
	defer srv.Close()
	stable, err := listReleases(context.Background(), srv.Client(), srv.URL, "stable")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stable, []string{"0.15.6", "0.15.5"}) {
		t.Fatalf("stable drops prerelease + draft: got %v", stable)
	}
	beta, err := listReleases(context.Background(), srv.Client(), srv.URL, "beta")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beta, []string{"0.16.0", "0.15.6", "0.15.5"}) {
		t.Fatalf("beta includes prerelease (not draft): got %v", beta)
	}
}

func TestFetchAssetVerifiesSHA256(t *testing.T) {
	blob := []byte("the-xc_node-binary-bytes")
	sum := sha256.Sum256(blob)
	sumHex := hex.EncodeToString(sum[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xc_node-linux-amd64":
			w.Write(blob)
		case "/SHA256SUMS":
			io.WriteString(w, "0000000000000000000000000000000000000000000000000000000000000000  other-asset\n"+sumHex+"  xc_node-linux-amd64\n")
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	ctx := context.Background()

	got, err := expectedSHA(ctx, srv.Client(), srv.URL+"/SHA256SUMS", "xc_node-linux-amd64")
	if err != nil || got != sumHex {
		t.Fatalf("expectedSHA = %q, %v; want %q", got, err, sumHex)
	}

	dest := filepath.Join(t.TempDir(), ".xc_node.new")
	if err := fetchAsset(ctx, srv.Client(), srv.URL, "xc_node-linux-amd64", sumHex, dest); err != nil {
		t.Fatalf("fetchAsset (good sha): %v", err)
	}
	if b, _ := os.ReadFile(dest); string(b) != string(blob) {
		t.Fatalf("dest not the downloaded blob: %q", b)
	}

	bad := filepath.Join(t.TempDir(), ".xc_node.new")
	if err := fetchAsset(ctx, srv.Client(), srv.URL, "xc_node-linux-amd64", strings.Repeat("a", 64), bad); err == nil {
		t.Fatal("fetchAsset must reject a SHA mismatch")
	}
	if _, err := os.Stat(bad); !os.IsNotExist(err) {
		t.Fatal("a SHA-mismatched download must leave no file")
	}
}

func TestInstallAgentBinaryStagesTrialAndPrev(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "xc_agent")
	if err := os.WriteFile(target, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	new1 := filepath.Join(dir, ".xc_agent.new")
	os.WriteFile(new1, []byte("NEW1"), 0o755)
	if err := installAgentBinary(target, new1, true); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(target); string(b) != "NEW1" {
		t.Fatalf("target not replaced: %q", b)
	}
	if b, _ := os.ReadFile(target + ".prev"); string(b) != "OLD" {
		t.Fatalf(".prev must be the replaced binary: %q", b)
	}
	f := strings.Fields(readFileStr(t, target+".trial"))
	if len(f) != 3 || f[1] != "0" || f[2] != "reach" {
		t.Fatalf(".trial = %v, want <ts> 0 reach", f)
	}

	// A second install while still on trial must NOT promote the unproven NEW1
	// into .prev — the last PROVEN binary (OLD) stays the rollback target.
	new2 := filepath.Join(dir, ".xc_agent.new2")
	os.WriteFile(new2, []byte("NEW2"), 0o755)
	if err := installAgentBinary(target, new2, false); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(target); string(b) != "NEW2" {
		t.Fatalf("target not replaced on 2nd install: %q", b)
	}
	if b, _ := os.ReadFile(target + ".prev"); string(b) != "OLD" {
		t.Fatalf(".prev must stay the proven OLD, not NEW1: %q", b)
	}
	f2 := strings.Fields(readFileStr(t, target+".trial"))
	if len(f2) != 2 {
		t.Fatalf("non-reach trial = %v, want <ts> 0 (no reach)", f2)
	}
}

func TestInstallFanoutRenames(t *testing.T) {
	dir := t.TempDir() + "/"
	os.MkdirAll(dir, 0o755)
	newPath := dir + ".xc_fanout.new"
	os.WriteFile(newPath, []byte("FANOUT"), 0o755)
	if err := installFanout(dir, newPath); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dir + "xc_fanout"); string(b) != "FANOUT" {
		t.Fatalf("fanout not installed: %q", b)
	}
}

func readFileStr(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestTargetReleaseDecision(t *testing.T) {
	cases := []struct {
		name           string
		tags           []string
		canary, server int64
		pin, installed string
		want           string
	}{
		{"newer, no canary", []string{"0.15.7", "0.15.6"}, 0, 7, "", "0.15.6", "0.15.7"},
		{"downgrade refused", []string{"0.15.5"}, 0, 7, "", "0.15.6", ""},
		{"equal refused", []string{"0.15.6"}, 0, 7, "", "0.15.6", ""},
		{"canary holds, no pin", []string{"0.15.7"}, 3, 7, "", "0.15.6", ""},
		{"held to pin, newer at pin", []string{"0.16.0", "0.15.7"}, 3, 7, "0.15.7", "0.15.6", "0.15.7"},
		{"no releases", nil, 0, 7, "", "0.15.6", ""},
	}
	for _, c := range cases {
		if got := targetRelease(c.tags, c.canary, c.server, c.pin, c.installed); got != c.want {
			t.Errorf("%s: targetRelease=%q want %q", c.name, got, c.want)
		}
	}
}

func TestProbeBinaryInvokesAgentVersion(t *testing.T) {
	dir := t.TempDir()
	// The probe must call the binary as `agent version` (cmd/xc_node routes that
	// to the agent, whose version prints the `features:` line); a bare `version`
	// prints no features. The fake asserts the exact argv, so a wrong invocation
	// exits non-zero (ok=false).
	reachScript := "#!/bin/sh\n[ \"$1\" = agent ] && [ \"$2\" = version ] || exit 7\necho 0.15.7\necho 'features: reached'\n"
	good := filepath.Join(dir, "good")
	os.WriteFile(good, []byte(reachScript), 0o755)
	reach, ok := probeBinary(context.Background(), good)
	if !ok || !reach {
		t.Fatalf("reach binary: ok=%v reach=%v, want true true (argv must be `agent version`)", ok, reach)
	}
	noFeat := filepath.Join(dir, "nofeat")
	os.WriteFile(noFeat, []byte("#!/bin/sh\n[ \"$1\" = agent ] && [ \"$2\" = version ] || exit 7\necho 0.15.7\n"), 0o755)
	if r, ok := probeBinary(context.Background(), noFeat); !ok || r {
		t.Fatalf("no-features binary: ok=%v reach=%v, want true false", ok, r)
	}
	bad := filepath.Join(dir, "bad")
	os.WriteFile(bad, []byte("#!/bin/sh\nexit 1\n"), 0o755)
	if _, ok := probeBinary(context.Background(), bad); ok {
		t.Fatal("a binary that does not run must give ok=false")
	}
}
