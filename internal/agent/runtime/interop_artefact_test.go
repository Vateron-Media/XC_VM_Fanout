package clusteragent

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestInteropArtefact: an off-air video granted end to end against MAIN's
// real ClusterApi and the node's real cluster:exec (the harness's exec.php).
// The agent says `artefact` at hello once `cluster:exec --types` lists
// artefact.fetch; MAIN's ArtefactGrants::offerOffAir grants the video; the
// agent fetches it in chunks through the artefact op, checks it and hands
// the command to cluster:exec, which places it; MAIN records the ack. A
// grant that names another SHA-256 is refused by the agent's own check, one
// that names another size by MAIN (ARTEFACT_CHANGED), and neither places
// anything.
func TestInteropArtefact(t *testing.T) {
	a, runPHP, ctx, restart := interopNodeEnv(t)
	panel := os.Getenv("XCVM_PANEL_DIR")
	if _, err := os.Stat(filepath.Join(panel, "src/Core/Cluster/ArtefactStage.php")); err != nil {
		t.Skip("the panel predates the artefact op")
	}
	php, _ := exec.LookPath("php")
	harness, _ := filepath.Abs("testdata/panel")
	dir := t.TempDir()

	// MAIN's off-air video: three chunks of ArtefactChunkMin and a short one.
	video := filepath.Join(dir, "main", "offair.ts")
	os.MkdirAll(filepath.Dir(video), 0o755)
	data := payload(700000)
	if err := os.WriteFile(video, data, 0o644); err != nil {
		t.Fatal(err)
	}
	restart("XCVM_INTEROP_OFFAIR=" + video)

	// The node's tree as cluster:exec sees it. Run as root, the harness
	// hands it to nobody, who must reach it through the test's directories.
	for p := dir; p != filepath.Dir(p) && strings.HasPrefix(p, os.TempDir()) && p != os.TempDir(); p = filepath.Dir(p) {
		os.Chmod(p, 0o755)
	}
	node := filepath.Join(dir, "node")
	for _, d := range []string{"config/cluster", "content/video"} {
		os.MkdirAll(filepath.Join(node, d), 0o755)
	}
	if err := os.Symlink(a.Client.State.path, filepath.Join(node, "config/cluster/agent.json")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XCVM_INTEROP_NODE", node)
	console := filepath.Join(harness, "exec.php")
	a.Exec = ExecViaPHP(php, console, time.Minute)
	a.run = a.localExec(a.Exec)
	a.Types = TypesViaPHP(php, console, time.Minute)
	a.ArtefactDir = filepath.Join(node, "config/cluster/artefacts")
	oldChunk, oldIdle, oldWait := ArtefactChunk, CommandsIdle, CommandsWait
	ArtefactChunk, CommandsIdle, CommandsWait = ArtefactChunkMin, 50*time.Millisecond, 300*time.Millisecond
	t.Cleanup(func() { ArtefactChunk, CommandsIdle, CommandsWait = oldChunk, oldIdle, oldWait })

	runPHP("command.php", "flows", "2")
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var row struct {
		Features *string `json:"features"`
	}
	json.Unmarshal([]byte(runPHP("node.php")), &row)
	if row.Features == nil || !slices.Contains(strings.Split(*row.Features, ","), FeatureArtefact) {
		t.Fatalf("MAIN recorded features %v", row.Features)
	}

	lctx, stop := context.WithCancel(ctx)
	done := make(chan struct{}, 2)
	go func() { a.RunCommands(lctx, a.run); done <- struct{}{} }()
	go func() { a.RunArtefacts(lctx); done <- struct{}{} }()
	t.Cleanup(func() { stop(); <-done; <-done })

	result := func(id string) (bool, string) {
		t.Helper()
		for i := 0; i < 300; i++ {
			if res := runPHP("command.php", "result", id); res != "pending" {
				var out []any
				if json.Unmarshal([]byte(res), &out) != nil || len(out) != 2 {
					t.Fatalf("result of %s: %s", id, res)
				}
				ok, _ := out[0].(bool)
				s, _ := out[1].(string)
				return ok, s
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("no ack of %s", id)
		return false, ""
	}
	left := func() []string {
		entries, _ := os.ReadDir(a.ArtefactDir)
		var out []string
		for _, e := range entries {
			out = append(out, e.Name())
		}
		return out
	}

	// The grant MAIN offers the node, fetched, checked and placed.
	var ids []string
	if err := json.Unmarshal([]byte(runPHP("artefact.php", "offer", video)), &ids); err != nil || len(ids) != 1 {
		t.Fatalf("offered %v: %v", ids, err)
	}
	ok, res := result(ids[0])
	var placed struct {
		Placed string `json:"placed"`
		Size   int    `json:"size"`
	}
	if !ok || json.Unmarshal([]byte(res), &placed) != nil || placed.Placed != "offair.ts" || placed.Size != len(data) {
		t.Fatalf("ack %v %s", ok, res)
	}
	placedFile := filepath.Join(node, "content/video/cluster/offair.ts")
	if b, err := os.ReadFile(placedFile); err != nil || !bytes.Equal(b, data) {
		t.Fatalf("placed video: %v", err)
	}
	if l := left(); len(l) != 0 {
		t.Fatalf("left in the artefacts directory: %v", l)
	}

	// A grant naming another SHA-256: MAIN serves the video's bytes, the
	// agent's check refuses them and nothing runs.
	id := runPHP("artefact.php", "tampered", video, "sha256")
	if ok, res := result(id); ok || res != "artefact refused: offair/not_on_air (offair.ts): sha256 mismatch" {
		t.Fatalf("ack %v %s", ok, res)
	}
	// A grant naming another size: MAIN refuses the chunks.
	id = runPHP("artefact.php", "tampered", video, "size")
	if ok, res := result(id); ok || res != "artefact offair/not_on_air: ARTEFACT_CHANGED" {
		t.Fatalf("ack %v %s", ok, res)
	}
	if b, _ := os.ReadFile(placedFile); !bytes.Equal(b, data) {
		t.Fatal("a refused grant changed the placed video")
	}
	if l := left(); len(l) != 0 {
		t.Fatalf("left in the artefacts directory: %v", l)
	}
	audit := runPHP("artefact.php", "audit", video)
	if !strings.Contains(audit, "artefact.refused") || !strings.Contains(audit, "artefact.failed") {
		t.Fatalf("MAIN's audit: %s", audit)
	}
}
