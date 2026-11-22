package clusteragent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// An operator's token.rotate_now, queued by MAIN's own CommandBus: the agent
// refreshes before it acks, so MAIN's command row says the rotation
// happened, with the epoch the node now holds, and that epoch is the one
// MAIN has on record once the node authenticates with it.
func TestInteropRotateNowAcksTheNewEpoch(t *testing.T) {
	a, runPHP, ctx := interopNode(t)
	oldIdle, oldWait := CommandsIdle, CommandsWait
	CommandsIdle, CommandsWait = 50*time.Millisecond, 300*time.Millisecond
	t.Cleanup(func() { CommandsIdle, CommandsWait = oldIdle, oldWait })

	runPHP("command.php", "flows", "2")
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := a.Client.Current()

	lctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		a.RunCommands(lctx, func(context.Context, *Command, WireCommand) (bool, []byte) {
			t.Error("token.rotate_now reached the node's PHP")
			return false, nil
		})
		close(done)
	}()
	t.Cleanup(func() { stop(); <-done })

	id := runPHP("command.php", "enqueue", TypeRotateNow, "{}")
	var out []any
	for i := 0; i < 300; i++ {
		if res := runPHP("command.php", "result", id); res != "pending" {
			if err := json.Unmarshal([]byte(res), &out); err != nil || len(out) != 2 {
				t.Fatalf("result of %s: %s", id, res)
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	after, _ := a.Client.Current()
	want := fmt.Sprintf("rotated to epoch %d", after.Epoch)
	if len(out) != 2 || out[0] != true || out[1] != want || after.Epoch <= before.Epoch {
		t.Fatalf("ack %v, want [true %q] with the epoch past %d", out, want, before.Epoch)
	}
}
