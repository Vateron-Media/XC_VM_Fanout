package clusteragent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAgentPublishesFlows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flows.json")
	a := &Agent{FlowsFile: path, Logf: t.Logf}
	a.publish(&Reply{State: "active", Mode: 1, Flows: 1})
	b, err := os.ReadFile(path)
	if err != nil || string(b) != `{"flows":1,"mode":1,"state":"active"}` {
		t.Fatalf("%s %v", b, err)
	}
	a.Unpublish()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("flows kept after a stop")
	}
}
