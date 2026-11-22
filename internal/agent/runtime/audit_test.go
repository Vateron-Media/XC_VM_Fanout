package clusteragent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHeartbeatRelaysAuditJSON(t *testing.T) {
	exact := `{"k":"` + strings.Repeat("y", MaxAuditBytes-8) + `"}`
	big := `{"k":"` + strings.Repeat("y", MaxAuditBytes-7) + `"}`
	if len(exact) != MaxAuditBytes || len(big) != MaxAuditBytes+1 {
		t.Fatalf("fixtures are %d and %d bytes", len(exact), len(big))
	}
	cases := []struct {
		name string
		file string // "" = no file
		want string // the audit sent, re-encoded; "" = none
	}{
		{"no file", "", ""},
		{"an object, passed through", `{"sql_connects":3,"redis_connects":0,"sites":{"sql src/a.php:10":3},"connects_since":1790000000,"future":{"x":[1,2]}}`,
			`{"connects_since":1790000000,"future":{"x":[1,2]},"redis_connects":0,"sites":{"sql src/a.php:10":3},"sql_connects":3}`},
		{"integers stay exact", `{"n":9007199254740993}`, `{"n":9007199254740993}`},
		{"exactly the limit", exact, exact},
		{"past the limit", big, ""},
		{"an array", `[1,2]`, ""},
		{"null", `null`, ""},
		{"a string", `"x"`, ""},
		{"trailing data", `{"a":1} {"b":2}`, ""},
		{"broken", `{"a":`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, cl := newReplayMain(t)
			a := &Agent{Client: cl, Logf: t.Logf}
			if c.file != "" {
				if err := os.WriteFile(filepath.Join(filepath.Dir(cl.State.path), "audit.json"), []byte(c.file), 0o640); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := a.Heartbeat(context.Background()); err != nil {
				t.Fatal(err)
			}
			got, ok := m.bodies["heartbeat"][0]["audit"]
			if c.want == "" {
				if ok {
					t.Fatal("sent an audit")
				}
				return
			}
			// What went on the wire, as MAIN decodes it.
			var sent, want any
			b, _ := json.Marshal(got)
			json.Unmarshal(b, &sent)
			json.Unmarshal([]byte(c.want), &want)
			sb, _ := json.Marshal(sent)
			wb, _ := json.Marshal(want)
			if string(sb) != string(wb) {
				t.Fatalf("sent %s, want %s", sb, wb)
			}
			if doc := a.readAudit(); doc != nil {
				if raw, _ := json.Marshal(doc); c.name == "integers stay exact" && string(raw) != c.want {
					t.Fatalf("re-encoded %s", raw)
				}
			}
		})
	}
}
