package supervisor

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStreamLogIsPrivateAndRedacted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stream.log")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	st := &stream{
		id:   "7",
		sup:  &Supervisor{now: time.Now},
		spec: Spec{LogPath: path},
	}
	st.emit(EventStreamStart, "http://host/live/user/secret/7.ts?token=hidden")
	entries := readLog(t, path)
	if len(entries) != 1 || entries[0].Source != "http://host/live/xxxxx/xxxxx/7.ts?token=xxxxx" {
		t.Fatalf("unredacted stream log: %+v", entries)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("stream log mode = %o", got)
	}
}
