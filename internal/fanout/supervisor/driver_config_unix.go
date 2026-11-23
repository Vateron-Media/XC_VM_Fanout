//go:build unix

package supervisor

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const maxDriverConfigBytes = 64 << 10
const maxDrivers = 64

// LoadDriverConfig reads driver ID to executable mappings from a root-owned,
// non-writable-by-group-or-others JSON file. A missing file disables drivers.
func LoadDriverConfig(path string) (map[string]string, error) {
	return loadDriverConfig(path, 0)
}

func loadDriverConfig(path string, owner uint32) (map[string]string, error) {
	out := map[string]string{}
	if path == "" {
		return out, nil
	}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != owner || info.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("driver config must be owned by uid %d and not group/other writable", owner)
	}
	body, err := io.ReadAll(io.LimitReader(f, maxDriverConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxDriverConfigBytes {
		return nil, fmt.Errorf("driver config exceeds %d bytes", maxDriverConfigBytes)
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("driver config: %w", err)
	}
	if len(out) > maxDrivers {
		return nil, fmt.Errorf("driver config has more than %d entries", maxDrivers)
	}
	for id, bin := range out {
		if !validDriverID(id) {
			return nil, fmt.Errorf("invalid driver id %q", id)
		}
		if !filepath.IsAbs(bin) {
			return nil, fmt.Errorf("driver %q executable is not absolute", id)
		}
	}
	return out, nil
}

func validDriverID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' && c != '-' && c != '.' {
			return false
		}
	}
	return true
}
