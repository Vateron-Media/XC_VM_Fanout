package clusteragent

import (
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// The owner of /xfile serves only files it can unseal to a path that hashes
// back to the ticket's ref, that ends in an allowed extension, and that lives
// under an allowed scan root. This mirrors the panel's FileTicketServer::path
// and ::allowed and ClusterSettings::pathAllowed.

// xfileExtensions is the fixed set the owner serves (FileTicketServer::EXTENSIONS),
// matched case-insensitively.
var xfileExtensions = map[string]bool{
	"log": true, "gz": true, "zip": true, "m3u8": true, "mp4": true, "mkv": true,
	"avi": true, "mpg": true, "flv": true, "3gp": true, "m4v": true, "wmv": true,
	"mov": true, "ts": true, "srt": true, "sub": true, "sbv": true, "jpg": true,
	"png": true, "bmp": true, "jpeg": true, "gif": true, "tif": true,
}

// defaultScanRoots is ClusterSettings::DEFAULT_SCAN_ROOTS.
var defaultScanRoots = []string{"/home/xc_vm/content", "/mnt", "/media"}

// mainHome is the deploy root (the panel's MAIN_HOME), always an allowed root
// alongside lb_scan_roots. ponytail: a constant, as telemetry.go hardcodes it;
// if the deploy root ever moves, derive it from the state/config anchor.
const mainHome = "/home/xc_vm"

// a bad scan-root entry: a relative path, a "." / ".." segment, "//", or a
// control char. Any bad entry rejects the whole set (ClusterSettings::scanRoots).
var badScanRoot = regexp.MustCompile(`(^|/)\.\.?(/|$)|//|[\x00-\x1f]`)

var errOffsetPastEnd = errors.New("offset past end of file")

// scanRoots parses lb_scan_roots as the panel's ClusterSettings::scanRoots does:
// a JSON array or newline-delimited list; each entry trimmed, its trailing "/"
// stripped, empties skipped. A single invalid entry rejects the whole set
// (returns nil, so only mainHome applies). An empty input yields the defaults.
func scanRoots(raw string) []string {
	var entries []string
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, "[") {
		var arr []string
		if json.Unmarshal([]byte(trimmed), &arr) == nil {
			entries = arr
		}
	}
	if entries == nil {
		entries = strings.Split(raw, "\n")
	}
	var roots []string
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		e = strings.TrimRight(e, "/")
		if e == "" || !strings.HasPrefix(e, "/") || badScanRoot.MatchString(e) {
			return nil
		}
		roots = append(roots, e)
	}
	if len(roots) == 0 {
		return defaultScanRoots
	}
	return roots
}

// allowedRoots is the scan roots the owner serves under: the replica's
// lb_scan_roots (defaults when unset/invalid) plus the deploy root.
func (a *Agent) allowedRoots() []string {
	raw := ""
	switch v := a.replicaSettings()["lb_scan_roots"].(type) {
	case string:
		raw = v
	case []any:
		var sb strings.Builder
		for _, e := range v {
			if s, ok := e.(string); ok {
				sb.WriteString(s)
				sb.WriteByte('\n')
			}
		}
		raw = sb.String()
	}
	return append(scanRoots(raw), mainHome)
}

// extAllowed reports whether path's extension is in the served set (case-insensitive).
func extAllowed(path string) bool {
	return xfileExtensions[strings.ToLower(strings.TrimPrefix(filepath.Ext(path), "."))]
}

// pathAllowed reports whether path, fully resolved, sits at or under one of the
// resolved roots. realpath (EvalSymlinks) canonicalises symlinks and ".."
// before the prefix test, so an escape through an intermediate symlink fails.
func pathAllowed(path string, roots []string) bool {
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	for _, root := range roots {
		rr, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		if real == rr || strings.HasPrefix(real, strings.TrimRight(rr, "/")+"/") {
			return true
		}
	}
	return false
}

// resolveFilePath unseals a ticket's file blob to a path and confirms it hashes
// back to the ticket's ref (FileTicketServer::path). Only the node-owned prefix
// "n." resolves on a node owner; "m." (MAIN-owned) and anything else refuse.
func (a *Agent) resolveFilePath(file, ref string) (string, bool) {
	if !strings.HasPrefix(file, "n.") {
		return "", false
	}
	blob, err := base64.RawURLEncoding.DecodeString(file[2:])
	if err != nil {
		return "", false
	}
	plain, err := cc.Open(a.Client.State.NodeBoxSk, "file", ref, blob)
	if err != nil || len(plain) == 0 {
		return "", false
	}
	path := string(plain)
	if !hmac.Equal([]byte(ref), []byte(cc.FileRef(a.Client.State.ServerID, path))) {
		return "", false
	}
	return path, true
}

// openChunk opens path symlink-safe (O_NOFOLLOW on the final component), stats
// the opened fd, and returns min(n, size-offset) bytes at offset. total is the
// file size (for Content-Range on a 416). offset>size returns errOffsetPastEnd;
// offset==size returns an empty chunk and no error. The fd is stat'd directly —
// no re-resolve — so the check and the read see the same file.
func openChunk(path string, offset, n int64) (chunk []byte, total int64, err error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !fi.Mode().IsRegular() {
		return nil, 0, errors.New("not a regular file")
	}
	total = fi.Size()
	if offset > total {
		return nil, total, errOffsetPastEnd
	}
	want := n
	if want > total-offset {
		want = total - offset
	}
	if want <= 0 {
		return nil, total, nil
	}
	buf := make([]byte, want)
	m, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, total, err
	}
	return buf[:m], total, nil
}
