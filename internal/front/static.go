package front

import (
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// serveFileRange serves one on-disk file, honouring Range requests (206) and
// returning 404 when it is missing or a directory. contentType, when non-empty,
// is set before ServeContent so it is not re-sniffed; empty lets ServeContent
// detect it. The caller is responsible for confining path to its root.
func serveFileRange(w http.ResponseWriter, r *http.Request, path, contentType string) {
	f, err := os.Open(path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || fi.IsDir() {
		http.NotFound(w, r)
		return
	}
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	http.ServeContent(w, r, fi.Name(), fi.ModTime(), f)
}

// contentTypeByExt maps the streaming/static extensions the front serves to the
// content types nginx sends; "" lets ServeContent sniff an unlisted type.
func contentTypeByExt(p string) string {
	switch strings.ToLower(filepath.Ext(p)) {
	case ".ts":
		return "video/mp2t"
	case ".m3u8":
		return "application/vnd.apple.mpegurl"
	case ".m4s":
		return "video/iso.segment"
	case ".mp4", ".m4v":
		return "video/mp4"
	case ".vtt":
		return "text/vtt"
	default:
		return ""
	}
}

// confine cleans a request remainder against a leading slash (so ".." cannot
// escape the root) and joins it under root, returning the full path and whether
// it names a file rather than the root or a directory marker. It mirrors the
// normalise-first discipline of serveImages.
func confine(root, remainder string) (string, bool) {
	clean := path.Clean("/" + remainder)
	if clean == "/" || strings.HasSuffix(clean, "/") {
		return "", false
	}
	return filepath.Join(root, filepath.FromSlash(clean)), true
}
