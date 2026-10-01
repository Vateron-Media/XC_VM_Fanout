package server

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
)

// Files the panel serves from disk — a movie (VOD) or a run of archive minutes
// (timeshift) — handed to the daemon like a live viewer, so no PHP-FPM worker
// is held for the length of a download. PHP authenticates the viewer, records
// its connection with pid 0, writes a manifest naming the parts into the
// files directory, and X-Accel-Redirects nginx to /file/<id>?c=<uuid>&m=<name>.
// The daemon reads and removes the manifest, then serves the parts back to
// back: HTTP ranges (http.ServeContent), the panel's throttle, a write deadline
// and the kill channel. The viewer is counted in /connections, /rates and the
// kick like a live one, so fanout_sync, the agent's registry and a drop handle
// it unchanged.
//
// No path ever travels in a URL: the manifest's name is 32 hex characters, and
// only the panel (xc_vm) writes the directory.

// filePart is one span of one file: Length -1 runs to the file's end. A
// part with a URL instead (a direct-proxy movie) is the manifest's only part:
// the daemon fetches it from its source, the viewer's range passed on.
type filePart struct {
	Path   string `json:"path"`
	URL    string `json:"url,omitempty"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

// fileSourceUA is the User-Agent a direct-proxy movie's source is asked with,
// the panel's as before.
const fileSourceUA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:101.0) Gecko/20100101 Firefox/101.0"

// fileSourceClient fetches direct-proxy movies. Their sources commonly serve
// self-signed certificates (the panel's cURL relay never verified them); a
// download may run for hours, so only the dial and the headers are timed.
var fileSourceClient = &http.Client{Transport: &http.Transport{
	TLSClientConfig:       &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // as the panel's relay, see above
	DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	ResponseHeaderTimeout: 30 * time.Second,
	MaxIdleConnsPerHost:   4,
}}

// fileManifest is what the panel wrote for one request.
type fileManifest struct {
	Type      string     `json:"type"`
	Parts     []filePart `json:"parts"`
	LimitPerc int        `json:"limit_perc"` // throttle after this share of the response, 0 = from the start
	Rate      int64      `json:"rate"`       // bytes/s once throttled, 0 = never throttled
	Range     string     `json:"range"`      // a Range the token carried, used when the request has none
	Expires   int64      `json:"expires"`    // unix seconds: a manifest left unread past it is refused
}

var manifestName = regexp.MustCompile(`^[0-9a-f]{32}$`)

// fileChunk bounds one write, so the throttle, the deadline and a kill act
// between chunks rather than after a whole buffer.
const fileChunk = 64 << 10

// manifestTTL is how long an unread manifest is kept by the sweep.
const manifestTTL = 2 * time.Minute

// errFileKilled ends a file viewer the panel dropped.
var errFileKilled = errors.New("dropped by the panel")

// SetFilesDir sets the directory the panel writes file manifests to.
func (m *Manager) SetFilesDir(dir string) { m.filesDir = dir }

// fileStream is the bookkeeping holder for the file viewers of panel id id:
// a Stream with no hub, used only for its connection set, so the viewers are
// listed, rated and dropped by the same code as live ones.
func (m *Manager) fileStream(id string, uuid string) (*Stream, *connStat) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.files == nil {
		m.files = make(map[string]*Stream)
	}
	st := m.files[id]
	if st == nil {
		st = &Stream{id: id, mgr: m}
		m.files[id] = st
	}
	return st, st.addConn(uuid)
}

// releaseFileStream forgets id's holder once its last viewer left.
func (m *Manager) releaseFileStream(id string, st *Stream) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st.connMu.Lock()
	empty := len(st.conns) == 0
	st.connMu.Unlock()
	if empty && m.files[id] == st {
		delete(m.files, id)
	}
}

// connStreams is every holder of viewers: the live streams and the file ones.
func (m *Manager) connStreams() []*Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Stream, 0, len(m.streams)+len(m.files))
	for _, st := range m.streams {
		out = append(out, st)
	}
	for _, st := range m.files {
		out = append(out, st)
	}
	return out
}

// readManifest takes the manifest name names: read, removed, and checked.
func (m *Manager) readManifest(name string, now time.Time) (*fileManifest, int) {
	if m.filesDir == "" || !manifestName.MatchString(name) {
		return nil, http.StatusNotFound
	}
	path := filepath.Join(m.filesDir, name+".json")
	raw, err := os.ReadFile(path)
	_ = os.Remove(path) // used once, whatever it holds
	if err != nil {
		return nil, http.StatusNotFound
	}
	var mf fileManifest
	if json.Unmarshal(raw, &mf) != nil || len(mf.Parts) == 0 || mf.Expires < now.Unix() || mf.LimitPerc < 0 || mf.LimitPerc > 100 || mf.Rate < 0 {
		return nil, http.StatusNotFound
	}
	if mf.Parts[0].URL != "" {
		u, err := url.Parse(mf.Parts[0].URL)
		if len(mf.Parts) != 1 || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || mf.Parts[0].Path != "" {
			return nil, http.StatusBadRequest
		}
		return &mf, 0
	}
	for _, p := range mf.Parts {
		if p.URL != "" || !filepath.IsAbs(p.Path) || filepath.Clean(p.Path) != p.Path || p.Offset < 0 || p.Length < -1 {
			return nil, http.StatusBadRequest
		}
	}
	return &mf, 0
}

// openParts opens the manifest's parts as one seekable body, and its newest
// modification time. Every file must be a regular file holding its span.
func openParts(parts []filePart) (*partsReader, time.Time, []*os.File, error) {
	var files []*os.File
	pr := &partsReader{}
	var mod time.Time
	for _, p := range parts {
		f, err := os.Open(p.Path)
		if err != nil {
			closeAll(files)
			return nil, mod, nil, err
		}
		files = append(files, f)
		fi, err := f.Stat()
		if err != nil || !fi.Mode().IsRegular() || p.Offset > fi.Size() {
			closeAll(files)
			return nil, mod, nil, os.ErrNotExist
		}
		n := p.Length
		if n < 0 || p.Offset+n > fi.Size() {
			n = fi.Size() - p.Offset
		}
		pr.parts = append(pr.parts, io.NewSectionReader(f, p.Offset, n))
		pr.size += n
		if fi.ModTime().After(mod) {
			mod = fi.ModTime()
		}
	}
	return pr, mod, files, nil
}

func closeAll(files []*os.File) {
	for _, f := range files {
		_ = f.Close()
	}
}

// serveFile is the client handler for /file/<id>.
func (m *Manager) serveFile(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/file/")
	mf, code := m.readManifest(r.URL.Query().Get("m"), time.Now())
	if mf == nil || id == "" {
		http.Error(w, http.StatusText(code), code)
		return
	}

	uuid := r.URL.Query().Get("c")
	fw := &fileWriter{ResponseWriter: w, rc: http.NewResponseController(w), m: m, limitPerc: mf.LimitPerc, rate: mf.Rate, done: r.Context().Done()}
	if uuid != "" {
		st, cs := m.fileStream(id, uuid)
		defer m.releaseFileStream(id, st)
		defer st.removeConn(uuid)
		fw.cs = cs
		fw.kill = cs.kill
	}
	if r.Header.Get("Range") == "" && mf.Range != "" {
		r.Header.Set("Range", mf.Range)
	}
	if mf.Type != "" {
		w.Header().Set("Content-Type", mf.Type)
	}
	w.Header().Set("Cache-Control", "no-store")
	start := time.Now()
	if mf.Parts[0].URL != "" {
		serveSource(fw, r, mf.Parts[0].URL)
	} else {
		body, mod, files, err := openParts(mf.Parts)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer closeAll(files)
		http.ServeContent(fw, r, "", mod, body)
	}
	dlog.Logf("viewer", "id=%s file detach uuid=%s dur=%s sent=%dKB", id, uuid, time.Since(start).Round(time.Millisecond), fw.written/1024)
}

// serveSource relays a direct-proxy movie from its source: the viewer's range
// asked for, the source's status and range headers passed on, the body
// written through fw (its throttle, deadline and kill).
func serveSource(fw *fileWriter, r *http.Request, src string) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, src, nil)
	if err != nil {
		http.Error(fw, "bad source", http.StatusBadGateway)
		return
	}
	req.Header.Set("User-Agent", fileSourceUA)
	if rg := r.Header.Get("Range"); rg != "" {
		req.Header.Set("Range", rg)
	}
	resp, err := fileSourceClient.Do(req)
	if err != nil {
		http.Error(fw, "source unreachable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
	default:
		http.Error(fw, "source answered "+strconv.Itoa(resp.StatusCode), http.StatusBadGateway)
		return
	}
	for _, h := range []string{"Content-Length", "Content-Range", "Accept-Ranges"} {
		if v := resp.Header.Get(h); v != "" {
			fw.Header().Set(h, v)
		}
	}
	fw.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(fw, resp.Body)
}

// fileWriter carries one file response: written in chunks, each under the
// write deadline, paced to rate once limitPerc of the response went out, and
// ended at once when the panel drops the viewer.
type fileWriter struct {
	http.ResponseWriter
	rc        *http.ResponseController
	m         *Manager
	cs        *connStat
	kill      <-chan struct{} // nil without a uuid: never ready
	done      <-chan struct{}
	limitPerc int
	rate      int64
	limitAt   int64 // bytes before the throttle, from the response's length
	written   int64
	paceStart time.Time
	paced     int64
}

func (fw *fileWriter) WriteHeader(code int) {
	if n, err := strconv.ParseInt(fw.Header().Get("Content-Length"), 10, 64); err == nil {
		fw.limitAt = n * int64(fw.limitPerc) / 100
	}
	fw.ResponseWriter.WriteHeader(code)
}

func (fw *fileWriter) Write(b []byte) (int, error) {
	total := 0
	for len(b) > 0 {
		select {
		case <-fw.kill:
			return total, errFileKilled
		default:
		}
		n := len(b)
		if n > fileChunk {
			n = fileChunk
		}
		if fw.rate > 0 && fw.written >= fw.limitAt {
			if err := fw.pace(n); err != nil {
				return total, err
			}
		}
		_ = fw.rc.SetWriteDeadline(time.Now().Add(time.Duration(fw.m.writeTimeout.Load())))
		k, err := fw.ResponseWriter.Write(b[:n])
		total += k
		fw.written += int64(k)
		if fw.cs != nil {
			fw.cs.bytes.Add(int64(k))
		}
		if err != nil {
			return total, err
		}
		b = b[n:]
	}
	return total, nil
}

// pace holds the next n bytes until the throttled part of the response is no
// further ahead of rate than they are.
func (fw *fileWriter) pace(n int) error {
	if fw.paceStart.IsZero() {
		fw.paceStart = time.Now()
	}
	due := time.Duration(float64(fw.paced) / float64(fw.rate) * float64(time.Second))
	fw.paced += int64(n)
	wait := due - time.Since(fw.paceStart)
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-fw.kill:
		return errFileKilled
	case <-fw.done:
		return errors.New("client gone")
	}
}

// partsReader reads spans of files back to back as one io.ReadSeeker.
type partsReader struct {
	parts []*io.SectionReader
	size  int64
	pos   int64
}

func (p *partsReader) Read(b []byte) (int, error) {
	if p.pos >= p.size {
		return 0, io.EOF
	}
	var base int64
	for _, s := range p.parts {
		if p.pos < base+s.Size() {
			want := base + s.Size() - p.pos
			if int64(len(b)) > want {
				b = b[:want]
			}
			n, err := s.ReadAt(b, p.pos-base)
			p.pos += int64(n)
			if err == io.EOF && n > 0 {
				err = nil
			}
			return n, err
		}
		base += s.Size()
	}
	return 0, io.EOF
}

func (p *partsReader) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		off += p.pos
	case io.SeekEnd:
		off += p.size
	default:
		return 0, errors.New("bad whence")
	}
	if off < 0 {
		return 0, errors.New("negative position")
	}
	p.pos = off
	return off, nil
}

// SweepManifests removes manifests nothing read within manifestTTL (a request
// nginx never forwarded), every minute until ctx ends.
func (m *Manager) SweepManifests(done <-chan struct{}) {
	if m.filesDir == "" {
		return
	}
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				m.sweepManifests(time.Now())
			}
		}
	}()
}

func (m *Manager) sweepManifests(now time.Time) {
	entries, err := os.ReadDir(m.filesDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		if fi, err := e.Info(); err == nil && now.Sub(fi.ModTime()) > manifestTTL {
			_ = os.Remove(filepath.Join(m.filesDir, e.Name()))
		}
	}
}
