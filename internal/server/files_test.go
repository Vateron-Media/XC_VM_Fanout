package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// filesFixture is a manager with a files directory, its client and control
// servers, and a scratch directory for the files it serves.
type filesFixture struct {
	t     *testing.T
	mgr   *Manager
	cli   *httptest.Server
	ctl   *httptest.Server
	media string
}

func newFilesFixture(t *testing.T) *filesFixture {
	t.Helper()
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	mgr.SetFilesDir(t.TempDir())
	f := &filesFixture{t: t, mgr: mgr, cli: httptest.NewServer(mgr.ClientHandler()), ctl: httptest.NewServer(mgr.ControlHandler()), media: t.TempDir()}
	mgr.SetFileRoots([]string{f.media})
	t.Cleanup(func() { f.cli.Close(); f.ctl.Close() })
	return f
}

func (f *filesFixture) file(name, body string) string {
	p := filepath.Join(f.media, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		f.t.Fatal(err)
	}
	return p
}

// manifest writes mf under a fresh name and returns the name.
func (f *filesFixture) manifest(mf fileManifest) string {
	if mf.Expires == 0 {
		mf.Expires = time.Now().Add(time.Minute).Unix()
	}
	raw, _ := json.Marshal(mf)
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	name := hex.EncodeToString(id)
	if err := os.WriteFile(filepath.Join(f.mgr.filesDir, name+".json"), raw, 0o600); err != nil {
		f.t.Fatal(err)
	}
	return name
}

func (f *filesFixture) get(path string, hdr map[string]string) (*http.Response, string) {
	req, _ := http.NewRequest(http.MethodGet, f.cli.URL+path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res, string(b)
}

func TestAFileIsServedWholeAndItsManifestUsedOnce(t *testing.T) {
	f := newFilesFixture(t)
	name := f.manifest(fileManifest{Type: "video/mp4", Parts: []filePart{{Path: f.file("m.mp4", "0123456789"), Length: -1}}})

	res, body := f.get("/file/7?c=u1&m="+name, nil)
	if res.StatusCode != 200 || body != "0123456789" || res.Header.Get("Content-Type") != "video/mp4" || res.Header.Get("Content-Length") != "10" {
		t.Fatalf("got %d %q %v", res.StatusCode, body, res.Header)
	}
	if res, _ := f.get("/file/7?c=u1&m="+name, nil); res.StatusCode != 404 {
		t.Fatalf("a manifest is read once, got %d", res.StatusCode)
	}
}

func TestARangeSpansTheParts(t *testing.T) {
	f := newFilesFixture(t)
	// Timeshift: the first minute from its offset, then the next whole.
	name := f.manifest(fileManifest{Type: "video/mp2t", Parts: []filePart{
		{Path: f.file("a.ts", "xxABCDE"), Offset: 2, Length: -1},
		{Path: f.file("b.ts", "FGHIJ"), Length: -1},
	}})
	res, body := f.get("/file/9?m="+name, map[string]string{"Range": "bytes=3-6"})
	if res.StatusCode != 206 || body != "DEFG" || res.Header.Get("Content-Range") != "bytes 3-6/10" {
		t.Fatalf("got %d %q %q", res.StatusCode, body, res.Header.Get("Content-Range"))
	}
	// The token's range stands in for a request without one.
	name = f.manifest(fileManifest{Parts: []filePart{{Path: f.file("c.ts", "0123456789"), Length: -1}}, Range: "bytes=8-"})
	if res, body := f.get("/file/9?m="+name, nil); res.StatusCode != 206 || body != "89" {
		t.Fatalf("got %d %q", res.StatusCode, body)
	}
}

func TestAManifestTheDaemonCannotTrustIsRefused(t *testing.T) {
	f := newFilesFixture(t)
	p := f.file("m.mp4", "data")
	for what, mf := range map[string]fileManifest{
		"relative path": {Parts: []filePart{{Path: "m.mp4", Length: -1}}},
		"unclean path":  {Parts: []filePart{{Path: f.media + "/../" + filepath.Base(f.media) + "/m.mp4", Length: -1}}},
		"expired":       {Parts: []filePart{{Path: p, Length: -1}}, Expires: time.Now().Add(-time.Second).Unix()},
		"no parts":      {},
		"a directory":   {Parts: []filePart{{Path: f.media, Length: -1}}},
		"past its end":  {Parts: []filePart{{Path: p, Offset: 9, Length: -1}}},
		"outside roots": {Parts: []filePart{{Path: "/etc/hostname", Length: -1}}},
		"the root":      {Parts: []filePart{{Path: f.media, Length: -1}}},
	} {
		if res, _ := f.get("/file/1?m="+f.manifest(mf), nil); res.StatusCode < 400 {
			t.Errorf("%s: got %d", what, res.StatusCode)
		}
	}
	if res, _ := f.get("/file/1?m=../../etc/passwd", nil); res.StatusCode != 404 {
		t.Errorf("a name that is not one: got %d", res.StatusCode)
	}
}

func TestAFileViewerIsListedRatedAndDroppedLikeALiveOne(t *testing.T) {
	f := newFilesFixture(t)
	// 1 MB paced at 64 KB/s from the start: it is still running when looked at.
	name := f.manifest(fileManifest{Parts: []filePart{{Path: f.file("big.ts", strings.Repeat("x", 1<<20)), Length: -1}}, Rate: 64 << 10})
	done := make(chan string, 1)
	go func() {
		res, err := http.Get(f.cli.URL + "/file/11?c=vod-uuid&m=" + name)
		if err != nil {
			done <- err.Error()
			return
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		done <- string(b)
	}()

	listed := func() bool {
		res, err := http.Get(f.ctl.URL + "/connections?detail=1")
		if err != nil {
			return false
		}
		defer res.Body.Close()
		var d []connDetail
		_ = json.NewDecoder(res.Body).Decode(&d)
		return len(d) == 1 && d[0].UUID == "vod-uuid" && d[0].StreamID == "11"
	}
	deadline := time.Now().Add(2 * time.Second)
	for !listed() {
		if time.Now().After(deadline) {
			t.Fatal("the file viewer is never listed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if res, _ := http.Get(f.ctl.URL + "/rates"); res == nil || res.StatusCode != 200 {
		t.Fatal("rates")
	}

	req, _ := http.NewRequest(http.MethodDelete, f.ctl.URL+"/connections/vod-uuid", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != http.StatusNoContent {
		t.Fatalf("drop: %v %v", res, err)
	}
	select {
	case body := <-done:
		if len(body) >= 1<<20 {
			t.Fatalf("the dropped viewer got the whole file")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the dropped viewer is still served")
	}
	deadline = time.Now().Add(time.Second)
	for {
		f.mgr.mu.Lock()
		n := len(f.mgr.files)
		f.mgr.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the holder outlives its last viewer")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTheThrottleStartsAfterItsShare(t *testing.T) {
	f := newFilesFixture(t)
	// 256 KB: the first half free, the second at 512 KB/s (about 0.25 s).
	name := f.manifest(fileManifest{Parts: []filePart{{Path: f.file("t.ts", strings.Repeat("y", 256<<10)), Length: -1}}, LimitPerc: 50, Rate: 512 << 10})
	start := time.Now()
	res, body := f.get("/file/3?m="+name, nil)
	took := time.Since(start)
	if res.StatusCode != 200 || len(body) != 256<<10 {
		t.Fatalf("got %d, %d bytes", res.StatusCode, len(body))
	}
	if took < 150*time.Millisecond || took > 2*time.Second {
		t.Fatalf("paced half took %s, want about 0.25 s", took)
	}
}

func TestUnreadManifestsAreSwept(t *testing.T) {
	f := newFilesFixture(t)
	name := f.manifest(fileManifest{Parts: []filePart{{Path: f.file("s.ts", "z"), Length: -1}}})
	old := time.Now().Add(-3 * time.Minute)
	_ = os.Chtimes(filepath.Join(f.mgr.filesDir, name+".json"), old, old)
	f.mgr.sweepManifests(time.Now())
	if _, err := os.Stat(filepath.Join(f.mgr.filesDir, name+".json")); !os.IsNotExist(err) {
		t.Fatal("an unread manifest past its TTL is kept")
	}
}

func TestADirectProxyMovieIsRelayedFromItsSourceWithTheViewersRange(t *testing.T) {
	f := newFilesFixture(t)
	movie := strings.Repeat("0123456789", 1000)
	var gotRange, gotUA string
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange, gotUA = r.Header.Get("Range"), r.Header.Get("User-Agent")
		http.ServeContent(w, r, "m.mp4", time.Time{}, strings.NewReader(movie))
	}))
	defer src.Close()

	name := f.manifest(fileManifest{Type: "video/mp4", Parts: []filePart{{URL: src.URL + "/m.mp4"}}})
	res, body := f.get("/file/8?c=u8&m="+name, map[string]string{"Range": "bytes=10-19"})
	if res.StatusCode != 206 || body != movie[10:20] || res.Header.Get("Content-Range") != "bytes 10-19/10000" || res.Header.Get("Content-Type") != "video/mp4" {
		t.Fatalf("got %d %q %q", res.StatusCode, body, res.Header.Get("Content-Range"))
	}
	if gotRange != "bytes=10-19" || gotUA != fileSourceUA {
		t.Fatalf("the source was asked with %q, %q", gotRange, gotUA)
	}

	// A source that fails is the viewer's 502; a manifest naming anything but
	// one http(s) source is refused.
	src.Close()
	if res, _ := f.get("/file/8?m="+f.manifest(fileManifest{Parts: []filePart{{URL: src.URL}}}), nil); res.StatusCode != http.StatusBadGateway {
		t.Fatalf("source down: got %d", res.StatusCode)
	}
	for _, mf := range []fileManifest{
		{Parts: []filePart{{URL: "file:///etc/passwd"}}},
		{Parts: []filePart{{URL: "http://a/1"}, {URL: "http://a/2"}}},
		{Parts: []filePart{{URL: "http://a/1", Path: "/etc/passwd"}}},
	} {
		if res, _ := f.get("/file/8?m="+f.manifest(mf), nil); res.StatusCode != http.StatusBadRequest {
			t.Errorf("%+v: got %d", mf.Parts, res.StatusCode)
		}
	}
}
