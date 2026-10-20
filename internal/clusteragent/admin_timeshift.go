package clusteragent

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// timeshiftBuf is the copy buffer for the concatenated-TS relay (chunking only;
// does not affect correctness). PHP uses the read_buffer_size setting.
const timeshiftBuf = 256 * 1024

// timeshiftStartTimestamp ports StreamUtils::timeshiftStartTimestamp: a numeric
// unix ts, or "YYYYMMDD-H", or "Y-m-d:H-i" interpreted as wall-clock in the panel
// timezone (loc). Returns 0 when it does not parse.
func timeshiftStartTimestamp(s string, loc *time.Location) int64 {
	s = strings.TrimSpace(s)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	atoi := func(x string) int { n, _ := strconv.Atoi(strings.TrimSpace(x)); return n }
	if strings.Count(s, "-") == 1 {
		p := strings.SplitN(s, "-", 2) // YYYYMMDD-H
		if len(p[0]) < 8 {
			return 0
		}
		d := p[0]
		return time.Date(atoi(d[0:4]), time.Month(atoi(d[4:6])), atoi(d[6:8]), atoi(p[1]), 0, 0, 0, loc).Unix()
	}
	dt := strings.SplitN(s, ":", 2) // Y-m-d:H-i
	if len(dt) != 2 {
		return 0
	}
	ymd := strings.Split(dt[0], "-")
	hm := strings.Split(dt[1], "-")
	if len(ymd) != 3 || len(hm) != 2 {
		return 0
	}
	return time.Date(atoi(ymd[0]), time.Month(atoi(ymd[1])), atoi(ymd[2]), atoi(hm[0]), atoi(hm[1]), 0, 0, loc).Unix()
}

// sanitizeSegmentName is StreamUtils::sanitizeSegmentName: urldecode, then strip
// every "/" and "\\" so a segment name can only be a bare file in the archive dir.
func sanitizeSegmentName(s string) string {
	if d, err := url.QueryUnescape(s); err == nil {
		s = d
	}
	s = strings.ReplaceAll(s, "/", "")
	s = strings.ReplaceAll(s, "\\", "")
	return s
}

type tsSeg struct {
	path string
	size int64
}

// serveAdminTimeshift ports Public/admin/timeshift.php: a relay/uitoken/password
// -admitted catch-up read. A uitoken forces extension=m3u8 and carries
// start/duration; a relay admission may only pull TS (relay + m3u8 → 404). With a
// `segment` it serves one archive file; else it builds the minute-file queue
// (ARCHIVE_PATH/<id>/<Y-m-d:H-i>.ts across [start, start+duration)) and answers
// either the VOD playlist (m3u8) or the concatenated byte-range stream (ts).
func (a *Agent) serveAdminTimeshift(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	clientIP := r.Header.Get("X-XC-Client-IP")
	adm, ok := a.admitAdmin(parsePositiveInt(q.Get("stream")), r, clientIP, true, a.Client.MainNowMs())
	if !ok {
		http.NotFound(w, r)
		return
	}
	ext := q.Get("extension")
	start, duration := q.Get("start"), q.Get("duration")
	if adm.kind == admitUitoken {
		ext = "m3u8" // the panel's uitoken path always serves the playlist
		if adm.tok != nil {
			if adm.tok.Start != "" {
				start = adm.tok.Start
			}
			if adm.tok.Duration != "" {
				duration = adm.tok.Duration
			}
		}
	}
	if adm.kind == admitRelayKind && ext == "m3u8" {
		http.NotFound(w, r) // the m3u8's segment URLs carry the password; a relay pulls TS only
		return
	}
	pol, ok := a.adminPolicy()
	if !ok {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.streamMeta(strconv.FormatInt(adm.streamID, 10)); !ok {
		http.NotFound(w, r) // StreamSource row required
		return
	}
	archiveDir := filepath.Join(pol.archivePath, strconv.FormatInt(adm.streamID, 10))

	// A named segment (an m3u8 sub-request): serve that one archive file.
	if segment := q.Get("segment"); segment != "" {
		name := sanitizeSegmentName(segment)
		path := filepath.Join(archiveDir, name)
		if name == "" || !withinDir(archiveDir, path) {
			http.NotFound(w, r)
			return
		}
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
		w.Header().Set("Content-Type", "video/mp2t")
		http.ServeContent(w, r, path, fi.ModTime(), f)
		return
	}

	// Build the minute-file queue from start + duration.
	loc := time.UTC
	if pol.defaultTimezone != "" {
		if l, err := time.LoadLocation(pol.defaultTimezone); err == nil {
			loc = l
		}
	}
	ts := timeshiftStartTimestamp(start, loc)
	dur := int(parsePositiveInt(duration))
	if adm.streamID <= 0 || ts <= 0 || dur <= 0 {
		http.NotFound(w, r)
		return
	}
	archiveName := func(sec int64) string { return time.Unix(sec, 0).UTC().Format("2006-01-02:15-04") + ".ts" }
	if st, err := os.Stat(filepath.Join(archiveDir, archiveName(ts))); err != nil || st.IsDir() {
		http.NotFound(w, r) // the first minute must exist (and be readable)
		return
	}
	var queue []tsSeg
	for i := 0; i < dur; i++ {
		p := filepath.Join(archiveDir, archiveName(ts+int64(i)*60))
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			queue = append(queue, tsSeg{p, st.Size()})
		}
	}
	if len(queue) == 0 {
		http.NotFound(w, r)
		return
	}

	if ext == "m3u8" {
		var b strings.Builder
		b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:60\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n")
		for _, s := range queue {
			b.WriteString("#EXTINF:60.0,\n")
			base := filepath.Base(s.path)
			if ui := q.Get("uitoken"); ui != "" {
				b.WriteString("/admin/timeshift?extension=m3u8&segment=" + base + "&uitoken=" + ui + "\n")
			} else {
				b.WriteString("/admin/timeshift?extension=m3u8&stream=" + strconv.FormatInt(adm.streamID, 10) +
					"&segment=" + base + "&password=" + a.secret("live_streaming_pass") + "\n")
			}
		}
		b.WriteString("#EXT-X-ENDLIST")
		out := b.String()
		w.Header().Set("Content-Type", "application/x-mpegurl")
		w.Header().Set("Content-Length", strconv.Itoa(len(out)))
		_, _ = io.WriteString(w, out)
		return
	}

	a.serveTimeshiftConcat(w, r, queue)
}

// serveTimeshiftConcat streams the archive queue as one video/mp2t body with HTTP
// byte-range support (single range only; 206/416), seeking across the files —
// the ts arm of timeshift.php.
func (a *Agent) serveTimeshiftConcat(w http.ResponseWriter, r *http.Request, queue []tsSeg) {
	var size int64
	for _, s := range queue {
		size += s.size
	}
	w.Header().Set("Content-Type", "video/mp2t")
	w.Header().Set("Accept-Ranges", "0-"+strconv.FormatInt(size, 10)) // faithful to timeshift.php's literal value
	start, end := int64(0), size-1
	status := http.StatusOK
	bad := func() {
		w.Header().Set("Content-Range", "bytes 0-"+strconv.FormatInt(size-1, 10)+"/"+strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}
	if rh := r.Header.Get("Range"); rh != "" {
		spec := rh
		if i := strings.IndexByte(spec, '='); i >= 0 {
			spec = spec[i+1:]
		}
		if strings.Contains(spec, ",") {
			bad()
			return
		}
		rs, re := int64(0), end
		switch {
		case spec == "" || spec == "-":
			// degenerate; fall through as the full body
		case strings.HasPrefix(spec, "-"):
			n, err := strconv.ParseInt(spec[1:], 10, 64)
			if err != nil {
				bad()
				return
			}
			rs = size - n
		default:
			ps := strings.SplitN(spec, "-", 2)
			v, err := strconv.ParseInt(ps[0], 10, 64)
			if err != nil {
				bad()
				return
			}
			rs = v
			if len(ps) == 2 && ps[1] != "" {
				if e, err := strconv.ParseInt(ps[1], 10, 64); err == nil {
					re = e
				} else {
					re = size
				}
			} else {
				re = size
			}
		}
		if end < re {
			re = end
		}
		if re < rs || size-1 < rs || size <= re {
			bad()
			return
		}
		start, end, status = rs, re, http.StatusPartialContent
	}
	w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.FormatInt(size, 10))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	remaining := end - start + 1
	var pos int64
	buf := make([]byte, timeshiftBuf)
	for _, s := range queue {
		if remaining <= 0 {
			break
		}
		fileStart, fileEnd := pos, pos+s.size
		pos = fileEnd
		if fileEnd <= start {
			continue // entirely before the requested range
		}
		f, err := os.Open(s.path)
		if err != nil {
			break // removed by the retention pass mid-stream
		}
		var off int64
		if start > fileStart {
			off = start - fileStart
		}
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			_ = f.Close()
			break
		}
		toCopy := s.size - off
		if toCopy > remaining {
			toCopy = remaining
		}
		n, _ := io.CopyBuffer(w, io.LimitReader(f, toCopy), buf)
		_ = f.Close()
		remaining -= n
	}
}
