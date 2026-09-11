package server

import (
	"sync"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsmeta"
)

// StreamMeta is the descriptive metadata the panel stores per stream
// (streams_servers.video_codec, audio_codec, resolution, bitrate).
//
// The panel obtained all of it by running ffprobe against a segment on disk --
// once at start-up and again every five minutes for the life of the stream. Every
// answer is already in the bytes passing through the fan-out, so this reads them
// there: the codecs and picture size out of the PMT and sequence header, and the
// bitrate by simply measuring, which is more accurate than ffprobe's estimate
// from a single segment.
//
// A zero field means "not determined". The panel keeps whatever it already had
// rather than overwriting a correct value with a guess.
type StreamMeta struct {
	VideoCodec string `json:"video_codec,omitempty"`
	AudioCodec string `json:"audio_codec,omitempty"`
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
	// BitrateKbps is measured over the recent window, in kbit/s.
	BitrateKbps int `json:"bitrate_kbps,omitempty"`
}

// metaCache holds what has been determined per stream.
//
// Codecs and picture size are read from a clean-join snapshot, which costs a
// copy of the ring, so it is done once and then only retried while something is
// still missing -- a stream whose metadata is complete is never re-parsed. The
// bitrate is a running measurement and is refreshed on every read.
type metaCache struct {
	mu      sync.Mutex
	entries map[string]*metaEntry
}

type metaEntry struct {
	info      tsmeta.Info
	lastParse time.Time

	// bytes/at are the previous byte-count sample the bitrate is differenced
	// against.
	bytes int64
	at    time.Time
	kbps  int
}

func newMetaCache() *metaCache { return &metaCache{entries: make(map[string]*metaEntry)} }

func (c *metaCache) forget(id string) {
	c.mu.Lock()
	delete(c.entries, id)
	c.mu.Unlock()
}

// metaRetry is how long to wait before re-reading a stream whose metadata came
// back incomplete -- a stream that has not yet carried a sequence header, say.
const metaRetry = 15 * time.Second

// minBitrateWindow is the shortest interval a bitrate is worth computing over.
const minBitrateWindow = 2 * time.Second

// StreamMetadata returns what is known about a stream, or false when this node
// does not have it.
func (m *Manager) StreamMetadata(id string) (StreamMeta, bool) {
	st := m.Get(id)
	if st == nil {
		return StreamMeta{}, false
	}
	if m.meta == nil {
		return StreamMeta{}, false
	}

	now := time.Now()
	c := m.meta

	c.mu.Lock()
	e := c.entries[id]
	if e == nil {
		e = &metaEntry{}
		c.entries[id] = e
	}
	needParse := !e.info.Complete() && (e.lastParse.IsZero() || now.Sub(e.lastParse) >= metaRetry)
	prevBytes, prevAt := e.bytes, e.at
	c.mu.Unlock()

	// Parsing needs a snapshot, which copies out of the ring -- do it outside the
	// cache lock, and only when something is still missing.
	var parsed tsmeta.Info
	if needParse {
		snap := st.Hub.Snapshot(0)
		parsed = tsmeta.Parse(snap)
	}

	published := st.publishedBytes.Load()

	c.mu.Lock()
	defer c.mu.Unlock()
	e = c.entries[id]
	if e == nil { // released while we were parsing
		return StreamMeta{}, false
	}
	if needParse {
		e.lastParse = now
		// Merge rather than replace: a later snapshot that happens to miss the
		// sequence header must not erase a size already determined.
		if parsed.VideoCodec != "" {
			e.info.VideoCodec = parsed.VideoCodec
		}
		if parsed.AudioCodec != "" {
			e.info.AudioCodec = parsed.AudioCodec
		}
		if parsed.Height > 0 {
			e.info.Width, e.info.Height = parsed.Width, parsed.Height
		}
	}
	if !prevAt.IsZero() {
		if elapsed := now.Sub(prevAt); elapsed >= minBitrateWindow {
			e.kbps = int(float64(published-prevBytes) * 8 / elapsed.Seconds() / 1000)
			e.bytes, e.at = published, now
		}
	} else {
		e.bytes, e.at = published, now
	}

	return StreamMeta{
		VideoCodec:  e.info.VideoCodec,
		AudioCodec:  e.info.AudioCodec,
		Width:       e.info.Width,
		Height:      e.info.Height,
		BitrateKbps: e.kbps,
	}, true
}
