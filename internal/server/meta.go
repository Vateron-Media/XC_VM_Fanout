// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

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
// copy of the ring, so it is not done per call: while something is still missing
// it is retried every metaRetry, and once everything is known it is refreshed
// only every metaRefresh. The bitrate is a running measurement and is refreshed
// on every read.
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

// metaRefresh is how often a stream whose metadata IS complete is read again.
//
// What sits under a stream id changes without this cache hearing about it: the
// supervisor fails over to a backup source, an operator forces another one, an
// encoder restarts with different settings. A complete entry that was never
// re-read therefore kept telling the panel that a 720p HEVC backup was still the
// 1080p H.264 primary, forever -- and the panel writes that answer into
// streams_servers, having retired the ffprobe that used to correct it. Five
// minutes is that ffprobe's own cadence, so the value can be stale for no longer
// than it ever was, at the cost of one ring snapshot per stream per five
// minutes.
const metaRefresh = 5 * time.Minute

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
	// Retry quickly while something is missing, then keep checking slowly: the
	// stream under this id can be replaced by another one at any time.
	interval := metaRetry
	if e.info.Complete() {
		interval = metaRefresh
	}
	needParse := e.lastParse.IsZero() || now.Sub(e.lastParse) >= interval
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
	if !prevAt.IsZero() && published >= prevBytes {
		if elapsed := now.Sub(prevAt); elapsed >= minBitrateWindow {
			e.kbps = int(float64(published-prevBytes) * 8 / elapsed.Seconds() / 1000)
			e.bytes, e.at = published, now
		}
	} else {
		// First reading, or the counter went BACKWARDS: publishedBytes belongs to
		// the Stream, and a stream torn down and re-created under the same id (a
		// panel re-registration, a teardown then a new viewer) starts a fresh one
		// at zero while this entry still holds the old sample. Differencing
		// across that boundary reported a negative bitrate_kbps to the panel.
		// Re-baseline instead and keep the last measured figure until the next
		// window produces a real one.
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
