// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// masterServer serves a master playlist, one media playlist per variant, and
// TS segments, recording every path it was asked for.
type masterServer struct {
	*httptest.Server
	mu   sync.Mutex
	got  []string
	body map[string]string
}

func newMasterServer(t *testing.T, body map[string]string) *masterServer {
	t.Helper()
	m := &masterServer{body: body}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		m.mu.Lock()
		m.got = append(m.got, name)
		m.mu.Unlock()
		if strings.HasSuffix(name, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(0))
			return
		}
		pl, ok := m.body[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, pl)
	}))
	t.Cleanup(m.Close)
	return m
}

func (m *masterServer) paths() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.got...)
}

// TestMasterWithSeparateAudioRenditionIsRefused: an EXT-X-MEDIA rendition that
// carries its own URI lives OUTSIDE the variant (RFC 8216), so the variant's TS
// segments hold video only. The parser skipped EXT-X-MEDIA as a tag it did not
// model and pickVariant looked only at EXT-X-STREAM-INF, so such a master was
// accepted and the channel went out silent — no audio, no error, and no
// fallback to ffmpeg, which would have muxed the rendition in. That breaks the
// package's refuse-loudly-rather-than-half-serve contract.
func TestMasterWithSeparateAudioRenditionIsRefused(t *testing.T) {
	srv := newMasterServer(t, map[string]string{
		"master.m3u8": "#EXTM3U\n" +
			"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",DEFAULT=YES,URI=\"audio.m3u8\"\n" +
			"#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"aud\"\nvideo.m3u8\n",
		"video.m3u8": "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nv1.ts\n",
		"audio.m3u8": "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\na1.ts\n",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/master.m3u8", Options{})
	if err == nil {
		_ = rc.Close()
		t.Fatalf("a master whose audio is a separate rendition was accepted; it would be "+
			"fanned out video-only. Fetched: %v", srv.paths())
	}
	if !IsFormat(err) {
		t.Fatalf("err = %v, want a format refusal so the caller falls back to ffmpeg", err)
	}
}

// TestMasterWithMuxedAudioRenditionIsServed is the other half: an EXT-X-MEDIA
// entry with NO URI describes audio that is already muxed into the variant's
// own segments. That is the common IPTV shape and must keep working — the
// refusal above must key on the rendition having a URI, not on the tag.
func TestMasterWithMuxedAudioRenditionIsServed(t *testing.T) {
	srv := newMasterServer(t, map[string]string{
		"master.m3u8": "#EXTM3U\n" +
			"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",DEFAULT=YES\n" +
			"#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"aud\"\nvideo.m3u8\n",
		"video.m3u8": "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nv1.ts\n#EXT-X-ENDLIST\n",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/master.m3u8", Options{})
	if err != nil {
		t.Fatalf("a master with muxed audio was refused: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if len(got) == 0 || got[0] != 0x47 {
		t.Fatalf("muxed-audio master delivered %d bytes", len(got))
	}
}

// TestMasterWithAMixedAudioGroupIsServed: RFC 8216 makes URI optional on an
// EXT-X-MEDIA AUDIO rendition, and a multi-language channel routinely ships one
// group holding both — the default language muxed into the variant's own
// segments (no URI) and the alternates as separate renditions (URI). The
// variant's TS therefore DOES carry audio. Marking the whole group demuxed as
// soon as any one rendition had a URI refused those masters, so a channel that
// played natively with sound was pushed onto ffmpeg for good.
func TestMasterWithAMixedAudioGroupIsServed(t *testing.T) {
	srv := newMasterServer(t, map[string]string{
		"master.m3u8": "#EXTM3U\n" +
			"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",DEFAULT=YES\n" +
			"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"Spanish\",URI=\"es.m3u8\"\n" +
			"#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"aud\"\nvideo.m3u8\n",
		"video.m3u8": "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nv1.ts\n#EXT-X-ENDLIST\n",
		"es.m3u8":    "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\na1.ts\n#EXT-X-ENDLIST\n",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/master.m3u8", Options{})
	if err != nil {
		t.Fatalf("a master whose audio group has a muxed default rendition was refused: %v\n"+
			"The variant's own segments carry that audio; refusing sends a working "+
			"native channel to ffmpeg permanently", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if len(got) == 0 || got[0] != 0x47 {
		t.Fatalf("mixed-audio-group master delivered %d bytes", len(got))
	}
}

// TestMasterWithSubtitleRenditionIsServed: subtitles living outside the variant
// is normal and costs the viewer nothing on a TS fan-out, so it must not be
// mistaken for the demuxed-audio case and refused.
func TestMasterWithSubtitleRenditionIsServed(t *testing.T) {
	srv := newMasterServer(t, map[string]string{
		"master.m3u8": "#EXTM3U\n" +
			"#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"English\",URI=\"subs.m3u8\"\n" +
			"#EXT-X-STREAM-INF:BANDWIDTH=1000000,SUBTITLES=\"subs\"\nvideo.m3u8\n",
		"video.m3u8": "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nv1.ts\n#EXT-X-ENDLIST\n",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/master.m3u8", Options{})
	if err != nil {
		t.Fatalf("a master with a subtitle rendition was refused: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if len(got) == 0 || got[0] != 0x47 {
		t.Fatalf("subtitle-rendition master delivered %d bytes", len(got))
	}
}
