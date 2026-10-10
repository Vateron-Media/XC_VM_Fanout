// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// masterServer serves a master playlist, one media playlist per variant, and
// TS segments, recording every path it was asked for.
type masterServer struct {
	*httptest.Server
	mu   sync.Mutex
	got  []string
	body map[string]string
	// segs holds the segments that are not the stock one, by name.
	segs map[string][]byte
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
			if seg, ok := m.segs[name]; ok {
				_, _ = w.Write(seg)
				return
			}
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

// splitMaster is a master whose only variant has its audio in a separate
// rendition, which carries its own URI and so lives OUTSIDE the variant (RFC
// 8216): the variant's segments hold video only.
const splitMaster = "#EXTM3U\n" +
	"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"Portugues\",LANGUAGE=\"por\",DEFAULT=YES,URI=\"audio.m3u8\"\n" +
	"#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"aud\"\nvideo.m3u8\n"

// TestMasterWithSeparateAudioRenditionIsJoined: the variant and its audio
// rendition are pulled side by side and come out as one programme, its table
// declaring both and the audio on a PID of its own. Taking the variant alone
// would fan the channel out silent, with nothing to say anything was wrong;
// that used to be refused, and ffmpeg ran the channel.
func TestMasterWithSeparateAudioRenditionIsJoined(t *testing.T) {
	// Two muxers that know nothing of each other: the same PIDs on both sides.
	video := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101))
	audio := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMTType(0x100, 0x101, 0x0f))
	for i := int64(0); i < 5; i++ {
		video = append(video, tsfixture.KeyframePCR(0x101, i*3600, i*3600)...)
		video = append(video, tsfixture.Fill(0x101)...)
		audio = append(audio, tsfixture.PESStart(0x101, i*1920, 0xff)...)
	}
	srv := newMasterServer(t, map[string]string{
		"master.m3u8": splitMaster,
		"video.m3u8":  "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nv1.ts\n#EXT-X-ENDLIST\n",
		"audio.m3u8":  "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\na1.ts\n#EXT-X-ENDLIST\n",
	})
	srv.segs = map[string][]byte{"v1.ts": video, "a1.ts": audio}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/master.m3u8", Options{})
	if err != nil {
		t.Fatalf("a master whose audio is a separate MPEG-TS rendition was refused: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var asm tspes.SectionAssembler
	var streams []tspes.ES
	frames := map[uint16]int{}
	for b := got; len(b) >= tsPacketSize; b = b[tsPacketSize:] {
		p := b[:tsPacketSize]
		switch pid := tspes.PID(p); {
		case pid == 0x100:
			if sec := asm.Feed(p); sec != nil {
				streams, _ = tspes.PMTStreamsSection(sec)
				if !bytes.Contains(sec, []byte{0x0a, 0x04, 'p', 'o', 'r', 0x00}) {
					t.Errorf("the audio entry does not name the rendition's language: % x", sec)
				}
			}
		case pid != 0 && tspes.PUSI(p):
			frames[pid]++
		}
	}
	var videoPID, audioPID uint16
	for _, es := range streams {
		switch es.Type {
		case 0x1b:
			videoPID = es.PID
		case 0x0f:
			audioPID = es.PID
		}
	}
	if videoPID != 0x101 || audioPID == 0 || audioPID == videoPID {
		t.Fatalf("PMT streams = %+v: want the video on 0x101 and the audio on a PID of its own", streams)
	}
	if frames[videoPID] != 5 || frames[audioPID] != 5 {
		t.Fatalf("frames by PID = %v: want 5 of video on %#x and 5 of audio on %#x", frames, videoPID, audioPID)
	}
}

// TestSeparateAudioThatCannotBeJoinedIsRefused: only MPEG-TS renditions are
// joined. Anything else keeps the refusal that sends the channel to ffmpeg,
// which is a format refusal so the caller does not retry it natively.
func TestSeparateAudioThatCannotBeJoinedIsRefused(t *testing.T) {
	const video = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nv1.ts\n"
	for name, audio := range map[string]string{
		"fMP4 audio":   "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2.0,\na1.m4s\n",
		"packed audio": "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\na1.aac\n",
	} {
		t.Run(name, func(t *testing.T) {
			srv := newMasterServer(t, map[string]string{"master.m3u8": splitMaster, "video.m3u8": video, "audio.m3u8": audio})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			rc, err := Open(ctx, srv.URL+"/master.m3u8", Options{})
			if err == nil {
				_ = rc.Close()
				t.Fatalf("accepted; fetched %v", srv.paths())
			}
			if !IsFormat(err) {
				t.Fatalf("err = %v, want a format refusal so the caller falls back to ffmpeg", err)
			}
		})
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
