// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/hlscrypt"
)

// aesUpstream serves a live AES-128 playlist: three encrypted segments, the key
// at /key.bin, and whatever key line the caller wants in front of them.
func aesUpstream(t *testing.T, keyLine func(base string) string, key []byte, ivOf func(i int) []byte) (*httptest.Server, map[string][]byte, *atomic.Int32) {
	t.Helper()
	plain := map[string][]byte{}
	enc := map[string][]byte{}
	for i, n := range []string{"s0.ts", "s1.ts", "s2.ts"} {
		b := tsSegment(int64(i) * 90000)
		plain[n] = b
		enc[n] = hlscrypt.EncryptCBC(b, key, ivOf(i))
	}
	var keyHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch name := strings.TrimPrefix(r.URL.Path, "/"); {
		case name == "key.bin":
			keyHits.Add(1)
			_, _ = w.Write(key)
		case enc[name] != nil:
			_, _ = w.Write(enc[name])
		default:
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n" +
				keyLine(r.Host) +
				"#EXTINF:2.0,\ns0.ts\n#EXTINF:2.0,\ns1.ts\n#EXTINF:2.0,\ns2.ts\n"))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, plain, &keyHits
}

// An AES-128 upstream is an ordinary IPTV source, and every one of them used to
// cost a permanent ffmpeg child: nativesrc refused the playlist outright and the
// puller fell back. The segments are now decrypted with the key the playlist
// names, and the ring gets the same plaintext MPEG-TS a clear source would give.
func TestAES128SegmentsAreDecrypted(t *testing.T) {
	key := []byte("0123456789abcdef")
	// No IV= on the key line: the sequence number is the IV, which is what an
	// encoder that omits it means (RFC 8216 §5.2).
	ivOf := func(i int) []byte {
		iv := make([]byte, 16)
		iv[15] = byte(i)
		return iv
	}
	srv, plain, keyHits := aesUpstream(t,
		func(string) string { return "#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n" },
		key, ivOf)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/live.m3u8", Options{})
	if err != nil {
		t.Fatalf("an AES-128 playlist was refused: %v", err)
	}
	defer rc.Close()

	want := append(append(append([]byte{}, plain["s0.ts"]...), plain["s1.ts"]...), plain["s2.ts"]...)
	got := make([]byte, len(want))
	if _, err := io.ReadFull(rc, got); err != nil {
		t.Fatalf("reading the decrypted stream: %v", err)
	}
	if string(got) != string(want) {
		t.Error("the bytes on the wire are not the upstream's plaintext segments")
	}
	if got[0] != 0x47 {
		t.Errorf("first byte %#x, want the TS sync byte: ciphertext reached the ring", got[0])
	}
	// The key is fetched once and reused, not re-fetched per segment.
	if n := keyHits.Load(); n != 1 {
		t.Errorf("the key was fetched %d times for 3 segments, want 1", n)
	}
}

// An explicit IV= on the key line wins over the sequence number.
func TestAES128HonoursAnExplicitIV(t *testing.T) {
	key := []byte("0123456789abcdef")
	iv, _ := hex.DecodeString("00112233445566778899aabbccddeeff")
	srv, plain, _ := aesUpstream(t,
		func(string) string {
			return "#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x00112233445566778899AABBCCDDEEFF\n"
		},
		key, func(int) []byte { return iv })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/live.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got := make([]byte, len(plain["s0.ts"]))
	if _, err := io.ReadFull(rc, got); err != nil {
		t.Fatalf("reading: %v", err)
	}
	if string(got) != string(plain["s0.ts"]) {
		t.Error("the explicit IV was not used: the first segment did not decrypt to its plaintext")
	}
}

// What this package still cannot read must still be refused, and refused as a
// FORMAT problem so the caller moves the stream to ffmpeg rather than retrying
// it for ever.
func TestUnreadableEncryptionIsStillRefused(t *testing.T) {
	key := []byte("0123456789abcdef")
	ivOf := func(int) []byte { return make([]byte, 16) }
	for _, tc := range []struct {
		name    string
		keyLine string
	}{
		// SAMPLE-AES encrypts inside the elementary streams: it needs a demuxer.
		{"SAMPLE-AES", "#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"key.bin\"\n"},
		// A key with nowhere to fetch it from is unusable.
		{"AES-128 with no URI", "#EXT-X-KEY:METHOD=AES-128\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, _ := aesUpstream(t, func(string) string { return tc.keyLine }, key, ivOf)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rc, err := Open(ctx, srv.URL+"/live.m3u8", Options{})
			if err == nil {
				rc.Close()
				t.Fatal("accepted a playlist this package cannot decrypt")
			}
			if !IsFormat(err) {
				t.Errorf("err = %v, want a format refusal so the caller falls back to ffmpeg", err)
			}
		})
	}
}

// METHOD=NONE switches encryption back off mid-playlist, and a segment after it
// is in the clear. Treating it as still-encrypted would decrypt plaintext into
// garbage.
func TestMethodNoneEndsEncryption(t *testing.T) {
	base, err := url.Parse("http://h/live.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	pl, err := parseHLSPlaylist([]byte(
		"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n"+
			"#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n#EXTINF:2.0,\ns0.ts\n"+
			"#EXT-X-KEY:METHOD=NONE\n#EXTINF:2.0,\ns1.ts\n"), base)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(pl.Segments) != 2 {
		t.Fatalf("parsed %d segments, want 2", len(pl.Segments))
	}
	if pl.Segments[0].Key == nil || !pl.Segments[0].Key.aes128() {
		t.Error("the first segment lost its AES-128 key")
	}
	if pl.Segments[1].Key != nil {
		t.Error("a segment after METHOD=NONE is in the clear, but carries a key")
	}
	if err := servable(pl); err != nil {
		t.Errorf("servable = %v, want the playlist accepted", err)
	}
}

// A key URI that answers with something other than 16 bytes — an origin's error
// page is the usual way — is a failed segment, not a format refusal: the next
// poll may well work, and a format refusal would pin the channel to ffmpeg.
func TestABadKeyResponseIsNotAFormatRefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch name := strings.TrimPrefix(r.URL.Path, "/"); {
		case name == "key.bin":
			_, _ = w.Write([]byte("<html>over connection limit</html>"))
		case strings.HasSuffix(name, ".ts"):
			_, _ = w.Write(hlscrypt.EncryptCBC(tsSegment(0), []byte("0123456789abcdef"), make([]byte, 16)))
		default:
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n" +
				"#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n#EXTINF:2.0,\ns0.ts\n"))
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/live.m3u8", Options{})
	if err != nil {
		// Refused at open: it must not be a format refusal.
		if IsFormat(err) {
			t.Fatalf("a key fetch that failed was reported as a format problem: %v", err)
		}
		return
	}
	defer rc.Close()
	// Otherwise the pull fails once the key cannot be used, and the error that
	// closes the pipe must likewise not be a format refusal.
	if _, err := io.ReadAll(rc); err != nil && IsFormat(err) && !errors.Is(err, hlscrypt.ErrCiphertext) {
		t.Errorf("pull ended with a format refusal: %v", err)
	}
}

// The other half of TestPlaylistTurningEncryptedEndsThePull: an upstream that
// starts in the clear and switches to AES-128 mid-window — a packager rotating
// into encryption, which happens on live channels — must keep streaming, with
// the segments after the switch decrypted rather than ending the pull.
func TestAnAES128SwitchMidPullKeepsStreaming(t *testing.T) {
	key := []byte("0123456789abcdef")
	clear0 := tsSegment(0)
	plain1 := tsSegment(90000)
	enc1 := hlscrypt.EncryptCBC(plain1, key, func() []byte { iv := make([]byte, 16); iv[15] = 1; return iv }())

	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch name := strings.TrimPrefix(r.URL.Path, "/"); {
		case name == "key.bin":
			_, _ = w.Write(key)
		case name == "s0.ts":
			_, _ = w.Write(clear0)
		case name == "s1.ts":
			_, _ = w.Write(enc1)
		default:
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			if polls.Add(1) == 1 {
				_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:1.0,\ns0.ts\n"))
				return
			}
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n" +
				"#EXTINF:1.0,\ns0.ts\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n#EXTINF:1.0,\ns1.ts\n"))
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/live.m3u8", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rc.Close()

	want := append(append([]byte{}, clear0...), plain1...)
	got := make([]byte, len(want))
	if _, err := io.ReadFull(rc, got); err != nil {
		t.Fatalf("the pull stopped at the switch to AES-128: %v", err)
	}
	if string(got) != string(want) {
		t.Error("the segment after the switch did not arrive as its plaintext")
	}
}
