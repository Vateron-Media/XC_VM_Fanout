// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsmerge

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsmux"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

const (
	pmtPID = 0x1000
	esPID  = 0x100 // both streams use it, as two muxers that know nothing of each other do
	aac    = 0x0f
)

// pmt is the fixture's PMT with one stream of the given type, its
// section_length cut to that one entry (the fixture declares room for more,
// which reads as a second, empty entry).
func pmt(streamType byte) []byte {
	p := tsfixture.PMTType(pmtPID, esPID, streamType)
	p[7] = 0x12
	return p
}

// videoStream is a video-only transport stream: tables, then a frame of two
// packets at each time.
func videoStream(times ...int64) []byte {
	out := tsfixture.Concat(tsfixture.PAT(pmtPID), pmt(0x1b))
	for _, t := range times {
		out = append(out, tsfixture.Keyframe(esPID, t)...)
		out = append(out, tsfixture.Fill(esPID)...)
	}
	return out
}

// audioStream is an audio-only transport stream on the same PIDs.
func audioStream(times ...int64) []byte {
	out := tsfixture.Concat(tsfixture.PAT(pmtPID), pmt(aac))
	for _, t := range times {
		out = append(out, tsfixture.PESStart(esPID, t, 0xff)...)
	}
	return out
}

// open is a stream that stays open after its bytes, as a live pull does.
func open(t *testing.T, b []byte) io.ReadCloser {
	t.Helper()
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(b)
	}()
	t.Cleanup(func() { pr.Close() })
	return pr
}

func quickWait(t *testing.T) {
	t.Helper()
	was := maxWait
	maxWait = 40 * time.Millisecond
	t.Cleanup(func() { maxWait = was })
}

// frames lists the PES starts of a transport stream as "<pid>@<time>".
func frames(ts []byte) []string {
	var out []string
	for ; len(ts) >= pktSize; ts = ts[pktSize:] {
		p := ts[:pktSize]
		if !tspes.PUSI(p) || tspes.PID(p) == 0 || tspes.PID(p) == pmtPID {
			continue
		}
		if t, ok := tspes.PTS(p); ok {
			out = append(out, fmt.Sprintf("%x@%d", tspes.PID(p), t))
		}
	}
	return out
}

// pmtOf returns the last PMT section of a transport stream.
func pmtOf(t *testing.T, ts []byte) []byte {
	t.Helper()
	var asm tspes.SectionAssembler
	var last []byte
	for ; len(ts) >= pktSize; ts = ts[pktSize:] {
		if tspes.PID(ts[:pktSize]) != pmtPID {
			continue
		}
		if sec := asm.Feed(ts[:pktSize]); sec != nil {
			last = append([]byte(nil), sec...)
		}
	}
	if last == nil {
		t.Fatal("no PMT in the output")
	}
	return last
}

func readAll(t *testing.T, m *Merger) ([]byte, error) {
	t.Helper()
	type result struct {
		b   []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		b, err := io.ReadAll(m)
		done <- result{b, err}
	}()
	select {
	case r := <-done:
		return r.b, r.err
	case <-time.After(10 * time.Second):
		t.Fatal("the merge never ended")
		return nil, nil
	}
}

// Every audio frame goes in front of the first video frame that is not earlier
// than it; the audio stream, which used the video's PID, is moved off it.
func TestAudioIsPutInFrontOfTheFrameItIsNotLaterThan(t *testing.T) {
	quickWait(t)
	video := io.NopCloser(bytes.NewReader(videoStream(0, 3600, 7200)))
	audio := open(t, audioStream(0, 1920, 3840, 5760, 7680, 9600))
	m, err := New(video, []io.ReadCloser{audio}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	got, err := readAll(t, m)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := []string{"101@0", "100@0", "101@1920", "100@3600", "101@3840", "101@5760", "100@7200"}
	if f := frames(got); !reflect.DeepEqual(f, want) {
		t.Fatalf("frames = %v, want %v", f, want)
	}
	if len(got)%pktSize != 0 {
		t.Fatalf("%d bytes is not a whole number of packets", len(got))
	}
}

// The video stream's PMT is replaced by one that declares both streams, on the
// same PID, with a CRC a strict demuxer accepts; the audio stream's own tables
// do not reach the output.
func TestThePMTDeclaresBothStreams(t *testing.T) {
	quickWait(t)
	m, err := New(io.NopCloser(bytes.NewReader(videoStream(0, 3600))), []io.ReadCloser{open(t, audioStream(0, 1920, 3840))}, []string{"por"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	got, _ := readAll(t, m)

	sec := pmtOf(t, got)
	streams, ok := tspes.PMTStreamsSection(sec)
	if !ok || !reflect.DeepEqual(streams, []tspes.ES{{PID: 0x100, Type: 0x1b}, {PID: 0x101, Type: aac}}) {
		t.Fatalf("PMT streams = %+v", streams)
	}
	if again := tsmux.Section(sec[0], sec[3:len(sec)-4]); !bytes.Equal(again, sec) {
		t.Fatalf("the PMT's CRC is not the one its bytes give")
	}
	if !bytes.Contains(sec, []byte{0x0a, 0x04, 'p', 'o', 'r', 0x00}) {
		t.Fatalf("the audio entry carries no language descriptor: % x", sec)
	}
	pats, pmts := 0, 0
	for b := got; len(b) >= pktSize; b = b[pktSize:] {
		switch tspes.PID(b[:pktSize]) {
		case 0:
			pats++
		case pmtPID:
			pmts++
		}
	}
	if pats != 1 || pmts != 1 {
		t.Fatalf("%d PAT and %d PMT packets, want the video stream's one of each", pats, pmts)
	}
}

// A rendition that says nothing does not hold the video for ever: after
// maxWait the frames go out without it, and it is not waited for again.
func TestVideoIsNotHeldForAudioThatDoesNotCome(t *testing.T) {
	quickWait(t)
	silent := open(t, tsfixture.Concat(tsfixture.PAT(pmtPID), pmt(aac)))
	started := time.Now()
	m, err := New(io.NopCloser(bytes.NewReader(videoStream(0, 3600, 7200, 10800))), []io.ReadCloser{silent}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	got, err := readAll(t, m)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if f := frames(got); !reflect.DeepEqual(f, []string{"100@0", "100@3600", "100@7200", "100@10800"}) {
		t.Fatalf("frames = %v", f)
	}
	if took := time.Since(started); took > 20*maxWait {
		t.Fatalf("four frames took %s: each one waited", took)
	}
}

// failing is a stream that fails once its bytes have been read, as a pull
// whose source went away does.
type failing struct{ r io.Reader }

func (f failing) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if err == io.EOF {
		err = errors.New("hls pull: 5 consecutive segment failures")
	}
	return n, err
}

func (failing) Close() error { return nil }

// An audio stream that fails takes the programme down, so the source is opened
// again instead of going on without its sound.
func TestAnAudioStreamThatFailsEndsTheProgramme(t *testing.T) {
	quickWait(t)
	video := open(t, videoStream(0, 3600, 7200, 10800))
	m, err := New(video, []io.ReadCloser{failing{bytes.NewReader(audioStream(0, 1920))}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	_, err = readAll(t, m)
	if !errors.Is(err, ErrAudioEnded) {
		t.Fatalf("err = %v, want ErrAudioEnded", err)
	}
}

// A finite programme, both playlists over, ends as its video does: cleanly,
// with what the two held.
func TestAProgrammeThatFinishesEndsCleanly(t *testing.T) {
	quickWait(t)
	m, err := New(io.NopCloser(bytes.NewReader(videoStream(0, 3600, 7200))), []io.ReadCloser{io.NopCloser(bytes.NewReader(audioStream(0, 1920, 3840, 5760)))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	got, err := readAll(t, m)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := []string{"101@0", "100@0", "101@1920", "100@3600", "101@3840", "101@5760", "100@7200"}
	if f := frames(got); !reflect.DeepEqual(f, want) {
		t.Fatalf("frames = %v, want %v", f, want)
	}
}

// Video that runs on past an audio stream that finished would be a channel
// gone silent with nothing to say so: it ends the programme.
func TestVideoThatRunsOnPastFinishedAudioEndsTheProgramme(t *testing.T) {
	quickWait(t)
	video := open(t, videoStream(0, 3600, soundLess/2, soundLess+7200, soundLess+10800))
	m, err := New(video, []io.ReadCloser{io.NopCloser(bytes.NewReader(audioStream(0, 1920, 3840)))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	got, err := readAll(t, m)
	if !errors.Is(err, ErrAudioEnded) {
		t.Fatalf("err = %v, want ErrAudioEnded", err)
	}
	if f := frames(got); len(f) == 0 || f[len(f)-1] != fmt.Sprintf("100@%d", soundLess/2) {
		t.Fatalf("frames = %v: the video within reach of the audio's end is kept", f)
	}
}

// Audio from before the video began is left out; what is on another timeline
// altogether is not held back for it.
func TestAudioFromBeforeTheVideoIsLeftOut(t *testing.T) {
	quickWait(t)
	const start = 900000
	m, err := New(io.NopCloser(bytes.NewReader(videoStream(start, start+3600))), []io.ReadCloser{open(t, audioStream(start-180000, start-90000, start-1920, start, start+1920, start+3840))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	got, _ := readAll(t, m)
	want := []string{
		fmt.Sprintf("101@%d", start-1920), fmt.Sprintf("101@%d", start), fmt.Sprintf("100@%d", start),
		fmt.Sprintf("101@%d", start+1920), fmt.Sprintf("100@%d", start+3600),
	}
	if f := frames(got); !reflect.DeepEqual(f, want) {
		t.Fatalf("frames = %v, want %v", f, want)
	}
}

func TestDeltaTakesTheShortWayRoundTheClock(t *testing.T) {
	const wrap = int64(1) << 33
	for _, c := range []struct{ a, b, want int64 }{
		{100, 40, 60},
		{40, 100, -60},
		{10, wrap - 10, 20}, // a is just past the wrap, b just before it
		{wrap - 10, 10, -20},
	} {
		if got := delta(c.a, c.b); got != c.want {
			t.Errorf("delta(%d, %d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestMoreAudioStreamsThanOneProgrammeTakes(t *testing.T) {
	if _, err := New(io.NopCloser(bytes.NewReader(nil)), nil, nil); err == nil {
		t.Fatal("no audio stream was accepted")
	}
	many := make([]io.ReadCloser, MaxAudio+1)
	for i := range many {
		many[i] = io.NopCloser(bytes.NewReader(nil))
	}
	if _, err := New(io.NopCloser(bytes.NewReader(nil)), many, nil); err == nil {
		t.Fatal("too many audio streams were accepted")
	}
}
