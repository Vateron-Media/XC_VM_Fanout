// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// renderBanner draws one frame of a flat background with filter applied and
// returns the PNG. Identical pixels mean identical text, size, position and
// colour — everything the banner is.
func renderBanner(t *testing.T, ffmpeg, filter string) ([]byte, string) {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.Command(ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "debug",
		"-f", "lavfi", "-i", "color=c=gray:s=320x180:d=1",
		"-vf", filter, "-frames:v", "1", "-f", "image2", "-c:v", "png", "pipe:1")
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		// Debug level makes the log enormous; only its tail says what went wrong.
		lines := strings.Split(strings.TrimSpace(errb.String()), "\n")
		if len(lines) > 8 {
			lines = lines[len(lines)-8:]
		}
		t.Fatalf("ffmpeg could not render the banner at all (%v) — the overlay would silently do nothing\n"+
			"filter: %s\n%s", err, filter, strings.Join(lines, "\n"))
	}
	return out.Bytes(), errb.String()
}

// drawtextSetting reads back what ffmpeg's option parser actually made of a
// drawtext option, from a -loglevel debug run. Purely for the failure message:
// "the viewer saw the option list as the banner" is the kind of thing a diff of
// two PNG hashes cannot say.
func drawtextSetting(log, key string) string {
	re := regexp.MustCompile(`Setting '` + regexp.QuoteMeta(key) + `' to value '(.*)'`)
	if m := re.FindStringSubmatch(log); m != nil {
		return m[1]
	}
	return "<not set>"
}

// TestDrawtextFilterDrawsTheMessageVerbatim is the escaping's real contract: the
// viewer must see the message, exactly, and nothing else.
//
// The oracle is drawtext's own textfile= option, which takes the text from a file
// and so needs no escaping whatsoever. Whatever the escaper is supposed to
// produce, that is what it has to look like on screen.
//
// A -vf value is unescaped TWICE — once by the filtergraph parser and once by the
// option parser — and drawtext then runs its own %{} expansion over the result.
// The old escaper handled one level: it wrapped the value in single quotes and
// used the shell's '\” break-out, which only survives the first pass. So the
// option parser saw a bare quote and swallowed the rest of the filter into the
// text — a viewer fingerprinted by username saw "obrien:fontsize=20:x=10:..." at
// the default size, position and colour, which is no fingerprint at all.
func TestDrawtextFilterDrawsTheMessageVerbatim(t *testing.T) {
	ffmpeg := systemFFmpeg(t)
	font := systemFont(t)

	// Every one of these is a message the panel really can send: the fingerprint
	// overlay draws a viewer's username or uuid, and the free-text message is
	// whatever an admin types.
	cases := []struct{ name, msg string }{
		{"a plain message", "channel back at 9"},
		{"an apostrophe", "it's back"},
		{"two apostrophes", "it's Bob's"},
		{"a username with an apostrophe", "o'brien"},
		{"a percent sign", "50% off"},
		{"a backslash", `back\slash`},
		{"a colon", "on at 3:00"},
		{"filtergraph separators", "a,b;c[d]=e"},
		{"something that looks like an expansion", "%{e:1+1}"},
		{"a double quote", `say "hi"`},
		{"non-ASCII", "ümlaut ✓"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sig := pendingSignal{text: tc.msg, fontSize: 20, color: "white", x: 10, y: 10}

			msgFile := filepath.Join(t.TempDir(), "msg.txt")
			if err := os.WriteFile(msgFile, []byte(tc.msg), 0o644); err != nil {
				t.Fatal(err)
			}
			want, _ := renderBanner(t, ffmpeg, "drawtext=fontfile="+font+":textfile="+msgFile+
				":fontsize=20:x=10:y=10:fontcolor=white:expansion=none")

			got, log := renderBanner(t, ffmpeg, drawtextFilter(font, sig))
			if bytes.Equal(got, want) {
				return
			}
			var why []string
			if v := drawtextSetting(log, "text"); v != tc.msg {
				why = append(why, "drawtext was handed the text "+strconv.Quote(v))
			}
			if v := drawtextSetting(log, "fontsize"); v != "20" {
				why = append(why, "fontsize came out as "+v+" — the filter's own options were swallowed into the message")
			}
			for _, line := range strings.Split(log, "\n") {
				if strings.Contains(line, "Stray %") {
					why = append(why, strings.TrimSpace(line)+" — drawtext gave up and drew nothing")
					break
				}
			}
			if len(why) == 0 {
				why = append(why, "the rendered banner differs from the message")
			}
			t.Fatalf("message %q is not what the viewer sees: %s\nfilter: %s",
				tc.msg, strings.Join(why, "; "), drawtextFilter(font, sig))
		})
	}
}

// TestDrawtextFilterEscapesTheFontPath: the font path is operator config, not a
// message, but it lands in the same filtergraph and was pasted in raw. A path
// holding a ',' or a ':' — a dated font directory, a path with a drive-style
// prefix — silently broke every overlay on the node.
func TestDrawtextFilterEscapesTheFontPath(t *testing.T) {
	ffmpeg := systemFFmpeg(t)
	font := systemFont(t)

	src, err := os.ReadFile(font)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "fonts,v2:1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	awkward := filepath.Join(dir, "it's a font.ttf")
	if err := os.WriteFile(awkward, src, 0o644); err != nil {
		t.Fatal(err)
	}

	sig := pendingSignal{text: "hello", fontSize: 20, color: "white", x: 10, y: 10}
	got, _ := renderBanner(t, ffmpeg, drawtextFilter(awkward, sig))
	want, _ := renderBanner(t, ffmpeg, drawtextFilter(font, sig))
	if !bytes.Equal(got, want) {
		t.Fatalf("the same font at an awkward path %q drew a different banner — the path broke the filter", awkward)
	}
}
