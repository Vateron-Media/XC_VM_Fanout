package gateway

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// rawurlencode is PHP's: every byte but A-Z a-z 0-9 - _ . ~ as %XX (upper case).
func rawurlencode(s string) string {
	const hexd = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexd[c>>4])
		b.WriteByte(hexd[c&15])
	}
	return b.String()
}

// mintOwn is ViewerKey::mintOwn: sealed with the node's own viewer key once
// it has one, else under the stream secret, sealed or (secure_stream_tokens
// off) in the legacy format.
func mintOwn(p *Policy, data string, nonces io.Reader) (string, error) {
	ctx := pick(p.Keys.Context, 0, 0)
	if len(p.Keys.Viewer) > 0 && len(p.Keys.Viewer[0].Value) > 0 {
		return sealRandom([]byte(data), p.Keys.Viewer[0].Value, ctx, nonces)
	}
	if len(p.Keys.Shared) == 0 || len(p.Keys.Shared[0].Value) == 0 {
		return "", errors.New("no key to mint with")
	}
	if p.RawKeys.AcceptLegacyCBC {
		return EncryptLegacy([]byte(data), p.Keys.Shared[0].Value, ctx), nil
	}
	return sealRandom([]byte(data), p.Keys.Shared[0].Value, ctx, nonces)
}

// sealRandom seals with a fresh nonce from nonces (crypto/rand, read through
// one buffer per playlist: one system call for all its tokens).
func sealRandom(plain, key, ctx []byte, nonces io.Reader) (string, error) {
	nonce := make([]byte, sealNonce)
	if _, err := io.ReadFull(nonces, nonce); err != nil {
		return "", err
	}
	return Seal(plain, key, ctx, nonce), nil
}

var (
	mediaSeqRe   = regexp.MustCompile(`#EXT-X-MEDIA-SEQUENCE:(\d+)`)
	targetDurRe  = regexp.MustCompile(`#EXT-X-TARGETDURATION:(\d+)`)
	extm3uLineRe = regexp.MustCompile(`#EXTM3U\r?\n`)
)

// Tokenize is HLSGenerator::tokenizeDaemonPlaylist for a refresh the gateway
// answers (no proxy route): each "<n>.ts" line a segment token for this
// viewer, the media sequence re-anchored (HlsSequence) and, for encrypted
// HLS, the key line. ivFile is STREAMS_PATH/<id>_.iv.
func Tokenize(p *Policy, r *Refresh, clientIP string, now time.Time) (string, error) {
	nonces := bufio.NewReaderSize(rand.Reader, 64*sealNonce)
	var mintErr error
	out := segLineRe.ReplaceAllStringFunc(r.Playlist, func(line string) string {
		seq := strings.TrimSuffix(line, ".ts")
		seg := strconv.Itoa(r.Stream) + "_d" + seq + ".ts"
		var payload string
		if r.HMAC != "" {
			payload = "HMAC#" + r.HMAC + "/" + rawurlencode(r.Identifier)
		} else {
			payload = rawurlencode(r.Username) + "/" + rawurlencode(r.Password)
		}
		payload += "/" + clientIP + "/" + strconv.Itoa(r.Stream) + "/" + seg + "/" + r.UUID + "/" + strconv.Itoa(p.ServerID) + "/" + r.Codec + "/" + strconv.Itoa(r.OnDemand)
		tok, err := mintOwn(p, payload, nonces)
		if err != nil {
			mintErr = err
		}
		return "/hls/" + tok
	})
	if mintErr != nil {
		return "", mintErr
	}
	if m := mediaSeqRe.FindStringSubmatchIndex(out); m != nil {
		daemon, _ := strconv.ParseInt(out[m[2]:m[3]], 10, 64)
		target := int64(hlsSEG)
		if t := targetDurRe.FindStringSubmatch(out); t != nil {
			if v, err := strconv.ParseInt(t[1], 10, 64); err == nil {
				target = max(1, v)
			}
		}
		seq := liveSequence(p.Paths.Signals, r.Stream, daemon, target, now)
		out = out[:m[0]] + "#EXT-X-MEDIA-SEQUENCE:" + strconv.FormatInt(seq, 10) + out[m[1]:]
	}
	if p.EncryptHLS {
		if iv, err := os.ReadFile(p.Paths.Streams + strconv.Itoa(r.Stream) + "_.iv"); err == nil {
			tok, err := mintOwn(p, clientIP+"/"+strconv.Itoa(r.Stream), nonces)
			if err != nil {
				return "", err
			}
			line := `#EXT-X-KEY:METHOD=AES-128,URI="/key/` + tok + `",IV=0x` + hex.EncodeToString(iv) + "\n"
			if loc := extm3uLineRe.FindStringIndex(out); loc != nil {
				out = out[:loc[1]] + line + out[loc[1]:]
			}
		}
	}
	return out, nil
}

// ── HlsSequence ─────────────────────────────────────────────────────────

const (
	hlsSEG          = 10
	hlsStaleTargets = 3
	hlsStaleMinSec  = 30
)

// seqState is HlsSequence's per-stream state (SIGNALS_TMP_PATH/hlsseq_<id>).
type seqState struct {
	Base   *int64 `json:"base,omitempty"`
	Last   *int64 `json:"last,omitempty"`
	Daemon *int64 `json:"daemon,omitempty"`
	At     *int64 `json:"at,omitempty"`
}

func i64(v int64) *int64 { return &v }

// reconcile is HlsSequence::reconcile: the published sequence and the next
// state. now nil always applies the off-air floor.
func reconcile(daemon, floor int64, st *seqState, now *int64, target int64) (int64, seqState) {
	var base, last int64
	if st != nil && st.Base != nil {
		base = *st.Base
	}
	if st != nil && st.Last != nil {
		last = *st.Last
	}
	continuous := now != nil && st != nil && st.Daemon != nil && st.At != nil &&
		daemon >= *st.Daemon && *now-*st.At <= max(hlsStaleMinSec, hlsStaleTargets*max(1, target))
	seq := daemon + base
	minimum := last
	if !continuous {
		minimum = max(last, floor)
	}
	if seq < minimum {
		base = minimum - daemon
		seq = minimum
	}
	next := seqState{Base: i64(base), Last: i64(seq)}
	if now != nil {
		next.Daemon, next.At = i64(daemon), i64(*now)
	}
	return seq, next
}

// liveSequence is HlsSequence::liveSequence: the state file read, re-anchored
// and written back under the same flock PHP takes; any I/O failure degrades to
// max(daemon, floor), as PHP's does.
func liveSequence(dir string, stream int, daemon, target int64, now time.Time) int64 {
	unix := now.Unix()
	floor := unix / hlsSEG
	if dir == "" {
		return max(daemon, floor)
	}
	f, err := os.OpenFile(dir+"hlsseq_"+strconv.Itoa(stream), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return max(daemon, floor)
	}
	defer f.Close()
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN) //nolint:errcheck
	raw, _ := io.ReadAll(f)
	var st *seqState
	if len(raw) > 0 {
		var s seqState
		if json.Unmarshal(raw, &s) == nil {
			st = &s
		}
	}
	seq, next := reconcile(daemon, floor, st, &unix, target)
	b, _ := json.Marshal(next)
	if err := f.Truncate(0); err == nil {
		_, _ = f.WriteAt(b, 0)
	}
	return seq
}
