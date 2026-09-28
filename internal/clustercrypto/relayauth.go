package clustercrypto

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"strings"
)

// X-XCVM-Relay-Auth (and X-XCVM-File-Auth, the same proof over a file
// ticket): the child node proving, per connect, that it holds the key its
// ticket names (the panel's Core\Cluster\Crypto\RelayAuth).
//
//	msg    = "xcvm-relay-auth-v1" ‖ lp(ticket wire) ‖ lp(METHOD) ‖ lp(target) ‖ u64(ts_ms) ‖ nonce[16]
//	sig    = node signature, purpose "relay", over msg
//	header = ts_ms "." hex(nonce) "." b64url(sig)

// RelayWindowMs is the ±window a relay auth's ts_ms is accepted in (the
// canonical request window).
const RelayWindowMs = 90000

var relayAuthRe = regexp.MustCompile(`^([1-9][0-9]{12,15})\.([0-9a-f]{32})\.([A-Za-z0-9_-]{86})$`)

// RelayAuthMessage is what the child signs.
func RelayAuthMessage(ticketWire, method, target string, tsMs int64, nonce []byte) ([]byte, error) {
	if len(nonce) != 16 {
		return nil, errors.New("clustercrypto: relay auth nonce must be 16 bytes")
	}
	if tsMs < 0 {
		return nil, errors.New("clustercrypto: relay auth ts_ms")
	}
	out := []byte("xcvm-relay-auth-v1")
	out = append(out, lps(ticketWire)...)
	out = append(out, lps(strings.ToUpper(method))...)
	out = append(out, lps(target)...)
	out = append(out, u64(uint64(tsMs))...)
	return append(out, nonce...), nil
}

// RelayAuthHeader signs the header with the node's key.
func RelayAuthHeader(sk ed25519.PrivateKey, ticketWire, method, target string, tsMs int64, nonce []byte) (string, error) {
	msg, err := RelayAuthMessage(ticketWire, method, target, tsMs, nonce)
	if err != nil {
		return "", err
	}
	sig := SignNode(sk, "relay", msg)
	return strconv.FormatInt(tsMs, 10) + "." + hex.EncodeToString(nonce) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// VerifyRelayAuth checks a header against the child key the ticket names
// and the ±RelayWindowMs window at nowMs; it returns the header's ts_ms and
// nonce for the replay check.
func VerifyRelayAuth(childPub []byte, header, ticketWire, method, target string, nowMs int64) (int64, []byte, bool) {
	m := relayAuthRe.FindStringSubmatch(header)
	if m == nil {
		return 0, nil, false
	}
	ts, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, nil, false
	}
	nonce, _ := hex.DecodeString(m[2])
	sig, err := base64.RawURLEncoding.DecodeString(m[3])
	if err != nil || ts-nowMs > RelayWindowMs || nowMs-ts > RelayWindowMs {
		return 0, nil, false
	}
	msg, err := RelayAuthMessage(ticketWire, method, target, ts, nonce)
	if err != nil || !VerifyNode(childPub, "relay", msg, sig) {
		return 0, nil, false
	}
	return ts, nonce, true
}
