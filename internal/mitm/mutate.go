package mitm

import (
	"bytes"
	"net/http"
	"strconv"
)

// The edits an attack is made of. Each returns a new value and leaves its
// input alone, so one captured exchange can be tampered with several ways.

// Flip is b with one bit of the byte at i changed; a negative i counts from
// the end. An empty b stays empty.
func Flip(b []byte, i int) []byte {
	out := bytes.Clone(b)
	if len(out) == 0 {
		return out
	}
	if i < 0 {
		i += len(out)
	}
	out[((i%len(out))+len(out))%len(out)] ^= 0x01
	return out
}

// FlipHeader changes one character of header name, keeping it the same length
// and still hex or decimal when it was: a value that parses but is wrong.
func FlipHeader(h http.Header, name string) http.Header {
	out := h.Clone()
	v := out.Get(name)
	if v == "" {
		return out
	}
	last := v[len(v)-1]
	switch {
	case last == '0':
		last = '1'
	case last >= '1' && last <= '9', last >= 'b' && last <= 'f', last >= 'B' && last <= 'F':
		last--
	case last == 'a' || last == 'A':
		last = '9'
	default:
		last ^= 0x01
	}
	out.Set(name, v[:len(v)-1]+string(last))
	return out
}

// Shift is h with the decimal header name moved by delta; a value that is not
// a number is left as it is.
func Shift(h http.Header, name string, delta int64) http.Header {
	out := h.Clone()
	if n, err := strconv.ParseInt(out.Get(name), 10, 64); err == nil {
		out.Set(name, strconv.FormatInt(n+delta, 10))
	}
	return out
}

// Without is h without the headers named.
func Without(h http.Header, names ...string) http.Header {
	out := h.Clone()
	for _, n := range names {
		out.Del(n)
	}
	return out
}

// With is h with header name set to value.
func With(h http.Header, name, value string) http.Header {
	out := h.Clone()
	out.Set(name, value)
	return out
}

// Answer is a Request attack that answers with from's reply instead of
// forwarding: an old reply played to a new request.
func Answer(from Exchange) Attack {
	return func(e *Exchange) {
		e.Status, e.ResHeader, e.ResBody = from.Status, from.ResHeader.Clone(), bytes.Clone(from.ResBody)
	}
}

// Refuse is a Request attack that answers status with an empty body: the
// request never reaches the server.
func Refuse(status int) Attack {
	return func(e *Exchange) {
		e.Status, e.ResHeader, e.ResBody = status, http.Header{}, nil
	}
}
