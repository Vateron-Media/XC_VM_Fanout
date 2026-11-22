// Package mitm is an on-path attacker for tests: a reverse proxy that sits
// between an agent and the server it talks to (MAIN's cluster API, a parent's
// relay or /xfile), records every exchange, and can rewrite a request or a
// reply, answer in the server's place, or send a recorded request again.
//
// It holds no key. Everything it does is what someone on the wire can do: the
// tests built on it (XC_VM ADR 0004, "The MITM harness") check that each of
// those is refused, by the server or by the agent, and that a refusal changes
// nothing.
package mitm

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
)

// MaxBody caps a body the proxy reads, either way.
const MaxBody = 16 << 20

// Exchange is one request and its reply as they crossed the proxy.
type Exchange struct {
	Method    string
	Path      string
	RawQuery  string
	ReqHeader http.Header
	ReqBody   []byte

	// Status is the reply's status; 0 until there is a reply. A Request
	// attack that sets it answers the client itself: nothing is forwarded.
	Status    int
	ResHeader http.Header
	ResBody   []byte

	// Upstream is the status the server gave, before a Response attack; 0
	// when the request was never forwarded.
	Upstream int
	// UpstreamBody is the body the server gave, before a Response attack.
	UpstreamBody []byte
}

// Clone is a deep copy of e.
func (e Exchange) Clone() Exchange {
	e.ReqHeader = e.ReqHeader.Clone()
	e.ReqBody = bytes.Clone(e.ReqBody)
	e.ResHeader = e.ResHeader.Clone()
	e.ResBody = bytes.Clone(e.ResBody)
	e.UpstreamBody = bytes.Clone(e.UpstreamBody)
	return e
}

// URI is the request's path and query.
func (e *Exchange) URI() string {
	if e.RawQuery == "" {
		return e.Path
	}
	return e.Path + "?" + e.RawQuery
}

// Attack rewrites an exchange in flight. A Request attack sees the request
// only; a Response attack sees the request as forwarded and the reply.
type Attack func(e *Exchange)

// Proxy forwards to Upstream (scheme://host[:port], no path), applying the
// attacks set at the time of each request.
type Proxy struct {
	Upstream string
	Client   *http.Client

	mu       sync.Mutex
	request  Attack
	response Attack
	log      []Exchange
}

// New is a proxy to upstream.
func New(upstream string) *Proxy {
	return &Proxy{Upstream: strings.TrimRight(upstream, "/"), Client: &http.Client{}}
}

// Set installs the attacks for the requests that follow; nil passes that
// half through untouched.
func (p *Proxy) Set(request, response Attack) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.request, p.response = request, response
}

// Clear stops attacking: every exchange passes through as it is.
func (p *Proxy) Clear() { p.Set(nil, nil) }

// Log is every exchange so far, oldest first, as the client saw it.
func (p *Proxy) Log() []Exchange {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Exchange, len(p.log))
	for i, e := range p.log {
		out[i] = e.Clone()
	}
	return out
}

// Last is the newest exchange whose path ends in suffix, and whether there is one.
func (p *Proxy) Last(suffix string) (Exchange, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.log) - 1; i >= 0; i-- {
		if strings.HasSuffix(p.log[i].Path, suffix) {
			return p.log[i].Clone(), true
		}
	}
	return Exchange{}, false
}

// Reset forgets the log.
func (p *Proxy) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log = nil
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBody))
	if err != nil {
		http.Error(w, "mitm: "+err.Error(), http.StatusBadGateway)
		return
	}
	e := Exchange{Method: r.Method, Path: r.URL.Path, RawQuery: r.URL.RawQuery, ReqHeader: r.Header.Clone(), ReqBody: body}
	p.mu.Lock()
	reqAttack, resAttack := p.request, p.response
	p.mu.Unlock()
	if reqAttack != nil {
		reqAttack(&e)
	}
	if e.Status == 0 {
		if err := p.forward(r.Context(), &e); err != nil {
			http.Error(w, "mitm: "+err.Error(), http.StatusBadGateway)
			return
		}
		if resAttack != nil {
			resAttack(&e)
		}
	}
	p.mu.Lock()
	p.log = append(p.log, e.Clone())
	p.mu.Unlock()
	for k, vs := range e.ResHeader {
		// The body's length is the one written below, whatever an attack did to it.
		if k != "Content-Length" && k != "Transfer-Encoding" {
			w.Header()[k] = append([]string(nil), vs...)
		}
	}
	w.WriteHeader(e.Status)
	w.Write(e.ResBody)
}

// forward sends e's request upstream and fills in its reply.
func (p *Proxy) forward(ctx context.Context, e *Exchange) error {
	req, err := http.NewRequestWithContext(ctx, e.Method, p.Upstream+e.URI(), bytes.NewReader(e.ReqBody))
	if err != nil {
		return err
	}
	req.Header = e.ReqHeader.Clone()
	req.Host = ""
	res, err := p.Client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(res.Body, MaxBody))
	if err != nil {
		return err
	}
	e.Status, e.Upstream = res.StatusCode, res.StatusCode
	e.ResHeader = res.Header.Clone()
	e.ResBody, e.UpstreamBody = rb, bytes.Clone(rb)
	return nil
}

// Replay sends e's request upstream again, byte for byte (after edit, when
// not nil), as a sniffer that copied it would: no attack applies and nothing
// is logged. It returns the exchange with the server's reply.
func (p *Proxy) Replay(ctx context.Context, e Exchange, edit Attack) (Exchange, error) {
	e = e.Clone()
	e.Status, e.ResHeader, e.ResBody, e.Upstream, e.UpstreamBody = 0, nil, nil, 0, nil
	if edit != nil {
		edit(&e)
	}
	if err := p.forward(ctx, &e); err != nil {
		return Exchange{}, err
	}
	return e, nil
}
