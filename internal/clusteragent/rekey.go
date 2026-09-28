package clusteragent

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// Re-key: recovery for a node whose tokens have all expired while its keys
// are intact (after an outage, or once a withdrawn licence is back).
//
//  1. GET challenge?cn=<uuid> — a single-use challenge in a document signed
//     by the pinned panel key ("hlt");
//  2. POST token_rekey — epoch 0 and no MAC (there is no session); the body is
//     SEALed to the panel box key under the request context and the request
//     is signed with the node key. It carries the challenge and a fresh
//     per-epoch key;
//  3. MAIN answers with a document signed "pre" (a granting record, so only
//     under a valid licence) that names this node and request, holding the
//     new epoch's token sealed to that key.
//
// MAIN allows one attempt per node a minute and only for an active node.

// ErrUnlicensed means MAIN's challenge says its licence is not valid, so a
// re-key would be refused; the node asks again later (RekeyPoll).
var ErrUnlicensed = errors.New("clusteragent: MAIN's licence is not valid; re-key deferred")

// Challenge is MAIN's signed answer to GET challenge?cn=.
type Challenge struct {
	Challenge  []byte
	LicenceOK  bool
	MainTimeMs int64
	// Policy is MAIN's transport policy, signed with the challenge (policy.go).
	Policy *Policy
}

// Challenge fetches a re-key challenge for this node from the first URL that
// answers with a document signed by the panel key and naming this node.
func (c *Client) Challenge(ctx context.Context) (*Challenge, error) {
	var lastErr error = ErrTransport
	for _, base := range c.urls() {
		ch, err := c.challenge(ctx, base)
		if err == nil {
			return ch, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func (c *Client) challenge(ctx context.Context, base string) (*Challenge, error) {
	status, h, body, err := getSigned(ctx, c.HTTP, strings.TrimRight(base, "/")+"/challenge?cn="+url.QueryEscape(c.State.NodeUUID))
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK || !panelSigned(c.State.PanelSignPub, "hlt", h, body) {
		return nil, fmt.Errorf("%w: challenge (HTTP %d) from %s", ErrTransport, status, base)
	}
	var doc struct {
		Typ        string  `json:"typ"`
		Cn         string  `json:"cn"`
		Challenge  string  `json:"challenge"`
		MainTimeMs int64   `json:"main_time_ms"`
		LicenceOK  bool    `json:"licence_ok"`
		Policy     *Policy `json:"policy"`
	}
	if json.Unmarshal(body, &doc) != nil || doc.Typ != "xcvm-challenge" || doc.Cn != c.State.NodeUUID {
		return nil, ErrTransport
	}
	raw, err := base64.StdEncoding.DecodeString(doc.Challenge)
	if err != nil || len(raw) != 32 {
		return nil, ErrTransport
	}
	return &Challenge{Challenge: raw, LicenceOK: doc.LicenceOK, MainTimeMs: doc.MainTimeMs, Policy: doc.Policy}, nil
}

// PanelBoxPub is the panel's X25519 key that pre-token bodies are sealed to.
// Nodes installed by this release get it with their install data; a node
// enrolled before takes it once from MAIN's signed health document.
func (c *Client) PanelBoxPub(ctx context.Context) ([]byte, error) {
	c.State.mu.Lock()
	have := append([]byte{}, c.State.PanelBoxPub...)
	c.State.mu.Unlock()
	if len(have) == 32 {
		return have, nil
	}
	var lastErr error = ErrTransport
	for _, base := range c.urls() {
		doc, err := c.Health(ctx, base)
		if err != nil {
			lastErr = err
			continue
		}
		s, _ := doc["panel_box_pub"].(string)
		key, err := base64.StdEncoding.DecodeString(s)
		if err != nil || len(key) != 32 {
			lastErr = ErrTransport
			continue
		}
		c.State.mu.Lock()
		c.State.PanelBoxPub = key
		err = c.State.saveLocked()
		c.State.mu.Unlock()
		return key, err
	}
	return nil, lastErr
}

// Rekey trades a fresh challenge for a new epoch. identity is sent as it is in
// hello (instance_id, boot_id, agent_version); MAIN quarantines a node whose
// instance_id differs from the enrolled one. On success the node holds only
// the new epoch.
func (c *Client) Rekey(ctx context.Context, identity map[string]any) (*cc.Token, error) {
	boxPub, err := c.PanelBoxPub(ctx)
	if err != nil {
		return nil, err
	}
	ch, err := c.Challenge(ctx)
	if err != nil {
		return nil, err
	}
	if ch.MainTimeMs > 0 {
		// Only the request timestamp depends on it; MAIN checks the window.
		// A challenge answers no request of this node's, so an old copy
		// replays: it stamps requests and never anchors the lease's clock.
		c.setOffset(ch.MainTimeMs)
	}
	if !ch.LicenceOK {
		return nil, ErrUnlicensed
	}
	var tok *cc.Token
	err = withReplay(ctx, c.setMainTime, func() error {
		var err error
		tok, err = c.rekeyOnce(ctx, boxPub, ch, identity)
		return err
	})
	return tok, err
}

// rekeyOnce sends one token_rekey with the challenge: a fresh per-epoch key,
// nonce and stamp each time.
func (c *Client) rekeyOnce(ctx context.Context, boxPub []byte, ch *Challenge, identity map[string]any) (*cc.Token, error) {
	ephSk, ephPub, err := cc.NewX25519()
	if err != nil {
		return nil, err
	}
	payload := map[string]any{}
	for k, v := range identity {
		payload[k] = v
	}
	payload["challenge"] = base64.StdEncoding.EncodeToString(ch.Challenge)
	payload["eph_pub"] = base64.StdEncoding.EncodeToString(ephPub)
	plain, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	r, err := newRequest(c.Agent, c.State.NodeUUID, "token_rekey", octet, 0, c.MainNowMs)
	if err != nil {
		return nil, err
	}
	nonce := r.nonce
	body, err := cc.Seal(boxPub, "rekey", string(r.ctx), plain)
	if err != nil {
		return nil, err
	}
	// Epoch 0 and no X-XCVM-Sig: there is no session, only the node key.
	h := r.header(body, nil, c.State.SignKey())

	var lastErr error = ErrTransport
	for _, base := range c.urls() {
		st, rh, rb, err := postWith(ctx, c.HTTP, strings.TrimRight(base, "/")+"/token_rekey", h, body)
		if err != nil {
			c.reached(ctx, base, err)
			lastErr = err
			continue
		}
		if st == http.StatusOK {
			tok, err := c.acceptRekey(rh, rb, nonce, ephSk)
			if err == nil {
				c.reached(ctx, base, nil)
				return tok, nil
			}
			if errors.Is(err, ErrTransport) {
				// A 200 whose panel signature or document does not check is
				// this URL's failure, as in callOnce.
				c.reached(ctx, base, ErrTransport)
				lastErr = fmt.Errorf("%w: an unsigned re-key reply from %s", ErrTransport, base)
				continue
			}
		}
		c.reached(ctx, base, nil)
		if st != http.StatusOK {
			if d := c.denial(st, rh, rb, nonce); d != nil {
				return nil, d
			}
		}
		lastErr = fmt.Errorf("%w: HTTP %d from %s", ErrTransport, st, base)
	}
	return nil, lastErr
}

// acceptRekey checks a re-key reply and makes its epoch the node's only one.
func (c *Client) acceptRekey(h http.Header, body, nonce, ephSk []byte) (*cc.Token, error) {
	if !panelSigned(c.State.PanelSignPub, "pre", h, body) {
		return nil, ErrTransport
	}
	var doc struct {
		Typ         string          `json:"typ"`
		Node        string          `json:"node"`
		Nonce       string          `json:"req_nonce"`
		TokenSealed string          `json:"token_sealed"`
		Epoch       uint64          `json:"epoch"`
		MainTimeMs  int64           `json:"main_time_ms"`
		Lease       json.RawMessage `json:"lease"`
	}
	if json.Unmarshal(body, &doc) != nil || doc.Typ != "xcvm-rekey" || doc.Node != c.State.NodeUUID || doc.Nonce != hex.EncodeToString(nonce) {
		return nil, ErrTransport
	}
	// A re-key is how a node whose tokens all expired comes back; the lease that
	// arrives with the new token is the one it serves on from here.
	return c.takeToken(doc.TokenSealed, doc.Epoch, ephSk, doc.Lease, true, doc.MainTimeMs)
}

// retryAfterMs is a denial's retry_after_ms, or 0.
func retryAfterMs(d *Denial) int64 { return d.RetryAfterMs }

// needsRekey reports whether err means the node has no usable token: MAIN
// says its epoch is gone, or (hard revocation mode) the licence is invalid.
func needsRekey(err error) bool {
	if errors.Is(err, ErrNoEpoch) {
		return true
	}
	var d *Denial
	if !errors.As(err, &d) {
		return false
	}
	return d.Reason == "TOKEN_EXPIRED" || d.Reason == "LICENCE_INVALID"
}
