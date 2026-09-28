package clusteragent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// The wire pieces every POST to MAIN shares (ADR 0004, "MAIN's API"): the
// session ops (callOnce), token_rekey (rekeyOnce) and the code ops
// (codeClient.callOnce). Each builds its body its own way (BOX, SEAL, plain
// JSON) and authenticates with what it has: a session MAC, the code's K_req,
// or none.

// maxSigned caps a panel-signed document fetched by GET (health, challenge).
const maxSigned = 64 << 10

// outRequest is one request as sent: a fresh nonce and stamp and the request
// context they are bound into. A retry builds a new one.
type outRequest struct {
	agent, node, contentType string
	epoch                    uint64
	ts                       uint64
	nonce                    []byte
	ctx                      []byte
}

// newRequest draws a nonce, stamps it on MAIN's clock (mainNowMs) and builds
// the context of a POST of op.
func newRequest(agent, node, op, contentType string, epoch uint64, mainNowMs func() int64) (*outRequest, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	r := &outRequest{agent: agent, node: node, contentType: contentType, epoch: epoch, ts: uint64(mainNowMs()), nonce: nonce}
	var err error
	r.ctx, err = cc.RequestContext(cc.Request{
		Proto: Proto, Agent: agent, Method: "POST", Path: cc.PathPrefix + op, ContentType: contentType,
		Node: node, Epoch: epoch, TsMs: r.ts, Nonce: nonce,
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// header is the request's headers for body. macKey adds X-XCVM-Sig (none on
// token_rekey, which has no session); signKey adds X-XCVM-Node-Sig, over the
// context and the body's hash, for the ops that require it.
func (r *outRequest) header(body, macKey []byte, signKey ed25519.PrivateKey) http.Header {
	h := http.Header{}
	h.Set(cc.HProto, strconv.Itoa(Proto))
	h.Set(cc.HAgent, r.agent)
	h.Set(cc.HNode, r.node)
	h.Set(cc.HEpoch, strconv.FormatUint(r.epoch, 10))
	h.Set(cc.HTs, strconv.FormatUint(r.ts, 10))
	h.Set(cc.HNonce, hex.EncodeToString(r.nonce))
	if macKey != nil {
		h.Set(cc.HSig, hex.EncodeToString(cc.MAC(macKey, r.ctx, body)))
	}
	h.Set("Content-Type", r.contentType)
	if signKey != nil {
		h.Set(cc.HNodeSig, hex.EncodeToString(cc.SignNode(signKey, "request", append(append([]byte{}, r.ctx...), cc.SHA256(body)...))))
	}
	return h
}

// postWith sends one POST over hc and reads at most MaxReply of the reply; a
// longer or unreadable reply is ErrTransport.
func postWith(ctx context.Context, hc *http.Client, url string, h http.Header, body []byte) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header = h.Clone()
	res, err := hc.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer res.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(res.Body, MaxReply+1))
	if err != nil || len(rb) > MaxReply {
		return 0, nil, nil, ErrTransport
	}
	return res.StatusCode, res.Header, rb, nil
}

// replyContext checks a MAC'd reply to the request whose context is reqCtx:
// its stamp, nonce and MAC under macKey. It returns the reply's context (what
// a BOX'd reply opens under), or ErrTransport.
func replyContext(macKey, reqCtx []byte, status int, h http.Header, body []byte) ([]byte, error) {
	ts, err1 := strconv.ParseUint(h.Get(cc.HTs), 10, 64)
	nonce, err2 := hex.DecodeString(h.Get(cc.HNonce))
	mac, err3 := hex.DecodeString(h.Get(cc.HSig))
	if err1 != nil || err2 != nil || err3 != nil || len(nonce) != 16 {
		return nil, ErrTransport
	}
	resCtx, err := cc.ResponseContext(reqCtx, uint32(status), h.Get("Content-Type"), ts, nonce)
	if err != nil || !cc.VerifyMAC(macKey, resCtx, body, mac) {
		return nil, ErrTransport
	}
	return resCtx, nil
}

// panelSigned reports whether h carries the panel key's signature of body
// under tag.
func panelSigned(panelPub []byte, tag string, h http.Header, body []byte) bool {
	sig, err := base64.RawURLEncoding.DecodeString(h.Get(cc.HPanelSig))
	return err == nil && cc.VerifyPanel(panelPub, tag, body, sig)
}

// verifyDenial returns the refusal in a reply when it is panel-signed ("den")
// and names node and the request's nonce, or nil.
func verifyDenial(panelPub []byte, node string, status int, h http.Header, body, nonce []byte) *Denial {
	if !panelSigned(panelPub, "den", h, body) {
		return nil
	}
	var d Denial
	if json.Unmarshal(body, &d) != nil || d.Node != node || d.Nonce != hex.EncodeToString(nonce) {
		return nil
	}
	d.Status = status
	d.Doc = append(json.RawMessage{}, body...)
	return &d
}

// getSigned fetches a panel-signed document (health, challenge) over hc: its
// status, headers and at most maxSigned of its body. The caller checks the
// signature, with the key it trusts.
func getSigned(ctx context.Context, hc *http.Client, url string) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	res, err := hc.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, maxSigned))
	if err != nil {
		return 0, nil, nil, err
	}
	return res.StatusCode, res.Header, body, nil
}
