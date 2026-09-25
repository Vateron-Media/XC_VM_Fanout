package clusteragent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// Break-glass enrolment by code (MAIN's EnrolCodeService): for a node MAIN
// cannot reach over SSH. The admin issues a code on MAIN; on the node,
// `xc_agent enrol <code>`:
//
//  1. pins the panel key by the hash the code carries, from MAIN's signed
//     health document at the code's URL;
//  2. makes the node's keys and sends enrol_code — MAC'd with the code's
//     K_req, signed with the new node key, the body SEALed to the panel box
//     key — then shows the SAS;
//  3. polls enrol_code_status every 10 s until the admin approves by typing
//     that SAS; the approval carries epoch 1, panel-signed ("pre") and MAC'd
//     with K_res; the state is written as the SSH install would.
//
// A sniffed code gives an attacker only a pending request the admin will not
// approve, as its SAS will not match the node's.

// EnrolPoll is how often a waiting node asks for the admin's decision.
var EnrolPoll = 10 * time.Second

// EnrolWait bounds the wait for approval (the code's lifetime).
var EnrolWait = 30 * time.Minute

// ErrEnrolRejected means the admin rejected the request (or it met too many
// wrong SAS entries): a new code is needed.
var ErrEnrolRejected = errors.New("clusteragent: the enrolment request was rejected on MAIN")

// Code is a decoded enrolment code.
type Code struct {
	ServerID uint32
	MainURL  string // scheme://host:port
	PanelFP  []byte // SHA-256(panel_sign_pub)[0:16]
	Secret   []byte
}

// ParseCode decodes base32(u8 1 ‖ u32 sid ‖ u8 len ‖ main_url ‖ fp[16] ‖ secret[16]),
// ignoring case, spaces and dashes.
func ParseCode(s string) (*Code, error) {
	text := strings.ToUpper(strings.NewReplacer("-", "", " ", "", "\n", "", "\t", "").Replace(s))
	raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(text)
	if err != nil || len(raw) < 6 || raw[0] != 1 {
		return nil, errors.New("clusteragent: not an enrolment code")
	}
	n := int(raw[5])
	if len(raw) != 6+n+32 {
		return nil, errors.New("clusteragent: not an enrolment code")
	}
	c := &Code{ServerID: binary.BigEndian.Uint32(raw[1:5]), MainURL: string(raw[6 : 6+n]), PanelFP: raw[6+n : 6+n+16], Secret: raw[6+n+16:]}
	if c.ServerID == 0 || !(strings.HasPrefix(c.MainURL, "http://") || strings.HasPrefix(c.MainURL, "https://")) {
		return nil, errors.New("clusteragent: not an enrolment code")
	}
	return c, nil
}

// Keys are the code's K_req and K_res.
func (c *Code) Keys() (req, res []byte) {
	salt := cc.U32(c.ServerID)
	return cc.HKDF(c.Secret, salt, []byte("xcvm/enrol-code/v1/req"), 32), cc.HKDF(c.Secret, salt, []byte("xcvm/enrol-code/v1/res"), 32)
}

func (c *Code) node() string { return "sid:" + strconv.FormatUint(uint64(c.ServerID), 10) }

// codeClient speaks the two code ops to one MAIN URL.
type codeClient struct {
	http     *http.Client
	agent    string
	base     string
	code     *Code
	req, res []byte
	panelPub []byte
	offsetMs int64
}

// pin fetches the health document and accepts its panel key only when it
// matches the code's hash and signs the document.
func (cl *codeClient) pin(ctx context.Context) (boxPub []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cl.base+"health", nil)
	if err != nil {
		return nil, err
	}
	res, err := cl.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if err != nil {
		return nil, err
	}
	var doc struct {
		MainTimeMs   int64  `json:"main_time_ms"`
		PanelSignPub string `json:"panel_sign_pub"`
		PanelBoxPub  string `json:"panel_box_pub"`
	}
	if json.Unmarshal(body, &doc) != nil {
		return nil, fmt.Errorf("%w: health from %s", ErrTransport, cl.base)
	}
	pub, _ := base64.StdEncoding.DecodeString(doc.PanelSignPub)
	box, _ := base64.StdEncoding.DecodeString(doc.PanelBoxPub)
	sig, _ := base64.RawURLEncoding.DecodeString(res.Header.Get(cc.HPanelSig))
	if len(pub) != ed25519.PublicKeySize || !hmac.Equal(cc.SHA256(pub)[:16], cl.code.PanelFP) {
		return nil, errors.New("clusteragent: MAIN's panel key does not match the code (wrong code, or not the MAIN that issued it)")
	}
	if !cc.VerifyPanel(pub, "hlt", body, sig) || len(box) != 32 {
		return nil, fmt.Errorf("%w: health from %s is not signed by the panel key", ErrTransport, cl.base)
	}
	cl.panelPub = pub
	if doc.MainTimeMs > 0 {
		cl.offsetMs = doc.MainTimeMs - time.Now().UnixMilli()
	}
	return box, nil
}

// call sends one code op and returns the verified reply body and its headers,
// or a verified *Denial.
func (cl *codeClient) call(ctx context.Context, op, contentType string, body func(reqCtx []byte) ([]byte, error), signKey ed25519.PrivateKey) ([]byte, http.Header, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}
	ts := uint64(time.Now().UnixMilli() + cl.offsetMs)
	reqCtx, err := cc.RequestContext(cc.Request{
		Proto: Proto, Agent: cl.agent, Method: "POST", Path: cc.PathPrefix + op, ContentType: contentType,
		Node: cl.code.node(), Epoch: 0, TsMs: ts, Nonce: nonce,
	})
	if err != nil {
		return nil, nil, err
	}
	b, err := body(reqCtx)
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cl.base+op, bytes.NewReader(b))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set(cc.HProto, strconv.Itoa(Proto))
	req.Header.Set(cc.HAgent, cl.agent)
	req.Header.Set(cc.HNode, cl.code.node())
	req.Header.Set(cc.HEpoch, "0")
	req.Header.Set(cc.HTs, strconv.FormatUint(ts, 10))
	req.Header.Set(cc.HNonce, hex.EncodeToString(nonce))
	req.Header.Set(cc.HSig, hex.EncodeToString(cc.MAC(cl.req, reqCtx, b)))
	req.Header.Set("Content-Type", contentType)
	if signKey != nil {
		req.Header.Set(cc.HNodeSig, hex.EncodeToString(cc.SignNode(signKey, "request", append(append([]byte{}, reqCtx...), cc.SHA256(b)...))))
	}
	res, err := cl.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(res.Body, MaxReply+1))
	if err != nil || len(rb) > MaxReply {
		return nil, nil, ErrTransport
	}
	if res.StatusCode == http.StatusOK {
		rts, err1 := strconv.ParseUint(res.Header.Get(cc.HTs), 10, 64)
		rn, err2 := hex.DecodeString(res.Header.Get(cc.HNonce))
		mac, err3 := hex.DecodeString(res.Header.Get(cc.HSig))
		if err1 != nil || err2 != nil || err3 != nil || len(rn) != 16 {
			return nil, nil, ErrTransport
		}
		resCtx, err := cc.ResponseContext(reqCtx, 200, res.Header.Get("Content-Type"), rts, rn)
		if err != nil || !cc.VerifyMAC(cl.res, resCtx, rb, mac) {
			return nil, nil, ErrTransport
		}
		return rb, res.Header, nil
	}
	sig, err := base64.RawURLEncoding.DecodeString(res.Header.Get(cc.HPanelSig))
	var d Denial
	if err == nil && cc.VerifyPanel(cl.panelPub, "den", rb, sig) && json.Unmarshal(rb, &d) == nil && d.Node == cl.code.node() && d.Nonce == hex.EncodeToString(nonce) {
		d.Status = res.StatusCode
		d.Doc = append(json.RawMessage{}, rb...)
		return nil, nil, &d
	}
	return nil, nil, fmt.Errorf("%w: HTTP %d from %s", ErrTransport, res.StatusCode, cl.base)
}

// ErrAlreadyEnrolled refuses to replace a node's working identity unless asked.
var ErrAlreadyEnrolled = errors.New("clusteragent: this node already holds a cluster identity (pass -force to replace it)")

// EnrolByCode enrols this node with a code and writes its state at path.
// onSAS receives the SAS to show the admin once MAIN holds the request.
// replace allows discarding an identity that already holds tokens.
func EnrolByCode(ctx context.Context, path, codeText, agent string, replace bool, onSAS func(sas string)) error {
	code, err := ParseCode(codeText)
	if err != nil {
		return err
	}
	if st, err := loadRaw(path); err == nil && len(st.Epochs) > 0 && !replace {
		return ErrAlreadyEnrolled
	}
	req, res := code.Keys()
	cl := &codeClient{http: &http.Client{Timeout: 10 * time.Second}, agent: agent, base: strings.TrimRight(code.MainURL, "/") + cc.PathPrefix, code: code, req: req, res: res}
	boxPub, err := cl.pin(ctx)
	if err != nil {
		return err
	}

	// The node's identity: a retry of an unfinished enrolment keeps its keys
	// (MAIN accepts the same request again); anything else starts afresh.
	uuid := ""
	if st, err := loadRaw(path); err == nil && len(st.Epochs) == 0 && uuidRe.MatchString(st.NodeUUID) {
		uuid = st.NodeUUID
	}
	if uuid == "" {
		if uuid, err = newUUID(); err != nil {
			return err
		}
	}
	if _, err := Keygen(path, uuid); err != nil {
		return err
	}
	st, err := loadRaw(path)
	if err != nil {
		return err
	}
	signKey := st.SignKey()
	signPub := signKey.Public().(ed25519.PublicKey)
	nodeBoxPub, _ := cc.X25519Public(st.NodeBoxSk)
	ephPub, _ := cc.X25519Public(st.PendingEphSk)
	payload, _ := json.Marshal(map[string]string{
		"node_uuid": uuid, "sign_pub": base64.StdEncoding.EncodeToString(signPub), "box_pub": base64.StdEncoding.EncodeToString(nodeBoxPub),
		"eph_pub": base64.StdEncoding.EncodeToString(ephPub), "instance_id": st.InstanceID,
	})
	if _, _, err := cl.call(ctx, "enrol_code", octet, func(reqCtx []byte) ([]byte, error) {
		return cc.Seal(boxPub, "enrol_code", string(reqCtx), payload)
	}, signKey); err != nil {
		return err
	}
	if onSAS != nil {
		onSAS(SAS(uuid, signPub, nodeBoxPub))
	}

	status, _ := json.Marshal(map[string]string{"node_uuid": uuid})
	deadline := time.Now().Add(EnrolWait)
	for {
		rb, h, err := cl.call(ctx, "enrol_code_status", "application/json", func([]byte) ([]byte, error) { return status, nil }, nil)
		var d *Denial
		switch {
		case errors.As(err, &d):
			return err
		case err != nil:
			// A transport failure: keep waiting.
		default:
			var doc struct {
				Typ   string `json:"typ"`
				State string `json:"state"`
			}
			if json.Unmarshal(rb, &doc) != nil {
				return ErrTransport
			}
			if doc.Typ == "xcvm-enrol-approved" {
				return cl.install(st, rb, h, boxPub)
			}
			if doc.State == "rejected" {
				return ErrEnrolRejected
			}
		}
		if time.Now().After(deadline) {
			return errors.New("clusteragent: no decision on MAIN before the code expired")
		}
		if !sleep(ctx, EnrolPoll) {
			return ctx.Err()
		}
	}
}

// install checks the approval (panel-signed, this node, this server, epoch 1
// sealed to the pending key) and completes the state, as Install does.
func (cl *codeClient) install(st *State, body []byte, h http.Header, boxPub []byte) error {
	sig, err := base64.RawURLEncoding.DecodeString(h.Get(cc.HPanelSig))
	if err != nil || !cc.VerifyPanel(cl.panelPub, "pre", body, sig) {
		return fmt.Errorf("%w: the approval is not signed by the panel", ErrTransport)
	}
	var doc struct {
		NodeUUID    string `json:"node_uuid"`
		ServerID    int64  `json:"server_id"`
		Epoch       uint64 `json:"epoch"`
		TokenSealed string `json:"token_sealed"`
		Cluster     struct {
			Policy struct {
				PolicyVer int      `json:"policy_ver"`
				MainURLs  []string `json:"main_urls"`
			} `json:"policy"`
		} `json:"cluster"`
	}
	if json.Unmarshal(body, &doc) != nil || doc.NodeUUID != st.NodeUUID || doc.ServerID != int64(cl.code.ServerID) || doc.Epoch != 1 {
		return errors.New("clusteragent: the approval is not for this node")
	}
	sealed, err := base64.StdEncoding.DecodeString(doc.TokenSealed)
	if err != nil {
		return ErrTransport
	}
	tok, _, err := cc.OpenToken(st.PendingEphSk, cl.panelPub, st.NodeUUID, sealed)
	if err != nil || tok.Epoch != 1 || tok.ServerID != doc.ServerID {
		return fmt.Errorf("clusteragent: first token: %v", err)
	}
	urls := doc.Cluster.Policy.MainURLs
	if len(urls) == 0 {
		urls = []string{cl.base}
	}
	st.ServerID, st.PanelSignPub, st.PanelBoxPub, st.MainURLs, st.PolicyVer = doc.ServerID, cl.panelPub, boxPub, urls, doc.Cluster.Policy.PolicyVer
	st.Epochs = []Epoch{{Epoch: 1, EphSk: st.PendingEphSk, TokenSealed: sealed}}
	st.PendingEphSk = nil
	st.Enrolled = false
	return st.Save()
}

func newUUID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], nil
}
