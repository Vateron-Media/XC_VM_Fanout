package clusteragent

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// The install flow (MAIN's LbInstallFlow::provisionCluster, over verified SSH):
//
//  1. keygen  — the node's keys are made here and never leave the node; the
//     public halves and the first per-epoch key go back over SSH;
//  2. probe   — this node checks MAIN's signed health with the panel key it
//     was given over SSH, before MAIN mints any token;
//  3. install — MAIN's reply (epoch 1, sealed to the per-epoch key) is
//     opened and checked before it is saved; then the agent can run.

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// KeygenResult is what goes back to MAIN over SSH.
type KeygenResult struct {
	NodeUUID string `json:"node_uuid"`
	SignPub  string `json:"sign_pub"`
	BoxPub   string `json:"box_pub"`
	EphPub   string `json:"eph_pub"`
	SAS      string `json:"sas"`
}

// SAS is the short authentication string an admin compares on both sides:
// six base32 groups of SHA-256(node_uuid ‖ ed25519_pub ‖ x25519_pub).
func SAS(uuid string, signPub, boxPub []byte) string {
	h := sha256.Sum256(append(append([]byte(uuid), signPub...), boxPub...))
	s := base32.StdEncoding.EncodeToString(h[:15])[:24]
	return strings.Join([]string{s[0:4], s[4:8], s[8:12], s[12:16], s[16:20], s[20:24]}, "-")
}

// Keygen creates (or, for a retried install of the same uuid that has not
// finished, reuses) the node's keys and the first per-epoch key.
func Keygen(path, uuid string) (*KeygenResult, error) {
	if !uuidRe.MatchString(uuid) {
		return nil, errors.New("clusteragent: node uuid")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	st, err := loadRaw(path)
	if err != nil || st.NodeUUID != uuid || len(st.Epochs) > 0 || len(st.NodeSignSeed) != ed25519.SeedSize || len(st.NodeBoxSk) != 32 || len(st.PendingEphSk) != 32 {
		// A new identity: nothing of a previous one is kept.
		st = NewState(path)
		st.NodeUUID = uuid
		st.NodeSignSeed = make([]byte, ed25519.SeedSize)
		if _, err := rand.Read(st.NodeSignSeed); err != nil {
			return nil, err
		}
		if st.NodeBoxSk, _, err = cc.NewX25519(); err != nil {
			return nil, err
		}
		if st.PendingEphSk, _, err = cc.NewX25519(); err != nil {
			return nil, err
		}
		if st.InstanceID, err = randomID(); err != nil {
			return nil, err
		}
		if err := st.Save(); err != nil {
			return nil, err
		}
	}
	signPub := st.SignKey().Public().(ed25519.PublicKey)
	boxPub, err := cc.X25519Public(st.NodeBoxSk)
	if err != nil {
		return nil, err
	}
	ephPub, err := cc.X25519Public(st.PendingEphSk)
	if err != nil {
		return nil, err
	}
	return &KeygenResult{
		NodeUUID: uuid, SignPub: hex.EncodeToString(signPub), BoxPub: hex.EncodeToString(boxPub),
		EphPub: hex.EncodeToString(ephPub), SAS: SAS(uuid, signPub, boxPub),
	}, nil
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// Probe fetches MAIN's health from each URL and returns the first that is
// signed by panelPub.
func Probe(ctx context.Context, panelPub []byte, urls []string) (string, error) {
	c := NewClient(&State{PanelSignPub: panelPub, MainURLs: urls}, "xc_agent/probe")
	var errs []error
	for _, u := range urls {
		if _, err := c.Health(ctx, u); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", u, err))
			continue
		}
		return u, nil
	}
	return "", errors.Join(append([]error{errors.New("clusteragent: no MAIN URL answered with a health document signed by the panel key")}, errs...)...)
}

// InstallData is MAIN's reply to keygen, sent over SSH.
type InstallData struct {
	ServerID     int64    `json:"server_id"`
	PanelSignPub []byte   `json:"panel_sign_pub"`
	MainURLs     []string `json:"main_urls"`
	PolicyVer    int      `json:"policy_ver"`
	Epoch        uint64   `json:"epoch"`
	TokenSealed  []byte   `json:"token_sealed"`
}

// Install completes the state with MAIN's first token after checking it opens
// with this node's per-epoch key, carries the panel's signature and names this
// node and server.
func Install(path string, d InstallData) error {
	st, err := loadRaw(path)
	if err != nil {
		return err
	}
	if len(st.PendingEphSk) != 32 || len(st.Epochs) > 0 {
		return errors.New("clusteragent: no pending enrolment (run keygen first)")
	}
	if len(d.PanelSignPub) != ed25519.PublicKeySize || len(d.MainURLs) == 0 || d.Epoch != 1 {
		return errors.New("clusteragent: install data is incomplete")
	}
	tok, _, err := cc.OpenToken(st.PendingEphSk, d.PanelSignPub, st.NodeUUID, d.TokenSealed)
	if err != nil {
		return fmt.Errorf("clusteragent: first token: %w", err)
	}
	if tok.Epoch != d.Epoch || tok.ServerID != d.ServerID {
		return errors.New("clusteragent: first token does not match this node")
	}
	st.ServerID, st.PanelSignPub, st.MainURLs, st.PolicyVer = d.ServerID, d.PanelSignPub, d.MainURLs, d.PolicyVer
	st.Epochs = []Epoch{{Epoch: d.Epoch, EphSk: st.PendingEphSk, TokenSealed: d.TokenSealed}}
	st.PendingEphSk = nil
	st.Enrolled = false
	return st.Save()
}
