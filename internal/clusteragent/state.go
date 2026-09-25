// Package clusteragent is the LB side of the XC_VM cluster API: the node's
// persistent identity and token epochs, and a client for MAIN's
// /cluster/v1/ operations (enrol_complete, hello, heartbeat, token_refresh).
//
// Every reply is authenticated before it is believed: a session reply by its
// MAC and BOX under the epoch's down keys, a refusal by the pinned panel key
// and by naming this node and this request's nonce. Anything else is a
// transport error and changes nothing.
package clusteragent

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Epoch is one token epoch the node holds: the per-epoch X25519 secret the
// token was sealed to, and the sealed token itself (opened on load, so a
// tampered state file cannot inject a token the panel did not sign).
type Epoch struct {
	Epoch       uint64 `json:"epoch"`
	EphSk       []byte `json:"eph_sk"`
	TokenSealed []byte `json:"token_sealed,omitempty"`
}

// State is the node's persistent identity, written 0600 and atomically.
type State struct {
	NodeUUID     string   `json:"node_uuid"`
	ServerID     int64    `json:"server_id"`
	NodeSignSeed []byte   `json:"node_sign_seed"`
	PanelSignPub []byte   `json:"panel_sign_pub"`
	MainURLs     []string `json:"main_urls"`
	PolicyVer    int      `json:"policy_ver"`
	Enrolled     bool     `json:"enrolled"`
	InstanceID   string   `json:"instance_id"`
	Epochs       []Epoch  `json:"epochs"` // newest first
	// PendingEphSk is the key of a token_refresh whose reply has not arrived.
	// It is persisted before the request is sent, so a retry after a crash or
	// a lost reply uses the same key and MAIN re-sends the same token.
	PendingEphSk []byte `json:"pending_eph_sk,omitempty"`

	path string
	mu   sync.Mutex
}

// LoadState reads the state file.
func LoadState(path string) (*State, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("clusteragent: state %s: %w", path, err)
	}
	if len(s.NodeSignSeed) != ed25519.SeedSize || len(s.PanelSignPub) != ed25519.PublicKeySize || s.NodeUUID == "" {
		return nil, errors.New("clusteragent: state is incomplete")
	}
	s.path = path
	return &s, nil
}

// NewState returns a state that Save writes to path.
func NewState(path string) *State { return &State{path: path} }

// Save writes the state atomically (temp file, fsync, rename), mode 0600.
func (s *State) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *State) saveLocked() error {
	sort.Slice(s.Epochs, func(i, j int) bool { return s.Epochs[i].Epoch > s.Epochs[j].Epoch }) // newest first
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// SignKey is the node's Ed25519 key.
func (s *State) SignKey() ed25519.PrivateKey { return ed25519.NewKeyFromSeed(s.NodeSignSeed) }

// AddEpoch records a newly issued epoch and keeps at most the two newest,
// matching MAIN, which never holds more than a node's current and next epoch.
func (s *State) AddEpoch(e Epoch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Epoch{e}
	for _, old := range s.Epochs {
		if old.Epoch != e.Epoch {
			out = append(out, old)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Epoch > out[j].Epoch })
	if len(out) > 2 {
		out = out[:2]
	}
	s.Epochs = out
}
