package gateway

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// PolicyVersion is the policy file's format (the panel's GatewayPolicy::VERSION).
const PolicyVersion = 1

// StaleAfter is how old a policy may be before every request goes to PHP:
// the panel rewrites it every minute (cron:cache).
const StaleAfter = 600

// Policy is the panel's tmp/gateway/policy.json (Core/Gateway/GatewayPolicy):
// what the gateway needs to answer as segment.php and key.php do, written by
// PHP from the caches those endpoints read.
type Policy struct {
	V         int    `json:"v"`
	ServerID  int    `json:"server_id"`
	WrittenAt int64  `json:"written_at"`
	Mode      string `json:"mode"`
	RawKeys   struct {
		Viewer          []hexKey `json:"viewer"`
		Shared          []hexKey `json:"shared"`
		Context         []hexKey `json:"context"`
		AcceptLegacyCBC bool     `json:"accept_legacy_cbc"`
	} `json:"keys"`
	RestrictSameIP bool `json:"restrict_same_ip"`
	IPSubnetMatch  bool `json:"ip_subnet_match"`
	EncryptHLS     bool `json:"encrypt_hls"`
	Headers        struct {
		Server     string `json:"server"`
		Protection bool   `json:"protection"`
		AltsvcPort int    `json:"altsvc_port"`
	} `json:"headers"`
	// ServeUntil is the node's lease fence on this host's clock: nothing is
	// served past it (PHP answers from then). 0: no lease limits the node.
	ServeUntil int64 `json:"serve_until"`
	Paths      struct {
		Cons      string `json:"cons"`
		Streams   string `json:"streams"`
		Archive   string `json:"archive"`
		Flood     string `json:"flood"`
		AgentSock string `json:"agent_sock"`
		Signals   string `json:"signals"`
		Spool     string `json:"spool"`
		Flows     string `json:"flows"`
	} `json:"paths"`
	// VerifyHost and AllowedDomains: StreamingRequestBootstrap's host check.
	VerifyHost     bool     `json:"verify_host"`
	AllowedDomains []string `json:"allowed_domains"`
	// ConnStore is where the node keeps its viewers: "agent" (the CONNECTIONS
	// flow on) or "php" (MAIN's Redis or MySQL, which only PHP reaches).
	ConnStore string `json:"conn_store"`
	// TimeOffset is the node's servers.time_offset: hls_last_read is now minus it.
	TimeOffset int64 `json:"time_offset"`
	// Live is what live.php reads for a playlist refresh.
	Live LivePolicy `json:"live"`
	// Redirect is each other server's base URLs, for a token it minted.
	Redirect map[string][]string `json:"redirect"`

	Keys Keys `json:"-"`
}

// LivePolicy is the settings live.php reads for a playlist refresh.
type LivePolicy struct {
	UseBuffer        bool  `json:"use_buffer"`
	InstantOff       bool  `json:"on_demand_instant_off"`
	Disallow2ndIP    bool  `json:"disallow_2nd_ip_con"`
	Disallow2ndIPMax int64 `json:"disallow_2nd_ip_max"`
	UniqueHeader     bool  `json:"unique_header"`
}

type hexKey struct {
	Hex   string `json:"hex"`
	Until *int64 `json:"until"`
}

// ParsePolicy reads a policy file's bytes.
func ParsePolicy(b []byte) (*Policy, error) {
	var p Policy
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	if p.V != PolicyVersion {
		return nil, fmt.Errorf("policy version %d, want %d", p.V, PolicyVersion)
	}
	var err error
	conv := func(list []hexKey) []Key {
		out := make([]Key, 0, len(list))
		for _, k := range list {
			v, e := hex.DecodeString(k.Hex)
			if e != nil && err == nil {
				err = fmt.Errorf("policy key: %w", e)
			}
			var until int64
			if k.Until != nil {
				until = *k.Until
			}
			out = append(out, Key{Value: v, Until: until})
		}
		return out
	}
	p.Keys = Keys{Viewer: conv(p.RawKeys.Viewer), Shared: conv(p.RawKeys.Shared), Context: conv(p.RawKeys.Context)}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// Fresh reports whether the policy is recent enough to act on: written in
// the last StaleAfter seconds (and not from a clock far ahead of this one).
func (p *Policy) Fresh(now int64) bool {
	return p != nil && p.WrittenAt > 0 && now-p.WrittenAt <= StaleAfter && p.WrittenAt-now <= 60
}

// PolicyFile is the policy as the file holds it now: re-read when its mtime
// or size changes, looked at no more than once a second.
type PolicyFile struct {
	path string

	mu      sync.Mutex
	checked time.Time
	mtime   time.Time
	size    int64
	cur     *Policy
}

// NewPolicyFile watches path.
func NewPolicyFile(path string) *PolicyFile {
	return &PolicyFile{path: path}
}

// Get returns the policy to act on at now, or nil: no file, an unreadable
// one, or one gone stale. A nil policy sends every request to PHP.
func (f *PolicyFile) Get(now time.Time) *Policy {
	f.mu.Lock()
	defer f.mu.Unlock()
	if now.Sub(f.checked) >= time.Second {
		f.checked = now
		f.reload()
	}
	if !f.cur.Fresh(now.Unix()) {
		return nil
	}
	return f.cur
}

func (f *PolicyFile) reload() {
	st, err := os.Stat(f.path)
	if err != nil {
		f.cur, f.mtime, f.size = nil, time.Time{}, 0
		return
	}
	if f.cur != nil && st.ModTime().Equal(f.mtime) && st.Size() == f.size {
		return
	}
	b, err := os.ReadFile(f.path)
	if err != nil {
		f.cur = nil
		return
	}
	p, err := ParsePolicy(b)
	if err != nil {
		f.cur = nil
		return
	}
	f.cur, f.mtime, f.size = p, st.ModTime(), st.Size()
}
