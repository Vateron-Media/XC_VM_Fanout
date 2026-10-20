// Package gateway answers a node's HLS segment, key and playlist-refresh
// requests without PHP-FPM (Phase 12, the panel's
// docs/superpowers/specs/2026-10-08-lb-segment-gateway-and-native-restreamer-design.md).
// Whatever it does not fully own goes back to PHP, so every step can be undone.
package gateway

import "github.com/Vateron-Media/XC_VM_Fanout/internal/viewertoken"

// The stream-link token open/mint now lives in internal/viewertoken (shared with
// the cluster agent's admin uitokens, which read the same panel-minted tokens).
// These aliases keep the gateway's call sites byte-for-byte unchanged.
type (
	Key        = viewertoken.Key
	Keys       = viewertoken.Keys
	TokenState = viewertoken.TokenState
)

const (
	Refused = viewertoken.Refused
	Opened  = viewertoken.Opened
	Unsure  = viewertoken.Unsure

	sealNonce = viewertoken.NonceLen
)

var (
	Seal          = viewertoken.Seal
	EncryptLegacy = viewertoken.EncryptLegacy
)

func pick(list []Key, i int, now int64) []byte { return viewertoken.Pick(list, i, now) }
