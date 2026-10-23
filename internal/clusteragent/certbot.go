package clusteragent

import (
	"encoding/json"
	"net"
	"os"
	"path"
	"strings"
)

// Certbot orchestration (P3): a PHP-free serve-only node issues/renews its own
// TLS cert. The agent resolves its domains from the replica, execs the kept
// certbot binary with --webroot, and syncs the result to bin/nginx/conf/
// server.{crt,key}, which the Go front hot-reloads (internal/front/cert.go). No
// ACME client — certbot stays. Dormant until PHP-free (certbotWebrootFor), so it
// never races cron:certbot. See
// docs/superpowers/plans/2026-10-23-lb-p3-certbot-orchestration.md.

// validHostname ports FILTER_VALIDATE_DOMAIN|FLAG_HOSTNAME: labels of letters,
// digits and hyphens (no leading/trailing hyphen), 1..63 each, ≤253 total, and
// not an IP address.
func validHostname(s string) bool {
	if s == "" || len(s) > 253 || net.ParseIP(s) != nil {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

// certbotDomains reads the node replica section (node.json: {etag, data}) and
// returns the valid hostnames in its domain_name (CSV), de-duplicated in
// first-seen order. Ports CertbotCommand::hostNames (trim + basename + validate).
func certbotDomains(nodeJSONPath string) []string {
	b, err := os.ReadFile(nodeJSONPath)
	if err != nil {
		return nil
	}
	var nj struct {
		Data struct {
			DomainName string `json:"domain_name"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &nj) != nil || nj.Data.DomainName == "" {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, d := range strings.Split(nj.Data.DomainName, ",") {
		d = path.Base(strings.TrimSpace(d))
		if validHostname(d) && !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	return out
}
