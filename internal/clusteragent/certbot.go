package clusteragent

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
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

// certbotSudo is the privilege prefix certbot runs under (certbot writes under
// bin/certbot/, owned via the install's sudoers entry). Tests set it to "".
var certbotSudo = "sudo"

// certbotArgs is the argv after the certbot binary: the serve-only --webroot
// branch of CertbotCommand, one -d per domain.
func certbotArgs(binPath, webroot string, domains []string) []string {
	bin := strings.TrimRight(binPath, "/") + "/"
	args := []string{
		"certonly",
		"--config-dir", bin + "certbot/config",
		"--work-dir", bin + "certbot/work",
		"--logs-dir", bin + "certbot/logs",
		"--agree-tos", "--expand", "--non-interactive", "--register-unsafely-without-email",
		"--webroot", "-w", webroot,
	}
	for _, d := range domains {
		args = append(args, "-d", d)
	}
	return args
}

// runCertbot execs `sudo <certbotBin> certonly --webroot …` (no shell; domains
// are separate args, so a replica-sourced domain cannot inject). It returns
// (true,nil) when a cert was obtained/renewed, (false,nil) when certbot reports
// the cert is not yet due for renewal, and an error on any other non-zero exit.
func runCertbot(ctx context.Context, certbotBin, binPath, webroot string, domains []string) (bool, error) {
	args := certbotArgs(binPath, webroot, domains)
	name := certbotBin
	if certbotSudo != "" {
		name = certbotSudo
		args = append([]string{certbotBin}, args...)
	}
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err == nil {
		return true, nil
	}
	if strings.Contains(strings.ToLower(string(out)), "not yet due for renewal") {
		return false, nil
	}
	return false, fmt.Errorf("certbot: %w: %s", err, strings.TrimSpace(string(out)))
}
