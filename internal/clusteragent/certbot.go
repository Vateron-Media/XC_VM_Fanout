package clusteragent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

// errHelperNoCertbot means the node's root helper predates the certbot verb: an
// existing serve-only node whose agent self-healed to this version before its
// helper was refreshed. The root-owned helper can only be rewritten by a
// server:install from MAIN over SSH (never by the xc_vm agent — that is the
// privilege boundary the helper exists to hold), so auto-TLS stays dormant and
// the node keeps its current certificate until then. Not a failure.
var errHelperNoCertbot = errors.New("node.root helper has no certbot verb")

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

// certbotSudo is the privilege prefix the helper runs under ("sudo -n"; "" in
// tests), and certbotHelper the root-owned xcvm-noderoot (XC_VM #473) that the
// serve-only sudoers grants NOPASSWD. The helper carries certbot's
// certonly/--webroot argv and the fixed bin/certbot + webroot paths, so
// `sudo certbot` — which could run hooks as root — is never granted
// (XC_VM_Fanout#129); the agent passes only the validated domains.
var (
	certbotSudo   = "sudo"
	certbotHelper = noderootHelper
)

// runCertbot runs `sudo -n xcvm-noderoot certbot <domain>…` and reads the
// outcome from certbot's output (the helper passes it through): (true,nil) a
// cert was obtained/renewed, (false,nil) not yet due for renewal, an error on
// any other non-zero exit. Domains are separate args, and the helper rejects
// any that are not a bare hostname, so a replica-sourced domain cannot inject.
func runCertbot(ctx context.Context, domains []string) (bool, error) {
	argv := append([]string{certbotHelper, "certbot"}, domains...)
	if certbotSudo != "" {
		argv = append([]string{certbotSudo, "-n"}, argv...)
	}
	out, err := exec.CommandContext(ctx, argv[0], argv[1:]...).CombinedOutput()
	if err == nil {
		return true, nil
	}
	if strings.Contains(strings.ToLower(string(out)), "not yet due for renewal") {
		return false, nil
	}
	if strings.Contains(strings.ToLower(string(out)), "unknown verb") {
		return false, errHelperNoCertbot
	}
	return false, fmt.Errorf("certbot: %w: %s", err, strings.TrimSpace(string(out)))
}

// certbotEvery is how often a PHP-free node renews its cert; cron:certbot runs daily.
var certbotEvery = 24 * time.Hour

// certbotRefresh resolves the node's domains from the replica and, if any, runs
// certbot. The xcvm-noderoot helper (root) both issues the cert and installs it
// as the front's bin/nginx/conf/server.{crt,key}: certbot's own state is
// root-owned (/etc/letsencrypt), which the xc_vm agent cannot read, so the sync
// happens in the helper, not here. No domains (IP-only node) is a no-op.
func (a *Agent) certbotRefresh(ctx context.Context) error {
	if a.CertbotWebroot == "" {
		return nil
	}
	domains := certbotDomains(strings.TrimRight(a.ReplicaDir, "/") + "/node.json")
	if len(domains) == 0 {
		return nil
	}
	if _, err := runCertbot(ctx, domains); err != nil {
		if errors.Is(err, errHelperNoCertbot) {
			// An existing node updated ahead of its helper: degrade to the
			// pre-feature state (keep the current cert) instead of erroring, and
			// say what refreshes the helper. server:install from MAIN restores it.
			a.logf("cluster: certbot: node.root helper has no certbot verb yet — re-run server:install from MAIN to enable auto-TLS; keeping the current certificate")
			return nil
		}
		return err
	}
	return nil
}

// RunCertbot renews the node's cert once at start and then daily, only on a
// PHP-free serve-only node (CertbotWebroot set by certbotWebrootFor); dormant
// otherwise so it never races cron:certbot / root_signals certbot_generate.
func (a *Agent) RunCertbot(ctx context.Context) {
	if a.CertbotWebroot == "" {
		return
	}
	t := time.NewTicker(certbotEvery)
	defer t.Stop()
	for {
		if err := a.certbotRefresh(ctx); err != nil && ctx.Err() == nil {
			a.logf("cluster: certbot: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
