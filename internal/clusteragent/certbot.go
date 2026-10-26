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
	"time"
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
	return false, fmt.Errorf("certbot: %w: %s", err, strings.TrimSpace(string(out)))
}

// certbotLiveDir resolves certbot's live dir for the primary domain:
// <bin>/certbot/config/live/<primaryDomain>/ when present, else the live subdir
// whose fullchain.pem is newest (certbot's -NNNN suffix on a domain-set change).
// "" when none holds a fullchain.pem.
func certbotLiveDir(binPath, primaryDomain string) string {
	live := strings.TrimRight(binPath, "/") + "/certbot/config/live/"
	if exact := live + primaryDomain + "/"; fileExists(exact + "fullchain.pem") {
		return exact
	}
	ents, err := os.ReadDir(live)
	if err != nil {
		return ""
	}
	var best string
	var bestMod time.Time
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		fi, err := os.Stat(live + e.Name() + "/fullchain.pem")
		if err != nil {
			continue
		}
		if best == "" || fi.ModTime().After(bestMod) {
			best, bestMod = live+e.Name()+"/", fi.ModTime()
		}
	}
	return best
}

// stageFrontFile writes data to an O_EXCL temp file in dir (os.CreateTemp, so a
// local user cannot pre-plant a symlink for this process to follow) at mode, and
// returns its path for the caller to rename into place.
func stageFrontFile(dir string, data []byte, mode os.FileMode) (string, error) {
	f, err := os.CreateTemp(dir, ".xc_cert_")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(name)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	if err := os.Chmod(name, mode); err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}

// syncFrontCert installs the certbot cert for primaryDomain as
// <bin>/nginx/conf/server.{crt,key} (cert 0644, key 0600). Both are staged to
// owner-only temp files, then the KEY is renamed in before the CERT — the front
// reloads on the cert's mtime, so the matching key is always already present.
// Any read/stage failure leaves the installed pair untouched. Ports
// CertbotCommand::syncFrontCert.
func syncFrontCert(binPath, primaryDomain string) error {
	live := certbotLiveDir(binPath, primaryDomain)
	if live == "" {
		return fmt.Errorf("certbot: no live cert dir for %s", primaryDomain)
	}
	certData, err := os.ReadFile(live + "fullchain.pem")
	if err != nil {
		return err
	}
	keyData, err := os.ReadFile(live + "privkey.pem")
	if err != nil {
		return err
	}
	dir := strings.TrimRight(binPath, "/") + "/nginx/conf/"
	crtTmp, err := stageFrontFile(dir, certData, 0o644)
	if err != nil {
		return err
	}
	keyTmp, err := stageFrontFile(dir, keyData, 0o600)
	if err != nil {
		os.Remove(crtTmp)
		return err
	}
	if err := os.Rename(keyTmp, dir+"server.key"); err != nil {
		os.Remove(crtTmp)
		os.Remove(keyTmp)
		return err
	}
	if err := os.Rename(crtTmp, dir+"server.crt"); err != nil {
		os.Remove(crtTmp)
		return err
	}
	return nil
}

// certbotEvery is how often a PHP-free node renews its cert; cron:certbot runs daily.
var certbotEvery = 24 * time.Hour

// certbotRefresh resolves the node's domains from the replica and, if any, runs
// certbot; on a fresh/renewed cert it syncs it to the front. No domains (IP-only
// node) or a not-due cert is a no-op.
func (a *Agent) certbotRefresh(ctx context.Context) error {
	if a.CertbotWebroot == "" {
		return nil
	}
	domains := certbotDomains(strings.TrimRight(a.ReplicaDir, "/") + "/node.json")
	if len(domains) == 0 {
		return nil
	}
	changed, err := runCertbot(ctx, domains)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	return syncFrontCert(a.CertbotBinPath, domains[0])
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
