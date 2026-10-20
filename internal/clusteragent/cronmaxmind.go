package clusteragent

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// cronmaxmind ports cron:maxmind's credentialed path (MaxMindUpdater): when
// maxmind_account_id + maxmind_license_key + maxmind_editions are configured, the
// agent downloads each edition's .mmdb from the MaxMind API. The no-creds GitHub
// release fallback (GeoLiteReleaseUpdater via the GitHubReleases subsystem) is
// NOT ported — a no-creds node keeps PHP's cron:maxmind, and the agent does not
// claim maxmind ownership there (see ownedCrons / the node_crons marker).

// seams (package vars so tests can redirect the dir and the API base).
var (
	maxmindDirPath = filepath.Join(mainHome, "bin", "maxmind")
	maxmindBaseURL = "https://download.maxmind.com/geoip/databases"
)

// maxmindCreds reports whether the credentialed MaxMind path is configured.
func (a *Agent) maxmindCreds() bool {
	s := a.replicaSettings()
	return strings.TrimSpace(logStr(s["maxmind_account_id"])) != "" &&
		strings.TrimSpace(a.secret("maxmind_license_key")) != "" &&
		len(parseEditions(s["maxmind_editions"])) > 0
}

// ownedCrons is the set of node crons the agent owns this tick — the always-on
// drains + tmp, plus maxmind only when its creds are configured. The node_crons
// marker carries this list so the node's crontab drops exactly these.
func (a *Agent) ownedCrons() []string {
	c := []string{"streams_logs", "lines_logs", "activity", "errors", "tmp"}
	if a.maxmindCreds() {
		c = append(c, "maxmind")
	}
	return c
}

func parseEditions(v any) []string {
	s, _ := v.(string)
	if s == "" {
		return nil
	}
	var out []string
	if json.Unmarshal([]byte(s), &out) != nil {
		return nil
	}
	return out
}

// updateMaxmind ports MaxMindCronJob's creds path with a self-contained cadence:
// a ~6h throttle, then a download only when the primary DB is missing or older
// than ~6 days (weekly + self-heal). No-creds → no-op (PHP keeps it).
func (a *Agent) updateMaxmind() error {
	if !a.maxmindCreds() {
		return nil
	}
	stamp := filepath.Join(maxmindDirPath, ".agent_checked")
	if fi, err := os.Stat(stamp); err == nil && time.Since(fi.ModTime()) < 6*time.Hour {
		return nil // throttle: checked recently
	}
	primary := filepath.Join(maxmindDirPath, "GeoLite2-Country.mmdb")
	fi, err := os.Stat(primary)
	due := os.IsNotExist(err) || (err == nil && time.Since(fi.ModTime()) > 6*24*time.Hour)
	_ = os.MkdirAll(maxmindDirPath, 0o750)
	_ = os.WriteFile(stamp, []byte("1"), 0o640) // throttle marker
	if !due {
		return nil
	}
	rel, ok := acquireCronLock(cronLockPath(a.cronsDir(), `XcVm\Cli\CronJobs\MaxMindCronJob`))
	if !ok {
		return nil
	}
	defer rel()
	s := a.replicaSettings()
	acct := strings.TrimSpace(logStr(s["maxmind_account_id"]))
	lic := strings.TrimSpace(a.secret("maxmind_license_key"))
	for _, ed := range parseEditions(s["maxmind_editions"]) {
		if err := a.downloadMaxmindEdition(ed, acct, lic); err != nil && a.Logf != nil {
			a.Logf("cluster: maxmind %s: %v", ed, err)
		}
	}
	return nil
}

// downloadMaxmindEdition ports MaxMindUpdater::downloadEdition: a conditional GET
// (If-Modified-Since from the dest mtime; 304 skips) with HTTP Basic auth, then
// the .mmdb extracted from the tar.gz and installed atomically + version.json.
func (a *Agent) downloadMaxmindEdition(ed, acct, lic string) error {
	url := fmt.Sprintf("%s/%s/download?suffix=tar.gz", maxmindBaseURL, ed)
	dest := filepath.Join(maxmindDirPath, ed+".mmdb")
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(acct, lic)
	req.Header.Set("User-Agent", "XC_VM-MaxMind-Updater/1.0")
	if fi, err := os.Stat(dest); err == nil {
		req.Header.Set("If-Modified-Since", fi.ModTime().UTC().Format(http.TimeFormat))
	}
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	tmp := dest + ".new"
	if err := extractMmdbToFile(resp.Body, tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	a.maxmindVersion(ed)
	return nil
}

// extractMmdbToFile streams the first *.mmdb entry out of a tar.gz to destTmp.
func extractMmdbToFile(r io.Reader, destTmp string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return errors.New("no .mmdb in archive")
		}
		if err != nil {
			return err
		}
		if strings.HasSuffix(h.Name, ".mmdb") {
			f, err := os.OpenFile(destTmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
			if err != nil {
				return err
			}
			_, err = io.Copy(f, tr)
			cerr := f.Close()
			if err != nil {
				return err
			}
			return cerr
		}
	}
}

// maxmindVersion ports MaxMindUpdater::updateVersionFile (date d.m.y per edition).
func (a *Agent) maxmindVersion(edition string) {
	path := filepath.Join(maxmindDirPath, "version.json")
	data := map[string]any{}
	if b, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(b, &data)
	}
	today := time.Now().Format("02.01.06")
	if strings.Contains(edition, "GeoLite2") {
		data["geolite2_version"] = today
	} else {
		if edition == "GeoIP2-ISP" {
			data["geoisp_version"] = today
		}
		key := strings.ToLower(strings.NewReplacer("GeoIP2-", "geo", "-", "_").Replace(edition)) + "_version"
		data[key] = today
	}
	b, _ := json.MarshalIndent(data, "", "    ")
	_ = os.WriteFile(path, b, 0o640)
}
