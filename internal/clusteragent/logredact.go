package clusteragent

import "regexp"

// logredact ports src/Core/Cluster/Redactor.php (MASK "***") for the node log
// drains: three regexes applied in order over a string, and a row helper that
// redacts every string value. It is NOT internal/redact (a URL-only masker with
// a different mask); MAIN must not be able to tell an agent-spooled row from a
// PHP-spooled one, so the output matches Redactor byte-for-byte.
var (
	// key=value in query strings / form bodies (value ends at & whitespace " ' \)
	reRedactKV = regexp.MustCompile(`(?i)\b(password|passwd|pass|token|username|user)=([^&\s"'\\]*)`)
	// scheme://user:pass@host
	reRedactCreds = regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.\-]*://)[^/\s:@"']+:[^/\s@"']*@`)
	// Xtream paths /live|movie|series|timeshift/<user>/<pass>/
	reRedactPath = regexp.MustCompile(`(?i)/(live|movie|series|timeshift)/[^/\s?"']+/[^/\s?"']+/`)
)

// redactLog ports Redactor::redact.
func redactLog(s string) string {
	if s == "" {
		return s
	}
	s = reRedactKV.ReplaceAllString(s, `${1}=***`)
	s = reRedactCreds.ReplaceAllString(s, `${1}***@`)
	s = reRedactPath.ReplaceAllString(s, `/${1}/***/***/`)
	return s
}

// redactLogRow ports Redactor::redactRow: redact every string value, leave keys
// and non-string values untouched.
func redactLogRow(row map[string]any) map[string]any {
	for k, v := range row {
		if s, ok := v.(string); ok {
			row[k] = redactLog(s)
		}
	}
	return row
}
