package clusteragent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseCodeMatchesThePanel(t *testing.T) {
	// EnrolCodeService::encode(42, "http://[fd00::1]:8080", 0x01×16, 0x02×16) on the panel.
	const code = "AEAA-AABK-CVUH-I5DQ-HIXS-6W3G-MQYD-AOR2-GFOT-UOBQ-HAYA-CAIB-AEAQ-CAIB-AEAQ-CAIB-AEAQ-CAQC-AIBA-EAQC-AIBA-EAQC-AIBA-EAQ"
	c, err := ParseCode(strings.ToLower(strings.ReplaceAll(code, "-", " ")))
	if err != nil {
		t.Fatal(err)
	}
	if c.ServerID != 42 || c.MainURL != "http://[fd00::1]:8080" || !bytes.Equal(c.PanelFP, bytes.Repeat([]byte{1}, 16)) || !bytes.Equal(c.Secret, bytes.Repeat([]byte{2}, 16)) {
		t.Fatalf("decoded %+v", c)
	}
	for _, bad := range []string{"", "not a code!", code[:len(code)-5]} {
		if _, err := ParseCode(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestEnrolByCodeRefusesToReplaceAWorkingIdentity(t *testing.T) {
	_, st := newFake(t)
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	const code = "AEAA-AABK-CVUH-I5DQ-HIXS-6W3G-MQYD-AOR2-GFOT-UOBQ-HAYA-CAIB-AEAQ-CAIB-AEAQ-CAIB-AEAQ-CAQC-AIBA-EAQC-AIBA-EAQC-AIBA-EAQ"
	if err := EnrolByCode(context.Background(), st.path, code, "t", false, nil); !errors.Is(err, ErrAlreadyEnrolled) {
		t.Fatalf("want ErrAlreadyEnrolled, got %v", err)
	}
}

// TestInteropEnrolByCode runs code enrolment against MAIN's real PHP
// (EnrolCodeService + ClusterApi, panel test fake of xcvm_core): the code is
// issued, the node pins the panel key, sends its request, the "admin"
// approves with the SAS the node printed, and the node collects epoch 1 and
// completes enrolment. Opt-in, as TestInteropWithPanel.
func TestInteropEnrolByCode(t *testing.T) {
	panel := os.Getenv("XCVM_PANEL_DIR")
	if panel == "" {
		t.Skip("XCVM_PANEL_DIR not set")
	}
	php, err := exec.LookPath("php")
	if err != nil {
		t.Skip("php not found")
	}
	dir := t.TempDir()
	harness, _ := filepath.Abs("testdata/panel")
	port := freePort(t)
	env := append(os.Environ(), "XCVM_PANEL_DIR="+panel, "XCVM_INTEROP_DB="+filepath.Join(dir, "main.sqlite"), fmt.Sprintf("XCVM_INTEROP_PORT=%d", port))
	runPHP := func(script string, args ...string) string {
		cmd := exec.Command(php, append([]string{filepath.Join(harness, script)}, args...)...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", script, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	code := runPHP("enrol_code.php")

	srv := exec.Command(php, "-S", fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(harness, "router.php"))
	srv.Env = env
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Process.Kill()
	waitPort(t, port)

	old := EnrolPoll
	EnrolPoll = 100 * time.Millisecond
	defer func() { EnrolPoll = old }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	statePath := filepath.Join(dir, "agent.json")
	sasCh := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- EnrolByCode(ctx, statePath, code, "xc_agent/interop", false, func(sas string) { sasCh <- sas })
	}()
	var sas string
	select {
	case sas = <-sasCh:
	case err := <-done:
		t.Fatalf("enrol ended before the request was held: %v", err)
	}
	if got := runPHP("approve.php", "WRON-GSAS-AAAA-AAAA-AAAA-AAAA"); got != "wrong_sas" {
		t.Fatalf("a wrong SAS: %s", got)
	}
	if got := runPHP("approve.php", sas); got != "approved" {
		t.Fatalf("approve: %s", got)
	}
	if err := <-done; err != nil {
		t.Fatalf("enrol: %v", err)
	}

	st, err := LoadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if st.ServerID != 7 || len(st.Epochs) != 1 || len(st.PanelBoxPub) != 32 || st.Enrolled {
		t.Fatalf("state after enrol: server %d, %d epochs, box %d, enrolled %v", st.ServerID, len(st.Epochs), len(st.PanelBoxPub), st.Enrolled)
	}
	c := NewClient(st, "xc_agent/interop")
	a := &Agent{Client: c, Version: "0.0.0-interop", Logf: t.Logf}
	r, err := a.Start(ctx)
	if err != nil || r.State != "active" {
		t.Fatalf("start after code enrolment: %v %+v", err, r)
	}
}
