// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Command xc_agent is an LB node's end of the XC_VM cluster API (MAIN ↔ LB
// plan, Phase 2). It holds the node's identity and token epochs, finishes
// enrolment, says hello, heartbeats and rotates its token. In this phase MAIN
// only records what it hears (shadow mode); nothing depends on it yet.
//
// The node's state file (default /home/xc_vm/config/cluster/agent.json, 0600)
// is written by the panel's install flow over SSH: node uuid, server id, the
// node's Ed25519 seed, the pinned panel key, MAIN's URLs and epoch 1.
//
//	xc_agent [-state path] [-interval 2s]   run the control loop
//	xc_agent health [-state path]           fetch and verify MAIN's signed health
//	xc_agent version
//
// Install flow (run by the panel over SSH, see internal/clusteragent/install.go):
//
//	xc_agent keygen  -state path -uuid <uuid>          keys stay here; prints the public halves (JSON)
//	xc_agent probe   -panel-pub <hex> -url <u> [...]   MAIN's health must verify before any token exists
//	xc_agent install -state path < install.json        checks and saves epoch 1
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/clusteragent"
)

var version = "dev"

func main() {
	args := os.Args[1:]
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet("xc_agent "+cmd, flag.ExitOnError)
	statePath := fs.String("state", "/home/xc_vm/config/cluster/agent.json", "node state file (written by the panel's install flow)")
	interval := fs.Duration("interval", 2*time.Second, "heartbeat interval (1s–3s)")
	uuid := fs.String("uuid", "", "keygen: the node uuid MAIN assigned")
	panelPub := fs.String("panel-pub", "", "probe: the panel signing key (hex), as received over SSH")
	var urls multiFlag
	fs.Var(&urls, "url", "probe: a MAIN cluster URL (repeatable)")
	fs.Parse(args)

	switch cmd {
	case "version":
		fmt.Println(version)
		return
	case "keygen":
		res, err := clusteragent.Keygen(*statePath, *uuid)
		if err != nil {
			log.Fatalf("xc_agent keygen: %v", err)
		}
		json.NewEncoder(os.Stdout).Encode(res)
		return
	case "probe":
		pub, err := hex.DecodeString(*panelPub)
		if err != nil {
			log.Fatalf("xc_agent probe: panel key: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		u, err := clusteragent.Probe(ctx, pub, urls)
		if err != nil {
			log.Fatalf("xc_agent probe: %v", err)
		}
		fmt.Println("OK " + u)
		return
	case "install":
		var d clusteragent.InstallData
		if err := json.NewDecoder(io.LimitReader(os.Stdin, 1<<20)).Decode(&d); err != nil {
			log.Fatalf("xc_agent install: %v", err)
		}
		if err := clusteragent.Install(*statePath, d); err != nil {
			log.Fatalf("xc_agent install: %v", err)
		}
		fmt.Println("OK")
		return
	case "run", "health":
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (run, health, keygen, probe, install, version)\n", cmd)
		os.Exit(2)
	}

	st, err := clusteragent.LoadState(*statePath)
	if err != nil {
		log.Fatalf("xc_agent: %v", err)
	}
	client := clusteragent.NewClient(st, "xc_agent/"+version)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cmd == "health" {
		var lastErr error
		for _, u := range st.MainURLs {
			doc, err := client.Health(ctx, u)
			if err != nil {
				lastErr = err
				fmt.Printf("%s: %v\n", u, err)
				continue
			}
			b, _ := json.Marshal(doc)
			fmt.Printf("%s: OK %s\n", u, b)
			return
		}
		log.Fatalf("xc_agent: no MAIN URL answered with a valid health document: %v", lastErr)
	}

	*interval = max(time.Second, min(3*time.Second, *interval))
	a := &clusteragent.Agent{Client: client, Version: version, Interval: *interval, Telemetry: telemetry}
	log.Printf("xc_agent %s: node %s, %d MAIN URL(s)", version, st.NodeUUID, len(st.MainURLs))
	err = a.Run(ctx)
	switch {
	case err == nil, errors.Is(err, context.Canceled):
		log.Printf("xc_agent: stopped")
	case errors.Is(err, clusteragent.ErrStop):
		// Exit 3: the supervisor must not restart a node MAIN has stopped.
		log.Printf("xc_agent: %v", err)
		os.Exit(3)
	default:
		log.Fatalf("xc_agent: %v", err)
	}
}

// telemetry is the shadow-mode heartbeat payload: load and memory, as the
// legacy watchdog reports them, so MAIN can compare the two.
func telemetry() map[string]any {
	out := map[string]any{}
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		if f := strings.Fields(string(b)); len(f) >= 3 {
			out["load"] = f[:3]
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if f := strings.Fields(line); len(f) >= 2 && (f[0] == "MemTotal:" || f[0] == "MemAvailable:") {
				out[strings.TrimSuffix(strings.ToLower(f[0]), ":")+"_kb"] = f[1]
			}
		}
	}
	return out
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }
