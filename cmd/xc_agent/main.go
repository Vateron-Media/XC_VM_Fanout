// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Command xc_agent is an LB node's end of the XC_VM cluster API (MAIN ↔ LB
// plan). It holds the node's identity and token epochs, finishes enrolment,
// says hello, heartbeats and rotates its token. Every heartbeat carries the
// host sampled each second (Phase 3); with the node's TELEMETRY flow on, MAIN
// takes the server's stats from it. The mode and flows MAIN sends are written
// to flows.json beside the state for the node's PHP.
//
// With CONNECTIONS on, the agent holds the node's viewers (registry.snap,
// rebuilt against the fanout and the PHP workers after a restart), admits new
// ones (the adm claim, MAIN's conn_admit, or MAIN's offline policy), and
// sends HLS touches on the P2 lane when MAIN takes them. Heartbeats go at
// most 3 s apart; under https_required the agent follows the signed challenge
// over plain HTTP while HTTPS fails. Hello and heartbeat say which policy the
// agent dials (policy_ver), and it keeps its last 3 known-good MAIN URL sets
// to fall back on. P0 events have a connection of their own, and a busy
// refusal of an ingest lane is waited out, not logged.
//
// The node replica (replica/ beside the state) holds the blocklist, the
// sections MAIN sends whole (settings, servers, node, crontab, cluster,
// secrets, bouquets and categories; secrets 0600 and never logged) and,
// while STREAMS is on, the R2 streams section (replica/streams/, one record
// per stream held, kept by deltas, full passes and a resync every 5
// minutes); `console.php cluster:apply` runs after a change, and
// config.changed fetches at once. Heartbeats relay the node's
// audit.json (at most 16 KiB) as `audit`.
//
// Artefacts MAIN grants (an off-air video's artefact.fetch, a node.root
// carrying a module archive or the pinned agent) download into artefacts/
// beside the state, in chunks over the bulk lane, beside the command loop;
// the agent checks their size and SHA-256 before it hands the command to
// `console.php cluster:exec`, and says `artefact` at hello only while
// `cluster:exec --types` lists artefact.fetch.
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
//
// Break-glass enrolment, when MAIN cannot reach the node over SSH:
//
//	xc_agent enrol [-state path] [-force] <code>   code from `console.php cluster:enrol-code`;
//	                                               prints the SAS the admin approves on MAIN
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
	"path/filepath"
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
	force := fs.Bool("force", false, "enrol: replace an identity that already holds tokens")
	phpBin := fs.String("php", "/home/xc_vm/bin/php/bin/php", "run: the PHP that runs MAIN's commands")
	console := fs.String("console", "/home/xc_vm/console.php", "run: the panel console (cluster:exec)")
	fanoutCtl := fs.String("fanout-ctl", "/home/xc_vm/bin/xc_fanout/sockets/control.sock", "run: xc_fanout's control socket, whose /events feed is followed while STREAMS is on; empty = off")
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
	case "enrol":
		// Break-glass enrolment with a code from `console.php cluster:enrol-code`.
		if fs.NArg() != 1 {
			log.Fatalf("usage: xc_agent enrol [-state path] [-force] <code>")
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		err := clusteragent.EnrolByCode(ctx, *statePath, fs.Arg(0), "xc_agent/"+version, *force, func(sas string) {
			fmt.Printf("Request sent. On MAIN, approve it with this SAS:\n\n  %s\n\n(console.php cluster:enrol-approve <serverID> %s). Waiting for the decision...\n", sas, sas)
		})
		if err != nil {
			log.Fatalf("xc_agent enrol: %v", err)
		}
		// A node MAIN had stopped may run again: the supervisor starts it.
		if exe, err := os.Executable(); err == nil {
			os.Remove(filepath.Join(filepath.Dir(exe), "stopped"))
		}
		fmt.Println("OK")
		return
	case "run", "health":
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (run, health, keygen, probe, install, enrol, version)\n", cmd)
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
	sampler := clusteragent.NewSampler(*statePath)
	stopSampler := make(chan struct{})
	defer close(stopSampler)
	go sampler.Run(stopSampler)
	a := &clusteragent.Agent{Client: client, Version: version, Interval: *interval, Telemetry: sampler.Latest, FlowsFile: filepath.Join(filepath.Dir(*statePath), "flows.json"),
		Exec: clusteragent.ExecViaPHP(*phpBin, *console, time.Minute), SpoolDir: filepath.Join(filepath.Dir(*statePath), "spool"),
		SocketPath: filepath.Join(filepath.Dir(*statePath), "agent.sock"), FanoutCtl: *fanoutCtl,
		ReplicaDir: filepath.Join(filepath.Dir(*statePath), "replica"), Apply: clusteragent.ApplyViaPHP(*phpBin, *console, time.Minute),
		ArtefactDir: filepath.Join(filepath.Dir(*statePath), "artefacts"), Types: clusteragent.TypesViaPHP(*phpBin, *console, time.Minute)}
	log.Printf("xc_agent %s: node %s, %d MAIN URL(s)", version, st.NodeUUID, len(st.MainURLs))
	err = a.Run(ctx)
	switch {
	case err == nil, errors.Is(err, context.Canceled):
		log.Printf("xc_agent: stopped")
	case errors.Is(err, clusteragent.ErrStop):
		// Exit 3: the supervisor must not restart a node MAIN has stopped
		// (revoked, unknown, enrolment not completed; expiry re-keys instead).
		log.Printf("xc_agent: %v", err)
		a.Unpublish()
		os.Exit(3)
	default:
		log.Fatalf("xc_agent: %v", err)
	}
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }
