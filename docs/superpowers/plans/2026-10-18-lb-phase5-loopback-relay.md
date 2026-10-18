# LB Phase 5 — Loopback Relay on a Serve-Only LB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a loopback stream (`streams_servers.parent_id > 0` — relay from a parent node) run on a serve-only LB via the Go agent + puller, instead of PHP's `startLoopback`/`LoopbackCommand`.

**Architecture:** MAIN classifies a loopback stream as a typed `"loopback"` runtime directive for a `serve_only` target (gated exactly like the transcode Spec). The Go agent, on seeing kind `"loopback"`, composes the node-local relay URL `http://127.0.0.1:31290/relay/<k>/<id>.ts` from its own relay key `k` + the stream id and registers it as the puller source via the existing `registerStream` PUT. The puller pulls it as a plain `video/mp2t` source; the agent's relay proxy signs the upstream connect to the parent with the MAIN-minted relay ticket already in the record. Delivery shifts from PHP's PUSH (IngestFeeder → daemon ingest) to the Go PULL — same bytes. Because that moves the feed to Go, PHP's loopback launcher is gated off for a `serve_only` node so the daemon is never double-fed.

**Tech Stack:** PHP 8.1 (XC_VM: `src/Core/Cluster`, `src/Domain/Stream`), Go (XC_VM_Fanout: `internal/clusteragent`).

**Spec:** `docs/superpowers/specs/2026-10-17-lb-phase4-5-lifecycle-design.md` (Phase 5 — loopback relay; D2 — the LB runs only the loopback relay among the special modes).

## Global Constraints

- **Cross-repo, coordinated (atomic):** XC_VM branch off `2.8.0`; XC_VM_Fanout branch off `feat/lb-transcode-serve` (has merged Phase 4a+4b). The PHP "emit loopback kind" + "gate PHP loopback off for serve_only" and the Go "register the relay URL" must land together — a serve-only LB with one side only either double-feeds the daemon or serves nothing.
- **Loopback is per-target, serve_only-gated.** A non-serve-only node keeps running loopback via PHP (directive stays `"other"`; the PHP gate does not fire). Only a `serve_only` target gets kind `"loopback"` and the Go path.
- **No new transport/auth/scheme.** The puller already pulls `video/mp2t` direct; the relay proxy (`internal/clusteragent/relayproxy.go`) already signs upstream with the MAIN-minted relay ticket from `record.data.tickets`; `parent_id` is already in `record.data.server`. Do NOT add relay auth, a URL scheme, or parent failover.
- **MAIN never composes the relay URL.** It carries the node-local key `k`, so only the agent can build it. MAIN emits kind + `parent_id` only.
- **No Claude attribution** in commits or PRs (see the repo's convention / user instruction).

## Review Focus

- **Loopback on a non-serve-only node** → directive stays `"other"`, PHP gate does NOT fire, PHP still runs it (regression guard). Test in Task 1 + Task 2.
- **Agent relay key `k` missing/unreadable** → the agent must fail closed (not register a broken URL / not crash). Test in Task 3.
- **`direct_proxy == 1` AND `parent_id > 0`** → proxy classification wins (loopback never displaces a proxy). Test in Task 1.
- **`encrypt_hls` on for a loopback stream** → the HLS `_.key` is provisioned at daemon registration (the generic path, `StreamProcess.php:3037-3040`), not only by PHP `startLoopback`. Verify in Task 4.
- **Flip loopback↔proxy / loopback↔other on re-assignment** → the agent re-registers the correct source or unregisters, never leaves a stale relay registration. Test in Task 3.

---

### Task 1: MAIN emits a typed `"loopback"` runtime directive for a serve-only target

**Files:**
- Modify: `src/Core/Cluster/StreamRuntimeDirective.php` (add a loopback representation)
- Modify: `src/Domain/Cluster/StreamReplica.php` (serve_only-gated composition — emit loopback for a `parent_id > 0` target)
- Test: `tests/Unit/StreamRuntimeDirectiveTest.php`, and the serve_only-gated-composition test (`tests/Unit/StreamReplicaRuntimeUpgradeTest.php`)

**Interfaces:**
- Consumes: `StreamRuntimeDirective::build()` → `array{kind,source,spec?}` (kind `proxy`/`other`/`transcode`); the serve_only-gated upgrade in `StreamReplica` (the `runtimeUpgrade` closure that already turns `other`→`transcode` per target, reading the held-streams snapshot which includes `streams_servers.parent_id`).
- Produces: a directive `array{kind:'loopback', source:null, parent_id:int}` for a `serve_only` target whose `parent_id > 0` and `direct_proxy != 1`. The Go `runtimeDirective` (Task 3) consumes `kind` and `parent_id`.

- [ ] **Step 1: Write the failing test** in `StreamRuntimeDirectiveTest.php`: a helper that classifies then upgrades a `parent_id>0`, non-proxy stream for a serve-only target yields `['kind'=>'loopback','source'=>null,'parent_id'=>N]`; a `direct_proxy=1` stream stays `['kind'=>'proxy',...]` even with `parent_id>0`; a `parent_id>0` stream for a NON-serve-only target stays `['kind'=>'other',...]`.
- [ ] **Step 2: Run it; verify it fails** (`make test ARGS="--filter StreamRuntimeDirective"` with the :3307 DB env) — no `loopback` kind yet.
- [ ] **Step 3: Implement.** Add `StreamRuntimeDirective::loopback(int $rParentID): array` returning `['kind'=>'loopback','source'=>null,'parent_id'=>$rParentID]`. In `StreamReplica`'s serve_only-gated per-target composition, when the target's `parent_id > 0` and the stream is not `direct_proxy`, set the directive to `loopback($parentID)` instead of leaving it `other` (place it beside the existing transcode-Spec upgrade; it runs only on the serve_only-gated path, so a non-serve-only target is untouched).
- [ ] **Step 4: Run it; verify it passes.**
- [ ] **Step 5: Run the serve_only-gated-composition test + `make gates` + `make phpstan`.**
- [ ] **Step 6: Commit** (`feat(cluster): classify a loopback stream as a serve-only runtime directive`).

### Task 2: Gate PHP's loopback launcher off for a serve-only node

**Files:**
- Modify: `src/Domain/Stream/StreamProcess.php` (`startLoopback`, ~`:2708` — the `parent_id != 0` branch that `shell_exec`s `console.php loopback`)
- Test: `tests/Unit/` (a StreamProcess/loopback test; model on existing StreamProcess tests)

**Interfaces:**
- Consumes: the node's `serve_only` flag — read it the way the rest of Part A does (the `config/cluster/serve_only` marker file, or `$rServers[SERVER_ID]['serve_only']`; use whichever Part A already established as the node-local serve-only signal — grep `serve_only` under `src/`).
- Produces: `startLoopback` returns without spawning `LoopbackCommand` (and without writing the PID/`current_source`) when the node is `serve_only`; the Go agent feeds the daemon instead.

- [ ] **Step 1: Write the failing test** — `startLoopback` on a `serve_only` node does NOT `shell_exec` the loopback worker (assert no spawn / early-return sentinel), while on a normal node it still does.
- [ ] **Step 2: Run it; verify it fails.**
- [ ] **Step 3: Implement** the `serve_only` early-return at the top of `startLoopback`'s `parent_id != 0` branch (before `writeStreamKeyIv`/`shell_exec`), returning the same shape it returns today minus the spawn (or a clear "handled by the node agent" no-op). Do not change the non-serve_only path.
- [ ] **Step 4: Run it; verify it passes.**
- [ ] **Step 5: `make gates` + `make phpstan`.**
- [ ] **Step 6: Commit** (`feat(cluster): a serve-only node leaves loopback to the Go agent`).

### Task 3: Go agent registers the loopback relay URL

**Files:**
- Modify: `internal/clusteragent/streamregister.go` (the `runtimeDirective` struct; `seedFanoutRecord` routing; a loopback-body composer)
- Test: `internal/clusteragent/streamregister_test.go`

**Interfaces:**
- Consumes: `runtimeDirective{Kind,Source,Spec}` (add `ParentID int64 \`json:"parent_id"\``); `a.registerStream(ctx,id,streamRegisterBody)`; the relay key `k` from the relay-key store in `internal/clusteragent/relayproxy.go` (`RelayKeyStore`/`.relay.key`); the relay proxy's local address `127.0.0.1:31290` (reuse the constant/field relayproxy.go already defines — do not hardcode a second copy).
- Produces: on kind `"loopback"`, a `streamRegisterBody{URLs:["http://127.0.0.1:31290/relay/<k>/<id>.ts"]}` PUT to `/streams/<id>`; a missing/empty `k` registers nothing (fail closed).

- [ ] **Step 1: Write the failing tests** in `streamregister_test.go` (model on `TestSeedFanoutRecordProxyRegisters`, using the `fanoutStub`): a kind `"loopback"` record → `PUT /streams/<id>` whose body URL is `http://127.0.0.1:31290/relay/<k>/<id>.ts` with the agent's relay key; a kind `"loopback"` with no relay key available → no call (fail closed); a record that flips `loopback`→`other` → `DELETE /streams/<id>` (unregister), and `loopback`→`proxy` → re-register with the proxy source.
- [ ] **Step 2: Run them; verify they fail** (`GOWORK=off go test ./internal/clusteragent/ -run 'Loopback'`).
- [ ] **Step 3: Implement.** Add `ParentID` to `runtimeDirective`; add `(d runtimeDirective) loopbackBody(k, relayAddr string) (streamRegisterBody, bool)` returning the relay-URL body when `Kind=="loopback"` and `k != ""`; in `seedFanoutRecord`, add a `loopback` branch (before the final `unseedFanoutRecord`) that reads `k` from the relay-key store and `registerStream`s the body, else unseeds. Keep the proxy and transcode branches as-is (so a flip routes correctly).
- [ ] **Step 4: Run them; verify they pass.**
- [ ] **Step 5: Run the full clusteragent suite** (`GOWORK=off go test ./internal/clusteragent/`), `gofmt -l .` (must be empty), `go vet`.
- [ ] **Step 6: Commit** (`feat(agent): register a loopback stream's node-local relay URL with the puller`).

### Task 4: Encrypted-HLS key verification + end-to-end notes

**Files:**
- Verify only (no production change expected): `src/Domain/Stream/StreamProcess.php:3037-3040` (the generic daemon-registration path writes `_.key` when `encrypt_hls`).

- [ ] **Step 1:** Confirm, by reading the registration path, that a serve-only loopback stream with `encrypt_hls` on gets its `_.key`/`_.iv` provisioned at daemon registration (the generic launcher setup), not only by PHP `startLoopback`. If it does, record the finding in the ledger (no code). If it does NOT (the key was written only by `startLoopback`, now gated off in Task 2), add a step to write it on the serve-only path and a test — otherwise encrypted-HLS loopback would serve no key.
- [ ] **Step 2:** Document the E2E validation steps in the ledger/PR (hardware-gated, needs a real serve-only LB + a parent node): assign a loopback stream (`parent_id>0`) to the serve-only LB; confirm MAIN emits kind `"loopback"`, the agent registers the relay URL, the puller pulls from the parent through the relay proxy, and a viewer is served — with PHP `LoopbackCommand` NOT running on the LB.

---

## Notes for the executor

- **Two PRs, coordinated:** the XC_VM PR (Tasks 1, 2, 4-verify) and the XC_VM_Fanout PR (Task 3) must be merged together — a serve-only LB with only one side either double-feeds (PHP gate missing) or serves nothing (Go register missing). State this in both PR descriptions.
- **Live E2E is hardware-gated** (shares Part A's ≥2-CPU MAIN + a second node as the parent); unit tests fully cover the classification, gating, and relay-URL composition.
- This is the last lifecycle phase before the php-fpm-removal step (which also retires the serve-only LB crons folded from 4c).
