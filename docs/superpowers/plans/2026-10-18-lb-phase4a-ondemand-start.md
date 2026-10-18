# LB Phase 4a — in-process on-demand stream start Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On a serve-only LB (no PHP behind the gateway), a not-running-but-registered stream is started in-process and served after a bounded wait, instead of deferring to a PHP backend that does not exist.

**Architecture:** Add `EnsureRunning(id) bool` to the gateway `Node` interface, implemented on `*server.Manager` as `touch()` (the registered-puller start — idempotent, single-flighted; a no-op for an ingest-fed transcode or an already-running stream) followed by a bounded poll of readiness (`Stream.onAir()`). The gateway serve path already has a serve-only "warmup" branch that cold-starts via `touch()` and relies on a 404-retry; Phase 4a replaces that reliance with an explicit `EnsureRunning` call — serve once ready, return a short `503 Retry-After` on timeout. `EnsureRunning` never drives the `/monitor/` → `Supervisor.Supervise` path (that needs a panel-composed `Spec`; the daemon never composes command lines). Shadow mode and PHP-backed nodes are untouched.

**Tech Stack:** Go (1.23); XC_VM_Fanout; `internal/gateway`, `internal/server`.

**Spec:** `docs/superpowers/specs/2026-10-17-lb-phase4-5-lifecycle-design.md` (Phase 4a), copied from XC_VM commit `e0f3f3a0f`.

## Global Constraints

- **Fork from `feat/lb-transcode-serve` (`fd65395`), not `master`.** `Policy.ServeOnly`, `Node.Serveable`, and the serve-only warmup path exist only on that branch; `master` lacks the #84/#85/#86 serve/transcode stack.
- **The daemon never composes encoder command lines** (security boundary, `internal/server/monitor.go:30-31`). `EnsureRunning` must use the registered-puller start (`Stream.touch()`/`startLocked()`), never `Supervisor.Supervise`.
- **Shadow mode must never start a stream.** `EnsureRunning` is called only from the serve path, only under `Policy.ServeOnly`; a shadow node short-circuits to `PHP` before the serve path (`internal/gateway/server.go:291-294`) and never has `ServeOnly` set. Do not call `EnsureRunning` from the judge/shadow path.
- **Idempotent / single-flight:** `startLocked()` already returns early for `removed || running || cfg==nil` under `s.mu` (`internal/server/server.go:652`). Reuse it via `touch()`; do not reimplement locking or restart a running stream.

## Review Focus

- **Concurrent viewers of the same not-running id** → exactly one start, no double-launch (relies on `touch()`/`startLocked()` single-flighting under `s.mu`; the test drives two concurrent `EnsureRunning` and asserts the puller started once).
- **Ingest-fed transcode (`cfg==nil`)** → `touch()` is a no-op (the external encoder feeds it); `EnsureRunning` must still bounded-wait for readiness and return true once data flows, not return false immediately. (Task 1 test `…IngestFedWaitsForData`.)
- **Unknown / unregistered id** → `EnsureRunning` returns false promptly without creating a stream or blocking the full timeout. (Task 1 test `…UnknownIdReturnsFalseFast`.)
- **Slow / never-ready start** → returns false within the bound; the serve path returns `503 Retry-After`, never blocks the worker indefinitely or 500s. (Task 1 `…TimesOut`; Task 3 `…TimeoutReturns503`.)
- **A non-serve-only (PHP-backed) node** → still defers (`Action==PHP`) for a not-running stream; `EnsureRunning` is not called. (Task 3 `…PhpBackendStillDefers`.)

---

### Task 1: `EnsureRunning` on the Manager

**Files:**
- Modify: `internal/server/server.go` (add the method + two tunables near the other stream timeouts)
- Test: `internal/server/ensure_running_test.go` (new; model on `internal/server/serve_live_idle_test.go` and `registered_test.go`)

**Interfaces:**
- Consumes: `Manager.Get(id) *Stream`, `Stream.touch()` (`server.go:772`), `Stream.onAir() bool` (`server.go:880`).
- Produces: `func (m *Manager) EnsureRunning(id string) bool` — looks up the stream; if absent, returns false; else `touch()` (starts a registered puller, no-op if already running or ingest-fed) and polls `onAir()` up to `ensureRunningTimeout` (sleeping `ensureRunningPoll` between checks), returning true as soon as it is on air, else a final `onAir()` at timeout. Package vars `ensureRunningTimeout` (default `3 * time.Second`) and `ensureRunningPoll` (default `75 * time.Millisecond`) — **vars, not consts**, so tests can lower them.

- [ ] **Step 1: Write the failing tests** in `internal/server/ensure_running_test.go`:

```go
// Lower the tunables so the timeout case is fast; restore after.
func withFastEnsure(t *testing.T) { old1, old2 := ensureRunningTimeout, ensureRunningPoll; ensureRunningTimeout, ensureRunningPoll = 400*time.Millisecond, 10*time.Millisecond; t.Cleanup(func(){ ensureRunningTimeout, ensureRunningPoll = old1, old2 }) }

func TestEnsureRunningStartsRegisteredPuller(t *testing.T) { /* register a stream whose puller produces data quickly; EnsureRunning(id) == true; stream onAir */ }
func TestEnsureRunningUnknownIdReturnsFalseFast(t *testing.T) { /* no such id; EnsureRunning returns false in well under ensureRunningTimeout */ }
func TestEnsureRunningIdempotentForRunning(t *testing.T) { /* an already-onAir stream; EnsureRunning == true ~immediately and the puller is not restarted */ }
func TestEnsureRunningTimesOut(t *testing.T) { /* a registered stream whose source never produces; EnsureRunning == false after ~ensureRunningTimeout */ }
func TestEnsureRunningIngestFedWaitsForData(t *testing.T) { /* cfg==nil ingest stream; touch() no-ops; EnsureRunning == true once Publish feeds data, false if none within the bound */ }
```
Use the existing harness for registering streams and feeding/withholding data (see `registered_test.go`, `serve_live_idle_test.go`, `serve_live_stall_test.go`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /root/work/XC_VM_Fanout-2.8.0 && GOWORK=off go test ./internal/server/ -run TestEnsureRunning -v`
Expected: FAIL — `m.EnsureRunning undefined`.

- [ ] **Step 3: Implement `func (m *Manager) EnsureRunning(id string) bool` in `internal/server/server.go`**

Look up with `m.Get(id)`; `nil` → return false. Else `st.touch()`, then poll `st.onAir()` on a `time.NewTimer(ensureRunningTimeout)` / `time.Ticker(ensureRunningPoll)` loop, returning true on first `onAir()`; on timer fire return `st.onAir()`. Add the two package vars beside the existing stream timeouts.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `GOWORK=off go test ./internal/server/ -run TestEnsureRunning -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/server/server.go internal/server/ensure_running_test.go
git commit -m "feat(server): Manager.EnsureRunning — in-process on-demand puller start + bounded readiness wait"
```

---

### Task 2: Expose `EnsureRunning` on the gateway `Node` interface + test fake

**Files:**
- Modify: `internal/gateway/server.go:60-73` (the `Node` interface)
- Modify: `internal/gateway/agent_test.go:110-143` (`fakeFiles`)

**Interfaces:**
- Consumes: `Manager.EnsureRunning` (Task 1) — `*server.Manager` already satisfies the new method, so it keeps satisfying `Node`.
- Produces: `Node.EnsureRunning(id string) bool`; `fakeFiles.EnsureRunning` driven by a `ensureRunning func(string) bool` field (default: return `f.Serveable(id)` so existing tests that register a stream behave as "startable → ready").

- [ ] **Step 1: Add `EnsureRunning(id string) bool` to the `Node` interface** (`internal/gateway/server.go:60-73`), documented as "start the stream in-process if registered and wait (bounded) for it to be ready; serve-only path only."

- [ ] **Step 2: Add the method to `fakeFiles`** (`agent_test.go`): a `ensureRunning func(string) bool` field; the method calls it, defaulting (when nil) to `f.Serveable(id)`.

- [ ] **Step 3: Verify the gateway package compiles and its existing tests pass**

Run: `GOWORK=off go build ./... && GOWORK=off go test ./internal/gateway/ 2>&1 | tail -5`
Expected: build OK; existing gateway tests still PASS (the new interface method is satisfied by `*Manager` and `fakeFiles`).

- [ ] **Step 4: Commit**

```bash
git add internal/gateway/server.go internal/gateway/agent_test.go
git commit -m "feat(gateway): add EnsureRunning to the Node interface (+ test fake)"
```

---

### Task 3: Serve-only warmup calls `EnsureRunning` (serve on ready, 503 on timeout)

**Files:**
- Modify: `internal/gateway/server.go` — `(*Server).serveLive` warmup handling (~`:512-521`) and the TS warmup arm it dispatches; add a small `retryLater(w http.ResponseWriter)` helper (503 + `Retry-After: 1`).
- Test: `internal/gateway/serve_live_warmup_test.go` (new; model on `TestJudgeLiveServeOnlyServesIdleRegistered` `internal/gateway/live_test.go:962-1027` and the `*Server` serve tests via `NewServer`/`serveReq`/`writeServePolicy`).

**Interfaces:**
- Consumes: `Node.EnsureRunning` (Task 2); `Policy.ServeOnly` (`policy.go:46`); `Refresh.Warmup` (`live.go:67`) — set by `JudgeLive` only when `p.ServeOnly && env.Serveable` (`live.go:299`, `:324`), so a warmup already implies serve-only.

- [ ] **Step 1: Write the failing gateway tests** in `internal/gateway/serve_live_warmup_test.go`:

```go
// ServeOnly policy + a fake node whose EnsureRunning returns true for the registered-but-idle stream.
func TestServeOnlyWarmupStartsAndServes(t *testing.T) { /* GET a not-running stream → Action resolves to Serve (not PHP); fake.ensureRunning was called with the id */ }
// EnsureRunning returns false (start timed out).
func TestServeOnlyWarmupTimeoutReturns503(t *testing.T) { /* response is 503 with Retry-After set; NOT an X-Accel-Redirect to a PHP location; no hang */ }
// Not serve-only: the warmup branch is never taken.
func TestPhpBackendNotRunningStillDefers(t *testing.T) { /* ServeOnly=false + not-running stream → X-Accel-Redirect @gw_live_php; EnsureRunning NOT called */ }
// Shadow never reaches serve.
func TestShadowNeverCallsEnsureRunning(t *testing.T) { /* mode=shadow; a /stream request → defer; fake.ensureRunning never called */ }
```
Assert PHP-defer via the `X-Accel-Redirect` header (`server.go:344-346`, `phpLocation`), serve via a 2xx from `serveLive`, and the fake records whether `EnsureRunning` was called.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `GOWORK=off go test ./internal/gateway/ -run 'ServeOnlyWarmup|PhpBackendNotRunning|ShadowNeverCalls' -v`
Expected: FAIL (timeout case currently 404-retries / does not 503; the serve case relies on the old re-dispatch).

- [ ] **Step 3: Wire `EnsureRunning` into the warmup branch of `(*Server).serveLive`**

Where `ref.Warmup` is handled today (the HLS re-dispatch at `~server.go:512-521` and the TS warmup arm), call `s.node.EnsureRunning(r.Stream)` first: on `true`, proceed to serve as today (HLS: re-dispatch to the segments handler, now populated; TS: serve the feed); on `false`, `retryLater(w)` and return. Add `retryLater`: `w.Header().Set("Retry-After", "1"); w.WriteHeader(http.StatusServiceUnavailable)`. Do not change the non-warmup paths; do not call `EnsureRunning` outside a `ref.Warmup` branch.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `GOWORK=off go test ./internal/gateway/ -run 'ServeOnlyWarmup|PhpBackendNotRunning|ShadowNeverCalls' -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full gateway + server suites**

Run: `GOWORK=off go test ./internal/gateway/ ./internal/server/ 2>&1 | tail -8`
Expected: PASS (no regression in the existing serve/warmup/shadow tests).

- [ ] **Step 6: Commit**

```bash
git add internal/gateway/server.go internal/gateway/serve_live_warmup_test.go
git commit -m "feat(gateway): serve-only warmup starts via EnsureRunning, 503 Retry-After on timeout"
```

---

## Notes for the executor

- **HLS readiness sliver.** `EnsureRunning` waits for `onAir()` (data flowing). For HLS the re-dispatch to the segments handler may still 404 for a sub-segment window until `Hub.HLSPlaylist()` is non-empty; that is the one short client retry the design explicitly tolerates ("bounded wait then retry"). Do not add a second poll for the playlist in Task 3 — keep `EnsureRunning` kind-agnostic on `onAir()`.
- **End-to-end validation needs a real serve-only LB** (a node with `policy.json` `serve_only:true` + a registered-but-idle stream). The unit tests above fully cover the logic; the live E2E shares the hardware gate noted for Part A's transcode E2E (the 1-CPU test MAIN).
- This is the first of the lifecycle phases; 4b (MAIN start/stop via the agent), 4c (retire the LB PHP lifecycle crons), and 5 (loopback) follow and are out of scope here.
