# ADR 0004 — The ring is the live tail too (retire the push-channel fan-out)

Status: **accepted** (2026-09-12) — implemented. `Hub` exposes `Join`/`Follow` + a
`wake` signal; `serveLive` and the overlay follow the ring by cursor; the `Sub`
push-channel machinery (`Subscribe`/`CatchUp`/`Unsubscribe`/`Count`/`subQueue`) is
removed. See [internals: TS fan-out](../en/04-internals.md#ts-fan-out--hub).

## Context

ADR 0001 made the `tsjoin` ring the **single source of truth** for a stream's
bytes: the TS prebuffer and the HLS view are both cut from it, the second HLS byte
store is gone, and GOP buffers are recycled so steady-state GOP allocation is ~0.
`Hub.Publish` also learned to fold a chunk into the ring **in place** when a stream
has no subscribers (the idle case), so an unwatched fed channel allocates nothing
per chunk.

One copy remains, and it is on the **watched** path. `Hub.Publish`, when a stream
has viewers (`internal/hub/hub.go`):

```go
b := make([]byte, len(chunk)); copy(b, chunk) // fresh heap alloc, every chunk
h.join.Update(b)                              // ring copies b's packets into its (recycled) GOP buffers
for s := range h.subs { s.ch <- b }          // N subscribers hold b until they drain it
```

`join.Update` copies the bytes it retains into the ring's own recycled GOP
buffers and keeps no reference to `b`. So `b` is a **second copy of bytes the ring
already holds**, allocated fresh every chunk, referenced only by subscriber
channels, and thrown to GC once every subscriber has drained past it.

At scale this is the dominant steady-state cost on a watched node:

- **Allocation / GC.** One `len(chunk)`-byte allocation per published chunk per
  watched stream. 500 watched streams at ~500 KB/s ≈ **~250 MB/s of allocation**
  that produces nothing the ring does not already have — enough to keep the GC
  permanently busy even with `GOGC=50` and the soft memlimit holding RSS.
- **Memory bandwidth.** Each source byte on a watched stream is memcpy'd
  **twice** after the socket read: once into `b`, once into the ring. The whole
  premise of the native remux (ADR 0002) is that copying a channel should cost a
  goroutine, not a process; this doubles the copy for no benefit.
- **Slow-viewer amplification.** `SubscriberQueue = 256` × ~12 KB ≈ **~3 MB
  pinned per slow viewer** before it is dropped.

The key observation: **`serveLive` already reads the ring forward with zero copy**
for the join burst. `Hub.Join` returns a cursor; `Hub.CatchUp` → `tsjoin.ReadFrom`
walks the ring in `joinRunBytes` (1 MiB) runs, each pinned only while it is
written to the socket, until the viewer reaches the live edge — and only *then*
subscribes to the push channel for the live tail. So a viewer's session already
runs on the ring-cursor model for its history and switches to the push-channel
model at the edge. Two models, one copy, coexisting on one path.

## Decision

Retire the push-channel fan-out. A live-TS viewer **follows the ring by cursor for
its whole session** — join *and* tail — and blocks on a per-hub wake signal when it
reaches the live edge. `Hub.Publish` stops copying and stops broadcasting: it folds
the chunk into the ring (as it already does for idle streams) and wakes the
followers. The ring becomes the single byte store for join, HLS **and** the live
tail; `Sub`, the subscriber map, and the per-chunk copy all go away.

## Design

### 1. Wake signal — closed-and-replaced channel, captured under `h.mu`

`Hub` gains `wake chan struct{}` (created in `New`, re-created on each publish).

```go
func (h *Hub) Publish(chunk []byte) {
    h.mu.Lock()
    h.join.Update(chunk) // copies retained bytes into recycled GOP buffers; keeps no ref to chunk
    close(h.wake)        // wake every follower parked at the edge
    h.wake = make(chan struct{})
    h.mu.Unlock()
}
```

No copy: `Update` already copies what it keeps, and nothing references `chunk`
after it returns — the caller may reuse its buffer, exactly the contract the
current no-subscriber branch already relies on.

**Race-freedom (no lost wakeups).** The `wake` channel a follower parks on is the
exact one it captured under the same `h.mu` that `Publish` takes to `close` it. If
a `Publish` lands between a follower's unlock and its `select`, the channel is
already closed, so the `select` returns immediately and the follower re-reads. This
is the standard closed-channel broadcast; it composes with `select` on
`ctx`/`kill`/idle, which `sync.Cond` does not.

### 2. Follower API on `Hub` (replaces `Subscribe`/`CatchUp`/`Sub`/`Unsubscribe`)

```go
// Follow returns the next run of the ring from c (pinned in a Burst the caller
// writes then Releases) and the cursor after it. atEnd reports the run reached the
// live edge, and then wake is a channel closed by the next Publish (or by
// teardown) for the caller to park on; when there is more to read now, wake is nil.
// behind reports c fell off the ring tail (a link slower than the stream). ended
// reports the stream was torn down (CloseAll).
func (h *Hub) Follow(c tsjoin.Cursor, max int) (b *Burst, next tsjoin.Cursor,
    atEnd bool, wake <-chan struct{}, behind, ended bool)
```

Under `h.mu`: if `closed`, return `ended`. Else
`parts, next, atEnd, behind := h.join.ReadFrom(c, max)` (unchanged; it already
pins when `parts>0` and reports `behind`). Build `Burst{Parts: parts, h: …}`. When
`atEnd`, also return the current `h.wake`. `Hub.Join(prebufMS)` stays as the entry
point (unchanged): PAT/PMT head + start cursor.

### 3. `serveLive` — one loop replaces the catch-up loop **and** the live select

```go
head, cur := st.Hub.Join(prebufMS)
st.attach(); defer st.detach()
if len(head) > 0 { if write(head) != nil { return } }
for {
    b, next, atEnd, wake, behind, ended := st.Hub.Follow(cur, joinRunBytes)
    if behind { reason = "dropped: fell behind the ring (link slower than the stream)"; return }
    if ended  { reason = "stream torn down"; return }
    for _, p := range b.Parts {
        select {
        case <-killC:          b.Release(); reason = "dropped by panel"; return
        case <-r.Context().Done(): b.Release(); reason = "client closed"; return
        default:
        }
        if err := write(p); err != nil { b.Release(); reason = writeFailReason(err); return }
    }
    b.Release()
    cur = next
    if atEnd {
        select {
        case <-wake:               // new data, or teardown
        case <-idleC:              // viewer idle-timeout (armed only while waiting)
        case <-killC:              reason = "dropped by panel"; return
        case <-r.Context().Done(): reason = "client closed"; return
        }
    }
}
```

- **Idle timeout** is now armed only while parked at the edge — precisely "the
  viewer is receiving nothing." That is exactly the condition it exists for, so the
  `lastChunk` bookkeeping and the re-arm-for-remainder dance disappear: a viewer
  actively draining a run never reaches the `idleC` case. One timer per viewer,
  simpler than today.
- **No gap, no duplication:** `next` points exactly one byte past the last one
  written, and the ring only appends and prunes-from-front, so the join→tail
  boundary is seamless — the same invariant `CatchUp` relies on today, now applied
  the whole way through.

### 4. Teardown

`Hub` gains `closed bool`. `CloseAll` (from `Unregister` / `DELETE /streams`) sets
`closed = true` and `close(h.wake)`, waking every parked follower; `Follow` then
returns `ended` so each `serveLive` returns and runs its deferred
`detach`/`removeConn` — the same outcome `CloseAll` reaches today by closing every
`Sub.done`.

### 5. Follower count (`Hub.Count`)

`memory.go` (`ringView.Viewers`) and the debug snapshot use `Hub.Count()` (the
subscriber-map size). Without the map, drop `Hub.Count` and read **`st.refs`** —
already the live-TS viewer count maintained by `attach`/`detach`, and a near
duplicate of the old `Count`. One redundant counter removed. (`refs` counts a
viewer from `attach`, i.e. including its catch-up phase; the old `Count` counted
only post-edge subscribers. `refs` is the more useful figure for the memory
report and debug line.)

### 6. Pins and recycling under continuous following

Unchanged from the catch-up path: `ReadFrom` pins when it returns parts, the caller
`Release`s after writing each run, so a follower holds a pin only for the duration
of one `joinRunBytes` (1 MiB) socket write, then re-reads. Recycling
(`putBuf`/`prune`) is suspended only during that short window — exactly as it is
during catch-up today. No change to `prune`, `putBuf`, or the free list.

### 7. Overlay path (`overlayTSWindow`)

Today the overlay feed goroutine drains `sub.C()` for its window. Rework it to the
cursor: take the current `cur`, read the ring forward with `Follow` to feed the
overlay ffmpeg's stdin for `OverlayTSDuration`, and **return the advanced cursor**
so `serveLive` resumes `Follow(cur)` exactly where the overlay left off.
`Hub.Snapshot(0)` still re-seeds the ffmpeg decoder at a keyframe (unchanged). A
single cursor, advanced in one place, preserves today's "no chunk stolen from the
raw loop" property without a shared subscriber. This is the fiddliest part of the
change and should stay minimal (cursor in, cursor out) and be covered by the
existing `overlay_ffmpeg` tests.

### 8. Removed

- `hub.go`: `Sub`, `subs map`, `Subscribe`, `CatchUp`, `Unsubscribe`, `Count`, and
  the per-chunk copy + broadcast in `Publish`.
- `defaults.SubscriberQueue` (dead once the channel is gone).
- Tests encoding the old contract: `burst_queue_test.go` (the 256-chunk drop)
  becomes obsolete; `hub_test.go` and `subscribe_race_test.go` are rewritten to the
  `Follow` semantics.

## Consequences

- **No per-chunk allocation on the watched path.** `Publish` allocates nothing
  (it already didn't when idle); the broadcast-buffer allocation class is gone.
- **One copy, not two.** Source bytes are copied once — into the ring's recycled
  GOP buffers — halving the fan-out's memory-bandwidth cost on watched streams and
  removing ~(source bandwidth × watched streams) of GC allocation.
- **No 256-chunk per-viewer pin.** "Too slow" is decided by the ring's own
  retention (the cursor fell off the tail), not a separate 3 MB queue.
- **One serve path.** Slow-drop, idle-drop, kick, client-close and teardown are
  all handled in a single `select`; the join-vs-tail split disappears.
- **Less lock work per chunk.** `Publish` does an O(1) close+replace instead of N
  channel sends under `h.mu`; followers take the lock only for a short capture and
  write outside it.

## Costs / risks

- **Wake latency:** a follower at the edge wakes on the next `Publish` (chunk
  cadence, tens of ms) — the same latency as a channel send today.
- **Thundering herd:** one `Publish` wakes all followers of that stream, which then
  each re-take `h.mu` to capture their next run — N lock acquisitions per chunk,
  comparable to N channel sends per chunk today. If it ever bites a very large
  single-channel audience, shard the wake or batch reads. Watch-item, not a
  blocker.
- **Core byte-path change — highest-risk area.** Mitigated by runnable hub/tsjoin
  unit tests (below) and by gating merge on the socket-bound server/integration
  suite, which must be run outside the assistant's sandbox (`GOWORK=off go test
  ./...`).
- **Overlay rework** is the trickiest piece; keep it to cursor-in/cursor-out.

## Test plan

- **`hub_test.go` (rewrite):** `Follow` returns the join burst then parks at the
  edge; a `Publish` wakes it; `behind` when the cursor is pruned out; `ended` on
  `CloseAll`; a published byte sequence reads back contiguously across the
  join→tail boundary (no gap, no dup).
- **Race test (`-race`):** concurrent `Publish` + many followers; every follower
  sees a contiguous prefix; a deliberately slow follower gets `behind`, never
  corruption.
- **`tsjoin` `ReadFrom`:** extend with an at-edge-then-append case (already covers
  cursor/prune/behind).
- **`server` / `integration` (socket-bound — run by a human):** join + tail
  delivery; slow viewer dropped; kick / ctx / idle-timeout still fire; teardown
  drops attached viewers.
- **Benchmark:** a `Publish`-with-N-followers benchmark asserting **0 allocs/op**
  on the watched path, versus the current per-chunk copy.

## Rollout

One PR (the two models cannot cleanly coexist on the same path), in staged commits
that each build and pass the hub/tsjoin unit tests:

1. Add `wake` + `Follow` + `ended` to `hub`/`tsjoin` with unit tests, keeping
   `Subscribe`/`CatchUp` temporarily.
2. Switch `serveLive` to `Follow`.
3. Rework `overlayTSWindow` to cursor-in/cursor-out.
4. Delete `Sub`/`Subscribe`/`CatchUp`/`subs`/`Count` + dead consts; rewrite the old
   hub tests.

## Alternatives considered

- **Refcounted `sync.Pool` for the broadcast buffer.** Recovers the allocation but
  keeps the redundant memcpy and both serve paths, and adds refcount bookkeeping
  across asynchronous drains and drops. Rejected as the end state; acceptable only
  as a stopgap if the full change must wait.
- **`RWMutex` split (ring reads vs. producer writes).** Orthogonal, and the cursor
  model already relieves the lock, so defer it.
