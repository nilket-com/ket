# ADR 127: Live-Chunk Release After Seal

**Status:** Promoted (plan). **Grounded 3-of-3 (Claude final pass, Codex iter-1,
Gemini iter-1)** on the design: lock-coupled active-reader count (no count for
fallback-only readers), a single stream-owned reader guard with dual duty
(decrement + preserve unsealed-drop cancellation), idempotent release at
seal-or-last-reader, `pull_live` sealed-payload fallback that bypasses pacing,
and budget symmetry. The one divergence — whether a fallback-only `pull_live`
increments the reader count — is resolved to Codex's "no" and surfaced in
§Council history, not papered. Topic was an **operator steer to B** over the
peers' iter-0 lean to C (SQL broadcast hints); the steer is conditioned on the
fallback, exactly the B-blocker Codex named at iter-0.
**Category:** Toron interior — distributed live-shuffle substrate (partition
store memory lifecycle). Correctness + memory; no new IR / opcode / wire type.
**Predecessors:** 124 (pacing semaphore + `farthest_released_index` +
retry-from-zero — this ADR adds reader-lifecycle tracking alongside, and must not
perturb 124's pacing accounting); 126 (single-source seal left the 2-copy steady
state as its named follow-on — this is that follow-on).
**Migration:** `plans/impl/127_live_chunk_release_after_seal.md`.

## Doctrine — release the append-copy once the sealed copy can serve everyone

> ADR 126 made `live.chunks` the single source `seal_live` derives the sealed
> payload from, then left both resident: the sealed payload in `inner.partitions`
> AND the live chunks (for in-flight `pull_live` consumers + 124
> retry-from-zero). That 2-copy window persists until `remove_query`. ADR 127
> collapses it to one copy by dropping `live.chunks` (and their append-side
> `ShuffleBytes` reservations) once the sealed payload exists and no reader needs
> replay — made **safe** by a `pull_live` fallback that serves the sealed payload
> when chunks are gone. The partition store never consults scheduler state; it
> guarantees `pull_live` is always answerable, so release timing cannot lose data.

This also **closes a latent silent-empty-read bug**: today, a `pull_live` whose
chunks are absent at a sealed partition falls through to `if live.sealed { return
None }` (`partition_store.rs:733`) and yields an **empty stream, not the sealed
data**. The fallback makes that path serve the sealed payload — so the fallback
is a correctness gate in its own right, not merely a release precondition.

**Window grounding.** The scheduler chooses `Sealed` before `Live`:
`sealed_ref_for_producer` (reading `accepted_manifests`) is checked first
(`scheduler.rs:828`), falling to `Live` only when no accepted manifest exists AND
the single producer is streamable (`:834-839`). Post-acceptance consumers read
the sealed payload via `pull` (`mod.rs:1543`). So a `Live` ref → `pull_live` is
dispatched **only in the seal→manifest-accept window**, and the fallback covers
exactly: (a) a pre-seal `pull_live` stream that reaches sealed state without a
chunk at the requested index, especially an empty partition, and (b) a `Live` ref
dispatched in that window after chunks have been released. The store stays
ignorant of acceptance state; `pull_live` is simply always-correct.

## Decisions

**D1 — Active-reader count, lock-coupled, only for chunk-backed streams.** Add
`active_readers: usize` to `LivePartition` (`placeholder()` → 0). A `pull_live`
that is satisfiable **immediately** from an already-released sealed payload
(partition `sealed && chunks` empty at construction) returns a one-shot fallback
stream with **no reader guard and no increment** — it never reads `live.chunks`,
so it must not hold the live-reader count (Codex iter-1 D1/§5.2). Otherwise,
increment **inside the same `inner.write()` critical section** that runs
`or_insert_with(placeholder)` in `pull_live` (`:691-695`), before the stream is
returned — so a concurrent `seal_live` cannot observe `active_readers == 0` for an
extant chunk-backed stream and release chunks out from under a just-created
reader.

**D2 — One stream-owned reader guard, two duties; decrement exactly once.** A
chunk-backed `LivePartitionStream` terminates either by `next()` returning
`None`/`Err` (the common drained case — where `:194` currently clears
`cancel_on_drop`) or by `LivePartitionStream::drop` (abnormal exit). The stream
keeps the guard in an `Option<_>`. On terminal `next()`, it `take()`s the guard
and finishes with `cancel_if_unsealed = false`; on stream `Drop`, it `take()`s
any remaining guard and finishes with `cancel_if_unsealed = true`. That single
finish operation must **(a)** decrement `active_readers`, and **(b) preserve the
existing ADR-124 behavior** — on abnormal drop of an *unsealed* partition, still
cancel it (`live.cancelled = ...` + notify + pacing close + downstream
cancellation); on normal end, do not cancel. Do not rely on an independent
`Drop` implementation on the guard unless it is explicitly disarmed, because
taking a guard on normal end would otherwise finish twice. A `Drop`-only
decrement would never fire for the common drained case and chunks would never
release (Codex iter-1 D2).

**D3 — Release: idempotent, two triggers, one action.** `maybe_release(live)`
releases **iff** `sealed && active_readers == 0`: `live.chunks.clear()` +
`live.reservations.clear()` (frees the 124 append-side `ShuffleBytes` via
`BudgetReservation`'s `Drop`). Called from two sites — in `seal_live` after the
sealed payload is installed/recognized and the live partition is marked
`sealed`, and in the decrement path. Clearing empty vectors is already a safe
no-op, so the helper does not need a separate `!chunks.is_empty()` condition
(Codex iter-1 idempotency).

**D4 — `pull_live` sealed fallback, bypassing pacing (the crux).** Restructure
the unfold: when `live.sealed && live.chunks.is_empty()`, serve the single sealed
payload from `inner.partitions[address]` as a one-item stream (index 0 yields the
payload; next poll returns `None`), instead of `return None`. **This branch must
NOT touch `farthest_released_index` or `add_permits`** — it is a sealed read
shaped as a stream, not a chunk read; touching the pacing accounting would
double-count permits already added as chunks were consumed pre-seal (Codex
iter-1). The chunks-present index-replay path (`:716-731`) is **unchanged** — the
fallback is an added branch. Missing sealed payload under `live.sealed` is an
**error**, not silent EOF (Codex iter-1 D5); and the fallback yields once for
`index == 0` then terminates — never re-emits for `index > 0`.

**D5 — `pull_live` return type unchanged.** The fallback yields the sealed
payload as a single `PartitionPayload` through the existing `LivePartitionStream`
contract. The consumer (`LiveInputStorageScan`, `mod.rs:3886`) already
`decode_record_batches` each yielded payload, so one large payload decodes
identically to many small chunks — no return-type change, no consumer change
(Gemini iter-1).

**D6 — Budget symmetry, no cross-free.** Releasing chunks drops the append
reservations (`live.reservations`, 124) in the same step. It must **not** touch
the sealed reservation (`inner.reservations[address]`, 126) — distinct lifetimes:
the append reservation dies at release, the sealed reservation at `remove_query`.
Net steady state after release: **one resident copy + one reservation** (Codex
iter-1 cross-free caution).

**D7 — Cleanup paths unchanged.** `remove_query`/`cancel_query` `retain` by
`query_id` and never iterate `live.reservations`/`live.chunks` expecting them
non-empty (grounded, `:748-789`); early release just makes their `retain` a
smaller no-op. No change needed there.

**D8 — Scope boundary.** Touch surface: `partition_store.rs` (the `LivePartition`
field, `seal_live` release call, `pull_live` fallback + reader-count, the
teardown decrement) and the guards. **No** scheduler change (the
`Sealed`-before-`Live` dispatch already routes post-accept consumers to `pull`);
**no** consumer change (`LiveInputStorageScan` keeps calling `pull_live`,
`mod.rs:3873`); **no** wire/manifest/IR change; **not** the chunk-granular
manifest (the literal-constant-memory successor, still deferred).

## Behavior gates
- `release_on_seal_when_no_readers` — seal with no active reader → `chunks`
  empty, append reservation freed, sealed payload + its reservation resident.
  *Fails on `main`* (chunks resident post-seal).
- `release_on_last_reader_end_after_seal` — reader draining at seal keeps chunks;
  chunks drop when that reader ends (exercise **both** normal-end and early-drop).
- `pull_live_after_release_serves_sealed_payload` — **the spine.** Release, then a
  fresh `pull_live` returns full data as one payload (NOT empty). *Fails on
  `main`* (the `:733` empty-return path).
- `retry_from_zero_after_release_is_correct` — retry `pull_live` post-release gets
  full data via the fallback (124 contract preserved).
- `released_chunks_free_append_reservation` — budget symmetry.
- `concurrent_seal_and_pull_live_does_not_release_under_active_reader` — spawn a
  `pull_live`, seal concurrently; chunks NOT released while the reader is
  mid-stream, ARE released after it drops (the lock-coupling regression, D1).
- `fallback_stream_does_not_add_pacing_permits` — a post-release fallback
  `pull_live` must not perturb the pacing semaphore permit count, and takes no
  reader-count (D1/D4).
- `double_release_is_noop` — seal-site release then reader-drop release: no panic,
  no double budget-free (D3 idempotency).
- `fallback_yields_once_not_for_index_gt_zero` — the sealed fallback emits the
  payload for `index == 0` only; a missing sealed payload under `sealed` errors
  rather than EOF (D4).
- `abnormal_drop_of_unsealed_reader_still_cancels` — the guard's duty (b):
  dropping a chunk-backed stream mid-read of an unsealed partition still cancels
  it (124 downstream-cancellation preserved, D2).
- Regressions: 124 pacing/retry/cancel + 126 seal/empty-IPC/atomic-budget green.

## Architecture guards
- `adr127_live_partition_tracks_active_readers` — field present; increment
  lock-coupled in `pull_live`, decrement in the teardown path.
- `adr127_release_drops_chunks_and_reservations` — both triggers call the release
  helper dropping `chunks` **and** `reservations`.
- `adr127_pull_live_falls_back_to_sealed_payload` — the unfold serves from
  `inner.partitions` when `sealed && chunks` empty, and the fallback branch does
  not touch `farthest_released_index`/`add_permits` (pins against regression to
  the `:733` empty-return AND against pacing double-count).
- **Retarget check (discipline #4):** `adr124_pull_live_unfolds_by_index_for_replay`
  asserts `live.chunks.get(index)` + index-replay. The fallback **adds** a branch
  but keeps that path → the 124 guard holds by *addition*, not inversion;
  verified in impl Phase 5 (retarget in-change only if it inverts).

## Name (honesty)
"Live-chunk release after seal." Delivers: collapse the 2-copy window to one
resident copy *once chunks are released* (common case: no-reader seal or
post-drain). Does **not** make the worst case (reader mid-stream at seal)
constant — inherent; literal-constant-memory remains the deferred chunk-granular
manifest. The fallback makes "release" always *safe*, never *lossy*; the spine
gate must fail on `main`.

## Council history
- **iter-0:** Claude recommended B (grounded the lifecycle hook); Codex + Gemini
  recommended C (SQL hints), Codex flagging B as "a correctness regression
  disguised as memory cleanup" *without* a sealed fallback.
- **iter-1:** operator steered to B conditioned on the fallback — exactly Codex's
  stated B-blocker. Claude conceded the active-reader-only design was unsafe,
  adopted the fallback, grounded that it closes the `:733` silent-empty-read bug.
  Gemini conceded C→B and converged on the identical design.
  Codex's iter-1 ADR+impl independently sharpened three load-bearing details that
  this promotion adopts: lock-coupled increment, decrement-once on **both** stream
  ends, and — the divergence reconciled here — **no reader-count for a
  fallback-only `pull_live`** (a stream satisfiable straight from the sealed
  payload takes no guard), plus the guard's dual duty (decrement **and** preserve
  unsealed-drop cancellation), idempotent release, and "missing sealed payload
  under sealed = error, not EOF."
- **final promotion pass:** Claude grounded the scheduler
  `Sealed`-before-`Live` window that bounds the fallback, reconciled to Codex's
  no-count-for-fallback shape, and added the lock-coupling + pacing-bypass +
  idempotency gates. Genuine 3-of-3 on the design (Claude final pass, Codex iter-1,
  Gemini iter-1 all agree on the five load-bearing decisions; the only divergence
  — whether a fallback-only reader increments the count — is resolved to Codex's
  cleaner "no" and surfaced here, not papered). C remains the natural next ADR (a
  clean SQL-frontend cut;
  `adr125_no_sql_hint_authoring_yet` is pre-positioned to retarget) — deferred by
  sequencing, not correctness.

## What to do next
- **SQL broadcast hint authoring** (C) — the deferred peer pick; retarget
  `adr125_no_sql_hint_authoring_yet`, read `optimizer_hints` in `lower_select`.
- **Chunk-granular sealed manifest** — literal constant memory (multi-entry
  per-partition); the consumer-`pull`-API-breaking successor to this ADR.
- **Executor build/probe reorder** — streamable joins (multi-cut arc).

## Promotion criteria
All behavior gates green (spine `pull_live_after_release_serves_sealed_payload`
and `release_on_seal_when_no_readers` must fail on `main` beforehand); the three
concurrency gates green; the three `adr127_*` guards hold; the 124 replay guard
holds by addition; ADR 124/126 regressions green; `cargo test -p toron` +
`cargo test -p toron --test architecture_test adr124` +
`cargo test -p toron --test architecture_test adr126` +
`cargo test -p toron --test architecture_test adr127` +
`cargo check -p toron --all-targets` green. Migration:
`plans/impl/127_live_chunk_release_after_seal.md`.
