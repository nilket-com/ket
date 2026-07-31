# Impl Plan — ADR 127: Live-Chunk Release After Seal

**Migration for** `plans/adr/127_live_chunk_release_after_seal.md`.
**Scope:** `crates/toron/src/distributed/partition_store.rs` only (production) +
its `#[cfg(test)]` unit tests +
`crates/toron/tests/architecture/distributed_execution/scheduler_dispatch.rs`
guards.
**Non-scope:** scheduler, consumer (`server/mod.rs`), wire/manifest/IR, Arrow IPC
helpers, chunk-granular manifest.

## Pre-flight anchors (verify before editing)
| Concern | Anchor |
|---|---|
| `LivePartition` struct / `placeholder` | `partition_store.rs:304-315 / 318-331` |
| `LivePartitionStream` + `with_cancel_on_drop` (sole prod ctor) | `:169-197 / :180` |
| `LivePartitionStream::next` clears `cancel_on_drop` on end | `:191-197` (`:194`) |
| `LivePartitionCancelOnDrop` (teardown hook) | `:174, :200-` |
| `pull_live` write-lock entry (`or_insert_with`) | `:691-695` |
| `pull_live` unfold (fallback site = empty-return) | `:716-735` (`:733`) |
| `farthest_released_index` / `add_permits` (do NOT touch in fallback) | `:727-729` |
| `seal_live` (release-site A, after sealed loop) | `:636-661` (sealed set `:642-643`) |
| append reservations (released on chunk release) | `live.reservations`, append at `:543` |
| sealed reservation (do NOT free here) | `inner.reservations[address]`, `:650` |
| `remove_query`/`cancel_query` (no change) | `:748-789` |
| 124 replay guard to re-verify | `scheduler_dispatch.rs adr124_pull_live_unfolds_by_index_for_replay` |

## Phase 0 — Failing gates first
Author the behavior gates (ADR §Behavior gates — incl. the concurrency,
guard-duty, and fallback-shape gates) + 3 `adr127_*` guards. Verify
`release_on_seal_when_no_readers` and
`pull_live_after_release_serves_sealed_payload` **fail on `main`** before
implementing (honesty check, [[feedback_cut_must_deliver_the_name]]).

## Phase 1 — Reader count, lock-coupled, fallback takes none (ADR D1)
Add `active_readers: usize` to `LivePartition` (`placeholder()` → 0). In
`pull_live`, **inside** the existing `inner.write()` block that runs
`or_insert_with(placeholder)` (`:691-695`):
- **If already-sealed-and-released** (`live.sealed && live.chunks.is_empty()`):
  clone `inner.partitions[address]` and return a **one-shot fallback stream with
  no reader guard and no increment** (Codex iter-1 §5.2) — it reads no chunks, so
  it must not be counted. If the sealed payload is missing under `sealed`, return
  an error. Scope any mutable borrow of `live` before cloning from
  `inner.partitions`; otherwise the fallback construction will fight the borrow
  checker.
- **Else (chunk-backed):** `live.active_readers += 1;` before building the
  guarded stream. The increment is committed before the stream exists, so a
  concurrent `seal_live` cannot see zero readers for an extant stream
  (`concurrent_seal_and_pull_live_does_not_release_under_active_reader` pins this).

## Phase 2 — Reader guard: dual duty, decrement once (ADR D2)
Extend `LivePartitionCancelOnDrop` (carries `inner` + `address`) into a guard
with **two** duties, both under `inner.write()`, exactly once per counted stream:
- **(a) decrement** `active_readers` (`saturating_sub(1)`; `debug_assert!(> 0)`),
  then call `maybe_release`.
- **(b) preserve 124 cancellation:** on **normal end** (from `next` on
  `None`/`Err`) do **not** cancel; on **abnormal `Drop`** of an *unsealed*
  partition, still set
  `live.cancelled = Some(PartitionMissingReason::CancelledTaskCleanup)`, notify,
  close pacing, and trigger downstream cancellation (the existing 124 behavior).
  `abnormal_drop_of_unsealed_reader_still_cancels` pins this.

Wire it like the existing `Option` pattern on `LivePartitionStream`:
`LivePartitionStream::next` *takes* the guard on terminal `None`/`Err` (`:194`)
and finishes with `cancel_if_unsealed = false`; `LivePartitionStream::drop`
*takes* any remaining guard and finishes with `cancel_if_unsealed = true`. The
stream-owned `Option` being taken on normal end means stream `Drop` sees `None`
→ exactly one decrement. Do not put an undisarmed `Drop` implementation on the
guard itself; a guard taken on normal end would then run both `finish(false)` and
its own `Drop`. Fallback-only streams hold no guard, so they never decrement
(Phase 1).

## Phase 3 — `maybe_release` helper (ADR D3, idempotent)
```text
fn maybe_release(live: &mut LivePartition) {
    if live.sealed && live.active_readers == 0 {
        live.chunks.clear();        // drop the encoded bytes
        live.reservations.clear();  // frees 124 append ShuffleBytes via Drop
    }
}
```
Call from `seal_live` for each just-sealed live partition after the sealed
payload is installed or recognized in `inner.partitions`, and after
`sealed = true` / `pacing_semaphore.close()` (`:642-651`). That ordering keeps
the fallback answerable before chunks can disappear. Also call from the Phase-2
decrement path. `clear()` on an already-empty vector is the idempotent no-op
(`double_release_is_noop`). Do **not** touch `inner.reservations[address]` (the
126 sealed reservation) — distinct lifetime (ADR D6).

## Phase 4 — `pull_live` sealed fallback, bypassing pacing (ADR D4)
Two fallback entry points serve the sealed payload, both bypassing pacing:
- **At construction (Phase 1):** already-sealed-released → one-shot stream, no
  guard, no increment.
- **Inside the unfold:** a counted reader can have been created before seal and
  reach sealed state with no chunk at `index` (notably an empty partition). In
  the unfold, replace the bare `if live.sealed { return None }` (`:733`) with:
```text
if live.sealed && live.chunks.is_empty() {
    if index == 0 {
        if let Some(p) = guard.partitions.get(&address).cloned() {
            return Some((Ok(p), (inner.clone(), address, 1)));  // next poll → None
        }
        return Some((Err(<missing-sealed-under-sealed>), ...));  // error, not EOF
    }
    return None;  // index > 0: already emitted, terminate
}
```
This branch does **NOT** touch `farthest_released_index` or `add_permits`
(`fallback_stream_does_not_add_pacing_permits` pins it), yields once for
`index == 0` only (`fallback_yields_once_not_for_index_gt_zero`), and errors on a
missing sealed payload rather than silent EOF. The chunks-present path
(`:716-731`) is unchanged, preserving retry-from-zero.

## Phase 5 — Guards + 124 re-verify
Author `adr127_live_partition_tracks_active_readers`,
`adr127_release_drops_chunks_and_reservations`,
`adr127_pull_live_falls_back_to_sealed_payload`. Run
`cargo test -p toron --test architecture_test adr124` and
`cargo test -p toron --test architecture_test adr127` — confirm
`adr124_pull_live_unfolds_by_index_for_replay` still passes (the fallback added a
branch; `live.chunks.get(index)` must still hold). If it inverts, retarget
in-change per discipline #4 (expectation: holds by addition).

## Verification
- `cargo test -p toron distributed::partition_store`
- `cargo test -p toron --test architecture_test adr124`
- `cargo test -p toron --test architecture_test adr126`
- `cargo test -p toron --test architecture_test adr127`
- `cargo test -p toron live_shuffle`
- `cargo check -p toron --all-targets` (new struct field —
  [[feedback_verify_all_targets_on_new_ir_field]])
- `cargo test -p toron` ; `git diff --check`

## Risk register
- **Decrement-once (load-bearing):** if both `next`-end and `Drop` decrement, the
  count underflows → premature release. The taken-`Option` is the guard;
  `release_on_last_reader_end_after_seal` must exercise **both** normal-end and
  early-drop.
- **Lock-coupling:** increment outside the `or_insert_with` write lock would race
  a concurrent seal; Phase 1 keeps it inside.
  `concurrent_seal_and_pull_live_does_not_release_under_active_reader` pins it.
- **Pacing double-count:** the fallback must bypass `add_permits`;
  `fallback_stream_does_not_add_pacing_permits` pins it.
- **Budget cross-free:** drop only `live.reservations` (append), never
  `inner.reservations[address]` (sealed). The latter dies at `remove_query`.

## Acceptance
ADR 127 accepted when: all behavior gates pass (spine
`pull_live_after_release_serves_sealed_payload` + `release_on_seal_when_no_readers`
failed on `main` beforehand; the concurrency + guard-duty + fallback-shape gates
green); the three `adr127_*` guards pass; the 124 replay guard holds; 124/126 live
suites green;
`cargo check -p toron --all-targets` + `git diff --check` clean. The cut delivers
its name: a **safe, single-resident-copy** live shuffle in the common case — not
constant memory.
