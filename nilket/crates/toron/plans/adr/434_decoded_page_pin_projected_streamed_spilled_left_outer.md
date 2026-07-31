# ADR 434 - Decoded-page pin for the projected streamed-spilled LeftOuter grouped shape

## Context

**ADR 433 named the producer** (diagnosis-only, board 22/22): the **decoded spill-page cache
eviction/reread loop** on the projected streamed-spilled LeftOuter path. q13's 5,860 spill pages decode
to a **12,562,560-byte working set — 1.498x** the 8,388,608-byte cache target, so the probe churns
**4,823,849 rereads / decodes / evictions**, **822.99x** read amplification (7.53 GB re-read of a
9.15 MB page-cache-hot spill), and **~9.66M voluntary context switches per child** (~2 per pending
`BlockingFileOps` read). `touch_cache_entry`'s linear `VecDeque::retain` LRU is the 30.82% top self
frame. In the 356 baseline q13 is **134.5 s / 97.31x DuckDB** — the flagship speed target.

**The fix substrate exists.** `try_pin_decoded_pages(memory_cap)` (`spill.rs:1863`) decodes every page
**once through the normal cache, then clears cache/LRU** and holds the pages resident
(`pinned_decoded_pages`) — removing probe-time reread, decode, per-op file read, and the linear LRU
touch. It is guarded: `join_spill_retained_footprint_memory_cap(memory_cap) = memory_cap × 16`
(`join.rs:3709`, multiplier `:43`). The guard estimates resident as
`resident_baseline + resident_index + writer_peak + page_reserve + loaded_page_estimate` and can reject
at **two stages** — a **preflight** check (`spill.rs:1881`) and a **mid-decode** check (`:1896`) — so
admission is **not** a function of the 12.56 MB decoded set alone. q13's projected path does not call
it (the caller at `streaming.rs:6496` is a different shape).

**Admission is measured, not assumed.** Whether pinning is admitted for the projected streamed-spilled
LeftOuter grouped shape depends on the full estimated footprint against the 16× cap; this cut lands a
**pin receipt** that records the decision and its inputs rather than asserting the set is "small."

## Decision — shape-fenced pin at the parent probe boundary; typed reject with clean fallback

Call `try_pin_decoded_pages(task_budget.memory.get())` **once at the parent probe boundary** — after
`probe_started`, before `visit_streaming_spilled_left_outer_scan_probe_chunks` (`streaming.rs:~7178`) —
so ownership and timing are explicit. Fenced to the **projected streamed-spilled LeftOuter grouped
shape** (the recognizer's `match_filter.is_none()` grouped branch), not a q13 identity.

- **Admitted** → the probe/materialize reads pinned decoded pages: no probe-time eviction, no reread,
  no per-page file read, no `touch_cache_entry`. The pin's own preload still performs O(page-count)
  reads/decodes once.
- **Rejected** (preflight or mid-decode) → `pinned_decoded_pages` stays absent and execution **falls
  through to the existing cache path, value-identical** — no regression.
- **Typed outcome:** the existing `bool` return does not distinguish the two reject stages; this cut
  adds a **typed rejection-stage** (Admitted / RejectedPreflight / RejectedMidDecode) surfaced in the
  pin receipt so both paths are observable and testable.

## Acceptance

- **Counter collapse — the proof of fix (re-run the 433 ledger).** The pin's preload is O(page-count),
  so the *aggregate* ledger does not go to zero. Require either **(a)** total reads / decodes / pending
  reads / evictions **≤ page count (~5,860)** with **probe-time rereads and evictions = 0**, or **(b)**
  the ledger **splits preload vs probe counters** and **probe-phase evictions and rereads = 0**
  directly. Voluntary context switches collapse from ~9.66M toward the preload floor. (b) is preferred
  for legibility.
- **Pin receipt (grounds admission):** records memory cap and retained cap; baseline / index / writer /
  page-reserve / decoded bytes; the preflight estimate; pages attempted and completed; and the typed
  outcome (admitted or rejection stage).
- **q13 correctness parity:** fingerprint `ac3784c060c3c226`, digest, schema
  `fnv1a64:06933e705bc02cb3`, 46 rows, and 9,152,992 spill bytes unchanged.
- **Speed claim only from an unprofiled 356-shaped run** — q13 under the exact 356 profile
  (`AllowedCPUs=0-1`, `CPUQuota=150%`, memory-unbounded, `TasksMax=512`, `RuntimeMaxSec=3600`), **7
  measured + 1 warmup with per-row fingerprint/digest/schema/rows/spill invariance**, stability at
  `spread=(max−min)/median ≤ 0.20`, the existing **one-retry** rule; report **before 134.5 s → after**
  with the band `max(0.10, 2 × max(spreads))`. No profiled timing is banked.
- **Survival:** **q13 `pass@compare` under 4 GiB / no-swap**; board stays 22/22. No full-board redux is
  implied unless one is actually run.
- **Async-body + widely-constructed-type discipline (433 lesson):** the added parent-boundary `.await`
  and any new held state must not overflow the reoriented-chain deep-recursion test; run `check -p
  toron --all-targets`, the FULL `architecture_test`, and the deep-recursion lib test; box new held
  state if the async body grows. `exec.rs` ratchet held; no cap/envelope change; no `.perf.data`.

## Guardrails and risks

- **Both reject stages are tested,** not just the happy path: a preflight-reject fixture and a
  mid-decode-reject fixture each assert `pinned_decoded_pages` absent and subsequent cached execution
  value-identical.
- The pin's preload cost is real; the win is amortizing 4.82M probe rereads into ~5,860 preload reads,
  not eliminating I/O. The acceptance bounds the preload, so it cannot silently become a new wall.
- Pinning holds the decoded set resident; the receipt + guard keep that within the retained-footprint
  cap so survival is unaffected.

## Forks

- **A — scope:** projected streamed-spilled LeftOuter **grouped-shape-only** pin (lean; minimal blast
  radius) vs pin any admitted streamed-spilled LeftOuter (wider; other shapes' footprints re-reasoned).
- **B — the LRU:** **defer the general O(1) LRU rewrite** (lean; pinning removes the linear touch for
  the pinned path) vs bundle it (scope creep; only helps shapes that cannot pin).

## Recommendation

**A = grouped-shape-only pin, B = defer O(1) LRU.** Smallest extension of the landed pin substrate to
the named branch, proven by the ledger probe-phase collapse, the pin receipt, and an unprofiled 356
before/after. If pinning does not fully close q13's 134.5 s, 433 already named the next wall — the
grouped fold (`fold_bound_grouped_aggregate_batch`, 10.81%); the O(1) LRU stays a separate follow-up
for shapes that cannot pin.

## Forward arc

q13's before/after is the first speed data point for the "benchmarks over time" article. q17/q19 are
diagnosed on their own numbers next, not assumed to share q13's producer. Grouped fold and O(1) LRU are
named, deferred follow-ons.
