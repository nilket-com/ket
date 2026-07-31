# ADR 345 - spill/stream the semi/anti existence-set build (don't retain the full existence batch)

## Status
Proposed.

## Context

q21's next cut, concrete: **make the semi/anti existence-set build spill/stream without retaining the full existence
batch.** ADR 344 named q21's wall as the `semi/anti existence-set build/spill accumulator` (LeftSemi, sf=1-measured
~1.12 GB retained / 1.53 GB requested). 344's existence build is **in-memory** (`build_in_memory_join_rows`,
exec.rs:2609) after retaining the **full** existence batch (`record_batch_memory_bytes(&existence_batch)`, 2592). This
cut clears that measured LeftSemi wall. **Wall-mover-or-green:** q21 greens, OR the next retained existence-set wall
appears at **LeftAnti with measured bytes** - and the anti side **stays unclaimed (structural-only) until reached**.

### Grounded (344 + 336 + the code)
- 336's `SpilledJoinBuild` path (`execute_spilled_hash_join`, `execute_spilled_hash_join_into_global_aggregate`,
  `guard_spill_retained_entry_memory`, and `SpilledJoinBuild::write/write_inner`, currently exec.rs:5575/5695/7251/7320+)
  spills the build map to disk - **but retains its input batches** (the q18 lesson). So reusing 336 as-is, or spilling
  the hash structure while first concatenating the 1.12 GB existence input, only **renames** the wall.
- Therefore 345 must spill the build map **and stream the existence input**: consume existence batches **one at a
  time**, write/index each into the spill, **drop each** - never `concat_or_empty` the existence side.

## Decision - streamed-spilled existence-set build, with a proven no-concat input seam

Replace the in-memory existence build for the matched semi/anti shape with a **streamed-spilled** build: pull existence
batches incrementally, write/index each into `SpilledJoinBuild`, **drop each input batch after writing** - the full
existence batch is never retained. Probe the streamed preserved side against the spilled build, applying the
`match_filter` residual **per candidate** (not key-only): LeftSemi emits preserved-where-match
(`l2.l_suppkey <> l1.l_suppkey`), LeftAnti emits preserved-where-no-match (`l3.l_suppkey <> l1.l_suppkey AND l3 late`).
Bounded resident = the 336-budgeted spill-writer pages/index + one input batch + the preserved probe chunk.

### Gate 1 (load-bearing): no `concat_or_empty` on the existence side
**The acceptance requires proof** the implementation does **not** `concat_or_empty` (or otherwise retain the full)
existence input before spilling - else the wall is only renamed (the 336 failure mode). Phase 0 **names the streaming
input seam** (existence batches consumed one batch at a time, written/indexed, dropped), and the proof is concrete:
an instrumented multi-batch existence scan/visitor test shows only one existence batch resident at a time **or** an
architecture guard pins that the matched existence-set path does not call `concat_or_empty`. The test must prove "no
full existence materialization," not just a different producer string.

### Gate 2: sequential anti outcome (no claim before reached)
- **First:** the LeftSemi existence-set wall is **gone** (resident drops from ~1.12 GB to the 336-budgeted spill
  state; 332 shows the producer moved off the in-memory existence accumulator).
- **Then, only after semi clears:** either **q21 green**, or **LeftAnti becomes the new measured named wall** (sf=1
  bytes via 332).
- **No anti byte table from structural-only evidence** - the anti's measured bytes appear only once it is reached.

### Residual contract (non-negotiable)
Spilled probing applies `match_filter` **per candidate**, not key-only membership. The seal includes q21's sf=0.01
oracle (`pass@compare`) **and** focused semi/anti residual tests (semi `<>`, anti `<>` AND late).

## Acceptance
- **Gate 1:** the LeftSemi existence-set wall cleared, **with proof of no full-existence concat/retain** (the named
  streaming seam + the no-`concat_or_empty` test).
- **Gate 2 (sequential):** q21 green OR a named LeftAnti existence-set wall (sf=1 measured, 332), not a silent OOM; the
  anti unclaimed until reached.
- **Residual:** per-candidate `match_filter`; q21 `pass@compare` at sf=0.01 (ADR 329 oracle) + focused semi/anti
  residual tests in the seal; full matrix 22/22.
- **No silent wall** (336-budgeted spill resident; named LeftAnti if reached); **green cells + q07/q14/q19 unchanged**;
  bounded to the matched semi/anti existence-set build; no cost-model change.

## Guardrails
- **No-concat proof (gate 1)** - spilling the build map while retaining the existence input only renames the wall; the
  test must prove the input is streamed/dropped, not concatenated.
- **Sequential anti (gate 2)** - semi-gone first; anti green/named only after reached; no anti byte table from
  structural-only.
- **Residual per candidate** - not key-only; sf=0.01 oracle + focused residual tests in the seal (ADR 329 gate).
- **Bounded** - matched semi/anti existence-set build only; reuse 336/344/343; no cost-model change.

## Risks
- **Rename-the-wall** - the 336 failure mode; gate 1's no-concat proof guards it.
- **Anti over-claim** - structural-only must not produce anti bytes or an anti green; gate 2 is sequential.
- **Residual dropped under spill** - a spilled probe that reverts to key-only membership wrong-answers q21; the sf=0.01
  oracle + residual tests gate it.
- **Spill invariants** - dropping input batches mid-write must not break `SpilledJoinBuild`'s page/index invariants
  (Phase 0 confirms the streaming-write seam).

## Forward arc
q21 green closes the distinctive wide-join case (the streamed-spilled existence-set build is reusable for other
big-build joins). If LeftAnti becomes the next wall, it reuses this cut's mechanism (the same streamed-spilled build +
the anti residual). Remaining after q21: q17 (build-side), q12/q18 (retained-input), q04/q20 (spill-writer) - each
scoped from its named wall, or the logical-completeness pivot.

## Why toron-home
Engine cut in `crates/toron` (exec.rs semi/anti existence-set build + 336 spill + the 343/344 reuse); driven via
`toron-tpch` reach + correctness at sf=1 / sf=0.01. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. 344 named q21's semi existence-set wall; 345 spills/streams it. Operator's tightenings:
**gate 1** - acceptance requires **proof of no `concat_or_empty` on the existence side** (the streaming input seam
named + tested; the 336 rename-the-wall failure mode); **gate 2** - **sequential** anti outcome (semi-gone first; then
q21 green or a measured named LeftAnti wall; no anti byte table from structural-only); **residual contract** -
per-candidate `match_filter`, with q21's sf=0.01 oracle + focused semi/anti residual tests in the seal. Wall-mover-or-
green; reuse 336/344/343; bounded.
