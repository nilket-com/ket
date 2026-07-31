# ADR 344 - q21 semi/anti extension (prove the sides, then cut)

## Status
Proposed.

## Context

The substrate's next query, q21 - the most semantically distinctive remaining wide-join case. ADR 343 greened q07 with
the reoriented-inner-chain streaming primitive; q21 **reuses those mechanics for its inner prefix** (`Join(1-2)`) but its
**semi/anti suffix** (LeftSemi `Join(3)`, LeftAnti `Join(4)`) is a different problem. **Phase 0 is "prove the semi/anti
sides," not "extend 343 and hope":** the "two big sides" hypothesis is a **measurement gate, not a conclusion**, and
the artifact must never present structural inference as sf=1 runtime bytes. Wall-mover discipline: **green-or-named-wall,
strong prior the honest output is a named semi/anti existence-set build/spill wall.**

### Grounded (337/342/343 + the code)
- q21: inner prefix `Join(1-2)` (343's shape) + semi/anti `Join(3)` LeftSemi / `Join(4)` LeftAnti, then grouped
  aggregate. 342: both semi/anti are build-side (preserved intermediate held).
- The existence sets are **lineitem l2 / lineitem l3** (EXISTS / NOT EXISTS) - **structurally large at sf=1**, so this
  is likely two-big-sides, not a build-small/stream-big reorient. *Likely, pending Phase 0 measurement.*
- The semi/anti suffix has load-bearing non-equi residuals: `l2.l_suppkey <> l1.l_suppkey` for the EXISTS branch and
  `l3.l_suppkey <> l1.l_suppkey AND l3 late` for the NOT EXISTS branch. They ride as the join's **`match_filter`**
  (exec.rs:1006/1022; ADR 323/329); any streamed/spilled probe **must** apply them per batch.
- Reuse paths: 343 reoriented-inner-chain mechanics (inner prefix); 336 spill (`execute_spilled_hash_join` +
  `guard_spill_retained_entry_memory`, exec.rs:4997/6732) and `execute_spilled_hash_join_into_global_aggregate` (5107)
  for a big build.

## Decision - prove the sides (gate), reuse 343 for the inner prefix, spill+residual for semi/anti

### Phase 0 - the side-proving table (evidence provenance is explicit)
For each q21 LeftSemi/LeftAnti join, produce a row:

| join_id | kind | preserved_side | preserved_source | preserved_bytes | existence_side | existence_source | existence_bytes | verdict |

`preserved_source` / `existence_source` are **explicit, one of**:
- **sf=1 trace** - actual, if the boundary is reached (strongest);
- **synthetic isolated-operator probe** - the operator run in isolation on sf=1-sized inputs (actual bytes, synthetic
  inputs);
- **sf=0.01 scaled** - measured at sf=0.01, scaled (inference);
- **structural-only** - bound-plan shape, no bytes (weakest).

**The acceptance rule (operator):** structural-only is enough to **size the next probe / name the wall**, **not** enough
to justify a **green-target implementation**. A green-target (the reorient branch) requires an **sf=1 trace or a
synthetic isolated-operator probe** showing the existence-set is small.

### The gate (measure, don't conclude)
- **Both sides large** -> not a build-small reorient; the cut is a **build-side spill/budget of the existence-set**
  (reuse 336), preserved side streamed as the probe, match_filter residual per batch. Honest output: a **named**
  existence-set build/spill wall.
- **Existence-set small (only if sf=1-trace / synthetic-probe shows it)** -> a 343-style reorient (build the small
  existence-set, stream the preserved side, semi/anti match + residual) may green q21.

### Inner prefix + residual contract
- Inner prefix `Join(1-2)` reuses the 343 primitive (small build, streamed probe, output-order restore), but **not** by
  pretending the current full-chain detector already matches q21: ADR 343's helper is fenced to the all-Inner 1-4
  chain. If needed, factor a prefix-capable inner-chain visitor while preserving the existing ADR 343 full-chain path
  unchanged.
- The `match_filter` residuals are part of the semi/anti probe contract: applied per batch, semi (emit
  preserved-where-match) / anti (emit preserved-where-no-match); the preserved side stays the output (no semantic flip).

## Acceptance (green-or-named-wall, strong prior named)
- **q21 sf=1: green OR a named semi/anti existence-set build/spill wall** (ADR 332 retention - producer + bytes), not a
  silent OOM. Strong prior: named wall. **A green claim requires non-structural measurement** (sf=1 trace / synthetic
  probe), per the evidence rule.
- **Residual correctness:** both semi/anti `match_filter` residuals applied; q21 `pass@compare` at sf=0.01 (the EXISTS
  + NOT-EXISTS-both-ways the ADR 329 oracle pins); full matrix 22/22.
- **Inner prefix reuses 343's primitive** (no new inner semantics; existing ADR 343 full-chain path unchanged);
  semi/anti is the new piece.
- **Green cells + q07/q14/q19 unchanged** (executor-local, no cost-model shift); no semi/anti semantic change.

## Guardrails
- **Evidence provenance** - the Phase 0 table labels every byte's source; structural-only sizes the next probe, never
  justifies a green-target.
- **Residual is load-bearing** - the match_filter non-equi residual must ride the semi/anti probe; a key-only semi/anti
  silently wrong-answers.
- **No semantic flip** - preserved side stays the output.
- **No silent wall** - an over-budget existence-set build/spill is named (332), not a silent OOM.
- **Bounded** - q21's matched shapes only; no global execute_stream; no cost-model change.

## Risks
- **Structural-as-measured** - never present a structural side as an sf=1 byte; the side-specific source columns + the
  green-target rule guard it (the 337/342 lesson).
- **Two-big-sides is a gate** - measured, not assumed; the cut shape (spill vs reorient) follows the table.
- **Residual** - a dropped or key-only residual passes a naive synthetic but wrong-answers q21; the ADR 329 oracle is
  the gate.
- **q21-only** - q17/q12/q18/q04/q20 separate.

## Forward arc
q21 green or its named existence-set wall closes the distinctive wide-join case; the existence-set spill mechanism (if
that's the verdict) is reusable for other big-build joins. Remaining: q17 (build-side), q12/q18 (retained-input),
q04/q20 (spill-writer) - each scoped from its named wall, or the logical-completeness pivot.

## Why toron-home
Engine cut in `crates/toron` (exec.rs semi/anti join + spill + the 343 inner helper); driven via `toron-tpch` reach +
correctness at sf=1 / sf=0.01. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. q21-next as a **scoped** semi/anti extension (not generalize-by-momentum). Inner prefix
reuses 343's primitive while preserving the existing ADR 343 full-chain path; the new work is the semi/anti
existence-set handling + residual correctness. Operator's tightenings: Phase 0 is a **side-proving table** with
explicit side-specific evidence sources (sf=1 trace / synthetic isolated probe / sf=0.01 scaled / structural-only);
**structural-only sizes the next probe but cannot justify a green-target**; the "two big sides" is a measurement gate,
not a conclusion; the non-equi `match_filter` residual is load-bearing (ADR 329). Green-or-named-wall, strong prior
named.
