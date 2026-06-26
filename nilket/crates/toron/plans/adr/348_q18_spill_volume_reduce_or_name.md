# ADR 348 - q18 spill-volume: reduce or name the excess (no cap-bump)

## Status
Proposed.

## Context

Step 1 of finish-sf=1, the q18 closer (operator's lean: q18 before q04/q20). ADR 347 converted q18's unbounded
retained-memory problem into a **precise spill-volume budget wall**: `spill_budget_exhausted@execute`, producer
`streamed inner join build spill writer`, **537.6 MB spilled vs a 512 MB per-query budget - over by ~743 KB (0.14%)**.
This cut **measures the q18 spill by producer/page/column, classifies the reduction levers, reduces the excess without
raising the budget, and accepts q18 green or a named irreducible spill-volume wall with evidence.** It teaches whether
spill volume is **encoding/projection** (reducible) or **policy** (the budget) - before the data-proportional index
family (ADR 349, q04/q20).

## Grounded (347 + the code)
- Wall: `spill_bytes_reserved_per_query = 512 MB` (budget.rs:97); q18 spills 537,631,256 B via `SpilledInnerJoinBuild`
  (exec.rs:6533), which writes the **LeftSemi preserved-side rows to compressed IPC pages before probing the existence
  side**, then **dedups matched refs after lookup** (`read_batch_page`, 6984/7058).
- **Codex correction - dedup-before-spill is not a free local lever:** because the preserved rows are written *before*
  the probe knows which match, early dedup only reduces writer bytes if Phase 0 proves a real mechanism (two-pass
  membership filter, side flip, or narrowed projection). It is **not** a local tweak.

## Decision - measure, classify levers, reduce or name (no cap-bump)

### Phase 0 - lever classification (each: measured-reducible / measured-not-enough / not-applicable)
Measure the 537.6 MB spill (producer, page count, per-page bytes, columns spilled, rows pre/post-dedup, compression),
then classify the levers **in order**:
1. **Column projection (first)** - does the spill write **all** build columns when only **output-needed** columns
   matter? Pruning to output columns is the clearest local reduction. Classify: measured-reducible / not-enough /
   not-applicable.
2. **Page/layout (second)** - page size, encoding, compression codec/dictionary; is there reducible per-page overhead?
   Classify.
3. **Dedup-before-spill (only with a proven mechanism)** - not free (rows written before probe). Classify
   **not-applicable** unless Phase 0 proves a two-pass membership filter / side flip / narrowed projection that avoids
   writing pre-matched rows. If a mechanism exists, classify it measured-reducible / not-enough.

### The cut (per the classification)
- Apply the **measured-reducible** lever(s) (column projection first; page/layout if it adds enough; a proven dedup
  mechanism only if classified reducible) so the spill fits under the **unchanged** 512 MB budget -> q18 green.
- If all levers are measured-not-enough / not-applicable -> a **named irreducible spill-volume wall** (the 512 MB
  per-query budget is the honest sf=1 wall), with the lever-classification evidence. An honest result, not a failure.

## Acceptance (green or named irreducible, no cap-bump)
- **q18 sf=1: green** (`pass@execute` + `pass@compare`) via the measured-reducible lever(s) under the **unchanged**
  512 MB budget; **OR** a **named irreducible spill-volume wall** (332: producer + bytes) with the per-lever evidence
  (measured-reducible / not-enough / not-applicable).
- **No cap-bump** - `spill_bytes_reserved_per_query` is **not raised**; reduce or name.
- **Correctness:** column projection / any proven dedup must not change the answer - q18 `pass@compare` at sf=0.01;
  full `correctness-matrix` 22/22.
- **Scale wall is verified directly** - run q18 at sf=1 under `--release`; the default `matrix` is a regression guard,
  not proof that the q18 spill-volume wall moved.
- **Green cells + q07/q12/q14/q17/q19/q21 unchanged**; de-branded; bounded; no cost-model change.

## Guardrails
- **No cap-bump** - reduce or name; never raise the 512 MB spill budget (spill volume grows with data; a raised budget
  defers - the sf=10-durability point).
- **Dedup-before-spill is not free** - classify not-applicable unless a real mechanism is proven (no assumed local
  early-dedup).
- **Column projection first** - the clearest local reduction; page/layout second; dedup-mechanism only if proven.
- **Reduction preserves correctness** - pruned columns are not needed downstream; the sf=0.01 oracle gates it.
- **Provenance** - the spill breakdown + the per-lever classification is measured, not assumed.
- **Canonical scale command** - q18 sf=1 is measured with the release run command, not inferred from the default-scale
  matrix.

## Risks
- **Assumed early dedup** - the Codex trap; dedup is not free on this LeftSemi-preserved-before-probe shape; only a
  proven mechanism counts.
- **Column projection not enough** - q18 is 0.14% over; projection likely closes it, but if not, page/layout or a
  named wall - measured, not forced.
- **Cap-bump temptation** - the budget stays 512 MB; an irreducible result is named, not papered over.
- **q04/q20 not here** - ADR 349 (the index).

## Forward arc
348 (q18 spill-volume) + 349 (q04/q20 index, bound-not-cap) **complete sf=1 (9/9, durably)** -> step 2: the perf/
regression rail -> step 3: the sf=10 atlas -> step 4: Rune-dataframe TPC-H. Each a Toron-side ADR. The q18 verdict
(reducible vs policy) informs whether spill-volume is a per-cell or a budget-policy question as sf=10 stresses it.

## Why toron-home
Engine cut in `crates/toron` (exec.rs `SpilledInnerJoinBuild` projection/layout; possibly a dedup mechanism); driven
via `toron-tpch` reach + correctness at sf=1 / sf=0.01. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. q18 spill-volume, green-or-named-irreducible, no cap-bump. Codex's tightening folded:
**dedup-before-spill is not a free local lever** (preserved rows written before the probe; needs a proven two-pass /
side-flip / narrowed-projection mechanism), so Phase 0 **classifies each lever measured-reducible / measured-not-enough
/ not-applicable**, with **column projection first** and **page/layout second**. Reduce or name; never raise the spill
budget.
