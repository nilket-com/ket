# ADR 363 - inner-build spill projection (clear q21's spill-volume wall)

## Context

The q21 closer. ADR 362 advanced q21 to a named `spill_budget_exhausted`: the **streamed inner join build spill
writer** spills 536,872,016 B vs the 512 MiB budget - **1,104 bytes over (0.0002%)**. Same family as ADR 348 (q18's
spill-volume). The 348 column-projection lever already exists (`SpilledInnerJoinBuild::write_projected_batch_chunks`,
spill.rs:474, used at streaming.rs:2285 for the LeftSemi side). This cut **measures the q21 spill by producer/page/
column** and, if the streamed inner build is writing dead columns, **routes it through the existing projected spill
writer** - q21 green or a named irreducible spill-volume wall, under the **unchanged 512 MiB budget. No cap bump.**

## Decision

### Phase 0 (load-bearing) - prove whether the spill writes dead columns
Measure q21's 536.87 MB spill by **producer / page / column** (env-gated trace): producer (the streamed inner join
build spill writer), page count, per-page bytes, **columns written** (all build columns vs output-needed = group/agg/
join-key), rows, compression. The decisive question: **does the streamed inner-build spill write columns the downstream
doesn't need?**
- Record which `write_streaming_scan_inner_build_batches` branch produces the spill: the exchange/repartition branch or
  the direct scan branch. If the implementation handles only the measured branch, say so; if it handles both, prove both
  use the same projected writer safely.
- **Yes (dead columns):** column projection is measured-reducible -> route q21's build through
  `write_projected_batch_chunks` (the existing 348 mechanism), spilling only retained columns.
- **No (already minimal):** classify the 1,104 B as **page/layout** (measured-reducible only if a real per-page lever)
  or **irreducible** - a named spill-volume wall with the per-lever evidence (no forced green).

Three-tier lever classification (348): column projection first; page/layout second; dedup-before-spill **not-applicable**
(348's caution for the build-before-probe shape) unless a proven mechanism.

### The cut
Apply the measured-reducible lever - expected: route the streamed inner build through `write_projected_batch_chunks` so
only output-needed columns spill - bringing the q21 spill under the unchanged 512 MiB budget. Reuse the existing
projected-write mechanism; no new inline spill path. If Phase 0 proves no dead columns and no reducible page/layout,
the deliverable is the **named irreducible wall** + evidence.

Projection is more than a retained-column list: remap build-key ordinals to the projected schema, construct the
`JoinKeyPlan` against that projected schema, and restore the original observable build/probe schema and row/column order
when materializing matches. This is load-bearing because the existing projected writer is already proven for one
semi/aggregate path; ADR 363 must prove the reoriented inner-build runtime remains schema-compatible too.

### Correctness scope (the 362 wording)
The `correctness` command is **strict-oracle only**:
- **Strict:** q21 `pass@compare` at sf=0.01 and sf=1 stays green.
- **Dev (Float64):** a **focused parity test** - the projected-spill path output == the un-projected path output in dev
  mode - **not** `correctness --mode dev`.

## Acceptance
- q21 sf=10 (fixed 4 GB): **green** (`pass@execute`) via spill-volume reduction under the **unchanged 512 MiB budget**;
  **OR** a named irreducible spill-volume wall (332) with lever-classification evidence.
- **No cap bump** - `spill_bytes_reserved_per_query` (512 MiB) unchanged.
- **Correctness:** strict `pass@compare` (sf=0.01/sf=1) + the dev parity test for the projected path.
- **Projected inner-build schema parity:** projected spilled pages remap keys correctly and materialize the same
  full-schema rows/column order as the un-projected inner-build spill path.
- **Seal gate includes `scripts/cargo-cgroup test -p toron --no-fail-fast`** (engine cut - ADR 361 lesson).
- **De-branded core** (topology-named; no q21/qNN in `crates/toron/src`); behavior-neutral at sf=1; q14/q18 + green
  cells unchanged.

## Guardrails
- **No cap-bump** - 512 MiB unchanged; reduce or name irreducible.
- **Measure by producer/page/column first** - prove dead columns before routing; no implementing off the generic
  producer.
- **Reuse the projected-write mechanism** - `write_projected_batch_chunks`, no new inline spill path.
- **Three-tier levers** - column projection first; page/layout; dedup not-applicable unless proven.
- **Correctness scoped** - strict via the command, dev via a focused parity test (not `correctness --mode dev`).
- **--no-fail-fast in the seal gate** - mandatory for this engine cut.
- **De-branded core; behavior-neutral at sf=1.**

## Risks
- **No dead columns** - then the 1,104 B is page/layout or irreducible; classify with evidence, don't force green.
- **Projection correctness** - the projected spill must yield the same matches/answer (strict + dev parity gates).
- **Cap-bump temptation** - rejected; the budget stays 512 MiB.
- **Skipping --no-fail-fast** - the 361 trap; the seal gate includes it.

## Forward arc
363 (q21 green or named irreducible) -> **359-redux** (the broader sf=10 atlas on the persisted surface: Toron absolute
+ regression bands + DuckDB; the streaming substrate's constant-memory verdict; the deeper materialize-each / exchange
terms - ADR 349) -> sf=10 fix arc -> sf=100 / Rune.
