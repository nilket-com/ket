# ADR 399 - post-397 q03 substrate re-profile + next precision strike

## Status
Proposed.

## Context

397 took Rune q03 sf=1 to ~410 ms (`Rune/Polars` 3.97x) via generic join projection pushdown. 398 confirmed q03 sf=10 is
**not a memory crack** - still CPU/data-movement bounded under 4 GB (peak 2.7 GiB, time/no-row) - so the sf=1 materialized
substrate loop is directly relevant to eventually pulling sf=10 into the completion window. **399 re-profiles post-397,
names the dominant Toron-owned producer, and strikes only that** - evidence-first, and explicitly **not "second verse of
397" by habit**: Phase 0 decides the producer and demands a cause-level identification before any cut.

## Decision

### Phase 0 - re-profile + confirm baseline
Re-profile Rune q03 **materialized sf=1** (395/397 symbol-gated loop: debug+no-strip, `readelf`/`nm` gate, off-by-default
execute loop, `perf record --call-graph dwarf` in `--scope`, DWARF-overhead verified). **Confirm baseline:** fingerprint
`666e0ac028036f70`, spill 0, materialized_path true, ~410 ms, `Rune/Polars` 3.97x - 397 persists.

### Producer-conditional cause-level analysis (before any strike)
Name the dominant Toron-owned producer, then - **per its class** - do the required cause work. A hot *frame* is a
symptom, never sufficient:
- **`build_in_memory_join_rows` / hashbrown entry path:** require a **cause-level ledger** - key encoding cost, hash
  count, insert/probe cardinality, resize/growth, duplicate-list behavior, memory-guard overhead. **"Hashbrown is hot"
  is not a target.**
- **Parquet / RLE / dictionary decode:** classify **irreducible decode vs Toron-caused redundant decode/materialization**
  (397-style; Polars ~108 ms is the fixable bound).
- **residual `memmove`/`memcpy`:** **trace the owning caller** before changing anything - the copy's Toron-owned
  producer, not the primitive.
- **Ledger only where the class needs it** - don't re-run the 397 scan ledger by habit if the class is the hash path.

### The stop-line - honest residual beats vague churn
**If Phase 0 cannot identify a Toron-owned producer with a behavior-neutral, narrow lever, 399 seals as a diagnostic:
the named residual (with the fixable-vs-Polars-floor split) and NO strike.** A named residual is a valid, preferred
outcome over a vague "optimize the hash table / decode / memmove" churn cut.

### The strike (only if the stop-line clears)
Strike the one cause-identified producer. **Behavior-neutral; generic; no q03 branding in `crates/toron/src`.** If it
touches `build_in_memory_join_rows`/`build_rows.rs`, the 396 parity set applies (Int32/Int64/generic keys, nulls, unique
+ duplicate, order); fold the deferred `BUILD_ROW_INLINE_OVERFLOW_CAP = 6` rationale comment if that surface is touched.

### sf=10 reach-signal probe (secondary, cheap)
After the strike (or diagnostic), a **cheap sf=10 q03 probe** - **not the primary metric**, a reach signal. Record status
(time/no-row vs newly-completing), **peak RSS, time/window**. sf=1 moving while sf=10 stays time/no-row is fine - the
point is the **over-time curve** as the substrate loop closes.

## Acceptance
- **Phase 0 re-profile + baseline confirm** (fingerprint `666e0ac...`, spill 0, materialized, ~410 ms) + the named
  dominant producer **with its class-appropriate cause-level analysis** (hash -> cause ledger; decode -> irreducible-vs-
  redundant; memmove -> owning caller).
- **Either** a strike on the cause-identified producer **or** a sealed diagnostic with the named residual (stop-line) -
  no vague churn cut.
- **If a strike:** q03 value gate (395) passes; **primary Rune sf=1 absolute median improves beyond the runs=7 band**
  (399 baseline ~410 ms); **secondary `Rune/Polars`**; the struck producer's profile share drops; behavior-neutral +
  shared-helper parity; no q03 branding.
- **sf=10 reach-signal recorded** (status + peak RSS + time/window) - over-time curve data, not a pass/fail gate.
- **No behavior change outside the materialized q03 family**; prior greens + sf=3 streaming path unchanged.
- **Full engine gate:** `--no-fail-fast` + `toron-tpch --no-fail-fast`; no cap bump; de-branded; instrumentation
  off-by-default; no committed `.perf.data`.

## Guardrails
- **Not "second verse of 397" by habit** - Phase 0 decides the producer; ledger only where the class needs it.
- **Cause, not symptom** - hash path needs the cause ledger; decode needs irreducible-vs-redundant; memmove needs the
  owning caller. A hot frame is never the target.
- **Stop-line** - no Toron-owned narrow behavior-neutral lever -> seal diagnostic with named residual, no strike.
- **Behavior-neutral, generic, not q03-shaped in core; shared-helper parity.**
- **Absolute median primary, ratio contextual; sf=10 probe secondary/cheap.**
- **Full engine gate; no cap bump; de-branded; off-by-default instrumentation.**

## Risks
- **Diffuse dominant / no narrow lever** - the stop-line: seal diagnostic with residual, don't force a churn cut.
- **Hash "hot" without a cause** - the cause-level ledger prevents a blind hash-table micro-opt.
- **Decode mostly irreducible** - named residual (Polars 108 ms bound); re-target.
- **Shared-helper regression** - 396 parity set + full gate.
- **Polars-noise false win** - absolute-median-primary.

## Forward Arc
399 strikes the third producer or names the residual; re-measure Rune absolute median (+ `Rune/Polars`, + the sf=10 reach
signal) - the over-time curve tracking whether the substrate loop pulls the heavy cells toward completion. Iterate until
Rune q03 approaches the ~2.5x Polars/DuckDB floor or the residual is named irreducible. Then the SQL/Rune ~2.4x
planner-shape lever; q08's `reoriented join build accumulator` wall (398) and the sf=3 streaming wall (393) as separate
threads.

## Grounding evidence (file:line)
397: `optimize_join_inputs_for_required` binder/pushdown.rs, Rune q03 sf=1 ~410 ms, `Rune/Polars` 3.97x, fingerprint
`666e0ac028036f70`, spill 0, sealed 2d83f49. 396: `BuildRowList` inline cap 6 (`exec/join/build_rows.rs`),
`build_in_memory_join_rows` join.rs:1345. 398: q03 sf=10 time/no-row, peak 2.7 GiB, CPU/data-movement bound; q19/q04
green control; q08 new named `reoriented join build accumulator` wall. profile 381; value gate + four-way 395; perf rail
356; Polars fixable bound ~108 ms sf=1.
