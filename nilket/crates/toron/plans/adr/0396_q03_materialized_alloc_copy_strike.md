# ADR 396 - q03 materialized allocation/copy precision strike

## Status
Proposed.

## Context

395 named the substrate cost: Rune q03 sf=1 materialized is **allocation/copy/materialization-dominated**, 12.41x slower
than Polars, correct + stable + spill 0. Profile top: `memmove` (~14.5%), Parquet dictionary/string materialization,
Arrow filter, and the highest Toron-owned site, join-build insertion. **396 is the first substrate strike: a narrow cut
on the measured dominant Toron-owned churn - not a broad rewrite, not q03-shaped in core.**

**The join-build hypothesis is de-weighted, not assumed (Codex grounding).** `build_in_memory_join_rows` already
`HashMap::with_capacity(rows)`; `BuildRowList` stores the first row **inline** and allocates a `Vec` **only on duplicate
keys**. q03 cardinality is **plan-dependent**: customer/order build sides are unique-key, but a lineitem build on
`l_orderkey` can be duplicate-heavy. So the `Vec`-realloc theory cannot be inferred from the SQL text; Phase 0 records
the actual build side/cardinality. If the hot build is mostly unique, the overflow theory is falsified, but HashMap
insert/key hashing or the memory guard may still be a join-build target. **Phase 0 evidence selects the target; the
strike is not chosen a priori.**

**Key guardrail (operator):** `memmove`/`malloc`/`realloc` are symptoms; strike the **Toron-owned producer**, not the
copy primitive.

## Decision

### Phase 0 - cardinality + per-stage allocation evidence (before selecting the target)
Temporary/env-gated instrumentation over the q03 materialized stages, attributed to the Toron-owned caller:
- **Join-stage stats (required before any join-build strike):** stage label / build-side lineage if available, key plan
  (Int32/Int64/generic), build rows, **unique keys, duplicate keys, overflow rows, max rows/key**, and **which join stage
  is hot**. If q03's hot build is mostly unique keys, the `Vec`-realloc theory is falsified; only HashMap/key/guard work
  remains a join-build candidate.
- **Per-stage allocation counts + bytes:** scan/filter output (Parquet decode -> Arrow filter -> batch), join build
  (HashMap growth + BuildRowList), join probe/output, aggregate/order (`execute_bound -> Vec` rebatch).
- Name the dominant Toron-owned producer (counts, bytes, %). `memmove` alone does not name it.

### The cut - strike only the evidenced dominant producer
Target selected by Phase 0, not assumed:
- **If join-build churn is confirmed**, choose the measured sub-cause:
  - duplicate-key overflow -> **duplicate-aware overflow sizing** or a **flat row-index arena preserving per-key order**
    - more specific than "reserve capacity" (already present);
  - mostly-unique HashMap/key work -> name it as hash-table/key construction cost, and only strike it if there is a
    narrow, behavior-neutral lever;
  - per-row `join_build_memory_fits` recomputation -> lift to O(1) incremental if it is real cost.
- **If Phase 0 points at Arrow filter / Parquet materialization** (the likely case for unique-key q03): strike that
  boundary (reduce redundant decode/filter/dictionary rematerialization / copies) instead.
- **If the aggregate/order rebatch dominates:** strike the `execute_bound -> Vec` rebatch copy.
Behavior-neutral: the join/aggregate result is identical (the 395 value gate must still pass).

### Shared-helper discipline - not q03-shaped in core
If the strike touches `build_in_memory_join_rows` / `insert_join_build_row` (shared join helpers), acceptance requires
**behavior-neutral parity across Int32/Int64/generic keys, nulls, unique and duplicate keys, and output-order** - no
q03-specific branch in `crates/toron/src`.

### Measurement guard - absolute Rune median, ratio as context
Polars was noisy at sf=1, so the **primary success metric is Rune's absolute execute_ms median improving beyond the
stability band** (runs=7/warmup=1), with **`Rune/Polars` as the contextual ratio**. **Do not bank a tiny ratio
movement**; a within-noise change is not a win - name the residual and re-target.

## Acceptance
- **Phase 0 evidence recorded** - join-stage cardinality (unique/duplicate/overflow/max-per-key/hot-stage) + per-stage
  allocation counts/bytes; the dominant Toron-owned producer named, not `memmove` generically.
- **Rune q03 value-correct at sf=1** - the 395 reproducible value gate (oracle/order/decimal tolerance) still passes.
- **Primary: Rune absolute median improves beyond the stability band**; secondary: `Rune/Polars` reported as context. A
  within-noise change is not banked - name the residual instead.
- **The struck site's allocation/copy drops** - re-profile (381 method) shows the producer's share fell.
- **Shared-helper parity** (if a shared join helper is touched): Int32/Int64/generic keys, nulls, unique + duplicate
  keys, output-order - behavior-neutral; no q03-specific core path.
- **No behavior change outside the materialized q03 family** - prior greens (q04/q06/q14/q19) + carried unchanged; sf=3
  streaming path unchanged.
- **Full engine gate:** `--no-fail-fast` + `toron-tpch --no-fail-fast`; no cap bump; de-branded (no qNN in
  `crates/toron/src`); instrumentation temporary/off-by-default; no committed `.perf.data`.

## Guardrails
- **Evidence selects the target** - cardinality/overflow + per-stage bytes before the strike; the join-build is not
  assumed (inline-first, pre-sized, unique-key q03).
- **memmove is a symptom** - strike the Toron-owned producer.
- **Narrow strike, not a rewrite; behavior-neutral** (value gate + shared-helper parity).
- **Not q03-shaped in core** - shared helpers stay generic; parity across key plans/nulls/duplicates/order.
- **Absolute median primary, ratio contextual** - don't bank within-noise movement.
- **Full engine gate; no cap bump; de-branded; off-by-default instrumentation.**

## Risks
- **Join-build theory falsified** (unique keys) - Phase 0 redirects to Arrow/Parquet materialization or the rebatch; no
  wasted join rewrite.
- **Struck site too small to move the median** - name the real dominant cost, re-target; still progress (named).
- **Shared-helper regression** - parity tests across key plans/nulls/duplicates/order + full engine gate.
- **Instrumentation skews the profile** - temporary/off-by-default; re-profile clean after.
- **Polars-noise false win** - absolute-median-primary guard.

## Forward Arc
396 strikes the first evidenced substrate producer; re-measure Rune's absolute median (+ `Rune/Polars` context) to track
the 12.41x closing toward the ~2.5x Polars/DuckDB floor. Iterate: next dominant producer -> next strike, until Rune q03
approaches peer-Arrow class. Then the SQL/Rune ~2.4x planner-shape lever, and the sf=3 streaming-substrate wall (393) as
the separate large thread.

## Grounding evidence (file:line)
395: Rune q03 sf=1 = 1339.8 ms, spill 0, materialized, 12.41x Polars; alloc/copy-dominated (`memmove` 14.5%, Parquet
dict/string materialization, join-build insertion). join build: `build_in_memory_join_rows` join.rs:1345
(`HashMap::with_capacity(rows)`, row-by-row), `insert_join_build_row` 1548, `BuildRowList` inline-first (Vec on duplicate
keys only), `join_build_memory_fits` per row. value gate + four-way + profile: ADR 395; symbol-retaining profile 381;
perf rail 356.
