# ADR 397 - post-396 q03 substrate re-profile + next precision strike

## Status
Proposed.

## Context

396 landed the first substrate strike (Rune q03 sf=1 1339.75 -> 1135.07 ms, -15.3%; overflow heap 15.7 MB -> 0;
join-build -31%). The dominant Toron-owned cost has **shifted**. **397 re-profiles first, names the new dominant
producer, and only then strikes it** - same evidence-first discipline as 396. The 395 profile's next-in-line is the
Arrow/Parquet materialization boundary, but **that is a hypothesis, not the plan** - and the persisted scan path
**already plumbs `projected_columns` + `pbc_predicate`**, so "pushdown is missing" is a candidate to prove, not the
default fix.

**Key guardrail:** `memmove`/`OffsetBuffer`/`FilterBytes` are symptoms. The false move here is seeing them and assuming
"push down more" without proving **where the copy was introduced**. Polars decodes + materializes the same Parquet and
does q03 in ~108 ms, so Toron's slower materialization is *fixable, not an irreducible floor* - but 397 must prove the
Toron-owned redundancy, not presume it.

## Decision

### Phase 0 - re-profile + a scan-request/materialization ledger
- **Re-profile** the Rune q03 **materialized sf=1** path: symbols (`readelf`/`nm` gate), same persisted data, **plan
  fingerprint `2b72...`, spill 0, materialized_path true**, off-by-default execute loop, DWARF-overhead verified.
- **Confirm 396 persists:** overflow heap 0, join-build time still reduced - no regression.
- **Scan-request / materialization ledger (required):** the actual q03 Rune `ScanRequest` - **projected columns
  requested, `pbc_predicate`, rows before/after scan, batches emitted (counts/sizes)** - plus the **post-scan filter/
  take/concat sites** and the **dominant copy producer** (frames %, counts/bytes).
- **Name the new dominant Toron-owned producer** from that ledger. The ledger discriminates among:
  - missing/insufficient pushdown (projection/predicate not reaching the scan),
  - filter materializing variable-width columns *after* scan,
  - unnecessary batch concat/rebatch,
  - join/probe/output materialization,
  - or mostly irreducible Parquet decode.

### The cut - strike the evidenced Toron-owned producer
Only after the ledger names it, and only the Toron-caused redundancy - not the Arrow/Parquet primitive, not irreducible
decode. Keep it narrow (a specific redundant copy / a pushdown the ledger proves absent / a removed rebatch), not a scan
rewrite. **Behavior-neutral; shared-helper/generic; no q03 branding in `crates/toron/src`.**

### Irreducible vs fixable
If the ledger shows the dominant cost is **mostly unavoidable decode**, 397 **names the residual** (with Polars' ~108 ms
as the fixable bound: the gap above that is Toron-owned) and re-targets - a named residual is a valid outcome, not a
forced micro-strike.

## Acceptance
- **Phase 0 re-profile + the scan-request/materialization ledger recorded:** scan columns requested, `pbc_predicate`,
  rows emitted, batches emitted (counts/sizes), post-scan filter/take/concat sites, and the dominant copy producer -
  with the 396-persistence check (overflow heap 0, build time reduced, fingerprint `2b72...`, spill 0).
- **The new dominant Toron-owned producer named** and classed (missing-pushdown / post-scan var-width filter / concat-
  rebatch / join-probe-output / irreducible decode) - not "push down more" presumed.
- **Strike behavior-neutral + shared-helper/generic** - no q03 branch; parity across the touched surface.
- **q03 oracle/value gate (395) still passes.**
- **Primary: Rune q03 absolute median improves materially beyond the runs=7 stability band** (396 baseline 1135.07 ms);
  **secondary: `Rune/Polars`** (contextual). Within-noise -> not banked; name the residual + the fixable-vs-irreducible
  split.
- **The struck producer's share drops on re-profile.**
- **No behavior change outside the materialized q03 family**; prior greens + sf=3 streaming path unchanged.
- **Full engine gate:** `--no-fail-fast` + `toron-tpch --no-fail-fast`; no cap bump; de-branded; instrumentation
  off-by-default; no committed `.perf.data`.
- If 397 touches the join-build surface, fold the deferred `BUILD_ROW_INLINE_OVERFLOW_CAP = 6` rationale comment.

## Guardrails
- **Re-profile before striking; ledger before target** - name the post-396 dominant from the scan-request/materialization
  ledger, don't guess or presume pushdown-missing.
- **Symptoms vs producer** - strike the Toron-owned redundant materialization, not `memmove`/Arrow/Parquet primitives;
  separate irreducible decode from fixable via the Polars 108 ms bound.
- **Behavior-neutral, generic, not q03-shaped in core.**
- **Absolute median primary, ratio contextual** - don't bank within-noise; name irreducible residual honestly.
- **Full engine gate; no cap bump; de-branded; off-by-default instrumentation.**

## Risks
- **Presuming pushdown-missing** - the ledger proves it present/absent before any pushdown change.
- **Dominant is mostly irreducible decode** - name the fixable-vs-irreducible split (Polars 108 ms bound), re-target;
  still progress.
- **Materialization strike too broad** - a specific redundant copy / rebatch / proven-absent pushdown, not a scan rewrite.
- **Shared-helper regression** - parity tests + full gate.
- **Polars-noise false win** - absolute-median-primary.

## Forward Arc
397 strikes the second evidenced producer (or names the irreducible residual); re-measure Rune absolute median (+
`Rune/Polars`). Iterate profile -> ledger -> strike until Rune q03 approaches the ~2.5x Polars/DuckDB floor or the
residual is named irreducible. Then the SQL/Rune ~2.4x planner-shape lever, and the sf=3 streaming-substrate wall (393)
as the separate large thread.

## Grounding evidence (file:line)
396: Rune q03 sf=1 1135.07 ms (-15.3%), overflow heap 0, join-build -31%, `Rune/Polars` 10.16x, sealed 2b7be08;
`BuildRowList` inline cap 6 (`exec/join/build_rows.rs`). 395: alloc/copy-dominated (`memmove` 14.5%, `OffsetBuffer::
extend_from_dictionary`, `FilterBytes::extend_idx`); fingerprint `2b72...`, spill 0, materialized. persisted scan
`ScanRequest` plumbs `projected_columns` + `pbc_predicate` (toron-tpch harness scan). Polars q03 sf=1 ~108 ms (fixable
bound). profile method 381; perf rail 356; value gate 395.
