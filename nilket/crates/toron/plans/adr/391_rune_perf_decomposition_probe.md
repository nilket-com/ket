# ADR 391 - Rune perf-decomposition probe (q03, logical-shape vs substrate)

## Status
Proposed.

## Context

The fear: can we narrow the sf=10 time gap by **better plans**, or are we up against the **execution substrate**? Rune
is the instrument - but with a grounded correction (Codex review): **Rune does not bypass the binder/optimizer.**
`RuneEngine::execute_script` returns a `LogicalPlan` (rune_engine.rs:1705); the harness still runs `bind(...)`
(binder.rs:206) and the same executor. Rune bypasses **SQL parsing/lowering** and lets us **hand-author/pin the logical
join shape** (join order, build side, exchanges, filter placement via `JoinStrategyPinSource::Rune`). So the probe
isolates **SQL-produced logical shape vs Rune-authored logical shape**, both bound the same way, on the same executor -
**not** "planner CPU vs substrate." **391 is diagnostic-only.** q03 canary (clean 3-table; hand-tunable without testing
query-writing mistakes).

## Decision

### The three rows + the (corrected) ratio split
| path | what it measures |
| --- | --- |
| **Toron SQL** | SQL-produced logical shape -> bind -> executor |
| **Rune hand-authored** | hand-authored logical shape (order/build-side/pins) -> **same** bind -> **same** executor |
| **DuckDB** | external reference |

- **SQL / Rune = logical-shape penalty** - does the SQL-produced logical shape (join order etc.) cost more to **execute**
  than a hand-authored one? (NOT planner CPU time.)
- **Rune / DuckDB = execution-substrate cost** - materialize-each + Arrow, given the best shape.
- **SQL / DuckDB = end-to-end gap.**
- Secondary telemetry: `plan_ms` / `bind_ms` if planner/binder overhead is of interest (not the headline).

**Discriminator:** Rune q03 much faster than SQL q03 -> **logical shape (join order) is the lever**. Rune q03 also slow
-> **executor substrate** (Arrow/materialize/spill) is the ceiling.

### Phase 0 - isolation proof (load-bearing; the probe is void without it)
Bind SQL q03 and Rune q03 at small scale and **record physical plan fingerprints/shapes**. **Prove Rune changes the
intended things** (join order / build side / exchanges / filter placement). **If the bound physical plan is the same as
SQL, the probe cannot answer the planning-vs-substrate question** - stop and re-author the Rune shape until the bound
plans differ as intended.

### Correctness - oracle/order, not just digest
The perf full-row canonical digest may not prove `ORDER BY`. Require the **q03 oracle/order comparator** for Rune at
sf=0.01/sf=1 (and persisted sf=10 if Rune completes) - the Rune result must equal q03's answer **including order**, so
we never compare a faster-because-wrong shape.

### Censored SQL - lower bounds, no fake ratio
SQL q03 is time/no-row at sf=10 (388/390). If it stays censored, **ratios involving SQL are lower bounds**
(`SQL/Rune > timeout_ms / Rune_ms`) - record them as bounds, never a fake exact ratio.

### Real raw rows
Add **`engine="rune"`** raw perf rows alongside `toron`/`duckdb`. Put the **SQL/Rune/DuckDB ratios in the
summary/results artifact**, not as fabricated raw run rows.

## Acceptance (diagnostic-only)
- **Phase 0 isolation proof recorded** - SQL vs Rune bound physical plan fingerprints differ as intended (order/build/
  exchange/filter); if identical, the probe is void and re-authored.
- **Three rows on the same sf=10 persisted data**, same fixed 4 GB, same perf rail, `engine="rune"` row + DuckDB ratio.
- **Ratio split with the corrected labels:** `SQL/Rune = logical-shape penalty`, `Rune/DuckDB = substrate cost`,
  `SQL/DuckDB = end-to-end`; SQL censored -> lower bounds, not fake exact.
- **Correctness:** Rune q03 == q03 oracle **including order** (sf=0.01/sf=1; persisted sf=10 if Rune completes) - not
  digest-only.
- **Symbol-retaining profile of the Rune path** if Rune is still slow (373/381 method) - the substrate-cost attribution.
- **Named verdict:** logical-shape-dominated / substrate-dominated / both (quantified), with the caveat that "planner
  CPU" is out of scope unless a true hand-built physical-plan row is later added.
- **No engine optimization bundled; harness-only (`crates/toron-tpch`)**; `crates/toron/src` unchanged; `--no-fail-fast`
  unchanged; de-branded; no qNN; large `.perf.data` uncommitted.

## Guardrails
- **The probe is void without the isolation proof** - bound plans must differ as intended; else re-author.
- **Label it logical-shape, not planner CPU** - Rune still binds; only SQL parsing/lowering is bypassed.
- **Oracle/order, not digest** - never compare a faster-because-wrong shape.
- **Censored SQL -> bounds, not fake ratios.**
- **Diagnostic-only; engine=rune raw rows real, ratios in the artifact; no engine change; --no-fail-fast; no qNN.**

## Risks
- **Bound plans identical (probe void)** - the isolation proof catches it; re-author the Rune shape (pins/order).
- **Mislabeled as planner cost** - corrected to logical-shape penalty; planner CPU is `plan_ms` secondary telemetry.
- **Faster-because-wrong shape** - oracle/order comparator gates it.
- **SQL censored** - lower-bound ratios, honest.
- **Rune also slow** - substrate ceiling; profile it; the honest materialize-each read.

## Forward Arc
391 quantifies q03's **logical-shape vs substrate** split. Then:
- **Logical-shape-dominated** -> a **join-order / logical-plan** cut (the SQL planner produces a costlier-to-execute
  shape; likely generalizes across the time cluster). A later ADR could add a true hand-built **physical**-plan row to
  separate planner CPU from logical shape if needed.
- **Substrate-dominated** -> a 385-style **execution precision-strike** on the named Arrow/materialize/spill frame, and
  the honest read on the materialize-each ceiling.
- Either way: a *measured* answer to "can we narrow the time gap, and which lever moves it." Extend to q07/the cluster
  once the q03 instrument is proven.

## Grounding evidence (file:line)
Rune returns LogicalPlan (not physical): `RuneEngine::execute_script` rune_engine.rs:1705; harness still binds:
`bind(...)` binder.rs:206, SQL perf path plans+binds lib.rs:2894; Rune can pin join strategy/build-side
`JoinStrategyPinSource::Rune`. gauge: no Rune perf entry. q03 = customer/orders/lineitem + group (`Q3_DEV_SQL`). perf
rail (356), symbol-retaining profile (381). q03 DuckDB=269ms; SQL q03 time/no-row at sf=10.
