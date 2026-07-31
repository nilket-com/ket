# ADR 261 — TPC-H Reachability Harness

## Status

Proposed.

## Context

`plans/benchmarks/tpc_reachability.md` (the Benchmark Lighthouse) already names Toron's progress metric
— **Query Reachability**: *which TPC query, if any, can Toron run end-to-end against the published
answer set, on a multi-node Wendelon-backed cluster, byte-for-byte?* — and warns that linear metrics
(ADR count, kernel count) mislead, because composition multiplies and the long tail dominates. The
lighthouse names a destination but provides **no gauge**.

The 256–260 subquery arc kept edging toward a claim — "the standard query surface is broadly complete"
(260's forward arc). That has been an *assertion*. This ADR builds the **instrument** that converts it
into a *measured matrix*: a real external workload (TPC-H) that, per query, either reaches `execute` or
records exactly where reality stops. Its value is honest **red** rows, not green ones.

This is the first concrete rung on the lighthouse ladder. It is deliberately **not** "TPC-H 22" and
makes **no** claim that any official query passes.

## Decision — build a reachability *gauge*, not a benchmark

Add a repo-local helper package **`crates/toron-tpch`** (a bin crate; the `crates/*` workspace glob
includes it) that generates/loads a tiny TPC-H-shaped dataset, attempts a query through Toron's
single-node rail, and emits **structured phase/status telemetry** in which a *recognized* engine gap is
honest evidence, not a broken run. The first green result is *"the harness attempted Q6 and reported
exactly where reality stops,"* not *"Q6 is correct."*

`toron-tpch` depends on `toron`; the `toron` **library must not depend on `toron-tpch`** (a guard).
CLI parsing is manual (matching Toron's no-extra-framework style):

```
cargo run -p toron-tpch -- status                                   # 22-query inventory matrix
cargo run -p toron-tpch -- prepare --mode dev --sf 0.01             # generate + cache under target/tpch/
cargo run -p toron-tpch -- run --query q06 --mode dev --sf 0.01 --allow-blocked
```

### Two modes

- **dev** — Rust-native generation through `tpchgen` (Apache-2.0). Build the four **Q6-needed
  `LINEITEM` columns** into workspace-Arrow `RecordBatch`es locally: convert the `TPCHDecimal`
  money/discount/quantity fields to **`Float64`** (apply the scale) and convert `TPCHDate` to `Date32`;
  avoid the string columns entirely. Do **not** depend on `tpchgen-arrow` in this first cut:
  `tpchgen-arrow 2.0.2` currently depends on Arrow 57.1 while Toron uses Arrow 58.3.0, so its
  `RecordBatch` type is not the same type Toron accepts. Runs a **Toron-authored Q6 proxy** (below)
  single-node through `plan → bind → execute`. **dev `pass` = "the query planned, bound, executed, and
  produced a result digest"** — a *reachability* fact, **not** a TPC-H correctness claim (there is no
  official answer to compare against on synthetic data).
- **strict** — a skeleton that accepts **user-supplied** official artifacts later (`--schema`,
  `--query-dir`, `--answers-dir`, or equivalent). With no inputs, strict Q6 is `not_attempted`
  (`reason=strict_inputs_required`), **not** a fabricated engine blocker. With inputs, it records the
  **first real phase it reaches** — the ADR does **not** predeclare the blocker (grounding shows
  several candidate first stops: `CREATE TABLE … DECIMAL(…)` rejecting in frontend planning, Arrow
  `Decimal128` having no Toron scalar domain, or the official Q6 `DATE + INTERVAL` form hitting the
  narrow `Date32 +/- Interval` path). **strict `pass` = matched the official answer set** (the
  lighthouse bar). No DBGEN/QGEN vendoring; no auto-fetch.

### The Toron-authored Q6 dev proxy

```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate <  DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24.0
```

Two deliberate, visible departures from strict TPC-H: `Float64` columns/literals replace `DECIMAL`; the
upper date bound is a **literal date**, not `DATE '1994-01-01' + INTERVAL '1' YEAR` (Toron's
`Date32 ± Interval` is still narrow and must not be dragged into a harness ADR). Q6's shape — single
scan + date-range + `BETWEEN` + `SUM(arith)` — is already supported, so the dev proxy should reach
`execute`.

### Telemetry contract

Stable `phase` and `status` enums, one human row + one JSONL row per attempt:

```text
tpch_reachability query=q06 sf=0.01 mode=dev phase=execute status=pass reason=none elapsed_ms=14.2 digest=…
```
```json
{ "suite":"tpch","query":"q06","scale_factor":0.01,"mode":"dev",
  "phase":"execute","status":"pass","reason":null,"elapsed_ms":14.2,"digest":"…" }
```

- **phase**: `generate` · `load` · `plan` · `bind` · `execute` · `compare`. `fragment` is a **reserved**
  value for later *distributed* reachability — ADR 261 does **not** owe a distributed run; the
  single-node rail is `SqlPlanner::plan → bind(&catalog) → Executor::execute_bound`.
- **status**: `pass` · `blocked` · `fail` · `not_attempted`.

### The load-bearing invariant — `blocked` is green, but only with a recognized reason

`blocked` is permitted to exit 0 (under `--allow-blocked`) **only for a reason drawn from an explicit
allowlist**. Any error with a non-allowlisted reason, any panic, any malformed/missing row is a
**`fail`** (red) — even when `--allow-blocked` is set. This is the integrity property that stops
"blocked" from silently absorbing real regressions. Initial recognized categories (exact strings may
shift at impl): `strict_inputs_required`, `unsupported_decimal_ddl`, `unsupported_decimal_arrow`,
`unsupported_date_interval`, `unsupported_query_surface`. The ADR requires the allowlist **and** a guard
that an unrecognized error does not become `blocked`.

## Scope

- **In**: `crates/toron-tpch` bin crate (manual CLI); `status` listing `q01`–`q22`; dev-mode generation
  of the Q6-needed `LINEITEM` columns via `tpchgen` row structs, building workspace-Arrow arrays with
  `TPCHDecimal→Float64` and `TPCHDate→Date32` conversion; the Toron-authored Q6 proxy; single-node
  `plan/bind/execute`; human + JSONL output under `target/tpch/`; a strict-mode skeleton
  (user-supplied provenance, allowed to stop at `not_attempted`/a recognized blocker); the
  `--allow-blocked` flag + allowlist + honesty guard; the
  `plans/impl/261_tpch_reachability_harness_results.md` first-matrix evidence artifact.
- **Out / deferred**: any official-TPC claim; the other 21 query bodies; official DBGEN/QGEN vendoring
  or auto-fetch; **Decimal implementation** (the blocker 261 *measures*, not fixes); strict answer-set
  comparison beyond what user-supplied artifacts make trivial; distributed/`fragment`/Wendelon-backed
  runs; performance thresholds; a generic benchmark framework.

## Discriminators (each is the first-cut acceptance bar)

- `cargo run -p toron-tpch -- status` prints all 22 query IDs with explicit dev/strict state (mostly
  `not_attempted`).
- `… run --query q06 --mode dev --sf 0.01 --allow-blocked` exits 0 and emits exactly **one** structured
  Q6 row, preparing the dev cache on demand if needed; if it executes, `status=pass phase=execute` with
  a digest; if it blocks, the deepest phase + a recognized reason.
- `… run --query q06 --mode strict --allow-blocked` (no official inputs) exits 0 with
  `status=not_attempted reason=strict_inputs_required` — **not** a fabricated blocker.
- **Honesty gate**: a synthetic *unrecognized* engine error classifies as `status=fail`, **not**
  `blocked` (the test that locks the invariant — it must fail if "blocked" can swallow an unknown
  error).
- Only `target/tpch/` is written; no generated data or official tooling enters git.
- `toron` has no dependency edge to `toron-tpch`; the results doc states plainly that dev-mode output is
  **not** a TPC-H result.

## Architecture guards

- `adr261_tpch_helper_is_not_a_toron_library_dependency` — no `toron-tpch` edge from the `toron` lib.
- `adr261_no_official_tpc_tooling_in_tree` — no `dbgen`/`qgen`/official-TPC source; no auto-fetch step.
- `adr261_no_second_arrow_recordbatch_boundary` — the helper does not depend on `tpchgen-arrow` for
  dev data; it builds workspace-Arrow batches from `tpchgen` rows so Toron sees a single Arrow major.
- `adr261_blocked_requires_recognized_reason` — an allowlist exists and an unrecognized error maps to
  `fail`, not `blocked`.
- `adr261_stable_phase_status_telemetry` — `phase`/`status` enums (or string constants) are pinned;
  `fragment` is reserved, single-node is the bar.
- `adr261_cache_root_is_target_not_tracked` — output defaults to `target/tpch`, never a tracked path.

## Consensus (grounded 2-of-3 refinements, surfaced)

3-of-3 on shape (helper package, dev-`Float64`, hardcoded Q6 proxy, status inventory, no vendoring,
single-node). Two refinements promote the **grounded** position over Gemini's iter-1:
- **`blocked` needs a recognized reason; unknown errors are `fail`.** Gemini's "`blocked` = any Toron
  error" would let real bugs masquerade as expected gaps — the opposite of the harness's purpose.
- **Don't predeclare the strict blocker.** Gemini predeclared `phase=bind reason=JoinKeyTypeMismatch`,
  which is both an assertion the gauge exists to *measure* and factually wrong (Q6 has no joins). The
  harness reports the first stop; it does not guess it.

## Forward arc

261 builds the gauge; the **next** SQL/engine ADR is selected from the first honest blocker row, not
council prediction. Likely follow-ons in order of probability: **Decimal numeric types** (if strict
schema/data is the first stop) → `Date32`/Interval arithmetic polish (if official Q6 text is the next
stop after Decimal) → strict Q6 answer comparison (once user-supplied inputs are usable) → Q1 then Q3
reachability as composition deepens → distributed (`fragment`) / Wendelon-backed reachability after
single-node rows are stable → multi-node byte-exact publishable automation (the lighthouse
destination). The benchmark ADR does not replace the SQL-surface arc — it gives that arc a scoreboard.
