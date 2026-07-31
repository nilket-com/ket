# Implementation Plan — ADR 267: Multiple same-function aggregate projection

## Summary
Naming-layer fix in two agreeing sites: give each distinct-signature aggregate entry a unique, internal,
stable output name (`__agg_<i>`, keyed on aggregate position) in **both** `AggregateOutputCatalog`
(`sql_planner.rs`) and the binder's `aggregate_schema` (`binder/aggregate.rs`), and have all three resolvers
(`resolve_natural` → projection + HAVING, `resolve_for_order_by`) return it. Removes the
natural-name-collision `Ambiguous` bail so multiple same-function aggregates (q01's four SUMs + three AVGs)
resolve. No exec/IR change (values unchanged; no IR naming field).

> **Reconciliation vs the promoted plan:** the plan said "frontend-only; no binder change," resting on the
> premise that the binder already synthesizes unique downstream names. That is true only on ADR 240's
> distinct-rewrite path; the general grouped-aggregate path re-derives field names in `aggregate_schema`
> from the natural name and re-collides. The frontend rename alone left q01 failing with
> `UnknownColumn { "__agg_0" }`, so Phase 1b extends the identical disambiguation into the binder. See the
> results doc.

## Phase 1 — synthesize a unique internal name per entry (`crates/toron/src/sql_planner.rs`)
- In `add_requirement` (`:7005`), alongside `natural_name`, compute a **unique internal** name for the
  entry — deterministic from the entry's position among same-natural-name entries (e.g. `sum`, then a
  stable internal form for the 2nd+ such as `__agg_sum_1`). Store it on `AggregateOutputEntry`.
- `natural_output_names` (`:7060`) → emit the unique name per aggregate entry (group keys unchanged) so
  same-function aggregate entries no longer duplicate each other. Do not fold in a broader group-key/name
  collision policy; those residual clashes stay fenced by the existing ambiguity path.
- Internal-name shape is unmistakably internal (e.g. `__agg_` prefix for the disambiguated 2nd+), so it
  can't be confused with a user column and architecture guards can pin it.

## Phase 1b — binder re-derives the same names (`crates/toron/src/binder/aggregate.rs`)
- `aggregate_schema` (`:235`) builds the `Aggregate` node's output fields via `aggregate_field` per call,
  whose name is the natural name (`SUM→"sum"`). Two same-function aggregates therefore produce two `"sum"`
  fields here independently of the frontend. Count natural-name collisions across `agg_fields` and rename
  the colliding ones to `__agg_<index>` (`field.with_name(...)`), where `index` is the call's position in
  `aggr_expr` — **the same key the frontend uses**, because `aggr_expr` is built from the catalog entries in
  order. This is the load-bearing piece the original "frontend-only" framing missed.

## Phase 2 — resolvers return the unique name (remove the collision bail)
- `resolve_natural` (`:7083`, projection + HAVING): find the entry by signature (unchanged) and return
  `Found(<unique name>)` instead of bailing `Ambiguous` on a shared natural aggregate name.
  `entry_for_signature` already keys by full signature, so distinct aggregates never collide;
  `Ambiguous` remains only for a genuine residual (e.g. a group key vs aggregate output-name clash), if
  any — keep that arm.
- `resolve_for_order_by` (`:7099`): for an aggregate-call ORDER BY, also return the entry's unique name by
  signature. Do not continue to route unaliased same-function calls through `order_by_visible_names`: in
  `SELECT SUM(a), SUM(b) ... ORDER BY SUM(b)`, both entries currently record the visible name `"sum"`, so
  resolving through the visible name can silently sort by the first aggregate. The signature identifies
  the intended entry; the unique internal name identifies its physical output.
- The projection rewrite (`rewrite_aggregate_calls_for_projection`, `:4870`) now resolves each call to its
  unique output reference.

## Phase 3 — final projection labels (the operator watch-item)
- User-visible labels are unchanged: a SELECT alias names the final column (the internal name never
  surfaces); a single un-aliased aggregate keeps its natural name (`sum`). Only genuinely-ambiguous
  **un-aliased** same-function aggregates take the deterministic internal name as their final label.
  Confirm the alias path overrides at the SELECT `Project` assembly; adjust un-aliased multi-aggregate
  final naming only if it would otherwise emit duplicate output names.

## Tests (`crates/toron/tests/aggregate_projection_accounting_test.rs` + toron-tpch gauge)
- **Aliased**: `SELECT l_returnflag, SUM(l_quantity) AS q, SUM(l_extendedprice) AS e FROM lineitem
  GROUP BY l_returnflag` plans/binds/executes; output columns are `q`/`e` (no internal name leak).
- **Un-aliased**: `SELECT l_returnflag, SUM(l_quantity), SUM(l_extendedprice) FROM lineitem GROUP BY
  l_returnflag` plans/binds/executes with two distinct, correct SUM columns.
- **ORDER BY / HAVING by same-function aggregate** (the fix claims these resolver paths): e.g.
  `… GROUP BY k ORDER BY SUM(b)` and `… GROUP BY k HAVING SUM(a) > 0` with sibling same-function
  aggregates present. The ORDER BY case must assert row order/data, not merely "no `Ambiguous`", so it
  proves `SUM(b)` resolved to the second aggregate's unique output rather than the first `"sum"` label.
- **Gauge no-regression**: `q01_attempts_and_blocks_at_aggregate_projection` flips to
  `q01_advances_to_avg_decimal` — `run_strict_q1` is now `blocked(unsupported_decimal_avg@bind)` (the
  decimal `+`/`-` from 266 is exercised); dev q01 likewise advances past the plan wall.
- The existing `malformed_aggregate_arity_or_modifiers_reject_cleanly` + projection-accounting guards
  still pass (don't loosen genuine rejections).

## Verification gate (ADR 264 seal gate; cargo via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt  -p toron -p toron-tpch -- --check
scripts/cargo-cgroup test -p toron --test architecture_test          # FULL — source-scanning guards
scripts/cargo-cgroup test -p toron --no-fail-fast                    # the WHOLE integration suite
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron-tpch                              # gauge + the q01 advance test
scripts/cargo-cgroup run  -p toron-tpch -- run --query q01 --mode strict --allow-blocked
scripts/cargo-cgroup run  -p toron-tpch -- matrix
git diff --check
```
Acceptance: aliased + un-aliased multi-same-function aggregates plan/bind/execute; ORDER-BY/HAVING cases
resolve; q01 advances to `unsupported_decimal_avg@bind`; q06 unchanged; full suite green.

## Seal
`impl: adr 267 multiple same function aggregate projection`,
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes
- **Internal, not observable.** The disambiguated name must never be a user-visible label when an alias
  exists; the aliased-output test is the guard.
- **Don't loosen genuine rejections.** Keep `ResolveOutcome::Ambiguous` for any real residual clash; the
  fix removes only the same-function-distinct-signature false positive.
- **Naming-layer once.** Fix the output naming, not three ad-hoc resolver patches — all three read the
  same unique name (avoids a twin gap).
