# ADR 263 — Results: Decimal `ScalarValue` (Q6 Slice)

## Headline

The gauge moved. Strict TPC-H Q6 (official-typed `Decimal128(15,2)` fixture + Q6 shape) advanced from
`blocked(unsupported_decimal_scalar@bind)` to **`pass@execute`** — the acceptance condition of the cut.

## Gauge — before / after

`scripts/cargo-cgroup run -p toron-tpch -- run --query q06 --mode strict --allow-blocked`

| when | phase | status | reason |
|------|-------|--------|--------|
| before (ADR 262 measured) | `bind` | `blocked` | `unsupported_decimal_scalar` |
| after (ADR 263) | `execute` | `pass` | `none` |

After (raw telemetry):

```
tpch_reachability query=q06 sf=0.01 mode=strict phase=execute status=pass reason=none elapsed_ms=14.3 \
  digest=ArrowIpcDigest([130, 66, 124, 232, 7, 97, 202, 5, 171, 18, 53, 142, 166, 186, 76, 113, \
  190, 246, 136, 123, 249, 109, 69, 143, 4, 164, 85, 198, 195, 160, 101, 173])
```

Matrix board cell movement (`scripts/cargo-cgroup run -p toron-tpch -- matrix`):

```
q06 dev=pass@execute strict=pass@execute
```

(`dev` was already `pass@execute` pre-263; the cut moved the `strict` cell.) The digest reflects the
fixture's computed revenue — a stable reachability digest, NOT a TPC-H answer (see scope below).

## What the slice actually did (and what it did NOT need)

`ScalarValue::Decimal128 { value: Option<i128>, precision: u8, scale: i8 }` plus three executor edges:

- **Comparison coercion (no `Cast` node).** The binder's `coerce_literal_value` rewrites the Q6
  comparison literals in place to `Decimal128(15,2)` (`l_discount BETWEEN 0.05 AND 0.07`,
  `l_quantity < 24`), so both sides match Arrow's exact-`(p,s)` cmp requirement at bind. **The budgeted
  Phase 3a `evaluate_cast` Decimal arms were not needed** — no `Cast` Expr reaches the executor on the Q6
  path, so adding them would have been dead code. (Honest tightening vs. the plan, which treated
  `evaluate_cast` Decimal cases as load-bearing.) A future query that emits a real Decimal `Cast`
  (non-literal operands of mixed `(p,s)`) will nominate that work.
- **Decimal `*`.** `evaluate_bound_binary` now routes `Mul` over two `Decimal128` operands to Arrow
  `numeric::mul`, branching around `evaluate_arithmetic`'s operand-equals-output assertion; the binder
  declares the matching widened type `Decimal128(p1+p2+1 ⊓ 38, s1+s2)` = `Decimal128(31,4)` for
  `(15,2)*(15,2)`.
- **Decimal `SUM` (global and grouped).** No new `AggState` variant. `numeric_value` yields `Int(i128)`
  for a `Decimal128` column, the existing i128 `SumInt` accumulator carries the raw running sum, and both
  finalizes — `sum_state_array` (global) and `sum_column_from_groups` (grouped) — rebuild a
  `Decimal128(p,s)` from the bound output type. (Tightening vs. the plan's "add a Decimal accumulator or
  Arrow-backed sum" — reuse was smaller and the bound output type already carries `(p,s)`.)

## Known simplification (owed to a later cut)

- **`f64 → Decimal` literal rounding.** The frontend still lowers decimal literals to `Float64`; the
  binder converts them to `Decimal128(p,s)` (`decimal_literal_from_f64`). This is a reachability
  simplification, not exact-decimal answer correctness — a later exact-literal cut is owed and is
  explicitly out of this slice.

## Scope held (deferred — each waits for a query to nominate it)

Decimal division, `AVG`/`MIN`/`MAX`, Decimal GROUP BY / join keys (`scalar_literal_key(Decimal128)` stays
`Unsupported`), official `DECIMAL` DDL (still rejected at plan), and answer correctness. The cut delivers
exactly its name: strict Q6's cell moved past `@bind`, nothing broader. (Grouped Decimal `SUM` was folded
in during review — see below — to keep bind and execute in lockstep.)

## No-regression verification (the monotonic-progress invariant)

263's declared gate is fully green:

```
scripts/cargo-cgroup check -p toron -p toron-tpch --all-targets    # clean
scripts/cargo-cgroup test  -p toron --lib                          # 1103 passed / 0 failed
scripts/cargo-cgroup test  -p toron --test architecture_test       # 2052 passed / 0 failed
scripts/cargo-cgroup test  -p toron-tpch                           # harness 7 + decimal_probe 2 + arch 10 + arrow_probe — all pass
```

Running the **full** `cargo test -p toron` integration suite (beyond 263's declared gate) surfaced **6
pre-existing failures across 4 binaries**, all "should-reject" / "loud rejection" accounting tests:

- `aggregate_projection_accounting_test::malformed_aggregate_arity_or_modifiers_reject_cleanly`
  (`COUNT(DISTINCT x)` / `SUM(DISTINCT x)` plan through — the aggregate arg path checks arity but not
  `duplicate_treatment`)
- `clause_accounting_test::unsupported_query_level_clauses_are_loud`
- `clause_accounting_test::unsupported_select_level_clauses_are_loud`
- `logical_type_ddl_test::derived_table_source_scope_rejects_general_subquery_features`
- `predicate_widening_test::in_list_rejects_empty_over_limit_subquery_unnest_or_rowvalue`
- `predicate_widening_test::unsupported_pattern_predicates_and_case_subquery_in_remain_unsupported`

**All 6 reproduce identically on HEAD with the 263 src changes stashed** — they are pre-existing on
`main` (rejection-accounting gaps unrelated to Decimal, likely trailing the ADR 023 DISTINCT test and the
256–262 subquery series), **not** introduced by 263. The 263 red row is earned, not self-inflicted: it is
zero. These pre-existing failures are flagged for a separate follow-on (rejection-accounting reconcile),
not churned mid-263.

## Test reconciles done in this cut

- `harness.rs`: `strict_q6_blocks_at_bind_on_decimal` → `strict_q6_reaches_execute` (asserts
  `Pass@execute` + digest); `matrix_regenerates_from_jsonl` updated to `strict=pass@execute`, keeping a
  blocked-render case for coverage.
- `decimal_probe.rs`: `decimal128_compute_blocks_at_bind` → `decimal128_compute_binds_and_executes`
  (binds + executes to `revenue = 100.00 * 0.06 = 6.0000`, `Decimal128(31,4)` raw `60000`); DDL probe and
  `arrow_decimal_probe.rs` retained.
- `architecture.rs` (toron-tpch): updated the pinned probe name.
- `binder_decomposition.rs` (toron architecture): bumped the binder-root bound `5500 → 5600` (Decimal
  literal coercion is real IR growth) and allowlisted `aggregate_kernel.rs` (Decimal SUM domain).

## Post-seal review fixes (folded into the seal)

1. **Grouped Decimal `SUM` (bind/execute lockstep).** The SUM domain accepts `Decimal128` at bind, so the
   *grouped* finalize had to too — `sum_column_from_groups` gained the `Decimal128(p,s)` arm mirroring
   the global path. Locked by `grouped_decimal_sum_executes` (would fail under the prior
   "unsupported SUM output type" error).
2. **Decimal literal robustness.** `decimal_literal_from_i128`/`_from_f64`/`decimal_rescale` now return
   `Result`: checked `pow`/`mul`, negative-scale and non-finite-float rejection, and a precision-fit
   check (`|raw| < 10^precision`) — an oversized/invalid literal is a clean bind error, never a wrong
   value or a panic. Both executor literal-array builders (`literal_array` is now fallible) propagate the
   Arrow error instead of `expect`. Locked by `oversized_decimal_literal_rejects_at_bind`.
3. **`arrow_decimal_probe.rs` asserts.** The grounding probe now *asserts* (was print-only): comparison
   requires exact `(precision, scale)` (different precision OR scale → `Err`), `(15,2)*(15,2)` widens to
   `Decimal128(31,4)`, and `aggregate::sum` carries the raw `i128` at the input scale.
4. **adr150 reconcile (a real seal miss).** Adding the multi-line `Decimal128` struct variant made
   rustfmt render the sibling `ScalarValue::Timestamp` struct variant multi-line, which broke
   `adr150`'s literal single-line `contains` check. The original seal ran `architecture_test` *before*
   the fmt that reformatted it, so it shipped red — caught by re-running the full guards after this
   round's fmt. The guard is now format-agnostic (checks `value: Option<i64>` + `unit: TimeUnit`),
   intent unchanged. Lesson: re-run `architecture_test` *after* `cargo fmt` — fmt can reformat source the
   guards scan.

Post-fix gate: `--all-targets` clean · `--lib` 1103/0 · full `architecture_test` 2052/0 (after fmt) ·
toron-tpch decimal_probe 4 / arrow_decimal_probe 2 / harness 7 · gauge `q06 strict pass@execute` with the
unchanged digest `[130, 66, 124, 232, …]`. The 6 pre-existing integration failures are unchanged (still
not 263's).
