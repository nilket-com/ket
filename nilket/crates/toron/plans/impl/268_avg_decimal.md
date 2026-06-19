# Implementation Plan — ADR 268: AVG(Decimal)

## Summary
`AVG(Decimal128(p,s)) → Decimal128(min(38,p+4), s+4)`, half away from zero, in pure i128 arithmetic. Four
sites: un-gate the input domain and add a decimal-aware output rule (`aggregate_kernel`), a dedicated
`AggState::AvgDecimal` with a checked decimal finalize, and thread the bound output type into every AVG
materializer so the decimal divide replaces the unconditional `Float64` (`exec`). Accumulation of the raw
i128 is the only thing already in place. No IR/opcode/Cast change; non-decimal AVG stays `Float64`.

## Phase 0 — keep the gauge honest
- Re-run `scripts/cargo-cgroup run -p toron-tpch -- run --query q01 --mode strict --allow-blocked` and
  confirm the pre-edit row is `blocked(unsupported_decimal_avg@bind)`.
- Update the stale telemetry/evidence comments that still frame Decimal AVG as a post-267 blocker
  (`toron-tpch/src/telemetry.rs`, `toron-tpch/src/lib.rs`, and `toron-tpch/tests/harness.rs`) as part of
  this cut.

## Phase 1 — output rule + input gate (`crates/toron/src/aggregate_kernel.rs`)
- Add `fn avg_numeric_output(input: &DataType) -> Option<DataType>` and make `is_avg_numeric_input` (`:224`)
  delegate to it. This is load-bearing: an invalid high-scale Decimal must be rejected by the input domain,
  not admitted and then masked by `stable_output_field`'s fallback type.
  - integer and float inputs -> `Some(Float64)`
  - `Decimal128(p,s)` -> `decimal_avg_output_type(p,s)`
  - everything else -> `None`
- Add `fn decimal_avg_output_type(precision, scale) -> Option<DataType>`: `out_scale = scale + 4`,
  `out_precision = min(38, precision + 4)`; return `None` if the derived `(out_precision, out_scale)` is not
  a valid `Decimal128` (`out_scale > out_precision`) -> a clean bind rejection, never a silent truncation.
- New `AggregateOutputRule::AvgNumeric`; flip the AVG kernel spec (`:107`) `Float64 -> AvgNumeric`. In the
  rule match (`:168`): `(AvgNumeric, Some(data_type)) => avg_numeric_output(data_type)`;
  `fallback_output_data_type` (`:196`): `AvgNumeric => Float64` for fallback-only paths. The physical type
  then flows through `stable_output_field -> aggregate_field` with no separate binder edit.

## Phase 2 — dedicated decimal AVG state (`crates/toron/src/exec.rs`)
- `enum AggState` (`:5709`): add `AvgDecimal { total: i128, count: u64, input_scale: i8 }`. (Carry only the
  input scale — the output `(p',s')` arrives at finalize via the bound output type. `total`/`count` mirror
  `AvgInt`.)
- `avg_state_for_data_type` (`:5944`): `Decimal128(_, scale) => AvgDecimal { total: 0, count: 0, input_scale:
  scale }`. (`numeric_value:6513` already yields `NumericValue::Int(i128)` for decimals.)
- `add_avg_value` (`:6006`): add an `(AvgDecimal{..}, NumericValue::Int(v))` arm — checked i128 add
  (`Kernel { "decimal AVG overflow" }`), checked count.

## Phase 3 — decimal finalize (`crates/toron/src/exec.rs`)
- `fn avg_decimal_finalize(total: i128, count: u64, input_scale: i8, out_precision, out_scale) ->
  Result<Option<i128>>`: `count == 0 -> None`; derive `scale_delta = out_scale - input_scale`, reject a
  negative delta as an internal type error, compute the multiplier with checked power/multiply helpers, and
  scale `total` with checked arithmetic (overflow -> `Kernel { "AVG decimal scale overflow" }`). Divide
  rounding **half away from zero** via the exact
  remainder: `q = scaled / count as i128; r = scaled % count as i128; if 2*r.unsigned_abs() >= count as u128
  { q = q.checked_add(scaled.signum()).ok_or(...) ? }`; verify `q` fits `out_precision` (else `Kernel`).
  Return `Some(q)`.
- Thread the output type into the two materializers (they currently take none, unlike SUM's twins):
  `avg_state_array(state, output_type)` and `avg_column_from_groups(groups, agg_idx, output_type)`. For
  `Decimal128(p',s')`: finalize via `avg_decimal_finalize` and build a `Decimal128Array`
  `.with_precision_and_scale(p', s')` (mirror `sum_state_array:6266`). For `Float64`: the existing
  `avg_finalize` path, unchanged.

## Phase 4 — wire the output type at every AVG call site (`crates/toron/src/exec.rs`)
- `bound_global_aggregate` Avg arm → `avg_state_array(&state, output_type_from_schema)`.
- `bound_grouped_aggregate` Avg arm → `avg_column_from_groups(&groups, agg_idx, output_type_from_schema)`.
- Legacy unbound global aggregate path (`:437`) -> initialize AVG from the evaluated argument type and
  derive the output field through the aggregate kernel, instead of hardcoding `AvgFloat`/`Float64`.
- Legacy grouped path (`:1685`) -> read `aggregate_output_types[agg_idx]`, pass to `avg_column_from_groups`.
- `window_agg_finalize` (`:4581`) → pass the output type to `avg_state_array` (window AVG(Decimal) stays in
  lockstep with grouped/global).
- Drop the hardcoded `AggregateFunction::Avg => DataType::Float64` assumptions for the decimal case (`:1697`,
  `:5783`). The grouped field construction already has `aggregate_output_types[agg_idx]`; the test-only
  legacy helper should delegate to the aggregate kernel for Decimal input. Leave integer/float AVG and all
  SUM Decimal handling untouched.

## Phase 5 — tests
- **Decimal AVG bind+execute** in the `toron-tpch` decimal probe (where `Decimal128(15,2)` fixtures and
  `decimal_add_sub_binds_and_executes` already live — `group_by_test`'s fixture is Int/Float only):
  - `AVG(Decimal128(15,2))` binds as `Decimal128(19,6)`; `AVG(0.06, 0.05) → 0.055000` (raw `55000`);
    `AVG(100.00, 200.00) → 150.000000` (raw `150000000`); grouped variant; `count = 0` / all-NULL → NULL.
  - **Discriminating**: a non-divisible sum such as `AVG(0.01, 0.02, 0.02) -> 0.016667`; assert both the
    widened raw Decimal value and the Decimal128 output type so same-scale rounding and Float64 collapse
    both fail.
- **Rounding helper** unit tests (inline `#[cfg(test)]`): positive *and* negative half-ties pin
  half-away-from-zero; odd-count non-tie sanity.
- **Regression**: integer AVG and float AVG still return `Float64`; Decimal SUM / `+` / `-` / `*` stay green.
- **Gauge** (`crates/toron-tpch/tests/harness.rs`): re-cut `q01_advances_past_aggregate_projection` to the
  new strict result — expect `pass@execute`; if a deeper wall appears, assert it and name ADR 269 from the
  measured row, not a guess. Re-pin the q01 digest. q06 unchanged.

## Verification gate (ADR 264 seal gate; cargo via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt  -p toron -p toron-tpch -- --check
scripts/cargo-cgroup test -p toron --test architecture_test     # FULL — a new AggregateOutputRule /
                                                                # AggState variant may trip a count guard
scripts/cargo-cgroup check -p toron -p toron-tpch --all-targets # new enum variants — benches/examples
scripts/cargo-cgroup test -p toron --no-fail-fast               # whole integration suite
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron-tpch                         # gauge + decimal AVG probe
scripts/cargo-cgroup run  -p toron-tpch -- run --query q01 --mode strict --allow-blocked
scripts/cargo-cgroup run  -p toron-tpch -- matrix
git diff --check
```
Acceptance: decimal AVG binds/executes to `Decimal128(p+4,s+4)` with the discriminating + signed-tie values;
q01 advances off `unsupported_decimal_avg`; q06 unchanged; full suite green. If q01 does not reach execute,
record the measured blocker in the results doc and let it name ADR 269 — don't paper over it.

## Seal
`impl: adr 268 avg decimal`,
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes
- **Scale policy is the decision, pinned by a discriminating test** (not "no error"); doc-comment names
  `s+4 / min(38,p+4) / half-away-from-zero`.
- **Overflow explicit** at both the `10^δ` pre-multiply and the precision-fit check; i256 widening is a named
  follow-on, not silently absorbed.
- **No Float64 collapse in strict**: a guard/test asserts the decimal AVG path materializes `Decimal128`.
- **One finalize, all three paths** (global/grouped/window) so windowed AVG(Decimal) can't diverge.
