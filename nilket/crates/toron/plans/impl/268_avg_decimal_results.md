# ADR 268 — Results: AVG(Decimal)

## Outcome — strict q01 reaches `pass@execute`

The acceptance's best case landed: with AVG(Decimal) implemented, **strict q01 reaches execute end-to-end**,
not merely "advances off the wall."

```
q01 dev=pass@execute  strict=pass@execute
q06 dev=pass@execute  strict=pass@execute   (unchanged)
```

After 263's Decimal `*`/`SUM` and 266's Decimal `±`, AVG was the last arithmetic piece in q01's body; with
268 every piece resolves. Gauge gate: `q01_reaches_execute_in_both_modes` (renamed from ADR 267's
`q01_advances_past_aggregate_projection`, which had measured the prior `unsupported_decimal_avg@bind`).

## The cut as built (smaller than iter-0 feared)

Grounding showed accumulation was already in place: `numeric_value` feeds a `Decimal128` into the i128
accumulator, so no new accumulation kernel was needed. The substance was four narrow pieces:

1. **`aggregate_kernel.rs` — typing.** `is_avg_numeric_input` now delegates to a shared
   `avg_numeric_output(input)` (the operator's tightening), so input-domain acceptance and output-type
   derivation are one source of truth — a high-scale Decimal whose `+4` widening can't fit `Decimal128` is
   rejected at the gate, never admitted then masked by a fallback type. `decimal_avg_output_type(p,s)` →
   `Decimal128(min(38,p+4), s+4)`, `None` when `s+4 > min(38,p+4)`. New `AggregateOutputRule::AvgNumeric`
   (Float64 for int/float, widened Decimal for Decimal). The flat `Float64` rule — AVG was its only user —
   was removed as dead; `AvgNumeric` subsumes it.
2. **`exec.rs` — dedicated state.** `AggState::AvgDecimal { total: i128, count: u64, input_scale: i8 }`
   (both peers' call over reusing `AvgInt`): self-describing, carries its own input scale. `numeric_value`
   already yields the raw i128; `add_avg_value` checked-adds it. Review tightening: the state constructor
   also validates Decimal inputs through the aggregate-kernel output rule, so high-scale Decimal inputs
   cannot form an executor state and later fall back to Float64 materialization.
3. **`exec.rs` — decimal finalize.** `avg_decimal_finalize` rescales the i128 sum from the input scale to
   the widened output scale and divides **half away from zero** via the exact remainder
   (`if 2·|r| ≥ count { q += sign }`), with checked `10^δ` multiply, a negative-scale-delta guard, and a
   precision-fit check. `avg_state_array`/`avg_column_from_groups` now take the output `&DataType` (they
   previously took none, unlike SUM's twins) and branch: Decimal → decimal finalize, else the unchanged
   Float64 path.
4. **`exec.rs` — five materialization paths wired.** bound global, bound grouped, legacy global, legacy
   grouped, and `window_agg_finalize` all thread the output type so global/grouped/window AVG stay in
   lockstep.

### The legacy unbound global path (operator catch, confirmed)
`exec.rs`'s legacy unbound global AVG arm hardcoded `AggState::AvgFloat` + a `Float64` field — a
`Decimal128` would have entered the **float** accumulator via `add_avg_value`'s `Int→f64` cast and emitted a
Float64 average (the silent collapse we're declining; a decimal-AVG test routed through it would have passed
for the wrong reason). It now derives the accumulator and output type from the observed argument (mirroring
the sibling MIN/MAX arm), keeping the historical Float64 default only for the empty-input case.

## Scale policy, pinned

`AVG(Decimal128(15,2)) → Decimal128(19,6)` (precision grows with scale to preserve the integer-digit
budget). Worked, tested values: `AVG(0.06, 0.05) = 0.055000` (raw 55000), `AVG(100.00, 200.00) =
150.000000`. Rounding is the unit-tested non-trivial part: `AVG(0.01, 0.02, 0.02) → 0.016667` (50000/3
rounds 16666.67 → 16667), and a **negative** tie `-5/2 → -3` (away from zero, not toward). Conservative
logical type: a widened decimal average is NAKED (no inherited unit) — q01's columns aren't logical-typed,
so unaffected.

## Tests

- `decimal_probe.rs`: `decimal_avg_binds_and_executes` (global; asserts `Decimal128(19,6)` type — fails a
  Float64 collapse — and raw 55000 — fails a scale-2 average that would round 0.055→0.06; two aliased AVGs
  also exercise 267's disambiguation) and `grouped_decimal_avg_executes` (per-group, lockstep with SUM).
- `exec.rs` unit tests: `avg_decimal_finalize_rounds_half_away_from_zero` (non-divisible +
  positive/negative ties + empty→NULL — the rounding cases the 2-row fixture can't reach) and
  `avg_decimal_state_rejects_unrepresentable_widening` (state construction rejects the high-scale edge).
- Regression: integer/float AVG still `Float64` (full suite green); Decimal SUM/`±`/`*` unchanged.

## Guard reconciles

- `sql_planner_surface.rs`: the kernel-catalog marker list and the AVG-rule assertion retargeted
  `output_rule: Float64 → AvgNumeric`; `AggState` variant count `13 → 14` (`AvgDecimal`).
- The `legacy_aggregate_output_type` test helper now delegates to the kernel so AVG(Decimal) yields the
  widened type, not Float64.
- Stale telemetry/evidence comments refreshed (`telemetry.rs`, `lib.rs` ×2, `harness.rs`); the
  `unsupported_decimal_avg` classifier + recognized-blocker entry are kept — they now fire only for the
  high-scale edge that can't fit precision (a genuine remaining gap), not for supported decimals.

## Deferred (named, unchanged)
- **ADR 269 — scalar `Decimal ÷ Decimal`** (its own operand-driven scale policy; reuses this divide).
- **i256-widened AVG intermediate** (removes the `10^δ` pre-multiply ceiling).
- **Decimal AVG logical-type widening policy** (give the widened average an inherited unit).

## Gate
fmt + `git diff --check` clean · `--all-targets` clean · `architecture_test` 2052 · full `--no-fail-fast`
green (lib 1105, group_by 220, decimal_probe 7, harness 8, …) · `toron-tpch` green · live matrix shows q01
strict `pass@execute`.
