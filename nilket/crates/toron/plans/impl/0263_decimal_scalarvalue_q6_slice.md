# Implementation Plan — ADR 263: Decimal `ScalarValue` (Q6 Slice)

## Summary

Add `ScalarValue::Decimal128`, the binder coercion/result-typing strict Q6 needs (cast to common
`(p,s)` for comparison; `p1+p2+1, s1+s2` for `*`; `SUM` carries), and the **explicit executor work** for
Decimal casts, `*`, and `SUM` (only the final comparison kernel rides Arrow). Acceptance is measured by
the gauge: `q06 strict` moves off `@bind`. Scope is the Q6 slice — no other decimal ops.

## Verify-first — Arrow kernel path GROUNDED (retained probe)

`arrow_decimal_probe.rs` (run): `cmp::lt` needs **exact matching `(p,s)`** (mismatched precision *or*
scale → `Err`); `numeric::mul (15,2)*(15,2)` → `Decimal128(31,4)`; `aggregate::sum(Decimal128)` → Ok.
Arrow's kernels *can* compute it; but Toron's executor doesn't route Decimal casts, `*`, or `SUM`
today (see Phase 3). Keep the probe as the kernel-path regression; extend it with cast checks if the
implementation chooses Arrow-backed `Int/Float → Decimal128` or `Decimal128 → Decimal128(p',s')` casts.

## Phase 1 — the variant + ripple sweep

- `ir.rs`: `ScalarValue::Decimal128 { value: Option<i128>, precision: u8, scale: i8 }`.
- Sweep the **exhaustive matches** (1023 refs / 16 files): `ScalarValue ↔ Arrow` (build/read a
  `Decimal128Array` with `(p,s)`), postcard ser/de, `Display`, comparison/ordering, any
  `data_type()`-style mapping (`→ DataType::Decimal128(p,s)`). Most arms are rote. Drive completeness
  with `--all-targets` + the FULL `architecture_test`.
- **Re-pin postcard goldens** in this cut (a new variant shifts encoding) — don't let the golden vector
  test land broken.

## Phase 2 — binder coercion + result typing

- Locate the comparison type-unification site (grep the literal `comparison operands have different
  types`). When one side is `Decimal128` and the other is `Int`/`Float`/different-`(p,s)` `Decimal128`,
  **insert a cast to a common `Decimal128(p,s)`** (Arrow cmp demands the exact match); choose the
  common type to preserve value (widen precision; scale up to the larger scale).
- Arithmetic typing: `Decimal128(p1,s1) * Decimal128(p2,s2) → Decimal128(min(38,p1+p2+1), s1+s2)`.
- `SUM(Decimal128(p,s))` → `Decimal128(p,s)`; wire the decimal Sum result type in `binder/aggregate.rs`.
- Casts emit a `Cast` Expr the executor runs (Phase 3a). Approach 1: literals stay `Float64`/`Int` in
  the frontend; the binder casts them.

## Phase 3 — execution (NOT free Arrow pass-through; explicit executor work)

Only the final **comparison** rides Arrow today (`evaluate_bound_binary` `Lt`/`Gt` call Arrow
`lt`/`gt`) — so Phase 2 makes cmp work only if the inserted casts execute. Decimal casts, `*`, and
`SUM` need real arms:

- **3a — Decimal casts.** `evaluate_cast` (`exec.rs:6615`) currently supports primitive numeric casts
  only. Add the Q6-needed Decimal cases: `Int32/Int64/Float32/Float64 → Decimal128(p,s)` and
  `Decimal128(p,s) → Decimal128(p',s')`, with precision/scale validation, rounding for the known
  Float→Decimal literal path, null preservation, and overflow as an error. Prefer Arrow's cast kernel if
  the probe confirms the exact behavior; otherwise a small targeted conversion is in-slice because the
  binder-inserted predicate casts are load-bearing.
- **3b — Decimal `*`.** `evaluate_bound_binary` (`exec.rs:2451`) passes the **left operand's** node type
  as `output_type`; it must pass the **binary node's** output type for widening decimal mul.
  `evaluate_arithmetic` (`exec.rs:7025`) asserts `operands == output_type` (false for widening) and
  dispatches Int/Float only. Add a **`Decimal128` arm routing to Arrow `numeric::mul`** (it widens
  `(15,2)*(15,2)→(31,4)` directly), branching around the operand-equals-output assertion.
- **3c — Decimal `SUM`.** `aggregate_kernel.rs:206` excludes Decimal; execution accumulates via Toron
  `AggState`/`NumericValue` (`exec.rs:5677`). Accept `Decimal128` in the SUM domain (result =
  `Decimal128(p,s)`) and add **a Decimal `AggState` accumulator (i128 running sum)** or a targeted
  Arrow-backed Decimal sum (the accumulator is likely smaller — decide at impl).
- **3d — Decimal scalar plumbing.** `ScalarValue::Decimal128 ↔ Decimal128Array` extraction, literal
  arrays, result builders (+ `NumericValue` if the `AggState` route uses it).
- **3e — keep keys out.** `scalar_literal_key(Decimal128)` stays **unsupported** (Decimal GROUP BY /
  join keys out of the slice) unless the gauge proves Q6 needs it.

## Phase 4 — measure, test, reconcile, seal

### Gauge (the acceptance test)

Re-run after each phase: `toron-tpch run --query q06 --mode strict --allow-blocked`. Target
`pass@execute`; a precise new blocker (missing cast, precision-scale edge, digest) is honest progress —
record it and decide in-slice vs a 264 nomination.

### Tests / guards

- **No-regression invariant (reconcile ADR 262's `strict_q6_blocks_at_bind_on_decimal`).** That test
  asserts `(Bind, unsupported_decimal_scalar)`; after 263 the bind wall is closed, so repoint it to
  `strict_q6_advances_past_decimal_scalar_bind` (`harness.rs`): assert `run_strict_q6(..).await` is
  **not** `(phase=Bind, reason=unsupported_decimal_scalar)` — it is `Pass`, or `Blocked`/`Fail` at a
  *strictly deeper* phase/reason. The monotonic-progress gate; don't leave the 262 test asserting the
  closed wall.
- `decimal_probe.rs` (toron-tpch): flip the strict assertions to the new reality;
  `arrow_decimal_probe.rs` retained as-is.
- Toron-side: a binder test that `Decimal128(15,2) < <int/float literal>` binds (coercion inserts the
  cast), an `evaluate_cast`/predicate execution test for `Int/Float → Decimal128`, and a decimal
  `*`/`SUM` result-type test; an execute test that a small `Decimal128` Q6-shaped query sums correctly.
- Guards: a `ScalarValue`-arm/golden guard update if one exists; otherwise the FULL `architecture_test`
  is the net.

### Evidence

Add `plans/impl/0263_decimal_scalarvalue_q6_slice_results.md` with the before/after gauge row:
`q06 strict` no longer reports `blocked phase=bind reason=unsupported_decimal_scalar`; record either
`pass@execute` or the new precise blocker. Include the `matrix` output so the benchmark board shows the
cell movement, not just the single command transcript.

### Verification gate (cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets      # the variant ripple — REQUIRED (not just --lib)
scripts/cargo-cgroup test  -p toron --lib
scripts/cargo-cgroup test  -p toron --test architecture_test     # FULL (golden/postcard + arm guards)
scripts/cargo-cgroup test  -p toron-tpch                          # the gauge + probes + no-regression test
scripts/cargo-cgroup run   -p toron-tpch -- run --query q06 --mode strict --allow-blocked
scripts/cargo-cgroup run   -p toron-tpch -- matrix
cargo fmt -p toron -- --check && cargo fmt -p toron-tpch -- --check
git diff --check
```

### Seal

`impl: adr 263 decimal scalarvalue q6 slice`,
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes

- **The variant ripple is the bulk** — budget for the mechanical sweep across 16 files; `--all-targets`
  + FULL `architecture_test` (not `--lib`) is mandatory to catch every exhaustive match and the golden
  re-pin.
- **Casts, `*`, and `SUM` are executor work, not Arrow pass-through** — comparison is the only free
  ride after casts. Don't under-budget the first pass: Decimal `evaluate_cast`, the bound-node output
  type for mul, and the Decimal SUM accumulator are deliberate changes.
- **Comparison needs exact `(p,s)`** (grounded) — a half-done coercion that leaves a `(15,2)` vs `(3,2)`
  comparison still errors at bind; the gauge catches it.
- **Don't broaden** — resist div/AVG/other-aggregate/GROUP-BY decimal "while we're here"; each waits for
  a query to nominate it. The cut delivers its name only if strict Q6's cell moves past `@bind`.
- **f64→Decimal literal rounding** is a known reachability simplification, not answer-correctness; note
  it in the results doc so the later exact-literal cut is clearly owed.
