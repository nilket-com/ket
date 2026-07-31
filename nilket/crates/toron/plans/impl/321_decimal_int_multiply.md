# ADR 321 — Implementation plan: Decimal × Int multiply operand alignment (q11)

Pairs with `plans/adr/321_decimal_int_multiply.md`. Engine cut (`crates/toron` binder `bind_arithmetic`). Reuses
ADR 263 Decimal×Decimal mul + the Arrow Int→Decimal cast. Multiply only.

## Phase 0 — confirm the anchors
- `bind_arithmetic` Mul arm (arithmetic.rs:1153) = `Decimal128 * Decimal128`; `coerce_decimal_arithmetic_literal`
  is literal-only (`builder.literal`). q11 strict: `ps_supplycost`(Decimal128(15,2)) * `ps_availqty`(Int32) →
  `unsupported_decimal_arithmetic@bind`.
- Confirm Int→Decimal cast is available for an arithmetic operand (Arrow `cast`; SumInt→Decimal exec.rs:6522).

## D1 — non-literal Int coercion in the Mul arm (`binder/arithmetic.rs`)
- In `bind_arithmetic`, for `op == Mul` where one operand is `Decimal128(p,s)` and the other binds to
  `Int32`/`Int64` **and is not already handled by the literal coercion** (i.e. a non-literal: column or computed
  int expr): cast the Int operand to `Decimal128(int_prec, 0)` (`Int32 → (10,0)`, `Int64 → (19,0)`) with a direct
  internal `append_cast`. Do **not** use `cast_if_needed`: it is limited to primitive widening and intentionally
  does not admit Int→Decimal. Then fall into the existing ADR 263 Decimal×Decimal rule →
  `Decimal128(min(38, p + int_prec + 1), s)`. Symmetric for `Int * Decimal`.
- Keep logical identity handling explicit: the existing logical-arithmetic gate runs before this arm and should
  continue to reject non-arithmetic logical identities; the inserted cast is only for arithmetic-eligible/naked Int
  operands, and it should not silently preserve or invent a declared identity.
- Keep `.min(38)` (the existing cap); do not add a fail-loud branch.
- Fire **only** for Decimal×(non-literal Int) Mul; leave Int-Int, Float, two-Decimal, literal, and +/-/÷ paths
  untouched.

## D2 — exec
- The binder-inserted `Cast(Int → Decimal128(int_prec,0))` lowers to the existing Arrow `cast`; then the existing
  Decimal×Decimal `numeric::mul`. Confirm no new exec arm needed (the cast + mul already exist).
- Add a small grounding assertion/probe if needed that `cast_if_needed` is not the mechanism for this cut; the
  implementation should be a dedicated Decimal/Int arithmetic cast path.

## Tests
- **Engine unit (`binder/tests.rs` / exec test home):**
  - `Decimal128(15,2) * Int32-col` → `Decimal128(26,2)`, correct values; `* Int64-col` → `Decimal128(35,2)`.
  - a **computed** int expression beside Decimal (e.g. `Decimal * (int_col + 1)`) coerces (proves "non-literal",
    not just "column").
  - `Int * Decimal` symmetric.
  - overflow pair (`p + int_prec + 1 > 38`) → output `.min(38)` (the existing cap; no fail-loud).
  - two-Decimal mul, Int-Int mul, Float mul, and Decimal×Int-**literal** (ADR 266) all unchanged; revert the
    coercion → q11-shape rejects (discriminating).
- **Architecture:** full `architecture_test` (`bind_arithmetic` shared).

## Phase 3 — re-measure q11 + the matrix
```
scripts/cargo-cgroup run -p toron-tpch -- run --query q11 --mode dev    --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- run --query q11 --mode strict --sf 0.01
```
- Expect q11 strict moves off `unsupported_decimal_arithmetic@bind` → `pass@execute` (keyed) → **reach 20** (q11
  both-modes-green); or a deeper measured blocker (record + name). dev stays green.
- **Record the `* 0.0001` reach/correctness boundary** in the results doc: q11 reaches execute, but
  `decimal_literal_from_f64` rounds `0.0001` to the SUM's scale (→ `0.00` at scale 2), so the green cell is reach,
  not correctness. q11 correctness is deferred and must account for this.
- Re-run `matrix` + `correctness-matrix`: q11 advances; **q15/q18/q19 and all others unchanged**.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron (shared `bind_arithmetic`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q11 --mode dev --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- run --query q11 --mode strict --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/321_decimal_int_multiply_results.md`: the coercion + output types (Int32/Int64/computed);
q11's new measured rows (strict off the wall → green or deeper blocker); **the `* 0.0001` reach/correctness
boundary recorded** (q11-green ≠ q11-correct); the matrix delta (q11 only); confirmation the cap policy, literal
coercion, and q15/q18/q19 are unchanged.

## Risk / blast radius
- **`bind_arithmetic` shared** — fire only for Decimal×(non-literal Int) Mul; full `--no-fail-fast` is the net.
- **Cap** — keep `.min(38)`; don't reopen the overflow policy.
- **Scope** — multiply only; +/-/÷ and Float-column deferred.
- **Reach ≠ correctness** — record the `* 0.0001` rounding boundary; do not imply q11 correctness from the green
  cell.

## Seal
`impl: adr 321 decimal int multiply`. Do not commit wip drafts.
