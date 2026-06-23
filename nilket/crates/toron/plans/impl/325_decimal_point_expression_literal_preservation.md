# ADR 325 — Implementation plan: decimal-point expression literal preservation

Pairs with `plans/adr/325_decimal_point_expression_literal_preservation.md`. A `crates/toron` cut: exact literal
lowering + the **load-bearing dev literal rewrite**. Flips q11 strict to `pass@compare`. **D2 (dev rewrite) is not
optional — without it, dev reach breaks for Float queries with fractional constants.**

## Phase 0 — probe the breadth before the broad change (write findings first)
1. **The dev rewrite (widest ripple — do this first):** confirm `Float64_expr * Decimal128_literal` and
   `Float64_col = Decimal128_literal` **reject today** (no coercion arm); then that a literal-only rewrite fixes
   both *comparison and arithmetic*. `coerce_decimal_arithmetic_literal`'s `is_numeric_literal` is Int/Float only —
   a Decimal128 literal isn't covered.
2. **Strict + dev:** a Decimal literal vs a Decimal column (strict) and vs a Float64 column (dev); inside arithmetic
   (263/266/302/321) — each binds + executes.
3. **Goldens/preimage:** lower one literal to Decimal128, run the golden suite — does any postcard vector shift?
4. **Precision/reject:** `0.0001 → (5,4)`; a `> 38`-precision literal rejects cleanly.
5. **Negative literal helpers:** `LAG`/`LEAD` defaults negate scalar literals after lowering; verify `-0.1` still
   works or explicitly record a measured reject. The preferred fix is extending the helper to negate Decimal128
   literals.

## D1 — exact lowering (`sql_planner.rs:6488`)
- In the `Value::Number(n, _)` arm: keep the existing integer path unchanged. Else if `n` is a
  **plain decimal point, no exponent** (no `e`/`E`) → build an exact `ScalarValue::Decimal128`:
  scale = digits after the point; precision = decimal digit count after removing the sign and decimal point
  (preserve leading/trailing zeros); value = the unscaled i128. **precision > 38 → clean lowering reject** (no cap).
- "Integer unchanged" means the existing expression-literal behavior: `Int32` when it fits, then the current
  fallback path. Do not smuggle in a new `Int64` expression-literal policy in this cut.
- Scientific notation (`1e-4`) and any non-plain form: keep current behavior (out of scope unless Phase 0 trivial).

## D2 — the dev literal rewrite (LOAD-BEARING)
- Add a **literal-only** rewrite: when an arithmetic or comparison binds a `Float32`/`Float64` operand against a
  `Decimal128` **literal**, rewrite the literal → that Float type by deterministic raw/scale conversion. Apply in
  **both** the arithmetic coercion path and the comparison coercion path (mirror where
  `coerce_decimal_arithmetic_literal` / the comparison literal coercion run). This preserves dev Float semantics;
  it is not an exactness mechanism.
- **Literal-only.** Do not add a general `Decimal128 → Float` cast, a widening-cast arm, or a user CAST surface.
  Non-literal `Decimal128` (a column/expr) beside Float keeps current behavior.

## Tests (`binder/tests.rs` + toron-tpch)
- A decimal-point literal binds to exact `Decimal128(text p,s)`; `0.05 → (3,2)`, `0.0001 → (5,4)`.
- `> 38`-precision literal **rejects** (not capped/truncated).
- Integer literals keep existing expression-literal behavior (`Int32` when it fits; no new Int64 policy).
- **Dev rewrite:** `Float64_col = 0.05` and `Float64_col * 0.0001` bind + execute in dev (the rewrite); a Float32
  literal-context probe is useful if cheap. Revert D2 → they reject (discriminating — proves D2 is load-bearing).
- Strict: a Decimal literal operates at its own scale (`Decimal * 0.0001` carries `0.0001`).
- No general Decimal→Float cast is reachable for a non-literal Decimal beside Float.
- Negative decimal-point `LAG`/`LEAD` defaults still lower if they were supported pre-cut.

## Phase 3 — flip q11 + the matrices
```
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q11
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
```
- q11 strict `fail(decimal_literal_scale@compare) → pass@compare` (correctness **18**); **flip the ADR 324
  expected-red test to expect pass**. q11 dev unchanged; reach 22/22.
- Re-run goldens; re-pin in-cut if the literal-type change shifted a preimage.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron + --all-targets + goldens
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/325_decimal_point_expression_literal_preservation_results.md`: the lowering + the dev
rewrite (comparison + arithmetic); the Phase-0 breadth findings (dev reject→fix, goldens); q11 strict flip to
`pass@compare` (correctness 18) + the ADR 324 test flip; q06 + other literal queries unchanged in both modes; any
golden re-pin; `--all-targets` clean; reach 22/22.

## Risk / blast radius
- **D2 is the break-risk** — dev literal rewrite in comparison *and* arithmetic; the discriminating revert test
  proves it.
- **Breadth** — full `--no-fail-fast` + `-p toron-tpch` + goldens + `--all-targets`.
- **Exactness** — `> 38` rejects; preserve text precision/scale.
- **No cast-surface creep** — literal-only.
- **Goldens** — re-pin in-cut if the preimage shifts.

## Seal
`impl: adr 325 decimal point expression literal preservation`. Do not commit wip drafts.
