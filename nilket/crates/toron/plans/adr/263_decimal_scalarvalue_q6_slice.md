# ADR 263 — Decimal `ScalarValue` (Q6 Slice)

## Status

Proposed.

## Context

ADR 262's measured red row named this cut: `q06 strict = blocked(unsupported_decimal_scalar @ bind)` —
a `Decimal128(15,2)` column compared against a `Float64` literal, because Toron's `ScalarValue` has no
Decimal, so decimal literals lower to `Float64` and comparison type-unification fails at bind.

The useful next invariant is *"strict Q6 no longer stops at bind"*, not *"Decimal exists as a type."*
So 263 adds exactly the Decimal support strict Q6's operations need, scoped tight; everything else
Decimal waits for a later query to nominate it. **Scope name: Decimal `ScalarValue` Q6 Slice** (not
"full Decimal"). Acceptance is *measured* by the gauge.

## Grounding — fork validated, executor reality corrected

The load-bearing assumption was "Arrow's Decimal128 kernels carry Q6." Probed
(`crates/toron-tpch/tests/arrow_decimal_probe.rs`), not assumed:

- `cmp::lt` on **same** `Decimal128(15,2)` → **Ok**; on **different precision** `(15,2)` vs `(3,2)` →
  **Err "Invalid comparison operation"**; on **different scale** `(15,2)` vs `(15,3)` → **Err**.
- `numeric::mul (15,2)*(15,2)` → **Ok → `Decimal128(31,4)`** (the SQL rule `p1+p2+1, s1+s2`).
- `aggregate::sum(Decimal128)` → **Ok**.

So Arrow's kernels *can* compute Q6's ops, with two caveats this makes load-bearing:

1. **Comparison requires both operands at the *exact* same `Decimal128(p,s)`** (Arrow cmp rejects
   mismatched precision/scale) — a binder cast is mandatory; `*` yields `Decimal128(p1+p2+1, s1+s2)`;
   `SUM` carries the input type.
2. **Toron's executor does not route every Decimal piece to Arrow today.** Comparison already calls
   Arrow (`evaluate_bound_binary`'s `Lt`/`Gt`), but the binder-inserted `Cast` nodes still need Decimal
   cases in `evaluate_cast` (`exec.rs:6615` currently supports primitive numeric casts only). `*` goes
   through Toron's `evaluate_arithmetic` (which passes the *left operand's* type as the output at
   `exec.rs:2451`, asserts `operands == output`, and dispatches Int/Float only at `exec.rs:7025`) and
   `SUM` excludes Decimal (`aggregate_kernel.rs:206`) and accumulates via Toron `AggState`/`NumericValue`
   (`exec.rs:5677`). So Decimal casts, `*`, and `SUM` need explicit executor work — they are NOT free
   pass-through.

(`ScalarValue` is a closed set of 10 with **1023 references across 16 files** — the variant ripple is
the mechanical bulk of the diff.)

## Decision

1. **`ScalarValue::Decimal128 { value: Option<i128>, precision: u8, scale: i8 }`** (mirrors Arrow), plus
   the rote variant arms across the 16 files (Arrow conversion, postcard, display, comparison).
2. **Binder coercion (comparison).** Where comparison type-unification raises `TypeMismatch`, add
   Decimal coercion: when one operand is `Decimal128` and the other is `Int`/`Float`/different-`(p,s)`
   `Decimal128`, insert a cast to a common `Decimal128(p,s)` so Arrow's exact-match cmp succeeds.
3. **Literal handling — Approach 1 (least-disruptive).** Keep the frontend lowering decimal/int
   literals as today (`Float64`/`Int`); the **binder casts the literal → the Decimal column's `(p,s)`**
   at comparison. This leaves all existing `Float64` behavior untouched (dev Q6 digest stays
   byte-stable) and adds only the additive `Int/Float → Decimal` cast strict Q6 needs.
   - *Known simplification:* `0.05` parses as `f64` then casts to `Decimal128(15,2)` (rounds to exactly
     `0.05` at scale 2 — correct for Q6's literals). Exact decimal-literal lowering (Approach 2, with
     bidirectional `Decimal↔Float` coercion) is the **answer-correctness** follow-on, not this slice.
4. **Execution — Arrow for comparison after casts; explicit executor work for casts, `*`, and `SUM`.**
   Comparison rides Arrow once the operands are cast to matching `(p,s)`, but those cast nodes are
   load-bearing: `evaluate_cast` must support `Int/Float → Decimal128(p,s)` and
   `Decimal128 → Decimal128(p',s')` for the Q6 predicates. `*` needs a Decimal arm that routes to Arrow
   `numeric::mul` using the *binary node's* output type (not the left operand's), branching around the
   operands-equal-output assertion. `SUM` needs a Decimal aggregate domain + either a Decimal `AggState`
   accumulator (i128 running sum) or a targeted Arrow-backed Decimal sum. **Verify with the gauge, not
   assumption.**

## Discriminator (the gauge is the acceptance test)

- `toron-tpch run --query q06 --mode strict --allow-blocked` moves `q06 strict` from
  `blocked(unsupported_decimal_scalar@bind)` to **`pass@execute`** — or to a *more precise* next blocker
  (a `cast`/precision-scale gap, or the digest), which is itself honest progress that names 264.
- **No-regression / monotonic-progress invariant (pinned by a test).** After 263, strict Q6 MUST NOT
  report `blocked(unsupported_decimal_scalar@bind)` again — the bind scalar wall is *closed*. It reaches
  `pass@execute` **or a strictly deeper blocker**: a *new* reason at `@execute` or later, never the same
  `@bind` reason. The test asserts `run_strict_q6` is **not** `(phase=Bind,
  reason=unsupported_decimal_scalar)`. A remaining red row must be *earned*, not self-inflicted — this
  keeps the gauge forward-only even if `pass@execute` needs one more narrow cut.
- `decimal_probe.rs`'s "blocks at bind" assertions flip to the new reality; the Arrow-kernel probe
  (`arrow_decimal_probe.rs`) is retained as the kernel-path grounding.

## Scope

- **In**: `ScalarValue::Decimal128` + the variant ripple; binder Decimal coercion for **comparison**
  (cast to common `(p,s)`); the `*` (`p1+p2+1, s1+s2`) and `SUM` result typing **and** their executor
  arms (Arrow `numeric::mul` via the bound node's output type; Decimal SUM domain + accumulator); the
  `Int/Float → Decimal128` and `Decimal128 → Decimal128(p',s')` casts those need, including executor
  support for those `Cast` nodes; Decimal scalar/array plumbing. Enough to carry strict Q6 from bind to
  execute.
- **Out / deferred (until a query nominates them)**: decimal `/`/`%`, `AVG`/`MIN`/`MAX`/other decimal
  aggregates, decimal in GROUP BY / window / join keys (`scalar_literal_key(Decimal)` stays
  unsupported), exact decimal-literal lowering (Approach 2) + answer-set correctness, `DECIMAL` DDL
  (`unsupported_decimal_ddl`, a frontend gap), the official Q6 `interval '1' year`
  (`unsupported_date_interval`).

## Ripple / guards (budget for the sweep)

A new `ScalarValue` variant trips exhaustive-match and serialization guards: **postcard preimage /
golden vectors must be re-pinned in this cut** (a new variant shifts encoding); any architecture guard
that lists/counts `ScalarValue` arms; the Arrow `ScalarValue ↔ array` conversion. Run the **FULL
`architecture_test` + `--all-targets`** (a new enum variant is exactly the case a bare `cargo test -p
toron` misses). This is the mechanical bulk of the diff.

## Forward arc

263 Decimal `ScalarValue` Q6 slice (strict Q6 → execute, or the next precise blocker) → broaden Decimal
to the next query's nominated op (AVG/division/casts; exact decimal literals for answer correctness) →
strict Q1 (decimal + GROUP BY) → `Date32 ± Interval` + official Q6 text → official-answer comparison via
`tpchgen::q_and_a` → Q3 (joins). The gauge keeps naming the next cut from measured evidence.
