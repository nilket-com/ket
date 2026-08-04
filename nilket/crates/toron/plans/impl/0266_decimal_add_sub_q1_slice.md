# Implementation Plan — ADR 266: Q1 reachability slice (decimal `+`/`-`)

## Summary
Add a q01 attempt to the TPC-H gauge and clear q01's first measured red row — decimal `+`/`-` with
Int/Float literal-to-decimal coercion. q01 advances from the arithmetic bind wall to the measured
`AVG(Decimal)` blocker (ADR 267). AVG/division stays out. Mirrors ADR 263's `*` slice.

## Phase 1 — q01 gauge attempt (`toron-tpch`)
- `src/lib.rs`: add `run_dev_q1` / `run_strict_q1` alongside the q06 pair, reusing the shared
  `run_q6` stage tracker by renaming it to a query-agnostic `run_query(query_id, ...)` if cleaner. The
  strict lineitem fixture grows `l_returnflag` / `l_linestatus` (`Utf8`) and `l_tax`
  (`Decimal128(15,2)`); dev mirrors the string columns and uses `Float64` for `l_tax`.
- `main.rs`: route `--query q01`/`q1` to the q01 runners and update the usage string. Leave unsupported
  query IDs as `not_attempted`.
- q01 SQL constants: use the repo's q06 naming style (`Q6_DEV_SQL` / `STRICT_Q6_SQL`; so
  `Q1_DEV_SQL` / `STRICT_Q1_SQL`). The shape is TPC-H Q1's projection (`SUM`s including
  `l_extendedprice * (1 - l_discount)` and `l_extendedprice * (1 - l_discount) * (1 + l_tax)`, the
  `AVG`s, `COUNT(*)`, GROUP BY `l_returnflag,l_linestatus`, ORDER BY), but with
  `WHERE l_shipdate <= DATE '1998-09-02'` instead of the official interval expression. `status_rows`
  marks q01 implemented.

## Phase 2 — binder: decimal `+`/`-` typing + arithmetic literal coercion (`crates/toron/src/binder.rs`, `binder/arithmetic.rs`)
- Reuse the comparison literal coercion path from `binder.rs` (`coerce_literal_value` and the checked
  `decimal_literal_from_*` helpers) from arithmetic binding. If visibility gets awkward, move the helper
  to a shared binder helper; do not duplicate unchecked decimal scaling math.
- When binding `a ± b` with one side `Decimal128(p,s)` and the other an Int/Float **literal**, rewrite
  the literal in place to `Decimal128(p,s)` (no `Cast` node; no executor cast arm in this cut). Covers
  `lit ± decimal` and `decimal ± lit`.
- Add the same-`(p,s)` `Decimal128 ± Decimal128` output-type arm beside the 263 `Mul` arm:
  `Decimal128(p,s) ± Decimal128(p,s) → Decimal128(min(38, p + 1), s)` — `+1` precision for carry,
  scale unchanged (NOT scale-widening like `*`). Q1's fixture case: `(15,2) ± (15,2) → (16,2)`.
- Keep mixed-scale/mixed-precision Decimal `+`/`-` out unless the implementation can prove it with
  existing machinery. There is no general Decimal→Decimal `Cast`/`evaluate_cast` surface to rely on.

## Phase 3 — executor: decimal `+`/`-` arm (`crates/toron/src/exec.rs`)
- In `evaluate_bound_binary`, beside the 263 `Mul`→`numeric::mul` arm (~`:2470`), add `Add`/`Sub` over two
  `Decimal128` operands routing to Arrow `numeric::add`/`sub`, branching around `evaluate_arithmetic`'s
  Int/Float-only assertion. Verify the binder-declared output `(p,s)` equals Arrow's actual output
  (empirically, as with 263's `*`); reconcile the typing arm if Arrow differs.
- Extend `arrow_decimal_probe.rs` with an asserting `numeric::add`/`sub` probe for `(15,2) ± (15,2)`.

## Phase 4 — telemetry reasons (`crates/toron-tpch/src/telemetry.rs`)
- Classifier (`reason_for_error`) + recognized-blocker allowlist (`RECOGNIZED_BLOCKERS`): add
  `unsupported_decimal_arithmetic` ("arithmetic operands must be Int32, Int64, Float32, or Float64 …
  Decimal128"), `unsupported_decimal_avg` ("AVG expects numeric input … Decimal128"), and — the measured
  q01 first wall — `ambiguous_aggregate_projection` ("ambiguous aggregate projection"). Put the decimal
  checks before the broad Decimal fallback, and the ambiguous check before the generic "unsupported"
  fallback. Precise-match discipline from ADR 262 (no bare-substring overclassification).

## Tests
- `toron-tpch` harness (truth 2): `q01_attempts_and_blocks_at_aggregate_projection` — `run_strict_q1`
  and `run_dev_q1` report `(Plan, ambiguous_aggregate_projection)` (q01's measured first wall).
- `toron-tpch` decimal_probe (truth 1): `decimal_add_sub_binds_and_executes` — `1 - l_discount` types to
  `Decimal128(16,2)` (values 0.94/0.95) and `SUM(l_extendedprice * (1 - l_discount))` executes to
  284.0000; `arrow_decimal_probe` asserts Arrow's `add`/`sub` `(15,2)±(15,2)→(16,2)`.
- binder/exec: `decimal ± int/float literal` binds (coercion) and same-`(p,s)`
  `decimal ± decimal → Decimal128(min(38, p + 1), s)`; a non-decimal arithmetic domain error still fires
  (the Decimal arm is additive, not a loosening).

## Verification gate (ADR 264 seal gate; cargo via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt  -p toron -p toron-tpch -- --check
scripts/cargo-cgroup test -p toron --test architecture_test          # FULL — source-scanning guards
scripts/cargo-cgroup test -p toron --no-fail-fast                    # the WHOLE integration suite
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron-tpch                              # gauge + probes + the q01 no-regression test
scripts/cargo-cgroup run  -p toron-tpch -- run --query q01 --mode strict --allow-blocked
scripts/cargo-cgroup run  -p toron-tpch -- matrix
git diff --check
```
Acceptance — two truths: (1) the decimal slice `SUM(l_extendedprice * (1 - l_discount))` binds + executes
on strict Decimal128; (2) full q01 reports `blocked(ambiguous_aggregate_projection@plan)` (dev + strict).
q06 unchanged; full suite green. NOT claimed: q01 reaching the arithmetic bind or `pass@execute`.

## Seal
`impl: adr 266 q1 reachability slice decimal add sub`,
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes
- **Stay at `+`/`-`.** No `÷`, no AVG — those are ADR 267's division/finalize surface. The honesty check:
  q01's *next* red row is `unsupported_decimal_avg`, not a division error.
- **Typing must match Arrow.** Same-scale `+`/`-` should carry scale and add one precision digit; confirm
  Arrow's `numeric::add`/`sub` output `(p,s)` equals the binder's declared type, or downstream SUM
  mis-types (as 263 verified for `*`).
- **Reason precision.** Classify on the specific messages; a bare "arithmetic"/"decimal" substring would
  overclassify unrelated errors (the ADR 262 reason-classifier lesson).
