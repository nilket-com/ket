# ADR 274 — Results: tpch.q10 (measured row)

## Measured row
```
q10 dev    = pass@execute
q10 strict = blocked(unsupported_decimal_group_key@execute)
q01/q03/q06/q14 reachability unchanged · q06 strict=pass@compare unchanged
```

q10 — the first **4-table** query — is in the gauge. dev (Float64) reaches execute end-to-end; strict walls at
execute on the Decimal `c_acctbal` GROUP BY key. No engine feature was added; this is a reachability attempt
and the measured row names ADR 275.

## The measured wall is the *exchange* partition key, not `group_value` (verify-first paid off)
The ADR predicted the wall at `exec.rs::group_value` (no `Decimal128` group-key arm) — and framed it
verify-first, "if no earlier wall appears." An **earlier** wall did appear: the GROUP BY repartition exchange
rejects the Decimal partition key *before* `group_value` is reached. The measured raw error is:
```
unsupported exchange partition key type: Decimal128(15, 2)
```
Both are the same conceptual gap (a `Decimal128` group/partition key), so the reason name
`unsupported_decimal_group_key` holds, and the classifier matches **either** message (`"exchange partition key
type"` or `"unsupported group by type"`) with `"decimal"` — but the *first* wall is the exchange, recorded
honestly rather than asserting the predicted `group_value`. (This is exactly why a grounded prediction stays
verify-first until measured.)

The 4-table comma join is **not** the wall (dev passes end-to-end; ADR 270's recursive routing lifts all three
equis), nor is the revenue arithmetic (q01/q03/271). dev's `Float64` `c_acctbal` partitions/groups fine.

## What was built (harness only)
- **Fixtures** (`lib.rs`): `nation` (`n_nationkey`, `n_name`, `n_regionkey`) via `NationGenerator` (25 rows);
  `customer` **expanded** (`c_name`, `c_acctbal`, `c_address`, `c_phone`, `c_comment`, `c_nationkey`, appended
  after `c_mktsegment`) and made **mode-sensitive** — `c_acctbal` is `Float64` (dev) / `Decimal128(15,2)`
  (strict), so `customer` now splits dev/strict like `lineitem`. `q03_tables` routes the mode-appropriate
  customer (q03 ignores `c_acctbal` → its row/digest unchanged); `q10_tables` = q03's three + `nation`.
- **SQL** (`Q10_DEV_SQL` / `STRICT_Q10_SQL`): tpchgen-2.0.2's exact Q10, `:1` → `'1993-10-01'`, interval upper
  bound → `DATE '1994-01-01'`, `ORDER BY revenue DESC` only (no tiebreak, no LIMIT). Same text both modes.
- **Wiring**: `run_dev_q10` / `run_strict_q10` (the first 4-table users of `run_query_with_tables`), CLI
  `run --query q10`, `status_rows` → q10 `implemented`.
- **Telemetry**: `unsupported_decimal_group_key` recognized blocker + classifier (matches the exchange-
  partition-key / group-by-type Decimal messages), placed before the generic `unsupported_decimal_arrow`
  bucket (ADR 262 precise-match) — the only telemetry change; no engine feature.

## Tests
- `harness.rs::q10_dev_executes_and_strict_blocks_at_decimal_group_key` — dev `pass@execute` (+ digest), strict
  `blocked(unsupported_decimal_group_key@execute)`, q10 `implemented`. Runs on an explicit 64 MB
  `std::thread` stack: q10's 4-table execution future overflows libtest's default test-thread stack (it
  overflows even on a multi-thread runtime — `block_on` polls the root future inline on the test thread — and
  with a raised `RUST_MIN_STACK`, which the libtest test thread does not honor). It is deep-but-finite: the CLI
  runs dev q10 fine on the main thread's larger stack (~7 ms), as does production. (This recurs for q05+ — wider
  joins need the explicit-stack test wrapper.)
- `harness.rs::decimal_group_key_error_classifies_precisely` — both the exchange-partition-key and the
  group-by-type Decimal messages classify as `unsupported_decimal_group_key`, not `unsupported_decimal_arrow`.
- q01/q03/q06/q14 reachability + the q06 correctness cell stay green (append-only, mode-routed customer).

## Forward arc (named by the measured row)
**ADR 275 — Decimal group-key support**: a `Decimal128` arm in the exchange partition-key hashing **and**
`exec.rs::group_value`/`group_column` (the first lifts the measured exchange wall; the second is the next wall
behind it) — the engine cut that turns strict q10 green. The cadence then points to a correctness cell (q01
oracle — grouped output) before q05 (6-table). Correlated-subquery and date-interval arcs remain named.

## Gate
fmt clean · `check -p toron-tpch --all-targets` clean · `test -p toron-tpch` green (q10 incl., q01/q03/q06/q14
+ q06 correctness unchanged) · live matrix shows the measured q10 row · `correctness-matrix` still q06
`pass@compare`.
