# ADR 261 — TPC-H Reachability Harness: first results

**Dev-mode output below is NOT a TPC-H result.** It runs a Toron-authored Q6 *proxy* (`Float64` money,
literal date bounds) over synthetic `tpchgen` data with no official answer-set comparison. It measures
*reachability* — which phase a query shape reaches — not benchmark correctness or performance.

## The gauge is built and reads green on dev Q6

`crates/toron-tpch` (a helper bin crate; `toron` has no dependency edge to it). Dev data is generated
from the pure-Rust `tpchgen` row structs into workspace-Arrow (`TPCHDecimal → Float64`,
`TPCHDate → Date32`, Q6 columns only — the Arrow-output generator variant is avoided because it pins a
different Arrow major). The query runs single-node through `plan → bind → execute`.

```
tpch_reachability query=q06 sf=0.001 mode=dev    phase=execute  status=pass          reason=none                   digest=ArrowIpcDigest([233, 38, 184, …])
tpch_reachability query=q06 sf=0   mode=strict    phase=generate status=not_attempted reason=strict_inputs_required  digest=none
```

- **dev Q6 reaches `execute` and produces a stable digest** — the first green *reachability* row. Q6's
  shape (single scan + date-range + `BETWEEN` + `SUM(arith)`) runs end-to-end on `Float64` money.
- **strict Q6 is `not_attempted`** pending user-supplied official schema/data/answers — honest, not a
  fabricated engine blocker.

## Reachability matrix (`toron-tpch status`)

```
q01..q05  dev=not_attempted  strict=input_gated
q06       dev=implemented    strict=input_gated
q07..q22  dev=not_attempted  strict=input_gated
```

## What this cut deliberately did NOT do

- It did **not** surface the strict Decimal blocker yet. Strict mode is a skeleton that stops at
  `not_attempted(strict_inputs_required)`; it does not load official `DECIMAL` data or compare against
  an answer set. Wiring the strict path (so the gauge reads the *first real* blocker — most likely
  Decimal, possibly a `Date32`/Interval gap on the official Q6 text) is the immediate follow-on.
- No Decimal implementation, no official query bodies, no DBGEN/QGEN, no distributed (`fragment`) run,
  no performance numbers.

## Honesty invariant (locked by tests)

`blocked` exits 0 (under `--allow-blocked`) only for a reason on the recognized allowlist
(`strict_inputs_required`, `unsupported_decimal_ddl`, `unsupported_decimal_arrow`,
`unsupported_date_interval`, `unsupported_query_surface`). Any unrecognized error/panic is a `fail`,
even under `--allow-blocked` — pinned by `unrecognized_error_is_fail_not_blocked`. Generated data and
the JSONL evidence write only under `target/tpch/` (gitignored); nothing enters git.

## Forward arc (selected from the gauge, not prediction)

Wire strict-mode loading so the gauge reports the first real blocker → **Decimal numeric types** (the
most likely first red row) → `Date32`/Interval polish for the official Q6 text → strict Q6 answer
comparison once inputs are usable → Q1/Q3 reachability as composition deepens → distributed/Wendelon
reachability. The harness is the scoreboard those cuts move.
