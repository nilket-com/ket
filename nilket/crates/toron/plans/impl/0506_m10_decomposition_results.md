# impl 506 — m10 decomposition: premise falsified, no profitable cut

Grounding for ADR 506. The decompose-first gate answered before any kernel: **there is no profitable
m10 execution cut.** No engine change made.

## Method

Loop-mode `perf record` (20× m10, profiling binary `9c26b044`, 8,192 coordinate, hash-skip on, 20,790
samples) — the ADR 502/505 annotate method that beats single-shot dilution.

## Result: the multiply is already vectorized

m10 is `SELECT SUM(l_extendedprice * l_discount) FROM lineitem`. Hot user-space symbols:

| symbol | share | class |
|---|---:|---|
| `parquet::…::read_column_metadata` + metadata thrift (`parquet_metadata_from_bytes`, PageEncodingStats, Encoding, skip_till_depth) | ~24% | gauge (per-open parse; loop-inflated) |
| `arrow_arith::arity::try_binary_no_nulls` | 4.8% | **the decimal multiply — already a vectorized per-batch Arrow kernel** |
| `NumericArrayReader::value` + `add_sum_value` + `fold_global_aggregate_state` | ~5% | per-row SUM fold (checked i128) |
| RleDecoder / coerce_i64 / unpack32 | ~3% | gauge (parquet decode) |

**The "per-row dispatch width" premise is falsified.** `l_extendedprice * l_discount` is evaluated as
a vectorized Arrow `Decimal128` binary kernel per batch — there is no per-row expression-tree dispatch
to hoist. The band re-grounding's ~96 ms "engine above floor" is parquet metadata/decode (gauge) plus
the already-vectorized multiply plus a small SUM fold.

The only per-row toron work left is the SUM fold (~5%): reading the product array and accumulating the
overflow-checked i128. Vectorizing that (an Arrow decimal-sum kernel) would shave a few ms at the risk
of the checked-overflow semantics — marginal, and below the "a cut must deliver its name" bar.

## Decision

**m10 is closed: no profitable execution kernel.** The arithmetic is already vectorized; the residual
is gauge (parquet) plus an inherent, correctness-critical SUM fold.

## Ladder saturated → encoded-column contract to council

With m10 closed, the primitive ladder is **saturated**: every rung is closed by mechanism
(m01–m10 scan/decode/arith), boundary-pinned (m04 producer-gated, m06 layout held), or gauge-floor.
The **encoded-column dict/RLE-preserving scan contract** is now the sole substantive engine lever and
enters **council** as a grounding-first, human-steered design cycle (interior/exterior placement,
cross-ring obligations — ADR 505 receipt). The next measurement frontier is the composed q-series,
which is also where the contract should be designed against rather than m04 alone.
