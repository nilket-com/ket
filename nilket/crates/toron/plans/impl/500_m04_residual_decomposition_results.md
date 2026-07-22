# impl 500 — m04 bypass residual decomposition (grounding results)

Grounding for the next m04 cut. No engine decision is made here; this lands the residual-decomposition
instrument and records what it measured. The kernel is **not** chosen — one further split is required
(see forward).

## The instrument

The `TORON_GROUPED_FOLD_RECEIPT_PATH` receipt (generic local grouped-aggregate path) gains three
batch-level buckets that bracket exactly the per-batch loop body **outside** the existing `fold_micros`:
`stream_pull_micros` (the awaited scan pull, which contains the parquet decode), `projection_micros`
(`apply_streaming_scan_input_ops`), and `finalize_micros` (the one-time output build). All are gated
on the receipt being `Some`, so a disabled run pays nothing.

Everything semantically "aggregate" — group-key evaluation, `GroupArrayReader` build, aggregate-input
eval, per-row hash-probe/update — is already **inside** `fold_micros` (aggregate.rs fold bracket wraps
the whole `fold_bound_grouped_aggregate_batch`), so the residual is provably not aggregate work.

## Rails verified

Interleaved 4-config isolation (release, sf=8, bypass default), medians:

| config | execute |
|---|---:|
| disabled | ~894 ms |
| fold-receipt-only (this instrument) | ~903 ms — ≈ disabled, **clean** |
| parquet-ledger-only | ~958 ms (**+60 ms**) |
| both | ~957 ms |

The instrument is production-faithful (fold-only ≈ disabled). The +65 ms combined observer effect is
**entirely the parquet ledger**, not these timers — so the decomposition uses the clean fold-only run,
and `stream_pull` is not contaminated by the ledger's in-`.next()` push overhead.

## Decomposition (clean fold-only run, ~900 ms)

| bucket | value | note |
|---|---:|---|
| `stream_pull` | 410 ms | = decode ~156 ms + **async framework ~254 ms** |
| `projection` | **0** | m04 has no projection; `apply_streaming_scan_input_ops` is a noop |
| `finalize` | **~0** (4 µs) | one-time, 3 groups |
| fold | 434 ms | inside `fold_micros`, not residual |
| remainder | ~56 ms | loop bookkeeping + fold-dispatch framing outside the brackets |

`residual (~310 ms) = async scan-stream framework ~254 ms + ~56 ms dispatch remainder`.

## What the evidence says

The residual is **per-batch async framework**, not per-row work — ~5.4 µs × 46,865 batch polls through
the async storage-scan stream (`.into_batches()`, `.instrument(span)`, tokio poll), beyond the raw
decode. Projection and finalization are measured-zero; the "per-row projection" going-in hypothesis is
refuted. This fixes the kernel *class* as **per-batch**: fewer/larger scan batches (≈1,024 rows/batch
today → 8× fewer polls would cut ~254 ms roughly proportionally) or lighter per-poll overhead (the
per-poll tracing span is the other suspect) — **not** projection elision.

## Forward

- Decide the kernel only after splitting the ~254 ms framework into its components (batch size vs
  per-poll tracing span vs stream-adapter layers), so the fix targets the dominant one — the same
  decompose-before-kernel discipline that has repeatedly overturned the intuitive target in this arc.
- **Parquet-ledger observer-effect note:** the ledger is now +6.7% on bypassed m04 (was +1.3% on the
  pre-bypass ~2 s control) — the bypass halved the wall but not the ledger's absolute per-batch push
  cost. Future m04 paired runs must not arm it without accounting; the O(1)-accumulation rewrite is the
  cleanup that restores it (a diagnostic-harness cut, not engine).
