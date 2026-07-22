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

- The ~254 ms framework was split (batch size vs per-poll span/adapter) — see "Framework-internal
  split — RESOLVED" below. Outcome: it is a **gauge artifact, not an engine cut**; the
  decompose-before-kernel discipline stopped an optimization aimed at a parquet-batch-size artifact.
- **Parquet-ledger observer-effect note:** the ledger is now +6.7% on bypassed m04 (was +1.3% on the
  pre-bypass ~2 s control) — the bypass halved the wall but not the ledger's absolute per-batch push
  cost. Future m04 paired runs must not arm it without accounting; the O(1)-accumulation rewrite is the
  cleanup that restores it (a diagnostic-harness cut, not engine).

## Framework-internal split — RESOLVED: gauge artifact, no engine cut

A `TORON_TPCH_PARQUET_BATCH_SIZE` probe (diagnostic-only, uncommitted→preserved) swept the tpch
parquet reader batch size. Using `execute` as ground truth with `fold_micros` as the per-row control:

| batch size | source batches | execute | fold_micros |
|---:|---:|---:|---:|
| 1024 (parquet default) | 46,865 | 915.7 ms | 441.4 |
| 4096 | 11,717 | 697.4 ms | 413.7 |
| 8192 | 5,859 | 669.2 ms | 402.4 |
| 16384 | 2,930 | 687.8 ms | 402.0 |
| 65536 | 733 | 1,200.3 ms | 395.4 |

`fold_micros` is flat (per-row control holds); execute drops **−246 ms (−27%)** from 1,024- to
8,192-row batches, then rises (U-curve: 65,536 is parquet decode degradation, not a fold effect). So
the residual is **batch-count-driven** per-batch poll/span/adapter machinery — confirmed.

**But the batch count is a gauge artifact, not an engine property.** Production scan contract (all
`StorageScan` implementors): the production backends `InMemoryStorageScan` / `InMemoryTableStorage`
emit **one whole morsel-sliced batch** per scan (`scan_in_memory_batch` → `ScanStream::from_batches(
…, vec![batch])`, storage.rs:699), and the distributed `TaskInputStorageScan` / `LiveInputStorageScan`
inherit upstream (in-memory) batch shape. **No production scan path emits ~1K-row batches** — only the
tpch `ParquetHarnessScan` does, via the parquet reader default. A production m04-shaped scan folds a
few large in-memory batches and pays ~0 per-batch framework.

Therefore the ~254 ms framework residual is a **gauge-fidelity artifact** (same class as the
thread-count confound and `stable_file_hash`), and the "engine floor" (~100 ms at 8,192) is itself
per-batch and would also vanish at production batch sizes. **No engine cut; the span-vs-adapter
ablation is not warranted** (it would optimize a floor production doesn't pay). The substantive m04
engine win was the ADR 499 exchange bypass (−51%); this residual is measurement overhead.

## Forward (updated)

- **No m04 engine cut.** The residual is a gauge artifact; m04 is resolved as an engine target.
- Any adoption of ~8,192 batches must be a **dedicated full-ladder gauge-coordinate change** (ADR 501),
  explicitly labeled a *Parquet-optimal proxy for production's large-batch contract* — not
  production-equal (parquet can't emit production's one-giant-batch without its own U-curve decode
  cost) — with a before/after ladder comparison before any ranking movement is treated as real.
- The `TORON_TPCH_PARQUET_BATCH_SIZE` env probe is preserved as diagnostic evidence.
- **Parquet-ledger observer-effect note** stands: +6.7% on bypassed m04; keep it out of decisive pairs
  until the O(1)-accumulation rewrite.
