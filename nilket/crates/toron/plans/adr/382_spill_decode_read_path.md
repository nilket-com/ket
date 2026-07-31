# ADR 382 - q03 spill-decode read-path (spill page format/decode, diagnostic-first)

## Context

ADR 381 named q03's dominant frame: `arrow_ipc::reader::RecordBatchDecoder::next_buffer` (10.54%) + `_int_malloc`
(9.98%) + `__memmove` (3.72%) - **spill-page decode/decompress/alloc**. The decode is **once** (the 380 preload decodes
into `LoadedPartitionWork.build_pages: Vec<RecordBatch>`; no re-decode), so decode-cache is N/A. Current spill pages are
**LZ4_FRAME-compressed Arrow IPC wrapped by spill encryption** - so "raw" is **not presumed zero-copy**: decompression is
real work, and dropping compression would blow `spill_bytes`. The real question: **is IPC+LZ4 the wrong internal
representation, or is this the inherent cost of reconstructing projected Arrow batches?** 382 may name that floor.
Diagnostic-first; **q03 only** (q07 partition/take/copy = ADR 383).

## Decision

### Phase 0 (diagnostic-first) - the measurement table
Confirm once-vs-repeated (expect once; if any path re-decodes, that's the cheaper fix). Then fill, for **current IPC+LZ4**
and each **candidate** (page sizes / formats):

| variant | page_count | rows/page | compressed bytes/page | decode ms/page | `next_buffer` % | decompress % | alloc/copy % | spill_bytes | writer_peak | guard outcome |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |

This decides whether a lever exists and which, **and proves it doesn't regress the budget**.

### Levers (decide by the table)
- **Larger/fewer pages (budget-gated):** may amortize per-page IPC framing, **but can raise `writer_peak` and
  resident-state pressure** - acceptance must show `spill_bytes`/`writer_peak`/guard are recorded, bounded, and
  budget-safe, not just profile share. A small bounded `writer_peak` increase is allowed only if it stays inside the
  existing guard with no cap bump and is justified by the measurement table.
- **Codec choice (if decompression-bound):** if LZ4 decompression dominates rather than IPC framing, measure codec/page
  alternatives directly. Uncompressed/raw payloads are admissible only as measured candidates and only if `spill_bytes`
  and resident guards stay within the existing budgets.
- **Raw/internal page candidate (NOT presumed zero-copy):** internal spill is same-process/trusted, so a raw Arrow
  buffer layout could skip IPC framing/validation - **but only wins if Phase 0 proves it reduces decode/decompress/alloc
  WITHOUT blowing `spill_bytes`/`writer_peak`** (it still needs the budget properties that compression currently gives;
  raw-uncompressed is rejected unless the table proves it is budget-safe). Call it the **raw/internal candidate**,
  measured.
- **Decode-cache:** N/A (once).
- **Inherent-data floor:** if the cost is decompress + buffer reconstruction of the already-projected data, the lever is
  small - **name it a floor** (IPC+LZ4 is not the wrong representation; the cost is reconstructing the batches), and the
  spill volume becomes the real term. No fake lever.

### The cut
Apply only the table-justified lever (page-size if framing-bound + budget-safe; raw/internal candidate if framing/
validation-bound + budget-safe; named floor if data-bound). Any spill page-format/size change is **broad (all spill)**
and **round-trip parity-gated**. Topology-named; no qNN; **no q07 claim**.

## Acceptance
- **Phase 0 table recorded** (current IPC+LZ4 vs candidates), once-vs-repeated confirmed.
- **q03 `RecordBatchDecoder::next_buffer` share drops materially** (re-profile, 381 symbol-retaining method) **OR a named
  inherent-data floor** with the table evidence (no fake lever).
- **Budget-safe, not just profile share:** `spill_bytes`, `writer_peak`, and resident-guard behavior recorded for the
  chosen variant. No cap bump; any `writer_peak` increase is explicitly bounded and guard-safe.
- **Correctness parity:** spill round-trip identical across all spill shapes + encryption; q03 strict `pass@compare`
  (sf=0.01/sf=1) + persisted sf=10 if it advances/greens.
- **No q07 claim** (ADR 383); **`--no-fail-fast` required**; de-branded; prior greens unchanged.

## Guardrails
- **Diagnostic-first; the table decides** - no blind format rewrite; once-vs-repeated + framing/decompress/alloc
  breakdown first.
- **Raw is a measured candidate, not assumed zero-copy** - LZ4+encryption stay; raw wins only if it cuts decode without
  raising `spill_bytes`/`writer_peak`.
- **Page-size is budget-gated** - `spill_bytes`/`writer_peak`/guard in acceptance, not just profile %.
- **Spill-format change is broad + round-trip-gated** (all shapes + encryption).
- **Honest floor; no q07 claim; no cap bump; --no-fail-fast; de-branded; no qNN.**

## Risks
- **Raw assumed zero-copy** -> it isn't (compression+encryption); the table gates it on measured decode + budget.
- **Page-size raises writer_peak** -> budget-gated acceptance catches it.
- **Inherent data decode** -> named floor, not a forced format change.
- **Broad format change breaks a round-trip** -> all-shapes + encrypted parity test.
- **Scope creep into q07** -> ADR 383.
- **Regression** -> `--no-fail-fast`.

## Forward arc
382 (q03 spill-decode: budget-safe page-size / raw-internal candidate / named floor) -> **383 (q07 partition/take/copy:
`take_bytes` + `partition_bucket_*` + `__memmove`; partly the 376 scan-path bucket residual)** -> rest of the time
cluster -> parallelism engagement-policy follow-up (381 side table). The representation decision here informs all future
spill work.

## Grounding evidence (file:line)
381: q03 `RecordBatchDecoder::next_buffer` 10.54% / `_int_malloc` 9.98% / `__memmove` 3.72%; q06 control spill_bytes=0
clean. decode once: `LoadedPartitionWork.build_pages: Vec<RecordBatch>` streaming.rs:182, `read_batch_page` ->
`decode_record_batch_ipc` spill.rs:911/915/1526/1531. current format: LZ4_FRAME IPC + spill encryption (write path 375).
q03 DuckDB=255ms. q07 -> ADR 383.
