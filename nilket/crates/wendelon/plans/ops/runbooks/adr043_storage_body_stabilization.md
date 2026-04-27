# ADR 043 Storage Body Stabilization

Date: 2026-04-27
Owner: `H6`
Depends On: ADR 039, ADR 040, ADR 042, ADR 043

## When To Use This

Use this runbook during and after rollout of ADR 043 Arrow IPC body
compression, or when storage-body density, decode cost, L2 fetch bytes, or
convergence metrics move away from the post-rollout baseline.

Primary triggers:

- LZ4 ratio moves outside the expected 0.3-0.6 band for analytical tables.
- decode latency rises while stats pruning is weak.
- uncompressed resident bytes stop decaying after a policy flip.
- decode errors are non-zero.
- L2 admitted record-body bytes do not fall after the policy moves to LZ4.
- operators are frequently opting tables back to `Uncompressed`.

## Metric Names

Prometheus deployments render metrics with the repository namespace prefix.
The queries below use `$P` for that prefix. For the default exporter, set
`$P` to the standard namespace prefix before the functional segment.

Functional segments to watch:

- `storage_compression_ratio{codec="lz4_frame"}`
- `storage_compression_decode_micros{codec="lz4_frame"}`
- `storage_compression_encode_micros{codec="lz4_frame"}`
- `storage_compression_uncompressed_resident_bytes`
- `storage_compression_decode_error_total{codec}`
- `storage_compression_l2_record_body_bytes_fetched_total{codec}`
- `pbc_slab_stats_pruned_total`
- `pbc_slab_stats_consulted_total`
- `storage_multirow_point_read_amplification_bytes`

Bench evidence to keep with rollout notes:

- `cargo bench --bench multi_row_fragments -- --quick`
- `multi_row_fragments case=l2-cold-read ... fetched_record_body_bytes=...`

## First Look

Check:

- LZ4 compression ratio:
  `histogram_quantile(0.95, sum by (le) (rate($Pstorage_compression_ratio_bucket{codec="lz4_frame"}[15m])))`
- LZ4 decode latency:
  `histogram_quantile(0.95, sum by (le) (rate($Pstorage_compression_decode_micros_bucket{codec="lz4_frame"}[15m])))`
- LZ4 encode latency:
  `histogram_quantile(0.95, sum by (le) (rate($Pstorage_compression_encode_micros_bucket{codec="lz4_frame"}[15m])))`
- convergence backlog:
  `$Pstorage_compression_uncompressed_resident_bytes`
- decode errors:
  `sum(rate($Pstorage_compression_decode_error_total[5m]))`
- admitted L2 record-body bytes:
  `sum by (codec) (rate($Pstorage_compression_l2_record_body_bytes_fetched_total[15m]))`
- stats-prune ratio:
  `sum(rate($Ppbc_slab_stats_pruned_total[15m])) / clamp_min(sum(rate($Ppbc_slab_stats_consulted_total[15m])), 1)`
- point-read amplification:
  `rate($Pstorage_multirow_point_read_amplification_bytes[15m])`

## Interpretation

The expected healthy shape after rollout:

- `storage_compression_ratio{codec="lz4_frame"}` sits near 0.3-0.6 on the
  default analytical schema.
- `storage_compression_decode_error_total{codec}` remains zero.
- `storage_compression_uncompressed_resident_bytes` decays as natural
  compaction or operator-driven recompact rewrites old uncompressed records.
- L2 admitted record-body bytes fall only because whole record bodies are
  smaller. ADR 043 does not add provider filtering or column Range-GET
  pushdown.
- `storage_multirow_point_read_amplification_bytes` does not improve just
  because bytes on disk are compressed. It tracks the uncompressed
  point-read axis.
- stats-prune ratio should remain in the same band as the pre-rollout
  baseline. A drop usually points to stats or locality coverage, not a
  storage-body codec defect.

## Decision Tree

If compression ratio is near or above 1.0:

- identify tables with high-entropy payloads or already-compressed columns
- consider a descriptor edit from `Lz4Frame` to `Uncompressed` for those
  tables only
- do not disable LZ4 cluster-wide based on one high-entropy tenant

If decode latency rises and stats-prune ratio is low:

- inspect locality policy and stats coverage for the affected query class
- check whether slabs are being admitted but mostly filtered after decode
- prioritize stats/locality fixes before compression opt-out

If encode latency rises on write-bound nodes:

- compare write throughput to the pre-rollout baseline
- check whether the workload is dominated by tiny records or high-entropy
  records
- use per-table opt-out if the density win does not justify write CPU

If uncompressed resident bytes stop decaying:

- verify old slabs are still resident and match current descriptors set to
  `Lz4Frame`
- trigger the existing layout recompact path for affected tables
- do not introduce a compression-only sweeper

If decode errors are non-zero:

- treat as corruption, feature-drift, or malformed IPC until proven otherwise
- inspect the slab id, tier pointer, and object-store hash validation around
  the first error
- stop rollout if the error appears on newly-written records

If L2 record-body bytes do not fall:

- verify the read path is fetching admitted whole record bodies, not projected
  columns
- compare `storage_compression_l2_record_body_bytes_fetched_total{codec}` with
  encoded-byte counters for the same rollout window
- confirm the table actually writes new records under `Lz4Frame`

If point-read amplification changes after rollout:

- inspect `max_record_bytes` enforcement and packing decisions
- compression should not alter the uncompressed amplification ceiling
- treat a shift as a packing/enforcement regression, not a density win

## Mitigation Actions

Ranked by reversibility:

1. wait for natural compaction if the convergence backlog is falling
2. run the existing table layout recompact path for stuck high-value tables
3. edit affected high-entropy tables from `Lz4Frame` to `Uncompressed`
4. tighten stats or locality coverage when decode CPU is caused by poor
   admission pruning
5. pause rollout if decode errors appear on fresh writes

Do not:

- add a compression-only sweeper
- auto-flip table policy based on one measurement window
- claim ADR 043 delivered column-level L2 Range-GET pushdown
- bump envelope format version to explain codec behavior
- add reserved codec variants without a follow-up ADR

## Success Criteria

- LZ4 ratio stays in the target band for representative analytical tables.
- decode errors remain zero.
- uncompressed resident bytes decay toward zero after the rollout window.
- L2 admitted record-body bytes fall in line with encoded-byte reduction.
- point-read amplification remains bounded by the uncompressed axis.
- operator opt-outs stay rare and explainable by table payload entropy.
- architecture tests and ADR grep guards stay green.

## Escalate When

Escalate if:

- decode errors are non-zero on fresh LZ4 writes
- convergence backlog is flat for more than one compaction window on high-value
  tables
- LZ4 ratio is near or above 1.0 for a broad class of analytical tables
- decode CPU rises materially while stats-prune ratio also falls
- quota or billing counters do not reflect encoded-byte reduction after
  recompact

## References

- `plans/adr/043_arrow_ipc_body_compression.md`
- `plans/impl/043_arrow_ipc_body_compression.md`
- `plans/impl/042_analytical_multi_row_fragments.md`
- `plans/ops/observability.md`
