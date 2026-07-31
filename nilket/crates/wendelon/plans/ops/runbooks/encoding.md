# ADR 057 Encoding Runbook

Date: 2026-05-06
Owner: `storage`

## Purpose

ADR 057 Encoding is the opt-in per-buffer encoding layer inside ADR 056 row-group
records. It stores `storage.encoding_directory` in authenticated Arrow IPC
schema metadata and materializes dense Arrow buffers before handing data to
stock Arrow readers.

## Rollout

Encoding is disabled by default. Enable it only with row-group writing:

```toml
[storage.row_group_writer]
enabled = true

[storage.encoding_writer]
enabled = true
target_simd_page_values = 1024
cascade_sample_rows = 256
min_buffer_bytes_for_encoding = 4096

[storage.l2_encoding_fetch]
enabled = true
max_simd_pages_per_record = 64
max_parallel_materialize_tasks = 4
```

Use `enabled_cascades` to remove a suspect cascade family without disabling the
whole writer. Labels are `plain`, `lz4_frame`, `bit_packed`, `rle`,
`dictionary`, `delta`, `fsst`, and `alp`.

## Watch

Primary signals:

- `wendelon_storage_l2_encoding_buffer_encodings_total{cascade}`: cascade mix.
- `wendelon_storage_l2_encoding_compression_ratio_bucket{cascade}`: encoded /
  decoded byte ratio.
- `wendelon_storage_l2_encoding_materialize_micros_bucket{cascade}`: decode CPU.
- `wendelon_storage_l2_encoding_fallback_total{reason}`: economic fallbacks.
- `wendelon_storage_l2_encoding_invariant_violations_total{violation_kind}`:
  fail-closed corruption or contract rejects.

Invariant violations should be treated as data-path incidents. The bounded
codes are `encoding_directory_parse`, `buffer_encoding_mismatch`,
`page_range_overflow`, `cascade_unsupported`, and `decoded_length_mismatch`.

## Disable

Fast rollback:

```toml
[storage.encoding_writer]
enabled = false

[storage.l2_encoding_fetch]
enabled = false
```

Existing Encoding records remain readable because readers materialize through the
authenticated directory. Disabling the writer only stops new Encoding records from
being emitted.

Cascade-specific rollback:

```toml
[storage.encoding_writer]
enabled = true
enabled_cascades = ["plain", "lz4_frame", "bit_packed", "rle", "dictionary"]
```

This removes `delta`, `fsst`, and `alp` from future selection while preserving
the rest of the Encoding surface.

## Triage

If `encoding_directory_parse` rises, sample a single record and verify that the
schema metadata contains both `storage.row_group_directory` and
`storage.encoding_directory`.

If `buffer_encoding_mismatch` or `decoded_length_mismatch` rises, disable the
writer and keep fetch enabled so old records remain readable. Preserve the slab
bytes for offline replay.

If materialization latency rises without invariant violations, reduce the
enabled cascade set first. Start by removing the highest-latency cascade shown
by `storage_l2_encoding_materialize_micros{cascade}`.
