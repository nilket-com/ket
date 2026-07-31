# ADR 056 Row-Group Fetch

Date: 2026-05-05
Owner: Storage

## Scope

Use this runbook when ADR 056 row-group fetch is enabled or being rolled out.
The writer and reader switches are independent and default off:

- `storage.row_group_writer.enabled`
- `storage.l2_row_group_fetch.enabled`

## Strategy Mix

Check which path is actually serving L2 projected reads:

- `storage_l2_row_group_fetch_requests_total{strategy="row_group"}`
- `storage_l2_row_group_fetch_requests_total{strategy="columnar_buffer"}`
- `storage_l2_row_group_fetch_requests_total{strategy="whole_record"}`

Expected rollout shape:

1. Writer off, reader off: no row-group requests.
2. Writer on, reader off: directories appear in record bodies, but reads use
   column-buffer or whole-record paths.
3. Writer on, reader on: selective projected reads should show
   `strategy="row_group"`.

## Fallbacks

Inspect `storage_l2_row_group_fetch_fallback_total{reason}`.

Common non-page reasons:

- `disabled`: reader switch is off.
- `too_many_row_groups`: row-group selectivity is too dense.
- `too_many_bytes`: selected bytes exceed the configured fraction.
- `too_many_ranges`: selected row groups produce too many encrypted chunk runs.
- `unpaged_record`: record predates ADR 056 or missed writer thresholds.
- `rehydration_available`: L1 rehydration policy won over projected L2 fetch.

Corruption is not a fallback. Directory parse/range/layout failures should be
treated as invariant violations and investigated before continuing rollout.

`storage_l2_row_group_fetch_invariant_violations_total{violation_kind="page_directory_parse_error"}`
can appear on either side of the writer/reader boundary. During reads it means
the embedded `storage.row_group_directory` could not be decoded or validated.
During writes it also covers the fail-closed fixed-point guard that builds the
directory metadata before emission; no record should be published from that
writer attempt.

## Savings

Use:

- `storage_l2_row_group_fetch_row_groups_total{decision}`
- `storage_l2_row_group_fetch_bytes_total{kind="phase_c"}`
- `storage_l2_row_group_fetch_bytes_total{kind="avoided"}`
- `storage_l2_row_group_fetch_selected_ranges`

Healthy selective scans should show skipped/no-survivor row groups and avoided
bytes growing with row-group requests. Dense scans should fall back rather than
fetch most of the record one row group at a time.

## Tuning

Tune conservatively:

- Lower `max_selected_row_group_fraction_micros` to force earlier fallback on
  dense predicates.
- Lower `max_selected_byte_fraction_micros` when large projected columns erase
  row-group savings.
- Raise `max_ranges_per_record` only after confirming L2 GET-rate capacity.
- Change `target_row_group_rows` only as a cluster-wide writer setting; mixed
  values are valid but complicate cross-replica byte comparison.

## Disable

To stop using row-group fetch immediately, set:

```toml
[storage.l2_row_group_fetch]
enabled = false
```

To stop producing new directories, set:

```toml
[storage.row_group_writer]
enabled = false
```

Existing directory-bearing records remain readable via whole-record decode.
