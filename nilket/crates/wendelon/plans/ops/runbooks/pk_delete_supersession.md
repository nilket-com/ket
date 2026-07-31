# PK Delete Supersession Runbook

ADR: `plans/adr/050_pk_delete_supersession.md`

## When to Use PK Delete

Use `TableDeleteRows` against `RowIdentityPolicy::PrimaryKey` tables
when the caller already has canonical row key identities. Predicate
delete, range delete, cascade delete, and PK-value-keyed client
ergonomics are outside ADR 050 V1.

## Expected Outcomes

PK delete is admitted synchronously against the latest visible PK
version chain:

- `deleted`: latest-as-of delete HLC is data; a tombstone marker is
  written and the visible-row digest subtracts the row element.
- `already_deleted`: latest-as-of delete HLC is already a tombstone;
  the request absorbs successfully and writes no marker.
- `not_found`: no version exists at or before the delete HLC; the
  request absorbs successfully and writes no marker.

Duplicate row key identities in one delete request are deduplicated
before marker packing.

## Metrics

Primary operator signal:

```text
wendelon_storage_pk_delete_outcomes_total{tenant, table, outcome}
```

Outcomes are `deleted`, `already_deleted`, and `not_found`.
The same classifications are also exposed as split counters for
dashboards that avoid outcome labels:

```text
wendelon_storage_pk_delete_requests_total{tenant, table}
wendelon_storage_pk_delete_rows_deleted_total{tenant, table}
wendelon_storage_pk_delete_rows_already_deleted_total{tenant, table}
wendelon_storage_pk_delete_rows_not_found_total{tenant, table}
```

Related transition and admission metrics:

```text
wendelon_storage_pk_delete_resurrections_total{tenant, table}
wendelon_storage_pk_delete_tombstone_boundary_versions_retained_total{tenant, table}
wendelon_storage_pk_upsert_no_digest_add_total{tenant, table}
wendelon_storage_pk_admission_visibility_lookup_seconds{tenant, table}
```

Interpretation:

- Rising `deleted` confirms visible rows are being superseded by
  tombstone markers.
- Rising `already_deleted` usually indicates idempotent callers or
  repeated delete workflows.
- Rising `not_found` usually indicates stale client state or delete
  requests for rows that never reached the table.

## Retention

ADR 050 keeps ADR 048's PK retention gate in force:

- Per-table `FixedWindow` and `Disabled` retention policies remain
  rejected for PK tables with `RETENTION_REJECTED_PK_DISALLOWED`.
- PK chains continue to use cluster MVCC prefix retention.
- Tombstones are normal PK boundary versions. A below-floor tombstone
  may remain as the retained boundary sentinel to avoid resurrecting
  older data.

## Resurrection

A later PK write after a tombstone is a normal data version. The
write supersedes the tombstone and makes the row visible again at
times at or after the resurrection HLC. Time-travel reads before the
tombstone still see the old data; reads between tombstone and
resurrection see absence.

## Troubleshooting

If PK deletes return `PBC_REJECT_LOCALITY_UNSUPPORTED`, the server
could not recover routing locality for the existing key version and
fails closed rather than writing an ambiguous tombstone marker.

If aggregate or CSB direct-answer behavior appears surprising after
PK deletes, verify the query path is observing tombstone metadata and
falling back to row-level evaluation when a tombstone can affect the
visible set.
