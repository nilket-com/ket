# ADR 049 Content-Hash Row Identity

Owner: `H6`
Depends On: ADR 045, ADR 046, ADR 047, ADR 048

## When To Use This

Use `RowIdentityPolicy::AppendOnlyContentHash` for append-style tables
that replay facts from cold storage, queues, or batch jobs and need
identical row content to converge to one visible row.

Use plain `AppendOnly` when duplicate physical facts must remain
distinct, or when write-path CPU is more important than idempotent
content replay.

## Expected Behavior

Admission canonicalizes the logical Arrow row, derives
`__row_key_id__` under:

```text
table.append-content-row.v1
```

and synchronously checks current visible membership for that key.

- Visible key present: duplicate content is absorbed, no row is
  written, and the write returns idempotent success.
- Key absent, tombstoned, or retention-evicted: the row is stored and
  visible membership is restored.

This is collapse semantics, not multiset semantics. Counts and
aggregates see one row per canonical payload.

## Descriptor Rules

- `IdentityProjection` and `TableSpreadProjection` are valid.
- `ColumnProjection` is rejected in V1.
- `MultiRow` layout is required.
- Additive nullable columns without defaults are valid.
- Additive non-null columns or defaulted columns are rejected with
  `CONTENT_HASH_REJECTED_NON_NULL_DEFAULT`.
- Decimal precision or scale changes are rejected with
  `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION`.

## Metrics

Watch these counters:

```text
wendelon_storage_content_hash_admissions_total{tenant,table,outcome}
wendelon_storage_content_hash_duplicate_absorptions_total{tenant,table}
```

High `outcome="absorbed"` is expected during replay. Unexpectedly low
absorptions during replay usually means the producer changed canonical
payloads, table object ids, tenant ids, or descriptor evolution added
non-null content.

## Triage

For duplicate rows that were not absorbed:

1. Confirm the table uses `AppendOnlyContentHash`, not `AppendOnly`.
2. Confirm the table object id and tenant id match the original
   ingest.
3. Compare the Arrow logical values, not IPC bytes or batch layout.
4. Check for logical column renames. V1 column tags fall back to
   logical-name hashes because descriptors do not yet expose assigned
   column ids, so a rename changes canonical row identity.
5. Check whether the previous key was tombstoned or retention-evicted;
   re-ingest after either condition stores a new visible row.
6. Check descriptor changes for rejected defaults or decimal changes.

For rejected writes, surface the `PbcRejectCode` name first. The
content-hash-specific reject codes are intentionally operator-readable
so schema tooling can stop before a replay-unstable descriptor lands.
