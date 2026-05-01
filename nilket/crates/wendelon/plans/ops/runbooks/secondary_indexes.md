# Secondary Indexes Runbook

ADR: `plans/adr/052_secondary_indexes.md`

## When to Use This

Use this runbook during secondary-index rollout, when an index remains
`building` or `dropping` longer than expected, or when query latency changes
after an indexed predicate starts planning through sidecar segments.

ADR 052 indexes one non-clustering user column per index. Index entries live in
local sidecar segments beside retained source slabs. The catalog lifecycle is:

- `absent -> building`: create accepted; writes start admitting active-buffer
  entries and backfill covers retained slabs.
- `building -> ready`: every retained source slab is covered and the replicated
  `IndexBackfillComplete` mutation is applied.
- `ready -> dropping`: drop accepted; planners stop using the index.
- `dropping -> absent`: every current voter has applied local drop-GC and
  committed `IndexDropGcComplete`.

## Metrics

Primary catalog and lifecycle signals:

```text
wendelon_storage_secondary_index_create_total{tenant, table, index}
wendelon_storage_secondary_index_drop_total{tenant, table, index}
wendelon_storage_secondary_index_lifecycle_transitions_total{tenant, table, index, from, to}
wendelon_storage_secondary_index_uncovered_slabs_total{tenant, table, index}
wendelon_storage_secondary_index_backfill_seconds{tenant, table, index}
wendelon_storage_secondary_index_drop_gc_seconds{tenant, table, index}
```

Sidecar build and active-write signals:

```text
wendelon_storage_secondary_index_segments_built_total{tenant, table, index}
wendelon_storage_secondary_index_segments_failed_total{tenant, table, index, reason}
wendelon_storage_secondary_index_entries_indexed_total{tenant, table, index}
wendelon_storage_secondary_index_active_buffer_failures_total{tenant, table, index, reason}
```

Query-path and fail-closed signals:

```text
wendelon_storage_secondary_index_query_plans_total{tenant, table, index, mode}
wendelon_storage_secondary_index_candidate_rows_total{tenant, table, index}
wendelon_storage_secondary_index_visible_total{tenant, table, index}
wendelon_storage_secondary_index_superseded_total{tenant, table, index}
wendelon_storage_secondary_index_stale_indexed_value_total{tenant, table, index}
wendelon_storage_secondary_index_segment_state_digest_mismatch_total{tenant, table, index}
wendelon_storage_secondary_index_logical_fingerprint_mismatch_total{tenant, table, index}
wendelon_storage_secondary_index_corruption_failures_total{tenant, table, index}
```

Audit records use `secondary_index.catalog_updated` with
`SecondaryIndexCatalogUpdate` detail. The record carries the index id, table id
when known, lifecycle state before and after the mutation, indexed column,
logical schema fingerprint, and the replica id for lifecycle-complete commits.

## Reject Taxonomy

Descriptor validation rejects use the `INDEX_REJECT_*` prefix:

| Code | Meaning |
|------|---------|
| `INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH` | Descriptor table id, schema generation, or logical schema fingerprint does not match the current table descriptor. |
| `INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE` | The requested indexed column type or encoding is unsupported. |
| `INDEX_REJECT_PK_COLUMN` | The requested index targets the table's declared primary-key column. |
| `INDEX_REJECT_SYNTHETIC_COLUMN` | The requested index targets a storage synthetic column. |

Schema-mutation rejects use the schema-update taxonomy:

| Code | Meaning |
|------|---------|
| `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED` | Secondary-index `Alter` is not supported; create a new index and drop the old one instead. |
| `SCHEMA_UPDATE_REJECT_INDEX_OBJECT_ID_COLLISION` | The requested `index_object_id` already exists with different catalog metadata. |
| `SCHEMA_UPDATE_REJECT_INDEX_NAME_COLLISION` | The `(tenant, namespace, index_name)` binding already exists. |
| `SCHEMA_UPDATE_REJECT_INDEX_COLUMN_COLLISION` | A non-dropping index already exists for the same `(tenant, table, column_ordinal)`. ADR 052 V1 allows one live index per table column. |

## Active Buffer Bound

`max_index_buffer_bytes` bounds the per-index active buffer used while a source
slab is still open. Hitting the bound does not reject the write. The node drops
the active buffer, marks the source slab uncovered, increments
`wendelon_storage_secondary_index_active_buffer_failures_total`, and later
rebuilds that slab through normal backfill.

Reason labels to search for:

| Label | Meaning |
|-------|---------|
| `active_buffer_overflow` | The active buffer exceeded `max_index_buffer_bytes`; coverage should retry from the retained source slab. |
| `encode_failure` | A row could not be encoded into an index entry. |
| `seal_failure` | The active buffer could not be sealed into a sidecar segment. |
| `persist_failure` | A sidecar segment was built but could not be persisted or indexed locally. |
| `corrupt_segment` | A sidecar failed decode or validation and is treated as uncovered. |
| `missing_segment` | Catalog metadata pointed at a sidecar that was not present locally. |
| `compaction_race` | Coverage changed while compaction was rewriting source slabs; retry coverage after reconciliation. |

## First Checks

1. Confirm the catalog state. `building` means planners must not use the index;
   `dropping` means planners must fall back to full scans while local sidecars
   are removed.
2. Check `wendelon_storage_secondary_index_uncovered_slabs_total`. A nonzero
   value means local coverage is incomplete and backfill should retry.
3. Group `wendelon_storage_secondary_index_segments_failed_total` by `reason`.
   The labels in the active-buffer section describe whether the node needs
   backfill retry, disk investigation, or corrupt-sidecar handling.
4. Compare `query_plans_total{mode="indexed"}` to
   `query_plans_total{mode="fallback_full_scan"}` for the same table and index.
5. For unexpected low selectivity, inspect candidate revalidation:
   `superseded_total` is MVCC churn, while `stale_indexed_value_total` means the
   indexed column changed after an older sidecar entry was written.

## Interpretation

- `NULL` indexed values are omitted from sidecar entries. Queries that require
  `NULL` matching must use full scan behavior.
- A rising `superseded_total` is normal on update-heavy tables. It measures
  index storage amplification from retained historical versions, not incorrect
  results.
- `logical_fingerprint_mismatch_total` indicates a descriptor / sidecar binding
  issue. New sidecars should bind to the table logical schema fingerprint only.
- `segment_state_digest_mismatch_total` or `corruption_failures_total` means the
  sidecar failed validation. The local node must mark that source slab
  uncovered and avoid serving indexed results from the corrupt segment.
- Drop-GC is complete only after all current voters have committed
  `IndexDropGcComplete`. A single replica confirmation is insufficient.

## Mitigation

- For `building` stalls with uncovered slabs, let backfill retry first. If the
  uncovered count is stable and segment failures continue, inspect local disk
  and sidecar persistence errors on the affected node.
- For corrupt sidecars, remove no catalog state manually. Rebuild coverage from
  retained source slabs or allow compaction/backfill to replace the sidecar.
- For query regressions from high `superseded_total`, evaluate whether the table
  needs compaction or a narrower retention window. Do not change PK/table
  retention policy to a mode rejected by existing retention invariants.
- For `dropping` stalls, identify current voters that have not committed
  `IndexDropGcComplete`; retry local sidecar deletion there before expecting the
  catalog row to disappear.

## Escalation

Escalate when a ready index emits corruption or logical-fingerprint mismatch
metrics for newly-built sidecars, when uncovered slabs remain nonzero after
backfill retries, or when a dropping index cannot reach absent because a current
voter repeatedly fails local drop-GC.
