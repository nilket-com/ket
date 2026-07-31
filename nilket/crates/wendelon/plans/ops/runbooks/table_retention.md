# Table Retention Runbook

ADR: `plans/adr/048_per_table_retention_policy.md`

## Scope

Per-table retention is controlled by the typed
`SetTableRetentionPolicy` lifecycle mutation. Wendelon does not parse
string policy expressions in the engine.

## Pre-change Checks

- Confirm the target table uses append-only row identity before setting
  `Disabled` or `FixedWindow`; primary-key tables only accept
  `InheritCluster`.
- Read the current `policy_generation` and submit it as
  `expected_policy_generation`.
- Check recent retention activity:
  - `wendelon_storage_table_retention_policy_updates_total`
  - `wendelon_storage_table_retention_read_floor_rejects_total`
  - `wendelon_storage_table_retention_read_filtered_rows_total`
  - `wendelon_storage_table_retention_l1_rows_pruned_total{kind="data"}`
  - `wendelon_storage_table_retention_l1_rows_pruned_total{kind="tombstone"}`
  - `wendelon_storage_table_retention_l2_rows_pruned_total{kind="data"}`
  - `wendelon_storage_table_retention_l2_rows_pruned_total{kind="tombstone"}`
  - `wendelon_storage_table_retention_pair_drops_total`
  - `wendelon_storage_table_retention_ambiguous_rows_retained_total`
  - `wendelon_storage_table_retention_lag_seconds`

## Change Procedure

1. Submit `SetTableRetentionPolicy` with the intended
   `TableRetentionPolicy`.
2. If the request is rejected for stale generation, re-read the current
   policy and retry with the new generation.
3. If the request is rejected with
   `RETENTION_REJECTED_PK_DISALLOWED`, leave the table on
   `InheritCluster` and adjust the cluster retention policy instead.
4. Verify an audit event with operation
   `table.retention_policy_updated`.

## After Shortening A Window

- Expect reads with snapshots older than the new floor to fail with
  `RETENTION_EXPIRED`.
- Watch filtered-row and prune metrics before expecting physical
  space to drop. Logical filtering takes effect before L1 or L2
  compaction reclaims bytes.
- If `ambiguous_rows_retained` rises, retention is choosing safety over
  reclaim because table ownership or lifecycle resolution was unclear.
- If reject counts rise unexpectedly, restore `InheritCluster` or a
  longer `FixedWindow` using the current generation.

## Rollback

Submit a new typed lifecycle mutation:

- `InheritCluster` returns the table to the cluster retention policy.
- `Disabled` stops table-level expiry for append-only event-history
  tables.

Rollback changes generation just like any other policy update.
