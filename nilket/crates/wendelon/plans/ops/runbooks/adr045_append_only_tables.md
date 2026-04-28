# ADR 045 Append-Only Tables

Owner: `H6`
Depends On: ADR 012, ADR 018, ADR 037, ADR 042, ADR 043, ADR 044

## When To Use This

Use this runbook during and after rollout of ADR 045 PK-less
append-only tables, or when an append-only ingest workload starts
behaving differently from baseline. The append-only path composes
on top of ADR 042 multi-row records, ADR 043 LZ4 compression, and
ADR 044 column-projected fetch — most observability lives in
those existing surfaces. ADR 045 adds a small operational layer
on top.

Primary triggers:

- `storage_append_only_intra_batch_index_exhaustion_total > 0` for
  any tenant.
- `storage_append_only_supersession_metadata_skipped_total` is zero
  on a tenant with sealed append-only slabs (regression: the CSB
  short-circuit's append-only fast path is not firing).
- A tenant's append-only ingest throughput diverges from PK-keyed
  baseline by more than 10%.
- A tenant declares a `__row_*` user column and expects it to
  survive admission (it should reject — defense-in-depth).
- `RowIdentityPolicy` shape mismatch: a tenant's table appears as
  PK-keyed in the catalog but writes are arriving without a PK
  column (or vice versa).
- A claimed `key_identity` collision (a second `RecordLocation` for
  the same `(tenant_id, key_identity)`) — cryptographically
  negligible but the ADR says to escalate.

## When To Declare A Table `APPEND ONLY` Vs PK-Keyed

Declare `APPEND ONLY` when:

- Each row is its own fact (logs, audit trails, time-series
  emissions, CDC sinks, sensor / metric readings).
- Two events with identical bytes at distinct ingest moments must
  be preserved as distinct rows.
- The workload is scan-shaped: full-table scans, range scans,
  analytical aggregates with predicates over user columns.
- Wide schemas with narrow projections (canonical ADR 044
  workload).

Declare a PK-keyed table when:

- Rows have a natural unique business key.
- You need point lookups by that key.
- You need update / upsert by key.
- You need delete-by-key (V1 — append-only delete lands in a
  follow-up ADR).

The decision is *not* reversible without re-creating the table.
The descriptor's `RowIdentityPolicy` is part of the fingerprint
and an alter cannot change it.

## Why Duplicate Payloads Are Preserved

ADR 045 §3 / §4: append-only identity is rooted in the write-
context tuple `(writer_node_id, row_hlc, intra_batch_index)`, not
in payload bytes. Two heartbeats with identical payload at
distinct write contexts produce distinct `__row_key_id__` values
by construction. This is the contract — the platform does not
silently dedupe identical events. Tenants who want dedupe-on-write
wait for the future content-hash identity ADR.

## What Row-Level Delete Looks Like (V1: Not Yet)

V1 ADR 045 ships *insert-only*. Update / upsert / delete-by-key
against `RowIdentityPolicy::AppendOnly` descriptors are rejected
by the storage-side validator. Retention in V1 is at the table /
range / slab lifecycle level (drop the table, reclaim the slab,
or wait for a future per-table retention ADR).

The follow-up row-tombstones ADR is sketched in ADR 045's
"Forward-compatibility analysis" with a concrete shape:
`__row_tombstone__: Bool` synthetic column,
`has_tombstones: bool` ADR 022 footer bit, `DeleteRows`
request shape, scan-local visibility set, retention-floor
compaction GC. Until that ADR ships, GDPR / audit-trail row
deletes are the trigger to escalate.

## Metric Names

Prometheus deployments render metrics with the repository
namespace prefix. The queries below use `$P` for that prefix.

Functional segments to watch:

- `storage_append_only_batches_total` (incremented on the
  server's success path only — after lease gate, idempotency
  replay miss, quota, and durable quorum)
- `storage_append_only_rows_written_total` (same site as
  `storage_append_only_batches_total`)
- `storage_append_only_intra_batch_index_exhaustion_total`
- `storage_append_only_supersession_metadata_skipped_total`

Plus the inherited ADR 042 / 043 / 044 metrics, which carry the
bulk of the storage observability for append-only tables:

- `storage_multirow_rows_per_record` (sanity check that
  append-only writes land in MultiRow records as required).
- `storage_multirow_recompact_total` (compaction load).
- `storage_compression_ratio{codec="lz4_frame"}` (compression
  density on append-only tables).
- `storage_l2_column_fetch_record_body_bytes_avoided_total`
  (cold-tier scan efficiency on wide append-only schemas).
- `agg_csb_short_circuit_total` divided by
  `agg_request_total` (CSB direct-answer hit rate).

## Reading The Metrics

### Adoption

```text
sum by (tenant) (rate($P_storage_append_only_batches_total[5m]))
```

Tenants whose rate is non-zero are running on the ADR 045 path.
Tenants whose rate is zero are PK-keyed.

### Ingest Throughput vs PK-Keyed Baseline

```text
rate($P_storage_append_only_rows_written_total[5m])
  / rate($P_storage_typed_table_rows_written_total[5m])
```

For tables of comparable shape, append-only should be within 10%
of PK-keyed throughput. Append-only's identity synthesis is
cheaper than PK canonicalization on most schemas (no PK column to
canonicalize, no duplicate-PK check), so a *win* is plausible.

### Identity-Synthesis CPU

Append-only identity synthesis (BLAKE3 over the ADR 045 §4
preimage) is measured by the dedicated bench
`benches/append_only_tables.rs`, not by an always-on
production histogram. Phase 8 deleted the
`storage_append_only_identity_synthesis_micros` Prometheus
histogram: a per-row timer adds non-trivial CPU for a metric
that is only useful on bench-grade comparisons, and the bench
is the right measurement surface. To investigate identity
CPU on a candidate workload, run

```sh
cargo bench --bench append_only_tables -- --sweep
```

and compare `case=identity-synthesis` per-row µs against
admission and packing.

### `IntraBatchIndex` Exhaustion

```text
increase($P_storage_append_only_intra_batch_index_exhaustion_total[1h])
```

Should remain zero in steady state. Non-zero means a single write
request tried to admit > 4 G rows. The single-batch limit is
already `TABLE_WRITE_MAX_ROWS = 65_536`, so this metric should
*never* fire under normal operation. Non-zero is either a tenant
shape bug (oversized batch slipped past upstream limits) or a
configuration drift. Escalate.

### Supersession-Skip Rate

```text
sum(rate($P_storage_append_only_supersession_metadata_skipped_total[5m]))
```

Should be non-zero on every tenant with sealed append-only slabs.
If zero on such a tenant, the writer is incorrectly populating
`supersession_metadata` for append-only — regression bug. File a
ticket against the storage path.

### CSB Direct-Answer Rate On Append-Only

```text
rate($P_agg_csb_short_circuit_total[5m])
  / rate($P_agg_request_total[5m])
```

Append-only is the workload that benefits *most* from CSB
short-circuit because the supersession axis is permanently None.
The ratio should run materially higher than on PK-keyed tables
once stat-prunable predicates land at scale.

### ADR 044 Column-Fetch On Append-Only

Append-only is the canonical workload ADR 044's planner thresholds
were tuned for. On append-only tenants:

- `bytes_avoided_total` should track ADR 044's Phase-11 targets
  within bench noise.
- `phase_a_round_trips` should run near 1.0 (the planner picks
  columnar fetch).
- The strategy distribution should be heavily skewed toward
  columnar.

## Common Distress Patterns

### Append-only writes start rejecting with `PBC_REJECT_INTRA_BATCH_INDEX_EXHAUSTED`

Symptom: `storage_append_only_intra_batch_index_exhaustion_total`
ticks up; writes return reject code `0x3A04`.

Cause: the admitted batch contains more rows than the
`IntraBatchIndex u32` capacity (4 G rows), or upstream batch
sizing got bypassed.

Action: cap upstream batch size at `TABLE_WRITE_MAX_ROWS =
65_536`. If the workload genuinely needs > 4 G rows per request,
escalate to the ADR 045 Open question 2 (`IntraBatchIndex` widen
to `u64`). Do not add a wraparound code path.

### Append-only descriptor admits but `MultiRow` packing fails

Symptom: descriptor validates; admission returns
`TableWriteAdmission`; storage path errors with "multi-row packing
requires MultiRow record_layout_policy".

Cause: a descriptor was constructed with `RowIdentityPolicy::AppendOnly`
but `RecordLayoutPolicy::SingleRow` somehow slipped past the
validator. This should be impossible — the validator hard-rejects
the combination.

Action: capture the descriptor bytes (`postcard::to_allocvec`),
file an ADR 045 / ADR 042 cross-cut bug. The validator must not
have run; investigate the catalog-load path.

### Tenant declares `__row_*` user column and writes reject

Symptom: descriptor with user column named `__row_hlc__` /
`__row_key_id__` / `__row_routing_locality__` rejects at admission.

Cause: ADR 045 §10 / Phase 1 validator rejects user-declared
columns that collide with the ADR 042 storage-only synthetic
column names (defense-in-depth against schema injection).

Action: rename the user column. The synthetic column namespace
`__row_*` is reserved for storage-internal columns.

### Mixed PK / append-only fleet shows no operational difference

Expected. Append-only is descriptor-driven; nothing negotiates at
the cluster level. Mixed fleets serve both shapes identically.
There is no node capability bit for append-only; do not look for
one.

### Identity-collision claim

Symptom: a second `RecordLocation` for the same
`(tenant_id, key_identity)` shows up in the redb identity index.

Cause: cryptographically negligible BLAKE3-256 collision OR a
key-management bug (writer-identity collision — ADR 018) OR an
HLC monotonicity violation (ADR 012) OR a code bug in the
identity-synthesis path.

Action: do *not* treat as ordinary corruption. Capture both
slab pointers' raw bytes, both source HLCs, both writer
identities. Surface to the storage / crypto teams immediately.
Per ADR 045 §8, the V1 implementation returns a hard error;
soft-recovery requires a council-level decision before being
enabled.

## Tool Support

The `dump_slab_record` binary surfaces ADR 045 storage identity
when asked. Default output remains user-column-only (the
synthetic columns stay invisible to ordinary inspection).

```sh
# User-column view (default).
cargo run --bin dump_slab_record -- <slab_path> <record_offset>

# Storage-identity view (ADR 042 trio: __row_hlc__, __row_key_id__,
# __row_routing_locality__).
cargo run --bin dump_slab_record -- --storage-identity <slab_path> <record_offset>
```

## Related ADRs

- ADR 037 — typed tenant tables, PK-derived row identity.
  `primary_key: PrimaryKeyDescriptor` is folded into
  `RowIdentityPolicy::PrimaryKey(...)` in ADR 045.
- ADR 042 — multi-row slab fragments. ADR 045 hard-requires this
  layout for append-only tables.
- ADR 043 — Arrow IPC body compression. Append-only's MultiRow
  default → `Lz4Frame` default.
- ADR 044 — L2 columnar fetch. Append-only is the canonical
  workload.
- ADR 040 / ADR 042 — CSB short-circuit. Append-only sealed slabs
  hit the supersession-axis-collapse fast path.
- ADR 018 — dual-CA mTLS identity. `WriterNodeId` is bound from
  the authenticated `NodeId`.
- ADR 012 — bounded clock drift / HLC monotonicity. The
  per-row HLC is an input to the append-only identity preimage.
- Brainstorm 004 — CDC. Append-only is the canonical CDC sink
  shape; CDC cursor design happens there, not in ADR 045.
