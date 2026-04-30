# ADR 051 Descriptor Fingerprint Split

Owner: `H6`
Depends On: ADR 051

## When To Use This

Use this runbook during ADR 051 rollout, after descriptor physical
retunes, or when CSB direct-answer / merge efficiency drops while
logical schemas appear unchanged.

ADR 051 splits descriptor identity into:

- `logical_schema_fingerprint`: table identity, Arrow schema,
  row-identity policy, and schema-evolution policy.
- `physical_descriptor_fingerprint`: logical fingerprint plus
  locality, record layout, record-body compression, and stats policy.

Existing aggregate, CSB, columnar, and tiered-stats cache consumers
bind to the logical fingerprint. A physical retune may change storage
shape without invalidating logical CSB or aggregate compatibility.

## Metrics

| Metric | Labels | Meaning |
|--------|--------|---------|
| `wendelon_storage_csb_merge_logical_fingerprint_mismatch_total` | `tenant`, `table` | A CSB direct-answer or merge candidate crossed a table / generation / logical-fingerprint boundary and was rejected. |
| `wendelon_storage_csb_payload_coverage_fall_through_total` | `tenant`, `table`, `reason` | The logical fingerprint matched, but parsed stats payload coverage was insufficient for the requested aggregate. |
| `wendelon_storage_csb_merge_pre_adr_051_cliff_total` | `tenant`, `table` | A CSB matched table and generation but carried pre-ADR-051 slot bytes or otherwise failed the new logical fingerprint interpretation. |
| `wendelon_storage_aggregate_cache_logical_fingerprint_hit_total` | `tenant`, `table` | Aggregate response cross-bind checks whose logical fingerprint matched the request. |
| `wendelon_storage_aggregate_cache_logical_fingerprint_miss_total` | `tenant`, `table` | Aggregate response cross-bind checks whose logical fingerprint differed from the request. |
| `wendelon_storage_descriptor_recompute_seconds` | `tenant`, `table` | Descriptor logical and physical fingerprint recompute duration on schema-update submission. |
| `wendelon_storage_descriptor_validate_seconds` | `tenant`, `table` | Descriptor validation duration on schema-update submission. |
| `wendelon_storage_descriptor_logical_only_mutations_total` | `tenant`, `table` | Accepted descriptor updates whose logical fingerprint changed while physical fingerprint did not. Expected zero by ADR 051 stacking; nonzero indicates an invariant breach or future preimage change. |
| `wendelon_storage_descriptor_physical_only_mutations_total` | `tenant`, `table` | Accepted descriptor updates whose physical fingerprint changed while logical fingerprint stayed stable. |
| `wendelon_storage_descriptor_combined_mutations_total` | `tenant`, `table` | Accepted descriptor updates whose logical and physical fingerprints both changed. |

Current fall-through reasons:

- `column_uncovered`: no parsed entry for the requested column, or the
  parsed Arrow stats tag did not match the descriptor's Arrow type.
- `missing_count`: `COUNT_NON_NULL` needed a non-null count payload that
  the CSB did not carry.
- `missing_min_max`: `MIN` / `MAX` needed a min/max flag and payload that
  the CSB did not carry.

## First Checks

1. Confirm whether the rate increase started immediately after an ADR
   051 rollout or schema-descriptor retune.
2. Compare the table's current logical and physical fingerprints in the
   catalog or the `table.schema_descriptor_updated` audit event. A
   physical-only change should preserve the logical fingerprint.
3. Check whether affected slabs were written before ADR 051. Pre-ADR-051
   CSBs reused the same 32-byte header slot for the old monolithic
   fingerprint, so they should miss until compaction rewrites them.
4. For payload-coverage fall-throughs, group by `reason` and inspect the
   table's `ColumnStatsPolicyV1`.

## Interpretation

- Rising logical-fingerprint mismatches during rollout are expected while
  old CSBs remain in L2. The rate should decay as slabs compact or are
  rewritten.
- A persistent logical-fingerprint mismatch for new slabs means the CSB
  header, aggregate request, or catalog descriptor is not binding the same
  logical axis. Treat that as an ADR 051 routing regression.
- Payload fall-through with `missing_min_max` after a stats-policy retune
  is graceful degradation: the system scans instead of answering directly
  from older CSBs.
- Payload fall-through with `column_uncovered` after an Arrow schema
  change usually means the aggregate references a column not covered by
  the candidate CSB's logical schema generation.
- The `table.schema_descriptor_updated` audit event carries
  `descriptor_mutation_axis`, previous/current logical fingerprints, and
  previous/current physical fingerprints. Use it to distinguish a
  physical retune from an actual logical schema change.

## Mitigation

- Let normal compaction rewrite old slabs when the mismatch rate is
  decaying and query latency remains inside SLO.
- Trigger a targeted compaction / rewrite for the affected table if
  pre-ADR-051 CSBs keep causing material aggregate latency.
- For `missing_min_max` or `missing_count`, update stats policy only if
  the table needs CSB direct answers for that aggregate shape; otherwise
  fall-through is acceptable.
- If new slabs still produce logical mismatches, stop the descriptor
  rollout and inspect ADR 051 routing guards before issuing more schema
  updates.

## Escalation

Escalate when either metric remains nonzero for newly-written slabs after
the compaction soak window, or when aggregate p99 regresses because CSB
direct answers no longer survive physical-only descriptor retunes.
