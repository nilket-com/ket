# Impl — Wendelon ADR 081: Secondary-Index Declaration and Lifecycle

Pairs with `plans/adr/0081_secondary_index_declaration_and_lifecycle.md`. The ADR was promoted
while ADR 0539/0541 confirmation was blocked, before the repository's ADR/impl dual convention was
made explicit. This implementation document is the overdue canonical companion and rides the one
`impl: adr 0081` commit with its code and evidence.

## Ground truth before implementation

- `SchemaUpdateEnvelope` and `SchemaObjectKind::Index` already carried create, drop, and lifecycle
  mutations.
- `SecondaryIndexCatalog` durably stored `Building -> Ready -> Dropping`, but backfill completion
  discarded the decoded `replica_id` and promoted on the first reporter.
- `run_secondary_index_backfill_once` and
  `gc_secondary_index_segments_for_dropping_index` had test callers only.
- table-write leaders admitted secondary-index active buffers; follower replica apply persisted
  the table rows without maintaining the corresponding buffers.
- no typed client method, CLI verb, or production status surface could declare or inspect an
  index.
- the gap gauge and refusal counter rendered as families but had no labelled series on a fresh
  profile because no declaration could establish one.

## Phase 1 — Supported declaration and observation

1. Add a validated `SecondaryIndexDescriptor::for_table_column` constructor. It derives table id,
   generation, logical fingerprint, column ordinal, and Arrow tag from the authoritative table
   descriptor; callers name the table and column rather than hand-building storage coordinates.
2. Add public client `create_secondary_index`, `drop_secondary_index`, and
   `list_secondary_indexes` methods. Mutations continue over `SchemaUpdateEnvelope`; status is an
   observation-only request/response pair because it must collect node-local coverage.
3. Add `wendelon index create|drop|status` using operator mTLS, one bootstrap peer, and the typed
   descriptor constructor. Status fails if a current data replica is unreachable rather than
   silently omitting it from a green view.
4. Update the secondary-index runbook with commands, receipt semantics, and the distinction
   between durable coverage gaps and process-local failure records.

## Phase 2 — One exact declaration cut

1. Install one manager-owned async read/write admission gate. Table-write leader admission and
   follower replica apply hold a read guard from catalog snapshot through durable append and
   active-buffer admission.
2. The lifecycle driver takes the write guard for declaration, fsyncs and rotates the active slab,
   then initializes the next active buffer.
3. Do **not** seal a suffix-only buffer created after catalog publication. It may omit rows written
   earlier in the same active slab while falsely marking that complete source slab covered.
   Discard the partial buffer and backfill the complete sealed slab. The declaration fixture pins
   this by placing one row before and one after publication; the unsafe behavior returns one entry,
   while the required cut returns two.
4. Maintain active buffers on replica apply as well as the leader so every local source slab has
   the same lifecycle guarantees.

## Phase 3 — Durable per-replica receipts and voter-aware Ready

1. Persist initial-backfill receipts in the catalog row's existing voter-receipt set. Its meaning
   is phase-tagged: backfill completion while Building/Ready; `mark_dropping` clears it before it
   becomes the drop-GC receipt set. Reusing that field preserves the exact postcard row shape for
   old snapshots. An `IndexBackfillComplete { replica_id }` inserts idempotently and promotes
   Building only when every current voter has a receipt.
2. Re-evaluate Building rows when Raft membership changes. Removing an incomplete voter drops its
   outstanding requirement without requiring completed replicas to re-submit. Adding a voter does
   not demote Ready.
3. Planner eligibility is local: catalog Ready plus this node's durable initial-backfill receipt.
   A new voter without a receipt scans. A replica with a receipt continues to select the index, so
   ADR 0539 refuses if a durable segment later disappears.
4. Lifecycle completion over the control carrier is restricted to an authenticated DataNode whose
   certificate node id matches the payload replica id. Operator create/drop retains tenant-admin
   mutation authorization.

## Phase 4 — Production lifecycle drivers

Spawn one node-owned lifecycle task and include it in ordered shutdown:

- Building and Ready rows run the existing coverage/backfill worker. A replica without a receipt
  first establishes its declaration cut; once historical coverage is complete it submits the
  durable completion mutation.
- Ready rows continue ordinary coverage work so slab rotation cannot strand a gap.
- Dropping rows run the existing reconciliation-gated segment GC and submit voter-aware
  `IndexDropGcComplete`.
- failures are named warnings and retry on the bounded cadence; a failed tick does not terminate
  the driver.

## Phase 5 — Immediate observability

Catalog create initializes labelled zero entries for:

```text
wendelon_storage_secondary_index_coverage_gap_slabs{tenant,table,index}
wendelon_storage_secondary_index_lookup_refusals_total{tenant,table,index}
```

The gap gauge continues to be replaced by the invariant-derived value on every coverage tick and
at lookup refusal. Explicit counter initialization is necessary: adding zero intentionally remains
a no-op in the generic counter helper, and absent is not a zero baseline.

## Controls

- descriptor construction derives and validates all table coordinates;
- one completion among two voters remains Building; the second promotes Ready;
- one drop-GC completion among two voters retains Dropping; the second removes the row;
- catalog postcard round-trip retains the replica receipt;
- a legacy postcard row keeps its drop-GC receipts without requiring a new trailing field;
- membership removal promotes a now-complete Building row without replay;
- a Ready catalog row falls back on a newly added replica until its receipt exists;
- declaration waits for an admitted write, forces rotation, and backfills both pre- and
  post-publication rows from the complete sealed slab;
- the unsafe suffix-buffer seal is a discriminating red (`1` entry versus required `2`);
- create alone renders both declared metric series at zero;
- CLI parsing pins create/drop/status as explicit verbs; and
- architecture guards pin the supported surface, lifecycle task, admission cut, durable receipt,
  local planner rule, authorization, and immediate series.

## Verification gate

Run after formatting, through the repository cgroup wrapper:

```text
scripts/cargo-cgroup fmt -p wendelon -- --check
scripts/cargo-cgroup test -p wendelon --lib
scripts/cargo-cgroup test -p wendelon --test architecture_test
scripts/cargo-cgroup test -p wendelon --no-fail-fast
scripts/cargo-cgroup test -p wendelon-client --all-targets --no-fail-fast
scripts/cargo-cgroup check --workspace --all-targets
scripts/cargo-cgroup test --workspace --no-fail-fast
git diff --check
```

The live ADR 0539/0541 confirmation is deliberately not part of the implementation seal. It needs
separate deployment authorization, a clean profile, an explicitly declared index, the two metric
series present before the measured window, sustained ingest and indexed lookups, and at least one
induced slab rotation.

## Implementation receipt — 2026-08-09

All commands ran through `scripts/cargo-cgroup` on the implementation tree:

- `fmt -p wendelon -- --check`: clean;
- `test -p wendelon --lib`: 1,122 passed, 0 failed, 6 ignored;
- `test -p wendelon --test architecture_test`: 163 passed, 0 failed;
- `test -p wendelon --no-fail-fast`: every library, integration, upgrade, and doc target passed;
- `test -p wendelon-client --all-targets --no-fail-fast`: 11 passed, 0 failed;
- `check --workspace --all-targets`: exit 0, zero warnings; and
- `test --workspace --no-fail-fast`: exit 0, every workspace and doc-test target passed.

The admission-cut fixture was also run against the deliberately unsafe suffix-buffer seal before
restoring the implementation: it failed with one indexed entry where the exact cut requires two.
That red proves the control detects the pre-declaration omission rather than merely exercising the
happy path.
