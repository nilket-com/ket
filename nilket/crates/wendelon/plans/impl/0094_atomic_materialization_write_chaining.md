# Impl — Wendelon ADR 0094: Atomic materialization write chaining

Pairs with `plans/adr/0094_atomic_materialization_write_chaining.md`.
This is one cross-crate implementation: Wendelon owns saga, consensus,
outcome-pin, and wire-section authority; Toron owns source chunking and the
materialization orchestration.

## Step 0 — grounding probes

1. **P1 — current refusal:** pin CTAS/materialize, `INSERT ... SELECT`, and
   `UPDATE` above 65,536 rows as Toron `ExecutionError` failures before
   provisioning or Wendelon dispatch. This identifies the shared helper the
   implementation splits.
2. **P2 — provisional invisibility:** write rows to a provisional CTAS object
   and prove they are unreachable both through name lookup and a direct
   object-id `TABLE_QUERY_REQ`. Any successful read blocks the ADR.
3. **P3 — outcome trivalence:** prove committed,
   `KNOWN_NOT_COMMITTED`, and unknown/expired/indeterminate survive through
   `StorageWrite::resolve_mutation` to CTAS recovery without collapsing.
4. **P4 — pin substrate:** pin `OUTCOME_PIN_CAPACITY = 1_024`, idempotent
   install/acknowledgement, and the absence of per-child Control Ring
   attestation.
5. **P5 — identity isolation:** prove `row_write_id.client_uuid` is derived
   from the CTAS operation/query id rather than a shared session id.

The probe results ride the implementation receipt. P2 and P5 are promotion
gates: a failed premise reopens the design before production code changes.

## Step 1 — chain state and identity

### 1a. Structured child identities

Add a shared helper beside `ClientWriteId`:

```text
chain_chunk_write_id(base, 0) = base
chain_chunk_write_id(base, i) = ClientWriteId {
    client_uuid: base.client_uuid,
    sequence_no: base.sequence_no.checked_add(i),
    idempotency_ttl_ms: base.idempotency_ttl_ms,
    _pad: base._pad,
}
```

Return a typed error on overflow. Add `CTAS_MAX_WRITE_CHUNKS = 128` in the
shared schema surface and re-export it through `wendelon-client`. A source
requiring more is rejected before provisioning.

Controls cover C6: byte-identical index zero; preserved UUID/TTL/padding;
checked overflow; canonical distinctness within one chain and across sagas;
P5's per-operation UUID premise.

### 1b. Saga declaration and cleanup debt

Extend `CtasSagaEntry` with:

- `chunk_count: Option<u16>`; together with `row_count`, `Some` means the
  complete chain is declared;
- durable chain-pin cleanup completion, false for a declared chain until all
  child pins have been acknowledged.

Add `DeclareChain { row_count, chunk_count }`, legal once in `Provisioned`.
It validates nonzero count, the 128-child bound, derivation overflow, and an
absent prior declaration without changing phase. `MarkRowDispatched` keeps
its wire fields and validates that its row count equals the declaration.
Toron invokes it only after the complete pin-installation loop succeeds; the
Control Ring cannot observe the data-ring pin store and does not pretend to
validate that external fact itself.

Add a mutation that records pin cleanup completion after idempotent data-ring
acknowledgement. It is legal for `RowsCommitted` and `Reclaimed`, idempotent,
and is required before terminal saga retention may prune a declared entry.
Resume/recovery checks pending cleanup before returning a terminal result.

Controls: declaration is one-shot and phase-bounded; mismatched dispatch is
rejected without state change; terminal pruning refuses pending cleanup;
cleanup marking is idempotent.

## Step 2 — consensus compatibility

### 2a. Snapshot v3 to v4

- Freeze the outgoing `SmState` plus CTAS catalog/entry spine as
  `snapshot_body.rs::v3`.
- Dispatch declared version 3 through it and consume exactly.
- Upconvert phase-aware: `row_count: Some(_)` becomes
  `chunk_count: Some(1)` with cleanup pending; an untouched historical
  `Provisioned` entry with no row count remains undeclared.
- Bump current snapshot format to 4.
- Add immutable v3 and current fixtures, including active Provisioned,
  RowDispatched, RowsCommitted, and terminal sagas; extend the ADR 0092 exact
  fixture/corpus guards.

### 2b. WAL body v2 to v3

- Freeze the outgoing `WendelonRequest`/`CtasSagaMutation` spine as
  `wal_body.rs::v2`.
- Add a replay-only current top-level request carrying the fields of legacy
  `MarkRowDispatched`. The v2 converter maps one old request to this one
  current request. State-machine apply atomically sets `row_count`, declares
  one child, marks cleanup pending, and enters `RowDispatched`.
- No live CTAS/control API can construct the replay request. An architecture
  guard limits references to the v2 decoder, request definition, and apply
  arm; injection from a live path fails the guard.
- Bump current WAL body format to 3; add immutable v2/current bodies for every
  request/mutation variant and extend exact-set guards.

Controls recover mixed v3-snapshot plus v2-tail states and compare them to a
current one-child twin. Historical chunk zero resolves the original
`row_write_id`. Hash-matched recognized-version mismatches retain ADR 0092's
format classification.

### 2c. Control-wire disposition

Document and guard the operational requirement: Wendelon,
`wendelon-client`, and Toron are upgraded stop-the-world for this cut.
`CtasSagaRequestWire`/`ResponseWire` remain unversioned; no mixed-version
decoder or compatibility claim is introduced. The implementation receipt
states the requirement without citing ADR 0574.

## Step 3 — authoritative table-write section encoding

Factor Wendelon's private `ClientBatch` encoder into a public
`wendelon-client` table-write-section function. It:

- uses the exact Arrow IPC options used by `table_write_once`;
- validates encoded length against the receiver's single
  `MAX_SECTION_BYTES` definition;
- returns encoded bytes or a typed over-cap result.

The receiver imports that same constant. Toron receives only the function and
typed result, never a copied byte constant. Refactor `table_write_once` to use
the function. Add an architecture guard whose exact caller census includes
the Toron chunker and Wendelon client send path; an alternate local encoder or
Toron byte-cap literal fails it.

C2 uses incompressible wide rows and asserts its premise: a row-maximal batch
encodes above 5 MiB while each individual row fits. Every emitted chunk is
re-encoded by the shared API and is within the cap. A single over-cap row is a
typed pre-provision refusal.

## Step 4 — Toron chunk planning

Split `materialize_write_select_batch` into shared coercion plus two policies:

- CTAS/materialize accepts the complete source and returns the coerced batch;
- `INSERT ... SELECT` and `UPDATE` retain the 65,536-row rejection, with a
  typed message naming materialization as the alternative.

For materialization, before `provision_ctas`:

1. slice a candidate at no more than 65,536 rows;
2. call the shared encoder/cap API;
3. halve on over-cap until it fits;
4. refuse an individually over-cap row;
5. reject more than 128 resulting chunks;
6. preserve row order and verify chunk rows sum to the source total.

Allocation-size estimates may seed a candidate but never replace encoded
validation. C1 uses 200,001 rows and checks exact frame count, per-frame row
bound, order, and total. Boundary controls retain cap-sized gate-less writes
and reject cap-plus-one without provisioning or dispatch.

## Step 5 — ordered pinning, dispatch, and recovery

The live path is:

1. execute source and compute the complete plan;
2. provision the saga/object;
3. apply `DeclareChain`;
4. pin all derived ids, with no dispatch during the loop;
5. apply `MarkRowDispatched` only after every pin succeeds;
6. send chunks sequentially and verify each write response against its chunk;
7. apply `AttestRowsCommitted` only when every child is committed;
8. acknowledge all pins, durably mark cleanup complete, then publish.

Pin-capacity failure or a crash during step 4 leaves a declared
`Provisioned` saga. Recovery knows no child dispatched, applies `Reclaim`,
acknowledges every possibly-installed pin, and marks cleanup complete. An
acknowledgement failure remains durable cleanup debt and is retried.

For `RowDispatched`, resolve every child:

- all committed: attest the total, clean pins durably, publish;
- any explicitly known-not-committed: reclaim, clean pins durably;
- any unknown/expired/indeterminate: record retry debt, retain every pin,
  remain unpublished.

Terminal resume and background saga reconciliation process pending pin cleanup
before terminal pruning. A cleanup crash after `RowsCommitted` and one after
`Reclaim` are test-support seams; both must converge to zero pins.

## Step 6 — end-to-end controls

- **C3 terminal:** force child `k` to explicit known-not-committed after an
  earlier commit; assert reclaim, unchanged name binding, provisional rows
  unreachable/reclaimed, no published attachment, no receipt, and zero pins.
- **C3 indeterminate:** erase/sever child outcome evidence; assert retry debt,
  no reclaim, no visibility, no attachment/receipt, and all pins retained.
- **C4:** stop after final child commit but before `AttestRowsCommitted` and
  again after attestation but before publish. The saga may contain pending
  certificate bytes, but the attachment catalog and caller have none. Recovery
  publishes one attachment and a receipt with the declared total.
- **C5:** induce pin-capacity exhaustion mid-installation and crashes after
  durable `RowsCommitted`/`Reclaim` before acknowledgement. Assert no child
  dispatched in the first case and eventual zero pins plus durable cleanup in
  every terminal case.

No new production test hook is needed for these controls. Existing injectable
catalog and storage-write seams induce each failure deterministically. The
controls run in the default suite and are also repeated through ADR 0093's
non-default `test-support` forwarding leg so the cross-crate acceptance command
remains explicit.

After C1–C6 are green, run 200k and 1M materializations on the performance
rail and record them as forward evidence, never as correctness substitutes.

## Verification gate

Run strictly sequentially after formatting:

```text
scripts/cargo-cgroup fmt  -p toron -p wendelon -- --check
scripts/cargo-cgroup test -p wendelon --lib
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p wendelon --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p wendelon --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron --features test-support --test <target> adr0094_
git diff --check
```

Replace `<target>` with the single named integration target that owns C3–C5;
do not use a broad feature-enabled sweep as acceptance. Add `--all-targets` if
the new fields reach a widely constructed reporting type. The implementation
receipt records P2/P4/P5, snapshot/WAL fixture versions, lockstep deployment,
C1–C6, and the forward performance numbers.

## Guard reconciliation

- Extend the snapshot and WAL body exact-set/corpus guards; do not weaken ADR
  0092 classification.
- Add the replay-only request confinement guard.
- Extend ADR 0093's test-support hook census for new seams.
- Add the shared encoder caller census and forbid Toron byte-cap literals.
- Pin source-before-provision and declaration/pin/dispatch ordering with a
  discriminating architecture control.

## Implementation evidence

- **P2 — provisional invisibility holds on the real data route.** The control
  provisions a saga, writes a genuine row to the provisional object, and sends
  a direct object-id `TABLE_QUERY_REQ`. Wendelon refuses the query before
  publication; name lookup is absent at the same point. The ADR's atomicity
  premise therefore rests on read behavior, not only catalog state.
- **P4 — the existing pin substrate is sufficient but deliberately bounded.**
  The durable store retains `OUTCOME_PIN_CAPACITY = 1_024`, idempotent install,
  and idempotent acknowledgement. A chain declares at most 128 children. The
  implementation installs the complete pin set before dispatch and records
  cleanup only after every acknowledgement succeeds; partial installation is
  reclaimed without dispatch and cleans the complete declared id set.
- **P5/C6 — child identity is operation-local and arithmetic.** CTAS derives
  `row_write_id.client_uuid` from the query/operation identity. Child zero is
  the historical id byte-for-byte; later children preserve UUID, TTL, and
  padding while checked addition advances the sequence. Overflow is a typed
  pre-provision refusal.
- **Consensus compatibility:** the snapshot format is v4 and WAL body is v3.
  Frozen v3 snapshot and v2 WAL spines upconvert historical one-child sagas;
  immutable v3/current mutation, snapshot, and WAL corpora are guarded by
  exact variant sets. The CTAS control carrier remains an explicitly lockstep,
  unversioned deployment seam; `TABLE_WRITE_REQ` itself is unchanged.
- **C1/C2:** 200,001 rows publish as four ordered children, each at or below
  65,536 rows. Incompressible wide-row controls size through the same Arrow
  encoder and 5 MiB section authority used by the live client, split below the
  row cap when required, and reject a single oversized row before provision.
- **C3/C4:** an explicit known-not-committed child reclaims while an
  indeterminate child retains retry debt and every pin. Neither path exposes a
  name, attachment, or receipt. A fully committed chain likewise has no public
  attachment or caller receipt until publication; recovery publishes exactly
  one attachment and the declared row total.
- **C5:** induced pin-install failure dispatches no child and releases the
  declared set. Crashes after durable `RowsCommitted` and durable `Reclaimed`
  leave cleanup debt that recovery converges to zero pins before terminal
  return or publication.
- **Control disposition:** no new ADR 0094 hook was added. C3-C5 use existing
  injectable catalog/storage seams and run under both the default `ctas_test`
  target and the explicit `--features test-support` acceptance leg. This is
  stronger than a feature-only control and leaves ADR 0093's hook census
  unchanged.
- **Verification:** formatting and diff checks are clean; Wendelon lib is
  1,257/0 and architecture 174/0, with its complete all-target sweep exiting
  zero. Toron lib is 1,697/0, architecture 2,145/0, and its complete all-target
  sweep exits zero; the lib sweep uses `RUST_MIN_STACK=33554432` for the
  pre-existing deep binder test surface. Toron TPCH is green (lib 198/0 with
  four ignored, binary 52/0, architecture 22/0, correctness 176/0 with two
  ignored).
- **Forward evidence:** the 200k/1M viz performance rail was not used as a
  correctness substitute and remains the first post-landing measurement.

## Post-landing chain-descriptor correction

The first viz acceptance run exposed a missing live-path gate: provisional
descriptor lookup compared each table-write id only with the saga's historical
`row_write_id`. That admitted chunk zero but rejected the first derived child as
`PBC_REJECT_SCHEMA_UNKNOWN`. The same equality existed on leader admission and
replica ingest.

`CtasSagaEntry::contains_row_write_id` now admits the durably declared,
operation-local half-open sequence interval while preserving the undeclared
single-id behavior. Both leader and replica descriptor fallbacks use that one
predicate, and an architecture census pins both call sites. The behavioral
control admits a declared child with a changed TTL, refuses the range end, and
was injection-verified against the original equality.

The missing product-path acceptance now materializes 199,999 rows with two
`Float64` columns through the public Rune client, Toron daemon, live Wendelon
table-write handler, and four-frame chain, then reads back all 199,999 rows.
Separately, public table-write refusals retain their typed `PbcRejectCode` and
render its stable name plus hex value instead of claiming every rejection is an
invalid PBC program.

The corrective seal is green: Wendelon lib 1,258/0 with eight ignored and
architecture 175/0; Toron lib 1,697/0, architecture 2,145/0, SQL listener 29/0
with one ignored, and both complete cross-ring test sweeps exit zero. Toron's
lib sweep retains the established enlarged test-thread stack for its unrelated
deep binder surface.
