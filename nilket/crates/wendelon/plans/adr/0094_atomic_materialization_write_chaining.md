# ADR 0094 - Atomic materialization write chaining beyond the 64Ki-row admission cap

## Context

Wendelon ADR 037 point 10 admits one public table-write frame containing at
most `TABLE_WRITE_MAX_ROWS = 65_536` rows. The row count is encoded through a
`u16` sentinel (`0` means exactly 65,536 rows); the constant, sentinel, and
all receiver admission checks are part of the frame contract and do not move.

Toron currently combines the complete output of CTAS/materialize,
`INSERT ... SELECT`, or `UPDATE` into one `RecordBatch` in
`materialize_write_select_batch`. That helper rejects more than 65,536 rows
before provisioning or any Wendelon request. ADR 037 assigned larger writes
to client- or Toron-side chunking, but no chaining mechanism was implemented.
This is the remaining product blocker for the PRODUCT-0137 200k/1M
scatter-threshold evidence.

The relevant byte limits are separate from the row limit:

- HRFT caps each frame section at `MAX_SECTION_BYTES = 5 MiB`. Receivers
  enforce this in `network/core.rs`; the generic send helper does not.
- Multi-row storage records have independent 1 MiB default and 8 MiB maximum
  packing limits. Wendelon's packer already splits records against those
  limits. They are not table-write frame limits.

Materialization already has the publication gate needed for atomic chaining.
Toron executes and validates the source before provisioning. Provisioning
creates a provisional object, but Wendelon's live descriptor lookup filters
that object: neither name lookup nor direct object-id `TABLE_QUERY_REQ` can
read its rows. The name becomes live, and the materialization certificate is
attached, only at `Published`. A failed chain can therefore leave committed
physical chunks on the provisional object without exposing a partial table;
reclaim removes that object only when non-commitment is proven.

Recovery cannot infer a chain from total row count. The 5 MiB wire bound can
split wide rows below 65,536, and recovery must not re-run the source. The
chunk count and deterministic chunk identities must therefore be durable.
ADR 548's terminal mutation outcomes provide the evidence, but they are
bounded and three-valued: committed, explicitly known-not-committed, or
unknown/expired/indeterminate. Only the explicit negative permits reclaim.

## Decision

### 1. Scope is materialization only

Toron chains writes only for CTAS/materialize, whose provisional object gives
the chain an atomic publication gate. `INSERT ... SELECT` and `UPDATE` against
live tables remain single-frame operations. Above the cap they return a typed
Toron error naming 65,536 rows and materialization as the supported
alternative; at or below the cap their behavior is unchanged.

Atomic chaining for those gate-less verbs requires a new Wendelon
chained-visibility primitive and is deferred.

### 2. Per-frame contracts remain authoritative

Neither copy of `TABLE_WRITE_MAX_ROWS`, the `u16` sentinel, any row-count
admission check, `MAX_SECTION_BYTES`, nor the storage record-packing limits is
raised or bypassed. Every child remains an ordinary `TABLE_WRITE_REQ` parent
write satisfying all existing frame and storage invariants. This ADR changes
no `TABLE_WRITE_REQ` wire shape.

### 3. The complete chain is planned before provisioning

The existing order remains:

1. execute and validate the source;
2. compute every chunk boundary and the total row/chunk counts;
3. provision the CTAS saga and provisional object;
4. durably declare the chain;
5. install all outcome pins;
6. mark rows dispatched;
7. dispatch chunks sequentially;
8. attest the chain, release its pins, and publish.

Source failure or an unrepresentable chain therefore leaves no saga or
provisional object, preserving current CTAS conflict and failure semantics.

### 4. Chain declaration and dispatch are distinct durable states

`CtasSagaEntry` gains an optional `chunk_count`; its existing optional
`row_count` and the new field together are the chain descriptor.
`DeclareChain { row_count, chunk_count }` is legal only while the saga is
`Provisioned` and both fields are absent. `MarkRowDispatched` retains its
current shape but is legal only when a matching declaration exists.

This distinction is load-bearing. A `Provisioned` declared saga is known not
to have dispatched a chunk. A `RowDispatched` saga may have done so and must
resolve outcomes three-valuedly.

### 5. Every outcome pin exists before any chunk can dispatch

After `DeclareChain` commits, Toron derives the complete child-id set and pins
all of it. Only after every pin succeeds may `MarkRowDispatched` commit; only
after that mutation succeeds may the first child be sent.

If pin installation fails, the saga remains `Provisioned`. Recovery can
enumerate the complete set from the declaration, durably reclaim the
never-dispatched provisional object, and release every installed pin. Pinning
and dispatch are never interleaved.

Pins remain installed throughout `RowDispatched`; the Control Ring has no
per-child attestation state that would justify releasing one early. After
`AttestRowsCommitted` or `Reclaim` is durable, Toron acknowledges every child
pin. `CtasSagaEntry` carries durable pin-cleanup completion state. Terminal
saga retention and pruning refuse to remove an entry while cleanup is
pending, and recovery retries idempotent acknowledgement before returning a
terminal result. Thus a crash after reclaim but before acknowledgement cannot
leak global pin capacity.

### 6. Child identities preserve `ClientWriteId`

For a base `row_write_id` and zero-based chunk index `i`:

- index 0 is the base id byte-for-byte;
- later indexes retain `client_uuid`, `idempotency_ttl_ms`, and padding, and
  set `sequence_no = base.sequence_no.checked_add(i)`.

Overflow is a typed declaration-time error. CTAS derives `client_uuid` from
the per-operation query id, not a shared session id, so one operation owns its
sequence range and concurrent sagas remain disjoint. Preserving UUIDv7 keeps
ADR 542's post-retention expiry classification intact. Index zero identity
makes every historical single-write saga a chain of one without translating
its outcome key.

### 7. One shared API owns write-section encoding and its cap

The Wendelon client factors its real `ClientBatch` Arrow encoder into a public
table-write-section API. That API encodes exactly the bytes used by
`table_write_once` and validates them against the same `MAX_SECTION_BYTES`
constant used by the receiver, returning either encoded bytes or a typed
over-cap result. Toron owns no byte-cap constant.

The materialization chunker first bounds rows, then calls this API. An
over-cap result halves the candidate until it fits; a single row that still
exceeds the section cap is refused by name. Both the chunker and the actual
send path are guarded as consumers of the shared API. Re-encoding in the send
path is accepted initially; eliminating that duplicate work requires evidence
and does not change the correctness boundary.

### 8. Chain length is bounded at 128 children

`OUTCOME_PIN_CAPACITY` is 1,024 globally. A new shared materialization-chain
constant limits one chain to 128 children, so one statement can occupy at
most one eighth of the global pin capacity. This supports at most about 8.4M
narrow rows and comfortably covers the 1M-row evidence target. The bound is
checked before provisioning; 129 children produce a typed error and no saga,
pin, or write.

This does not guarantee eight concurrent maximal chains—unrelated pins share
the same capacity. Capacity-aware admission is deferred.

### 9. Recovery preserves three-valued evidence

For a `RowDispatched` chain, recovery resolves every derived id:

- all explicitly committed: apply `AttestRowsCommitted` with the declared
  total, release every pin durably, then publish;
- any explicitly known-not-committed: apply `Reclaim` with
  `RowKnownNotCommitted`, release every pin durably, and leave the name
  unchanged;
- any unknown, expired, or indeterminate: keep the saga unpublished with
  retry debt, retain all pins, and issue no receipt.

Absence is never interpreted as non-commitment.

### 10. Snapshot and WAL bodies both advance

The saga-entry spine changes, so snapshot format v3 is frozen and current
becomes v4. Conversion is phase-aware: a historical entry with durable
`row_count` becomes a declared one-child chain; an untouched `Provisioned`
entry remains undeclared. Immutable v3/current fixtures and exact corpus
guards pin both shapes.

The mutation spine changes, so WAL body v2 is frozen and current becomes v3.
One old WAL request must convert to one current request. Old
`MarkRowDispatched` therefore converts to a replay-only top-level consensus
request that atomically installs the one-child declaration and transitions
the saga to `RowDispatched`. Live control paths cannot construct that request;
an architecture guard confines it to the v2 decoder and state-machine apply.
All other v2 mutations convert normally. Immutable v2/current WAL fixtures
and exact request/mutation corpus guards pin the conversion.

### 11. CTAS control traffic requires a lockstep upgrade

`CtasSagaRequestWire` and `CtasSagaResponseWire` are unversioned postcard
carriers. The new mutation and entry fields change their shape. This upgrade
therefore requires stop-the-world lockstep deployment of Wendelon,
`wendelon-client`, and Toron; mixed-version CTAS operation is not supported or
claimed. This is unrelated to ADR 0574's public Rune request version.

A versioned CTAS saga carrier is the named path to mixed-version deployment.

## Acceptance

The controls are independent; none substitutes for another. PRODUCT-0137's
200k/1M run is forward performance evidence, not a correctness control.

- **C1 — row-bound chunking:** a 200,001-row materialization declares the
  observed frame count; every frame has at most 65,536 rows and frame rows sum
  exactly to 200,001.
- **C2 — wire-section compliance:** incompressible wide rows force chunks
  below the row bound; every emitted section is encoded and accepted by the
  shared API at no more than the receiver's cap. A single over-cap row is
  refused before provisioning.
- **C3 — intermediate failure, two arms:** an explicit
  known-not-committed child reclaims with nothing visible and no receipt; an
  unknown child remains unpublished with retry debt, is not reclaimed, and
  has no receipt. The arms are induced separately.
- **C4 — no premature publication evidence:** before `Published`, the
  materialization attachment catalog has no certificate and the caller has no
  receipt, including after the final child commits. The pending certificate
  bytes inside the saga are expected. After completion or recovery, the
  attachment and receipt report the declared total.
- **C5 — pin lifecycle:** failure or capacity exhaustion during all-pin
  installation leaves a declared `Provisioned` saga and no dispatched child;
  recovery reclaims and releases every installed pin. Pins remain throughout
  `RowDispatched`. Crashes after durable `RowsCommitted` and after durable
  `Reclaim`, but before acknowledgement, are recovered to zero chain pins and
  durable cleanup completion. Neither terminal entry can be pruned first.
- **C6 — derivation:** child zero equals the base id byte-for-byte; UUID, TTL,
  and padding survive; checked overflow changes no saga state; canonical ids
  are distinct within and across concurrent sagas.
- **P2 — publication premise:** before `Published`, provisional rows are
  unreachable by both name resolution and direct object-id
  `TABLE_QUERY_REQ`. This probe gates implementation.

## Guardrails

- Source execution and complete planning precede provisioning.
- Declaration precedes every pin; all pins precede `MarkRowDispatched`; that
  mutation precedes every child write.
- Pin acknowledgement follows durable chain attestation or reclaim, and
  terminal pruning follows durable cleanup completion.
- Per-frame row, HRFT section, and storage-record admissions remain intact.
- The shared encoder/cap API is the only table-write section authority used by
  Toron's chunker and Wendelon's send path.
- Frozen snapshot/WAL spines are replay-only. The replay-only legacy dispatch
  request is absent from every live control path.

## Risks and deferrals

- Snapshot v4 and WAL body v3 are the third consecutive consensus-format
  exercise. The frozen-spine discipline is preferred to inference.
- The unversioned saga carrier imposes a lockstep operational upgrade.
- Whole-chain pinning holds up to 128 pins for the write phase. This is the
  deliberate cost of enumerable recovery.
- The first implementation may encode a candidate once for sizing and again
  for sending. Performance evidence decides whether pre-encoded payload reuse
  merits a later API change.

## Forward

1. Chained visibility for `INSERT ... SELECT` and `UPDATE`.
2. A versioned CTAS saga carrier for mixed-version deployments.
3. Capacity-aware chain admission against the shared pin budget.
4. PRODUCT-0137 200k/1M scatter-threshold evidence.
5. Drop-outcome compaction carried from ADR 0093.
