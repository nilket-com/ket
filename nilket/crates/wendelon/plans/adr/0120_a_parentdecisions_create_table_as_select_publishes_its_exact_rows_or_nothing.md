# ADR 0120 - A ParentDecisions CREATE TABLE AS SELECT publishes its exact rows or nothing

## Status
Proposed (local), 2026-11-20.
- **Design:** CTAS-02 r2, reviewed by Codex 01a1fa37 and 01a1fa3e. Codex 01a1fa3e cleared this plan and the first test-first implementation slice, locally only, with no push.
- **Delivery:** one end-to-end delivery, in slices (D10).

**Scope:** plain `CREATE TABLE … AS SELECT` (and `IF NOT EXISTS`) under ParentDecisions.

**Refused before any durable effect, and at apply:** MATERIALIZE, OR REPLACE, a backup hold, and a result schema outside ADR 0119's declared AppendOnly set.

**Out of scope:** counters, physical reclamation of abandoned rows, Today's idle-prune gap, and automatic retirement of retained descriptors.

## Context
**Today on `cbdf7ae24` (the public baseline, a fresh PD local profile):**

| Statement | Answer |
|---|---|
| a plain CTAS, an empty CTAS, `IF NOT EXISTS` on a fresh name, `OR REPLACE` | exit 1, `CTAS provision rejected: invalid_transition`; the target is not found, before and after a restart |
| `IF NOT EXISTS` over a bound, same-schema name | `OK` from Toron's pre-SELECT winner check (`T/coordinator.rs:3733`), which never reaches Provision |

**Why it is refused:**
- every `CtasSaga(_)` request is refused at apply (`raft_storage.rs:7438-7447`);
- the PD write, read and certification paths resolve tables through lookups that hide provisional ids;
- an empty result cannot be written, because a claim needs 1..=64 children (`decisions.rs:3007`);
- Today's saga publishes on Toron's attestation;
- Toron learns the mode only from a refusal.

**The contract:**
- a PD CTAS publishes exactly the original logical rows, multiplicity included, or ends **terminal and unpublished**;
- recovery never re-runs the SELECT and never invents bytes;
- the declaration commits to canonical row identities and content: ADR 0119's identities and `request_digest`, not an Arrow serialization.

## Decision
### D1. The chain, in the PD decision state
`DecisionState.ctas` holds:
- **`chains[(tenant, op)]`:** the object, the statement fingerprint, the declaration, `dispatch_hash`, epoch, lease, phase, and the accepted/refused chunk bits;
- **`by_object[(tenant, object)] → op`;**
- **`retired[(tenant, object)]`:** permanent; `Abandoned { op, at }`;
- **`visible_from[(tenant, object)]`:** permanent while bound;
- **the retained-descriptor budget.**

Today's `CtasSagaEntry`, and every Today byte, are unchanged.

### D2. The bounded declaration
- **The codec:** fixed layout. `u16` count (big-endian), then `count` entries of 44 bytes each: `sequence_no` u64 BE, `rows` u32 BE, `request_digest` 32 bytes.
- **The decoder,** before allocating:
  - count ≤ 128;
  - length == 2 + count × 44, with checked arithmetic.
- **At apply:**
  - sequence numbers consecutive from the operation's base;
  - each chunk 1..=65,536 rows;
  - the sum checked.
- **The command field is bounded on the wire** (Codex 01a1fa67): `CtasBegin.declaration` is a byte field whose deserializer refuses more than 5,634 bytes (2 + 128 × 44) before copying anything. The encoder refuses more than 128 chunks; it never truncates the count.
- **An empty result** declares zero chunks.

### D3. Begin: one command chooses the declaration, object and owner
`CtasBegin { tenant, op, fingerprint, descriptor, declaration, dispatch_hash, stamp_ms }`.
- **Guards:**
  - PD;
  - no backup hold;
  - the tenant writable;
  - **the object is not the reserved KV id**, whatever the catalog holds;
  - the op id issued no later than the committed clock + skew and no earlier than the committed clock − 15 min (same-id expiry checked **at the authority**). The committed clock is the applied decision clock or the stamp, whichever is later, as for a Reserve's issuance, never the stamp alone (S2 correction);
  - the descriptor: AppendOnly, generation 1, ADR 0119's declared schema;
  - the declaration valid;
  - **the object exclusive:** no descriptor of that id at any generation, provisional or not; no drop outcome (ADR 0118 D3's check, reused); not `retired`; not owned by another chain;
  - **the target name is unbound** for a fresh operation, refused with a typed `NameBound` (so IF NOT EXISTS can validate a concurrent winner); after the same-operation replay, and checked again at Publish;
  - the retained-descriptor budget and terminal-summary capacity available.
- **Replay:**
  - an existing chain under `(tenant, op)` with the **same** fingerprint answers that chain unchanged. The losing Begin's object, rows and secret are discarded; nothing is renewed or replaced;
  - a different fingerprint refuses.
- **Effects:** the provisional descriptor (`insert_provisional`, after the exclusivity proof), and the chain in `Dispatching`, with lease = stamp + 60 s and epoch 1. The catalog insert, the one fallible step, runs **first**; a refusal changes nothing.
- **Ordinary Create** also refuses a provisional-owned id and a `retired` id.

### D4. Chunk admission at Reserve
A chunk is an ordinary PD AppendOnly table parent, `ParentKey { tenant, Table(object), op, base + i }`.

`validate_claim` admits a **first** Reserve for a provisional object only when **all** of these hold:
- a chain owns the object;
- the phase is `Dispatching`;
- `operation == Write`;
- the key's `(client_uuid, sequence_no)` is declared;
- `claim.request_digest` equals the declared digest;
- the children's rows sum to the declared count.

A Reserve for an existing key follows the existing replay and conflict rules, in any phase.

**A key named by a CTAS operation is that operation's chunk in every phase, never an ordinary write** (Codex 01a1fad7):
- `validate_claim` routes a first Reserve to this rule when its object is chain-routed **or** its key's client id names a chain;
- so a chunk delayed past Publish, which ends the object's routing, cannot land as an ordinary write;
- it is admitted only for the operation's own object, while `Dispatching`, as declared;
- otherwise it is refused: `ChunkUndeclared` (another object, another operation's key on a routed object, a non-write, an undeclared sequence, digest or row count) or `WrongPhase`;
- both are definitive, because the declaration never changes and a chain leaves `Dispatching` once; the coordinator answers either as `WRITE_REFUSED`, known not committed, never as a stall;
- a recorded key replays in any phase.

The rule lasts while the chain is held. Prune (D9) removes a chain only after every key it names is at or below the retirement floor, where every first Reserve is refused anyway.

**Two lookups, kept distinct:**
- **public** (admission, reads, export) hides provisional ids;
- **private** (the coordinator after D6, receivers planning a recorded claim, certification, base fetch, replacement) resolves an object owned by a chain or retained after abandonment, whatever the phase.

### D5. Evidence from the deciding transition only
`conclude` is the only place a parent becomes terminal. It sets the chain's `accepted[i]` or `refused[i]` in the same apply, for a body whose key and digest match the declaration.
- **Sets no evidence:** anything refused before Reserve.

### D6. The dispatch capability
- Toron draws a 32-byte secret per operation, and the chain stores `BLAKE3(secret)`.
- **Chunk writes** use `TABLE_CTAS_CHUNK_WRITE_REQ`, carrying the secret. The coordinator checks it against the applied chain before any digest work.
  - **The frame:** node-only, like every table write, with the same tenant authorization and admission. Its meta is a table write's meta, then the operation (16 bytes), then the secret (32 bytes). It is answered as a table write.
  - **The credential proves ownership** only when all of these hold, **in any phase**:
    - the frame's operation has a chain over this table;
    - the key's client id is that operation;
    - `BLAKE3(secret)` is the chain's commitment.

    Only then does the coordinator use the private descriptor, so a recorded chunk's lost acknowledgement replays after Publish or Abandon.
  - **The chunk is bound to the declaration before any Reserve** (Codex 01a1fad7):
    - an undeclared sequence is answered `WRITE_REFUSED`, known not committed;
    - a declared sequence with other rows or another digest is a key conflict.
  - **No new chunk outside `Dispatching`:** after the absent lookup, a chain that left `Dispatching` refuses a new chunk, known not committed. The reducer's rule (D4) holds independently.
  - **The stream:** the chunk takes its own stream, as a table write does, with the same bounded early deadline answer and the same draining of the remainder (Codex 01a1fad7).
  - **A credential whose chain this coordinator has not applied:**
    - it takes a linearizable cut, applies it, and looks again;
    - absent then, the answer is `AUTHORIZATION_FAILED`, and nothing is reserved;
    - a cut that fails is indeterminate, never a refusal.

    The chunk's coordinator is the row's primary, which may lag the leader that answered the Begin.
- **Owner commands** (Renew, Publish, Abandon) **never carry the secret** (Codex 01a1fa67):
  - the secret is checked at **authenticated leader admission**, on a dedicated typed proposal path (slice 2);
  - the admitted command names the **winning owner's commitment** (`BLAKE3(secret)`), the tenant, the operation and the epoch;
  - apply rechecks those against the chain's immutable commitment, its epoch and phase;
  - neither an admitted command nor the public commitment is accepted as a caller's credential;
  - stamps come from the gated leader clock;
  - the secret stays out of forwarded commands, the WAL, snapshots, Debug output and receipts.
- If the admission path cannot enforce this, the fallback is a command-specific owner signature with existing primitives, so a captured Renew cannot be replayed into a newly extended lease.
- **The admission path, as built (slice 2):** `CTAS_DECISION_REQ`, carrying a typed `CtasProposal`.
  - **The caller check:** the route admits Toron's node-to-node adapter or the tenant mutation role, as `CTAS_SAGA_REQ` does, bound to the request's tenant.
  - **Leader only, never forwarded:** a follower answers `NotLeader` with its hint.
  - **The owner check:** the leader compares an owner command's secret with the applied chain's commitment. A mismatch, or no chain, is refused `CtasNotOwner` before any proposal.
  - **Stamps:** every CTAS command but Prune takes the gated leader stamp: Begin, Renew and Recover are judged on the committed clock with it; Publish, Abandon and Conclude (S4 r2) record the committed clock with it as their terminal time (D9).
  - **The secret is never printed:** the proposal's and the dispatch credential's `Debug` redact it.

### D7. Takeover, publication and abandonment
| From | Command | Guard | To |
|---|---|---|---|
| `Dispatching` | `CtasRenew { op, epoch, owner, stamp }` | owner | `Dispatching`, lease = stamp + 60 s |
| `Dispatching` | `CtasRecover { op, new_epoch, stamp }` | the lease lapsed at the stamp; `new_epoch = epoch + 1` | **`Recovering`** (one way): no first Reserve from here |
| `Dispatching` | `CtasPublish { op, epoch, owner, stamp }` | owner; every chunk accepted, none refused, Σ = row count | `Published` |
| `Recovering` | `CtasConclude { op, epoch, stamp }` | all accepted → publish; any refused → `Abandoned(ChunkRefused)`; a chunk Reserved → refuse (wait); a declared chunk with no evidence and no body → `Abandoned(SourceBytesLost)` | as stated |
| `Dispatching` | `CtasAbandon { op, epoch, owner, cause, stamp }` | owner | `Abandoned(cause)` |

**Publish's effects, in one apply** (by the owner, or by recovery's Conclude; one shared path):
- **refused under a backup hold** (ADR 0736): the binding is a logical catalog change. Phase, visibility and budget stay, and publication succeeds after the exact-token release; a Publish committed before the hold still replays;
- the name must be unbound, otherwise `Conflict`. Ownership and phase are checked **before** a conflict is recorded;
- `publish_provisional`, the one fallible step, **first**;
- **the object leaves provisional routing**, so an ordinary write on the published table is admitted at once. Abandoned and conflicted objects stay routed and keep refusing fresh writes;
- `visible_from = log index`;
- the lifecycle;
- the replayable summary.

**`Published` is the terminal success.** A repeated Publish, Recover or Conclude on it answers `Published`, so a lost ack replays success; Abandon after Publish refuses.

**Abandon and Conflict:**
- conclude every still-Reserved chunk body `Refused { CtasAbandoned }`;
- retain the descriptor privately, under budget;
- record `retired`.

### D8. Visibility at a cut
- The reader refuses a table whose `visible_from > K`.
- Every chunk's `Finalize` precedes Publish, so `K ≥ visible_from` proves all rows, and `K < visible_from` sees no table.
- **Restore** uses the target cluster's own boundary.

### D9. Exclusion, prune, export and the fence
**Exclusion, prune and export:**
- the survivor policy excludes a `retired` object's rows from the next generation;
- maintenance proposes `CtasRecover` / `CtasConclude` after the lease and the attempt horizon, and `CtasPrune` after the terminal retention;
- the roster skips `retired` ids once their detail is pruned.
- **Prune's durable time anchor** (recorded now for slice 4, Codex 01a1fa67):
  - a terminal transition records its gated leader stamp (`terminal_at_ms`) beside its log index;
  - retention expiry is measured against a durable, monotone high-water stamp (the maximum stamp any CTAS command applied), never against a fresh Begin's own stamp alone;
  - so a regressing leader clock cannot re-admit a pruned operation id inside its window.
  - a chain is pruned only after every key its operation names is at or below the retirement floor, so D4's rule never lapses for a key that could still be first reserved.

**As built (slice 4):**
- **The committed clock.** Begin, Renew and Recover are judged on, and raise, the committed decision clock: the applied clock or the gated stamp, whichever is later. Leases and windows are measured on it.
- **Terminal time** (S4 r2, Codex 01a1fbf1 R1). The terminal commands carry a gated leader stamp: `CtasPublish` and `CtasAbandon` (the owner's, stamped by the leader at admission) and `CtasConclude` (maintenance's or a caller's, stamped the same way). A terminal transition (Publish, Conflict, Abandon, or recovery's Conclude) records `terminal_at_ms`, the committed clock with its own stamp (the maximum of the applied clock and that stamp), and raises the clock. So the retention runs from the terminal transition itself, never from a stale clock left by the last stamped command; a terminal replay answers from the record and changes nothing. This is the anchor above: the high-water stamp is the committed clock.
- **Prune.** `CtasPrune { tenant, op }` (appended) removes a terminal chain's record only once both hold:
  - the committed clock is at least `terminal_at_ms + CTAS_TERMINAL_RETENTION_MS` (the 15-minute operation window);
  - the operation was issued at or below the retirement floor, so every key it names refuses a first Reserve without the chain.

  The object's `retired`, `by_object` routing, `visible_from` and retained descriptor stay; a repeated Prune answers `Unknown`. Prune makes the 1,024 retained-descriptor bound reachable (its control: four rounds of 256 abandoned chains, each pruned after the floor passes it).
- **Leader maintenance** (`ctas_step`, every maintenance tick, leader only, at the gated clock), with no client traffic:
  - Recover for a dispatching chain whose lease lapsed on the committed clock;
  - Conclude for a recovering chain whose Conclude can decide now, by the reducer's own predicate (`conclude_outcome`, S4 r2, Codex 01a1fbf1 R2): refused evidence abandons, even with another chunk still Reserved (abandonment concludes it); a fully accepted chain publishes, so it is not proposed under a backup hold; a chunk with no evidence still Reserved waits; one never reserved abandons (`SourceBytesLost`). So a held chain never fills a batch;
  - Prune when admissible.

  The reducer decides each, and a refusal is derived again only while its condition holds.
- **Floor-safe exclusion.** A CTAS object retired (abandoned or conflicted) at log index `r` is excluded from every generation with cut `t ≥ r`. It joins ADR 0118's dropped tables in:
  - the survivor policy (excluding iff a drop was finalized, or a CTAS object retired, at or before `t`), checked where targets are proposed, staged and validated;
  - the per-table exclusion in report preparation and candidate building;
  - the idle trigger: a retirement no certificate excludes yet triggers a generation.

  Its inherited base entries become displaced debt, which cleanup reclaims through the retained descriptor.
- **The backup roster.** A chain still dispatching or recovering refuses the roster now (`LifecycleNotSettled`): its publication is a binding the hold blocks. A published table is an ordinary table; a retired object is never listed.
- **Public outcome resolution** (delivery correction, Codex 01a1fc44, 01a1fc48). Toron's `ResolveMutation` answer for an operation id, within one 30 s bound over every lookup:
  1. the Today CTAS saga;
  2. only on a ParentDecisions authority's typed answer to it (`WrongDecisionMode`, as `CatalogError::CtasUnderParentDecisions`), the chain in the same tenant (`CtasProposal::Resolve`: a linearizable read, no proposal);
  3. on authoritative absence, the ordinary mutation outcome under the original identity (the id at sequence zero). On ParentDecisions that read answers `Expired` for an id at or below the retirement cutoff, which every pruned chain's operation is.

  A lookup that fails or times out answers a correlated SQL error (`CatalogError`, `StorageError` or `StatementTimeout`); it is never read as absence and never drops the stream. A chain present answers an **appended** message, `SqlMessage::PdCtasOutcome`:

  | Chain | `state` | Publication | Rows |
  |---|---|---|---|
  | Dispatching / Recovering | `Pending` | `Unspecified` | the evidence so far |
  | Published | `Published` | `Committed` | every declared chunk accepted; the exact total, 0 for an empty declaration |
  | Conflict | `Conflict` | `KnownNotCommitted`, `BindingConflict` | every declared chunk accepted |
  | Abandoned(cause) | `Abandoned(cause)` | `KnownNotCommitted` | exactly the accepted and refused chunks: none, some or all |

  The counts come from the decisions alone: `accepted_rows` sums the declared rows of accepted chunks (the reserve guard checks each chunk's rows against its children), and a chunk never reserved counts toward `declared_*` only. The answer carries the chain's real object and epoch, claims no reclamation of an abandoned object's copies, and has no old binding or retirement. Every earlier `SqlMessage` keeps its discriminant and bytes; a decoder built before this variant refuses it as an unknown discriminant. That is the boundary, not support for older clients (pre-release: there are none).

**The fence:**
- the PD marker v23;
- PD WAL and snapshot format 33;
- the PD envelope 26;
- a typed `WrongDecisionMode` before proposal (at the receiving authority) and at apply;
  - a ParentDecisions authority answers every `CTAS_SAGA_REQ` with `CtasSagaReject::WrongDecisionMode` (appended), at every member and before any proposal; at apply, the same code;
  - a Today authority answers `CTAS_DECISION_REQ` with `Refused(ModeToday)` before any proposal, and does not serve `TABLE_CTAS_CHUNK_WRITE_REQ`;
- Today's encodings unchanged.

### D10. Slices (one delivery)
1. **The decision-state core:**
   - D1, D2, D3, D4 at Reserve, D5, D7 at apply (Renew, Recover, Publish, Conclude, Abandon);
   - D3's Ordinary-Create extension;
   - the fence bumps.
2. **The coordinator, receivers and private lookups:**
   - D6's chunk request; D4's private lookup at every recorded-claim site; certification and replacement;
   - D8's reader guard; wrong-mode at the authority before proposal.
3. **Toron:**
   - the PD path (form/type refusal, materialize → Begin → chunks → Publish / Abandon on an inadmissible chunk), renewals, and the typed-mode fallback before proposal only;
   - the public controls.
   - **As built:**
     - **Selecting the path.** Toron's Today saga Resolve is answered `WrongDecisionMode`, typed and before any proposal. Mapped to `CatalogError::CtasUnderParentDecisions`, it is the only answer that selects the decision chain; any other failure fails the statement.
     - **Forms.** OR REPLACE and MATERIALIZE are refused before any effect.
     - **Before the SELECT.** The operation is read with `CtasProposal::Resolve`: a linearizable read at the leader, proposing nothing.
       - A declared operation is never re-run, so a changed source cannot redeclare it.
       - The same fingerprint adopts its status: Published succeeds, Conflict is judged as IF NOT EXISTS requires, Abandoned and in-progress are errors.
       - Another fingerprint is refused.
     - **Bound names.** A name bound before the SELECT, at Begin (`NameBound`) or at Publish (`Conflict`) gives IF NOT EXISTS success on the same schema; otherwise `TableAlreadyExists` or `SchemaMismatch`.
       - A binding seen before the SELECT may be this operation's own publication, by a concurrent run of the same statement after the first resolve. The operation is therefore resolved again, authoritatively, before the binding answers (Codex 01a1fb6a).
       - Declared: the same statement adopts its status, and another statement is refused.
       - Absent: the captured binding is judged as independent.
     - **The caller's deadline** (`context.deadline`) is carried through preparation, Begin, renewals, every chunk write (`WriteOptions.deadline`) and Publish.
       - Each fresh effect is checked against it first: an expired deadline after the SELECT begins nothing.
       - A command cut off by it was sent, so its outcome stays with recovery.
       - A lost Begin's whole adoption wait, every pending read included, is bounded by 150 s and by the deadline.
     - **Renewal and abandonment** (Codex 01a1fb6a):
       - the owner renews on a fixed period (20 s) for as long as it dispatches, also while one chunk is in flight;
       - a refused renewal ends dispatch, leaving the operation to recovery;
       - abandonment is claimed only on Abandon's applied `CtasAbandoned`. A refusal (a takeover made the chain terminal first) is reported without claiming it.
     - **The declaration.** The SELECT runs and its result is materialized and chunked as Today does. An empty result declares no chunk. Chunks get consecutive sequences from Toron's chunk base, and their digests come from `ctas_chunk_request_digest`.
     - **The owner's secret** is drawn per statement with the platform CSPRNG and never logged.
     - **A Begin that replays another owner's chain** (another commitment) is a lost Begin: that client dispatches nothing and waits a bounded time for the winner's terminal status.
     - **Dispatch.**
       - Chunks go through `ctas_chunk_write`, with renewal as above.
       - An inadmissible chunk (refused, known not committed, or a key conflict) abandons the chain, as above.
       - An unknown outcome leaves the chain to recovery.
       - Then Publish.
4. **Maintenance and the backup lifecycle:**
   - maintenance Recover / Conclude / Prune;
   - exclusion, roster, export and restore.

## Rationale
- **One Begin removes the undeclared state.** Toron has materialized every row before it provisions.
- **Admission is bound to the declared digest and the dispatch phase, ownership to the secret.** The one-way `Recovering` fence closes new dispatch at takeover, while recorded claims still finish and repair.
- **Evidence written in `conclude` survives certification:** it never depends on a body that a generation erases.

## Consequences (The "Negatives")
- **The descriptor of an abandoned object is retained** privately until a later unit proves it unused; the budget bounds the count and the bytes.
- **A source change after a coordinator crash** ends the statement unpublished; the user re-runs it.
- **A pre-0120 PD store or peer is refused** (marker, formats, envelope).

## Acceptance (each control RED before its fix)
1. **The declaration codec:** 0, 128, 129, inconsistent lengths, overflow.
2. **Begin:** a first Begin; a same-fingerprint replay (the winner unchanged, the loser discarded); a different fingerprint refused; exclusivity against a live object, a provisional one, an identical descriptor of an owned object, a dropped id and an abandoned id, each with the existing owner and visibility intact; same-id expiry at the authority; a backup hold; an excluded type; budget saturation.
3. **Reserve:** a matching first Reserve; a wrong digest, wrong rows, a non-write and another tenant refused with no evidence; a first Reserve after Recover refused; replay of a recorded key in any phase.
4. **Evidence:** Finalize Accepted, Abort, a lineage closure and Abandon's closure each set exactly one bit; refusals before Reserve set none.
5. **Takeover:**
   - Reserve, then Recover, then Conclude waits, then Accepted, then Conclude publishes;
   - Recover, then a refused Reserve, then Conclude → `SourceBytesLost`.
6. **Publish:** owner publish; a lost-ack replay; Abandon after Publish refused; a stale-epoch or non-owner Publish refused before any Conflict; Conflict on a bound name.
7. **The empty declaration** publishes empty.
8. **Interrupted publication:** Recover then Conclude publishes.
9. **The coordinator, receivers, reader and mode boundary** (slice 2, in process through the routes):
   - public traffic cannot reach the provisional table;
   - a wrong credential and a wrong digest reserve nothing and set no evidence;
   - the owner's chunk commits through receivers that resolve the private descriptor;
   - a generation certifies it and keeps the evidence, and publication binds the exact multiset;
   - a cut paused before Publish sees no table, and a fresh cut sees the exact multiset;
   - the route is tenant-bound: owner commands with a wrong secret append nothing, a follower answers with its hint, and the owner's renewal is admitted;
   - wrong mode is refused in both directions;
   - a replacement recovers a base that holds an abandoned chain's retained chunk;
   - an abandoned chain no longer dispatches.
   - **Added after Codex 01a1fad7:**
     - an undeclared chunk paused across Publish is refused, with nothing recorded; the published multiset holds, and a later ordinary INSERT succeeds;
     - after Publish, a new key under the operation is refused at the reducer, and the accepted key replays;
     - a declared chunk crossing Abandon is refused, and a committed chunk's lost acknowledgement replays after Publish and after Abandon;
     - a chunk held after its Reserve answers its deadline on its stream, then completes;
     - a replacement repairs an owed, uncertified, still-dispatching chunk at its real generation;
     - an abandoned chunk's physical copies become retired-intent debt, which certified cleanup reclaims through the retained descriptor;
     - Begin judges its operation on the committed clock.
10. **The public multi-chunk and changed-source discriminators** (slice 3).
11. **Maintenance, backup after an abandonment, and export on an idle cluster** (slice 4).
12. **The fence**, and Today's goldens unchanged.
