# ADR 0699: a store-bound parent decisions authority for a disposable three-node checkpoint

Status: proposed, 2026-11-07 (Claude, Codex). Implementation lands with this plan, one unit at a time. Under the operator's 2026-11-07 authorization to redesign whatever is needed to remove the finite cluster lifetime.

## Why
The three-node workflow test stopped on write admission (DC-193). The Root2 activation contract r9 would have fixed it only after slices with per-cluster lifetime budgets: 32,768 parent decisions, 16,384 lineages, 4,096 cuts and a control log ending at `E_hard`. The operator ruled that a finite-life cluster is not acceptable.

The design that replaces it (review packets `reviews/dc54-index-continuity-r1/l2/lifetime/`, contract `DEV-CHECKPOINT-CONTRACT.md`) has two tiers:
- Signed governance stays verified from genesis.
- Leases, epochs and write decisions live in checkpointed Raft state, with reclamation.
- Range primaries and ADR 0100 Option B stay.

It is a candidate, not an approved protocol.

## Decision
1. A new decision authority, `ParentDecisions`, is bound to a store when the store is created and to the cluster it joins. Today's authority is the default and is unchanged.
2. The first target is a **disposable development checkpoint**: fixed three voters, same-table multi-child atomic statements and single-key KV, latest-at-K reads by complete base scan, same-store restart and failover, reclamation while healthy. It excludes copy and re-entry, membership change, migration, transfer, split, index-assisted reads, counters and historical as-of. **It is not three-node readiness and not production activation.**
3. Six units of work, the default path untouched. S1 to S5 are delivered here; S6 is left to a later ADR:
   - S1: mode.
   - S2: decisions.
   - S3: admission.
   - S4: reader.
   - S5: reclamation and restart.
   - S6: the unchanged workflow test.
4. The mode refuses ordinary service until S6. No user-reachable setting can enable part of the protocol.

## S1: the mode and its boundaries
- `DecisionMode { Today, ParentDecisions }` (`consensus/decision_mode.rs`). It is a crate-private field of `NodeBootstrapConfig`, with no configuration key, and is refused unless the profile allows development keys.
- **Store, in order:**
  1. Admission refuses an existing store of the other authority. A marker must be a regular file with exactly the expected bytes, or it is refused, never treated as absent.
  2. The control log's first artifact is a format-9 checkpoint holder, synced before its name exists. From then on, a binary on the other authority has a record to refuse.
  3. The log store verifies by replay that every record is this authority's, then writes the durable `PARENT_DECISIONS` marker, before it creates the vote file or any other artifact.

  A creation interrupted after step 2 resumes through the existing interrupted-first-creation path, which recognises the holder and marker as carrying no state. One interrupted before step 2 holds only the lock, so it is a fresh store to either authority. An unmarked store with anything else, a saved vote included, is refused.
- **Control log and snapshot:** ParentDecisions writes format 9 for every record; Today writes 8, matching the frozen golden bytes. ParentDecisions refuses any record of another format, checkpoints included, at replay, before state is applied and without changing a file. Today keeps its acceptance of older checkpoint records.

  An older binary refuses format 9 at its existing gate during control-log replay. That comes after its read-only vote load and before it opens the vote file for writing, or opens slab, index or key files. Before that point it writes only the lock and the clean-shutdown record. This rests on the Today open path, whose gate is unchanged, and was not run against a real older binary. Offline readers and backup accept Today only.
- **Raft:** frames between ParentDecisions nodes carry envelope version 2, requests and responses alike.
  - Envelope 2 is valid only on Raft request and response types, enforced where every frame header is read. Every other frame stays version 1.
  - The Raft handler refuses the other authority's version before decode.
  - The Raft client refuses a response of another version or an unexpected type.
  - An older binary refuses version 2 when reading the frame.
- **Join:** a `ParentDecisions` capability bit. A cluster refuses a joiner whose bit differs, before it becomes a learner.
- **Service:** data-plane frames and streams are refused with `Unsupported` at dispatch, and readiness reports `ParentDecisionsIncomplete`.

**S1 does not change** `SmState`, snapshot bodies, request variants or any write or read path. New-mode format 9 bodies are today's bodies until a later unit needs a deliberate codec, which it will propose then.

## S2: the decision reducer
Review record: `reviews/dc54-index-continuity-r1/l2/s2/` (DESIGN, ADDENDUM-1, CODEX-1, CODEX-2).

- **A pure reducer** (`consensus/decisions.rs`) over replicated state, reachable only through the state machine.
  - **Commands:** Reserve, Finalize, Resolve, Abort, AdvanceFloor.
  - **Terminal outcomes:** a parent reaches exactly one terminal status. Accepted needs a quorum of child acknowledgements in every child that includes the child's holder. Refused needs a closed bound lineage (`LineageClosed`) or the coordinator's abort. The first terminal status wins.
  - **Refusals:** every refusal changes nothing, the committed clock included.
  - **Raft coordinates.** A parent's body is keyed by `rsv`, the Raft log index of its Reserve entry. Its terminal status carries `dec`, the log index of the first terminal decision. Duplicates and competing commands never move either.
    - A reader at cut K sees nothing before `rsv`, Reserved until `dec`, then the decision.
    - Decode validates the coordinates: `dec > rsv`, and each body sits at its `rsv`. It also checks body and key consistency: each retry key names a body with its key and digest, every Reserved body keeps its key, and no two bodies share a key.
- **Lineages.** In this mode a lease grant is accepted only under four conditions:
  - it is for the current epoch;
  - its range is exactly one of its holder's merged primary ranges in the ring of `epoch_history`'s last configuration;
  - its holder has a registered incarnation;
  - its start precedes its expiry.

  Other grants are denied, and today's overlap rule still applies after this check.
  - **Lineage entry:** each accepted grant has one, carrying the holder's store incarnation and a generation from one counter. The counter is never reused and is checked before use. Overflow denies the grant.
  - **Renewal:** a grant keeps its generation only when it continues the existing grant with no gap and does not shorten it. A shorter grant is denied.
  - **Ends:** a matching release, a replaced incarnation and a new epoch each end lineages. A denied or absent release creates no generation.
  - **Open:** a lineage is open while its generation, epoch and incarnation are all current. Expiry is not consulted.
  - **Bound:** lineages are bounded by the range universe, 193 for three nodes.
- **Identity and time.**
  - **Keys:** a parent key is tenant, namespace (`Kv` or `Table`), a UUIDv7 and a sequence. Its issuance is the uuid's time.
  - **Expiry:** the expiry triple borrows the counter SDK's values. Whether table and KV callers fit them is an S3 obligation.
  - **Clock:** the committed clock is the maximum of proposers' stamps. Leader provenance of those stamps is an S3 obligation.
  - **Lookup:** it compares the request digest before returning any status. A key with no record, issued at or before the cutoff, is `OutcomeUnavailable`, never New.
  - **Floor:** AdvanceFloor retires terminal retry keys. It never removes a parent body, Refused ones included. Bodies stay until certified reclamation (S5), and their byte limit can stop admission until then.
- **Placement.** Each child's plan is checked against the consensus-time ring that table drops use: the last epoch configuration and the tenant's replication factor.
  - **Replica sets:** every replica segment the child touches must have the plan's set, with the holder first. So a child cannot span two replica plans, and S3 splits children at segment boundaries.
  - **Children:** children are canonical and non-overlapping, and lie inside open lineages.
  - **Coordinator:** the coordinator is named separately, as the child holding the first input row's ring position, which is how the caller routes.
  - **Refused:** active migrations, tenants that refuse writes and unavailable tables are refused.
- **Evidence.**
  - **Signed commands:** Reserve, Finalize and Abort are signed.
  - **Bindings:** acknowledgements bind the reservation, the whole claim's digest, the child and the acknowledging store.
  - **Verification:** a verified value has one private constructor. In a non-test build nothing verifies, so these commands cannot take effect. The signature scheme is S3's.
  - **No producer:** an architecture test pins that no production code builds a decision command.
- **Formats.** This mode now writes control-log and snapshot format 10, marker v2 and Raft envelope 3.
  - **Retired S1 artifacts:** S1's format 9, marker v1 and envelope 2 are refused by both authorities, with no conversion. Their stores existed only in tests.
  - **Today's format:** WAL format 8 refuses a decision record on write and on read. Its golden bytes are unchanged.
  - **Snapshot envelope:** the decision state is `#[serde(skip)]` in `SmState` and travels in a format-10 envelope.
  - **Codec functions:** the envelope is written and read only by `write_state_body` and `decode_state_body`. Today's encoder refuses a state that holds one.
  - **Goldens:** the format-10 bodies have their own golden.

## S3a: signer binding, forwarding and leader time
Review record: `reviews/dc54-index-continuity-r1/l2/s3/` (DESIGN, CODEX-1).
- **Signer registry.** `BindSigner` binds a store incarnation to its node's decision-signing key, immutably.
  - **Apply rules:** apply accepts a binding only for the node's current incarnation. It refuses a second key for an incarnation and one key for two nodes. Bindings are never pruned, so retained evidence keeps verifying on replay.
  - **Authority:** current authority is checked separately. An old incarnation's lineage is closed, so it cannot start new work.
- **Verification.** Signed Reserve, Abort and child acknowledgements are checked with Ed25519 (strict) against the replicated key for the exact signer incarnation. An unbound signer verifies nothing.
  - **No stand-in:** the test-only signature is gone, so tests exercise this verifier.
  - **Retries:** a recorded parent answers Reserve, Finalize and Abort as recorded, before any evidence check, so a retry after authority moved still learns its result.
- **Keys.** Each node has a fourth role key for decisions. In this mode the keys are the development keys unless set, and the field is crate-private like the mode itself.
  - **Startup check:** the local key must be in the map, and the role may not reuse a key of the lease, transfer or audit roles, nor one key for two nodes.
  - **Production custody** is a production-required item.
- **Forwarding.** A data node sends `DECISION_PROPOSE_REQ` to the leader. The request has no field for a stamp, a floor or a key.
  - **Leader checks:** the leader admits it for the node authenticated on the connection. A Reserve must come from its coordinator. A binding names the sender's configured key.
  - **Stamping:** the leader stamps a Reserve with its own clock, and only while the drift test holds at proposal time: a cluster median exists and the offset is within `MAX_OFFSET_MS`.
  - **Local leader:** it takes the same path.
  - **Routing:** the router sends the request to the control ring only from an authenticated data node.
  - **Response:** the applied result and its log index, NOT_LEADER with a hint, a typed refusal, or indeterminate.
- **Floor.** Only the leader's maintenance task proposes `AdvanceFloor`, at its healthy clock minus E, and never lower.
- **Incarnation registration under this mode.** The leader also forwards a follower's own **first** `RegisterIncarnation`, and its exact retry, under the same authentication. In this mode apply replaces ADR 0697's appending-leader rule with that admission.
  - **Replacement:** apply refuses any registration that would replace an established incarnation, so another store at the same node id cannot retire the owner. This checkpoint has no replacement, copy or re-entry.
  - **Today's authority** keeps the appending-leader rule.
- **Maintenance task.** A ParentDecisions node registers its store incarnation if its node has none, then binds its key, then, as leader, advances the floor. A different established incarnation for its node is left alone.
- **Formats.** The decision state now carries the signer registry, and signed bodies use the real scheme. So this mode writes control-log and snapshot format **11**, marker **v3** and Raft envelope **4**.
  - **Retired:** S2's format 10, marker v2 and envelope 3, and S1's equivalents, are refused by both authorities, with no conversion.
  - **Fixtures:** S2's format-10 fixture is kept beside the format-11 golden.

## S3b: child writes and parent-owned versions
Review record: `reviews/dc54-index-continuity-r1/l2/s3/` (S3B-ALGORITHM, CODEX-4).
- **Payload.** A child write carries the parent's own user rows for one child and names `(rsv, ordinal, claim digest, schema generation)`.
  - **Independent check.** Each receiver re-derives every row's identity, its ring position and the child's manifest from those rows and the replicated claim. It builds its own storage frames and trusts nothing the sender computed.
  - **Senders.** The sender must be authenticated as the role the claim records: the coordinator's holder for a child write, and the child's holder for a child replicate.
  - **Receiver.** The receiving store must be in the plan at its current incarnation. A holder also needs its lineage open and an unexpired, verified lease from the current leader covering the child range.
  - **Tables.** Only primary-key tables are admitted in this mode: append-only identities depend on the original batch, and content-hash identities are shared.
- **Ownership.** A parent owns exactly the versions `(table, key, commit HLC)` its manifest names. Each version has one owner entry. Another parent's claim to it is refused even with identical bytes.
  - **Commitment.** The manifest's content commitment binds each row's canonical bytes, table and schema generation.
  - **Intent record (P).** P plus its owner entries are written once, at intent, in one transaction, in the store's index. They are authority, not cache: the open-time rebuild never touches them, and an initialized store missing them refuses to open. Today's stores never create them.
- **Order.** Every step for one parent runs under that parent's lock, inside a task that keeps the lock until its work finishes. Inside the lock the recorded decision is read again:
  - **Refused** marks the intent Refused and admits nothing more. A missing intent is never recreated.
  - **Accepted** re-acknowledges only a complete intent.
  - **Reserved** records the intent, appends only the versions with no copy, verifies the exact set, then marks it Complete.
  - **The ack** is signed with the store's decision key only after Complete.
- **Visibility.** A version is tentative until its parent is recorded Accepted at the reader's cut (S4). Only a matching recorded Refused refuses it. Absence or uncertainty never does.
  - Physical removal of refused versions, and dropping their records, are S5's.
- **Old paths.** The old replicate handlers refuse in this mode and are never an alternative ingress.
- **Frames.** The two new frames are routed as node-only data frames. Dispatch still refuses them while the mode refuses service; S3c opens the write path.

## S3c: the coordinator
Review record: `reviews/dc54-index-continuity-r1/l2/s3/` (S3C-PLAN; CODEX-1 R1 and CODEX-7).
- **Authoritative lookup.** `DECISION_LOOKUP` (a node-only control frame) is answered by the leader from applied state after a linearizable read barrier.
  - It returns the parent's **original** retained claim or its recorded status, a conflict, or whether a new claim may be made for the key.
  - No clock and no current-authority check precede it.
- **Coordinator.** In this mode a table write is coordinated by the node holding the lineage of its first input row's ring position. Any other node answers NOT_LEADER, with that holder as the hint.
  - **Lookup first.** The coordinator looks the parent up before anything else. A recorded Accepted answers COMMITTED, and a recorded Refused answers KNOWN_NOT_COMMITTED. A Reserved parent is driven only by the coordinator its claim records:
    - at any other node, the answer is NOT_LEADER with that coordinator as the hint, and nothing is rebuilt;
    - at that node, it is resumed from the original claim (its HLC, children and reservation) only while the same store incarnation holds the coordinator lineage open;
    - otherwise the answer is indeterminate, and recorded closure and Resolve settle it. It is never KNOWN_NOT_COMMITTED.
  - **The record decides after Reserve.** Whatever a Reserve answers (a fresh reservation, a racing attempt's reservation, or a terminal record), the coordinator looks the parent up again and acts only on the recorded claim. It never uses the claim it built. If that lookup fails, the answer is indeterminate.
  - **New claims.** Only an absent, eligible key gets a new claim. It is stamped with the coordinator's HLC, split into one child per replica segment the rows fall in, signed, and reserved through the leader.
  - **Children and Finalize.** Each child's holder writes it and gathers its plan's acknowledgements (S3b), and Finalize decides. Anything else is indeterminate and retryable with the same identity.
  - **Request digest.** It binds the table, the schema generation and each row's identity and content. It does not depend on row order or time.
- **Resolve driver.** The node holding the coordinator child's range proposes Resolve for reserved parents with a closed bound lineage, bounded per tick. The same task marks local intents whose parent is recorded Refused.
- **Clock samples.** Forwarded proposals and lookups carry the sender's clock reading. The leader records it in its drift tracker and never uses it as a decision stamp. Without it the leader rarely hears follower clocks, and its health check could not pass.
- **No migration seeding.** Epoch reconciliation seeds no migrations in this mode. The fixed membership forms before any write is served, so no data moves. An in-flight migration would refuse every Reserve over its range.
- **Service.** Dispatch still refuses data frames in this mode. Only a test-only bootstrap option, set by no configuration, CLI or normal bootstrap, opens table writes and child frames.
- **Control.** A three-voter in-crate cluster in this mode:
  - a write whose first row's primary is not the Raft leader is redirected to that primary;
  - the primary coordinates it to COMMITTED;
  - it is recorded Accepted, with complete intents on a majority of the plan;
  - a retry learns the recorded result;
  - two attempts of one request racing to Reserve return the winner's HLC and decision, whether the winner was still Reserved (the loser resumes its claim) or already Accepted;
  - a retry at any other node for a Reserved parent is sent to the recorded coordinator and leaves the record unchanged;
  - a 40-row statement across several primaries is one Accepted parent, with every child complete on a majority of its plan.

## S4: the reader
Review record: `reviews/dc54-index-continuity-r1/l2/s4/` (DESIGN, CODEX-1).
- **Served shape.** In this mode a table query is served only streamed, LINEARIZABLE and with `as_of_hlc = 0`, by complete inventory. Other consistencies, historical cuts, the frame path, index plans, aggregates, point and KV reads, deletes, migration pulls and merges are refused, and a table with more than one schema generation is refused. Children do not record their generation.
- **Cut K.** `DECISION_CUT` is a node-only control pair. The leader answers its read index after a linearizable barrier, and the reader waits, bounded, until it has applied K. One K serves the whole statement, and statuses are read with `status_at(K)`, so a decision applied later never enlarges the statement's view.
- **Positive inventory.** The inventory is the children of every parent Accepted at K for the table and tenant, under a per-statement bound with checked arithmetic (typed `QUERY_BUDGET_EXCEEDED`).
  - **Plan.** Each child's recorded plan must name this store; otherwise the read is refused.
  - **Local intent.** It must be Complete, with the recorded claim digest, content and count, a canonical manifest, the claim's table and HLC, and every version owned by its child.
- **Selection before predicate.** Per key, the newest version in the inventory wins. Two parents at an equal full HLC have no winner, and the statement is refused. A tombstone wins its key and suppresses it. A selected version's routing locality is derived from its own verified bytes, never from the key's current locality, which a later version of the key may have moved.
- **Proof.** Every winner must be stored here, every copy carrying the manifest's row digest. The executor then runs over exactly those rows (`ScanSource::Proved`), with the predicate, projection and retention unchanged. A missing or differing copy is repaired once; otherwise the statement is refused, retryable. A partial answer is never given.
- **Read authority.** Checked before the inventory, after the proof, after each streamed batch and before the success terminal:
  - no local persistence distrust;
  - the tier authority is ready;
  - this store is the registered incarnation and serves in the last configuration;
  - K is applied.

  A failure ends the statement without a success terminal, retryable. S5 attaches its certified generation and K_min to the same check.
- **The statement's deadline.** The caller's deadline is derived once at entry, and the execution spends the same one.
  - The cut, the applied wait and the proof, repair included, end when the deadline passes or the statement is cancelled.
  - Both are checked again before any success, an empty answer included.
  - A repair already persisting finishes under its parent's lock in its own task.
- **Repair.** `TABLE_CHILD_FETCH` is a data pair between members of a child's recorded plan, at their current incarnations, for a parent recorded Accepted.
  - **Source.** Under the parent's lock, it serves rows rebuilt from stored copies verified against its own complete intent.
  - **Requester.** It runs the replica checks against the recorded claim and persists under its own parent lock, typed as a repair, with no acknowledgement. A repair also re-appends a stored version missing under a complete intent.
  - **Sources.** They are tried holder first, one bounded attempt each.
  - **Ordinary writes.** A child write or replicate of an Accepted parent stays refused.
- **Containment until S5.** In this mode no maintenance rewrite drops a stored version: retention, tombstone and prune rewrites reclaim nothing, and LSM compaction plans nothing. Tiering is unchanged. Healthy reclamation is S5's.
- **Service.** Dispatch still refuses data frames and streams in this mode. Only the test-only opening also lets the table query stream and the fetch pair through.
- **Controls.** A three-voter in-crate cluster in this mode:
  - at every node, a read right after each acknowledgement is exact;
  - a Reserved parent with complete children is invisible, and the older Accepted version shows through;
  - the newest Accepted version wins;
  - a Refused parent's stored children are invisible;
  - a statement paused after its cut does not see a write accepted meanwhile;
  - authority withdrawn before the scan or mid-statement gives no success terminal;
  - a statement delayed past its deadline at the cut, or cancelled while waiting to repair, ends with no rows;
  - on a table whose locality projects a non-key column, a newer version of a key at another locality, physically complete and then Reserved or Refused, leaves the older Accepted version in every answer, including with row identities;
  - the boundaries are refused, and the budget refuses;
  - a store that missed a replicate is repaired by its read and answers exactly; so is a complete intent whose stored version was lost;
  - with every source refusing, or only a tampered source, there is no answer and nothing is persisted;
  - a prune under containment keeps every version.
- **Not in S4:**
  - single-key KV and the zero-child `RecordNoop` (S4b);
  - schema evolution in mode;
  - restart certification and reclamation (S5).

## S4b: single-key KV, single-row DELETE and RecordNoop
Review record: `reviews/dc54-index-continuity-r1/l2/s4b/` (DESIGN, CODEX-1, ADDENDUM-1, CODEX-2).
- **Format 12.** Marker v4, envelope 5. Formats 9 to 11 and their markers and envelopes are refused by both modes, with no conversion and no reset.
  - **`Claim.operation`** is `Write`, or `Delete { request, selection }`.
  - **`ParentStatus::Noop`** is never a body's status.
  - **Retry keys name a body or a noop** (`RecordKind`), and the first record of a key wins: a Reserve over a recorded noop returns the noop and creates no children, and a noop over a recorded body returns that body unchanged.
- **KV.** A single-key write (`WRITE_REQ`) is a one-row parent of the existing default KV table (`TableObjectId::DEFAULT_KV`, generation 0, PK `key`) under the `Kv` namespace. It runs through the S3 coordinator, child, intent, verification and repair paths.
  - The wire's key identity must be the key's (`blake3_tenant`). Storage uses the table row identity.
  - A Kv claim is one child of one row at one position, and never deletes. No schema may take the reserved id in this mode.
  - A read (`READ_REQ`) is LINEARIZABLE at `as_of 0` only, with a zero `min_hlc` (a causal lower bound is not proved by a log cut, so a nonzero one is refused), through the S4 cut, authority and proof. An absent answer passes the same final authority check as a present one. `READ_REQ` carries no caller deadline. Its point inventory is filtered by the key's exact identity, so a different key at the same position never answers. No Accepted version gives KEY_NOT_FOUND.
- **DELETE.** One row id, of a PrimaryKey table whose locality is a function of the id alone (IdentityProjection, TableSpreadProjection). Anything else is refused.
  - **Routing.** The holder of the id's position coordinates; any other node redirects. Lookup comes first.
  - **Evaluation.** One evaluation capability (`proved_point`) proves the row's winner at a cut K, or a stored typed tombstone, or absence over the complete inventory, repairing first if needed. Anything unproved is indeterminate, never "not found".
  - **Absence and deadline.** Absence and tombstone answers pass the final authority check like presence. The caller's deadline is derived once: a recorded outcome is always answered, while a fresh evaluation, its proposal, the child write and Finalize spend the deadline. An expired evaluation proposes nothing, and expiry is answered WRITE_DEADLINE_EXCEEDED, indeterminate.
  - **Select at K, publish at dec.** A live winner gives one tombstone child for the key at a commit HLC merged with, and strictly above, the winner's. The reducer checks that inequality, the single child at the row's position, the namespace and the digest. Otherwise the coordinator signs a **RecordNoop**, recorded terminal at its own index, which linearizes at K.
  - **Frozen counts.** Deleted, already-deleted and not-found come from the frozen selection, and a retry answers from the record, even after a later insert.
  - **What it does not establish.** No serializable or linearizable read-modify-write DELETE. The tombstone suppresses every lower-HLC version of the key, including a concurrent write ordered below it, and two concurrent deletes may both report the row.
- **RecordNoop (reducer).** The reducer checks, after lookup first:
  - E0 and E1;
  - the coordinator's signature;
  - an outcome that is not Deleted, with a cut below the record's index;
  - the table, generation and supported shape;
  - the position recomputed from the row;
  - the coordinator's lineage open over that position at its current incarnation;
  - the digest.

  It consumes retained bytes under the existing bound. Snapshot validation covers both record kinds, and the floor retires only keys. No lag limit applies.
- **Typed tombstones.** A delete child's version is derived from the recorded claim, and its payload only names the row. Persistence appends the tombstone once, through today's tombstone packing. Verification requires owned tombstone locations, and absence is never proof. Repair serves the same typed child.
- **Controls.**
  - Reducer: the Kv shape; the matched-delete checks; first-record arbitration in both orders; noop refusals; the floor keeping the noop and its bytes; validation.
  - Three voters, KV: redirect then commit, exact reads, a retry, a forged identity, newer Reserved and Refused invisible, a different key at the same position, repair.
  - Three voters, DELETE: a matched delete and its retry; both noops; a noop retry after an insert; a winner ahead of a rewound coordinator clock; a concurrent insert, by HLC order; multiple ids refused; unproved data indeterminate with no noop.

## S5a: certified generations
Review record: `reviews/dc54-index-continuity-r1/l2/s5/` (DESIGN, CODEX-1, ADDENDUM-1, CODEX-2). S5 is split into S5a (the authority handoff) and S5b (physical cleanup).
- **Format 13.** Marker v5, envelope 6. Formats 9 to 12 and their markers and envelopes are refused by both modes, with no conversion and no reset.
- **Replicated generations.** The reducer appends three commands.
  - **`ProposeTarget`.** A target `{gid, T, predecessor_gid, members, chunk_bits 8, survivor_policy 1}` is admissible only under a closed cut:
    - every body with rsv ≤ T is terminal with dec ≤ T, and none is Reserved;
    - the covered noops are exactly those with dec ≤ T;
    - every covered record's retry key is already retired by the floor, judged per record;
    - the predecessor is the current generation;
    - the members are the current voters at their current incarnations;
    - gid, T and the certified floor `Fr` only increase, with T below the target's own index.

    The target is frozen when recorded. A new target with the same predecessor supersedes an uncommitted one, and its reports are refused thereafter.
  - **`Report`.** A member's signed per-chunk counts and roots, exactly 256 chunks. It is refused when stale, for another target, from a non-member or a wrong incarnation, or with partial coverage. It is deduplicated per member: an equal re-report is idempotent, and a different one is refused.
  - **`CommitGeneration`.** It applies only when every member has reported and all agree per chunk, rechecking the recorded target. On apply:
    - the closed set's bodies, noops and keys are erased, and their bytes subtracted;
    - the certificate becomes current, with `K_min` the commit's index and `Fr = T`;
    - the predecessor link is recorded.
  - **Bounded metadata.** Only the current certificate and one pending target with its reports are kept, and a monotonic `next_gid` stops a late report from recreating retired state.
  - **The driver.** The leader proposes Commit only for a pending target whose members are still the current voters at their current incarnations. A pending target that is no longer current (a replaced store, or a voter change) is never committed and never counts a missing report as agreement. It is replaced by a fresh target from the same predecessor, at a fresh gid, with the current members. A store keeps its incarnation across restarts, so a restart alone leaves its target current.
- **The local base.** `storage_index.redb` gains `base_active_v1`, `base_staged_v1` and `base_meta_v1`.
  - **One entry per key:** `{hlc, row_digest, tombstone, position}`.
  - **Chunks.** The chunk is the position's high byte. Each chunk's sum is over a new ADR 046 domain on the length-delimited `(tenant, table, key identity, hlc, row digest, tombstone)`.
  - **Build.** A member builds its candidate under a lifecycle permit, and only after its base is validated for this start. The candidate is its active base plus the winning version of every Accepted child in the closed set.
    - Each child must pass the S4 proof: a complete local intent matching the recorded child, a canonical manifest, and every version owned by the child. A failing child is repaired once.
    - Each winning version from a child must be stored here with its content, or as typed tombstone locations of the same kind. A missing copy is repaired once through the typed repair and proved again.
    - Anything unproved means no candidate and no report. A durable Complete flag alone is never the proof.
    - **A candidate staged before this start** (found staged for the pending target on reopen) inherits nothing. It is rebuilt with this start's proofs and repairs, and the rebuild must reproduce its staged chunks. Only then are its staged rows restored to exactly that inventory and its report (re)sent. A mismatch overwrites nothing and reports nothing. This holds whether or not its report was already recorded.
    - An equal full HLC with other content refuses the build, and so does any table in the candidate (inherited ones included) with a retention floor.
  - **Staging** is fenced: the active base must be the target's predecessor, a later staged target supersedes, the same target is idempotent or conflicting, and a key appears once.
  - **Report.** The report is signed from the durable staged metadata only.
- **Activation.** After the commit applies, one transaction does all of the following:
  - checks the staged candidate's gid, target digest, predecessor and chunks against the certificate, with the active base at the predecessor, and checks that the staged **rows** hash to the certificate's chunks;
  - replaces the active base and clears the staged one;
  - deletes the intents and owners of the erased parents.

  A committed generation with no matching candidate, or with staged rows that do not hash to its certificate, leaves the store behind the boundary: a typed refusal, with no reset and no copy. Tombstones that win their key stay in the base.
  - **Trust follows the activated generation.** Activating a candidate this run did not build or re-prove first closes reads and readiness (the base is unvalidated again). After an unclean start, it also owes the base proof again before any close may record the directory clean.
- **Reader.** Per key, the newest of the base entry (proved stored, or typed tombstone locations) and the Accepted-at-K versions of retained bodies wins, and an unlisted version is invisible. The S4 read authority also requires:
  - K ≥ `K_min`: a statement whose cut predates the current generation ends with no success terminal, retryable (cut expired);
  - the local active generation is the committed one;
  - the store is not behind the boundary;
  - the base is validated for this start.
- **Lifecycle.** The build, the report and activation run under a `LifecyclePermit` that shares drain accounting with writes. A permit dropped without completing sets a sticky revocation: that run never records a clean shutdown, and the next start is unclean. A permit is refused once close has begun. An orderly close first stops the decision maintenance loop between steps, while Raft still runs, and waits, bounded, for its current step. Only a step still running past the bound is aborted, and its permit then revokes the clean record.
- **Restart.** The node is not Ready (`CertifiedBaseUnvalidated`), reads refuse, and nothing is built or reported, until the base is validated for this start:
  - the leader's cut is applied first, so the committed generation compared is the cluster's current one;
  - a committed generation still staged is activated first (the crash between Commit and activation);
  - the active base must hash to its recorded chunks, and those chunks must equal the current certificate;
  - after an unclean start, every entry must also have a stored copy with its content, or typed tombstone locations.

  A staged, uncommitted candidate never replaces the active base.
  - **The clean record.** An unclean start under ParentDecisions owes this proof to the data directory's owner. Until a successful validation discharges it, no close records the directory clean, whatever other recovery finished, and the next start is unclean again. Discharging it never clears a sticky revocation.
- **Frozen policy (`survivor_policy` 1).** In this mode the reducer refuses, at apply, table drops, tenant deletes and retention-policy changes, and CTAS, a write path outside parent decisions. Descriptors carry no retention. The reader refuses a table with a retention floor, and the build refuses to certify one. Physical versions stay contained (S4) until S5b.
- **Capacity (arithmetic, not measured).** Records stay at least `DECISION_EXPIRY_MS` (about 65 minutes). At about 0.4 KiB per single-row claim, the 256 MiB retained bound sustains about 170 parents per second before typed backpressure, until a generation commits. This is eventually bounded state, not a throughput claim, and rows per second are not parents per second.
- **Controls.**
  - Driver: after every member reported, a member's store is replaced (a new incarnation). The full report set is not committable; the driver replaces the target from the same predecessor at a fresh gid with the current members; a late report for the old target is refused; the replacement commits and erases the cut.
  - Reducer: the closed-cut counterexample (rsv 10, T 20, dec 30 is not erasable); a Reserved parent or an unretired key blocks the cut; wrong digest, partial chunks, disagreement and a changed re-report are refused; the barrier waits for every member; an abandoned target, then a skipped-gid commit from its predecessor; a late report for the abandoned target; erasure, bytes, `K_min`, `Fr` and validation.
  - Storage: staged rows changed under unchanged metadata never activate and change nothing; a restage with other evidence is refused; a restage that reproduces the evidence restores the rows, which then activate; chunk sums cover every chunk and depend on content; staging is fenced by the predecessor and by later targets (a late worker is superseded); a mismatched activation changes nothing; activation is atomic and idempotent and retires only intents at or below T.
  - Lifecycle: an interrupted permit revokes the clean record for good, even after a later permit completes; close drains permits and refuses new ones; an orderly close that begins while a certification step holds its permit waits for the step and records the directory clean.
  - Frozen policy: a retention change, a table drop, a tenant delete and CTAS are refused in this mode, and change nothing.
  - Three voters, under a controlled clock (a finite multi-generation run, not a capacity test):
    - generation 1 erases every record and byte, every node's base holds one entry per key, and the erased intents are retired;
    - reads stay exact from the base, and newer versions win over base entries;
    - a statement paused after its cut across generation 2 ends with no rows (cut expired);
    - a clean restart and an unclean restart each refuse reads until validated, then answer exactly;
    - for generation 3, a deleted base row keeps its tombstone as the key's entry, a noop is erased, no candidate stays staged, every covered intent is retired in every state, and the metadata is the current certificate only;
    - a follower that staged and reported generation 3 and crashed before activating it refuses reads, then activates on reopen and answers exactly;
    - a member stopped while the target waits for its report reopens on its store with the same incarnation, reports through the maintenance path, and the same target commits, activates everywhere and retires the records;
    - a follower whose complete intents lost their stored copies (a row and a winning tombstone) repairs them from a peer before it reports, and then reads exactly from its base; a version whose owner entry is not its child's gets no report from that store, and the generation does not commit;
    - a member that reported, lost a survivor's copy and had its staged rows shortened, then crashed before Commit, re-proves the recovered candidate on reopen: the copy is repaired from a peer and the staged rows are restored. The generation then commits, and it serves exactly and closes clean;
    - the same damage with the generation committing while the member is down: on reopen the shortened rows are never installed, the store is behind the boundary, every read is refused, it is not Ready, and a complete close does not record it clean;
    - an unclean start whose base fails its physical proof stays not Ready (`CertifiedBaseUnvalidated`) with reads refused after the tier recovery completes, twice; no complete close records it clean; once the base proves, reads are exact, a close records it clean, and the next start is clean.
- **Not in S5a:**
  - physical cleanup and the survivor rule replacing containment (S5b);
  - a node-level MVCC retention setting is refused by the reader and the build, not at apply;
  - KEK retirement is not refused in this mode;
  - there is no three-voter control of the build's equal-HLC refusal.

## S5b: certified cleanup of sealed local slabs
Review record: `reviews/dc54-index-continuity-r1/l2/s5b/` (DESIGN, CODEX-1, ADDENDUM-1, CODEX-2). With it, every tiered version is kept in this mode, and a node retention window is refused here.
- **Positive debt.** `cleanup_debt_v1`, keyed `(tenant, key, HLC, table)`, is written only inside the activation transaction:
  - each version of a retired intent that this intent still owns and that is not its key's new base entry (an equal HLC protects either kind), with the manifest digest and the intent;
  - each previous base entry displaced by another version of its key, with its digest and kind.

  Activation now removes only its own intents' owner entries. A retry of an active generation records nothing. Collection starts with S5b: S5a's earlier activations kept no evidence, so their obsolete versions stay contained.
- **The survivor rule.** A stored local version is reclaimable only if ONE index read transaction finds debt for it under exactly one table (agreeing with any table its frame stamps), no owner entry, and no active base entry of that table at its HLC; and every stored copy matches the debt's kind and digest, under the table's descriptor as the writer resolves it (the reserved KV table's is synthetic). An unlisted, ambiguous, owned, base-protected or mismatching version stays. Evidence that cannot be established now (no descriptor, an unreadable copy) or any index error refuses the step retryably, and nothing is cached.
- **Evidence in the API.** `CoherentFacts` is built only by that one read, for exactly the claimed slab's versions. `ContentEvidence` is built only from the stored copies. `ReclamationPlan::decide` is the only way to a plan, and it records the active generation. The rewrite reclaims in this mode only under `ReclaimAuthority::Certified(plan)`: it must reclaim exactly what the plan decided, and a newer activation since the decision refuses it. Ordinary rewrites stay contained.
- **The step.** Each decision-maintenance tick, once the base is validated for this start and the store is not behind the boundary, one bounded step runs under a lifecycle permit. It claims one sealed local slab through the existing maintenance claim, decides, and rewrites through the existing rewrite and install path. The cursor skips a slab only once all of its evidence was read and none was reclaimable, until the next activation. A bounded sweep retires a debt record only if, in one write transaction, it is still exactly the record seen, with no owner, no base entry at its HLC and no indexed copy (never-appended versions included).
- **Re-admission.** `begin_parent_intent` already refuses a version that has stored copies (`ForeignVersion`), so a version gains an owner only after cleanup has removed every copy, and an owned version is kept.
- **Not cleaned:** tiered slabs (contained in this mode); LSM merges (the picker stays off in this mode); active slabs; anything unlisted. **LSM certified elision stays an open obligation of this ADR, not delivered by S5b:** all five elision call sites under one captured plan, plus LSM recovery.
- **Bounds.** Debt is cumulative history, not a steady state; tiered copies, preserved conflicts and pending publications can hold it indefinitely. A distinct-version count is not a byte bound, and `MAX_RETAINED_CLAIM_BYTES` bounds admission metadata, not payload bytes or elapsed time.
- **Controls:**
  - Storage: activation records exactly the losers and the displaced entry; another producer's owner survives; a retry adds nothing; facts come from one read; debt retires only from its exact record, never while owned again, and without a rewrite when nothing was appended.
  - A plan decided at one generation refuses after the next activates; a fresh plan reclaims exactly the obsolete version; the winner, an unlisted row and mismatched-content debt stay; debt retires once no copy remains.
  - Three voters, under a controlled clock: overwrites and a delete, then one generation. Every node reclaims exactly its obsolete versions and retires their debt, the winners and the winning tombstone stay, and reads are exact. A KV key's obsolete versions are reclaimed the same way. A copy read that fails once refuses that step, and the same generation's next visit reclaims. A cleanup interrupted before its rewrite leaves the next start unclean, the base proved and reads exact.
  - Crash boundaries, on a mixed slab (obsolete, winner, retained parent's version, unlisted row, mismatched debt):
    - a crash after the replacement is written and before it is published reopens with every row, and a fresh cleanup then reclaims exactly the obsolete version;
    - a crash after install reopens with the obsolete version gone and every protected row stored.

## Not established
Anything about the protocol's correctness, write latency, §8 (still FAILED and open), copy and re-entry, checkpoint trust, governance, upgrades, indexes, counters, migration, or a readiness date. The production-required list in the contract stays open.
