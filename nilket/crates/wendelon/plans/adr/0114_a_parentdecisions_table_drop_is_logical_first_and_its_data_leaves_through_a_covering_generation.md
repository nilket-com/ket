# ADR 0114 - A ParentDecisions table drop is logical first, and its data leaves through a covering generation

## Status
Proposed, 2026-11-19. **Stage (a) only is implemented by this ADR's implementation.** Every later stage, and both serving refusals, stay held.

**The lineage:**
- **DROP-01–07:** the design, and the race controls behind DC-269, DC-271 and DC-272, all delivered on main before this plan;
- **DROP-08–11:** receipt authority, publication authority, and the Prepare stage;
- **the rulings:** Codex 01a1f4a5 … 01a1f66b.

**This is the fourth unit of the cutover** to ParentDecisions as the sole serving decision authority, after product ADR 0738 and wendelon ADRs 0112 and 0113.

**Both refusals stay in force:**
- the replicated refusal of `TableDropPrepare` under ParentDecisions;
- the typed admin refusal `UnsupportedUnderParentDecisions` (DC-268).

General SQL DROP stays gated.

## Context
**Today, a drop** fences the table and drains its writes (Prepare), unbinds the name with a tombstone, debt and an immutable participant stamp (LogicalFinalize), reclaims the data by L1/L2 sweeper slices, and retires the descriptor (Settlement).

**Under ParentDecisions that sequence is unsafe:**
- **The base needs the descriptor:** the certified base carries every active entry forward, and live entries need their table's descriptor.
- **Commit happens at Finalize, not at the write:** a parent Reserved with a quorum of valid acknowledgements when the fence lands concludes **Accepted after the fence** (DROP-04's control).
- **A drop needs a store's sign-off (receipt) that no row of the table remains.** No such evidence exists today (DROP-08):
  - **records carry no table key;**
  - **SingleRow-layout and default-KV data rows are stored unattributed** (`table_object_id: None`);
  - **raw append bytes survived a manager reopen:** an append whose index write failed became indexed and locatable through `SlabManager` after reopen (DROP-10). That API does not force durability; this was not a power-loss trial.

**Delivered on main, which this design relies on:**
- **DC-269:** an installed certificate cut refuses any begin at or below it.
- **DC-271:** the debt sweep retires a retired intent's debt only under its producer's reservation stripe.
- **DC-272:** activation fences the producers of the intents it retires.

## Decision
### D1. Phases (ParentDecisions; Today is unchanged)
1. **Prepare:** fence, and conclude the table's Reserved parents (D2).
2. **LogicalFinalize:** name unbound; tombstone and outcome; descriptor retained.
3. **A covering generation** under survivor policy 2.
4. **Local activation:** fenced, as DC-272 does.
5. **Local erasure.**
6. **A store's receipt.**
7. **Settlement.**

**SQL-visible completion is step 2. Steps 3–7 are designed here, but only stage (a) is built.**

### D2. Prepare decides (stage (a), implemented)
**One successful `TableDropPrepare` fences the table and, in the same apply, concludes every Reserved parent of exactly that table within its tenant as `Refused { TableDropped }`, with `dec` set to the Prepare's log index.**

**Refusal checks, all before any change:**
- **the backup hold:** checked by this transition itself, because the shared Prepare does not check it;
- **the conclusion's premises:** every live reservation precedes this log index;
- **the shared Prepare's checks:** schema, binding, dependent indexes, outcomes, and fence conflict and replay.

**Outcomes:**
- **A refused Prepare** leaves no fence and no concluded parent.
- **A replay** concludes nothing new, so `live` is decremented once.

**Untouched:**
- terminal parents (an Accepted parent replays Accepted);
- KV parents and other tables;
- keys and retry floors, which keep their normal replay and retirement.

**After the fence:**
- a Finalize, Resolve or Abort of a concluded parent replays the refusal;
- a new Reserve or RecordNoop of the table is refused (`TableUnavailable`);
- **correctness does not depend on any Abort arriving.**

**Bounded:** a derived `live_by_table: (tenant, table) → {rsv}` of Reserved bodies.
- **Maintenance:** maintained at Reserve and in `conclude`, with emptied buckets removed; **never serialized**; rebuilt on decode; checked last in `validate`.
- **Cost:** Prepare visits only that table's bucket, at most the live parents.

### D3. Writers past intent admission
**Delivered by DC-269, DC-271 and DC-272. No table drain:**
- no producer of a retired intent survives its activation;
- no begin at or below the installed cut succeeds;
- debt of an intent's version is retired only under its producer's stripe.

### D4. The covering generation (later stage)
**Survivor policy 2:** a `(tenant, table)` whose drop has `finalize_log_index ≤ target.t` is excluded before any descriptor check; a mixed slab keeps the live table's rows byte for byte.

**When a covering generation commits:**
- **What it needs:** `covered(t)` needs every body and noop at or below t to be terminal with its key retired. Progress is therefore **conditional**: an unrelated Reserved parent or a missing member report delays it. **E is a retry-window parameter, not a deadline.**
- **On an idle cluster:** the leader's floor proposal (strictly advancing while its decision clock is healthy) supplies the entry that lets `t` reach the LogicalFinalize index. It must be proposed, committed and applied, and the target then needs every member's report.

### D5. Store receipts (later stage, held)
**Necessary conditions** (DROP-09, as accepted). Sufficiency is not established, so **no signing path exists:**
1. **The covering cut is installed,** and its activation was fenced.
2. **Authoritative row coverage:**
   - **the marker:** an initialized accounting marker;
   - **the counts:** per-(tenant, table) record counts maintained from the actual old and new rows at every `RECORDS_TABLE` write site, recounted at rebuild, with zero rows removed;
   - **the unattributed count:** `UNATTRIBUTED = 0`;
   - **no work outstanding:** no publication in flight, orphaned or restart-required, and no rebuild or deferred recovery pending.

   **Unknown coverage refuses to sign, byte-preserving.**
3. `count(tenant, T) = 0`.
4. **Every other lookup surface for T is empty,** each by a bounded check or an invariant that eliminates the scan: secondary-index segments, base, owners, intents and debt. (`intent_key` is `(rsv, ordinal)`, not keyed by table, so its check must be named.)
5. **Pointer correspondence:** the exact owner, incarnation, slab and content identity of each pointer the store signs for. A local G1 row against a replicated G2 row at the same slab is the negative control. **A predecessor's pointers stay a durable obligation** that Settlement checks.
6. **The replicated digest** of that pointer set, checked at apply and at Settlement.

**A future positive signing path must name the authority that excludes every post-sign re-indexing,** including durable append work before its publication marker. A readiness sample cannot do it. **That authority belongs to a separate recovery/publication-authority unit.**

### D6. Tiered data and participants (later stage)
**Settlement is blocked, durably,** where:
- a follower owns a tiered slab of T (dependency A: per-store tier rewrite authority);
- a stamped participant has departed (dependency B).

**Kept:** NO-L2-DELETES. "Settled" means unreachable and unindexed, never erased. A replacement cannot discharge an unaccounted predecessor.

### D7. Backup (later stage)
**Hold first:** Prepare is refused under a hold (D2 checks it).

**Drop first:** the roster answers `Unsettled`, and the export fails, bounded; a retry after settlement succeeds.

### D8. Formats and compatibility
**Stage (a) emits no new production bytes and changes no admitted behaviour:**
- `RefusalCause::TableDropped` exists **only in test builds**, so production can neither encode nor decode it;
- `live_by_table` is not serialized;
- the ParentDecisions Prepare stays refused in production.

**So the PD format stays at marker v20, WAL and snapshot 30, envelope 23, and the corpus is unchanged.**

**The first stage that emits the new variant or changes apply semantics owns its compatibility fence:**
- a PD format bump at that boundary, **or** a replicated capability that every current voter must hold;
- it excludes any peer that still refuses Prepare or still applies policy 1;
- **equal decoding is not equal apply.**

**Existing stores are never refused at open for receipt reasons. They refuse to sign.**

### D9. Staging
- **(a):** D2 (implemented).
- **(b):** policy 2 and the covering trigger.
- **(c):** receipts and settlement, after the recovery/publication-authority unit and the attribution work.
- **Enablement:** removes the refusals, only with dependencies A and B, or a durable admissible subset.

## Rationale
- **The Raft order of Prepare and its conclusion** makes "no parent of the table is Accepted after its fence" a replicated fact.
- **DC-269, DC-271 and DC-272 make activation the physical boundary** for writers past admission.
- **Receipts stay unbuilt until coverage is authoritative.**

## Consequences (The "Negatives")
- **SQL DROP stays unavailable under ParentDecisions** until D9's enablement.
- **Physical settlement can lag without a deadline,** and backups fail, bounded, meanwhile.
- **Stores without authoritative coverage can never sign,** so their drops stay unsettled.

## Acceptance (stage (a))
**Two kinds of evidence, described separately:**
- **reducer evidence:** controls 1–5 drive `TableDropPrepare` through the real dispatcher (`apply_request_at_log_index`) on a fixture state the test admits (`SmState::pd_drop_prepare_admitted`, test-only, never serialized);
- **replicated evidence:** control 6 proposes it through Raft.

1. **One Prepare concludes the table's two Reserved parents** (one with a quorum of gathered acks, one abandoned), with the same `dec`:
   - an Accepted parent keeps Accepted, and its replay;
   - **witnesses keep their exact status and bucket:** a Reserved KV parent, a Reserved parent of another table in the same tenant, and a Reserved parent of the same table id in another tenant;
   - `live` drops by exactly 2;
   - Finalize after the fence replays the refusal;
   - a fresh Reserve and a fresh RecordNoop of the table are refused.

   **RED with the conclusion neutralised:** the racing parent stays Reserved.
2. **A refused Prepare changes nothing,** comparing decision state, fences and the pending parent:
   - with admission off (the production refusal);
   - with a generation mismatch;
   - under a backup hold.
3. **A replayed Prepare answers `Success`,** concludes nothing new, and decrements `live` once.
4. **A snapshot taken before Prepare, with the original state ADMITTED and Reserved parents,** decodes through the production body codec:
   - the decoded state does **not** admit, because admission is not serialized;
   - the bucket is rebuilt and validated;
   - re-admitted explicitly, the same Prepare at the same index gives equal decision states and fences, both valid, with the bucket emptied.
5. **The existing gate-off test** stays green, and the PD corpus and goldens are unchanged.
6. **The replicated control** (`parent_decisions_cluster_tests::drop_prepare`), on three in-process voters:
   - **setup:** every replica is admitted by `WendelonStateMachine::admit_pd_drop_prepare_for_test`, which is test-only and off by default, with no public route or production setting. A machine applies its admission to any snapshot state it installs; one rebuilt from its log starts unadmitted, so the test neither restarts nor installs a snapshot after admission;
   - **the parent:** a write's coordinator pauses before Finalize, keeping it Reserved;
   - **the Prepare:** proposed on the leader with `client_write`, answering `Success` at committed index L;
   - **the check:** a follower applied through L, and **the leader and that follower each hold the fence and `Refused { TableDropped, dec: L }`** for the parent.
