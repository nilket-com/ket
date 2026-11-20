# ADR 0115 - A certified generation excludes a logically dropped table at its finalized cut

## Status
Proposed draft, 2026-11-20. **Design only; implementation held.**

**Its place:** stage (b) of wendelon ADR 0114, which this ADR cites. 0114's stage (a) (the Prepare decides) was delivered at `ff9cdeb7f`.

**Rulings:** DROP-12, Codex 01a1f6aa and 01a1f6ad.

**Both production DROP refusals stay in force.** SQL admission, reclamation receipts, settlement and enablement are separate units.

## Context
**After ADR 0114 stage (a),** a ParentDecisions drop Prepare (test-admitted) fences the table and concludes its Reserved parents. LogicalFinalize then records a permanent `TableDropOutcome { operation_id, finalize_log_index }` and a `TableDropTombstone`. Settlement later removes the tombstone and **every descriptor generation** (`retire_unbound_object`); the outcome is never removed.

**The certified base:**
- **It carries every active entry forward:**
  - the survivor rule is policy 1: "newest Accepted version per key, winning tombstones kept";
  - candidates are built per key `(tenant, table, key_identity)` from the active base plus Accepted bodies at or below t;
  - a child needs its table's descriptor.
- **Nothing excludes a dropped table,** so its entries would stay certified forever and block settlement.

**Two further gaps:**
- **An idle drop never gets a target:** `next_target` proposes only when a body or noop at or below t exists.
- **The policy is checked in one place only:** `propose_target` checks it; the decision-state `validate` checks a pending target's digest but not its policy; and members rebuild a pending target directly in `prepare_report`.

## Decision

### D1. The cumulative exclusion filter (survivor policy 2)
**`Excluded(t)`:** the set of `(tenant, table)` that have a `TableDropOutcome` with `finalize_log_index ≤ t`.

**For a target with cut t, the build:**
- **drops from the inherited base seed** every entry of an excluded `(tenant, table)`;
- **skips in `prepare_report`'s `read_state`** every Accepted child of an excluded `(tenant, table)`, **before** any generation, descriptor or plan lookup;
- **includes every other slot** exactly as policy 1 does.

**The target's policy** is **2** iff `Excluded(t)` is non-empty, and 1 otherwise. **The digest binds the cut and the policy; the shared, immutable outcome history determines the excluded set.** No serialized list is needed.

**Determinism:**
- An outcome with `finalize_log_index ≤ t` was applied at or before t, and `t < recorded_at`. Every member that builds, reports or restages has applied past `recorded_at`.
- **Outcomes are immutable and never removed,** so `Excluded(t)` is identical everywhere and at every restage.
- **The filter never reads** a tombstone, a descriptor, or the member's current applied state beyond t.

**With two drops** `F1 ≤ t < F2`, `Excluded(t)` holds only the first, even when both finalizations were applied by build time. A target at `t < F2` can already be policy 2 because of F1.

**The first excluding generation for a drop at F** is the first **committed** target whose closed cut is `t ≥ F`. A pre-F target includes the table fully, and every later target keeps excluding it.

**Other tables:**
- **Exclusion is per `(tenant, table)` slot:** every other slot's entries, positions and preimages are byte-identical, and only the chunks that held excluded entries change count and root.
- **The same object id under another tenant** is a different slot, and unaffected.
- **Physical slabs are not rewritten** by certification.

### D2. The idle trigger, distinct from the filter
**`next_target` also proposes when some outcome has `finalize_log_index ∈ (fr(), t]`**, for the closed cut t it computes. That outcome is the reason to propose, even with no erasable body or noop.

**All existing preconditions are kept:**
- a current pending target suppresses proposals;
- the cut search (`applied − 1`, then below each `blocking_point`);
- `covered(t)`, including retry-floor key retirement;
- current membership;
- every member's report before Commit.

**The trigger goes quiet** once a committed certificate's `t ≥` every outcome's `finalize_log_index` (then `fr()` is at least that).

**Progress is conditional, with no deadline:**
1. **An entry after F must be applied:** while the leader's decision clock is healthy, the strictly advancing floor (`AdvanceFloor`, one per leader tick) gives one; it must be proposed, committed and applied.
2. **`covered(t)` must hold.**
3. **Every member reports.**

**E is a retry-window parameter, not a deadline.**

**Cost:** the trigger needs only *existence* of a finalize index in `(fr(), t]`, and per-table exclusion uses the permanent catalog's keyed lookup.
- **The index:**
  - **owner:** `SmState`, beside `table_drop_outcomes`;
  - **content:** a derived `pd_finalize_indices: BTreeSet<u64>` of the finalize indices of ParentDecisions drop outcomes;
  - **maintenance:** **incremental, at the common outcome insertion boundary.** Every outcome is recorded through one helper, `record_table_drop_outcome(state, outcome)`, used by both LogicalFinalize and the legacy Finalize (`raft_storage.rs:8515`). On an admitted ParentDecisions machine it also inserts the finalize index. Outcomes are never removed, so there is no removal path. **The decode rebuild reads the whole catalog through the same rule, so the incremental and rebuilt indexes agree** (checked in validation);
  - **lifecycle:** never serialized; rebuilt from the catalog on decode and on cold recovery; checked against the catalog in assembled-state validation (D3).
- **Admission scope:**
  - **the index exists only on an admitted machine** (D3): Today and unadmitted ParentDecisions machines neither build nor maintain it, and pay nothing;
  - **after a future enablement** it is O(number of historical PD drops) resident entries, about 8 bytes each plus B-tree overhead, stated as that enablement's cost.
- **The trigger:** one existence query `range((fr(), t]).next()`, O(log d), on each `next_target` call.
- **The build:**
  - `Excluded(t)` is never enumerated;
  - each candidate slot's `(tenant, table)` is checked by a keyed catalog lookup, `finalize_log_index ≤ t`, O(log d), memoized per distinct `(tenant, table)` seen in this build;
  - the extra allocation is O(distinct tables in the candidate) for the memo;
  - total time is O(entries + children) lookups into the memo.

### D3. Admission is a construction-time input of the store, and reaches every path before use
**Why 0114's post-construction setter cannot serve this stage** (`ff9cdeb7f`):
- **decode:** `decode_state_body` restores and validates `envelope.decisions` **before** combining it with `envelope.sm` (`raft_storage.rs:11601`), so a policy/outcome check cannot run there;
- **install:** `install_snapshot` copies the machine flag in **after** publishing to the current snapshot file (`:5659`);
- **cold recovery:** `recover_sm_inner` replays the suffix **before** the machine exists.

**So admission becomes part of the store's own configuration,** fixed when the store opens:
- **the source:** a `#[cfg(test)]` field of the raft-store open options, beside `decision_mode`, default **off**. A test sets it in a node's configuration before that node opens;
- **the carrier:** `SnapshotRuntime` holds it. That is the per-machine configuration already handed to `recover_sm_inner`, to decode, and to `install_snapshot`;
- **there is no setter after construction, no public route, and no production field.** 0114's `admit_pd_drop_prepare_for_test` setter is replaced by this input. 0114's admitted Prepare reads the same flag, so there is one mechanism.

**Where the admission is applied, in order:**
1. **Decode, then validate the assembled state:** `decode_state_body` assembles `envelope.decisions` with `envelope.sm` as today. Then a new **assembled-state validation**, given the admission, runs **before** the state is returned:
   - it rebuilds `pd_finalize_indices` (admitted only) and checks it against the catalog;
   - **for a pending target, it checks `policy == expected_policy(outcomes at target.t)`**: 1 iff no outcome has `finalize_log_index ≤ t`, else 2. The digest binds t and the policy; membership in the exclusion set relies on the same replicated outcome history, so **a policy that disagrees with the outcomes at t fails even when the target's digest is internally correct**;
   - **unadmitted: any policy-2 pending target fails** (`InvalidData`).
2. **Cold recovery:** `recover_sm_inner` decodes the snapshot through (1), with the runtime's admission, sets the state's admission, and **only then** replays the committed suffix. The first replayed command already sees the admission.

**The replay contract, separate from snapshot validation:**
- **A refused proposal is a valid Raft apply outcome.** `ProposeTarget` returns `WendelonResponse::Decision` with its refusal (`raft_storage.rs:7378`); recovery propagates only outer apply or decode errors, discards the response, and advances `last_applied` (`:4545`).
- **So an unadmitted replay of a committed `ProposeTarget(policy 2)`** is a refusal, with no pending target, and **the open succeeds.** Turning reducer refusals, or the sight of a policy-2 proposal in the log, into fatal recovery errors would change the replay of legitimately refused commands, so neither is done.
- **Admitted test stores therefore require a same-admission reopen,** as an explicit prerequisite: a store written under admission is reopened under admission.
- **No cross-admission refusal is claimed for histories held only in the log.** A store does not record its admission durably, and none is added.
- **What *is* refused across admission** is the distinguishable case: a **snapshot already holding an accepted policy-2 pending target**, which an unadmitted machine refuses at the assembled-state validation, before use or publication (1, 3).
3. **Install:** `install_snapshot` decodes through (1) with the machine's admission **before** publishing.
   - **Unadmitted, a policy-2 pending snapshot is refused before publication.** The previous durable snapshot stays current.
   - The state the install publishes already carries the admission. The post-publication copy at `:5659` is removed.
4. **Apply:** `propose_target` admits policy 2 iff admitted **and** the outcomes at t require it. It refuses a policy that disagrees with the outcomes at t.
5. **Member-side:**
   - `prepare_report` refuses to report a policy-2 target unless admitted;
   - the initial build and **the recovered restage** (`restage_base`'s rebuild) apply the filter only when admitted.

**Without admission (production and every unadmitted machine):** decode, validation, replay, install, apply and build behave exactly as today, except that policy-2 states are refused as above. **Valid policy-1 states, snapshots and logs are unaffected, byte for byte.**

**The format and capability fence:** stage (b) emits nothing in production and changes no reachable apply, so **the fence is deferred to the unit that first admits or emits policy 2 in production.** That unit must exclude older voters (a PD format bump or a replicated capability) **before** admission.

### D4. Enablement obligations recorded here
1. **Reused object ids:**
   - creation (`apply_schema_update`'s Create, `raft_storage.rs:7786`) checks only `table_schemas.latest_by_id`, and settlement removes every generation while the outcome survives;
   - **a reused id would let policy 2 exclude new data;**
   - **before enabling DROP, Create must also refuse any `(tenant, id)` that has a `TableDropOutcome`.**
2. **Settlement must wait** for the excluding generation's activation **and** the completion of the table's cleanup, because certified cleanup needs the table's descriptor (`content_evidence`), which settlement retires.

## Rationale
- **An exclusion derived from immutable replicated outcomes at the target's cut** is a pure function every member computes identically. The digest already binds t and the policy.
- **Keeping the trigger separate from the filter** lets an idle cluster reach the cut without changing which drops a given cut excludes.

## Consequences (The "Negatives")
- **A drop's exclusion waits for `covered(t)`:** every key at or below t must retire, for any table. Without a deadline.
- **Policy-2 targets appear only after enablement.** Until then the code is test-admitted only.
- **Unadmitted machines refuse policy-2 states.** No production state can contain one before enablement.

## Acceptance (test-admitted; failing controls first)
1. **The idle trigger:**
   - with an admitted Prepare and LogicalFinalize at F and no body or noop, `next_target` gives `None` until `applied > F` and covered, then a target with `t ≥ F` and policy 2;
   - quiet after a commit with `t ≥ F`;
   - **RED on `ff9cdeb7f`:** `None` forever.
2. **The filter versus the trigger:** two drops `F1 < F2`:
   - a target with `F1 ≤ t < F2` excludes only the first, even with both outcomes applied at build time, and is policy 2;
   - a later target excludes both.
3. **Survivors, on identical source input** (inherited base entries and Accepted children of T and U, sharing a chunk):
   - U's preimages are byte-identical;
   - other chunks' sums are unchanged;
   - only the shared chunk changes;
   - two members build identical candidates, and Commit succeeds.
4. **Same name, fresh id:**
   - a table re-created under a fresh id after the drop is included;
   - **the same id under another tenant** is included;
   - a reused id is refused at Create, as an enablement control.
5. **The recovered restage:**
   - after LogicalFinalize, **remove the tombstone and every descriptor generation** of T (simulating settlement);
   - **the actual recovered-candidate path** (`restage_base` on reopen) rebuilds the identical candidate, with no descriptor lookup for T.
6. **Activation:**
   - T's base entries become `DisplacedBase` debt;
   - T's retired intents become `RetiredIntent` debt;
   - U is untouched;
   - DC-269's cut and DC-272's fence hold.
7. **Admission across the snapshot boundary,** using **one** pending policy-2 snapshot with a matching digest:
   - **admitted:** a cold open, and a follower's install, both succeed;
   - **unadmitted:** the open fails closed and the install is refused **before publication**, with the previous durable snapshot still current, its bytes kept;
   - **same-admission reopen:** for a store written and reopened under admission, a policy-1 snapshot whose committed suffix first introduces policy 2 recovers with **the live and recovered pending target and digest, the outcome state and the applied index all equal**;
   - **a committed, refused proposal followed by a valid entry, on an unadmitted machine:** a `ProposeTarget(policy 2)` committed and refused, then a valid entry. The reopen succeeds, with no pending target, the applied index past both, and the live and recovered states equal;
   - **a pending target whose policy disagrees with the outcomes at its t,** with an internally correct digest: refused by the assembled-state validation in both lanes;
   - unadmitted, `propose_target` refuses policy 2, and `prepare_report` refuses a policy-2 target;
   - with no outcome, targets, digests and snapshots are byte-identical to `ff9cdeb7f`.
8. **Cost:**
   - the trigger is one existence query on `pd_finalize_indices`;
   - the build uses memoized keyed lookups;
   - **unadmitted and Today machines hold no index** (a control asserts the index is absent);
   - validation fails closed if an admitted index disagrees with the catalog.
