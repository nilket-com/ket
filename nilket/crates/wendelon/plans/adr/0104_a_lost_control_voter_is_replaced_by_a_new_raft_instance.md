# ADR 0104 - A lost control voter is replaced by a new Raft instance

## Status
Accepted for implementation, 2026-11-15 (member replacement gate 1; Codex 01a1de91, with its precision edits folded in). Not delivered. This ADR establishes gate 1 only; gates 2-4 stay closed. The operator chose automatic instance exclusion ("Option 1").

## Context
- In ParentDecisions (PD), a voter that has lost its disk cannot simply return. A blank store under the old id would vote and acknowledge entries it no longer holds.
- Today, a fresh store under a current voter id is reachable and grants votes before JOIN is sent (STARTUP-GATE-15, hazard window 5→7).
- The operator chose automatic exclusion. Old honest instances may return with their old disk or keep live sessions. No operator isolation, unique address or host destruction is assumed.
- OpenRaft 0.9.24 counts votes (`handle_vote_resp(target: C::NodeId)`) and replication progress by `C::NodeId`. Only a distinct id that the core counts keeps an old instance's replies out of a new instance's quorum.
- **Trust model (unchanged):** operator-issued privileged node credentials, and honest participants under crash, restart, partition and stale-state faults. No Byzantine instance authentication, and no detection of a bit-perfect rollback of a whole store.

## Decision

### D1. Two id spaces, typed
| Type | Meaning | Users |
|---|---|---|
| `RaftInstance(u32)` (newtype; `WendelonTypeConfig::NodeId` stays `u32`) | one durable Raft store's identity, never reissued | OpenRaft core (votes, progress, membership ids, `LogId.leader_id`), the Raft network adapter, raw log coordinates, WAL authorship, root2 `EntryPosition2` |
| slot (existing `NodeId{id}` / configured node id) | logical member | placement, epoch peers, ring, lineage, signer, incarnations, SAN, addresses, client routing |

- **Identity rule:** `R < 2^31` has slot R by rule, and is issued only at formation or by a Join grant. `R ≥ 2^31` is allocated, and its slot is `instances[R].slot`.
- **`slot_of(state, R) -> Result<Slot, UnknownInstance>`** is the only crossing. It is total for `R < 2^31`. For an allocated R with no record it returns `UnknownInstance` and never guesses.
- **PD-only restriction (new, explicit):** configured node ids lie in `1..2^31` (2,147,483,647 slots). A higher id is refused at config load and at grant.
- **Allocator:** `enum Allocator { Next(u32), Exhausted }`, starting at `Next(2^31)`. It issues `Next(n)` and becomes `Next(n+1)`, or `Exhausted` after `u32::MAX` has been issued. That is exactly 2^31 = 2,147,483,648 allocations, with no wrap. `Exhausted` refuses every replacement grant (typed `InstanceSpaceExhausted`).
- **Today mode:** unchanged. It keeps its full existing u32 node-id domain, high-bit values included. Today has no allocator and `slot_of` = identity. A Today high-bit node id is never read as an allocated PD instance. Every PD allocation operation and artifact (`GrantInstance`, `BeginReplacement`, the PD page layout) is refused in Today.

### D2. Replicated state (PD DecisionState; all never pruned; carried by the PD snapshot; rebuilt identically by replay)
- `instances: BTreeMap<u32 /*R*/, InstanceRecord { slot, birth: [u8; 16], origin: Join | Replacement{token} }>`: every GRANTED instance, including Join grants with `R < 2^31`. Formation's R is not granted (D4) and has no entry.
- `allocator: Allocator`.
- `retired: BTreeSet<u32>`: every instance id a committed membership change removed. An id leaves at most once and is never granted or admitted again.
- `replacements: BTreeMap<Token, Replacement { slot, r_old, pre_voters: BTreeSet<u32>, phase: Begun | Reserved{r_new, add_index: Option<u64>} | Done{r_new} }>`. `pre_voters` is the applied uniform voter set at Begin. At most one is open at a time. Done records stay, so a replay of Begin, Reserve or Done under a completed token answers its Done record and never starts a second replacement.
- `Node::remove_member_for_replacement` (node.rs 1665; Today-era, no caller) is refused in PD.

### D3. The Raft-store page: birth, identity, role, vote
The PD vote page (`vote.wnd`, one O_DIRECT page, rewritten in place, fdatasync before return) becomes:
```text
{ birth: [u8; 16],
  identity: Option<{ instance: u32, origin: Formation | Join | Replacement, role: Full | ControlOnly }>,
  vote: Option<Vote<u32>> }
```
- **Birth B.** 128 bits from the OS RNG, written by the FIRST page write. That write happens at the first open of a pristine store, after the PD commit-0 checkpoint holder (which `open` already creates first). B lives and dies with the vote and the identity: there is no sidecar.
- Today's vote page is unchanged.

**Classification at open** (raft_storage.rs `inspect_artifacts` / `is_pristine`):

| Artifacts | Class | Allowed |
|---|---|---|
| no artifacts | pristine | open writes the holder, then `{B, None, None}` → born |
| holder only (crash between the holder and the birth-page write) | pristine (the existing `is_pristine` holder exception, made explicit) | open writes a new birth page. Safe: an identity exists only inside a page, so a holder-only store never held one. A lost already-issued identity means a lost page, so the new birth B′ is refused by every grant that names the old one (D4) |
| holder + page `{B, None, None}` | born (no obligations: no Raft object has ever existed for it) | first-identity paths (D4), each bound to B |
| holder + page with identity + empty log | identified. This is NOT proof that it never started: the page may carry a durable vote | resume per `origin` (D4) with the page and its vote untouched. The page is never rewritten with `vote: None` |
| holder + page with identity + log | Recovery | same-store restart under R; role read from the page |
| any page without the holder | invalid | fail closed (known log loss: the holder precedes the page) |
| log, snapshot, checkpoint or committed state without an identity-bearing page | invalid | fail closed; R is never re-derived |

### D4. First-identity paths (all before `WendelonRaft::new`; no Raft object or listener exists before the identity write)
- **Grant request.** `GrantInstance { birth B, kind: Join{slot N} | Replacement{token} }` is one new typed control message. It is answered only by the leader, linearizably, as a committed state change. It reuses STARTUP-GATE-15's boundary: an old server's unknown-type error fails the joiner closed.
- **Idempotence by birth AND intent.** If some `instances[R].birth == B` and that record has the same slot and the same kind (and, for a replacement, the same token), the answer is that R, whatever the completion state. The same birth with a different slot, kind or token is refused: it is not a grant for that other operation. A grant is never re-made for another birth.
- **Route authentication.** `GrantInstance` keeps ADR 0103's actor rule: an unscoped `DataNode` whose node id equals the authenticated remote. In addition, the request's slot (Join: N; Replacement: the record's slot) must equal that authenticated slot. ADR 0103's checks on other routes are unchanged.

| Path | Leader-side check (committed) | Identity write | Order after the identity write |
|---|---|---|---|
| Formation (Bootstrap mode, born store, formation intent file present) | none (no leader exists) | `{B, {R = slot, Formation, Full}, None}` + fdatasync, **then** the intent file is unlinked and the directory fsynced | `Raft::new` → publish → `initialize(members)` |
| Join (Join mode, born store) | N in `1..2^31`; N is not `slot_of` any current voter or learner; N not in `retired`; no `instances` record for slot N with another birth; no open replacement for N → `instances[N] = {N, B, Join}` | `{B, {R = N, Join, Full}, None}` | `Raft::new` → publish → existing JOIN (wire unchanged). `validate_completed_member_join` additionally requires `instances[N].origin == Join` |
| Replacement (born store, replacement token file present) | `replacements[token]` is Begun, or Reserved for this same B → `instances[r_new] = {N, B, Replacement{token}}`, phase `Reserved{r_new}`; `Exhausted` refuses | `{B, {R = r_new, Replacement, ControlOnly}, None}` | `Raft::new` (ControlOnly, D7) → publish the Raft handler → wait for the leader-side controller (D8) |
| Lost store, no token | the grant is refused: N is served or retired, or has a record under another birth | none | refuse: "lost store for a current member; run a replacement" |

**The three traces from 01a1de8b:**
1. **Replacement R reissue.** Reserve r_new for B, use it (vote or acknowledge), lose only the Raft artifacts while the data directory and the token survive, then restart. The blank store is pristine and gets a NEW birth B′. `GrantInstance{B′, Replacement{token}}` finds `replacements[token] = Reserved{r_new}`, whose record has birth B ≠ B′, and is refused. r_new is never handed to B′.
   - Lost response: the store is intact, so the retry carries the same B and gets the same r_new.
   - Crash before the identity write: the born page keeps B, so the retry gets the same r_new.
   - Crash after the identity write: the store is identified and resumes as itself.
2. **Concurrent fresh stores for N on ordinary join.** Each has its own birth (B1, B2). Grants are serialized by the log: the first committed grant records `instances[N].birth = B1`, and the second is refused. B2's process fails closed before `Raft::new` and never sends JOIN.
3. **Formation.** The intent is consumed (unlinked and fsynced) AFTER the identity page and BEFORE `Raft::new` and publication, so no Raft request can be served while the intent still exists.
   - A crash between the page write and the unlink leaves an identified store. The page is authoritative, and the leftover intent is unlinked on the next start.
   - If the Raft files are lost after the unlink, the intent is gone too: a pristine Bootstrap without an intent refuses.
   - If the Raft files are lost before the unlink, nothing was ever served under R = N.
   - On restart, an identified Formation store with an empty log reruns `initialize` under the same R, with its page and vote intact.
   - A NotAllowed from `initialize` is not proof that formation completed. Completion is judged only from applied membership containing R as a voter; until then, the store keeps resuming as a Formation store.

**Snapshots** carry `instances`, `allocator`, `retired` and `replacements`. A learner that installs a snapshot gets them atomically with the state.

### D5. Network envelope (PD Raft frame 17 → 18; Today 15 unchanged). Unchanged from r1
- The request carries `sender: {instance R_s, slot S_s}` and `target_instance: R_t`.
- The receiver checks these in order, after decode and before HLC harvest, OpenRaft dispatch and `confirm_replacement_replay` (the ADR 0103 position):
  1. the unscoped DataNode role;
  2. `S_s == SAN slot == context.remote`;
  3. `R_s == req.vote.leader_id.node_id`;
  4. `R_t ==` own durable R.
- Admission never consults membership, `instances` or `replacements`. That gives stale-view catch-up under the declared credential model.
- **Outbound.** R → `slot_of` → the slot's epoch-peer address and SAN. An allocated R with no applied record fails the call as Unreachable (transient while apply lags commit).
- **Wrong target, or any admission failure.** This is a stream failure with no response frame, and a prompt reset is required (an explicit reset if dropping the stream does not produce one). The caller stays suspect; ADR 0735 discards that exact session in both caches; the redial is judged again by `target_instance`.
- A delayed reply is credited to its own target.
- A returning removed instance can bump terms (no PreVote). That costs liveness only.

### D6. Application boundary (class D split). Unchanged from r1
- **Kept as R:** LogIds, WAL / `RaftLogEntryFixed` authorship, root2 `EntryPosition2` (dormant root2 untouched), membership, vote and progress.
- **Translated at apply time:** `apply_request_at_log_index` (raft_storage.rs 5592, 6675) and `admitted_retirement` (4814, 5454) receive `slot_of(state, entry.log_id.leader_id.node_id)`. `UnknownInstance` poisons apply.
- **Classes A–C:** "am I leader" compares with own durable R; hints leave Raft as `slot_of(R)`; coordinator equality is `slot_of(current_leader) == coordinator`.

### D7. Class E and control-only containment
| Consumer | Rule |
|---|---|
| OpenRaft quorum and progress | untouched: distinct R |
| `formed_placement` (decisions.rs 618) and the ADR 0720 seal (1325, 1749) | **Deliberate change.** Voters are projected through `slot_of` before the comparison with epoch-peer slots. The only tolerated learner is a Reserved `r_new` on an existing voter slot. Joint configurations still return None (refusal kept) |
| Secondary-index readiness (`reconcile_backfill_readiness`, schema/secondary_index.rs 480) | **Existing catalog semantics preserved:** it is given the slot set, which a replacement leaves unchanged. It promotes Building→Ready only, never demotes, and receipts stay slot-keyed |
| raft_storage.rs 1585/8322, decision_proposals.rs 357, node.rs 3758, backup roster | each site is classified "slot set" (project, deduplicate) or "Raft member" (keep R) at implementation, with one control per semantic class |

**Gate-3 requirement (stated, not built here).** Receipts and readiness evidence are slot-keyed today, so slot N's evidence from R_old would read as N's. Before any later gate lets a replacement serve, that evidence must become incarnation-specific. Gate 1 never lets the replacement serve, so nothing is decided on old N's receipts.

**Control-only role (durable; R3):**
- A Replacement identity is written with `role: ControlOnly` in the Raft-store page (D3). It stays there after Done and on every restart, until a later explicitly authorized activation. That activation is a gate-2/3/4 ADR operation; none exists in gate 1.
- Startup reads the role from the page before constructing anything. A ControlOnly boot builds a distinct `ControlOnlyNode`, which holds only:
  - the data-directory owner;
  - the log store, state machine and snapshot builder;
  - `WendelonRaft`, constructed with `Config { enable_elect: false, .. }` before `Raft::new` (openraft 0.9.24 config.rs 230; raft_core.rs 1443 checks it);
  - a Raft-only router (data and control handlers `None`).
- It constructs no `DataServer`, `ControlServer`, slab manager or `DecisionProposals`. So none of these can be spawned, by construction:
  - decision maintenance (node.rs 2474-2532: `ensure_signer_once`, `advance_floor_once`, `takeover_step`, intent sweep, `generation_step`, `validate_certified_base`);
  - the background services at node.rs 2772-2950: tenant policy reconciler, eviction watchdog, membership advancer, LSM compaction and GC, op driver, secondary-index lifecycle, lease manager, pending finalization, drop reclaim, CDC observers, migration and slab sweepers.
- **Implementation precondition (resolved FIRST, before the rest of the adapter).** Raft receive, apply, snapshot and restart/replay must be correct without a slab manager, on the real durable Raft store and state-machine path; a constructor-only or in-memory test does not count. Hooks to inspect:
  - `confirm_replacement_replay` / `finish_replacement_log_replay` (raft_server.rs 148);
  - the state machine's `TierObserverEvent` delivery;
  - checkpoint and durable-recovery callbacks.

  No durability work is removed or deferred, and no data callback is faked, to make it pass. If it cannot hold within this design, the concrete hook is reported and the work stops.

### D8. Transition and reconciliation (leader-side controller; resumable on any leader from applied state)
| # | Step | Settling committed fact | Reconciliation |
|---|---|---|---|
| 1 | `BeginReplacement{token, slot N}` (operator CLI → leader) | `replacements[token] = Begun{r_old = R serving N}`. Refused if another is open, N is not a voter slot, N's R is the local leader, or the token is known with different arguments | lost reply: the same token answers its record (any phase, including Done) |
| 2 | `GrantInstance{B, Replacement{token}}` | `Reserved{r_new}` + `instances[r_new]` | D4: idempotent by B; another birth is refused; `Exhausted` refuses |
| 3 | identity write, ControlOnly start | local page | D3 / D4 |
| 4 | `add_learner(r_new, addr(N), blocking=false)` | applied membership has r_new as a learner; record update `add_index =` that entry's committed index | appended but uncommitted: OpenRaft refuses another change, so wait for commit or truncation; never re-propose while that log id is present |
| 5 | promote when `matching(r_new) ≥ add_index` | none | wait; quorum loss stays pending |
| 6 | `change_membership(ReplaceAllVoters(T), retain = false)` with `T = pre_voters − r_old + r_new`, derived from the recorded pre-change configuration (never from the current one) | applied UNIFORM membership with r_new a voter and r_old absent from voters AND nodes | see below |
| 7 | `ReplacementDone{token}` | `retired += r_old`; phase `Done{r_new}` (kept forever) | derived from step 6's fact; idempotent by token |

**Step 6 resumption (R2).** OpenRaft 0.9.24 `change_membership` (raft/impl_raft_blocking_write.rs 49-101) makes two core calls with an await between them. A leader loss or crash between them leaves the cluster JOINT. The controller therefore always reconciles from the effective and applied membership:
- **Uniform, equal to T, r_old not a node:** go to step 7.
- **Joint exactly `[pre_voters, T]`, both sides validated** (any leader, after controller loss): call `change_membership(ReplaceAllVoters(T), false)` again. `Membership::next_coherent` → `find_coherent(T)` leaves the joint config for uniform T in ONE core call, and `retain = false` removes r_old from the node set.
- **Any other joint config** (including one whose last config matches T but whose first is not `pre_voters`), **or a uniform config that is neither T nor `pre_voters`:** a foreign change. Refuse and stop the controller with a typed error (PD forbids other membership changes while sealed).
- **Uniform `pre_voters` (plus the r_new learner) with no pending entry:** retry step 6.
- Completion is never inferred from the mere presence of an uncommitted membership entry.
- **Appended but uncommitted:** wait for commit or truncation.

**Losses:**
- **r_new's disk lost after step 6 commits:** Done is derived, and slot N is served by r_new. A fresh store gets a new birth, a new token and a new allocated R; r_new is never reissued.
- **r_new's disk lost before step 6:** the replacement stays Reserved, and another birth is refused. Gate 1 has no cancel (limit; the follow-on is "abandon: retire r_new, close the token").
- **A Join grant whose store is lost before JOIN completes:** the same limit applies to slot N.

### D9. Compatibility (pre-release; old PD stores refused, not migrated)
- PD WAL/snapshot format 24 → 25.
- PD mode marker v14 → v15.
- PD Raft frame 17 → 18.
- PD vote page layout (gated by the marker).
- New typed control messages: `BeginReplacement`, `GrantInstance`, `ReplacementDone`, and `MEMBERSHIP_QUERY` only if still needed for the operator's status view.
- Goldens `wal_v25` / `snapshot_v25`.
- Today unchanged.

## Control map (per semantic boundary; none run now)
| # | Boundary | Oracle |
|---|---|---|
| 1 | wrong target on a live old session | the old process refuses `target_instance = r_new` with no frame; the stream resets promptly; the caller stays suspect, discards that exact session in both caches, and redials r_new |
| 2 | delayed old reply | r_new's progress and vote tally are unchanged |
| 3 | blank store, surviving data directory and token | grant refused (birth mismatch); no Raft object or listener exists |
| 4 | same-store restart | R and role are preserved |
| 5 | catch-up before promotion | no promotion while `matching < add_index` |
| 6 | controller or leader killed after steps 1, 2, 4, the joint commit and 6 | one grant, one r_new; joint resumed to uniform T with r_old removed from nodes; Done once; replayed Begin/Grant/Done after Done answer the Done record |
| 7 | stale-view catch-up | a receiver with only C0 admits r_new's AppendEntries and snapshot |
| 8 | outbound attribution | a reset stream fails `call`; a delayed response is attributed to its own target |
| 9 | replay determinism | an allocated R's entry replayed after retirement, from a snapshot and from a full log, maps to the same slot; an unknown allocated R poisons apply |
| 10 | formation and bootstrap (R1 traces) | (a) concurrent Join grants for N: exactly one birth wins; (b) formation intent is unlinked before `Raft::new` (fault injection at each step); (c) lost response or crash before the identity write: same R for the same birth; (d) page without holder, and log without identity: fail closed |
| 11 | class-E seal | writes stay admitted while r_new is a learner; refused only during the joint entry; admitted after Done; readiness promotion unchanged by a replacement |
| 12 | id range and exhaustion | an id ≥ 2^31 is refused in PD and accepted in Today (and not read as allocated there); `Next(u32::MAX)` issues once, then `Exhausted` refuses |
| 14 | grant intent | the same birth with a different slot, kind or token is refused; a grant whose slot differs from the authenticated slot is refused |
| 13 | ControlOnly containment | after Done and after a restart, the replacement has election disabled from construction, no data or control handler, and none of the node.rs 2474-2950 duties running (observed through the absence of those handles, not log text) |

**Eventual daemon verification (not launched).** One real three-daemon replacement test, run once.
- Survivor data correctness:
  - kill N and delete its Raft store (inside the run directory);
  - begin the replacement, start a fresh ControlOnly process with the token, observe Done;
  - acknowledged rows read back exactly from the survivors, and new writes commit.
- Exclusion with the old instance actually reachable:
  - stop the replacement process (its store stays intact);
  - start the OLD process on its preserved old disk at N's address;
  - observe its refusals of `target_instance = r_new` (the survivors' own counters or RPC results, not a failure to bind a port), survivors still committing, and r_new's progress unchanged;
  - then stop the old process, restart r_new, and see it catch up.
- Replacement data readiness is not claimed or tested.
- Cost reference: the existing real three-daemon smoke measured about 3 minutes per execution, plus a dev rebuild on bigfoot. One host first.

## Consequences
- About 30 `slot_of` crossings.
- A PD id cap of 2^31 − 1. Old PD stores refuse to open.
- A one-commit-round write refusal during the joint entry.
- Slot N's data stays unavailable until gates 2–4.
- A returning removed instance can bump terms.
- A Reserved replacement or a Join grant whose store is lost before completion is stuck in gate 1, with no cancel.
- The replacement's HLC identity stays slot N; it authors no data commits in gate 1, and uniqueness against a lingering old process is a gate-2 obligation.
- A ControlOnly member counts in quorum but never leads. With 3 nodes, at least one Full member must be up for an election.

## Not decided here
- Gates 2–4 (data incarnation, signer, incarnation-specific readiness, reclaim, and the activation that ends ControlOnly).
- Cancel or abandon.
- Address change.
- DC-243 step 2.
