# ADR 0116 - A ParentDecisions drop stays logically finalized until a settlement authority exists

## Status
Accepted, 2026-11-20 (Codex 01a1f762), with the legacy-transition boundary (D1b).

**Its place:** the first, independently landable unit of stage (c) of wendelon ADR 0114. It cites ADR 0114 (the logical drop) and ADR 0115 (the excluding generation).

**Rulings:** DROP-13; Codex 01a1f75c (positive receipts and settlement held; this refusal-only unit and its controls first) and 01a1f762 (GO, with the legacy transitions refused too).

**Both production DROP refusals stay in force.** This ADR enables nothing. It removes the paths by which a test-admitted ParentDecisions drop can lose its fence, its descriptor or its data **without** evidence, and it keeps the one unattended step a drop needs: the roll-forward from Prepare to LogicalFinalize.

## Context
**Reachability today (`95706c34c`):**
- **In production, none of this is reachable.** A ParentDecisions `TableDropPrepare` is refused at apply (`raft_storage.rs:7399-7414`; `pd_drop_admitted()` is constant-false outside test builds). So no PD fence, outcome, tombstone or reclaim debt can exist, and SQL DROP stays disabled.
- **Under ADR 0114's test admission, a prepared drop runs as follows.** The observations are from the three-node in-process fixture.
  1. **The roll-forward:** the ADR 0095 sweeper (spawned in every mode, `node.rs:3236`, every 200 ms) proposes `TableDropLogicalFinalize` with no attendance (`roll_forward_one_prepared_drop`, `network/data_server.rs:3323`). LogicalFinalize stamps reclaim debt with a ring-derived node-id participant set.
  2. **Legacy reclaim:** on its next pass the sweeper takes a **legacy reclaim slice** for each participant.
     - **The leader's own slice runs locally** on its ParentDecisions store (`run_drop_reclaim_slice`: LocalL1, then TieredL2, then VerifyIndexes) and records durable progress. One observed slice examined one item. That alone does not establish a row deletion, but the path is the ADR 0095 one and takes no account of the certified base.
     - **A remote participant's slice is refused at dispatch:** `DROP_RECLAIM_SLICE_REQ` is not in the PD `opens()` set (`child_writes.rs:1401`). In this fixture the pass then fails with `early eof` and is retried every 200 ms. A stamp naming only the leader would have no remote dispatch.
  3. **The legacy transitions:** after a PD Prepare, the older `TableReclaimCompletion` and `TableDropFinalize` dispatch with no PD guard (`raft_storage.rs:7419-7426`). The first records a completion index on the fence. The second validates it, **removes the fence** and records a permanent outcome **with no tombstone and no stamped reclaim debt**; after that the roll-forward can no longer reach LogicalFinalize. A direct-apply control shows the completion is accepted (`Success`).
  4. **Settlement:** in this fixture the sweeper never proposes settlement. **A directly proposed `TableDropSettlement` is accepted at apply** (no PD guard, `raft_storage.rs:7431`): it retires T's descriptors and removes the tombstone and reclaim debt with no activation or cleanup evidence. That is OBS-0115-1's precondition, and it **replays** the same way.

**What settlement needs, and why it is not yet available:**
- **The ruling:** an empty active base plus zero certified debt does not prove that T's rows are gone (ADR 0114 D5/D6; DROP-10: a row absent from the index became locatable after reopen).
- **A positive authority must account for:**
  - authoritative row attribution, concurrent and recoverable publications, every lookup surface, and exact pointer correspondence, including the replicated digest;
  - the immutable LogicalFinalize participant stamp (including migration sources), departed participants, replacement and joint membership;
  - secondary-index and tier work.
- **Until that authority exists, the fence-to-outcome path, the descriptor, the tombstone and the stamped reclaim debt must stay as LogicalFinalize leaves them.**

## Decision

### D1. Settlement under ParentDecisions is refused at replicated apply
`TableDropSettlement` is refused at the dispatcher when the state is ParentDecisions (`decisions.is_some()`), with no effect on any catalog.
- **This covers direct proposal, the sweeper and replay alike,** because all three reach the same apply.
- **The response is an error the proposer sees.** On replay it is an ordinary refused entry: the open succeeds, the applied index advances, and nothing changes.
- **Today's settlement (ADR 0095) is unchanged.**

A later ADR replaces this refusal with a typed settlement authority, built only by the coherent check listed in the Context above. Until then, **no PD state can be settled.**

### D1b. The legacy reclaim completion and finalize are refused under ParentDecisions
`TableReclaimCompletion` and `TableDropFinalize` are refused at replicated apply when the state is ParentDecisions, with no effect.
- The fence stays, and no outcome is recorded without its tombstone and stamped debt.
- The normal LogicalFinalize then still stamps them.
- Today's transitions and the legacy encodings are unchanged.

**Each refusal names itself:** there is one product constant per refusal (`PD_DROP_SETTLEMENT_REFUSED`, `PD_LEGACY_DROP_TRANSITION_REFUSED`, `PD_LEGACY_DROP_RECLAIM_REFUSED`). A control therefore asserts the intended refusal, not merely a non-success.

### D2. Legacy physical reclaim is refused under ParentDecisions at its one entry point
`run_drop_reclaim_slice` returns `Unsupported` under ParentDecisions **before** loading, starting or persisting any progress, and before touching any slab.
- It is the single function behind the leader's local call and the remote frame handler.
- The remote frame also stays refused at dispatch.

### D3. The roll-forward is kept; the sweeper's pass ends there under ParentDecisions
`reconcile_drop_reclaim_once` still rolls one prepared drop forward to LogicalFinalize in every mode. Under ParentDecisions it then returns with nothing further to do: no slice and no settlement proposal.
- **A ParentDecisions drop therefore reaches LogicalFinalize unattended,** and stays logically finalized (tombstone, stamped debt and descriptor kept) until the later settlement authority exists.
- **Under Today the pass is unchanged.**

### D4. Format and obligations
- **No format change:** no new replicated state and no index change. The refusals change apply only for ParentDecisions states that production cannot create.
- **Kept:** both production DROP admission gates, the table-id reuse guard at its existing gate, and ADR 0114's backup/restore obligations.
- **Recorded for the later authority (DROP-13's direction):**
  - an exclusion watermark scoped to the exact outstanding drop, never recreated from a permanent outcome after settlement;
  - activation evidence that is not sufficient to settle on its own;
  - generation reports keep their staged-inventory meaning.

## Rationale
**The narrowest unit that makes the unsafe states unreachable.**
- A guard at apply, rather than at the proposer, also covers direct proposals and replay.
- A guard at the slice entry point covers both callers.
- The roll-forward stays so that a prepared drop is not stranded.

## Consequences (The "Negatives")
- **A test-admitted ParentDecisions drop can never complete.** It stays logically finalized, holding its descriptor and stamped debt, until the settlement authority lands.
- **The ADR 0095 sweeper becomes mode-aware;** its Today path is untouched.
- **ADR 0115's restage control can no longer settle for real.** It now simulates settlement, as 0115's acceptance (5) words it:
  - a `cfg(test)` helper applies the settlement apply's own removals in place on every member, under one write guard;
  - the reporter makes them durable with a snapshot before its reopen;
  - the third member is held at its certification step instead of stopped.

  The architecture census of state-machine writers goes from 12 to 13 for that helper.

## Acceptance (failing controls first, on `95706c34c`)
1. **The premature-settlement control:** `decisions::tests::a_parent_decisions_settlement_is_refused_with_no_effect`. A settlement carrying exactly the stamped participants, finalize index and epoch receives the intended refusal. The tombstone, reclaim debt, descriptor and outcome are unchanged. **RED on `95706c34c`:** `Success`.
2. **Through replay:** `decision_mode_tests::a_committed_parent_decisions_settlement_replays_as_a_refusal`. A committed settlement in the recovered suffix replays as a refusal, the applied index passes it, and the tombstone, debt and descriptor are kept. **RED:** the tombstone is removed.
3. **The legacy-slice discriminator:** `drop_exclusion::a_legacy_reclaim_slice_is_refused_under_parent_decisions`. Once every member has applied the finalize index, `run_drop_reclaim_slice` returns the intended `Unsupported` refusal on every node, and no node records legacy progress. **RED:** the leader's slice returns `Ok(Progress, examined 1)`.
4. **Logical-finalize progress:** `drop_exclusion::a_prepared_drop_reaches_logical_finalize_unattended_without_legacy_reclaim`. Its scope: one leader, no leadership change induced.
   - After a Prepare alone, the drop is logically finalized unattended (GREEN on `95706c34c`: the roll-forward exists).
   - Once every member has applied it, a reconcile pass returns `Ok(false)`, no node records legacy progress, and the tombstone, debt and descriptor are kept. **RED:** the pass fails at a remote legacy slice.
5. **The legacy transitions:** `decisions::tests::legacy_reclaim_completion_and_finalize_are_refused_after_a_parent_decisions_prepare`. After an admitted Prepare, `TableReclaimCompletion` and `TableDropFinalize` receive the intended refusal at the real dispatcher, and the fence and catalogs are unchanged. LogicalFinalize then stamps the tombstone and debt. **RED without D1b:** the completion gives `Success`.
6. **ADR 0115's restage control, reworked:** it is kept, using the simulated settlement above. It still asserts the restage on reopen, the held member's first build after settlement, and the fresh id in generation 3.
7. **Today is unchanged:** the existing ADR 0095 drop reclaim and settlement tests pass as they do on `95706c34c`.
8. **Production neutrality:** the goldens and corpus are unchanged.
