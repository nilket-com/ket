# ADR 0121 - A ParentDecisions cluster retention window hides rows below one certified floor

## Status
Proposed (local), 2026-11-21.
- **Reconciliation:** PD-RETENTION-RECON (Codex 01a20043).
- **Design:** ADR0121-S1-DESIGN (Codex 01a2004d).
- **This plan:** reviewed by Codex 01a20051, which cleared the local signed plan with its amendments, and the test-first S1 implementation (binding, target and certificate, idle generation, read visibility). Source and delivery review precede any push.
- **Source review:** Codex 01a20095 held the first candidate (`77bf9d1b9`) for R1 (unbound is not Disabled at CTAS Begin and restore), R2 (a reopened member re-proves its staged minimum) and R3 (decode validates the retention invariants); folded in here and in the impl.- **Delivery:** this ADR is S1, logical visibility only. Physical reclamation (S2, age as certified cleanup; S3, the certified L2 rewrite) is a separately reviewed unit, as are the Section 8 harness arm and any measurement.

## Context
**The frozen Section 8 workload** (`toron-tpch` `s7_measure`, `scripts/s7-measure/manifest.json`):
- a 15 s cluster window (`cluster_retention_window_secs: 15`);
- one AppendOnly table that restates `InheritCluster` at policy generation ZERO (`drive.rs:754–763`);
- a gate that is INVALID unless L2 retention prunes rows in the measured interval (`evaluate.rs:545–550`).

**On `73d56bc29` ParentDecisions has no retention at all:**
- **Startup:** the node policy is refused in mode (`node.rs:785–799`). The window is node-local, from each node's own `now_ms` (`storage/retention.rs:87`).
- **The table setting:** every `SetTableRetentionPolicy` is refused at apply (`raft_storage.rs:7710–7714`), including the restatement.
- **Read and build:** the reader refuses a floor (`reader.rs:1194`) and the build refuses to certify one (`certification.rs:788–806`).
- **The trigger:** the generation trigger needs something to erase and has no age input (`generations.rs:339–377`).

**Base membership is already a cleanup authority.** Activation writes `DisplacedBase` debt for any previous base entry missing from the new base (`storage/index/certified_base.rs:1217–1240`). It writes `RetiredIntent` debt for an owned version with no new winner (`:1270–1295`). Leaving an aged winner out of a base would therefore authorize today's cleanup at once. That is physical reclamation, and it is not this ADR's.

**The baseline, test-only on `73d56bc29`** (bigfoot `adr0121-baseline-b.log`, `076ea0317d148db8`):
- `an_idle_base_older_than_the_window_gets_a_next_target` fails at `decisions_tests.rs:2483`: `next_target` is `None` after the committed clock passed T0 + 15 s.
  - It uses synthetic report chunks and a stamped `AdvanceFloor`. It shows only the absent age trigger, not that a stored row aged or that an idle clock advanced a certificate.
- `an_inherit_cluster_restatement_keeps_the_ordinary_contract_under_parent_decisions` fails at `decision_mode_tests.rs:1306` with `Error("table retention is frozen under ParentDecisions")`; its Today half held.
- The existing fences (`table_retention_is_frozen_under_parent_decisions`, `the_parent_decisions_mode_refuses_a_node_retention_window`) pass.
- The reader floor is unreachable test-only: both floor sources are fenced.

## Decision
**Names.**
- **K** and **K_min** choose which decisions a statement sees; **t** and **fr** are generation cuts. These keep their meaning.
- **The decision floor** (`AdvanceFloor`) retires retry keys. It is unrelated and unchanged.
- **The retention floor F_g** is a row-HLC bound carried by generation g's signed target. It decides only which **served** row versions of a governed table are visible under g.

**Governed rows.**
- Rows of an AppendOnly table whose policy is `InheritCluster` (every table create establishes it) in a store whose bound window is above 0.
- AppendOnlyContentHash stays outside ParentDecisions admission (ADR 0119) and is not enabled here.
- PK tables are never governed. A key's winner, a winning tombstone included, is served whatever its age.

### D1. One replicated, immutable window
- **Configuration:**
  - a mode-only setting: `cluster.parent_decisions_retention_window_secs` in the daemon file, `NodeBootstrapConfig::parent_decisions_retention_window` in code (absent or 0 means none). A Today node refuses it.
  - `mvcc_retention_policy` stays `Disabled`-only in mode (`node.rs:785–799`, unchanged), so the ordinary sweeper, LSM and Today-reader floors never see a window.
- **The binding** is `DecisionState.retention: Option<RetentionBinding { window_ms }>`, set only by `DecisionCommand::BindRetention { window_ms }`:
  - unbound: it binds;
  - bound to the same value: a no-op success (replay, or a duplicate from another leader);
  - otherwise: refused `RetentionRebind`, with nothing changed.
  - **There is no unbind and no rebind.**
- **Bootstrap ordering.** Raft participation, apply and the decision maintenance loop never wait on the binding. Only serving does: child writes, reads, delete evaluation, report signing and the backup roster.
  1. Formation completes as today.
  2. The leader's maintenance proposes `BindRetention` from its own configuration while no binding exists (on its first pass as leader, which can precede the end of formation; the binding is immutable either way).
  3. `reserve` refuses `FormationIncomplete` while unbound. This is the existing ADR 0720 seal point (`decisions.rs:2965–2978`), so the first admitted write always follows one bound window. A window of 0 is bound like any other value; it is the Disabled default.
- **The gate.** A member serves, and signs a generation report, only if its configured window equals the binding. Absent a binding, nothing is served and no `reserve` is admitted.
  - **Already-open followers:** the bind reaches them as an ordinary applied entry. The gate reads applied state at each serving decision, so a follower that disagrees closes at the bind's apply, before any write can be admitted.
  - **A mismatched incarnation:**
    - refuses child writes, reads and delete evaluation with a typed final refusal;
    - signs no report. The report path checks the pending target's window against its configuration before staging, so a mismatched member cannot sign for a generation and a generation cannot commit without it;
    - shows the readiness reason `RetentionWindowMismatch`, next to ADR 0700's reasons.

    It never rebinds and never writes. **Every all-member generation stalls** until the member is fixed. **Write availability follows the actual placement and quorum:** a write needing the mismatched member's children is refused, and others may still be admitted. The result is loud, never divergent.
  - **Reopen, snapshot install and replacement** (ADR 0107/0108) all reach the same gate: the binding is in `DecisionState`, so it is replayed, carried in the ParentDecisions snapshot and read by a new incarnation before it serves.
- **Format fences.**
  - The decision state, `Target`, `Certificate`, `ReportBody` and the staged candidate change encoding. Pre-release, they are bumped in place: the ParentDecisions WAL and snapshot format versions, the raft and decision envelope versions (`decision_mode.rs` `check_raft_frame` and `check_peer_exchange_version`), and the store marker v23 to v24.
  - An older ParentDecisions store is refused whole before mutation, and so is a peer on either side, as at every earlier bump.
  - **Decode validates the retention invariants** of the committed and pending generations (Codex 01a20095 R3): with no binding there is no generation (legal genesis); with one, every cut carries the bound window, a zero window carries no floor, no stamp or floor lies beyond the applied clock (`floor <= clock - window`), a pending floor is not below the certificate's, and a certified least visible row is at or above its floor. A snapshot (an install included) contradicting any of these is refused.
  - **The supported prior-version route is logical export and restore:** an export taken by the previous binary restores onto a fresh store of this binary, which binds its own configured window before the first restored write. The existing cross-binary control (`an_old_binarys_export_restores_verified_on_the_new_binary`) covers it with window 0.

### D2. The floor is frozen into the signed target
- **The target.** `Target` gains `retention: RetentionCut { window_ms, stamp_ms, floor_ms }`. `target_digest`'s domain becomes "wendelon generation target v2", so every member signs the cut.
- **At apply** (`propose_target`, after today's checks, which keep cut closure, gid, predecessor, members and survivor policy):
  - `window_ms` equals the binding;
  - `clock' = max(clock_ms, stamp_ms)`, as at `ctas.rs:991`;
  - `cert.floor_ms <= floor_ms <= clock' - window_ms` (saturating), with `floor_ms == 0` when the window is 0;
  - an age-only target (D3) needs `floor_ms > cert.floor_ms` and `floor_ms > cert.oldest_visible.physical_ms`.
- **An ordinary target needs no fresh clock sample.** A target with something to erase may carry `stamp_ms = 0` and `floor_ms = cert.floor_ms` (0 with window 0). Ordinary generation and commit progress therefore never waits on the decision clock, and a disabled window progresses exactly as today.
- **A target that erases something also raises the floor** when the leader has a sample and `now_ms - window_ms >= cert.floor_ms + RETENTION_STEP_MS`: then it carries `stamp_ms = now_ms` and `floor_ms = now_ms - window_ms`. It commits anyway, so visibility follows the window at no extra generation. (Found in implementation: without it, a row that entered the base after an age-only generation stayed visible until the next age-only target, because that certificate agreed no visible row.)

  The clock is raised **only on success**, so a refused proposal leaves the clock and the state unchanged. The legal time is therefore the committed decision clock at the target's own successful apply, which comes after every entry at or below t.
- **Frozen afterwards.** `Pending` stores the target. `commit_generation` copies `{window_ms, floor_ms, oldest_visible}` into the `Certificate`. Nothing recomputes them:
  - a later tick moves only `clock_ms`;
  - replay is identical;
  - a leader change does not replace a current pending target (`generations.rs:341–349`);
  - a stale target is replaced at a fresh gid and stamp, still bounded below by the certificate;
  - a snapshot carries both.
- **The HLC form:** `F_g = HybridTimestamp { physical_ms: floor_ms, logical: 0, node_id: 0 }`. A version is hidden iff its full HLC is **strictly below** F_g. A version at exactly F_g stays visible.
- **The statement's floor** is the floor of the certificate it validated: the current certificate read in the same `read_state` as `ReadAuthority::check`'s K_min test (`reader.rs:330–363`). It is the certificate with `committed_at <= K`.
  - **A newer certificate commits after K:** then K < K_min, and the next check, per batch or before the success terminal, refuses the statement ("cut expired: below K_min", `reader.rs:361–363`). So one answer never mixes two floors.
  - **Delete evaluation** (`proved_point`, `reader.rs:1343`) takes its floor the same way, at its cut.

### D3. The age trigger, from durable staged evidence
- **The minimum.** The staged candidate (`StagedCandidate`, `certification.rs:620–629`) gains `oldest_visible: Option<HybridTimestamp>`: the minimum HLC over the candidate's governed, live (non-tombstone) base entries whose HLC is at or above the target's floor. PK entries never count. It is computed while staging and stored with the staged chunks, so a member that reopens before reporting reports the same value from durable evidence. **A reopened member rebuilds its recovered candidate under the frozen target and must reproduce the staged minimum before it restages or signs** (R2); otherwise it signs nothing and its staged evidence is left as it is.
- **Agreement.** `ReportBody` carries it. The survivors and their preimages (HLC included, `certified_base.rs:254–265`) must agree for a commit, so the value agrees too. A report that differs only in it is refused `ReportMismatch`, like a chunk.
- **The trigger.** `next_target(state, applied, now_ms)` keeps today's closed-cut search and blocking rule unchanged. With nothing else to erase, it proposes iff all of these hold:
  - the bound window is above 0;
  - `cert.oldest_visible` is `Some(o)`;
  - `o.physical_ms < now_ms - window_ms`;
  - and `now_ms - window_ms >= cert.floor_ms + RETENTION_STEP_MS`, where `RETENTION_STEP_MS = max(1 s, window / 4)`.

  `now_ms` is the leader's gated decision clock (`decision_proposals.rs:1699`), and it becomes `stamp_ms`. The proposed floor is `now_ms - window_ms`, which is above `o`. So an age-only generation strictly advances visibility over at least one real row, and the next one needs a new oldest visible row aged past the window, plus the step.
- **Cases:**
  - **No governed rows, or a PK-only store:** `oldest_visible` is `None`, so there is no age-only target.
  - **A blocked cut** (an outstanding reservation below applied) or **a current pending generation:** nothing is proposed until today's rules allow a target.
  - **An unavailable member or a delayed activation:** the generation stalls as today.

  The step limits how often age-only targets are proposed. **The floor has no wall-time bound:** a governed row may stay visible arbitrarily longer than window + step, but never less than the window.
- **An idle cluster.** The age-only target's own gated stamp advances the committed clock, so no write is needed.

### D4. Visibility applies to served rows only; base membership is unchanged
**The certified inventory is exactly today's** (`SURVIVOR_POLICY` and `SURVIVOR_POLICY_EXCLUDING` unchanged):
- an aged winner stays in the base;
- a covered intent's aged winner becomes a base entry;
- ordinary supersession, DROP exclusion and CTAS exclusion are unchanged, for intent children and inherited rows alike;
- age writes no `DisplacedBase` or `RetiredIntent` debt, and nothing is reclaimed by age.

**Served paths apply F_g of the validated certificate to governed rows:**
- **The table query:** `reader.rs:1194`'s refusal is replaced. The executor's logical filter receives F_g for a governed table and `None` otherwise. It applies to base entries and to post-cut Accepted rows alike.
- **Delete evaluation:** `proved_point` treats a governed winner strictly below F_g as not found (`DeleteOutcome::NotFound`), exactly as the read does.
- **The backup roster:** refuses `RetentionActive` (`logical_backup/roster.rs:64`, `:209`) for a governed table while the bound window is above 0. An export cannot carry hidden rows to a restore, where a fresh store would serve them. A window-0 store exports as today. **An immutable positive-window store holding governed tables has no supported logical export route in S1.**
- **Restore onto a fresh target** (`logical_backup/restore.rs:288–325`):
  - Today the empty target is rostered, then `establish` runs and the chunks are imported, and only then is the target rostered again. A per-table refusal would pass the first roster on an empty positive-window target and fail the second after the import.
  - So restore reads the target's bound window (a linearizable `GetRetentionBinding` admin request with its own answer extension; the backup roster and its pinned extension are unchanged, so artifacts of the previous binary still restore) and checks the incoming roster against it **before `establish` or any import**. Only an accepted answer naming the explicit Disabled binding (`Some(0)`) lets an incoming governed table proceed; an unbound target, or an answer without a binding (a bare reply decodes to `None`), is refused `RestoreRefusal::RetentionBindingUnknown` (R1). An incoming governed AppendOnly table onto a positive-window target is refused, typed (`RestoreRefusal::RetentionWindowBound`), with no tenant, descriptor or row imported.
  - A restore onto a window-0 target is unchanged and still verifies. The guard is scoped to that incompatible combination only.
- **A fresh CTAS Begin needs the explicit Disabled binding** (R1: unbound is not Disabled; before the bind it is refused `FormationIncomplete`, like a Reserve, so no chain can begin and survive a later positive bind; a replay of an existing chain is answered as before). **On a positive-window store it is refused at Begin** (`CtasRefusal::RetentionWindowBound`, `ctas.rs` `apply_begin`) before any durable effect. CTAS on a window-0 store is unchanged. Why it is refused:
  - ADR 0120 promises the exact original rows or a terminal, unpublished outcome.
  - Chunk rows carry their own write HLCs, which precede Publish (`apply_publish`, `ctas.rs:735`, with `visible_from` the Publish index). One operation may span `CTAS_OPERATION_WINDOW_MS` (15 min, `ctas.rs:32`).
  - With a 15 s window, a certified floor can fall between the oldest and the newest chunk HLC, so the first public read after Publish would see only part of the result.
  - Base membership does not settle this. The refusal is the smallest contract-preserving choice. Enabling the interaction (for example, aging a published table's rows from its publication) needs its own reviewed decision. The meaning of a successful CTAS is not redefined.
- **Unchanged:**
  - historical reads stay refused (`reader.rs:1040–1047`); `RETENTION_EXPIRED` is not served in mode;
  - KV, CDC and every other mode refusal are untouched.

**Retention is measured from the row's HLC**, not from its acknowledgement. A row is hidden only once a certified floor passes its HLC. A late write can therefore be hidden sooner after its acknowledgement than the window.

**A PK tombstone winner** is kept as the winner and still means deleted to clients.

**A late write** whose HLC is strictly below the floor, Accepted after t:
- it keeps its ordinary Accepted receipt and its retry and resolve identity (resolve answers from the decision chain, not from visibility);
- it is not served, and delete evaluation finds it not found.

There is no new write status.

**The build** judges a governed table against the target's cut, never the node policy and `now_ms` (`certification.rs:788–806`). `BuildRefusal::Policy` remains for any other floor source, which stays unreachable.

### D5. `SetTableRetentionPolicy` keeps the ordinary contract
- In mode, `InheritCluster` takes the ordinary apply (`raft_storage.rs:9123–9171`), with every check:
  - validation; the tenant exists and is not deleted; the table exists; no drop in progress; the PK rule;
  - `set_next_generation` (`schema/lifecycle.rs:339–363`), so the first success at ZERO advances the generation to 1 with the effective policy unchanged, and a stale repeat is refused and changes nothing.
- `Disabled` and `FixedWindow` stay frozen (the arm at `raft_storage.rs:7711` is narrowed to them).

## Controls
**Reducer:**
- `BindRetention`: bind, replay, conflicting rebind;
- `reserve` refused while unbound, and the bind accepted while serving is closed;
- `propose_target`:
  - a floor above `clock' - window`, below the certificate's, or with a different window is refused, and a refused proposal leaves `clock_ms` unchanged;
  - replay is identical;
  - a stale-target replacement stays monotone;
  - window 0 is semantically equivalent to today (the wire format and the digest change on purpose). Ordinary generations progress and commit with no fresh clock sample;
- age-only targets:
  - must strictly advance past `oldest_visible`;
  - are not proposed when `oldest_visible` is `None`, the store is PK-only, the cut is blocked, a generation is pending, or the step is not reached;
- reports differing only in `oldest_visible` give `ReportMismatch`;
- the two baselines, inverted.

**Gate:**
- a member with a mismatched window, opened before the bind:
  - closes at the bind's apply;
  - refuses a read, a child write and delete evaluation;
  - signs no report, so the generation does not commit;
- the same after reopen, after snapshot install, and for a replacement incarnation.

**The discriminating cluster control** (three voters on the ADR 0110 controlled clock, window 15 s bound):
1. Certify rows a (old) and b into generation g1, so they are inherited base winners.
2. Accept row e after g1's t with an HLC below the coming floor; it is a covered intent winner. Row c is at the floor's exact millisecond and logical 0, and row d is above it.
3. Commit floor generation g2, then run one cleanup pass.

Asserted at every member:
- reads return exactly {b, c, d} under g2: a and e are hidden, c is visible at equality;
- **a and e are still base entries in each member's durable inventory, and no cleanup debt exists for them** after g2's activation and after the cleanup pass;
- every other row and key is unchanged, including a PK key p1 written before g1 and never updated, and a PK key p2 whose winner is a tombstone;
- deleting a or e by identity answers not found;
- the backup roster is refused `RetentionActive`;
- a non-zero `as_of_hlc` is refused as today;
- the L2 pruned-row counter delta is 0.

**A late write after the active cut:** a governed row Accepted strictly after the active target's t, with an HLC below the active floor, is:
- read **before** a generation covers it: hidden;
- read through the same identity by resolve and retry: the ordinary Accepted outcome, never a second write;
- unchanged once the next generation covers it.

**Restore:**
- an AppendOnly backup artifact onto a fresh positive-window target is refused `RetentionWindowBound`, with no tenant, descriptor or row imported;
- the same artifact onto a fresh window-0 target restores verified.

**CTAS:** on a positive-window store, Begin is refused `RetentionWindowBound` and nothing is durable. On a window-0 store, ADR 0120's controls pass unchanged.

**The idle trigger, separately:** one governed row r, then no writes; only the controlled clock advances. An age-only generation commits with `floor_ms > r.hlc.physical_ms`. Its certificate's `oldest_visible` is `None`, so a further idle step proposes nothing. r is hidden and is still a base entry with no debt. A reopen of one member before its report reports the same `oldest_visible`.

## Rationale
- **Physical membership is cleanup authority today.** Keeping the inventory unchanged is the only way to make S1 purely logical. Changing membership by age belongs to the reclamation unit, under its own review.
- **A replicated, immutable binding** is the only agreement that survives formation, reopen, snapshot install and replacement. Independent local bindings prove nothing about first formation.
- **Fixing the floor in the signed target** at its apply's committed clock makes it identical on every member and immune to later ticks, replays and leader changes.

## Consequences (The "Negatives")
- **The Section 8 physical prune gate is not met by this ADR.** Hidden rows stay stored and contained until S2/S3. Storage grows with the hidden backlog.
- **A misconfigured member makes the cluster unavailable for writes and generations** until its configuration is fixed: loud by design.
- **Visibility can lag the window without bound** while a cut is blocked, a member is down or activation is delayed. Retention is a minimum keep, never a deadline.
- **Export is refused** for governed tables while a window is bound, so such a store has no supported logical export route in S1. Restoring a governed AppendOnly artifact onto a positive-window target is refused.
- **CTAS is refused** on a positive-window store until its first-visibility interaction has its own reviewed decision.
- **Format bump:** older ParentDecisions stores and peers are refused.
- **A finding, not changed here:** the Today executor filters every version below a passed floor, PK rows included (`data_server.rs:12656`, `13419`). In mode no floor is passed for PK tables.

## Open Questions (Optional)
- **S2/S3:** age as certified cleanup debt, and certified L2 rewrite and retirement, to reach the frozen physical activity gate.
- **The Section 8 harness:** a ParentDecisions arm that maps `cluster_retention_window_secs` onto `parent_decisions.retention_window` and keeps every other manifest value.
