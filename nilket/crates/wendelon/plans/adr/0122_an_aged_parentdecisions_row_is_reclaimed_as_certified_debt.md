# ADR 0122 - An aged ParentDecisions row is reclaimed as certified debt

## Status
Proposed (local), 2026-11-22.
- **This plan:** cleared by Codex 01a200d7 for a local signed commit and the test-first implementation, with four clarifications folded in (D1, D3, D4): the two restart paths, producer semantics on a shared key, exact op-tied counting, and the idle tombstone bound. Source and delivery review precede any push; no capacity run.
- **Corrected per Codex 01a20151 (source review HOLD):** the L2 count is made once by the originating worker from proof of its own committed effect, with no registration to outlive a job (R1); the controls prove physical removal from the committed objects, add a distinct certified-LSM count and zero credit for uncommitted and recovery-only effects (R2); and each control judges its own work by its store's tally while the exported series must be present and well formed (R3).
- **Design:** ADR0122-DESIGN, reviewed by Codex 01a200d2, which asked for this plan with five corrections:
  1. an ordered omission rule;
  2. an explicit signed policy with fences and an upgrade matrix;
  3. exact age attribution;
  4. committed-removal counting;
  5. a corrected baseline.
- **Base:** origin/main `a9d411d4f` (wendelon ADR 0121 S1).

## Context
- **ADR 0121 S1** hides governed AppendOnly rows strictly below one certified floor and keeps them in the certified base, so nothing is reclaimed. Base membership is cleanup authority: activation records `DisplacedBase` debt for omitted or replaced base entries and `RetiredIntent` debt for losing owned versions (`storage/index/certified_base.rs:1186–1300`).
- **Three consumers already reclaim exactly debt** that passes one survivor rule (debt under one table, no owner, no base entry at its HLC, content evidence of the debt's kind and digest):

  | Consumer | Where |
  |---|---|
  | ADR 0699 S5b cleanup of sealed local slabs | `network/data_server/cleanup.rs` |
  | ADR 0703 certified LSM elision | `storage/manager/certified_lsm.rs` |
  | ADR 0704 certified tier cleanup of the leader's own tiered slabs | `storage/manager/certified_tier.rs`; certified `Tier`/`Retire`, from the leader-gated LSM tick at `node.rs:4851–4860` |

- **No certified path reports a retention counter.** The frozen Section 8 gate needs `wendelon_storage_table_retention_l2_rows_pruned_total{kind}` to rise (`toron-tpch/src/bin/s7_measure/evaluate.rs:545–550`).
- **The baseline,** test-only on `a9d411d4f`: `retention_window::an_aged_row_leaves_the_certified_base_and_is_reclaimed`.
  - One governed row r is certified in g1; then only the clock moves, and an age-only g2 commits with a floor above r.
  - It asserts, at every member: r is not a base entry; it is owed as debt; then each member seals its active slab, runs `certified_cleanup_step` until no local copy of r remains, and **the step's own result** must show a reclaim; finally the table reads empty.
  - **Observed:** FAIL at the first assertion (`retention_window.rs:935`, "the aged row left the certified base at 41"). The later assertions are not yet reached on main.

## Decision
### D1. The omission, after every ordinary rule
A candidate for a target whose survivor policy has the aged component (D2) is built as today:
1. inherited entries, minus ADR 0115's excluded tables;
2. children of Accepted parents, minus CTAS-excluded objects;
3. the newest version per key, the equal-full-HLC conflict refusal unchanged;
4. **only then**, every **governed** winner whose full HLC is **strictly below** `F = target.retention.floor_hlc()` is omitted: inherited or from a child, data or tombstone.

- **Governed** is ADR 0121's `retention_governed`: a bound window above 0, an AppendOnly descriptor, and policy `InheritCluster` or none.
- **Floor equality:** a winner with HLC at or above F is kept. F has `logical = 0, node_id = 0`, so a real row at F's millisecond is at or above it.
- **Disabled** (window 0, so no floor): no aged component and nothing omitted.
- **Pairing:**
  - an AppendOnly data row whose deleting tombstone is at or above F is not a winner (the tombstone is) and is unchanged;
  - a tombstone below F is omitted, and its deleted row is below F too.
- **A delayed child below F,** Accepted after its key's aged tombstone was omitted, stays hidden: S1's read filter hides HLC below F at every read, and its covering generation omits it.
- **A delayed child at or above F** is a newer version: it wins whether or not the old tombstone is in the base, so nothing resurrects.
- The chunk sums cover the survivors only.
- `oldest_visible` keeps its meaning: the least governed live HLC at or above F, which is now every governed base entry.
- **The idle tombstone bound** (S1's trigger is preserved in this unit):
  - `oldest_visible` excludes tombstones (`certification.rs:728`), and an age-only target needs `Some(oldest_visible)`;
  - so when a store's last governed winner is a tombstone at or above F, the clock alone schedules no generation that would omit it later;
  - it is omitted by the next generation with a covering floor that something else triggers (an ordinary, erasing target that raises the floor by a step).
  - Recorded as a limit and controlled.

### D2. An explicit signed policy, and its fences
**The policies:** `SURVIVOR_POLICY_AGED = 3` (policy 1 plus D1) and `SURVIVOR_POLICY_EXCLUDING_AGED = 4` (policy 2 plus D1).
- **The rule:** `next_target` sets the policy to `compose(state.survivor_policy_with(ctas, t), target.retention.floor_ms > 0)`, and `propose_target` admits only exactly that.
- **Where it is checked:** the certification check at `certification.rs:513` and the decoded-state check `validate_assembled_state` (`raft_storage.rs:3478–3492`) compare against the same composition. The build reads exclusion from {2, 4} and age from {3, 4}.
- **Why it is explicit:** the omission is signed into the target digest as a policy, not inferred from the cut. A build that does not know policy 3 or 4 refuses to report a target carrying it, and so does one that disagrees on the composition.

**Fences**, because a member applying `ProposeTarget` under the old rule would refuse what a new member records, so applied state would diverge:
- the ParentDecisions **WAL and snapshot formats 34 → 35**: an older log or snapshot is refused whole before it is applied or installed;
- the **Raft and decision envelope 27 → 28**, peer exchange included: a mixed peer is refused before decode, both ways, joiner checks included;
- the **store marker v24 → v25**: `StagedCandidate` and `DebtRecord` change their durable shape (D3).

**The upgrade matrix:**

| From → to | Answer |
|---|---|
| a v24 (ADR 0121 S1) store opened by this build | refused whole, unchanged |
| a v25 store opened by an S1 build | refused whole, unchanged |
| a mixed S1/S2 cluster | frames refused before decode, both ways; no apply divergence possible |
| a snapshot of either, installed by the other | refused before install |
| a replacement of another build | the same frame and marker fences |
| a window-0 store, by logical export and restore | the supported route (the cross-binary control) |
| a positive-window store holding governed tables | **unsupported in S1 and here**: its roster is refused `RetentionActive` and no preservation route exists; it is recreated |

Pre-release: no migration.

### D3. Exact age attribution, from durable staged evidence
- **The evidence:** the candidate build knows exactly which winners step 4 of D1 omitted: the winners that would survive with age filtering disabled, minus the survivors.
  - They are staged durably, in the same transaction as the staged rows, in a local table `base_aged_staged`, keyed by the debt key `(tenant, table, key, HLC)` with `(row_digest, tombstone)`.
  - Their count and digest are kept in `StagedCandidate.aged: Option<AgedEvidence { count, digest }>`.
- **At activation,** inside its transaction: for every debt record it writes (`DisplacedBase` or `RetiredIntent`), `DebtRecord.aged = true` **iff** the staged evidence holds the same key, digest and kind. The staged table is cleared with the other staged rows. `DebtSource` is unchanged, so the `RetiredIntent` producer lock and evidence kinds are unchanged.
- **No overcount:** an unrelated losing or refused version in the same table below F becomes `RetiredIntent` debt but is not in the evidence, so it is not aged.
- **A shared debt key keeps exact producer semantics.**
  - Activation visits refused intents too, so key equality cannot establish agreement with a manifest that never entered the winner set.
  - A debt record is labelled aged only when the **full** evidence (key, digest, kind) matches.
  - A `RetiredIntent` record keeps its `RetiredIntent` source and producer stripe whatever its label.
  - A refused intent's version whose digest differs from the evidence at the same key is not credited, and the content checks are unchanged.
- **Two restart paths:**
  - **Pending, before signing** (`prepare_report`'s recovered branch): a recovered candidate rebuilds the omission and must reproduce `AgedEvidence` exactly before it restages or signs (ADR 0121's R2 rule extended); otherwise it signs nothing and keeps its staging.
  - **Committed, at activation** (`activate_committed` → `activate_base`, on restart too): in the same transaction that installs the base, the actual `base_aged_staged` rows are verified against the staged count and their canonical digest (full key, digest and kind, in key order) **before** any debt is labelled or any evidence cleared. An inconsistent table refuses the activation and leaves the old base, debt and staging intact.
  - Staging for a new gid replaces a superseded pending gid's evidence atomically, including when the new omission set is empty.
  - A crash after activation leaves labelled debt.
- **A pristine replacement** follows its existing `publish_recovered` path: its installed base already excludes aged rows and it holds no older copies, so it has no aged evidence. Its later generations build normally.
- **Size and lifetime:**
  - one entry per aged winner of one generation, local to each member and never replicated;
  - bounded by that generation's governed inventory;
  - as a worked example, not a memory bound: at 2,500 rows/s, one 3.75 s step's arrivals are about 9,400 entries, more if generations lag;
  - it lives from staging to activation.

### D4. Counting committed removals only
**The series:** only the row counters the gate reads.
- New row-only recorders, `observe_storage_table_retention_l{1,2}_rows_pruned(data, tombstone)`, add to `…_rows_pruned_total{kind}`.
- They **never touch the shared retention-lag gauge**: the existing `observe_storage_table_retention_l{1,2}_prune` set it, and an unmeasured zero would overwrite a real observation.
- **Bytes are not attributed.** A rewritten object mixes causes, and ADR 0704 retains superseded objects, so `…_l2_bytes_reclaimed_total` is not moved by certified reclaim. This measures rows removed from the committed active inventory, not object-store bytes freed.

**L2, the emission point: the originating worker counts once, from proof of its own committed effect (Codex 01a20151 R1).**
- **When:** at the end of the worker's own successful delivery of the op it allocated, after the local install (or retirement) returned.
- **Proof, from one coherent read of the canonical lease:** that op is decided Applied under its digest for its current owner, and its own effect is still in force:
  - for `Tier`, the slab's committed pointer is exactly the job's fresh candidate, and so is this store's;
  - for `Retire`, the retirement is in force and the slab is retired here.
- **The Raft observer may install the effect first**; it earns nothing, and the worker still counts it, once, when it verifies its own effect.
- **Nothing is held between calls,** so nothing can outlive a job or be consumed by another: there is no registration.
- **Not counted** (an undercount, never an inflation):
  - selection, upload, `NothingSelected`, `GenerationMoved`, and a refused or ambiguous submission;
  - an error or a cancellation after acceptance, and a crash: the effect is completed by recovery or the observer, which earn nothing;
  - a superseded or unresolved effect, and an outcome no longer retained.
- Counters are process-local, as Prometheus counters are. The lag gauge and byte counters are untouched.
- **One recording point** (`observe_aged_rows_pruned`) feeds the exported series and, under test, the store's own tally of exactly the same rows (R3).

**L1:** the same committed-removal rule.
- S5b cleanup counts the aged occurrences of a slab rewrite after it is installed.
- The ADR 0703 LSM counts them after its certified install is Applied and delivered.

### D5. Unchanged
- ADR 0121's binding, gate, cut, trigger, read filter, delete evaluation, roster and restore refusals, and the CTAS refusal on a positive window;
- the node MVCC policy refused in mode;
- the ordinary sweeper retention rewrite contained or fenced;
- the survivor rule and the three consumers.

## Controls
**Reducer and mode:**
- `next_target`/`propose_target` compose policies 1/2/3/4 exactly; a wrong composition is refused at apply;
- certification refuses to report an unexpected policy;
- the format fences: frames 27 and 29 refused and 28 accepted; WAL and snapshot 34 refused; marker v24 refused; golden v35 against v34 (only the shapes that moved);
- validate: a pending policy contradicting its cut is refused on decode.

**Index:**
- activation labels `aged` exactly from staged evidence: an aged winner, a below-F losing intent and a below-F refused intent in one table give exactly 1 aged;
- the shared displaced/retired key;
- the staged evidence cleared with staging;
- a recovered candidate whose `AgedEvidence` alone differs is refused, with nothing restaged.
- **the committed path:** a changed `base_aged_staged` row with unchanged metadata, after report and commit and before activation, refuses the activation with the old base, debt and staging intact;
- a superseded pending gid's evidence replaced atomically, the empty set included;
- **a shared key's producer semantics:** a refused intent whose digest differs from the evidence at that key is not credited, and its `RetiredIntent` source and stripe are kept.

**Cluster, on ADR 0110's clock, at every member:**
- **The baseline, inverted:** r omitted; its debt aged; sealed and cleaned, the step reports the reclaim; no copy; reads unchanged.
- **Exactness:**
  - covered intent winners below F are omitted;
  - a row at F's millisecond is kept;
  - a deleted pair below F goes, and one with its tombstone at or above F keeps the tombstone;
  - PK keys and a winning PK tombstone are kept;
  - a dropped table's exclusion composes (policy 4);
  - a delayed below-F child after an aged tombstone stays hidden and is omitted when covered.
- **L2,** with `LocalFs` L2 on the leader:
  - an aged row in a tiered slab with a surviving neighbour goes by a certified `Tier`; the new object lacks exactly it;
  - an all-aged tiered slab goes by `Retire`;
  - the count rises by the exact aged occurrences through the path the evaluator reads, observed per store;
  - the committed replacement object, decoded from L2, holds exactly the survivors; a retired slab has no pointer, local or committed;
  - an `AfterUpload` cut, a `BeforeSubmit` cut, an error or a cancellation after acceptance (the effect then completed by recovery or the observer) count nothing;
  - **observer-first:** the Raft observer installs the effect while its worker waits, earning nothing; the worker then counts it exactly once; a replay counts nothing more;
  - each control judges its own store's tally, so in-process parallel runs cannot credit each other; the exported series must be present and well formed, and moves by at least that much;
  - **a mixed-cause tier job** (an aged removal, an ordinary debt removal and a retained neighbour): the store's L2 tally rises by exactly one, of the correct kind; on a registry of its own, the row recorders move only their row series, never lag, bytes or pairs.
- **L1:** an aged row's local copies are reclaimed by S5b (one per member) and, with S5b paused, by the certified LSM alone, counted once each; a non-aged reclaim counts nothing.
- **The idle tombstone bound:** a last governed tombstone at or above F is not scheduled by the clock alone, and is omitted by a later ordinary generation with a covering floor.
- **v24 refusal:** a positive-window v24 store with governed tables is refused at open with its data intact.

## Rationale
- **Omission is the only authority cleanup already trusts.** Reusing it, behind an explicit signed policy, keeps one survivor rule.
- **Attribution from the build's own evidence** avoids overcounting unrelated losers.
- **Counting at committed delivery** keeps the frozen gate's meaning: aged rows removed from the committed active inventory.

## Consequences (The "Negatives")
- **ADR 0704 cleans only the leader's own tiered slabs,** so a follower's L2 objects keep aged rows. Follower-owned L2 reclaim is a **prerequisite of the three-node physical bound**, tracked as its own unit.
- **Retained superseded objects** (ADR 0704 keeps them) still occupy object storage wherever a space budget counts them; that is tracked separately.
- **An error, a cancellation or a crash between acceptance and the worker's own verification undercounts** the L2 series. Counters reset with the process.
- **One certified tier job per leader-gated LSM tick** may lag Section 8 ingest; this is a measurement question, not decided here.
- **Format bump:** older stores and peers are refused; positive-window stores holding governed tables have no preservation route.
- This ADR makes no Section 8 activity claim, no 1 GiB budget claim and no three-node readiness claim; each needs its own evidence.

## Open Questions (Optional)
- Follower-owned L2 reclaim, and the accounting of retained superseded-object bytes.
- Whether the certified tier job rate meets Section 8 ingest.
