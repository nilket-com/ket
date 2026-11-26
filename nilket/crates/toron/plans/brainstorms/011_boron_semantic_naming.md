# Brainstorm 011: Semantic Boron Naming Across the Strong and Weak Rings

**Category:** Interior (workspace-wide vocabulary across `nilket`, `toron`, `toron-client`, `wendelon`, and `wendelon-client`)

## Context

Nilket already enforces [no-planning-phase-names.md](../../../wendelon/plans/conventions/no-planning-phase-names.md)
(*"name by what, not by when"*), [no-version-suffixes.md](../../../wendelon/plans/conventions/no-version-suffixes.md),
and [no-project-name-in-protocols.md](../../../wendelon/plans/conventions/no-project-name-in-protocols.md).
Row-group execution stripped `phase_b_fetch` in favor of `predicate_fetch`
because names tied to an ADR's drafting chronology force archaeology on every
future reader.

Yet the transactional core of Nilket was built under intense cutover pressure
and still carries **cutover-contrast names** — identifiers that describe *which
migration step or historical bug* motivated them rather than *what mathematical
object they represent*:

1. **`DecisionMode::{Today, ParentDecisions}`** (`--decision-authority`,
   `MODE_MARKER = "PARENT_DECISIONS"`, `ProposalRefusal::ModeToday`,
   `ChildRefusal::ModeToday`): `Today` is a calendar adverb that immediately
   decays into "yesterday," and once retired there is only one authority.
   `ParentDecisions` named an ADR 0100 contrast ("decide visibility at the
   parent rather than leaking child appends early"); outside that bug report,
   "parent decisions" tells a reader nothing — every tree has parents and every
   branch is a decision.
2. **`ParentKey` / `Claim` / `ChildWrite` / `bodies` / `keys` / `RecordNoop`:**
   "Parent/Child" suggests nested subtransactions or foreign-key hierarchies,
   hiding the actual two-step LTAP physics: **unauthoritative payload staging on
   ring arcs** followed by **atomic spacetime-arc certification**.
3. **A five-way collision on `Finalize*`:** `WendelonRequest::Finalize`,
   `DecisionCommand::Finalize`, `FinalizationMarker`, `TableDropFinalize`, and
   `TableDropLogicalFinalize` all overload one verb across incompatible legacy
   and current protocols — while the resulting state isn't even called
   `Finalized`, it is `Status::Accepted`.
4. **Four unrelated counters all named `generation`:** table schema version,
   range-lease holder tenure, certified-base sequence `gid`, and deployment
   frame generation.
5. **Bespoke per-verb and ordinal sprawl across both rings:** seven separate
   `Ctas*` outcome types in `toron-client` (`CtasCompositePhase`,
   `CtasPublicationCause`, etc.), the entire `*2` suffix family in
   `storage/mutation_visibility/root2/` (`ClaimProjection2`, `Digest2`, `Log2`,
   `NodeRef2`, `ChildAck2`), and brand-prefixed protocol surfaces
   (`WendelonRequest`, `WendelonResponse`, `WendelonClient`, `ToronClient`).

With the [Boron semiring](../../../wendelon/plans/brainstorms/021_boron_semiring.md)
($\mathcal{B}_{\text{weak}} \ltimes_K \mathcal{B}_{\text{strong}}$) in place,
every operation has an exact mathematical identity: **one of five Boron
generators** (`read`, `merge`, `update`, `await`, `send`) acting on **one of two
persistence rings** (`_strong` durable state in Wendelon, `_weak` ephemeral RAM
state in Toron). This note defines the target vocabulary so the codebase reads
directly like its algebra.

---

## 1. The Two Naming Axes: Boron Generator × Ring Strength

Every cross-ring or engine-boundary entry point declares **(a) its Boron
generator** (fixing its commutation law and Herlihy consensus cost) and **(b)
its ring persistence domain** (`_strong` on durable $\Omega_{\text{strong}}$ vs.
`_weak` on ephemeral $\Omega_{\text{weak}}$).

### A. Strong Ring (`wendelon` — Durable $\mathcal{B}_{\text{strong}}$)

| Canonical Boundary Signature | Consensus # | Mathematical Role (Replaces Vague Legacy Terms) |
|---|---|---|
| `read_strong(read_arcs, cut_k)` | **1** | Snapshot read over `CertifiedBase@B` + `Certified` in $(B, K]$; zero conflict footprint against transitions $> K$. |
| `merge_strong(target_arc, op_id, delta)` | **1** | Silent, self-commuting monoid shift (`AppendOnly` row batch under `table.live`; monotone `max` `activated_cut`). |
| `update_strong(footprint, tx_id, cut_k, manifest)` | **$\infty$** | **Replaces "ParentDecisions" write/DDL vocabulary.** Certifies at log index $L$ iff $(W \cup M) \cap R_{\text{arcs}} = \emptyset$ and $W \cap W_{\text{arcs}} = \emptyset$ across $(K, L)$, then publishes `manifest`. |
| `await_strong(guard_arc, predicate, deadline)` | **1** | Partial test projection suspending until `predicate` holds (`cut >= K`, lease active, reader drain). |
| `send_strong(intent_token, payload)` | **1** | Emits per-token durable outbox work (`IntentToken = (tx_id, ordinal)`) for tiering, compaction, retention, CDC, or query-cancel tombstones. |
| `resolve_strong(tx_id)` | **1** *(at $K$)* | Queries the durable outcome of an `Indeterminate` `update_strong` or `merge_strong` by `tx_id` without re-executing mutations. |

#### Bridging the Public `update_strong` Call and Its Internal Sub-Steps
At the `toron` $\to$ `wendelon` boundary, `update_strong` is **one** conceptual
transition. Internally on the Strong Ring, durability splits into three cleanly
named phases (which also replace ad-hoc trace span names like `"coordinator
before"`, `"children"`, `"coordinator after"`):

1. **`reserve_update`** (currently `DecisionCommand::Reserve`): binds open
   `ArcTenure`s and bounds `commit_hlc`.
2. **`stage_arcs`** (currently `child_write_keyed` in `child_writes.rs`): writes
   immutable, unreferenced payload chunks to replica heaps and returns signed
   `StageReceipt`s (Boron Law 4: zero visibility authority while staged).
3. **`certify_update`** (currently `DecisionCommand::Finalize`): verifies
   `StageReceipt` quorum + clean `(read_arcs, write_arcs)` over $(K, L]$ and
   records `CertStatus::Certified { commit_index: L }`.

*(Naming step 2 `stage_arcs` and step 3 `certify_update` also makes a future
protocol optimization self-evident: when `stage_arcs` uploads content-addressed
chunks before Raft, `certify_update` can collapse `reserve + certify` into a
single Raft round whenever `ArcTenure` is unchanged.)*

### B. Weak Ring (`toron` — Ephemeral RAM $\mathcal{B}_{\text{weak}}$, No Raft Group)

| Canonical Boundary Signature | Consensus # | Mathematical Role on Ephemeral Compute State |
|---|---|---|
| `read_weak(cells, cut_k)` | **1** | Reads cached `TableDescriptor` at `(table_id, SchemaRev)`, optimizer facts, or session-pinned cut $K$. |
| `merge_weak(cell, op_id, delta)` | **1** | Shuffle stage-completion bitsets (`\|=`), partial aggregates/sketches, monotone descriptor cache fills. |
| `update_weak(footprint, op_id, f)` | **$\infty$** | **Single-owner CPU `CAS` / local `Mutex` (never Raft):** per-node slot/memory admission, open transaction session buffers, local query-state transitions. |
| `await_weak(cell, predicate, deadline)` | **1** | Shuffle stage barriers (`completed == all_workers`), local slot waits, descriptor-sync waits. |
| `send_weak(token, msg)` | **1** | Morsel dispatch to workers, cross-ring cancel emission (`send_weak` $\to$ `send_strong`), trace carriers. |

#### Why Toron Has No Raft Group (Consensus $\infty \neq$ Network Consensus)

An early note ([Brainstorm 002](002_weak_control_ring.md)) imagined an in-memory
Raft group across Toron coordinators. Separating **consensus number** from
**crash survival** ([Brainstorm 021 §5](../../../wendelon/plans/brainstorms/021_boron_semiring.md))
shows why Toron needs **zero Raft**:

- Inside a single process, CPU atomics (`compare_exchange`) and local `Mutex`es
  **already have Herlihy consensus number $\infty$**. Network Raft is strictly
  the price of making a `CAS` register **durable across machine crashes**.
- Every case in Brainstorm 002 splits cleanly into Strong truth on Wendelon or
  single-owner Weak RAM in Toron:
  1. **Active queries & open `BEGIN ... COMMIT` sessions:** Owned by a single
     Toron coordinator (`update_weak` = local memory). If that coordinator
     crashes before `COMMIT`, the session aborts cleanly on disconnect with zero
     locks or state to unwind on Wendelon; if it crashes *during* `COMMIT`, the
     reconnecting client calls `resolve_strong(tx_id)` on Wendelon.
  2. **Tenant quotas:** Durable quota limits live on Wendelon
     (`UpdateTenantQuota`); runtime admission on Toron uses local CPU `CAS`
     semaphores per coordinator (`quota / active_coordinators` or node-local
     memory/core budgets), never a Raft round-trip per query.
  3. **Shuffle barriers:** Workers reporting stage completion perform bitwise-OR
     `merge_weak` (`completed |= 1 << worker`) + `await_weak`
     (`completed == all_workers`) inside the query's owning coordinator
     (consensus #1, not $\infty$).

Put simply: **if state must survive a node crash, it belongs in Wendelon
(`_strong`); if it may vanish on a node crash, one owning Toron process's CPU
`CAS` implements `update_weak` with no Raft group.** Multi-statement LTAP
`BEGIN ... COMMIT` therefore needs neither Toron Raft nor cross-ring 2PC: open
statements accumulate on Toron via `read_strong(R, cut_k)` + local `update_weak`
+ unauthoritative `stage_arcs`, and `COMMIT` executes **one**
`update_strong(footprint, tx_id, cut_k, manifest)`.

---

## 2. Retiring Overloaded Verbs and Splitting Colliding Nouns

### A. Retiring the Five-Way `Finalize*` Collision
`Finalize` currently means five different things across legacy and active paths.
Replace each with its exact semantic verb:

| Legacy Identifier | What It Actually Does | Target Identifier |
|---|---|---|
| `DecisionCommand::Finalize` + `Status::Accepted` | Certifies staged receipts at Raft log index $L$. | **`CertCommand::Certify`** $\to$ **`CertStatus::Certified`** |
| `WendelonRequest::Finalize(TenantFinalization)` + `FinalizationMarker` | Deprecated `Today`-mode high-water marker (`E_safe`). | **Delete** with `Today` mode. |
| `TableDropPrepare` $\to$ `TableDropLogicalFinalize` | Fences `table` cell, then unbinds `names` cell and emits reclaim debt. | **`FenceTableDrop`** $\to$ **`UnbindDroppedTable`** (or **`CellEffect::UnbindTable`** inside `UpdateClaim`). |
| `TableDropFinalize` / `TableDropSettlement` / `TableReclaimCompletion` | Legacy `Today`-mode physical drop stages. | **Delete** with `Today` mode; physical reclamation is async `HeapIntent` drain. |

### B. Splitting the Four Overloaded `"Generation"` Nouns
Today, a parameter `generation: u64` can mean any of four unrelated counters.
Under [newtype-units.md](../../../wendelon/plans/conventions/newtype-units.md),
each gets an incompatible newtype and distinct noun:

| Current Field / Variable | Actual Domain | Target Newtype & Noun |
|---|---|---|
| `schema_generation: u64` | Monotone DDL revision of `(TenantId, TableObjectId)`. | **`SchemaRev(u64)`** |
| `Lineage::generation: u64`, `expected_generation` | Range-holder lease tenure incremented on lease gap or takeover (`decisions.rs:100`). | **`LeaseTenure(u64)`** on **`ArcTenure`** (replaces `Lineage`) |
| `gid: u64` (`CommitGeneration`, `active_gid`) | Sequence number of a materialized base snapshot at cut $B \le K$. | **`BaseSeq(u64)`** paired with **`SnapshotCut(LogIndex)`** |
| `StoreIncarnation` | Unique birth identity of a physical node store directory across replacements. | Keep **`StoreIncarnation`** (already exact) |

---

## 3. Full Identifier Crosswalk (`wendelon`, `toron`, `nilket`)

| Current Identifier(s) | Problem | Target Semantic Identifier |
|---|---|---|
| `DecisionMode::{Today, ParentDecisions}`, `RawDecisionAuthority`, `--decision-authority`, `ModeToday` | Calendar/cutover contrast; a single surviving mode needs no mode enum. | **Delete enum** post-cutover. Name module **`certifier`** (`SpacetimeCertifier`, metrics `tx_cert_*`). |
| `ParentKey` (`decisions.rs:77`) | Sounds like a tree key; actually the transaction idempotency & response handle $\iota$. | **`TxId`** (`{ tenant, scope: CellScope, client_uuid, seq }`). |
| `ParentNamespace` (`Kv`, `Table`) | Tied to "Parent" metaphor; omits catalog cells. | **`CellScope`** (`Kv`, `Table(TableObjectId)`, `Catalog`). |
| `Claim`, `Operation::{Write, Delete}`, `RecordNoop` | Verb-specific claim variants + separate noop command; hides uniform `(R, W)` structure. | **`UpdateClaim`** carrying `Footprint` + `SmallVec<[CellEffect; 4]>` (`write_arcs = ∅` subsumes `RecordNoop`). |
| `bodies`, `keys` (`decisions.rs`) | Generic container words (`keys` collides with row/crypto keys). | **`cert_ledger`** (`TxId -> CertRecord`), **`retry_index`**. |
| `ChildWrite`, `child_writes.rs`, `ChildAckBody`, `ChildRefusal` | "Child" obscures Boron Law 4 (staged heap chunks carry zero authority until certified). | **`ArcStage`**, **`arc_staging.rs`**, **`StageReceipt`**, **`StageRefusal`**. |
| `Target`, `Report`, `CommitGeneration` (`decisions.rs:472`) | Collides in grep with build targets and test reports. | **`BaseCutTarget`**, **`BaseInventoryReport`**, **`InstallBaseCut`**. |
| `OwnerOp` / `SlabOp` / `CancelOp` / `AckOutcomes` | Mixes deprecated bare ops with signed owner ops; hides outbox token $m \in \mathcal{M}_{\text{outbox}}$. | **`HeapIntent`** / **`SignedHeapIntent`** keyed by **`IntentToken { tx_id, ordinal }`**. |
| `BeginBackupHold` / `ReleaseBackupHold` | Cross-ring Strong lock held across per-table Weak scans ($K_i \ge R$). | **Retire** once export uses one **`SnapshotCut`** $K$ + local **`CutPin`** reader-drain lease. |
| `CtasSaga` + 7 `Ctas*` outcome types in `toron-client` | Bespoke multi-type state machine for one SQL verb (`CREATE TABLE AS SELECT`). | **`UpdateClaim`** writing `{names, new_table}` + uniform **`CommitOutcome`** / **`MutationResolution`**. |
| `WendelonRequest`, `WendelonResponse`, `WendelonStateMachine`, `WendelonClient`, `ToronClient` | Brand-prefixed protocol/client structs ([no-project-name-in-protocols.md](../../../wendelon/plans/conventions/no-project-name-in-protocols.md)). | **`RingCommand`**, **`RingResponse`**, **`RingStateMachine`**, **`wendelon_client::Client`**, **`toron_client::Client`**. |
| `Root2*`, `ClaimProjection2`, `Digest2`, `Log2`, `NodeRef2`, `ChildAck2` (`root2/types.rs`) | Chronological `*2` iteration suffix across an entire module. | **`TrustAnchor`** family (or retire unused `Root2` surfaces; strip `*2` on any surviving types). |

---

## 4. Target Rust Surface Sketch (Including the Epistemic Outcome Triad)

A critical Nilket invariant (ADR 0100, ADR 0732) is that **timeout or transport
loss never invents `Refused`**, and **a recorded refusal is never answered as
success**. Encoding the three-valued epistemic outcome directly into
`update_strong`'s return type prevents callers from treating an `Err` as
"known not committed":

```rust
/// Half-open locality interval [start, end) on the Unified Ring.
pub struct RingArc {
	pub start: Locality,
	pub end: Locality,
}

/// Hierarchical cell address in Ω_strong: (tenant, scope, key_arc).
pub struct CellArc {
	pub tenant: TenantId,
	pub scope: CellScope,
	pub arc: RingArc,
}

/// Guard (R \ W) and write (W) sets for spacetime-arc certification over (K, L].
pub struct Footprint {
	pub read_arcs: SmallVec<[CellArc; 4]>,
	pub write_arcs: SmallVec<[CellArc; 4]>,
}

/// Closed Raft log cut K against which reads and certifications are evaluated.
pub struct SnapshotCut(pub LogIndex);

/// Atomic state effects applied at commit_index L if Footprint certifies clean.
pub enum CellEffect {
	StageRows(StagedArcManifest),
	TombstoneRows(StagedArcManifest),
	BindCatalogName { name: Identifier, table: TableObjectId, rev: SchemaRev },
	UnbindCatalogName { name: Identifier, expected_table: TableObjectId, rev: SchemaRev },
	SetTableState { table: TableObjectId, rev: SchemaRev, state: TableLifecycleState },
}

/// Three-valued epistemic result of a Strong-Ring mutation.
/// Callers MUST NOT treat `Indeterminate` as aborted; resolve via `resolve_strong(tx_id)`.
#[must_use]
pub enum CommitOutcome {
	Certified {
		tx_id: TxId,
		commit_cut: SnapshotCut,
		commit_hlc: HybridTimestamp,
	},
	Refused {
		tx_id: TxId,
		decided_at: Option<SnapshotCut>,
		reason: RefusalReason,
	},
	Indeterminate {
		tx_id: TxId,
		stop_reason: IndeterminateReason,
	},
}

/// Consensus-∞ Strong-Ring transition: certifies clean (read_arcs, write_arcs)
/// over (cut_k, commit_cut] and atomically publishes `effects` + `outbox` tokens.
pub async fn update_strong(
	&self,
	footprint: Footprint,
	tx_id: TxId,
	cut_k: SnapshotCut,
	effects: SmallVec<[CellEffect; 4]>,
	outbox: SmallVec<[HeapIntent; 2]>,
) -> CommitOutcome;
```

Every Boron invariant is visible in that single signature without opening an ADR:

- **Commutation & conflicts:** `footprint` (`read_arcs`, `write_arcs`) over
  `(cut_k, commit_cut]`.
- **Call-indexed idempotency ($\iota \in \text{CallId}$):** `tx_id: TxId`.
- **Zero-row DML / `RecordNoop`:** `effects.is_empty()` with non-empty
  `footprint.read_arcs` still records a durable `Certified` receipt under `tx_id`.
- **DDL (`CREATE`, `DROP`, `CTAS`):** `BindCatalogName` / `UnbindCatalogName` /
  `SetTableState` composed with `StageRows` in one `effects` slice — no bespoke
  `CtasSaga` or five-stage `TableDrop*` state machine.
- **Transactional outbox (`send` = $\mathbf{1} \otimes a^\dagger_m$):** `outbox`
  emits per-token `HeapIntent`s atomically with `effects`, commuting with
  concurrent producers and background sweepers (`m_1 != m_2`).
- **Epistemic honesty:** `#[must_use] CommitOutcome` forces handling `Certified`,
  `Refused`, and `Indeterminate` explicitly.

---

## 5. Observability & Metric Alignment

In [Brainstorm 020](../../../wendelon/plans/brainstorms/020_live_memory_on_the_database_website.md)
and operator runbooks, metrics and trace spans are user-facing identifiers:

- **Uniform Boron labels:** Tag ring boundary counters and histograms by
  `ring = "strong" | "weak"` and `op = "read" | "merge" | "update" | "await" | "send"`
  rather than ad-hoc subsystem names (`parent_decisions_*`).
- **Four canonical `update_strong` spans:** Name the four coordinator spans
  `admit` $\to$ `reserve` $\to$ `stage_arcs` $\to$ `certify` consistently across
  code, OpenMetrics families, and OTLP traces.

---

## 6. Rollout Discipline: Trim With the Wind, Never Burn Two Ladders

A wholesale tree-wide rename while `Today` mode and Section 8 diagnostics are
still active would create merge churn and duplicate multi-hour test-ladder runs.
Instead, execute the rename across three zero-waste seams:

1. **Immediate vocabulary freeze in new code & ADRs:**
   - Stop minting new `ParentDecisions*`, `Today*`, `Finalize*`, `*2` type
     suffixes, bare `(u64, u64)` arc tuples, brand-prefixed structs, or
     unqualified `generation: u64` fields.
   - New modules, traits, metrics, and ADRs adopt `{read, merge, update, await,
     send}_{strong, weak}`, `TxId`, `RingArc`, `Footprint`, `SnapshotCut`,
     `CommitOutcome`, and split nouns (`SchemaRev`, `LeaseTenure`, `BaseSeq`).
2. **Piggyback the `ParentDecisions` rename onto `Today`-mode deletion:**
   - Retiring `Today` mode *already* edits every `DecisionMode` match arm,
     `ModeToday` refusal, and dual-mode fixture.
   - Delete `DecisionMode` outright in that cut (never leave a 1-variant
     `DecisionMode::ParentDecisions` relic), renaming the surviving `decisions`
     and `child_writes` modules to `certifier` and `arc_staging` in the same
     subtractive pass.
3. **Couple wire/serde renames to the planned pre-release tag-compaction cut:**
   - Pure Rust identifier renames preserve wire bytes (using explicit codec tags
     or `#[serde(rename = ...)]` temporarily where needed), and any serialized
     variant compaction lands once in the scheduled pre-release `RingCommand`
     format cut (leaving pinned `Root2` audit vectors untouched until `Root2`
     itself is retired).

[The Boron Semiring](../../../wendelon/plans/brainstorms/021_boron_semiring.md) ·
[Weak Control Ring](002_weak_control_ring.md) ·
[Live memory on the database website](../../../wendelon/plans/brainstorms/020_live_memory_on_the_database_website.md) ·
[Ring-product placement](../conventions/ring-product-placement.md) ·
[Identity and ownership algebra](../conventions/identity-ownership-algebra.md) ·
[No planning-phase names](../../../wendelon/plans/conventions/no-planning-phase-names.md) ·
[No project name in protocols](../../../wendelon/plans/conventions/no-project-name-in-protocols.md)
