# Wendelon × Toron identity and ownership algebra

**Status:** convention. This note spans both rings, so its home is **Interior** in the Toron aggregator under [ring-product-placement.md](ring-product-placement.md); Wendelon links here. Every new contract carries an **Algebra** section (§5), written before the design. If a question does not apply, explain why rather than omit it.

**Why.** Reviews in the readiness arc exposed identity omissions and an ordering mistake. As of this review on 2026-10-14:
- **F9 (cross-tenant pending):** the local pending secondary index at the cited baseline uses canonical `write_id`, although lookups need `tenant × canonical write_id`. B1c addresses that omission in source-cleared commit `df973034`, not yet published at this review.
- **Counter replica collision (B1d):** a node-wide service keys live operations by bare `op_id`, while allocation is per node and tenant. The selected B1d design uses `tenant × origin_node × op_id`; its baseline demonstration and executable acceptance remain pending at this review.
- **V-S2 ordering:** plan review corrected the proposed ordering before publication. At the cited baseline, envelope context is checked **before** `bind` recomputes the digest in the deployment frame (`storage/mutation_visibility/root2/replay.rs:320-322`). This is an ordering-review example, not a claim that a faulty production ordering was deployed.

Each would have been caught on paper by asking: *what is this identity the product of, and what owns it on the ring?* Stated laws also make optimizations provable rather than guessed: batching commutative adds, pushdown, fusion, and placement of compute on a "weak ring".

Unless a different revision is explicitly named, source citations are pinned to `f2f046c2acf6321f5c49d9352f670bb326bf3e12`; paths are relative to `nilket/crates/wendelon/src/`. B1c, B1d and S3 design statements are distinguished from that published baseline. **(confirm)** marks an open question, not an established law or a claim about current behavior.

## 1. Coordinates (the basis)

| Coordinate | Type / source | Notes |
|---|---|---|
| tenant | `TenantId` (u64) | The isolation coordinate. **Never** implied by another coordinate. |
| mutation identity | `MutationIdentity` (`storage/mutation_visibility/identity.rs:10-18`) | Product includes generation, tenant, Raw/Table scope, table, schema generation, caller and sequence. Raw scope requires zero table/schema generation; do not collapse it into only table + schema. |
| key | raw row identity or counter identity | Raw writes re-derive `Identity::blake3_tenant` per tenant (`network/data_server.rs:15910-15948`). Counters use `counter_identity(CounterKey)` with a counter-domain keyed hash and **no tenant in that hash** (`storage/counter.rs:290-304`); tenant remains a separate coordinate. |
| client write id | `ClientWriteId { client_uuid[16], sequence_no u64, ttl, pad }` (`schema/core.rs:162`) | **Canonical** bytes = `client_uuid ‖ sequence_no.to_le_bytes()` (24 bytes). TTL and pad are excluded. Client-issued: UUID is specified per session and sequence monotonic within it. The server cannot infer a distinct session from a reused UUID/sequence. |
| idempotency key | `(tenant u64, uuid, seq)` (`network/data_server.rs:227`) | = tenant × canonical write id. |
| network node | `NodeId { id u32, _reserved u32 = 0 }` (`schema/core.rs:104`) | Specified as cluster-unique at bootstrap, with reserved bits required zero. **No incarnation coordinate:** a restart configured with the same numeric id has the same identity; the type alone proves neither allocation uniqueness nor a restart fence. |
| HLC | `HybridTimestamp { physical_ms, logical, node_id }` (`schema/core.rs:22`) | Lexicographic total order of the tuple, used in version ordering. Equal tuples remain possible and equal-HLC versions are retained (`schema/core.rs:806-819`); this is not operation uniqueness or a wall-clock guarantee. Counter HLC is synthesized from `op_id` (§3). |
| raft position | (term, node, index) | Root2 `Log2`/`EntryPosition2`. Genesis is (0,0,0), and zero coordinates are refused by `bind`. |
| marker | `FinalizationMarker` (`schema/core.rs:248-269`) | Includes log index, term, epoch, commit HLC, payload hash, safe-read index and requested durability. A marker struct alone authenticates no commit provenance. Distinct from the counter marker, whose `tentative_op_id` refers to a replica-local tentative offset under same-slab pairing (`storage/manager.rs:935-939`). Do not equate these marker identities. |
| epoch | request epoch, admission epoch, or marker epoch | These are different roles despite sharing integer representations. In the counter request, epoch is an input value and is not a proven allocation generation; replica handlers do not establish such a fence (`network/data_server/counter.rs`, replicate/rollback handlers). Do not generalize this finding to all epoch-bearing subsystems. |
| op_id | per-(node, tenant) counter leader allocator (`storage/counter.rs:1364-1490`) | Fresh allocator starts at 1; recovery seeds past the maximum across **applied records and pending witnesses**. Scope is the node/tenant allocator; unwitnessed ids can be reused after recovery. No incarnation or overflow-safety proof is implied. |
| locality / placement | `Locality`, `PlacementKey` | Counter routing uses locality derived from counter identity. Record routing may instead use stamped partition placement plus migration-ledger ownership; locality alone is not every record's serving authority (§2). |
| deployment frame / pinned root identity | Root2 (generation, cluster), plus separately pinned root context | Generation/cluster is a frame, not the complete root identity: `RootIdentity2` also has pin/admin key (`storage/mutation_visibility/root2/provisioning/inventory.rs:37-42`); `PinnedBoundTuple2` carries manifest/inventory pins and record (`storage/mutation_visibility/root2/provisioning/replica_anchor.rs:33-40`). Compare envelope frame before `bind`; preserve the other pins independently. |
| local slab offset | `RecordLocation { slab_id, offset, ... }` (`storage/core.rs:90`) | A physical locator needs its node/store/slab context. The replica service maps a leader-issued op id to its own tentative location and writes that local offset into its counter marker (`storage/counter.rs:774-775`). Numeric equality does not make these identities interchangeable. |

**Rule 1 (no implied coordinates).** If two values can be equal for different tenants, nodes, epochs or sessions, then the identity of anything keyed by them must include the distinguishing coordinate, or the contract must state why a collision is harmless.

## 2. Owner functions (the ring)

| Owned thing | Owner function | Change events |
|---|---|---|
| static counter route | For nonempty sorted/deduplicated nodes, replicas start at `locality % n` and take `min(max(rf,1),n)` successors. The counter handler selects the first as owner (`network/data_server/replica_routing.rs:39-78`; `network/data_server/counter.rs:224-240`). Empty topology has no owner. | topology/RF changes require an explicit contract; this static implementation does not prove a dynamic handoff. |
| partitioned record serving | Stamped `PlacementKey`, router and migration-ledger override (`network/data_server/replica_routing.rs:81-110`), rather than raw locality alone | placement or ledger changes; verify each consuming path before claiming transfer behavior. |
| counter op allocation | One cached `CounterLeader` per local node/tenant, shared across that tenant's keys (`network/data_server.rs:6609-6655`); per-key request ownership is checked separately. | process restart/service recreation; reseed from applied records + pending witnesses, not all allocated ids. |
| a raft group's log | the raft leader for that term | an election (term++) |
| a Root2 root | Deployment frame plus pinned root context (§1), bound into trust/root state; not itself a Raft leader or a data-routing function | bootstrap establishes it; no migration or deployment-transition procedure is established here. |
| pending-finalization index entries | Local node/store; at f2 the secondary key omits tenant. The selected B1c v2 rebuild derives tenant-scoped entries from primaries at every open. | open/reopen rebuild under B1c; not a distributed ownership-transfer mechanism. |

**Rule 2 (overlap).** Distinct allocators can send equal local operation numbers to an overlapping replica. Their live-operation maps must preserve the distinguishing origin (the selected B1d case). This is **not** a rule to add owner to every data key: tenant/key identity may intentionally remain stable across owners. Audit the effective key, including instance scope, and justify each omitted coordinate.

**Rule 3 (handoff).** Each change event is a contract-time question: what does the new owner inherit (live tentatives, caches, allocator state), and what must it never trust? F-CTR-4 includes live-index visibility; propagation to a later committed value is **lifecycle-dependent**, not yet established as a universal handoff defect. Establish reachability before designing its fix, and answer transfer questions before claiming a dynamic-ring capability.

## 3. Laws (what each subsystem relies on)

| Subsystem | Law | Where it is used |
|---|---|---|
| Writes | Retry behavior is scoped to the route and retained record/cache lifetime. Canonical tenant/write identity must converge; A1/A2/pending replay have different retention and response semantics. This is not a universal exactly-once or original-rowcount law. | enumerate the specific retry/restart route and its retained evidence |
| Writes across tenants | Required independence: equal canonical write ids under distinct tenants denote distinct operations. B1c addresses the pending-index omission; f2 still has that defect. | tenant isolation, including restart/rebuild readers |
| Counter replica tentative | **Selected B1d law**, not f2 behavior: idempotent under tenant × origin × op id only while pending, for identical key/value/HLC; changed content conflicts. `ReplicaOpKey2` is absent at f2. After pending removal, re-drive may append again. | qualify fan-out retries; do not infer crash-generation or universal add idempotency |
| Counter adds (many) | Pure integer addition is commutative; the implemented operation includes checked-u64 overflow, acceptance, previous/current responses, idempotency records and ordered versions. Different orders can change accepted operations and even final value on overflow. Counter HLC = `(op_id,0,0)` (`storage/counter.rs:1433-1438`). | No batching/reordering law is established: preserving HLC order alone is insufficient. **(confirm)** the full observable equivalence before optimizing. |
| Root2 reduce | Deterministic fold under the required trust/context/limits. Exact replay of the **current covered position plus fingerprint and digest** can yield `AlreadyProcessed` after applicable checks (`storage/mutation_visibility/root2/reducer.rs:66-94`). This is not arbitrary historical-coordinate deduplication: the replay dispatcher itself treats `AlreadyProcessed` as an internal fault (`storage/mutation_visibility/root2/replay.rs:343-345`). A refusal fails the replay operation without skipping the entry, not necessarily the process. A successfully consumed next-coordinate denial advances coverage, not authorization success; finishing the step can still fail. | replay/recomputation with the same inputs and checks |
| Root2 governance (R-a) | The selected, source-cleared S3 contract (`976c55f8`, unpublished at this review) verifies a deployment/command-digest envelope without **submitted-entry** position binding. Installed/bootstrap position is still signed (`storage/mutation_visibility/root2/governance_command.rs:149-152`); reuse requires matching installed context/digest and still passes applicable issuer/scope checks and reducer rules. At f2 the primitive is dormant and the nine governance kinds remain unsupported by the verifier. | explicit re-submission law for S3; not a current serving capability |
| Query (Toron) | Predicate pushdown, boolean distribution, residual minimization and logical identity arithmetic are specified by Toron ADRs 0075, 0078, 0094, 0169 and 0170, with their own semantic preconditions. | check each rewrite's conditions; this note does not prove optimizer or cross-ring snapshot correctness |

**Rule 4.** Every retry, re-drive or replay in a contract is written as an **application of a named law**. "Re-drive is safe" means "re-drive is safe because operation X is idempotent under identity Y".

## 4. Visibility by phase

| Thing | Phases | Readable at |
|---|---|---|
| a row write | route-specific append, acknowledgements, witness persistence and finalization-marker milestones; their ordering is not asserted as one universal pipeline here | **(confirm per path)** the actual milestone order, snapshot eligibility and which reads can feed later writes. |
| a counter op | replica tentative (**live-indexed immediately**, `storage/manager.rs:906-931`) → commit marker | `read_committed` consults that live index; the leader reads its base before fan-out. Whether an orphan can feed a later committed add depends on service/allocator/recovery lifecycle. F-CTR-4 reachability question; not a blanket committed-only guarantee. |
| a Root2 entry | acquired committed entry → context/bind/verify → fold/coverage → durable replay mark | Dormant replay only, with no production proposer/serving activation. Coverage/mark progress is not equivalent to authorization or successful command effects. |

**Rule 5.** A contract states, for each phase, *who can read it and whether that read can feed a later committed value*. A "yes" on an uncommitted phase is a finding until it is justified.

## 5. Contract template: the Algebra section (required, before the design)

```
## Algebra
1. Identity:   <operation> = <coord> × <coord> × ... ; omitted coordinates and why the omission is safe.
2. Owner:      owner function (§2 row); behaviour on each change event (handoff/rebalance/membership/restart).
3. Laws:       idempotent / commutative / associative / deterministic — and which retries/re-drives use which law.
4. Visibility: per phase, who reads it, and whether such a read can reach a committed value.
5. Collisions: for each map/cache/index introduced or touched: its key vs the identity in (1). Must be equal or justified.
```

The plan reviewer checks §5 line by line against the census. A narrower **effective** map/cache/index key is a HOLD until the contract proves why omitted coordinates are supplied by instance scope, cannot coexist, or collide harmlessly. A justification must cover lifetime, reopen, retries and owner changes; neither a type name nor a short test substitutes for it.

## 6. Open items surfaced by writing this note

- **F-CTR-4:** live tentative visibility is proven; committed propagation remains lifecycle-dependent (§2 Rule 3, §4). Establish the reachable lifecycle first.
- **Counter HLC synthesized from op_id:** batching/reordering needs an equivalence proof including order, overflow, acceptance, responses and replay (§3).
- **No network NodeId incarnation:** Root2 `NodeRef2` and `OriginalGrant2` separately carry incarnation fields (`storage/mutation_visibility/root2/types.rs:40-43,67-74`). Reserved network NodeId bits must remain zero today. Using them as a generation would be a separately reviewed wire/identity contract change, not an available restart fence (§1).
- **F-PF-1:** the pending-finalization backlog has no bound; record the bound in each touching contract's Visibility/Laws.
- **Toron × Wendelon seam:** what Toron's read snapshots assume about Wendelon phase visibility, to be filled in per read path **(confirm)**.

## Relation to existing conventions

- `ring-product-placement.md` classifies **products** (Interior/Exterior × Substrate/Composition) to decide where a design lives. This note classifies **operations** (identity × owner × laws × visibility) to decide what a design must preserve. They are complementary: classify the product first, then state the algebra.
- The existing **Leap Rule** flags new substrate capability disguised as composition. This convention additionally requires coordinate-changing fixes (such as adding tenant to a key or threading origin) to receive explicit identity/ownership integration review; this is an adopted review rule, not a quotation that every bug fix is a new Substrate capability.
- This convention makes previously implicit review questions explicit. A source census and written laws support review; they do not prove every future implementation or optimization correct.

## Adoption

- Every new contract carries §5; R7 and F-CTR-4 are initial consumers. Explicitly justify non-applicable entries.
- Plan review checks the Algebra section against the source census before implementation selection.
- Changes to this convention follow docs-only review/signing and a separate joint fast-forward publication decision. This note does not authorize code changes, runtime activation or publication.
