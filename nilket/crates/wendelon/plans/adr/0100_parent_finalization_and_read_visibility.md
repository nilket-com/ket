# ADR 0100 - Parent finalization and read visibility

## Status

Accepted; partially implemented (2026-10-17). Landed ADR 0100 slices, in ancestry order: authority substrate and logical adapters (d4c2004c, f3bdcc9d, 653620e5, 5e90873e, 8c649102, f8bd0011, b44d017f, eb45df36); R2 codecs, durable local storage, recovery/admission repairs, and claim/signature checks (34808b0b, 8e361b93, 7f6585a9, 7dc4f620, c3068914); dormant manifest/bootstrap/identity/origin and anchored provisioning (d967c53e, 1ad8f7b2, b6c81048, 9dc784c9, 8f1811b7, 4154c125); owner inventory/snapshot/core/directory and replica/install/administrator checks (bde6fb62, 46285381, 10b7dc48, 609dbdf2, 31db233d, 88156fb6, 70d455cc, 52a2c7a0, f88bb502); G1d/G1e admission and replay slices (170f3a98, 2a4afe1d, 2aeb609e, 02eab5fa, 57df2290, 3529a4fc, b367b7f9, 5972987b); retained control log and recovery/committed-entry adapters (2be56f64, ad341e1b, 9d17b51b, 8da24406); and root2 post-root, bounded-prefix, profile-2, provisioning, owned-replay and G2 coverage (549887b9, a0eb4a77, bd99078a, b291b0ae, 72dc0792, c6e603a1, d6ce07b8, 0c83b021, 0ab73574, df08b82f, 76bf5218, f41e1cd5, 37d9e967, 4fac574f, 7174c4e1, 0e32df24, 26dba22f, c56c39fa, 3cc42d94, 09c66346, e48a8173, 10bb55b1). Dormant implementation slices do not complete end-to-end latest-at-K logical read visibility, production activation, migration or mixed-version readiness, or the operating ceiling; lease safety, P3, and the campaign remain held. ADR 0101 now tracks R7 administrator-key lifecycle; compromise audit/runbooks, handover, and key custody remain open there.

## Context and established facts

The store requires a logical commit to govern what readers can see. [Toron ADR 0542](../../../toron/plans/adr/0542_write_path_progress_bounds.md), Phase 0 at line 146, says durable child appends are not the logical commit and that visibility advances once through the parent finalization. The bounded ADR 629 pin contradicts that statement for the tested paths:

| Mutation held after durable append, before parent Finalize | Table full / PK scans | Embedded Toron SQL full / PK SELECT |
| --- | --- | --- |
| INSERT id 629, baseline id 630 | inserted row returned | inserted row returned |
| DELETE id 629, after committed INSERT | row hidden; PK KEY_NOT_FOUND | row hidden; PK SELECT empty |

The writer's OK followed a matching applied parent Finalize in both operations. Evidence: `adr629-table-sql-visibility-pin-1`, SHA256SUMS `27c0bdcf18763889232a633c009ac6baf890350e24999a48dfa55a2d653c4d98`, accepted via chatd request `01a10d7c-8235-75a6-a28b-c4daf8a89953`. Single fresh node, RF1, signed lease, gate ON, real loopback mTLS and embedded SQL planner/coordinator/storage scan; public SQL listener and distributed visibility were not tested. Earlier raw-path and epoch pins are separate evidence. No actual refused Finalize was injected in this table pin.

Code inventory below is at `c9793d3d524b48f2f65898fe0787a0664dbc9dcc`:

- `network/data_server.rs:9034` appends table children durably before pending-intent persistence at `:9180`. The existing test barrier blocks that persistence before transaction commit, and hence before parent Finalize.
- Table preparation at `:5794` checks LINEARIZABLE's minimum HLC against the admission watermark, then at `:6018` runs against the slab. The minimum is an admission threshold, not an upper read bound or a row's finality proof.
- `storage/manager.rs:2704` selects the newest version by HLC, then suppresses a tombstone; `:3321`/`:3327` point lookup likewise uses the version list. `storage/core.rs:843` treats zero as-of HLC as latest. Neither selector has a parent acceptance predicate.
- `consensus/raft_storage.rs:1490` describes E_safe as the **highest** FinalizationMarker HLC seen in an epoch; `:3972` advances it by maximum. A later marker can overtake an earlier pending mutation. This is not a gap-free finalized frontier.
- `RecordLocation` (`storage/core.rs:90`) has location, HLC, tombstone and placement fields, but no parent acceptance identity. Existing pending records (`network/pending_finalizations.rs:84`) carry the parent write identity and finalization separately. A durable binding from every logical version to its parent must be designed, not inferred from equal HLCs.
- `network/data_server/mutation_protocol.rs:174` persists a terminal outcome, updates replay state, releases pending CDC and then removes the pending witness. Log-store lookup at `consensus/raft_storage.rs:875` recognizes a matching Finalize proposal from applied entries, not a separately persisted Accepted/Refused decision. Introducing refusal must change both resolution and lookup; Raft client_write returning Ok is not acceptance.

This study uses those code facts; all proposed structures below are new mechanisms. No existing mechanism has been proved equivalent.

## Required semantics, shared by all options

1. Every mutation has a durable identity bound to tenant, table/raw scope, generation, parent operation, content/children digest and admitted epoch. No cross-tenant replay key or HLC-only join. One parent decision covers every child and tombstone. Accepted, Refused and Unknown are distinct; timeout alone never invents Refused.
2. A read returns only accepted logical versions at its chosen statement cut. Unknown and refused versions never hide the previous accepted version. Version selection must filter eligibility **before** choosing the newest version and interpreting its tombstone. This applies to raw reads, PK/index scans, range/column scans, aggregates, joins, materializations, migration and tiered reads; caller trust does not waive it.
3. At a fixed cut, a multi-child parent is all visible or none. A replica lacking required durable children or the cut's decision state waits or returns a typed unavailable error. It cannot claim a complete result by silently omitting accepted data. Streaming has a documented partial-results-then-error contract if a later range becomes unavailable; it never returns terminal success on incomplete coverage.
4. Acknowledgement requires an accepted decision and the declared durable quorum, plus whatever visibility-install durability the selected design needs before that OK. “Acknowledged stays visible” means preserved in accepted history and selectable at an eligible cut, subject to later accepted updates/deletes and declared retention/drop rules; it does not mean an overwritten row must remain the latest row forever. Fresh reads carrying a successful write token cannot use a cut before that acceptance.
5. AdvanceEpoch is a Raft-ordered cut: pre-cut accepted decisions remain accepted; post-cut old-epoch Finalize is durably Refused. A repeated accepted parent is idempotent even after the epoch changes. Refused and unknown proposals never advance acceptance checkpoints, E_safe-as-acceptance, CDC, committed counters or replay-success records. A cut does not itself justify deleting ambiguous bytes.
6. Control decisions, row identity bindings and data-availability state must recover consistently. Missing/corrupt authoritative metadata fails closed rather than interpreting every missing entry as accepted. Fallback reads need the old accepted version retained until the new decision and retention rules permit its removal.

## Option A: reads at a closed finalized HLC frontier

### Definition, persistence and proof obligations

A frontier F for a scope proves: every admitted mutation with commit HLC <= F has an irreversible Accepted or Refused decision, and no future admission can introduce a new mutation at or below F. A simple maximum Finalize HLC proves neither. Before any child can become indexed, register its identity/HLC as a durable obligation, including a crash between append and pending-intent persistence. Close admission timestamps through an ordered range/epoch protocol, including clock rollback, holder replacement and delayed old-epoch operations. Persist the closed timestamp, unresolved obligations and terminal decisions with their Raft cut; publish F only after required data replicas have installed the corresponding visibility state.

Refused bytes with HLC <= F remain dangerous: an upper HLC bound alone includes them. Pure A therefore additionally requires atomic removal from logical read indexes (retaining accepted predecessors), with durable applied proof on every serving replica, **before** F passes them. Unknown work holds F back. If refused exclusion uses a per-parent lookup instead, this is the hybrid below, not pure A. E_safe may remain a highest-accepted progress metric; do not relabel it F without these stronger invariants.

Reads use a nonzero explicit upper bound <= F. The current zero/latest sentinel must first resolve to a valid cut, never slip through as unbounded latest. Historical reads use the requested HLC only when <= the closed frontier and inside retention. A closed historical snapshot then cannot gain a late low-HLC mutation. Multi-range statements acquire one common cut: minimum ready frontier across required ranges, with epoch/generation and replica-install evidence. A finalization advancing a frontier is not permission to publish partial children.

### Pending work and read-your-writes

A stuck low-HLC parent holds the frontier even if unrelated later writes commit. Give acquisition a declared wait budget; on expiry return a typed frontier-unavailable result or an explicitly requested older cut. There is no unconditional finite frontier-advance guarantee under permanent unknown work. Only a proved terminal resolution/epoch policy closes the gap; neither client cancellation nor an administrative timeout can label a possibly accepted write refused.

Writer OK needs its accepted HLC/decision token. Read-your-writes waits until all required frontiers cover that token, or returns unavailable at the read's bound. Own Finalize is necessary but not sufficient: an earlier unrelated hole can still block the writer. Returning commit HLC merely as LINEARIZABLE min_hlc would not fix the current unbounded read selection; both the lower requirement and an eligible upper cut are needed.

### Costs and consequences

At steady state, one cut check and existing HLC selection avoid a finality lookup per row. Costs move to durable admission registration, closed-timestamp coordination, replica installation, refusal cleanup and gap tracking. Every scope's slowest obligation causes staleness; a global frontier couples unrelated startup transactions, while per-range frontiers reduce coupling but make cross-range analytics pay for the slowest included range. Closed frontier lag must be reported separately from query latency and failures; fast stale reads are not fresh-read success.

CDC can publish accepted events before F advances only if its documented per-parent ordering permits that; a frontier-consistent CDC snapshot must wait for the matching cut. Observation records need pending obligations, F/cut and replica-ready state. Replay restores those states before admitting reads; snapshots must include them. Refused garbage can only be retired once old cuts/readers and recovery no longer need its identity, and published frontiers cannot be reversed after restart.

## Option B: staged logical versions with an accepted-parent decision index

### Read algorithm and correctness argument

Persist each logical version's exact parent identity and digest at append/index publication, initially staged. In the same Raft state-machine application that decides Finalize, durably maintain Accepted{decision_index, epoch, commit_hlc, child_digest} or Refused{decision_index, cause}; absence is Unknown. Acquire a fixed, validated accepted-decision cut K for a statement. A fresh/latest request requires a NEW Control Ring freshness barrier to establish K; no reviewed read-index/ensure-linearizable mechanism exists in the inventoried code. The existing LINEARIZABLE min-watermark check is not that barrier. A separate implementation review must choose read-index/quorum confirmation or a proved lease-based protocol, including leader changes, stale leaders, clock assumptions and the applied-decision catch-up condition. An explicitly stale request names its older cut. An arbitrary follower applied index is not evidence of freshness. Cut acquisition is bounded and fails unavailable on missing authority. For each key, walk newest-to-oldest eligible historical versions and accept only versions whose matching decision is Accepted at or before K; then interpret an accepted tombstone. Staged/refused tombstones do not suppress accepted predecessors. The primary-key, locality and secondary-index paths must locate those predecessors, not just filter the already-selected latest candidate.

For each accepted parent, require its complete child manifest installed on each serving range at K. Do not promote individual row bits in a way that makes a multi-row parent partially visible. Mutable promotion is safe only behind an atomic parent/cut publication, with crash-consistent readiness evidence. A control replica knowing acceptance but a data replica missing children produces unavailable, not an empty successful scan. Unknown application after lost ACK resolves from the authority; it never becomes success because of transport completion.

The argument is direct: eligibility implies exact Accepted at the cut; no Refused/Unknown version is eligible; parent-level admission plus child readiness gives atomicity; ack tokens require a cut containing the durable decision; retained predecessors make staged DELETE/UPDATE harmless. This is a proposed proof obligation, not a proof of existing code.

### Performance, caching and retention

Fresh K acquisition is also new read-path work. The accepted responder capture (`adr629-data-ring-responder-capture-1/REPORT.md`) isolated own apply-finish-marker to response-send-begin gaps of 46.194–1125.231 ms in 11 slow renewals, without proving notification queue residence or root cause. The earlier grant-path characterization (`adr629-p2-c-seed-lease-spike-analysis-1/REPORT.md`) reported nearest-rank p99 about 922.6 ms and a roughly 2.237 s maximum; that end-to-end tail is not the measured isolated responder interval. The reviewed Openraft 0.9.24 source/capture identifies a shared-core scheduling/response risk. A per-statement read-index/barrier through that same core can be delayed by that work; independence and a fast fresh-read bound have not been established. B removes unrelated pending-parent frontier blocking, but does not thereby prove better fresh-read availability than A. Both barrier wait and scan/replica readiness must fit the declared read deadline, with typed unavailable rather than stale-as-fresh success.

K may be amortized within the same statement. Reuse across statements is only an explicit bounded-stale contract: the caller opts in, validated cut age and epoch/generation remain within the declared window, and any read-your-writes token must still be covered. Restart, unknown age, expired window or an uncovered token prevents reuse. Fresh/latest reads cannot silently reuse a cached K to conceal the barrier tail. Such a lease/age check would itself require review; no new time assumption is selected here.

A naive implementation performs a decision lookup per candidate version. Start with batched unique-parent lookups per row group/request; expose counts for unique parents, candidate versions skipped, cache hits, misses and prior-version walks. A positive cache entry keys tenant/generation/incarnation/parent/content plus acceptance index and authoritative store generation. Accepted and Refused terminal facts are immutable, but cut eligibility still depends on K. Unknown is not terminal: negative caching needs revision invalidation or bounded revalidation. No positive cache reconstructed from an unverified physical row or a log proposal.

Compaction/tiering must preserve parent provenance and historical accepted predecessors. It may materialize accepted-state summaries as an acceleration only when tied to a durable decision cut and generation. Row-group stats, secondary indexes and Bloom filters must not leak staged values through results or eliminate accepted predecessors. Packed groups can contain parents in different states; physical group membership is not logical membership. Reclaim refused rows as background debt after pinned readers/recovery no longer require them; cleanup failure does not reopen visibility. Decision entries cannot simply expire with the bounded idempotency cache while surviving rows still depend on them. Safe pruning requires durable summaries/provenance, not a TTL guess. Before any soak, declare bytes per decision (encoded and resident), index/snapshot overhead, expected accepted/refused counts, pending backlog and capacity policy. At the provisional 100–300 requests/hour, 24 hours is 2400–7200 requests and 72 hours is 7200–21600, not necessarily the same number of mutations: freeze the mutation mix and maintenance producers and compute their decision counts separately. Current entry size is unmeasured; no storage budget or growth claim is made. Budget exhaustion must fail admission visibly, not evict proof still required by surviving rows.

### Replicas, historical reads and finalization-time meaning

The decision index and its applied cut belong in Control Ring snapshots and replay. Serving replicas prove their control state is at least K and their data view covers required accepted children. Independent replica caches must not authorize newer or foreign cuts. Restart blocks reads until decision state and row links are validated; pending bytes remain hidden and recoverable. A terminal response after restart must refer to the same accepted decision, not re-admit the write.

HLC-only as-of cannot promise an immutable snapshot if later Finalize accepts an older commit HLC. Recommend the first implementation contract be **latest-at-K only**: resolve one accepted-decision cut per statement and return the latest eligible accepted version at that cut. Public historical HLC-as-of requests return typed unsupported until a later reviewed contract defines them; they must not silently become latest or masquerade as stable snapshots. A later historical contract can use a statement token (K, H), where K is the accepted-decision cut and H the requested historical HLC. “Latest” resolves K once; H=latest is interpreted within that K. Pure historical-HLC immutability would additionally need A's closed-time mechanism or a reviewed change assigning visibility time at finalization. Do not silently redefine today's HLC API. Multi-range statements use one K (and H if a future historical mode is selected), propagate the cut through every await/retry/migration, and check readiness before returning data. Across independent future Control Rings, a global cut protocol would be an additional design; this study assumes the current shared Control Ring.

Read-your-writes carries the accepted decision index and parent token. The read waits for cut K >= token and data readiness; a full exchange/ack does not force unrelated earlier staged work to finalize. Own write visibility does not imply serializable transaction isolation or a total order on conflicting key writes. Accepted versions still obey a separately pinned conflict/HLC order; this study does not fix ADR629's other isolation or replay findings.

CDC should resolve against this same decision authority; refused parents must neither release nor advance checkpoints. Existing pending CDC release is a useful seam, not a proved shared authority. Observation must separately identify append/staged debt, accepted-decision installation and read-visible publication; its current sequence-stability cut is not itself acceptance. Payload/state digests for analytics must use the read cut and eligible logical versions; physical scans remain explicitly physical diagnostics.

## Option C: B as authority, closed frontiers/materialized visibility as acceleration

Use B's exact parent decisions as the safety authority. For older closed regions with no unresolved work, install a certified visible-only version index/row-group summary, tied to cut/generation. Reads in that region use a cheap immutable summary; the recent tail uses batched parent eligibility. A statement still has one explicit cut, and historical snapshot semantics remain documented. Refused cleanup cannot be inferred from F alone.

This avoids A's requirement that every unrelated gap block every fresh read, while offering an eventual cheap scan path. It adds promotion/compaction state and duplicate representations whose equivalence must be pinned. Do not implement C first to disguise unmeasured B overhead. An equivalent existing mechanism would need the same durable row-parent binding, exact decision membership and predecessor fallback; none was found in this inventory.

## Comparison and recommendation

| Dimension | A: closed frontier plus installed refusal exclusion | B: exact parent decisions at fixed cut | C: B plus certified summaries |
| --- | --- | --- | --- |
| Correctness foundation | Closed admission, resolved gaps, replica-visible install | Durable row-parent binding, acceptance index, child readiness | B plus proof that summaries equal eligible versions |
| Fresh reads behind unknown parent | Entire included scope may stall | Unknown skipped; prior accepted state remains readable | Same as B in tail |
| Per-read work | Cut acquisition + HLC version selection | New freshness barrier + unique-parent lookups + eligible predecessor selection | Summary path for closed prefix; B in tail |
| Read-your-writes after acceptance | Earlier gaps may hold F below own write | Wait for own decision/readiness at common cut | Same as B |
| Historical immutability | Closed HLC frontier supplies it | Deferred initially; later fixed (K,H), or extra closure mechanism | Explicit cuts; optional closed historical prefix |
| New mechanisms / durable state | Obligation registry, time closure, refusal exclusion/install | NEW freshness barrier; parent provenance, decision map, visibility readiness | All B plus summary/promotion certification |

**Recommend B for user selection**, with batched parent lookups and latest-at-K as the initial public read contract; consider C only after measuring B. It directly addresses the demonstrated staged INSERT/DELETE behavior and makes post-cut old-epoch refusal meaningful without requiring a new global closed-timestamp protocol. It costs real storage/index/read-path work; it is not a small predicate added after current latest-row selection. Scope it as a prerequisite spanning raw/table/secondary/tiered paths, then integrate epoch fencing and monotone leases before P3. A remains defensible if immutable HLC-only historical snapshots and cheap broad scans outweigh the cost of holding a closed frontier, but existing E_safe cannot supply it. The shared-core barrier tail prevents a measured fresh-read availability ranking until the declared comparison runs.

This recommendation does not claim transaction isolation, general OLTP readiness, or trust acceptance for customer data. The user chooses the contract before production implementation, including B's proposed historical-API deferral. The store's analytics can use the same fixed K across ranges without historical HLC support; CDC must still bind its acceptance checkpoint to that cut. Historical/public as-of API compatibility needs an explicit inventory before any narrower contract is committed.

## Fresh-only formats and epoch-fence integration

The lease-safety draft reserves WAL5/snapshot6 with refusal of old WAL4/snapshot5 on fresh clusters. Those versions are not implemented on this study's base. A selected plan must inventory every serialization ingress and data/index/header format: bumping Control Ring WAL/snapshot alone does not encode parent identity in slabs or persistent row indexes. Register all required versions before creating any new cluster. Old/mixed artifacts must fail before mutation, preserving bytes; no in-place migration, permissive default “accepted,” or retroactive parent binding from HLC. Fresh lease issuer/signing keys and generation namespace remain separate from TLS keys.

Raft AdvanceEpoch and Accepted/Refused decisions share one replayed order. Snapshot captures epoch plus decisions/frontiers/readiness references consistently; snapshot installation never makes staged rows accepted. Pre-cut accepted work survives; post-cut old-epoch proposals refuse; already accepted exact replay preserves its decision. Proposal matching and resolver code must inspect the durable decision. Pending and refused garbage remains recoverable/inspectable until safe reclamation, without charging committed counters. Key reuse after forgetting identity is not allowed to relabel surviving physical versions.

## Re-audit of ADR0542 and adjacent assumptions

- Phase0's “visibility once at parent Finalize” is invalidated for the tested raw/table/embedded SQL paths; parent Finalize is currently a response/Control Ring/CDC seam, not a universal read publication gate. The contract must be repaired, not edited to bless tentative visibility.
- Parent-batch durable quorum/finality as the commit unit is not disproved by early reads, but its read-atomicity consequence is unproved. Pin multi-locality partial durable groups and all-child visibility, not just one-row RF1.
- The current durable terminal-outcome implementation supersedes Phase0's historical statement that only a process-local response remains. That fact alone does not supply row visibility or tenant/content-safe replay; keep ADR629's characterization findings separate.
- Typed failures, pending-ledger durability and true-lifetime task permits are not disproved. However indeterminate work must remain hidden through caller cancellation/restart; a client deadline does not cancel or safely expose it.
- Existing CDC release after finalization is not shown broken by this pin. It can disagree with SQL's tentative view; a shared consistent snapshot/analytics claim is unavailable until both use a compatible cut. A rejected proposal must not be treated as successful CDC finality.
- Observation sequence stability tracks registered publications/debt, not accepted membership. A physical digest can be stable and still contain unfinalized bytes; finality certification and logical equality need separate acceptance checks. Maintenance copies must preserve provenance.
- Analytics totals over today's latest table scans can include tentative INSERTs or omit tentatively deleted rows even before a write ACK. Any assumption that read totals describe accepted store history is invalid for these paths. The independent client ledger does not change server visibility semantics.
- Public SQL ingress, multi-node behavior, DDL/materialization visibility, retained history, conflict isolation and every low-level read/export surface need explicit coverage; single-node SELECT evidence cannot settle them.

## Measurement plan (proposal only)

After contract selection and reviewed instrument: compare an exact current baseline, B correctness build and optional batched/cache variants on identical fresh synthetic fixtures. Declare all parameters/counts and freeze source/artifacts first. No real personal data. Use the existing A1/A2 synthetic customers/orders and INSERT/UPDATE/DELETE/replay shapes; resolve acknowledged, refused and indeterminate outcomes separately.

1. Startup profile: 100–300 requests/hour, declared bursts and idle gaps; interleave full/PK SELECTs and multi-range customer analytics. Report K-barrier latency (including leader/read-index or lease validation and apply catch-up), scan/lookup latency, replica readiness and total latency separately, with p50/p95/p99, sample counts, error/censored counts and uncertainty; do not infer a latency percentile from only completed operations.
2. A1/A2 diagnostic profile: fixed input sizes and mutation sequence, cold/warm caches, one/few/many parents per packed row group, unresolved latest versions and tombstones, plus two writers where coverage is actually supported. At least five interleaved paired runs per performance arm, release builds and identical machine conditions. Record throughput, per-query/lookup latency, unique-parent lookups, memory, storage amplification, predecessor walks and compaction costs; workload/data size is frozen at instrument review.
3. Inject one early unknown parent while later parents finish. Measure A frontier lag and read-your-writes wait versus B K-barrier latency, decision-cut/data-install lag and predecessor cost. Exercise the shared-core stall under concurrent writes and maintenance; do not subtract its tail out of fresh-read totals. If bounded-stale reuse is selected, measure it as a separate caller-declared arm and report actual cut age and read-your-writes eligibility. Staleness is separately (a) age of chosen cut and (b) distance from acknowledged/accepted operations to that cut. Clock differences are never subtracted across hosts without a separately declared bound. Historical cut correctness is compared by exact rows/digest.
4. Fault/restart profile: held append/pending/Finalize/replica install, epoch cut, lost reply, mixed/old formats, compaction and tiered reads. Report unavailable observations and partial stream errors. CPU/allocator/disk and memory growth accompany latency; a faster result missing accepted rows fails correctness, not performance.

No fixed overhead budget or claim of scalability is selected here. Required acceptance tolerances and fixture sizes come back with the implementation/instrument plan. Bounded unresolved work and cleanup debt need operator metrics/limits; neither option promises liveness through arbitrary partitions.

## Pins required before implementation acceptance

- Flip the accepted raw and table/SQL pre-Finalize pins: staged INSERT hidden; staged UPDATE preserves old row; staged DELETE preserves old row; writer still waits for matching acceptance. Use exact current pin paths plus public SQL and replica routes where declared.
- Raft cut in both orders: accepted pre-cut visible; post-cut old-epoch Finalize refused and forever invisible while bytes remain. Replay must not turn refused proposal into OK; lost reply resolves to the exact same terminal decision.
- Multi-child parent: hold one locality/replica install, no successful partial statement, no premature CDC; one common cut through streamed batches and retries. Separate explicit unavailable from falsely empty results.
- New freshness barrier: leader change, stale/isolated leader, follower behind K, timeout/stalled core and lost response; quorum/read-index versus any proposed lease alternative gets its own safety pins. Fresh requests never use an expired/stale cached cut; explicitly bounded-stale reuse honors age, epoch and write-token checks.
- Same-key accepted order: newer HLC accepted first, then lower HLC accepted later; reversed acceptance order; repeated exact parent; conflicting contents and DELETE/UPDATE. Pin selected latest-at-K conflict order separately; B must not be taken to settle ADR629 isolation findings.
- Read-your-writes with own token, a prior unrelated pending parent and a lagging replica; fresh/read-bound equality, stale old cut and finite wait/error policy. Conflicting accepted writes and historical version selection need separate pins.
- Initial latest-at-K contract: public HLC-as-of rejected with typed unsupported; no silent latest fallback. Eligibility still walks to prior accepted versions, including before staged/refused tombstones. If historical support is selected instead, historical (K,H) or selected closed-HLC semantics: later acceptance cannot mutate a pinned snapshot; zero/latest sentinel cannot bypass eligibility; older accepted tombstone and newer refused/staged versions choose correctly.
- Crash/reopen at append-before-intent, decision-before-index install, outcome-before-witness removal and compaction promotion. Recover Unknown hidden, Accepted complete, Refused hidden; missing metadata unavailable. Verify source bytes preserved on rejected formats.
- Replay/log/snapshot equality across decisions, epoch and visible logical digest; snapshot pruning retains necessary parent proof. Cache cold/warm/invalidation, negative-to-accepted transition, duplicate identity/content mismatch and tenant/generation separation.
- Compaction/L2/secondary/statistics/migration/materialization paths retain predecessors and exact parent provenance; refused cleanup interrupted/retried never exposes data. No dropping staged unknown bytes on mere timeout.
- Observation authority and CDC separately bind to acceptance/read cut; maintenance cannot invent accepted state; accepted/physical digests differ explicitly. Mandatory ingress inventory prevents trusted/internal raw paths from bypassing the contract.

## Open decisions for review and user selection

1. Select B, A or C; B is recommended. No implementation authority yet.
2. For B, recommend latest-at-K first, public historical HLC-as-of typed unsupported until a later ADR. Alternatively select explicit (accepted Raft cut, historical HLC) snapshots now, or require a separately designed closed-HLC-only history contract. Inventory public/internal as-of users before narrowing the API. Existing zero/latest behavior resolves a cut either way; prior accepted-version fallback remains mandatory even with historical reads deferred.
3. Approve the proposed shared Control Ring statement cut and completeness policy; streaming unavailable results end with an error, not successful truncation. No new transaction isolation claim.
4. Agree scope includes logical raw reads and all derived paths; physical diagnostics remain privileged and explicitly physical. Freeze exact backend/DDL coverage or retain a HOLD for uncovered bypasses.
5. Review implementation stages: authority/provenance and version inventory first; readers/replica readiness and crash pins second; epoch refusal integration third; performance gates before P3/campaign resumption. Separate per-stage packages; no code begins from this draft.


## User selection and delivery order (2026-10-05)

The user selected B with latest-at-K only for the initial public contract. Historical HLC-as-of requests become typed unsupported after the caller inventory is reviewed; no silent current/latest fallback. A fixed K covers the statement's included ranges and streamed batches. Incomplete coverage ends in an error, never successful truncation. All logical raw/table/derived read paths are in scope; privileged physical diagnostics remain explicit. This changes no isolation/conflict-order claim and does not declare store trust acceptance.

Selected delivery order, each with a review gate:

1. Authority/provenance and version inventory, plus the NEW freshness-barrier design choice. Its stage-1 draft comes back as a separate review package; this is planning, not production authorization.
2. Logical readers, common cut/replica readiness and crash pins; historical callers are inventoried before typed unsupported is installed. No claim that stage 1 alone fixes visibility.
3. Integrate Raft-ordered epoch refusal with durable Accepted/Refused decisions. Lease monotonicity is ordered with this safety prerequisite; exact replay preserves acceptance. No P3 availability work before the combined safety pins pass.
4. Performance/capacity gates, then separately reviewed P3 and campaign resumption. Bounds and freeze require a later instrument review.

No stage begins production changes from this selection record alone. Claude reviews each concrete package; GitHub push and main fast-forward beyond this plan record are not authorized. ADR629 layouts 2–4, A3, C1/F16/F17 and F9 remain held alongside lease-safety and P3.

## S1c implementation and R0 cost results (reviewed)

S1c closes the R0 per-entry CPU gate for the measured cases; the snapshot memory gate at X remains failed. This is a dormant reducer implementation and a bounded cost result, not completed read visibility, an operating-ceiling selection or store-trust acceptance. Main fast-forward and push remain held. R2/R3 durable storage and Raft integration, lease-safety, P3 availability work and ADR629 campaign resumption are not authorized by this record.

### Remaining gates

- **X snapshot encode still fails the memory gate:** 523.352 MiB additional requested live allocation exceeds the frozen 512 MiB bound, unchanged from the original R0 assay. This measures allocation above the retained input baseline, not installed-state RSS or total process memory.
- **Snapshot codec and full validation remain costly, with slower large-state observations:** X encode elapsed median was 1111.710 -> 1459.596 ms; full-validation elapsed maximum was 433.097 -> 790.220 ms. Decode elapsed maximum was 1026.986 -> 1220.124 ms. These are separate seven-process samples, not paired observations or a causal decomposition. The snapshot cadence and memory gates must be reviewed before R3; the reducer result does not waive them.
- **Complete rate profiles remain unavailable** for E, H, R, P32768, B and X because required semantic arms are absent. Renewal-only arithmetic cannot turn these into profile passes.
- **No production occupancy ceiling is selected:** frozen `supported_cases` remains empty. All case/profile gates are reported independently.
- **L1/K1/R7 remains open:** retained closed lineages still consume the bounded lineage inventory. S1c changes neither the encoded capacities nor the retention policy. This is a multi-day availability prerequisite before dogfooding, regardless of the improved CPU cost.
- The assay contains no NewGrant/Cut latency arm, installed RSS measurement, arbitrary retained-root/last-root-drop measurement or durable I/O/Raft integration measurement.

### Implementation and validation

Dormant source: initial S1c `f8bd00119aa165b2e8e88a98615d52b9d655502d`, followed by signed corrections `b44d017f0b01b993905eab1b0759d715d6d1fd5f`, tree `09f07c7aed5bc9dad4812046ebbd3aebdebcb544`. The implementation retains the existing canonical bytes and public contract while using persistent copy-on-write collections and incremental trusted-state checks. Full validation stays at the untrusted decode boundary.

The follow-up moves immutable Cut/Release/Revoke checks before mutation, pins the Reserve bootstrap bound, preserves multi-defect decoder error precedence and extends legacy/new reducer equivalence and no-full-walk guards. The oracle retains its prior reducer bodies; shared primitive codecs and trust fixtures limit independence. The guard is a source/AST check with artifact corroboration, not a compiler-resolution proof.

Pinned Rust 1.95.0 ladder `s1c-ladder-2`, clean signed source at start/end: Wendelon 2202 passed / 0 failed / 13 ignored; Wendelon-client 13/0/0; Toron 5729/0/25; TPC-H 674/0/6; Toron-client 24/0/0. Visibility debug/release checks each 97/0/3; boundary checks 4/0/0. Formatting and release-library checks passed. Shared warmed target and a separate serial strict TPC-H warmup are disclosed; earlier development failures and source-bound prechecks are retained. Ordinary-library compile-out evidence covers test oracle/assay hooks, with source checks as the primary evidence.

All 108 original construction fixture/arm/omission identities agree in order. The original assay adaptation is fixture/model construction only; the r1-to-r2 instrument interdiff is empty. No timed arm body changed in the corrective follow-up; reducer work remains within its measured call.

### Frozen rerun and comparison

Claude cleared the implementation and launch via chatd `01a1144b-5138-70d5-9335-238c0ac45b45` and accepted the measurement via `01a11454-bd97-76fd-a55e-4dfe5c440273`. Freeze `ae50178f8cd613acae3e8163e4ee09c3542e03273186993917ddbfaaa97062e5` preserves the original R0 cases, arm inputs and omissions, seven repetitions, process order, rate profiles, proposed gates and external watchdogs. Only source/artifact/provenance bindings changed. Exact release executable SHA256: `b390490e44b53561942fcc4490561b7261e0783822ccd9477502a1f4615f669b`.

63/63 fresh sequential processes completed with rc 0. Readout: READ, reasons []; no STOP/ACTIVE, every owned process-group cleanup verified. No replacement, rerun or tuning occurred. The recorded one-second launch inspection gave 97.205% idle CPU and found no build/storage workload process; normal desktop/system services remained. Postflight found no assay/build/storage workload. Evidence was copied back and checked against the remote listing, and the final retained manifest verifies on both hosts.

CPU-time medians below are milliseconds, from the same frozen seven-process distributions:

| Case / arm | Original R0 | S1c |
| --- | ---: | ---: |
| P4096 renewal | 9.529150 | 0.003851 |
| P4096 cover | 10.477740 | 0.003001 |
| P4096 reserve | 9.675340 | 0.016004 |
| P4096 finalize | 9.595876 | 0.010727 |
| P32768 renewal | 78.066727 | 0.007004 |
| X renewal | 446.800267 | 0.013727 |

At P4096, frozen Deploy-P3 modeled demand is median 0.037752%, maximum 0.049531% of one CPU core; Deploy-current is median 0.074721%, maximum 0.098251%. These are frozen profile arithmetic from process CPU observations, not achieved cluster throughput. The P3 profile uses the approved 2 s target (96 renewals/s at the stated 192-range model); the separate 2.5 s sensitivity assumption selects no policy.

All nine cases meet the unchanged 50 ms measured-reducer and 2 s encode/decode elapsed gates. P1/P256/P4096 have complete coverage for all six frozen profiles and meet the 25% one-core profile gate. Other cases' profile coverage is unavailable as listed above. The allocation gate is met for every case except X. These outcomes do not infer NewGrant/Cut costs or authorize arbitrary full-state occupancy.

### L1 capacity finding and provenance

The unchanged lineage cap is 16,384. In the conditional fixed-roster model of 192 fresh lineage grants per complete generation, 85 complete generations fit (including the initial generation), then 64 slots remain; this permits 84 complete subsequent generations before the next one cannot fill all 192 ranges. Cut itself is not prevented solely by this count. Actual eviction/rejoin adapters may change ranges and grants per epoch; no live authority2 membership campaign established that mapping. These are code-derived arithmetic limits, not a measured lifetime. R7 must resolve the retained-lineage policy before production multi-day use.

Evidence roots:

- Workstation: `~/.agent-bridge/wendelon-adr0100-s1c-implementation-r2/` (source, signature, ladder, equivalence/guard map, immutable construction freeze and disclosures); SHA256SUMS `f3e1078185f81f0840cd5d20c5bf6db43838600187d005c8e8540f45ad1237dd` (151 entries).
- Workstation: `~/.agent-bridge/wendelon-adr0100-r0-cost-s1c-2/`; bigfoot: `~/work/toron-evidence/adr0100/r0-cost-s1c-2/`. SHA256SUMS `0f38893ca2830e612a1de7a82064a42250ce38d04fa8343576feb7761c3a3ed3` (273 entries).
- Rerun `readout.json`: `3ef82b7d324fe824ced87d847c0692ab8e523fd856906c330350dd3059d1ce8e`.
- `comparison.json`: `e99985dda91f84a316c69de82dcb8d11d867a0fb750a85c6b25ba92f0d2f10a6`, retaining each case/arm sample, min/median/max, allocation metric, unavailable arm and profile/gate comparison against original `r0-cost-1`.

Original packages, failed development steps and original R0 evidence remain separate and unchanged. This record selects no new implementation scope, deadline, rate policy, capacity or production activation.
