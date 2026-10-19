# ADR 629 - trust in acknowledged writes: a three-node store soak

**Status:** STAGE A A1 CHARACTERIZED and A2 REVIEWED INSTRUMENT DEVELOPMENT (limited diagnostic coverage).
A3 is HELD on P1–P4: authenticated reachable storage route, non-destructive finality observation, stored-effect
concurrency coverage where claimed, and real three-node readiness/placement/ingress checks. Stage B P1/P2
scope is reviewed; P1 is delivered as the bounded implementation recorded below, without host readiness
or staging acceptance. P2 coverage inventory and least-privilege actor design require review before P2
production code. Stage C remains separately reviewed.
This is not trust, durability or security acceptance for a store.

Selected-contract prerequisite: [Wendelon ADR 0100 — parent finalization and read visibility](../../../wendelon/plans/adr/0100_parent_finalization_and_read_visibility.md).
The user selected B (staged versions with accepted-parent decisions), latest-at-K only initially.
Its stage plans and implementations require separate review; lease-safety, P3, the campaign and A3 remain HELD.
This prerequisite belongs to Wendelon storage semantics, outside ADR 629 Stage B, and authorizes no code.

**A correctness, integrity and security-evidence record. Wi-Fi is a supported target. No performance claim
is made.**

## The question

Before any real customer data goes in, the user wants to dogfood Toron with a small web store. The question
is whether a three-node Toron + Wendelon cluster, under startup-scale traffic and for days, can hold a small
store's data with these properties:
- acknowledged writes are not lost or altered;
- no operation applies twice;
- related rows stay consistent;
- each tested security boundary holds.

This record does three things:
- it states that property as a TARGET contract;
- it characterizes today's behaviour against that target;
- it stages the work needed to earn evidence for it.

**What evidence can and cannot show.** A clean campaign is evidence about the paths it exercised, under its
declared faults. It cannot establish "no loss" or "no security issues" in general. **No real customer data
is stored until** authentication, authorization, storage and recovery contracts are established and tested.
Every campaign uses synthetic data.

## Context

### What exists (cited; read at main 379dab14)

**SQL writes.** CREATE TABLE, INSERT, UPDATE, DELETE and DROP TABLE are accepted, one statement per request
(`toron/src/sql_planner.rs`).
- UPDATE requires a primary-key table. It reads the matching rows through the distributed scheduler, then
  rewrites whole rows through the INSERT writer (`toron/src/coordinator.rs`).
- Every write goes from the coordinator to Wendelon (`toron/src/daemon.rs`, `toron/src/storage.rs`).

**Durability lives in Wendelon:**
- Raft log with fsync, snapshots and crash recovery;
- group commit, and `fdatasync` on data writes;
- recovery rules in `wendelon/plans/ops/durability.md`.

A table batch becomes visible once, after every required locality group is durable on a quorum
(`toron/plans/impl/0542_write_path_progress_bounds.md`, "Grounded commit model"). Toron nodes keep no table
data.

**Operation identity.** A SQL mutation's Wendelon `ClientWriteId` is derived from the statement's
`QueryId`: `client_uuid` is the QueryId bytes, `sequence_no` is 0, and `idempotency_ttl_ms` is 300 000
(`toron/src/coordinator.rs:4053-4062`, and its callers at :865, :1212, :1249, :1399, :1436, :4314, :4510).

**Outcome resolution.** `ToronClient::resolve_mutation(query_id)` returns `ResolvedMutationOutcome`
(`toron/src/storage.rs:108-123`), which has a `resolution` (Present | Expired | Unknown) and three
INDEPENDENT numeric axes:
- `write_status`;
- `stop_reason`;
- `commit_knowledge`.

It also returns optional `delete_counts`. **Present means a durable outcome record exists; by itself it does
not certify that the write committed.**

**Outcome retention** (`wendelon/src/network/mutation_outcomes.rs`):
- An outcome's lifetime is its write id's TTL, clamped to 1 000–300 000 ms (`:51`). SQL mutations use
  300 000.
- Capacity pruning (`OUTCOME_CAPACITY` = 4 096) evicts the oldest UNPINNED outcomes; pinned outcomes are
  excluded (`:349-395`).
- Pins are bounded at 1 024.
- Expired tombstones are bounded at 4 096 and kept for 15 min.

The effective lifetime and eviction path of each SQL operation is a Stage A finding, not assumed.

**Read snapshot.** A SQL request runs with `HlcBound(0)` (`toron/src/daemon.rs:1991`). The storage scan sets
an explicit read bound only when it is greater than 0 (`toron/src/storage.rs:972`). No SQL syntax for a
client-held or cross-request snapshot was found. Stage A A1 confirms this; nothing here assumes snapshot
support.

**Serving:**
- The protocol is custom: postcard frames over QUIC (not Postgres wire), one statement per connection.
- The SQL listener uses mTLS on non-loopback addresses, with the tenant taken from the peer certificate.
- `toron start` runs as a long-lived daemon.

**Entry points named in config** (`toron/src/config.rs`):
- the SQL listener (`[sql] listener_bind`);
- the worker task listener (`[worker] task_listener_bind`);
- the Wendelon client bind;
- the health, ready and metrics endpoints.

Wendelon nodes have their own data and Raft listeners. Each has its own intended peer policy, which A1
enumerates.

### Gaps against the target (code-established unless marked)

- **G1. No multi-statement transactions.** BEGIN, COMMIT and ROLLBACK are rejected.
- **G2. No primary-key uniqueness.** PK tables keep a version chain per key, and reads return the latest
  (`wendelon/plans/adr/0050_pk_delete_supersession.md`, Context).
  - An INSERT of an existing key is therefore expected to supersede silently. That is INFERRED; no test pins
    it.
  - There is no unique constraint on other columns, and no `ON CONFLICT`.
- **G3. The data-server dedup caches have no eviction.** `idempotency` and `table_delete_idempotency` are
  DashMaps (`wendelon/src/network/data_server.rs:741,745`).
  - They are inserted into on writes (`:8787, :9305, :9571, :10088`; `data_server/mutation_protocol.rs:206`).
  - No `remove`, `retain` or `clear` was found in `wendelon/src`.
- **G4. The connection task set is emptied only at shutdown.** One shared `JoinSet` holds every connection
  task (`toron/src/daemon.rs:316`) and is joined only in shutdown (`:536, :547`). Retaining finished tasks
  until then is INFERRED from Tokio's semantics.
- **G5. Default admission.** With no `[tenant_budgets]`, concurrency = worker memory / 64 MiB
  (`toron/src/distributed/budget.rs`, `omitted_config` / `derive_max_concurrent_queries`). The example's
  64 MiB budget gives 1, with no queue.
- **G6. Replication.**
  - Wendelon defaults to factor 1.
  - Toron's bootstrap status hard-codes `replication_factor: 1` (`toron/src/daemon.rs:2503`).
  - Actual placement and quorum are a readiness question (A3), not a fact.
- **G7. Never run in this shape.** All multi-host Toron runs (ADRs 624–628) used `toron_fixture_worker`
  with fixture storage. Their PKI and authentication say nothing about a Wendelon-backed deployment.
- **G8. A write-path defect remains unresolved.** ADR 0542 acceptance 8 is UNRESOLVED
  (`toron/plans/impl/0542_acceptance8_control_arm_r3_evidence.md`). The original symptom was writes accepted
  but later unreadable (`polariton_soak_findings_20260807.md`).
- **G9. Point lookups scan.** A PK-equality lookup is a distributed scan. This is context only.

## The target contract (under characterization, not asserted)

These are the properties the campaigns test for. Where Stage A finds today's behaviour differs, the
difference is recorded as a FINDING. The expectation is never silently amended to match.

**Three separate properties:**
1. **Durable commit.** An operation that committed is retained, through the declared faults, under the
   declared fault model (below).
2. **Read visibility and consistency.** A "later read" is a read whose request begins after the client
   received the operation's acknowledgement. The level tested is a FINDING to establish, per entry node:
   whether a later read from ANY node observes the committed effect. Read-your-writes and monotonic reads
   are measured, not assumed. "Every read from any node" is a candidate contract, not a fact.
3. **Logical final state.** At a consistency cut (below), each table equals the EXPECTED state. That state
   is computed from:
   - every committed operation, including committed unknowns once resolved;
   - each operation's declared semantics (insert, supersede, update, delete);
   - the permitted orderings of concurrent operations.

   Response arrival order is not commit order.

**Once-only.** A re-submission with the same QueryId and identical request must not apply twice. Final
state alone cannot prove once-only: superseding inserts and idempotent assignments can hide duplicates. So
once-only is claimed only for operations with a declared WITNESS (below). Otherwise it is reported as
coverage unavailable.

**Fault model, declared per campaign:**
- replica placement and quorum size per table, as observed at readiness;
- persisted disks: no injected loss of fsynced data;
- which failures may overlap: at most one node down at a time unless a campaign declares otherwise;
- clock assumptions;
- Wi-Fi.

"Survives within the replication factor" is not used without that declaration.

## Outcome classification

Every client operation ends in exactly one class:

| Class | Requires |
| --- | --- |
| `acked` | the client received a success response; the terminal-exchange meaning is pinned in A1 |
| `rejected` | ONLY a typed outcome that establishes no effect (a typed rejection response, or a resolved outcome, including a Present one, whose axes establish no effect, as pinned in A1) |
| `committed (resolved)` | a resolved outcome whose axes establish commit |
| `ambiguous` | everything else: a transport loss or timeout, an error after possible commit, Unknown, Expired, or Present whose axes establish neither commit nor no-effect (including unrecognized codes) |

**Rules:**
- An ambiguous operation stays ambiguous unless a declared witness settles it. **Unknown or Expired is never
  proof of absence.**
- Every raw axis value is retained, and unrecognized codes stay ambiguous.
- The original client observation (success, error or no response) is kept, along with the full resolution
  history. Resolving an ambiguity appends evidence; it never overwrites it.

**Witnesses** are declared per operation kind and frozen in A2. The candidates below are to be shown sound,
or the operation's once-only and outcome claims are withheld:
- **INSERT of a new key:** the row's `op_id` column is bound to the operation's QueryId.
  - Presence of the bound `op_id` with exactly the expected content can witness the observed effect.
  - Absence alone, including after the resolution window closes, does NOT prove not-committed. Expiry or
    eviction of an outcome record establishes none of these: that the operation never committed, that an
    already-dispatched mutation can no longer commit, or that a scan sees all committed data.
  - A negative witness requires all of: independently established mutation finality (no future commit),
    the declared read-consistency coverage, and no later removal. Otherwise the operation stays ambiguous.
  - These premises are pinned, or the negative claim is withheld. An elapsed TTL is never treated as a
    cancellation fence.
- **UPDATE and DELETE:** content cannot decide them once a later operation touches the key.
  - The baseline workload therefore SERIALIZES further mutations of a key while an earlier mutation of that
    key is ambiguous. This keeps a witness available; it is not evidence about a key once its witness is
    gone.
  - Any remaining ambiguity is reported as coverage unavailable.
- **Non-idempotent business events** (an order or a charge): each is created once, under a business key
  with a unique `op_id`. Once-only is witnessed only through a declared check: exactly one row bearing that
  business key's history. That check is only valid if G2's real behaviour (A1) permits detecting a second
  application.

## The independent ledger (oracle)

The ledger lives on the client host, outside every Toron and Wendelon root, and is append-only. Its records
are fsynced in this order:
1. **Intent, BEFORE sending:**
   - tenant and QueryId;
   - operation kind and business key;
   - the exact request (SQL text and parameters) or its canonical hash plus every required input;
   - the expected row content hash;
   - the invocation time.
2. **The typed outcome on response:** the class, every raw axis, the error text and the response time.
3. **Each later resolution attempt and its result.**

**Restart rule.** After a generator restart, any intent with no terminal record is `ambiguous`. It is NEVER
re-submitted under a new identity. A re-submission under the same QueryId, with the identical request, is
allowed only as a declared retry-pin action.

**Content hash.** A deterministic encoding: schema version, fixed field order, explicit null markers and
canonical scalar encodings. It is specified and negative-tested in A2.

**History.** The ledger keeps each key's full operation history, including expected supersessions and
deletions. Expected state is derived from that history, and invocation and response intervals bound the
permitted orderings. Concurrent updates have their allowed outcomes declared independently; lost-update
behaviour is a finding, and serializability is not assumed.

## The consistency cut and reconciliation

No cross-request snapshot is assumed (see Read snapshot). Unless A1 establishes a supported cross-table
snapshot, reconciliation uses a CHECKED QUIESCENT INTERVAL:
1. Stop new load.
2. Resolve every resolvable ambiguity, recording each attempt.
3. Establish quiescence on BOTH sides:
   - the client ledger has no operation outstanding;
   - the server and storage mutation path has no pending work that could still publish. A request that
     timed out or disconnected, or whose resolution record expired, can still leave work pending.
4. Export every table through Toron, in sequence.
5. Re-verify quiescence on both sides. Re-checking that nothing new was admitted rules out new requests
   only; it does not rule out an old one completing.
6. Compare the export against expected state.

**Withholding.** If any operation could still publish, or its finality or quiescence cannot be observed or
fenced, the consistent-cut and oracle verdicts are WITHHELD and the unresolved operation is reported. The
evidence points that establish server-side quiescence and finality are defined at A2 review. No time-only
assumption is accepted.

**The cut governs both reconciliation and the analytics oracle.** Analytics queries run inside the same
quiescent interval, and their expected totals are computed from the same expected state. An analytics query
run under live load is reported as observed, with no oracle claim.

**Hard failures at a cut:**
- an unexpected absence or change relative to the history-derived logical state at the cut, after
  accounting for allowed committed supersessions and deletes and for the permitted concurrent histories;
- a row with no ledger origin;
- a witnessed double application;
- a referential break under the frozen delete policy;
- an analytics total that differs at the cut.

Ambiguities that cannot be witnessed are counted and reported as coverage unavailable, never as passes.

**Relationships.** Acknowledging parents before children does not protect against later deletes or
concurrent edits. A2 freezes the policy:
- customer deletion is a soft-delete (`state = deleted`, with personal fields scrubbed), with no cascading;
- orders and tickets keep their references;
- the declared races (deleting a customer while an order for it is in flight; two concurrent edits) are
  pinned as findings.

G1 remains unresolved for multi-row invariants. The single-row order/license/payment schema does not close
it.

## The store workload (frozen in A2)

**Data.** A synthetic desktop-software store. Every personal field carries a detectable planted marker.

**Schema.** Every table has a single-column primary key, plus `op_id` and `content_hash`.
- `customers(id, email, name, address, country, state, created_at, op_id, content_hash)`
- `orders(id, customer_id, product, amount_cents, currency, status, license_key, license_state,
  payment_ref, created_at, op_id, content_hash)`
- `support_tickets(id, customer_id, order_id, subject, state, updated_at, op_id, content_hash)`

**Atomicity.** The user selected option (b): an order carries its license and payment state in one row,
so each business event is one statement.

**Traffic.** Startup scale, declared as parameters:
- about 100–300 requests/hour on a daytime curve, with bursts of a few dozen;
- deliberate quiet stretches;
- the mix: signups, orders, license activations, refunds, profile edits, soft-deletes, ticket churn and
  point reads;
- a declared rate of concurrent same-key updates;
- analytics every 3–4 hours, inside a quiescent cut.

## Security evidence (enumerated paths, not a general claim)

**Entry points.** A1 enumerates every entry point, each with its intended peer and tenant policy, a positive
control and its rejection tests:
- the SQL listener;
- the worker task listener;
- health, ready and metrics;
- the Wendelon client, data and Raft listeners.

The tenant-client rejection rule applies to the SQL path. Worker and Wendelon peers are tested against
THEIR membership policy, not the tenant rule.

**Sinks.**
- **Retained or returned ON PURPOSE:**
  - synthetic data in the ledger, table exports and oracle files;
  - authorized response payloads (SELECT and export responses necessarily carry the planted fields).

  These are declared together with their intended recipient and access policy. A normal authorized data
  response is not itself a leak.
- **Operational sinks, inventoried per host:**
  - logs;
  - traces;
  - audit segments;
  - metrics;
  - error and response text;
  - crash output.

  They are scanned for planted markers, as are any unauthorized responses and unexpected metadata. A hit
  fails the campaign. The full per-host and per-entry-point sink inventory is kept visible, and any sink
  that is not scanned is reported as missing coverage.

**At rest.** A declared inventory of what is encrypted on disk and what is not. No claim goes beyond it.

**Authority.** No firewall, service, production-semantics or configuration-default change is authorized by
a passing pin or by this ADR.

## Stages

### Stage A (authorized by this plan's commit once reviewed)

**A1. Local deterministic characterization** (bigfoot ladders; tests only; no production change). Each item
pins today's behaviour as a finding:
- **G2:** a duplicate-key INSERT (what it returns, and what stays visible).
- **INSERT, UPDATE and DELETE retries:**
  - the same QueryId with the identical request;
  - the same QueryId with different contents;
  - each against the typed outcome.
- **Resolution:** across a coordinator restart, a Wendelon leader change, TTL expiry and capacity pruning.
  Decode the raw axes. Pin the client's success-acknowledgement meaning and the terminal exchange.
- **Identity binding:** tenant + QueryId + the exact request.
- **Concurrency:** a same-key UPDATE racing an UPDATE, and a DELETE racing an UPDATE.
- **G3 and G4:** entry growth per statement.
- **Snapshot and visibility:** cross-request snapshot support, and the read visibility seen from another
  coordinator.
- **Entry points:** the enumeration and each one's peer policy.

**A2. Instrument and topology review** (its own Codex review). It covers:
- the load generator, the ledger, the content-hash encoding, the classifier and the witnesses;
- the quiescent-cut reconciler, the marker scanner and sink inventory, and the resource sampler (per process:
  RSS, fds, slab files, disk);
- negative tests for each of the above;
- the real-daemon topology and configuration: RF, admission, mTLS for each entry point, and the per-host
  manifest;
- the frozen workload and the delete policy.

**A3. Readiness, then a diagnostic** (its own Codex review). This runs only after A2 is cleared:
- **readiness gates:** authenticated three-host bring-up; observed placement and quorum per table; every
  entry point's positive control and rejection tests;
- **then a frozen 2-hour no-injected-fault DIAGNOSTIC baseline,** with every measurement and failure
  threshold declared in advance.

The diagnostic is not a 24-hour acceptance campaign.

### Stage B (unselected)

These are candidates only, each its own ADR, chosen after Stage A:
- PK uniqueness or ON CONFLICT (G2);
- dedup-cache eviction (G3);
- reaping connection tasks (G4);
- RF plumbing (G6);
- the G8 acceptance.

### Stage C (separately reviewed)

1. A 24-hour undisturbed baseline.
2. Faults, one per identified run:
   - kill -9 of a Toron node, and of a Wendelon node, mid-write;
   - a clean restart of each host;
   - a full-cluster restart;
   - a Wi-Fi blackout;
   - disk full, on an ISOLATED test volume only.
3. 72 hours, after a clean 24 hours.

A failed campaign is retained unchanged. Its gap gets its own ADR, and the next campaign runs under a new
identity.

## Guardrails

- Synthetic data only.
- Build and test on bigfoot in isolated ladder worktrees.
- Every cited figure is read from its artifact.
- Pin findings stay findings: no silent expectation changes, and no production fix within Stage A.
- Disk-full tests only on a dedicated test volume.
- A clean campaign is evidence for its exercised paths and declared faults only.

## Not in this ADR

- Multi-statement transactions (G1). They are recorded as a precondition for real data with multi-row
  invariants.
- Postgres wire, ORMs, connection reuse, indexed point lookups, and performance.
- The web store application.


## A1 findings (reviewed)

A1 characterizes today's behaviour; it delivers no production fix. Claude independently reviewed the three
parts in chatd: `01a103c5` (part 1), `01a103e0` (part 2), `01a103e2` (F10 correction), and `01a1042b`
(part 3). The results below are read from retained ladders, with code explanations separated from observations.

### Scope and evidence layers

SQL pins use the real ToronDaemon ingress and public ToronClient. Their Wendelon fixture is one live,
disk-backed node, RF1, with simulated storage transport, on bigfoot. Parts 1/2 use configured tenants and
loopback SQL without mTLS. F9's part 3 extension uses authenticated public SQL, a common CA, and client-1 /
client-2 certificates. The second daemon still has node fallback tenant 1: its client certificate supplies
tenant 2. Both coordinators share the Wendelon client object and storage process. No three-host placement,
replicated durability, storage-process crash or power-loss result is established by this fixture.

Native cache, finalizer and object/server-reopen tests are separate layers. The daemon connection pin is
cfg(test); client completion pins exercise the response collector. Existing Wendelon QUIC/TCP integration
tests exercise their own real transport fixtures; they do not upgrade the SQL storage fixture to that topology.
All data are synthetic. A successful test means the observed current behaviour matched its characterization,
not that the target contract holds.

### F1–F15

| Finding | Observation | Limit / consequence |
| --- | --- | --- |
| **F1 (G2): duplicate PK** | Different-id INSERTs for one key both succeed; the latest content supersedes the first. | Latest-row presence cannot count applications or preserve business-event history. |
| **F2: exact-request binding** | Changed SQL under an existing QueryId is not detected. INSERT/UPDATE return row_count 1/1/1 and equal batch vectors for original/identical/changed requests; changed content is not applied. Resolutions remain equal to the original. | The server response does not certify application of changed bytes. Bind exact request bytes locally before send. |
| **F3: DELETE response versus effect** | Original/identical/changed replay responses have rows 1/0/1 with equal batch vectors; durable delete_counts remain [1,0,0]. Changed DELETE leaves its new target present. | Empty fresh candidates short-circuit to zero; nonempty candidates can return the cached original deletion count. Affected-rows is not an effect witness. |
| **F4: isolated UPDATE replay** | Original increment changes 10 to 11; identical retry stays 11; changed assignment to 99 also stays 11. | Positive once-only increment witness for these isolated attempts only, not concurrent-update correctness. |
| **F5: coordinator restart** | INSERT resolves Present/0/0/1 before/after Toron restart; exact row remains. Strengthened in part 2: first/replay rows 1/1 and batch vectors equal. | The same Wendelon client object and storage process survive; INSERT only. Not independent cold-client or storage restart evidence. |
| **F6 (G3): cache entries** | Four new write/delete pairs measure write-cache/delete-cache lengths (2,1), (4,2), (6,3), (8,4). Replays add no entries. Native write replay changes OK(0) to DUPLICATE(3), preserving NONE/COMMITTED. | Entry counts only; no RSS, byte cost, long-run slope or cache eviction evidence. |
| **F7 (G4): connection entries** | Eight SQL health requests leave eight retained JoinSet entries; all eight are joined successfully and the set becomes empty. | Completion is established when drained, not asserted at the preceding count. No memory-cost claim. |
| **F8: client success** | Collector returns success on Complete while the writer stays open; EOF without Complete errors. | The public client's later bounded terminal-read result is ignored in code; that public fault path is not injected here. |
| **F9: cross-tenant id collision** | Both tenant orders × INSERT-identical/changed, UPDATE-identical and DELETE-identical produce second-tenant SQL success row_count 1 while its intended effect is absent and its resolution is Unknown/23/0/0. All eight cases also reproduce through certificate-derived authenticated public SQL in part 3. | Tenant-isolation integrity defect on the declared paths. Authentication does not tenant-scope the dedup cache. No row-data disclosure observed; no deployed LAN/security acceptance. A Stage B candidate and precondition for multi-tenant or real data. |
| **F10: native TTL/reopen split (amended)** | TTL1000ms: resolution becomes Expired/22/0/3, while the populated warm cache returns DUPLICATE/3/0/1. After object/server reopen on the same disk directory, cache is empty; replay returns 22/0/3 without byte growth (4096→4096), and the exact original payload remains readable. | The native pin's id is non-v7 ([0x44;16]); cold refusal is backed by retained expiry evidence. Tombstone eviction is not exercised. Expired remains ambiguous. |
| **F11: no-effect layer witness** | A synthetic BACKPRESSURED finalizer result becomes Present/21/3/2, retained after object/server reopen; slab bytes stay zero. | KNOWN_NOT_COMMITTED at the finalizer layer only. No public SQL no-effect mutation is driven through this path. |
| **F12: second coordinator** | A second Toron daemon reads the exact acknowledged row. | Same Wendelon client object and storage process; not independent clients or replicated placement. |
| **F13: old-v7 first SQL submission** | A v7 id dated 360000ms old on first submission, SQL TTL300000ms, returns Remote StorageError; row absent; resolution Expired/22/0/0. | SQL maps Expired to UNSPECIFIED(0), distinct from native INDETERMINATE(3). Neither is KNOWN_NOT_COMMITTED. No previously admitted work is tested. |
| **F14: cross-request read** | Reusing one SELECT QueryId returns exact decoded amount 10, then 11 after an acknowledged UPDATE. | No held snapshot on this request path. Does not characterize one multi-table SELECT during concurrent writes. |
| **F15: SQL outcome capacity** | 4097 distinct public SQL mutations complete in 59573ms, before TTL: oldest resolution changes Present→Expired/22/0/0; latest is Present. Warm oldest replay still succeeds with rows1; latest amount4096 stays intact, and oldest resolution stays Expired. | Detailed resolution can disappear under this workload while replay cache remains effective. No application count is inferred from latest-row state. Capacity is not a universal one-minute lifetime. |

The cache key omits tenant (`data_server.rs::idempotency_key_for`), while the durable outcome store is
tenant-scoped. That code mechanism is consistent with F9; the pin establishes the acknowledgement/effect/
resolution discrepancy on its declared path. Preserve all three observations: a received success is `acked`,
then reconciliation finds unexpected absence. Do not retrospectively relabel that acknowledgement as rejection.

### Typed axes and clock context

Keep resolution classification, write status, stop reason and commit knowledge independent:

| Raw axes (status / stop / knowledge) | Meaning / layer |
| --- | --- |
| 0 / 0 / 1 | OK / NONE / COMMITTED; Present SQL resolutions in successful original mutations. |
| 3 / 0 / 1 | DUPLICATE / NONE / COMMITTED; native cached replay. |
| 21 / 3 / 2 | WRITE_ATTEMPTS_EXHAUSTED / ATTEMPT_EXHAUSTION / KNOWN_NOT_COMMITTED; synthetic finalizer witness only. |
| 22 / 0 / 3 | WRITE_OPERATION_EXPIRED / NONE / INDETERMINATE; native expiry response. |
| 22 / 0 / 0 | WRITE_OPERATION_EXPIRED / NONE / UNSPECIFIED; SQL Expired mapping (`storage.rs:1227–1233`). |
| 23 / 0 / 0 | WRITE_OPERATION_UNKNOWN / NONE / UNSPECIFIED; SQL Unknown mapping. |
| 24 / — / — | WRITE_KEY_CONFLICT (counter add): the write id already names a different add. Nothing applied; zero values; not retryable as-is. Added after this soak; not observed in it. |

`operation_identity_is_expired` compares a client-chosen UUIDv7 timestamp plus clamped TTL (1000–300000ms)
against storage wall time. Non-v7 identities do not use that fallback. The native outcome-store v7-age unit
and F13's first SQL submission are separate from F10's retained expiry evidence. Timestamp bytes are not
authenticated issuance time. Client/server offset, client-ahead timestamps and clock rollback matter.

Do not claim every submission after 300s is permanently refused: the warm cache is checked before durable
replay, and retained/pinned outcomes precede timestamp fallback. Nor does refusing a new lookup cancel work
already admitted. A2/A3 must record the ledger-client to storage clock context and review retry cutoff and
settlement explicitly; nominal TTL is not a cancellation fence. Unknown/Expired and plain SQL errors remain
ambiguous unless independent typed evidence establishes no effect.

### Ingress inventory and unavailable coverage

The retained ingress inventory names SQL, WorkerTask, Wendelon data/control/Raft, outbound Wendelon-client
binds, and observability policies. Toron remote SQL uses tenant certificates; WorkerTask uses toron peer
identities; ready/metrics use monitor-role HTTPS/mTLS. HTTP health is plaintext and constrained to loopback,
not mTLS. SQL Health is a separate protocol request. The full Toron suite runs existing policy tests; ladder
3 also explicitly runs Wendelon's real QUIC join, TCP/TLS fallback, cross-domain-role and operator-peer tests.
Some existing rejection tests accept error/silence without receiver-observed cause; a timeout is not evidence
of certificate rejection. A3 still requires named rejection points, receiver corroboration and valid follow-up.
No encryption-at-rest, all-log-sink, backup confidentiality or compromised-node acceptance follows here.

Coverage unavailable at this A1 identity:
- **SQL/table-outcome leader change:** the live RF1 fixture has no successor. Existing RF3 chaos
  control/finalization tests are not SQL mutation/effect witnesses; a bound RF3 instrument is needed.
- **Bound UPDATE/UPDATE and DELETE/UPDATE:** no causal gate exists after candidate materialization in the
  live integration harness. Unsynchronized overlap is not substituted for evidence. See C1 below.
- **Multi-table concurrent snapshot:** F14 is cross-request only; code supplies HlcBound(0), not a held
  read timestamp. The checked quiescent reconciliation cut remains necessary.
- **Admitted work across identity expiry:** no pin binds pre-expiry admission to completion afterwards.
- **Public SQL KNOWN_NOT_COMMITTED:** unavailable, not declared impossible. Present-resolution mapping
  preserves typed knowledge if observed, but F11 is synthetic-finalizer only; plain SQL errors cannot be
  classified as no-effect on this evidence.
- **Public terminal-exchange fault:** collector controls and ignored-terminal-read code fact only.
- **Storage-process crash, power loss and independent-client restart:** native tests reopen objects/server
  state on the same directory; they do not establish those stronger failure models.

**C1 — must-close concurrent mutation gap.** Before A3 diagnostic results claim anything about concurrent
updates, close C1. A lib-internal `#[cfg(test)]` coordinator read-then-write gate is permitted by Stage A;
its absence in the integration harness does not mean instrumentation is forbidden. Alternatively, the
A2/A3 instrument can bind actual operation intervals and report observed lost updates. Unbound timing may
establish an observed failure, never absence of lost updates. Freeze the selected route and permitted
orderings at A2 review. Marking C1 unavailable here does not discharge it.

### Bindings on A2

- Use one tenant and synthetic data; F9 must be fixed under separately reviewed scope before multi-tenant
  or real customer data. G1 also remains a precondition for real multi-row invariants.
- The ledger owns fresh CSPRNG/v7 operation ids, never deterministic/shared generators. Bind each id to
  exact request bytes/content hash locally and fsync intent before send; refuse changed bytes under one id.
- Record acknowledgement, raw typed resolution and independent state separately. Preserve durable
  delete_counts; affected-rows is not substituted for deleted effect. A missing detailed outcome after
  capacity pressure does not settle ambiguity or authorize a fresh-id retry.
- Unknown and Expired remain ambiguous. Do not assume a no-effect class is reachable through public SQL;
  absent a pinned typed no-effect result, a SQL failure cannot enter `rejected` just because it errored.
- Serialize conflicting writes until C1 closes. Keep the independent ledger and checked quiescent cut;
  no cross-request/multi-table snapshot is assumed. Record client-to-storage clock context and review
  retry/settlement boundaries explicitly. Do not infer universal TTL or capacity windows from one pin.

### Stage B candidates (unselected)

1. Tenant-scoped dedup identity / exact-request binding, with F9 first: false success across authenticated
   tenants is an integrity defect and a precondition for multi-tenant or real-data use.
2. G3 dedup-cache retention/eviction.
3. G4 connection-task reaping.
4. G2 uniqueness or explicit conflict semantics.
5. G6 RF/topology plumbing and actual placement/quorum evidence.
6. G8's unresolved acknowledged-but-unreadable acceptance.

No fix is selected or silently included in these test-only commits. The A2 instrument and any A3 campaign
require their own review; the 24-hour/72-hour target has not run.

### Retained ladders, provenance and corrections

All ladders run on bigfoot in isolated clean worktrees with Rust1.95.0, SSH agent forwarding and
RUST_MIN_STACK=67108864. They explicitly reuse a warmed target, with development builds stopped; start/end
source identity matches. Parts 2/3 print an empty porcelain line; part 1 enforces cleanliness in its script.
All figures below sum retained test-result lines, not inferred additions or development runs.

| Ladder / source commit / tree | Toron passed/failed/ignored | TPC-H | Client | Steps |
| --- | --- | --- | --- | --- |
| ladder-a1-1 / 7a2bbfc1 / 8f8fe010 | 5717/0/20 | 674/0/6 | 24/0/0 | 7 rc0, final0; SQL5/0; connection/cache1 each; fmt0. |
| ladder-a1-2 / 7cdf4c36 / 86aeac00 | 5719/0/20 | 674/0/6 | 24/0/0 | 11 rc0, final0; SQL7/0; native outcome-store6/0; individual native/connection pins1 each; fmt0. |
| ladder-a1-3 / 3b1adaee / 2b113e5f | 5722/0/21 | 674/0/6 | 24/0/0 | 16 rc0, final0; SQL10/0/1 plus capacity1/0 separately; outcome-store6/0; native/connection pins1 each; four transport/auth tests1 each; fmt0. |

The extra ignored test is the separately executed capacity control, not missing capacity evidence. Native
TTL emitting wait is1025ms in ladder2 and1018ms in ladder3; F15's59573ms is ladder3, not the58989ms development
run. Received enum values replace asserted numeric constants in parts2/3. Existing unused adr589_plain_write
warning remains; there is no zero-warning/full-Wendelon-suite claim. No release artifact or host staging is
produced by these characterization packages.

Raw ladders and development runs stay on bigfoot at `~/work/toron-evidence/adr629/ladder-a1-{1,2,3}` and
`dev-a1`. Exact copied remote sets/hashes are checked:27,40,55 ladder files respectively. Workstation packages
are `~/work/.agent-bridge/adr629-a1`, `adr629-a1-part2` and `adr629-a1-part3`, with raw logs, source bundles,
signature verification, remote listings and test summaries. Their SHA256SUMS are:
- part1: `dd49f92e983312b2fd72596004e2e41ece9aa5c3978c6836b674bb63f10447fa`;
- part2: `a7a28a3d7764d14a83a0e3598fcd340638cf640d6d9e186bf7d238c2a120b7d5`;
- part3: `132813106d57228e467fc7d437e237accf27fcf7f86cf64e42e24c323f08c60f`.

Part2's original package is unchanged. Its separately hashed ADDENDUM-1
(`1bf81c983a480b04cb3383947209e0dfae18f2e86ac512fc8f8ede5ae9f82e83`) corrects attribution of cold expiry:
non-v7 retained evidence, not v7 timestamp fallback. Claude accepted corrected F10 in01a103e2 and disclosed
that Claude's first checksum claim preceded a successful check (wrong cwd); the subsequent checks passed. The
disclosure of the failed check and correction stay in chatd; no original evidence is rewritten.

Development failures and operator slips are retained, not cited as passing evidence:
- mutation-1: reversed helper integer types; cache-growth-1: expected OK instead of native DUPLICATE;
  cache-growth-2: used packed response accessor methods as fields.
- ttl-part2-1: expected Unspecified where native Expired response carried Indeterminate; corrected
  characterization in ttl-part2-2. An accidental duplicate let binding was caught before testing/commit.
- mutation-part3-1: expected native Indeterminate at SQL resolution, which maps Unspecified; corrected
  without changing production semantics. Later controls strengthen typed StorageError and exact row decoding.
- initial part3 bundle fetch named a nonexistent branch in a HEAD-only bundle; checked HEAD→review-ref
  fetch succeeded, without changing main/source.
- during ladder3 compilation, SSH status inspection stalled once and a bounded fresh check timed out.
  Subsequent inspection found the original ladder active, with I/O pressure and available memory/disk;
  no restart or source change occurred. The retained build-inspection.json records the observation; a
  particular cause is not established by these data.

A1 is limited characterization delivered. It is not the assertion that acknowledged writes survive all
faults, apply once under concurrency, or safely hold real customer data. The ledger and subsequent reviewed
campaign must expose these gaps rather than turn unavailable evidence into a pass.


## A2 instrument-development record (reviewed)

A2 is a reviewed, bounded instrument-development package, not a runnable trust soak. Claude’s package verdict is chatd 01a10640-428c-741f-a041-29a7f0000bcd; the R1/R2 and record verdict is chatd 01a10656-461a-772c-9a0b-44b0bc26986f. A3 remains held on authenticated storage reachability, a non-destructive pending-publication/finalization observer, stored-effect concurrency coverage where claimed, and real three-node readiness/placement/ingress checks. No production fix, Stage B implementation, host staging or soak campaign is delivered here.

The test-only source is cc739373e860a00076bbb62847932797bea7ebd0 (tree c97f01c2e0d275b3a2091e10aad5557fb9a07b27; parent 9c8e0e22). Five files add 644 lines without production semantics changes. The cfg(test) candidate gates bind real planner/executor materialization before captured mutation requests. The public-client probe invokes authenticated ToronClient with explicit UUIDv7 and records exact request/response bytes and typed outcome axes, with create-new evidence. Its authenticated SQL fixture is disk-backed RF1/simulated storage; it is not three-host replication evidence.

The pinned ladder-a2-1 kept the same clean source identity at both ends under Rust 1.95.0. All nine steps passed: Toron 5727/0 with 25 ignored, TPC-H 674/0 with 6 ignored, toron-client 24/0; explicit C1 two tests/four orderings, authenticated adapter, routing and real-loader pins; initial Python 39 tests; formatting. A separately copied final Python suite passed 42 tests. R1/R2 corrections subsequently passed 46 tests with ResourceWarnings treated as errors, without a Rust change or another Rust ladder. Existing warnings, failed development checks, the warmed-target provenance and raw logs are retained. No release-artifact or zero-warning claim is made.

F16, lost-update payload: two concurrent UPDATE candidates both read amount=10 and each submits amount=11, in both capture-writer orders. F17, delete-resurrection payload: DELETE/UPDATE retains the earlier candidate in both orders, including UPDATE submission after the captured DELETE returned. Stale payload is established; stored lost update/resurrection is inferred and unpinned. The capture writers do not commit to Wendelon. No concurrent-update acceptance or safe personal-data deletion claim follows.

The independent ledger fsyncs exact intent and send records before adapter invocation, uses 74 CSPRNG bits in each UUIDv7, refuses torn tails instead of repairing them and keeps append-only raw acknowledgement/resolution history. No automatic mutation resubmission is permitted; ambiguity reserves the key. R1 preserves class acked while flagging non-committing resolutions, including Unknown, Expired and non-committing Present axes. Later committed evidence does not erase a discrepancy. These flags appear in both reconciliation and run summaries; they are findings, not a no-effect witness. R2 defines sequential client mutation RPCs with per-key gating. Unknown outcomes on distinct keys may coexist. A late resolution cannot release another operation's reservation. The sole-writer assumption spans the whole cluster but is neither enforced nor observable for other clients; client policy does not establish storage-level serialization.

The two-hour workload proposal is startup-scale: 592 planned requests including resolution, exports and analytics, averaging 296/hour; quiet windows and small bursts; one synthetic tenant; customers, orders/payment/license rows, support tickets; scrubbed customer soft-delete without cascades. Typed length-prefixed canonical row hashes bind exact integer, text and null content. The CLI exposes proposal only. Initialization, deployment, thresholds, collectors and a causal settlement observer still require A3 review.

The actual Toron loader rejects LAN storage client binds even with TLS, contrary to its message about allowing authenticated remote transport. The loopback-bind control parses. The real Wendelon loader parses requested RF3 with explicitly dummy parser fixture materials; no TLS validity, startup, placement or quorum is established. The client routing pin begins with a local bootstrap and then resolves an advertised remote leader directly after ring refresh. Proxy-hop support is not proof that all client traffic remains local. A reviewed reachable route is a prerequisite; no guard relaxation is implemented in A2.

Exports can be structurally checked and observed differences reported, but reconcile remains WITHHELD. The proposed 120 s observation budget, quiet intervals, TTL/Expired results, connection closure and metrics are not storage finality. The available MutationDrainRegistry wait is DROP-coupled, so the instrument does not destroy customer tables to manufacture a cut. No durability, exactly-once, transaction or security acceptance follows. Marker/resource/clock helpers cover declared evidence only, with missing or failed collection unavailable; no uncollected sink is certified.

Provenance: original reviewed package /home/me/work/.agent-bridge/adr629-a2 (SHA256SUMS 3a76bf2b...), plus revision /home/me/work/.agent-bridge/adr629-a2-r2, with SHA256SUMS 089a4a895aa273c28a72f5814e715303063d5392cb929ad9dfc100c2978bc229 over 110 files. Complete ladder logs are retained on bigfoot under /home/me/work/toron-evidence/adr629/ladder-a2-1; mirrored review packages and labelled synthetic/adapter development artifacts remain separate from any A3 campaign.


## Stage B P1 scope (reviewed)

User order: P1/P2 first to unblock a separately reviewed A3 readiness and diagnostic package. Scope verdict: chatd 01a1065f-fdc4-72c2-98cc-38880ad2477c, reviewing draft c272559ae4a8acf254849f35e3a9287d5e3ed66e0f7fc11fe6b2acf84c411160. Production helper or semantics changes outside this scope return for review. F16/F17, F9, growth and G2 are not bundled. Origin publication remains held.

## P1: permit an authenticated storage client to bind a reachable local address

### Established problem and alternatives

`toron/src/config.rs:833-840` normalizes the client bind before its storage TLS configuration. `normalize_wendelon_client_bind` at :1378 rejects every non-loopback address, although its message says "must remain loopback until authenticated remote transport is configured". P1 fixes this missing condition; the guard is not a deliberate ban on authenticated remote storage.

A2's real-loader refusal and loopback control distinguish config parsing from deployment. Wendelon's client initially bootstrapped locally chooses advertised remote owners after `ControlRingTopologySource` refresh; the new routing pin resolves the remote leader directly. `factory.get_connection(target)` dials that target. Proxy-hop handling does not prove transparent local forwarding. Alternatives are therefore: (a) TLS-conditional reachable bind, proposed here; (b) explicit ingress-pinned proxying, which changes routing/failure semantics and is deferred; (c) relaxing all binds without TLS, rejected; (d) retaining loopback under an unproven forwarding assumption, not readiness evidence.

### Exact semantics proposed

Normalize the complete storage TLS configuration first, preserving its all-or-none requirement. A loopback bind remains accepted with or without storage TLS as today. A wildcard or a concrete non-loopback local bind is accepted only when the storage TLS tuple is complete and its configured cluster domain is valid. Partial TLS, malformed addresses and missing TLS on a non-loopback bind fail closed. Port 0 remains a permitted outgoing client bind; this does not weaken worker/SQL advertised-address checks.

Configuration completeness grants permission to attempt authenticated startup, not proof that the certificate is valid. Real material loading and identity/role validation must succeed before readiness. Wrong certificate, key, CA, domain, node SAN or role must not fall back to plaintext or the static unauthenticated RF1 branch. No membership, owner selection, RF, quorum, storage admission, SQL tenant identity, or mutation-result semantics change.

The authenticated branch remains `wendelon-client::Client::connect_quic`, with dynamic ring refresh. A2's LAN-refusal pin becomes a superseded characterization pin mapped to the TLS-conditional invariant; old evidence stays unchanged. The source should contain the new invariant, not a test that expects the old refusal after the fix.

### Security argument and boundaries

The outbound storage path currently builds QUIC+TLS from certificate/key/root material, dials canonical node names and verifies the peer's DataNode role, cluster domain and exact target node (`quic_transport.rs`, `tls.rs`). The receiving storage service must continue to authenticate/authorize the presenting actor for each data/control/peer operation. Configured paths alone do not bypass those gates. Freeze the actual presenting storage actor and trust roots in the implementation package; do not treat Toron SQL tenant certificates or monitor/operator identities as peer certificates.

`endpoint_from_tls` uses an Endpoint::server and installs a client configuration. Moving that socket to a LAN address therefore also exposes its QUIC handshake surface even though the storage client has no application accept loop. The implementation review must inventory inbound behavior, bound accepted/pending handshake resources and prove no data/control service is accidentally served by that client endpoint. If a safe outbound-only endpoint requires a helper change, bring that change back as a scoped P1 amendment; do not silently change the shared server helper. P1 is not a general authentication/access-control fix, and F9 remains a recorded tenant-isolation limit.

Negative tests: non-loopback without TLS; each partial TLS tuple; missing/unreadable/bad key/material; wrong CA/domain/node SAN; valid certificate of the wrong role; an untrusted inbound connection to the client socket; trusted inbound traffic must not invoke a data/control handler. Positive tests: old loopback behavior; TLS with wildcard/concrete client bind; dynamic refresh to a different storage owner over the real authenticated path. Validate the rejection point and unchanged state, not just a timeout. Config tests do not substitute for real TLS tests.

### Evidence, ladder and rollback

Unit loader invariants and real transport tests come first on loopback/one host; observed client target/address and peer identity are recorded. Then the pinned full Toron/TPC-H/client ladder plus affected Wendelon/TLS tests and release builds in an unchanged worktree, Rust 1.95.0, signed commits, retained failures. Actual three-host startup, joins, RF/quorum/placement and role controls belong to the separately reviewed A3 readiness package.

Rollback: stop affected Toron clients cleanly before reverting code/config. A previous binary rejects the new LAN bind, so code rollback alone cannot leave such config running. Reverting to loopback does not claim a working remote ring; keep multi-node readiness false unless an independently proven route exists. P1 changes no on-disk data format, trust roots or firewall rules. No running storage node is reset to RF1 as a shortcut.

### Reviewed deployment and inbound refinements

Use each host's concrete LAN address with port 0 in the proposed A3 client configuration rather than wildcard binding. Readiness must observe stateful-firewall return traffic to the ephemeral QUIC client port and whether unsolicited inbound traffic is dropped or rejected. Record those observations rather than infer them from prior UDP 7701–7703 or port 5201 openings. No firewall change is authorized; any required change is a user decision at A3.

Inventory whether server_config(material) requires and verifies inbound client certificates. Where it does, pin an untrusted peer's TLS rejection point explicitly; a timeout is not rejection evidence. If it does not, return for the outbound-only client-helper sub-amendment. Also distinguish verifier configuration from whether the client endpoint actually drives inbound TLS; an absent application accept loop is not proof of certificate rejection.



## Stage B P2 scope (reviewed)

Same scope verdict: chatd 01a1065f-fdc4-72c2-98cc-38880ad2477c. P2 authority inventory may proceed in parallel as reading/design; no P2 production code before P1 package submission. If the coverage contract cannot be implemented without a semantics/fence change, return for a separate scope decision.

## P2: a non-destructive observer that can certify a bounded stable export window

### Proposal and excluded shortcut

Propose a **read-only, causally bracketed observer**, not a DROP operation, mutation cancellation, quiet timer or a new transaction API. It reports authoritative work and an activity sequence; the harness brackets every export/analytics call with validated observations. A zero gauge alone never certifies anything. A temporary admission fence is an alternative if observation coverage proves inadequate; that would change write semantics and needs a separate scope review. It is not implicitly authorized by this draft.

The observer proves a stable observation window only when its complete contract holds. It does **not** certify that every previously submitted client intent will never be admitted later, that an Unknown/Expired operation had no effect, or that no future mutation can occur after the window. Ambiguous operations remain ambiguous. It must not bridge a restart or turn a conditional cut into a permanent settlement claim.

### Existing authority and mandatory coverage inventory

`storage/mutation_drain.rs` has the node-local lifecycle mutex, per-(tenant, table object, schema generation) admitted work counts and a startup-reconciliation flag. Its cloneable guards cover spawned local durable append work (`data_server.rs:9044-9058`). Its existing wait requires a committed DROP closure and cannot be used to create a non-destructive cut.

That registry is a starting point, **not yet established as complete finalization coverage**. `data_server.rs` also has durable pending-finalization records, orphan-witness recovery, replica/child paths, retry work and startup recovery. Before implementation review, enumerate every path that can still change logical table visibility or its mutation finalization, including INSERT, UPDATE's write path, DELETE, replicas/forwarding, abandoned caller futures, durable retries/recovery and relevant DDL/retention. Map admission/registration, handoff, terminal publication and release to their owning authority. Metrics, live requests, response writes and in-memory dedup counts are not that authority.

Every registered branch must retain a counted obligation across detached work and transfer to persistent pending state without a zero gap. Persistent work must enter the observation authority before startup is called reconciled. Unknown/uncovered obligations make coverage unavailable, never zero. Missing instrumentation may require extending guard ownership/registration, but must not change the algorithm's acknowledgement, retry, outcome, retention or durability semantics. Any change to those semantics returns for scope review.

### Exact observable contract proposed

A bounded authenticated request identifies node, tenant, immutable table-object IDs and schema generations. A typed response includes:

- the requested scope echoed exactly and whether it exists/is supported;
- node identity and fresh process incarnation (not just PID or host boot ID);
- current membership/placement epoch and schema identity;
- startup reconciliation and named coverage status;
- an exact checked activity sequence for that scope;
- outstanding logical-publication/finalization obligations, including durable pending/recovery debt, with a complete breakdown or an explicit unavailable reason.

The activity sequence changes whenever work capable of altering that scope is admitted/registered, and whenever a logical publication, finalization or relevant recovery/metadata change occurs. Counting and sequence updates share the relevant admission/registration linearization authority. A snapshot must be coherent with it; independent reads of registry and durable-store counters cannot be assembled into a false zero. If multiple authorities cannot provide a coherent report, return unavailable until their handoff is explicitly pinned. Sequence overflow, unreadable persistent state, partial startup, unknown work type, unsupported protocol, stale schema/epoch, truncated/partial response or failed authentication all withhold the cut.

The response contains diagnostic identities/counters, not rows, SQL, hashes of customer contents or key material. Use a narrowly scoped read-only diagnostic actor and permission for this action on the authenticated control plane. Its credential must not inherit general Operator/ClusterAdmin privileges; SQL QuerySession, DataNode peer, tenant admin, backup/auditor/monitor and unauthenticated actors do not inherit the observation action. Exact actor encoding and trust-domain policy must be reviewed with the implementation. If a dedicated actor cannot fit, bring that decision back and make Operator custody explicit in A3: credential location/holder, removal after the campaign and no broad credential silently installed on the load generator. Exact permission/action and wire discriminant/version allocation must be listed at implementation review. Unsupported older peers fail closed rather than returning a default zero. Requests/replies are byte/item bounded. No new unauthenticated HTTP endpoint or TLS relaxation.

### Checked-cut protocol

1. The sole producer stops new workload submissions and records its ledger head; every known client attempt has a classified response or remains explicitly ambiguous. Stop/close evidence is checked. This is not assumed to cancel remote work or stop other writers.
2. Observe every required storage node/scope. Each first observation must be supported, coherent, reconciled and zero outstanding. Save incarnation, activity sequence, membership/placement and schema identities.
3. Only after **all** first replies arrive, perform the declared exports/analytics reads. Record their actual queries, responses and routing. No DDL/cut-triggered data mutation. Disable table-retention/time-dependent visibility in the synthetic workload and verify this metadata; no row-TTL claim follows.
4. After **all** reads finish, observe every same node/scope again. Require zero outstanding, identical checked activity sequences, incarnations, membership/placement and schema identities, and complete coverage. The sequence cannot reset/reuse within an incarnation; restarts and rollover invalidate it.
5. Certify only the export window causally enclosed by these paired replies. Use caller ordering and per-node tokens, never subtract unrelated host clocks. If any node is missing/unsupported/nonzero/changed, the result is WITHHELD; elapsed time or repeated quiet polls cannot rescue it.

This can establish that no covered logical-publication/finalization activity crossed the window, not snapshot isolation for arbitrary concurrent users or equality of every replica. A3 readiness still has to identify all required nodes and routes; an RF3 setting is not a proof of complete scope. A newly admitted request after the second observation is outside this cut. An earlier intent that was not yet admitted remains ambiguous even if it arrives later. A checked cut does not upgrade Unknown/Expired into no-effect or erase R1 discrepancies. Reconciliation may identify an acknowledged row absent during a checked stable window; its scope and outstanding-intent limitations remain explicit.

### Pins and reader negatives

Deterministic gates/barriers, with no sleeps as proof: snapshot versus admission at the lock; detached local append after response/caller drop; guard-to-durable-pending handoff; finalization/recovery across startup; delayed replica publication; DELETE; work that starts and finishes between observations (same zero counts but changed sequence); schema/placement change; unsupported/uncovered work; and observer failure. A held publication is nonzero/unavailable, never a certified zero. Prove stable positive exports through the real daemon/client path without dropping tables or changing rows.

Auth negatives cover every denied role, CA/domain mismatch, tenant/object scope mismatch, malformed and oversized requests, invented/mismatched tokens and old protocol. Observer requests must not mutate user data, cancel tasks, retire namespaces or silently discard finalization debt.

Reader negatives: one node missing, forged zero counter, no persistent coverage, missing first/last observation, equal timestamps without causal bracketing, changed sequence after a zero-to-zero cycle, reset incarnation/epoch, wrong object/generation, an export before all first replies or after a last reply, out-of-order/duplicate replies, ambiguous intents mislabelled no-effect, discrepancy flags discarded, time-dependent retention unverified and a cleanup action used as the cut. A positive cut remains conditional on its exact required scope. No-cut fixtures keep WITHHELD.

### Ladder, bounds and rollback

Source/wire/reader package and the coverage inventory precede any staging. Use the pinned full three-crate ladder, affected Wendelon/control/TLS/store tests and exact protocol/reader negatives; retain failed/superseded runs, binary identities and initial evidence. No host count, deadline, fault matrix or campaign is inferred from this plan: those freeze at the instrument/readiness review. An observer deadline reports unavailable; it neither cancels work nor manufactures terminality.

Rollback disables the new observer/client command and returns the reconciler to unconditional WITHHELD. A read-only observer has no persistent admission gate to reopen and no table data to restore. Old peers' unknown-action handling must be pinned before release. Do not copy an older database over newer writes, drop tables, delete pending records or claim that rollback finished a publication. P1 can remain delivered while P2 is unavailable, but A3 cannot gain an oracle claim.

### Credential least privilege

Pin that the observer credential cannot perform data reads/writes, peer/cluster control, administrative mutations or other diagnostic actions; only its reviewed finality-observation action is permitted. Record credential custody and identity hashes at A3 without exposing private keys in evidence. Broad administrative credentials are not the default load-generator identity.

## P1 outgoing client endpoint sub-amendment (reviewed)

Scope verdict: chatd 01a10665-461f-7235-8223-61d53449719c. This addition is authorized within the separate P1 package, with no staging before review.

`server_config(material)` calls `rustls_server_config_with_peer_roots`, which uses `tls_pki::build_client_cert_chain_verifier`; that constructs WebPkiClientVerifier without allow_unauthenticated. Inbound client certificates are therefore required when an incoming handshake is driven. The storage client's endpoint has no application accept loop, so this is not proof that its real unsolicited handshakes reach TLS certificate validation. Do not replace that limitation with a timeout claim.

Proposal: add a dedicated `client_endpoint_from_tls(material, bind_addr)` beside the existing Wendelon QUIC helper. It constructs `quinn::Endpoint::client(bind_addr)`, then installs the unchanged `client_config(material)`. `wendelon-client::Client::connect_quic` uses it. The shared `endpoint_from_tls` / `endpoint_from_tls_with_peer_roots` server constructors stay unchanged; no server node/peer listener loses inbound service.

This makes an outgoing storage client's socket client-only: incoming unsolicited new connection attempts have no server configuration to accept them. Traffic on its authenticated outgoing connections remains served normally. It removes the avoidable inbound handshake surface rather than opening a new application service or relying on firewall state. No certificate, peer name/role/domain/node verification, ALPN, client idle/keepalive policy, routing, membership, wire version, mutation outcome or storage format changes.

Tests: actual outgoing data/control requests through the helper; dynamic remote-owner resolution plus the real production Client::connect_quic path; no new incoming connection accepted or application handler invoked at the client socket; distinguish actual protocol rejection/response from no-response where applicable (a timeout alone never establishes rejection); old server helper's untrusted/absent certificate rejection at the TLS verifier with the accept path driven; valid-peer old-helper positive; unchanged wrong-domain/node/role outbound failures. Reuse finite existing transport policy through client_config. No new dependency is required (quinn is already used inside Wendelon).

A3 still freezes concrete per-host bind: host LAN IP, port 0; return-path and unsolicited-traffic firewall observations; no firewall edit authorization. Deployment/packet behavior is not inferred from constructor or loopback tests.

Rollback reverses only the Client call/helper addition while stopping clients and restoring reviewed bind configs; old server helper behavior is preserved. This proposal contains no code change and no staging.

Conditions: retain or positively test server-initiated streams on client-initiated connections wherever a Wendelon flow needs them; otherwise state the surveyed absence with code references. The helper prevents new incoming connections, not streams on an existing connection. The CLI client at wendelon/src/cli/index.rs:238 uses the old server helper on 0.0.0.0:0; record this as an observed, separate candidate and leave it unchanged. Old server-helper absent/untrusted certificate rejection must drive the accept path and name the TLS rejection, not count a timeout. A3 packet/firewall behavior remains separate.

## Stage B P1 results (reviewed)

Implementation clearance: chatd `01a106af-0986-7463-b38b-f23ae68088d6`, answering package request
`01a106ae-7be3-7316-9bb1-569876dd9bb6`. Source: `c049cdda73285b29cd128ac3a9cbdafc268a2326`,
tree `7302440aa374eeaecd7a88b70173ee789c62cdbb`, on the reviewed plan `c4e3ecdd`.
Production changes are in signed commit `6fb7c801`; `eb49b506` and `c049cdda` add tests only.
This section records P1 delivery, not three-node readiness or store trust acceptance. P2 and A3 remain held
on their separate inventory, implementation and readiness reviews. Origin publishing remains held.

### Delivered behaviour and bounded evidence

Toron normalizes the complete storage TLS tuple before checking the storage client bind. A concrete LAN
or wildcard bind is accepted only with complete TLS and a valid storage cluster domain; without TLS the
bind remains loopback-only. IPv4/IPv6, all eight TLS-presence combinations, invalid domains and malformed
addresses are pinned. Parsing does not establish usable key material, packet reachability or startup.
The former A2 real-loader rejection pin is superseded by
`adr629_p1_real_config_loaders_accept_tls_configured_storage_binds`; its retained evidence is unchanged.

Only the production SDK `Client::connect_quic` switches to `client_endpoint_from_tls`, which constructs
Quinn's client-only endpoint and installs the unchanged TLS client configuration. Node listeners and shared
server helpers remain unchanged. Peer-opened streams on an authenticated outgoing connection exchange
bytes successfully. An unsolicited new dial expires within the test observation, with zero admitted
handshakes: this is not called a TLS rejection or a LAN firewall measurement.

Driven old-server-helper tests record receiver-side TLS rejection of absent and untrusted client
certificates. New-helper outbound tests record wrong CA/name rejection at TLS and wrong target/domain
rejection at authenticated peer binding; malformed, mismatched and observed unreadable key material
refuses. No role, trust-domain, ALPN or node-verification rule is relaxed.

The production SDK pin bootstraps through one of two fixture nodes, discovers the other, and establishes
an authenticated session to it. The bounded SDK read observed `NOT_LEADER`; SDK retries prevent attributing
the terminal reply to a particular peer. This proves discovered-route authentication and a client-level
response, not successful placement, read/write readiness, finality or replication. The fixture uses
runtime-owned teardown; a stalled development graceful-shutdown attempt is retained and no graceful
daemon-shutdown claim follows.

### Retained ladder and provenance

Final `ladder-p1-2`: Rust 1.95.0, source/tree as above, clean and identical start/end identity, all twelve
steps and `final.rc` zero. Full summaries including documentation tests:

| Suite | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| Toron | 5729 | 0 | 25 |
| toron-tpch | 674 | 0 | 6 |
| toron-client | 24 | 0 | 0 |
| Wendelon | 2071 | 0 | 10 |
| Wendelon-client | 13 | 0 | 0 |

Focused config, transport, SDK and real-loader pins pass; formatting passes; both release steps have zero
warnings. Wendelon's full suite ran with `--test-threads=1`. Failed `ladder-p1-1` is retained: its parallel
quarantine-counter assertion saw delta 2 instead of 1. The package demonstrates the process-global,
label-keyed metric, the assertion spanning awaits and independent tests incrementing that bucket; both
metric and maintenance sources are byte-identical to the plan base. This supports serialization but does
not identify the original competing writer or prove its interleaving. No maintenance fix or base-suite
reproduction is claimed. The dedicated worktree changed only between completed ladders. Warm target,
fixture-data reuse and cached production-equivalent release bytes are declared; no clean-build or
reproducible-build claim is made.

Package: workstation `~/work/.agent-bridge/adr629-p1/`, with 121 files and exact remote/local ladder
file-set/hash verification; raw ladders on bigfoot `~/work/toron-evidence/adr629/ladder-p1-{1,2}`.

- `SHA256SUMS`: `af0d2a5820e01fa11973afb643d0b26a05db7fbc5e0590fd525f01adeaa19bb7`.
- `review.md`: `99cf8e21b697301d01731bc12d9f6412303c67ffe15b046e98109e2ff3583387`.
- `source.diff`: `6007ac29acad4c2f6a8d138a0fa85cd2dc0b87881bfc69cb9120d777ad30e826`.
- Release daemon: `c6dfb672ed7603834efde2c63e334a1d462926335d391abf15151bdb66e9d092`.
- Release probe: `484e6842b6d149e575802df02d1cca1d4931bc632f70a29a8d72fc637fb835cf`.

Development failures, owned interruptions, all signatures and the Quinn constructor evidence are retained.
No artifacts are installed by this record. A3 must still freeze concrete LAN bind addresses with ephemeral
ports, successful routes to every required node, placement/readiness and actual return-path/unsolicited
firewall observations. No firewall change is authorized. Separate candidates are isolation of the flaky
maintenance metric test and the CLI's existing server-mode client endpoint; neither is changed in P1.

## P2 observation authority selection (reviewed)

User decision and design clearance: chatd `01a10747-ce8c-704f-9852-3900d870bb27`. The user selects
**option A, the full observation authority**, accepting the additional implementation effort. The reviewed
inventory is `~/work/.agent-bridge/adr629-p2/review.md` SHA256
`de6141a957c5897645d041f85323a2a11cc3d1abcd2fb4a251894bcedf00f38e`, with verified package manifest
`31fc64c0f826886cafc5414333558872c945d24a51553efc3352a3eb0910a76d`.
Existing DROP-drain and pending-store gauges cannot certify complete coherent finality. The selected design
extends observation authority; it does not impose a DROP closure, mutation admission fence or new outcome
semantics. Unmapped work and unavailable evidence withhold observation coverage.

Option B was considered as a cheaper ledger-scoped observed-state diagnostic, not an equivalent stable cut.
Its assessment and source references remain at `~/work/.agent-bridge/adr629-p2-alternative-b/assessment.md`
SHA256 `bcd7ddf3fbf644737d748ab8cb24a49a97e49f33a0e85b8225efb42dd370248f`, package manifest
`1e08af0d84d8345b171b7a550a465412bcbe2f0992941b6cfa44dd7f68c1f190`.
Final-row equality cannot generally count duplicate applications, certify continuous retention, or exclude
metadata A→B→A transitions between samples. Blanket SQL-success-to-exact-publication binding is false on
A1's characterized paths. Those corrections remain in the alternatives record; B is not substituted for P2.

### Selected authority and credential

A short synchronous node-local mutex owns checked activity sequence, fresh process incarnation, owned work
and durable shadow obligations. It is never held across awaits or while taking storage, Raft or database
locks. Observation tokens move or clone into detached futures and blocking closures before spawn; persistent
shadow debt is registered before the active owner releases. Metadata uses a sequence-validated collect,
not independent zero gauges. Unknown variants, unmapped producers, missing peers, partial startup or
unreadable state report unavailable. This is an observation-only mechanism; registration must not alter
mutation admission, acknowledgement, retries, cancellation, retention or durability semantics.

The conservative node-wide sequence and blocking are selected subject to an early practicality check.
Slice C measures actual idle-client sequence advancement on a real synthetic daemon before A3. If continuous
maintenance makes stable windows unattainable, return for a scope decision (for example, proving and
separating physical from logical publication). No such separation is assumed or silently implemented.

The dedicated `FinalityObserver` actor chains to Ops roots, holds only `FinalityObserve`, has no node SAN,
and carries exact `finality:<object>:<generation>` claims for one tenant. Proposed control pair 0x5E/0x5F,
wire version 1, at most 16 scopes and 64 KiB metadata. All existing permissions/opcodes and other roles are
negative tests on both QUIC and TCP; old peers are unsupported, never zero. Indistinguishable deployed
Ops/Data roots limit the cryptographic trust-domain claim and must be disclosed.

### Implementation and review gates

Proceed A→E, with a separate immutable source/evidence package and pinned ladder reviewed **after each
slice and before the next**:

1. A: observation core and deterministic barrier fixtures for admission/snapshot, owned release,
   zero→zero activity, overflow, poison/unavailable, unknown features, metadata collect and incarnation.
   No DROP or write-outcome change; lock-order proof is included.
2. B: table/raw primary/follower and detached/blocking-work coverage; zero-gap persistent handoffs and
   cancellation pins. From B onward, each package includes a before/after existing write-heavy test or
   benchmark on the same host/profile, read from retained artifacts. This is a regression gate, not a
   performance claim; a large regression returns for decision.
3. C: startup/recovery/replay, control metadata and maintenance publication coverage, including the early
   real-daemon sequence-stability measurement above.
4. D: narrow actor, bounded wire/client and exhaustive authorization/old-peer negatives.
5. E: actual stable export bracketing and refusal readers through real daemon/client paths. Host schedule,
   counts, bounds and readiness remain a later freeze/review.

If B or C cannot retain obligations without changing mutation semantics, stop and request the explicit
authority/fence amendment. An always-unavailable observer is safe but is not P2 delivered or A3 ready.
This selection authorizes slice A first; source integration and staging remain independently reviewed.
Bigfoot fast-forward publication only; origin remains held.

## P2 slice A results (reviewed)

Slice A is cleared as a **dormant observation core**, not P2 delivered or an A3 oracle. Review verdict:
chatd `01a10770-e2cf-71db-9e64-a42c3758c6df`, answering package request
`01a10770-6a72-73dc-8c9a-963f99515534`. Source commit
`4dc633caa152263dd6796c7770e2051857ba0cf7`, tree
`1a05ec61a028baaf3dad278d1d92f12111b6cc70`, parent plan `52d5fc6e`.
Two files (+655) add the private storage observation module and its declaration. No production node
constructs it and no writer invokes it yet; no actor, wire, mutation admission, outcome, retry,
cancellation, DROP, store format or durability policy changes.

The core has a short synchronous mutex, checked node-wide sequence, cloneable work owners, durable
shadow obligations, a fresh registry-lifetime UUIDv4 incarnation, and unavailable coverage until all
five components reconcile. Last-owner release advances sequence, including zero-to-zero activity.
Durable installation overlaps active ownership. External metadata reading holds no authority lock and
is bracketed by sequence checks. Poison, accounting failure and overflow withhold observation without
refusing an ordinary mutation. Fifteen named fixtures cover lock ordering, ownership in a running
blocking job, handoff, accounting and collection refusals. They do not establish production database
cancellation, startup recovery, real metadata completeness or a real-daemon incarnation. Those remain
B/C obligations.

Retained package: `~/work/.agent-bridge/adr629-p2-a/review.md`, SHA256
`290e240e2b13223c1511a959107a6960a576ab0413ff189bd5f7294c3b809438`;
54-file manifest `72c4aa79ca87ab4a82630230f5218510cdcaf6cf36b827e9a8457462c47c4f7b`.
Exact remote/local file sets and hashes match for 39 ladder files and four development files.
Bigfoot `~/work/toron-evidence/adr629/ladder-p2-a-1` has identical clean start/end source identities,
Rust 1.95.0, all ten step statuses and final status zero: toron 5729/0/25, toron-tpch 674/0/6,
toron-client 24/0/0, Wendelon serialized 2086/0/10, Wendelon SDK 13/0/0, focused core 15/0/0,
real config pin 1/0/0, formatting, release daemon and release probe. Counts include libtest/doc-test
summaries. The warm target and reused pinned worktree are recorded; no clean-build claim. Release
warnings zero. The expected caught poison-fixture panic and inherited test-only warnings are retained.
A final-document cwd/path mistake failed before writing, then was rerun with an absolute path; source
and ladder were unaffected.

Daemon `c6dfb672ed7603834efde2c63e334a1d462926335d391abf15151bdb66e9d092` and probe
`484e6842b6d149e575802df02d1cca1d4931bc632f70a29a8d72fc637fb835cf` equal P1's release hashes.
This is consistent with removal of the unreferenced private module, not a stale-build claim; pinned
source/build logs are retained. No artifacts were staged.

Carried review conditions: C needs explicit startup durable initialization while coverage is unavailable
and must pin exact versus conservative scope matching on completion. Sticky reasons must be reported per
node for diagnosis. D must project node-wide blockers without exposing other tenants' identities.
B must include the same-host/profile write-heavy before/after comparison; C must perform the early
real-daemon sequence-stability measurement. Slice B is now authorized; later slice review gates,
origin hold and A3 campaign hold remain.


## P2 slice B results (reviewed)

Slice B is cleared as **write-path observation ownership**, not P2 delivered or an A3 oracle.
Review verdict: chatd `01a10830-9163-71d0-89ae-41bc8e0de607`, answering package request
`01a1082f-cb0b-71b7-a287-a7090620c928`. Final signed source
`ba2b4d49d1f9d1bda3590e00d068346804979369`, tree
`15e8bbb3ae6302d7b039aa184623d3ea4235604a`, review base `3e73c9dc`.
The signed chain is `d0091978` (ownership), `1f56e64e` (helper carve), `ba2b4d49`
(four helper-header comment lines). Thirteen Wendelon source/test paths, +2078/-1012;
indentation-ignored +1182/-116. No amend or force rewrite.

A single registry is shared by SlabManager and its pending-finalization store. Primary raw/table,
bifurcated and follower entries register ownership synchronously; tokens clone before spawning
local jobs, JoinSet work and both blocking executors. Publication and actual pending redb jobs
retain ownership after caller cancellation. Independent pending-store ownership begins before
waiting for its write lock. Successful DB commit hands off to durable observer debt inside the
blocking job before its active owner releases, independently of the legacy post-await gauge.

Observer revisions are reserved under the existing redb write transaction before commit. Newer
committed revisions win when cancelled callers allow handoffs to arrive out of order. The actual
redb delete-then-new-upsert interleaving is pinned by
`adr629_p2b_actual_redb_late_delete_shadow_cannot_erase_a_newer_commit`; the reverse ordering is
covered by the controlled core fixture, not a second actual DB fixture. Unknown commits withhold
observation. Revision history drains when all reserved updates settle; an indefinitely held handoff
can retain other intervening versions. This is not a bounded-memory proof under starvation.
Opening the same pending-store path with a different registry marks both UnknownProducer without
refusing ordinary service.

Queue and in-flight IO owners accompany the existing buffers through CQE processing. The real
kernel fixture `adr629_p2b_real_kernel_write_keeps_owner_through_cqe_after_waiter_abort` witnesses
an actual aligned write, CQE and file bytes after waiter cancellation. Synthetic queue/in-flight
fixtures are labelled separately. An unknown in-flight exit withholds observation rather than
claiming kernel cessation. Existing public IoRequest, buffers, replies and kernel behavior remain
unchanged. No authority mutex is held across awaits or external/store/database locks.

All fourteen B proofs and fifteen retained A proofs pass. Actual redb cancellation, both blocking
executors, real CQE, detached primary and follower jobs are covered. The primary publication fixture
explicitly disables its cloned fixture's existing lease gate to isolate ownership; the follower fixture
disables its pending reconciler to avoid unrelated resolution. These are not authenticated ingress,
production lease-policy or remote-cancellation claims.

The cancelled-upsert gauge discrepancy is a pinned **pre-existing finding**: the authoritative DB
contains one pending record while the legacy count remains zero after caller cancellation. The
observer retains its debt. The gauge defect is not repaired here and remains a separate candidate.
Stored concurrency gaps and sensitive-customer trust are not closed by this slice.

### Pinned ladder and retained failures

Bigfoot `~/work/toron-evidence/adr629/ladder-p2-b-3`: clean, identical start/end source/tree as above,
Rust 1.95.0, all ten step statuses and final status zero. Counts include libtest/documentation summaries:

| Suite | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| Toron | 5729 | 0 | 25 |
| toron-tpch | 674 | 0 | 6 |
| toron-client | 24 | 0 | 0 |
| Wendelon, serialized | 2100 | 0 | 10 |
| Wendelon-client | 13 | 0 | 0 |
| A+B focused proofs | 29 | 0 | 0 |
| Real config pin | 1 | 0 | 0 |

Formatting and both release builds pass; release warnings zero. The dedicated worktree and warm target
are declared, not a clean-build claim. Release daemon SHA256
`e82b7513aec0b5897996bfa687a4a427f8a16d70b814b0d3b6cf44513d5f21db`, probe SHA256
`911bbc8cfe5491c86b6a0100c48035cb0922facd289d023cb3f1c35b64793f3f`.
No artifacts are deployed by this record.

Both failed pinned ladders remain retained. Ladder 1 failed the unchanged root caps; synchronous
helpers reduced data_server.rs to 16329/16340 and manager.rs to 4630/4650. Their explicit architecture
source manifests include both helper modules. Ladder 2 failed two AST source-concatenation checks
because inner module doc comments appeared mid-file. Exactly four header comment lines in both
helpers were changed to ordinary comments; no parser guard or cap was weakened. Development 7's
initial missing source-manifest entries and development 9's corrected architecture/AST/focused results
remain retained. The whole serialized Wendelon development precheck passed 2100/0/10 before final
signing, with all thirteen candidate paths matching by hash. All ten development roots and original
baseline overhead captures, including failures, remain separate from final ladder evidence.

### Matched overhead regression check

Bigfoot `~/work/toron-evidence/adr629/p2b-overhead-interleaved` compares base `3e73c9dc` with final
source `ba2b4d49`. The existing real-node
`adr0539_two_ordinary_sampler_sized_ingest_cycles_remain_readable` is used solely as a coarse ingest
regression check; it remains an ADR 539 smoke test, not that ADR's correctness acceptance or an exhaustive
hot-path benchmark. Existing codec/packing/crypto benches do not exercise these changed paths.
Each side/profile has one warm-up and five measured invocations, alternating pair order, on the same
host/toolchain/target. All four binaries were built and copied before measurements; all 24 invocations
passed. Wall seconds below are read directly from retained `*.time` files; libtest's independently
rounded test-duration summaries are preserved separately in the package.

| Profile | Side | Wall seconds median | Wall seconds min–max | Max RSS KiB median | Max RSS KiB min–max |
| --- | --- | ---: | --- | ---: | --- |
| Debug | Before | 41.11 | 40.96–41.97 | 151000 | 145540–155796 |
| Debug | After | 41.83 | 40.79–42.08 | 154644 | 152636–157764 |
| Release | Before | 7.30 | 6.94–7.36 | 101416 | 96036–108032 |
| Release | After | 7.36 | 7.05–7.66 | 103332 | 98032–107844 |

Wall-time median differences are about +1.8% debug and +0.8% release, with overlapping ranges:
no regression signal at this resolution. This does not establish zero CPU/allocation overhead or
performance under stalled handoffs. Raw samples, per-run load snapshots, build/source identities,
full binary hashes and memory figures are retained; no failed-source overhead run was used.

### Provenance and next gate

Workstation package `~/work/.agent-bridge/adr629-p2-b-r3/`:
345-file SHA256SUMS `9ca9aa48495cec0b38dbcc7862489f9a7933c7782628c4badd0a83cb85a5f33d`;
review.md `0b64bc557a0b4ec6bf82aadd81729ec4810e7807f537c69bdf6e75bdeed1ed94`.
Exact remote/local file sets and every copied hash verify for final ladder/overhead, both failed
ladders and all development captures. Four overhead binaries remain on bigfoot, explicitly listed
as remote-only with hashes; no local retention of those binaries is claimed. The source bundle and
all three source signatures verify. Source is promoted only to bigfoot main; origin remains held.

Production observation coverage remains unavailable. No observer actor, opcode, endpoint or usable
cut is enabled. Slice C is now authorized, beginning **first** with the required real-daemon sequence
stability measurement, synthetic client work stopped and background maintenance running normally.
Report that diagnostic before the rest of C; unattainable windows return for a scope decision.
Then cover startup durable initialization, recovery completion scope matching, replay, metadata,
drop/retention, peer and unmapped producers. The whole serialized Wendelon suite is a pre-signing
check for C. D/E and A3 remain held behind their separate review gates.


## Stage B P3: Data Ring lease availability (approved plan)

Claude cleared this plan for user review in chatd `01a10c38-be84-7211-9dd1-aa1076b1d964`. User concrete-plan approval is recorded in `01a10c3f-12b3-7082-b4ec-8b20ba56616a`, binding revision 2 SHA256 `6772c6880cfa7f81faa4f736ab28d6cabd8e063dd19a5291a54a3a7fd22e02ce`. Implementation on an isolated branch is authorized; source/ladder/compatibility/acceptance/resource review precedes staging. Origin publication, campaign launch and A3 remain held.

# ADR 629 Stage B P3: bounded Data Ring lease renewal and admission waiting

Status: APPROVED FOR ISOLATED IMPLEMENTATION; staging and campaign remain held. The user selected the scope for planning in chatd `01a10c29-9678-77f6-9577-cacf59de7230`: early renewal with X=2500 ms and H=250 ms, one same-flight admission wait W=3000 ms, and a 6000 ms signed lease term. Revision 2 was cleared by Claude and then approved by the user as recorded above. This section authorizes isolated implementation only; it does not launch a campaign. P2 C completion, the stability campaign and A3 remain held behind their existing gates.

This is Wendelon's **Data Ring range lease**, not Toron's coordinator QueryLease from ADR 628. Root-cause work on the pre-send response gap is optional and separate; no audit durability or Openraft priority change is selected here.

## 1. Problem, evidence, and deliberately bounded claim

Startup seeding produced safe write refusals during an expired-cache interval while a same-holder renewal was still pending. The twelve-trial earlier characterization includes a request-anchored 3000 ms grant taking about 1554 ms, followed about 1002 ms after completion by a renewal taking about 2237 ms. The cached lease expired before that renewal installed; the client exhausted its retry budget in the gap. This is a bounded availability finding, not a demonstrated mutation loss or a population failure rate.

The ONE responder extension is complete. Final report: `~/work/.agent-bridge/adr629-data-ring-responder-capture-1/REPORT.md`, sha256 `97cfa1b6bff0f50102b13242af8801e13ecd706b21135615605e1140e8dab664`; manifest `b569a9381653be910857475e4f69d2b70dcbf8ac38e0e825d6e2737145b56403`, 102749 files. Exact public listing covers 102712 files; the unchanged reviewed readers independently return identical `READ_DIAGNOSTIC` readout `098101c4f837685646d683b879b0a0442252e049d82aadfa686256601a16d6e3` on bigfoot and the copied workstation tree. Corrected full manifest check rc0 is retained in sibling `adr629-data-ring-responder-capture-1-verification`.

The extension stopped at coverage_target: 15 fresh-profile trials, 11 renewals strictly over 500 ms,2119.697495 seconds; all 20340 inserts and full exports passed, errors empty, cleanup verified independently. All-oneshot wrapper perturbation is broader than the channels emitting markers. Compared with the prior capture, median renewal durations are similar but the observed tail is larger; differing duration, coverage-based stopping and instrumentation do not establish overhead causality or a stall rate. Own apply-finish marker to send-begin is 46194–1125231us; send calls 0–1 us and first-ready intervals 28–123 us. Notification enqueue/dequeue and the cause of the pre-send gap remain unmeasured. No second diagnostic extension is planned.

Selected quantities are engineering envelopes, not inferred universal latency/clock bounds:

| Quantity | Selected value | Meaning |
| --- | ---: | --- |
| L | 6000 ms | Ordinary signed grant lifetime, anchored at each range request |
| X | 2500 ms | Conditional grant-path envelope, retaining earlier 2237 ms evidence |
| H | 250 ms | Maximum extra scheduling/queue headroom in the availability claim |
| R | X+H =2750 ms | Required renewal reserve before the actual signed expiry |
| W | 3000 ms | One maximum expiry-only wait for an already active matching renewal |

**Narrow waiting contract:** at the designated pre-mutation gate, expiry is the only failed authority predicate, the matching holder/epoch/range renewal is already active, and a currently authorized, signed, committed grant installs and remains usable before the original absolute wait/request deadlines. That request is not refused solely for that expiry miss. Every other gate still applies. Initial issuance, missing cache, denied/unknown/failed renewal, identity change, a stale or unusable grant, shorter caller budget and exhausted waiter capacity do not inherit this promise.

**Conditional continuity claim:** with a valid predecessor, unchanged ownership/epoch/issuer policy, usable clocks, each grant completing within X, and aggregate scheduling/queue delay within H, the selected 6 s envelope is tested for isolated and back-to-back delays. This does not promise availability under arbitrary repeated stalls, a hung proposal, unlimited ranges, clock faults, suspension/restart or transfer. Two serial 2.5 s grants can exceed a 3 s request-anchored term; changing scheduling alone cannot close that envelope. A wait cannot invent valid ownership. Admission authorization and later error publication have distinct times; a delayed task may report its safe failure late.

## 2. Code baseline and change inventory

Code references checked at main `7a08db71d37b925fd37a58d88a34ad13561675ab`, tree `b0ec463f7d3ac1480d9e7c121226c6d0d1c6785a`, clean. The diagnostic source `5fd4921a` remains a private measurement branch; this plan does not promote its forwarding wrapper or diagnostic feature to production.

- `wendelon/src/network/data_server.rs:3792`, `refresh_leases_once`: one physical/HLC anchor before the range loop; serial grants, cached leader view, then cache insertion. `spawn_lease_manager` at3922 schedules the pass. These are the scheduling/installation seams.
- `data_server.rs:3499`, `lease_gate_result`: combines absent/wrong identity/expired into Missing and validates a copied lease across a current-leader await; callers currently pass request timestamps. Its linearizable-read caller at10350 also uses this helper. The new write-specific reason classifier must not silently alter historical/read semantics.
- `write_bifurcated` at8488 and `write_table_bifurcated` at8838 perform write lease gates; `table_write` at9279 and `table_delete` at9773 own admission clocks, mutation permits/drain and preparation. All production table INSERT/DELETE and raw write routes must be inventoried, including each child range of a multi-range statement.
- `data_server/observation.rs`: observation registration occurs before the returned future is polled. `storage/mutation_drain.rs` separately linearizes DROP closure and counted mutations. A waiter remains owned by both applicable lifetimes.
- `consensus/hlc.rs` supplies monotone HLC merging; `consensus/monitor.rs` supplies SystemClock and a drift monitor. Neither a monotone logical counter nor sparse peer samples prove elapsed-time lease safety under an arbitrary wall-clock fault.
- `node.rs:890–936`: ordinary 3 s default and integral-u32 TTL validation; TTL/3 derived cadence. `config.rs:107–108,791–820`: omission uses the ordinary default; generated local convenience profile explicitly uses 90000 ms. ADR080's existing local profile is not silently rewritten.
- `control_server.rs:3656–3717` signs remote requests with the configured TTL cap; `raft_storage.rs:4000` applies range/epoch/holder overlap rules. Both local and remote grant paths remain consensus-committed and signed. Existing same-holder renewal/epoch/overlap behavior is pinned before compatibility claims.

Implementation will first inventory the exact base, final call sites, numeric statuses and lock order, and return the source package for review. Helpers live in semantic modules; architecture file-size/AST guards are preserved. No `.codex`/`.claude` repository files or production `adr629_*` names.

## 3. Per-range flight and installation semantics

Each currently owned inclusive range has a retained generation record: holder, epoch, exact start/end, current authority/leader view, a unique process-local flight generation, request HLC, request elapsed-clock marker, requested/effective TTL, old grant identity and terminal result. States are queued/not-started, active, installed, denied, failed/unknown, or superseded. Only **active** is wait-eligible. A queued range is not an in-flight renewal. Late subscribers see the retained result; no bare one-shot notification without retained state.

Keep one active grant caller per node, preserving serial Control Ring proposal traffic; select due owned ranges by earliest deadline, with fair deterministic tie-breaking. Never overlap flights for the same range. Range count/queued records are bounded by the current routing snapshot; removed ranges and terminal generations with no references are reclaimed. A stalled caller cannot create an unbounded series of detached retries. Epoch/ownership/leader invalidation wakes waiters promptly; cancellation is not proof the old consensus request ceased. An underlying request with unknown outcome cannot be relabelled denied or authorize a write.

Every range samples its own clocks/HLC **immediately before issuing its request**, after waiting for the node proposal lane. One pass-wide timestamp is forbidden. The configured term is request-anchored, not extended by RPC, commit, apply, response logging or cache installation time. Requested expiry uses checked integral-millisecond addition; local and remote paths preserve the requested HLC logical/node fields identically. Overflow/unrepresentable time fails without a grant. A remote cap may shorten the term; scheduling and waiting use the actual verified descriptor, never the requested larger value.

On reply/install: validate shape, exact range/holder/epoch/request HLC, signature and issuer, current leadership/ownership/routing, current clock usability and still-valid expiry. Re-fetch current views after awaits. The request's original leader snapshot is not an install authorization. Serialize generation/cache installation and record publication; obsolete generations never replace a newer valid generation. Within an unchanged identity, reject a regressing start/expiry. An already-expired response is retained as unusable, never notified as installed. Membership or leader changes invalidate wait success even if the old response later arrives. Failures may leave a predecessor usable only while its own gates still pass.

The installation check is a defined local authorization point, not a claim that membership cannot change afterwards. Final write gates separately revalidate current views. No authority lock is held across Raft/transport awaits. A state-machine or replica pin exposing a pre-existing handoff/late-old-grant integrity gap becomes a HOLD for a scope decision; do not silently patch it or claim safe handoff from cache checks alone.

## 4. Expiry-aware scheduling, retry and multiple ranges

After a usable grant, request the next refresh at the earlier of:

1. actual signed expiry minus R, conservatively mapped into the local elapsed clock; and
2. that range's own request marker plus effective TTL/3 (floor integral milliseconds).

The second value retains ADR080's cadence as an upper cap. Ordinary 6 s requests therefore target 2 s from that range's own request, earlier than the 3.25 s reserve deadline. Explicit 90 s targets 30 s; explicit 3 s with the selected reserve targets 250 ms and incurs substantially greater proposal load. Completion time is not the next timer anchor. A target already passed triggers one prompt due action after the lane is available, not a burst or busy loop. Epoch wake-ups coalesce; a timer/watch crossing never creates a duplicate flight.

Each range's due/selected/issued/install times and queue delay are observable. For the envelope, X covers issuance-to-usable-install time. A next request cannot start until its own preceding flight settles: its earliest eligible start is max(the selected target, that preceding same-range completion). H bounds additional polling and other-range queueing after that eligibility, and the actual start must still precede the signed reserve deadline. Record target lateness and additional eligible-start lateness separately. In the one-range back-to-back X control, a target at2s becomes eligible at2.5s; with H≤0.25s it starts by2.75s, before the6s grant's3.25s reserve deadline. This is a conditional envelope, not an exemption for unbounded queueing. The single-lane restriction means a many-range queue can exceed H; such a run is outside the continuity envelope and must be reported. It still fails safely with bounded admission wait. Test multiple ranges and a blocked earlier range; one-full-range capture results are not extrapolated to arbitrary topology.

Capacity diagnostics and A3 readiness record the actual number of owned ranges, never assume one. Report owned-range count × observed issuance-to-usable-install duration against the effective TTL/3 window, using median, tails and maximum with failures/censoring separately. With heterogeneous terms, additionally report per-range durations and the conservative smallest window. This is an empirical capacity screening quantity, not a latency guarantee: full-lane scheduling traces must still show that each range fits its reserve and that additional eligible-start queueing stays within H. A product that exceeds the window, a reserve miss, or unavailable range/latency coverage prevents claiming the continuity envelope for that topology; do not hide it with success-only averages. Bind this measurement and the real range roster into A3 readiness.

Failures schedule one bounded backoff, initially 50 ms, doubling to a 1000 ms cap, per range and reset only by usable installation. At most one retry is active/queued per range. Backoff does not reset any old expiry or caller wait deadline. No retry is started just to satisfy a waiter, and a waiter does not initiate/reissue a lease. After the predecessor lapses, a future successful same-owner request is a fresh current grant, not continuity across lapse. A waiter bound to a failed/superseded flight ends; it does not silently hop to that later acquisition. Retry periods outside X/H are outside the availability claim. Retry/queue fixtures and traffic measurements must demonstrate no catch-up storm or starvation.

## 5. Clock/HLC policy and its limits

The wire authority remains the signed HLC interval and existing consensus epoch/overlap rules. Do not import ADR628's coordinator boundary formula. Lease time and row commit/snapshot time have different roles.

For new requests use a fresh monotone node HLC, not the client's timestamp. For write validity sample a fresh server HLC after awaited view lookups and compare the greater of client HLC and this fresh server HLC with the signed expiry. Equality is expired. This comparison is write-specific: historical read bounds and stored row/commit HLCs are not rewritten. In particular, waiting never re-mints a logical operation ID or changes a previously constructed append-only identity/content hash. Preparation that must be rerun preserves its original stamp or aborts safely; the concrete path inventory must prove consistency.

In addition, retain a per-request suspend-aware elapsed anchor (Linux CLOCK_BOOTTIME) sampled before request minting. A descriptor can authorize no later than that anchor plus its actual checked physical duration. The elapsed cap is a process-local conservative guard under the declared operating clock assumptions, not a new cross-node rate/skew proof. It can shorten authority; it can never extend the signed expiry. This prevents wall rollback or suspend from extending locally trusted cache lifetime. Local expiry/reserve/wait calculations round conservatively in integral milliseconds, use checked arithmetic and compare at use sites after awaits, not only in a timer task. A failed/unsupported/inconsistent clock read, exhausted HLC/time arithmetic, or observed time discontinuity with no safe mapping rejects installation/use and wakes waiters to safe failure. Retained grants without matching process-local anchors after restart are not used as cached admission authority; obtain a newly validated grant. Cold restart therefore has an intentional write-refusal window until a fresh usable grant installs; it is excluded from the stall-continuity promise and must not be counted as recurrence of this expiry-renewal defect. Pin rejection of restored cached authority and first admission only after fresh installation. Measure restart-to-first-admission using one harness elapsed clock with causally bound process-start, grant-install and successful admission records; report setup/issuance failures and unavailable coverage, and never subtract clocks from different processes.

BOOT time does not by itself prove a cluster-wide clock-rate/skew bound or namespace survival. Availability evidence declares no suspend/restart/clock discontinuity and the existing cluster clock discipline; it records clock context as context, not proof. Deterministic controls exercise rollback/forward step, suspend-like elapsed jumps and delayed polling: limits are safe gate refusal, not uninterrupted availability. Do not add drift numbers or claim general cross-clock safety from this finite campaign. If the minimal clock helper requires a platform/API expansion, it is named in the implementation review and pins error/read-uncertainty handling; do not fall back silently to a suspend-paused wait clock.

Wait cap is request-gate BOOT entry+W, shortened by the original caller remaining budget and existing WriteClocks expiry. CLOCK_BOOTTIME gives an elapsed cap despite suspension; WriteClocks keeps its existing deadline/stall semantics. Renewal is not mutation durability progress and must never call `progressed()` or reset stall/caller deadlines. A resume after W/expiry cannot enter mutation even if a timer/future is ready. No 3 s end-to-end response guarantee follows from W.

## 6. Expiry-only waiting and fresh final gates

Introduce an internal write classifier that separates valid, exactly matching expired, missing, wrong epoch/range/holder, ownership/routing mismatch, no leader, issuer/signature invalid, and clock unusable. Retain existing numeric wire statuses/taxonomy; an internal reason is not a new success status. Only an expired but otherwise currently valid cached identity, paired with an active renewal for that identity, is eligible. Signature/auth/epoch/policy failures never wait.

A logical mutation has at most one lease-wait episode and one original absolute deadline. For multi-range statements, all affected ranges must pass the final gates; eligible active flights can be observed concurrently within the **same** W, not W per child. Any other failed authority predicate wins immediately. Ready results are retained and rechecked before subscribing/awaiting and on wake, so both notification-before-subscribe and after-subscribe are safe.

Use dedicated nonqueued wait permits: proposed node cap 64 and per-range cap 16, additionally limited by the existing mutation work permits for table writes/deletes. Admission to the wait uses try-acquire, never an unbounded semaphore queue. A request waiting on several ranges counts once at node level and once in each attached range. Acquire the complete permit set atomically/all-or-none; partial acquisition is released. Saturation returns the existing expiry refusal with known-not-committed semantics if no earlier work exists. These capacity values are proposed plan choices, not measured optimal defaults, and are explicit review items. No payload copy per subscriber; retained frames remain subject to existing frame/batch limits. Report peak waiter/flight count and retained bytes; counts alone are not a byte-memory proof.

Cancel, timeout, denial, unknown result, clock fault, identity/leader/epoch/ownership change, shutdown or DROP closure ends the wait. One caller cancelling does not cancel a renewal shared by other requests. Release only that caller's tokens/permits, exactly once; do not erase the shared retained result while others still reference it. Cancellation/error notification is distinct from cessation of a consensus request or detached mutation.

The designated wait point is after authoritative known-outcome/pending-intent replay and routing identity is available, but before new mutation work. Establish the concrete INSERT/DELETE/raw-write inventory before code clearance. No lease/cache/flight/observation mutex, store lock, content-hash/secondary-index exclusion guard, or quota reservation is held across the wait. Existing mutation work/drain **ownership counts** may remain held; they are not mutex guards. Acquire/reacquire preparation fences after the wait, preserving DROP and index ordering.

After waking, re-run current membership/primary/range/leader/issuer/signature/expiry, caller clocks/cancellation, degraded/persistence-failure fences, tenant/schema generation, DROP/retention/integrity policy and quota admission before mutation. A cached success or earlier metadata snapshot is not reused as authority. Pure/prepared payload reuse requires a current descriptor/identity check; otherwise rebuild under the unchanged operation stamp or fail before publication. This does not provide transaction isolation for the captured business UPDATE payload.

A late gate after asynchronous preparation performs a new non-waiting full check; it does not allocate another W. Define the final authorization point immediately before durable/mutation submission, including all child ranges. A lease check before a long awaited operation is not that point. Pins must exercise a request already crossing mutation while expiry and handoff occur: existing replica/finalization fencing stays authoritative, and any absent protection is reported as a must-resolve scope gap. This plan does not authorize bypassing it or masking a previously untested integrity defect.

## 7. Observation authority, DROP drain and outcome finality

The request's P2 WorkToken is created at its original entry before polling/spawning and remains active throughout permit wait, lease wait, validation, mutation, detached IO and finalization as applicable. A waiter is unresolved work even before a durable intent exists. Cloning/child scopes conserve existing ownership; do not create an unobserved gap on task handoff. Keep applicable MutationDrainGuard held across the bounded wait so committed DROP either counted the request before closure or refused it; the post-wait check prevents publication after a newly observed close. Cancellation before work releases counts; committed or detached work retains its own owner until true completion. Observer unavailability remains sticky; this feature cannot turn missing accounting into zero or reject unrelated service just because P2 observation is unavailable.

Background renewal/install work must participate in existing P2 C metadata/maintenance observation at the selected base, without authorizing P2 coverage prematurely. Pending waiter counts cannot be subtracted to fabricate sequence stability. Pin no stable zero while a waiter is active, exact release after safe refusal, and retained durable debt/IO after caller cancellation. Observation scopes must not disclose another tenant's identity.

Before new durable work, expiry/wait saturation/timeout/denial is known-not-committed only after confirming this attempt has no existing pending/committed outcome. Consult known/durable replay first. Once work crosses the mutation boundary, keep the existing committed/indeterminate/recoverable-pending semantics. A wait deadline is not proof a previous attempt did not commit; never overwrite an authoritative outcome with a timeout refusal. Failed outcome persistence remains a fail-closed error, not a fabricated durable rejection.

Record response numeric write_status, stop_reason, commit_knowledge and resolved outcome/visible rows separately for INSERT and DELETE. Lost post-commit replies resolve through the durable authority under the same operation ID. No new ID retries, no client retry count/backoff changes and no multiplied W: one logical request's existing absolute budget must flow through attempts. If current public-client attempt deadlines cannot accommodate the selected wait, record bounded failure/coverage unavailable and return for a narrow budget-scope decision; do not silently extend them. Retention/TTL limits and existing replay findings are not fixed here.

## 8. Defaults, configuration, grant/epoch compatibility and deployment

Change the ordinary omission/default from 3000 ms to 6000 ms in the one existing DataRingLeaseTiming/config authority, feeding both local renewal and remote leader TTL cap. Add no shadow role default or environment toggle. Preserve explicit configured TTLs, including existing 3 s profiles and generated 90000 ms local profiles; retained files are never rewritten. Update the local-profile renderer's current literal3000ms replacement anchor together with the ordinary template/default; a real generated-profile pin must still produce exactly90000ms, not silently fall through to the new ordinary value. They do not inherit a 6 s envelope. Requested/effective term and chosen scheduling policy are exposed in configuration/diagnostic records.

All existing ADR629 diagnostic configurations explicitly set `data_ring_lease_ttl_millis = 3000`; they would retain that value, yield a 250 ms target cadence (about four times the prior cadence), and receive no 6 s envelope. New A3 and soak configurations must explicitly set 6000 ms or omit the field to select the new ordinary default. Readiness checks bind both configuration and actual effective signed term; retained historical configurations/evidence are unchanged. Emit one clear startup warning when the effective target cadence is below 1000 ms, including configured/effective term, selected cadence and reserve. This declared warning threshold is operational visibility, not a safety limit or measured performance threshold; valid explicit 3000 ms is warned, not rejected.

Timing validation now requires integral-u32 TTL and positive TTL−R for this availability policy; production configurations at/below 2750 ms fail startup clearly, rather than generate an immediate busy loop or pretend to meet the reserve. Such configurations previously accepted by ADR080 require documented migration/explicit rejection pins. Do not silently clamp small configured terms upward. Internal tiny-duration deterministic fixtures may use an explicitly constructed scaled test policy, with no production bypass. Explicit 3 s is supported but measured as high-frequency renewal; 90 s retains the TTL/3 upper cap. Idle-soak evaluator/config/docs must name the new deadline-driven cadence instead of asserting unconditional TTL/3, without weakening unexpected-writer checks.

LeaseDescriptor, AcquireLease metadata, Ed25519 preimage, wire discriminants, WAL/snapshot schemas and authority state-machine overlap semantics remain unchanged. 6 s fits the existing wire field. A larger request or new binary never edits/extends an existing signed grant. Recovered 3 s/90 s grants retain their exact intervals and overlap effects. Matching same-holder renewal may replace them only through a new authorized committed proposal. A distinct holder cannot take overlapping authority merely because a process was killed, request timed out, the local cache was invalidated, or a new default was installed. Epoch transition remains the existing explicit fencing boundary; issuer/leader transition invalidates local trust, not an invented release of replicated ownership.

Mixed policy/rolling upgrades are **not certified**. An old leader cap can yield a shorter descriptor; preserve its signed term and fail the6s envelope rather than treat it as6s. Read pins cover existing 3 s grants and older wire carriers, not a rolling deployment approval. For any deployment proposal: stop ingress, settle or accurately retain unresolved operations, verify cleanup/authority state, stop all affected holders/Control Ring participants in the declared roster, record old signed grants/epoch, install one reviewed build+configuration consistently, restart/renew, and reopen only after current signed ownership/readiness. No membership bump, holder release, data deletion or firewall change is implicit. If the grant/state compatibility pins reveal an incompatibility, revise the plan before deployment.

Nominal same-epoch stale-grant exclusion can increase by 3 s for ordinary grants. Failover also depends on membership/leader/fencing and is measured separately; no exact added failover latency or guaranteed 6 s recovery claim.

## 9. Mandatory acceptance matrix

Named `#[cfg(test)]` barriers or a separately reviewed test feature bind real request/range/generation/commit/response boundaries. Paused logical/elapsed clocks and Notify gates establish causal ordering; do not use arbitrary sleeps to stand in for a running victim. Authenticated process controls use the real SQL/storage route and only synthetic records. Compile-out proof/source map and exact artifacts accompany any added feature.

| Group | Controls | Required observable result |
| --- | --- | --- |
| A scheduling | Fresh issuance; quiet 6 s; actual effective3s/90s; request vs completion anchoring; later ranges; timer/watch crossing; retry caps | Per-request anchors, no burst/duplicate flight, bound/fair queue and actual cadence; real owned-range capacity quantity and reserve coverage; no success claim for queued-but-not-active |
| B finite stalls | Isolated and repeated back-to-back X delays; start queue delay below/at/above H; longer/unbounded proposal | Inside-envelope current grant continuity where preconditions hold; outside envelope bounded safe refusal, no local extension |
| C wait timing | Grant before/at/after W; old expiry equality; caller budget shorter than W; stalled poll/resume; expired response | One absolute wait, equality fails, no usable-after-deadline or deadline reset |
| D authority failures | Missing cache/leader, wrong epoch/range/holder/current primary, denied/unknown grant, malformed term, bad signature/issuer, policy denial | No eligible wait/admission; exact current typed outcome retained |
| E flight ordering | Notification before subscribe/after subscribe; late subscriber; old result after invalidation/new grant; same-holder regression | Retained result, monotone generation/install, no lost wakeup or stale overwrite |
| F ownership | Current epoch/holder/range/issuer change during wait and install; grant 3 s→6 s; 6 s→configured 3 s; overlap distinct holder; restart | Existing consensus fence and signed duration preserved; no double admission; no cached anchor reused on restart; fresh-grant-before-admission pin and restart-to-first-admission timing |
| G mutation boundary | INSERT/DELETE/raw paths and all child ranges; preparation await; already-submitted write during handoff | Fresh final gate, unchanged row identity stamps, existing replica/finalization fence; uncovered pre-existing safety defect means HOLD |
| H resources |64 node /16 range plus existing table work limit; all-or-none multi-range permits; flood, cancelled subscriber, hung shared renewal | Counts/bytes bounded, no extra waiter queue/proposal storm, exact permit/owner release |
| I finality | Known replay; pending replay; no-work timeout; lost committed reply; cancel before/after mutation; restart; expired retention | Raw response/resolution/row witness agree within existing contract; no timeout-created KNC for prior uncertain/committed work |
| J observation/DROP | Waiter vs zero export; DROP closure during wait; detached IO after caller drop; observation unavailable | Active waiter counted, no false stable zero; cancellation conserves remaining debt; fresh DROP check |
| K time | Per-range anchors; backward/forward wall; logical carry/overflow; BOOT read failure/uncertainty; suspend-like jump; timer starvation | Strict write-time expiry and elapsed cap, safe failure; no rewritten historical-read snapshot/commit stamp |
| L compatibility/config | Old grant signature/WAL replay; local/remote cap; defaults omission; explicit 3 s/90s; small/overflow config; old cap | One authority, exact effective terms; explicit6000/omitted A3 readiness; below1000ms startup warning; clear unsupported policy and no rolling-compatibility claim |

Before/after controls use the same reviewed barrier schedules and exact operation identities; they do not manufacture passive fault statistics. Through public SQL retain response, resolution and authoritative row/export witnesses. Any clock/routing/barrier mechanism that fails to exercise its declared seam is coverage unavailable, not pass. Bind all records to process/query/range/epoch/flight, with causal single-clock ordering; truncated/malformed/foreign/contradictory evidence refuses conclusions. Every reader rule needs a meaningful negative, including forged installed grants, absent requests, wrong generation, an invented same-flight wait, and saved-finality disagreement.

## 10. Ladder, overhead, resources and campaign gates

All Rust builds/tests only on bigfoot, isolated pinned1.95.0 worktree, SSH agent forwarding, `RUST_MIN_STACK=67108864`, bounded jobs. Pre-signing includes the WHOLE serialized Wendelon suite and relevant architecture/AST/file-size guards; named-crate fmt only. Final retained ladder: Toron, toron-tpch, toron-client `--no-fail-fast`; Wendelon serialized `--no-fail-fast`; Wendelon-client; named new proofs/config/ordinary compile-out controls; fmt and release daemon/probe builds. Same clean source/tree at start/end, exact artifact hashes/warnings/rc/counts. Failed/superseded runs retained, no unstable snapshot reset mid-build and no unrelated-failure claim without comparative evidence.

Overhead/resources: ordinary before/after release artifacts (no timing features), same host/config/dataset (explicit 6000 ms for the matched code-overhead comparison), no competing build, warm-up then at least 5 measured interleaved pairs each in quiet one-range, projected 1356 seed and saturated-waiter controls. Freeze protocols and thresholds BEFORE measurements: proposed no-stall seed wall-time median regression gate≤5%, peak RSS median increase≤max(5%,8MiB), no recurring FD/retained-flight/waiter growth after cleanup, and exact configured permit maxima. Measure the ordinary-default 3 s-before/6 s-after policy/traffic difference separately and label it as a configuration comparison, not matched code overhead. Publish all raw ranges/pairs; passing gates is bounded smoke evidence, not zero overhead or days-long leak freedom. Quiet Raft lease count must match actual schedule+enumerated epoch/retry allowance, with unknown residual writers refused. Record total Raft/apply/sync/audit deltas, CPU/RSS/FDs, waiter bytes/peak, proposal-lane queue time and baseline noise. No post-hoc relabelling of a exceeded gate as noise; return for review. Deliberately waiting latency is reported separately from no-stall overhead.

Implementation package review precedes staging. A separate instrument/reader/topology/frozen matrix review precedes any process campaign. New roots, synthetic PKI, source/tag/artifact/dependency hashes and retention/provenance must be explicit. Preflight/body/cleanup/collection readiness all rehearsed end-to-end for every declared layout, using actual role argv and real mTLS acceptance/rejection, before launch. STOP on unknown cleanup/body/instrument error; no selective reseeding/retry or promotion of failed output. Natural errors retain finality/resolution, including unknown interrupted writes. No recurrence-count target that invites another unbounded diagnostic extension. The existing P2 stability/A3 campaign is not resumed by passing these tests.

## 11. Rollback and unchanged integrity gaps

Use separate signed subject-only plan and implementation commits, immutable baseline tags, current/build/config hashes and backups. No origin publication or install from this draft. Operational rollback first stops ingress, records/resolves or conservatively preserves outstanding operations, performs the reviewed drain/cleanup, then stops affected processes. Restore the previous tested binaries/configuration without overwriting newer data or consensus state. Previously issued 6 s signed grants survive exactly as replicated; an old binary's 3 s default cannot shorten or erase them. Wait for actual fencing/expiry or obtain an existing authorized same-holder renewal, with current epoch/issuer checks, before reopening. No forced epoch/release or shorter-term mixed operation is assumed safe. Metadata/wire format remains unchanged; any format change returns for its own compatibility review. Rollback rehearsal includes a retained 6 s grant, a pending renewal, waiting requests and committed-but-unanswered mutation.

C1/F16/F17 remain unchanged must-close gaps. A2 captured lost-update and delete-resurrection **payloads** in both orderings; stored lost update/resurrection effects were inferred and unpinned. This availability work adds no transaction isolation, conflict detection or personal-data deletion assurance. It cannot settle those operations from response status alone or declare customer integrity accepted. P2 C observation coverage/sequence stability and remaining D/E gates remain separate. The broader A1 tenant/replay/growth findings are not repaired by this scope.

## 12. Review/authorization sequence and exact plan choices to confirm

1. Claude independently verifies the already sent responder report, then reviews this plan.
2. User reviews the concrete plan including the clock/config/capacity/overhead refinements; scope approval for planning is not source authorization.
3. Only after both: a plan-only ADR amendment, signed subject-only commit, then an isolated implementation branch. Source/ladder/compatibility/pin evidence returns for review before staging.
4. Separate instrument/readers/freeze and operational rehearsal clearance before any host run. Results/rollback require their own gates. A3 remains held.

Reviewer attention: (a) Stage B P3 destination, (b) TTL/3 upper cadence cap plus expiry reserve, (c) single active node proposal lane and conditional many-range claim, (d) write-only fresh-HLC/BOOT elapsed cap and unchanged read semantics, (e) 64/16 waiter caps and proposed 5%/8MiB resource gates, (f) explicit rejection of too-small configured TTLs and whole-roster nonrolling compatibility. These are concrete draft decisions for review, not hidden implementation choices.


---

# Stage B P3 prerequisite: monotone committed range-lease authority

Status: SCOPED AND AUTHORIZED for isolated implementation by chatd01a10d0e (Claude clearance and user approval). Accepted plan SHA256 81271646613836778290071ae5d8cbdc12fe8239501177f999091bc5a56cd8c3. P3 availability remains paused until this prerequisite lands; implementation source, full pinned ladder and mandatory fencing evidence return for review before P3 rebase or staging. Origin, campaign and A3 remain held.

Expected consequences: old-epoch in-flight renewals are refused across a committed membership change; measure the resulting transient write refusals. A nonrolling 6s->3s configuration shrink can refuse renewals until a descriptor covers retained expiry; it cannot shorten signed authority.

## Finding and scope

Base3438b769 (ADR-only plan over source7a08db71). Legacy AcquireLease accepts a same-range/same-holder proposal without requiring nondecreasing epoch, start or expiry, then replaces the record. Direct SM characterization pins already prove expiry and epoch regression. They use unsigned fixtures and do not establish public double admission.

The new pins use real loopback QUIC+mTLS, receiver-derived authenticated identities, production WendelonDataServer::request_remote_lease, the real routed Control Ring handler, Ed25519-verified returned descriptors and real Raft commits. A test-only HrftService wrapper holds one fully parsed, authenticated request before forwarding it into the unchanged handler. It does not bypass the handler's current-epoch, leader or canonical-range checks. Both reordering orders are tested. A waiter timeout does not cancel its spawned RPC; the added cancellation case separately aborts and joins that actual RPC future while the already-received server request is held. Server-constructed replies and caller-observed replies are distinguished.

The controlled path represents delayed request handling followed by another independently submitted grant request. It is not evidence that the existing serial periodic manager automatically retries an unknown proposal, nor of leader change, process restart or packet-level loss. Report exact observed outcomes from the retained run, including failed development attempts. The larger public two-holder question remains limited: in the tested stable one-node canonical ring, a second real mTLS node cannot obtain this range (DENIED_INVALID_TARGET), and its existing final lease predicate is Missing. This is not a two-primary handoff test or a completed authenticated SQL mutation. Leader-change/restart/handoff double admission is coverage unavailable until the required implementation pins below.

The repair only changes lease proposal/apply semantics, fresh-cluster initialization and replay and their necessary compatibility plumbing. No transaction isolation, replay tenant scoping, C1/F16/F17, audit durability, timing priority or P3 stall policy repair is bundled.

## Selected rule: monotone comparison, not expected-prior CAS

Select a deterministic monotone comparison at the committed apply boundary. CAS alone prevents some stale updates but would still allow a matching-prior renewal to shorten a live interval; it adds expected-prior identity to both local/remote proposals and adds unknown-response recovery. Monotonicity directly protects the interval and uses the descriptor's existing epoch/HLC bounds. An expected-prior design is not selected for this prerequisite.

The rule is always on in the prerequisite binary, from its first committed entry. Apply it to AcquireLease; no Legacy policy, activation marker, management-only activation command or separate policy-specific acquire is selected. The implementation review freezes the representation. Existing LeaseDescriptor and its signature preimage remain byte-identical. Never edit or extend a signed descriptor in memory; retain or reject the complete actual descriptor.

Evaluate in this order, without reading host clocks or configuration during apply:

1. Require a valid committed current membership epoch. Incoming epoch must equal the current committed epoch. Older and uncommitted future epochs are LeaseDenied, even if their requested start follows a stored expiry. A missing/contradictory epoch history fails closed. Membership changes retain their existing sequential authority fence.
2. Reject malformed range, non-positive interval, overflow-inconsistent interval or unsupported grant representation. Proposal-side authorization remains required: authenticated holder, current leader/signer and canonical ownership. The state-machine epoch check closes the proposal-to-apply epoch race; it does not certify that a leader never changes after a response.
3. Exact byte-identical descriptor replay is Success without mutation. Equal starts/bounds with a different descriptor are not silently relabelled as an identical replay. Existing committed-epoch validation still precedes this replay exception.
4. For an exact-range, same-holder, same-epoch replacement, require both incoming start >= retained start and incoming expiry >= retained expiry, with at least one strict increase unless it is exact replay. Reject a later-but-shorter grant too. Compare full ordered HLC tuples, not only physical milliseconds. A request with an older start may not replace authority merely because the old record has now expired.
5. Sweep every overlapping retained interval. An overlapping current-epoch interval of another holder/range blocks a grant when retained expiry > incoming start. Never infer expiry from response latency, a dropped caller, process death or cache invalidation. New-epoch grants may supersede old-epoch intervals only through the existing committed epoch fence. An incoming old epoch never skips a newer record. Reversed/unmapped overlapping state fails closed rather than being erased.
6. Only after all predicates pass insert the incoming complete signed descriptor. A denied proposal changes no lease, epoch, ownership or response state and cannot cancel an earlier unknown proposal.

Wire result: reuse WendelonResponse::LeaseDenied and existing LEASE_RESP DENIED_OVERLAP for monotonicity refusals; no descriptor accompanies it. Proposal-side stale-epoch rejection remains DENIED_EPOCH_STALE. The local manager observes LeaseDenied; the remote caller observes Denied(status). Neither claims that its earlier unknown grant did not commit. Retain a current valid cache entry; retry/backoff belongs to the separately reviewed P3 policy. Do not report a rejected incoming descriptor as GRANTED while retaining another one: that would give the caller false authority. Exact replay Success returns the exact already-committed descriptor only.

ReleaseLease remains an explicitly authorized release under its existing contract, not a timeout-created release. Pins cover release races and whether a previously authorized pending acquire can reopen after release; any required change to release semantics returns for scope review. This plan makes no irreversible holder-incarnation fencing claim.

## Fresh clusters and format refusal (selected)

There is no existing-cluster upgrade or migration in this scope. Current formats are WAL4 and snapshot5; propose WAL5 and snapshot6, rechecked against the implementation base. Their semantics include the always-on monotone rule. No activation entry, persisted policy switch, existing-cluster E->E+1 fence or legacy semantic replay is needed.

The new binary refuses every older-format WAL or snapshot with a clear unsupported-format error, before replay, state mutation or startup readiness. No upconversion, in-place migration, header rewrite or automatic deletion. Enumerate and test every ingress: startup and recovery, WAL segments and mixed-version tails, snapshot loading/install, and backup restore. An old snapshot paired with a new WAL, or a new snapshot paired with an old WAL, must not bypass refusal. Pre-versioned artifacts are older formats too. Unknown future formats and malformed artifacts fail closed. Failure preserves the source artifacts; tests compare their bytes before and after. Any frozen old decoders retained for cheap offline inspection are isolated from production startup/recovery/install and are optional, not a deliverable.

Pin that an actual pre-prerequisite binary rejects the new WAL and snapshot versions. Pin new-format full replay versus snapshot-plus-tail equality for lease map, membership epoch and response/audit results, including monotonicity denial and exact replay. Snapshot installation cannot change the rule or erase the durable membership history that the rule requires. No historical legacy regression needs to be reproduced except insofar as old-format refusal requires its fixture.

Bootstrap must durably establish the initial membership epoch before the first grant. Missing or contradictory committed epoch history denies the grant; never substitute a host-local default. Pin initialization order and cold restart of a fresh new-format cluster.

Each new cluster has a fresh authority namespace: newly generated lease-signing keys and issuer identity, with a newly declared trust domain. Reusing an empty data directory, epoch1, a node number or a previous signing key is not fresh authority. Readiness binds the complete trusted issuer identities and public-key fingerprints to a retained inventory of EVERY issuer key in the diagnostic and soak roots, and requires the new keys to differ from all of them. Missing/unreadable inventory or an ambiguous reused identity holds readiness. Concrete issuer-identity representation and trust provisioning are frozen at source review; if the current representation cannot distinguish the new authority safely, return for scope review instead of silently reusing it. Deterministic development signing keys are not fresh-cluster production provisioning.

Tests present earlier-cluster signed descriptors to the new verifier, including coincident range, epoch and node numbers, and require rejection. Bind control-peer authentication/trust provisioning separately from lease-descriptor verification; changing a TLS certificate alone does not replace a lease-signing key. No existing directory is wiped or silently reclassified as fresh. Deployment reserves a fresh declared root exclusively and refuses unexpected state.

## Terms, restarts and rollback

3s->6s in the same epoch is accepted only if start and expiry are nondecreasing. 6s->3s (and90s->6s ADR080 profiles) may be refused until a newly requested descriptor covers the retained expiry; it is not silently shortened. An explicit new committed epoch fence can supersede the old epoch, only with the required final-gate, replica and finalization evidence below. The effective signed term is always measured, never inferred from a new default.

Retry: exact descriptor replay is idempotent; a regenerated later request follows the rule. A lost reply remains unknown until separately resolved. A restarted caller cannot revoke pending work; a late old proposal is refused if stale under the current committed epoch or monotone comparison, without reducing state. No global active-proposal or transport-cancellation guarantee is claimed from a local lane.

Leader change: current signer/issuer checks remain independent of monotone interval state. The new descriptor must have current authorized issuance and final gate checks. A late old leader result cannot manufacture current issuer authority. Tests cover proposal before leadership/epoch change and completion afterward; do not assert that the apply rule itself reads a host-local current-leader variable.

Operational rollback means redeploying a tested build that understands the new WAL/snapshot formats and always-on rule. An old binary on new data is refused, not a supported rollback. Pin that refusal; no header downgrade, log truncation, restore over newer state, automatic state deletion or reseeding. Availability policy can be disabled in a compatible tested build without disabling this safety rule. Offline old-format inspection, if retained, is not permission to start production on old state.

## Mandatory implementation evidence (not executed by this planning package)

- Apply: all rules above, both ordering orders, exact replay, equal-bound non-identical descriptors, malformed intervals, lower/future/missing epoch, distinct-holder overlaps and release cases. No mutation on denial. Use deterministic log/epoch barriers and no host-clock-dependent apply.
- Authenticated control: real mTLS local/remote grant paths, explicit response/descriptor binding, stalled/cancelled caller and late server completion, leader and membership change, and no forged holder. Distinguish server-generated reply from client receipt.
- Fencing: BOTH holders' actual final write gates and replica/finalization paths across holder handoff and epoch change under the NEW rule. Include writes admitted before the change and resumed after it, as well as late grants and callbacks. Inventory each gate's committed ownership/epoch and issuer checks. An existing gap is HOLD for a scope decision, not a passing prerequisite. A canonical rejection in a stable single-node ring is not this coverage.
- Formats: new binary refusing each older WAL/snapshot ingress without modifying artifacts; actual old binary refusing new formats; new-format full replay versus snapshot-plus-tail equality; mixed-format, corrupt and inconsistent-history controls. No legacy policy or activation fixtures.
- Config/readiness: explicit3s/6s/90s, cold restart with cached/unknown authority, initial durable membership before issuance, fresh signing keys and issuer identity versus the complete retained diagnostic/soak inventory, earlier-cluster descriptor rejection and isolated fresh-root provisioning. Report the effective signed term.
- Whole pinned1.95.0 ladder before promotion: all relevant Wendelon serialized tests, Wendelon-client, Toron/TPCH/client, architecture/format guards and release artifacts with clean same-source identities. Preserve failures. Inventory local/remote committed producers against P2 observation and attribution; register any new producer. Reusing AcquireLease does not excuse an unobserved new path. There is no activation producer in this plan.
- Source and operational package returns for Claude/user review. No staging or campaign until separately cleared.

## Authorized implementation sequence

Record this signed subject-only ADR-only plan amendment and explicitly fast-forward bigfoot main from3438b769 after checking its head. Origin remains held. Inventory first on an isolated implementation branch. Run the whole serialized Wendelon precheck plus architecture/format guards before signing; use cargo fmt -p only. Bring source, full pinned ladder, apply/format/fresh-authority/fencing pins and producer registration for review before rebasing P3. An existing fencing gap is HOLD for scope review. No staging or campaign is authorized.
