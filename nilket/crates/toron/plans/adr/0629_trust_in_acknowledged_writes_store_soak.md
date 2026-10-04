# ADR 629 - trust in acknowledged writes: a three-node store soak

**Status:** STAGE A A1 CHARACTERIZED and A2 REVIEWED INSTRUMENT DEVELOPMENT (limited diagnostic coverage).
A3 is HELD on P1–P4: authenticated reachable storage route, non-destructive finality observation, stored-effect
concurrency coverage where claimed, and real three-node readiness/placement/ingress checks. Stage B P1/P2
scope is reviewed; P1 implementation is authorized within that scope, with its package/ladder review before
staging. P2 coverage inventory may proceed as reading/design; its production work waits for P1 submission. Stage C remains separately reviewed.
This is not trust, durability or security acceptance for a store.

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
