# ADR 629 - trust in acknowledged writes: a three-node store soak

**Status:** PLANNED (reviewed; Codex 01a10349). This record authorizes Stage A only: A1 and A2
development. Within Stage A, host staging and runs wait for their own reviewed package (A3). No Stage B fix is
selected here. Stage C is reviewed separately.

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
