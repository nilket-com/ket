# ADR 582 - Maintenance quarantine control surface

**Status:** OPEN. Implementation is under review. Closes ADR 0097 decision 4
when its acceptance holds. Deployment stays parked throughout.


## Context

ADR 0097 decision 4 bounds maintenance failures with durable receipts: a
deterministic failure backs off, quarantines after three attempts, and the
quarantine survives restart and upgrade until an explicit requeue clears it. The
Wendelon side is complete. The policy exposes a report and an operation-scoped
requeue, both exercised only by tests. An operator has no live, supported way to
see a quarantine or clear one, so the only remedy today is editing the receipt
table by hand. That is why decision 4 cannot close.

This is a cross-ring seam, and the rule for those is fixed: Wendelon owns the
choreography, Toron submits one request per logical operation, and Toron keeps
no cache of transient Wendelon admin state. So the record lives here and decides
the operator contract. The Wendelon half is more than serialization: bounded
source-level paging, a receipt-set fingerprint, and an administrative
reservation are small changes to the policy itself, and the record decides them
here so the wire and the policy are designed together.

Three properties of the existing machinery shape the contract.

The state is node-local. Receipts live in each node's persistent index and are
never replicated. The existing data-path client deliberately fans out to every
replica and merges, which is right for coverage and wrong here: a maintenance
verdict must stay attributable to one node.

The state is unbounded on the wire. The in-memory report can carry up to 65,536
receipts of up to 4,096 slabs each. A wire operation that built the whole report
and truncated afterwards would still allocate it.

Requeue clears by identity, not by review. It removes every quarantined origin
for one operation over one exact slab set. An origin quarantined between the
operator's read and the operator's command would be cleared without anyone
having looked at it.

This is cluster control, not tenant administration. A quarantine holds back
maintenance that serves every tenant on the node; resetting its budget is not a
tenant's decision. The control ring already distinguishes `ControlStatus` from
`ClusterControl`, and Wendelon already has a bounded administrative audit
record.


## Decision

**Path.** Operator command, to a Toron admin client, to one named Wendelon node,
over an authenticated control-ring request, to the maintenance policy. No
fan-out, no aggregation, no Toron-side cache. The node in the request is the
node in the response.

**Two verbs.**

- `maintenance report --node <id>`: node-local policy state, paged, as one
  canonical tagged stream of items:

  ```
  ReportItem =
      QuarantinedWork { operation, slabs, origins[], fingerprint }
    | RetryableCooldown { operation, slabs, failures, deadline }
    | InFlight { operation, slabs }
  ```

  A `QuarantinedWork` item groups every quarantined origin for its identity -
  each with origin, attempts, and bounded detail - and carries the fingerprint
  over all of them, so a command can name what the operator reviewed. Only
  that variant groups origins; every variant is paged and byte-bounded the same
  way, in one canonical order by variant, operation, then canonical slab set.
  Pages are bounded in items, in slab ids per item, and in encoded bytes; a
  cursor continues.

**A page is atomic by work identity.** The unit of the report is one work item:
one operation over one canonical slab set, carrying EVERY quarantined origin
for that identity and the fingerprint over all of them. Items sort by
operation, then canonical slab set; origins sort within the item. An item is
never split across pages, because any page that showed part of an identity
while supplying the fingerprint over the whole of it would let an operator
authorize a clear over origins never displayed - the unreviewed clearing this
record exists to prevent.

**Pages are built inside the policy and continue only between complete items.**
The page is assembled under the policy lock from the cursor's ordered position
until the next item would exceed the item bound or the encoded byte budget, so
the node never materializes the full report to cut a page from it. The cursor
carries a report incarnation and a policy generation. The incarnation is a
random value created at policy hydration, so a generation counter restarting
after a node reopen cannot make a pre-restart cursor look current; a cursor
from another incarnation is refused. The generation advances on every
report-visible mutation - receipt changes, retryable cooldowns, in-flight
reservations, and the enabled or disabled state - not only on receipt changes.
A cursor presented after the generation moved is refused as `StaleCursor`, and
the operator starts a new report. Pages are therefore a consistent view or an
explicit refusal, never a silent miss.
- `maintenance requeue --node <id> --operation <op> --slabs <exact set>
  --expected <fingerprint>`: compare-and-clear one reviewed quarantine. The
  node recomputes the fingerprint under its policy lock and clears only if it
  matches; otherwise nothing is cleared and the outcome says so.

**Compare-and-clear holds a reservation across the durable delete.** The policy
lock cannot be held across the asynchronous receipt deletion, and between
validating the fingerprint and deleting, a maintenance attempt could record a
new receipt under the same identity that the delete would then erase unreviewed.
The complete requeue sequence is pinned, and the audit events sit inside it:

1. authorize the actor and validate every wire bound;
2. emit the ATTEMPT audit event; if the sink refuses it, return
   `AuditUnavailable` and touch nothing;
3. lock; compare the fingerprint; reserve the exact slab set against
   maintenance admission, which refuses any attempt over that set at admission
   so its body is never entered; unlock;
4. delete the durable receipts;
5. clear memory and release the reservation;
6. emit the COMPLETION audit event.

A store failure at step 4 disables admission before the reservation is
released, as every other store failure does.

**The fingerprint is pinned.** It is a versioned, domain-separated canonical
SHA-256 over the COMPLETE quarantined receipt set for one operation over one
canonical slab set: operation, canonical slabs, and for every quarantined
origin its origin code, attempts, failure and deadline state, and bounded
detail, in one canonical encoding. It covers every quarantined origin for that
identity regardless of which report page displayed them. The derived metric
reason is not encoded. An unknown fingerprint version is refused, never read as
the current one.

**Permissions.** Report requires `ControlStatus`. Requeue requires
`ClusterControl`. A read-only observer can inspect the policy; a tenant
administrator cannot reset a budget that affects every tenant.

**Interface.** The primary mutation interface is a dedicated Toron admin command
backed by the control ring. Ordinary Rune and SQL are query surfaces and do not
carry privileged cluster control; a read-only Rune view over the report may
follow under its own record.

**The response is an envelope**, so a completion-audit failure has somewhere to
live without being confused with the outcome it follows:

```
RequeueResponse {
    node, operation, slab_set_summary, fingerprint,
    outcome: Cleared | PolicyDisabled | InFlight | NotQuarantined
           | StaleReport | AuditUnavailable | StoreFailure,
    completion_audit: NotAttempted | Recorded | Failed { diagnostic },
}
```

Outcomes:

- `Cleared { origins }`
- `PolicyDisabled { diagnostic }`
- `InFlight`
- `NotQuarantined`
- `StaleReport { quarantined_origins }` - the receipt set changed since the
  report. It carries a bounded count and no fingerprint: handing back the next
  authorization token would let a client retry over a set the operator never
  reviewed. Another report is required.
- `AuditUnavailable` - the audit sink refused the attempt event; nothing was
  cleared.
- `StoreFailure { maintenance_disabled: true }`

Report outcomes are the page, `PolicyDisabled { diagnostic }`, `StaleCursor`,
or a bound refusal naming the bound.

**Lost response.** A response lost after a successful clear is not reported as
success on retry. Doing so honestly would need a durable idempotency ledger,
which this record does not add. The contract is read-after-ambiguity: the client
fetches the report again. If the expected fingerprint is absent, the system has
converged and the client says so without claiming who cleared it; if the
fingerprint changed, a new operator decision is required.

**Audit fails closed, before mutation.** The audit sink is fallible, so "one
event whatever the outcome" cannot be promised as a single ordinary event: a
clear that succeeds and an emit that then fails would leave an unaudited
mutation. The ATTEMPT event is therefore step 2 of the sequence - actor, target
node, operation, slab count with a short prefix or digest, expected fingerprint
- and a refused attempt is `AuditUnavailable` with nothing touched. The
COMPLETION event is step 6 and carries the outcome. A completion emit failure
cannot undo a clear that already happened, so it is reported in the envelope as
`completion_audit: Failed` and logged, and the attempt event stands as the
durable record that a clear was authorized. When the attempt event itself was
refused there is no completion event, so the envelope says `NotAttempted`
rather than claiming either. Neither event carries the slab
list.

**Bounds are enforced before allocation, and the page is bounded in bytes.**
Page item count, cursor shape, slab-set size, and fingerprint length are
checked at the wire before the policy is consulted, and an oversized request is
refused with a named bound. A page also carries a maximum encoded byte budget:
assembly stops before the next complete item would exceed it, because a legal
item count of maximal slab sets can still exceed the control frame. One maximal
item - the slab-set bound with every origin quarantined - must itself fit the
protocol frame limit, and that fit is a compile-time or startup assertion, not
a runtime discovery.


## Acceptance

The decisive test runs against two real nodes, each with its own index.

1. Quarantine different work on each node, reopen both indexes, and prove the
   quarantines are the hydrated state, not the pre-reopen state.
2. Report node A and prove nothing from node B is returned; the response names
   node A.
3. Test actors, not permission names: a read-only observer can report and
   cannot requeue; a tenant administrator cannot requeue node-wide state; an
   operator with cluster control can do both.
4. Requeue one operation on A with the reported fingerprint and prove another
   operation over the same slabs survives another reopen.
4a. Interleave: between fingerprint validation and durable deletion, submit
   maintenance work over the same identity that would fail deterministically,
   and prove it is refused at admission as in flight with its body never
   entered, so no unreviewed receipt can be written; after release, prove the
   next attempt records normally.
4b. Prove `StaleReport` carries no fingerprint, and that a client holding only
   the stale response cannot requeue without a fresh report.
5. Exercise every outcome: disabled, in-flight, not-quarantined, stale report,
   audit-unavailable with nothing cleared, and receipt-store failure, which
   also leaves the node's maintenance disabled. The stale report arises under
   normal policy behaviour, not by a second origin quarantining behind a
   quarantine, which admission forbids: take a report, have another operator
   requeue the identity, let the work fail its way back to quarantine, then
   submit the first report's fingerprint and prove `StaleReport`.
6. Drop a successful response and prove the client performs read-after-ambiguity
   rather than reporting an idempotent success.
7. Prove an oversized page request and an oversized slab set are refused before
   the node builds a report or touches the policy; prove a page never splits a
   work item and stops before the byte budget; prove one maximal item fits the
   frame; prove a cursor after a receipt change, a cooldown change, a
   reservation, or a disable is refused as `StaleCursor`; prove a cursor from
   before a node reopen is refused by incarnation even when the generation
   counter matches.
8. Prove the attempt event precedes mutation and the completion event follows
   it, both bounded and without the slab list; prove a refused attempt event
   clears nothing; prove a failed completion emit after a successful clear is
   reported as `completion_audit: Failed` alongside `Cleared`.
9. Prove a fingerprint covers every quarantined origin for the identity, not
   only the origins on the displayed page, and that an unknown fingerprint
   version is refused.


## Guardrails

- The Toron side never spells a Wendelon-internal type and never caches a
  report. A stale report is a wire outcome, not a client-side reconciliation.
- Requeue without an expected fingerprint is refused at the wire. There is no
  unconditional clear.
- The response's node is the node that served it. A client that routes to a
  different node than it was asked for is a defect, not a fallback.
- The report never aggregates across nodes and never fans out. A future
  cluster-wide view composes node reports and says which node each row is from.


## Risks

- Fingerprinting hashes the complete quarantined receipt set for one identity
  under the policy lock. That set is bounded by the origin taxonomy, not by any
  page bound, so the hash is over at most one receipt per origin; the cost is
  measured in the acceptance run, not assumed.
- A generation-bearing cursor refuses a page after any report-visible mutation
  on the node - receipts, cooldowns, reservations, or enabled state - so a busy
  node can make a long report restart. The alternative, a live
  cursor that may silently miss concurrent inserts, was rejected because the
  report is what the operator reviews before authorizing a clear.
- Read-after-ambiguity puts a second round trip on the operator's rare path
  rather than a ledger on every node's hot path. That is the intended trade.
- A read-only Rune view is deferred. Until it lands, the report is reachable
  only through the admin command, which is the audience it serves.


## Forward

One plan commit and one implementation commit. The implementation touches the
Wendelon control ring (two operations, their permissions, the fingerprint, and
the audit event), a control-ring client method in Toron's admin client, and the
`maintenance` verb in the Toron command line. Review corrections fold into the
implementation commit until the round closes; nothing is pushed before that.

Two follow-ups remain deliberately outside this contract. Deterministic attempts
one and two are durable backoff receipts but do not fit the report grammar above;
if operators need that pre-quarantine visibility, add a distinct `BackingOff`
variant rather than misreporting them as retryable races. The page byte estimate
is backed by a fail-closed encoder and a maximal-item fixture; a future wire-schema
change should add an encoder-dominance test before changing either side of that
bound.

The implementation also narrows ADR 044's architecture guard from the generic
word `fingerprint` to certificate- and TLS-fingerprint spellings. ADR 582's
quarantine fingerprint is not an authentication model, while those narrower
spellings retain ADR 044's boundary.
