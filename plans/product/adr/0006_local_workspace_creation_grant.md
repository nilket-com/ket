# ADR 0006: creating a local workspace is a one-shot, attributed grant

Status: accepted 2026-08-02. This record implements stage 2 of PRODUCT-0002
section 9: visible, attributed startup preserving process ownership. It answers
the single question PRODUCT-0005 reserved — who may create a local workspace when
discovery returns `Absent` — and changes nothing about the observer contract.

## Context

PRODUCT-0005 made the local workspace observable. Discovery classifies a profile
into eight states and attaches without ceremony when one verifies, but it is
forbidden to create, clean, signal, or own anything. `Absent` is therefore an
honest dead end: the interface can name it and can do nothing about it.

TORON-532 supplies the missing half. `nilket local` is a foreground supervisor
that generates configuration, holds an ownership lock, starts two role processes
in the correct order, and drains them in the reverse one. It is deliberately not
a daemon, and it protects its own children with a parent-death contract.

What is missing is the edge between them. A user whose workspace is not running
must leave the product, find a terminal, and type a command whose name the
interface already knows. Closing that gap is a lifecycle decision, which is why
PRODUCT-0004 left `ket local` unassigned and PRODUCT-0002 staged availability
rather than asserting it.

The stage boundary is what keeps this record small. Remembering a user's consent
is a different authority question from performing one act on request, and fusing
them would blur whether a failure belongs to startup mechanics or to persistent
mode policy. This record decides only the act.

## Decision

### 1. `Absent` permits an offer; it does not authorize creation

Observing that no workspace exists licenses exactly one thing: showing the user
that none exists and that one can be started. No discovery state authorizes
creation, and no other state — `Starting`, `Stale`, `Corrupt`, `Incompatible`,
`Unverifiable` — produces an offer. A profile that cannot be described is not a
profile that may be replaced.

The offer names what would be created and where. It is a proposal, not a
progress indicator.

### 2. Acceptance grants exactly one creation attempt

Explicit user acceptance authorizes one attempt against the resolved profile
root. The grant is consumed whether the attempt succeeds or fails.

A failed attempt does not retry, escalate, reset, or re-offer without a new
acceptance. This record confers no remembered preference, no automatic retry, no
lazy start, and no daemon semantics. Consent is per-act, and its persistence is a
separate authority decision belonging to stages 3 and 4.

### 3. Nilket remains the supervisor; the product is only the initiator

Creation spawns Nilket's existing local profile command. The product does not
generate configuration, provision PKI, choose tenancy, select ports, order role
startup, or supervise roles. Every one of those remains Nilket's, unchanged.

The initiator's contribution is attribution: it decides that an attempt happens,
records that this session made it, and holds the resulting process handle. That
is the whole of its authority.

### 4. Creation is ordered so that failure leaves no false identity

The supervisor's startup order becomes:

```text
validate candidate -> acquire ownership lock -> typed endpoint preflight
  -> generate profile and PKI -> start roles -> publish runtime
```

Endpoint preflight moves ahead of generation. A failed attempt may leave a
minimal profile root and its lock file, because those are shape rather than
identity: under PRODUCT-0005 they classify as `Absent`, which is true — no
workspace was created. It must not leave a profile marker, a development PKI, or
any other identity artifact describing a deployment that never existed.

This ordering matters more now than it did when a human typed the command.
Creation performed on someone's behalf must not durably alter the profile as a
side effect of failing.

### 5. Preflight evidence names the requirement, not the holder

A typed endpoint conflict identifies the role that needs the endpoint, the
transport, the endpoint itself, and the fact of conflict. It does not speculate
about who holds it.

The available evidence supports exactly two claims, and they come from different
observations:

- a **held profile lock** proves another Nilket supervisor owns this profile; and
- an **occupied endpoint under a freely acquired profile lock** proves a creation
  conflict, and nothing more.

Whether the occupant is a foreign process, a stale listener, or another product
is not observable from a failed bind. Naming it would be the observer's
limitation stated as a claim about the system, which PRODUCT-0005 rejects in the
discovery path and this record rejects in the creation path.

### 6. Success is proved by discovery, not by process creation

A spawned process is not a workspace. After launching, the initiator watches the
PRODUCT-0005 contract move `Starting` to `Ready` and attaches only on `Ready`.
It does not infer availability from a successful spawn, an exit status, or a
readiness line it parsed itself.

This keeps one definition of readiness across the product and means a creation
that starts a process but never produces a workspace fails visibly rather than
presenting an unusable attachment.

### 7. Lifetime is bound to the initiating session

A workspace created this way must not outlive the session that created it. With
daemon semantics excluded there is no honest alternative: a foreground supervisor
that survives its parent, with no controlling terminal and no service manager, is
a daemon in everything but name.

- **Graceful exit** sends `SIGINT` to the supervisor this session launched, and
  waits for Toron to drain before Wendelon.
- **Abrupt death** triggers a parent-death contract on that supervisor, whose own
  role contracts then prevent orphaned descendants. The parent-death setup must
  recheck the captured parent identity to close the setup race.
- **An attached workspace is never stopped.** A deployment this session merely
  discovered is untouchable, exactly as PRODUCT-0002 section 9 requires.

TORON-532 establishes the Nilket-to-role edge. The product-to-Nilket edge does
not exist yet and is created here; it reuses that pattern rather than inventing a
second one.

### 8. Ownership is a live handle, never an inference

Authority to stop comes only from the child process handle this session created.
It is never derived from profile identity, a runtime manifest, a session nonce, a
PID read from disk, or any discovery state.

A workspace that matches every identifying detail of one this session started, but
for which no live handle is held, is an attached workspace and is not stopped.
This is the rule that keeps a restart, a crash, or a recycled PID from converting
an observer into an owner.

## Rejected

- **Treating `Absent` as authorization:** collapses observing that something is
  missing into permission to create it, which is the distinction stage 2 exists
  to preserve.
- **Offering on `Corrupt`, `Incompatible`, or `Unverifiable`:** proposes creation
  over a profile whose condition is unknown or damaged, where the honest act is
  to report and stop.
- **Remembering consent in this record:** a persistent authority is a different
  decision, and combining them would make a startup failure indistinguishable
  from a mode-policy failure.
- **Automatic retry after a failed attempt:** converts a one-shot grant into an
  open-ended one and can hammer a conflicted endpoint on the user's behalf.
- **Preflight after profile generation:** leaves identity artifacts and
  development PKI describing a deployment that was never created.
- **Naming the occupant of a conflicted endpoint:** states the observer's
  limitation as a fact about the system.
- **Inferring readiness from a successful spawn:** creates a second definition of
  ready beside the one PRODUCT-0005 already owns.
- **Letting a created workspace outlive its initiator:** daemonization without
  the word, without a stop grammar, and without a service model.
- **Deriving stop authority from profile identity or discovery state:** lets an
  observer become an owner, and would let one session stop another's deployment.
- **Teaching the product to supervise roles directly:** duplicates TORON-532's
  supervisor and discards its ordering guarantees.

## Consequences and sequencing

- `Absent` stops being a dead end. The shortest path from a cold machine to a
  working workspace no longer leaves the product.
- The product gains its first child process and therefore its first process
  lifetime obligation. That obligation is bounded to one handle, one session, and
  one signal path.
- Nilket's startup order changes once, in a direction that is correct
  independently of this record: no caller benefits from PKI generated before a
  preflight that will fail.
- **Closing the editor stops the workspace.** The next session forfeits instant
  reattachment and pays recovery cost instead — measured at roughly 400 ms cold
  against effectively zero to attach, over a store that has already reached 267 MB
  in ordinary dogfooding, and which grows. This is recorded as a known cost, not
  an oversight.
- That cost is the intended pressure toward stages 3 and 4. A user who pays it
  repeatedly is the evidence that remembered consent, lazy availability, and an
  honest service model are worth their own records. Those records inherit this
  mechanism; none of them is smuggled in here.
- PRODUCT-0005's observer contract is unchanged. Discovery still creates nothing,
  cleans nothing, signals nothing, and owns nothing.
- Implementation receives its own plan and must land offer-authority, creation
  ordering, preflight typing, readiness-by-discovery, lifetime, and
  ownership-inference proofs with positive controls.
