# Workstream A: Governance, ADR Closure, And Calendar Dependencies

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS0

## Mandate

Lock the program's semantics before implementation fans out. This workstream exists to prevent the team from building against unresolved assumptions and then paying for it twice.

## Architectural Position

Phase 0 is not paperwork. It is the control surface for the rest of the program.

My position is:

- No critical-path implementation starts while the semantics are still "proposed."
- ADR acceptance is the entry criterion for dependent code, not its exit condition.
- If an ADR open question can change client semantics, failure semantics, or compliance posture, it is a blocking dependency.
- Calendar-dominant work starts immediately, even if the code path will not be ready for months.
- ADR review needs a fixed weekly cadence or it will drift indefinitely.

## Scope

- close critical ADR open questions
- promote load-bearing ADRs to `Accepted`
- establish the design-lock decision log
- file the IANA PEN request
- kick off CMVP lab engagement
- start KMS vendor/legal onboarding
- establish the CI program scorecard and benchmark baseline artifacts

## In Scope ADRs

Minimum blocking set:

- ADR 018: identity, OID, cert rotation, CRL posture
- ADR 020: KMS abstraction, rotation semantics, escrow boundaries
- ADR 025: audit schema, evidence path, break-glass parameters
- ADR 026: FIPS build posture, transport boundary, evidence path

Second-order review set:

- ADR 019
- ADR 021
- ADR 027
- ADR 029

## Non-Goals

- implementing runtime hot-path refactors
- building customer-facing operator tooling
- writing the final compliance pack

Those belong to later workstreams. This workstream only makes those efforts buildable.

## Required Decisions

The following decisions must be explicitly locked in this workstream:

1. Stellarator failure semantics and fencing policy
2. transport truth-up strategy:
   - implement the claimed path now
   - or rename/retract the claim now
3. authoritative audit evidence story for the pilot
4. supported node replacement contract:
   - first-party operator
   - or tightly specified external automation
5. first production root-KEK provider target

## Deliverables

- ADR status updates with clear accepted/rejected decisions
- ADR acceptance log and review-minutes template
- `plans/gap/design_lock_20260422.md` or equivalent decision log
- CI scorecard artifact definition and dashboard
- benchmark baseline artifact set
- IANA PEN filing record
- CMVP lab kickoff record
- KMS vendor engagement record

## Work Packages

### A1. Decision Triage

Create a single list of unresolved decisions, grouped by:

- correctness blockers
- pilot blockers
- regulated-pilot blockers
- GA/compliance blockers

Do not allow duplicate decision threads to live in multiple docs.

### A2. ADR Closure Sprint

For each blocking ADR:

- convert open questions into explicit option sets
- force an owner recommendation
- record rejected alternatives
- move the ADR to `Accepted` only after the review group signs off

Recommended operating cadence:

- one ADR acceptance review per week
- published minutes every review
- explicit list of downstream workstreams unblocked by the decision

### A3. Calendar-Dominant Kickoffs

Start on Day 0:

- CMVP lab outreach and contract process
- KMS vendor legal/security review
- IANA PEN filing

These are not engineering wait states. They are long-tail dependencies and must begin immediately.

### A4. Design-Lock Log

The design-lock log should be append-only and every entry should have:

- decision owner
- date locked
- explicit rationale
- superseded-by field when changed
- explicit reopen conditions

I am adopting this because it is the minimum discipline needed to keep narrow but load-bearing decisions from disappearing into Slack and PR comments.

### A5. Scorecard And Baselines

Create the baseline measurement set for:

- Raft commit latency under isolated and mixed load
- request-admission latency and allocation profile
- Stellarator write latency
- crash/failure harness pass rate
- metrics completeness

If there is no baseline, there is no proof of improvement.

## Exit Criteria

- all critical-path ADR decisions are locked
- all blocking ADRs are `Accepted`
- scorecard exists and is populated
- baseline benchmark artifacts are published
- IANA PEN, CMVP, and KMS external processes are in flight
- no later workstream is blocked on unresolved semantics
- review cadence and ownership are explicit enough that ADR closure cannot silently stall

## Recommended Ownership

- direct owner: architecture lead
- required reviewers:
  - security lead
  - runtime lead
  - SRE/operations lead
  - compliance/program lead

## Risks

- ADR review drifts into philosophy debates rather than decision closure
- external legal/procurement timelines slip
- teams start coding against unofficial hallway decisions

## Opinionated Constraints

- Do not let every team maintain its own local interpretation of Phase 0 decisions.
- Do not merge implementation PRs against a still-`Proposed` ADR when the PR depends on its unresolved semantics.
- Do not accept "we can clarify this later" for anything that affects correctness, auth, or compliance posture.
- Do not accept an ADR as "accepted" if its only hard parts are still deferred to follow-up notes.

## Open Questions

- Which ADRs beyond 018/020/025/026 are truly blocking for the first customer-facing pilot?
- Do we require the review group to sign every accepted ADR, or can some low-risk decisions be delegated to the owning lead?
- What is the canonical home for the design-lock decision log so it does not diverge from the ADR set?
- How aggressively do we gate Phase 1 on external calendar items that are started but not yet completed?
- Do we want a hard one-ADR-per-week acceptance cadence, or should the review group be allowed to batch lower-risk ADRs once the blocking set is closed?
