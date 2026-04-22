# Workstream J: FIPS, Supply Chain, And Release Compliance

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS9

## Mandate

Turn the current FIPS and release-security story from a pilot engineering posture into a defensible enterprise release posture.

## Architectural Position

FIPS-ready and FIPS-certified are not the same thing. The plan must be explicit about that difference or it will create avoidable trust damage.

My position is:

- CMVP lab work starts immediately because the calendar is long
- transport TLS inside the defensible boundary is non-negotiable for the regulated SKU
- transport boundary truth matters as much as primitive selection
- the symbol sweep remains a regression test, not a marketing claim
- supply-chain artifacts are mandatory for regulated release classes

## Scope

- CMVP lab engagement
- FIPS transport path
- Security Policy authoring
- KAT/self-test evidence
- SBOM generation
- artifact signing
- SLSA target
- release compliance pack

## Non-Goals

- KMS provider implementation itself
- general runtime hot-path work
- Toron integration

## Work Packages

### J1. Lab And Boundary Lock

Start the lab process immediately and lock:

- target module boundary
- supported platforms
- transport story
- expected evidence set

### J2. Transport FIPS Path

Move the FIPS build to a transport path that can be defended.

If the transport path is not ready in time, be explicit:

- customer-facing pilot may be "FIPS-ready, certification in progress"
- regulated GA cannot claim certification without the full path

My bias is to treat transport-boundary inclusion as mandatory for the regulated SKU, not as a nice-to-have after certificate work has already started.

### J3. Self-Test Evidence

Expand from symbol sweep only to:

- KAT coverage
- startup self-test behavior
- documented failure mode

### J4. Supply Chain

Add:

- SPDX SBOM
- artifact signing
- SLSA target and attestation plan

Preferred staging:

- SBOM early
- signed artifacts early
- SLSA L3 later, but on a committed schedule

### J5. Compliance Pack

Produce a release artifact set that legal, compliance, and customer security teams can actually consume.

I want a distinction between:

- pilot evidence packet
- GA compliance pack

The former should ship earlier and be narrower. The latter is broader and more polished.

## Deliverables

- CMVP engagement record
- FIPS boundary note
- transport FIPS implementation or explicit fallback posture
- KAT/self-test suite
- SBOM generation
- signing pipeline
- SLSA roadmap/implementation
- customer-facing compliance package
- pilot evidence packet

## Exit Criteria

- the system has an honest and documented FIPS posture for its release class
- supply-chain artifacts exist for regulated release candidates
- transport and crypto claims are aligned
- CMVP track is active and visible if certification is not yet complete
- pilot and GA compliance artifacts are distinguished clearly enough that no one confuses "in progress" for "certified"

## Recommended Sequencing

1. lab kickoff
2. boundary decision
3. transport path
4. self-test evidence
5. SBOM/signing
6. compliance pack

## Risks

- upstream transport or crypto dependencies slip the certification path
- the team markets "FIPS" long before the evidence supports it
- supply-chain work is deferred because it feels non-functional until late in the cycle
- the team produces one giant compliance pack too late instead of a smaller pilot packet earlier

## Opinionated Constraints

- Do not let the symbol sweep be described as full FIPS validation.
- Do not claim a certified transport posture until the transport path is actually inside the defensible boundary.
- Do not leave SBOM/signing/SLSA until release week.
- Do not collapse pilot evidence and GA certification language into one ambiguous marketing claim.

## Open Questions

- What exact pilot language is acceptable if CMVP is in progress but not complete?
- Is `rustls-aws-lc-rs` mature enough for the target regulated-pilot window, or do we need a fallback posture in the merged release plan?
- What minimum SLSA target do we require before regulated pilot versus regulated GA?
- Which customer-facing compliance artifacts are mandatory for the first regulated pilot, and which can wait for GA?
- What platform set do we certify first, and which platforms are explicitly out of scope until a later validation cycle?
