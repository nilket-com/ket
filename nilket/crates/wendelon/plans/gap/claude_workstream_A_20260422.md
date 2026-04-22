# Workstream A — Governance, ADR Closure, and Calendar Dependencies

**Merged-plan anchor:** WS0
**Author / Role:** Claude, as Lead Architect on Workstream A
**Version:** v2 — integrates peer review of `codex_workstream_A_20260422.md`. Claude-Codex overlap on this workstream is near-total; the documents are harmonized rather than competing. Points of agreement, points of refinement, and remaining divergences are called out in §9.
**Phase alignment:** Phase 0 (Foundation), weeks 1-6, with continuing ADR acceptance cadence through Phase 2.
**Gap items closed:** R-8, R-13, R-14, R-17 (schema piece), R-29, plus process substrate for every other workstream.

---

## 1. Why this workstream exists

Every other workstream in the merged plan depends on unresolved ADR Open Questions, unfilled IANA paperwork, unsigned KMS vendor contracts, or a CI scorecard that does not yet exist. If WS0 does not land, the remaining nine workstreams will either block on their dependencies or — worse — land against assumptions that later turn out to be wrong. Neither failure mode is recoverable on the calendar we have.

This workstream is therefore positioned as the program's *foundation*, not its *administration*. Governance here is load-bearing architecture: the decisions made or deferred in Phase 0 permanently shape what Phase 1-5 can deliver.

## 2. Strategic position (opinionated)

I take four architectural positions, each of which I will defend against "let engineering start; we can fix governance later":

### 2.1 ADR acceptance is the entry criterion for dependent code, not its exit

Today every reviewed ADR (018, 019, 020, 021, 025, 026, 027, 029) is *Proposed*. Regulated procurement treats Proposed as unstable. Engineers land code against Proposed ADRs routinely, and when an Open Question resolves differently than assumed, the code is wrong. The fix is not more careful engineering; it is ADR acceptance *before* the code that depends on it.

**Rule:** No PR that implements a semantic from an ADR merges while the ADR is still Proposed. The PR description must cite the ADR at Accepted, with the acceptance date.

This is stricter than the "Proposed is fine for pilot" posture the current repo has been operating under. It is deliberately stricter. Pilot is where the semantics get exercised; pilot against an unstable ADR is pilot against an unstable product.

**Companion rule (adopted from Codex's WS-A):** An ADR is not "Accepted" if its only hard parts are deferred to follow-up notes. A Proposed ADR whose Open Questions resolve by writing "resolves via a follow-up design note" has not been accepted; it has been postponed. Acceptance requires the hard parts in the ADR itself, not in a follow-up document that may or may not be written. This closes a loophole where ADRs accumulate "Accepted, pending note" status and ship with the pending-note never landing.

### 2.2 One ADR per week acceptance cadence; architect owns the calendar

A review group that accepts ADRs "when we get around to it" accepts zero ADRs. Phase 0 runs a hard cadence: **one ADR per week, Friday review, published acceptance minutes.** The review group is 4 people: architect lead, security architect, compliance lead, and one rotating engineer from any workstream. Each week publishes:

- The ADR being reviewed.
- The closed Open Questions and their rationales (one paragraph each, "because").
- The rejected-alternatives list explicitly.
- The acceptance date and the set of downstream workstreams unblocked.

Eight ADRs in six weeks is achievable; two weeks of slack covers re-reviews if any Open Question re-opens the decision.

### 2.3 Design-Lock Log is append-only and each entry has a decision owner

Not every decision belongs in an ADR. The merged plan identifies five "locked decisions required by Gate 0" (Stellarator fencing policy, transport truth-up path, pilot audit evidence source of truth, supported node replacement contract, first production root-KEK provider target). These are narrower than ADR scope but wider than a PR. They go in `plans/gap/design_lock_20260422.md`.

**Canonical home and ADR relationship (resolving Codex's open question on log-vs-ADR divergence).** The Design-Lock log lives at `plans/gap/design_lock_20260422.md` (and supersedes `plans/gap/design_lock_<date>.md` on future major revisions). Each DL entry either:

- References an ADR that governs the decision space (DL narrows the ADR to a concrete choice inside its option set), or
- Stands alone when the decision does not rise to ADR scope (operational conventions, staffing decisions, naming conventions).

Source-of-truth rules:
- If an ADR and a DL entry conflict, the ADR wins. DL is a refinement, not an override.
- If a DL entry invalidates an ADR assumption, the ADR is re-opened (Proposed again) and re-reviewed. DL does not silently supersede ADRs.
- Every DL entry includes a `related_adrs` field listing ADRs it refines.

This prevents the divergence failure mode: a DL entry quietly drifting from the ADR it refines, which is the risk Codex correctly flagged.

**Structure, opinionated:**

```
## DL-001: Stellarator failure semantics
- Decision owner: <name>, <role>
- Date locked: 2026-05-08
- Decision: On local pending-intent persistence failure, the node fences itself from quorum participation for 30s and the write returns FinalizationTerminalFailure; see WS1.
- Rationale: One paragraph.
- Supersedes: —
- Reopens on: <explicit conditions>
```

The log is append-only. If a decision needs to change, DL-001a supersedes DL-001; neither is deleted. This gives the program an audit trail of its own architectural evolution — and, importantly, gives new joiners a way to read what was decided and why without having to ask the person who wrote it.

### 2.4 The CI Program Scorecard is the truth, not the tracker

Tracking tools (Jira, Linear, project spreadsheets) drift. The CI Program Scorecard does not, because it is generated from the same tests the program must pass. If a workstream's exit criterion is "≥1,000 injected crash iterations without stranded quorum-durable writes," the scorecard row for that criterion is a CI job whose green/red state *is* the workstream's completion state. A workstream owner cannot mark themselves done against a red scorecard; a workstream owner can never mark themselves done by declaring intent, only by moving the scorecard.

**Rule:** Gate reviews read the scorecard first. A gate cannot be approved while the scorecard is red on that gate's rows.

## 3. Design — the four deliverables of Workstream A

### 3.1 ADR acceptance pipeline

**Deliverable:** `plans/adr/ACCEPTANCE.md` documenting the process; one-ADR-per-week cadence; acceptance minutes template; list of dependent workstream + gap items unblocked per ADR.

**ADR priority order for Phase 0 (architect opinion):**

Codex's WS-A distinguishes a tight blocking set (018, 020, 025, 026) from a second-order set (019, 021, 027, 029). I agree with this split and make it explicit:

**Tier-1 (blocking; full review-group acceptance required):** ADR 018, 020, 025, 026. These four drive every downstream workstream in the program; accepting any of them loosely is a cascading risk.

**Tier-2 (accepted with architect-lead sign-off; review group optional):** ADR 019, 021, 027, 029. These have fewer Open Questions, their semantics are less exposed to customer-facing contracts, and gating them behind full review-group approval consumes Phase-0 bandwidth without improving outcomes.

Weekly schedule:

1. **Week 1:** ADR 018 (Identity / dual-CA / OID). Tier-1. Blocks WS4, WS5, WS6. Open Questions: IANA PEN (filed in parallel; decision locks when assignment lands), cert-rotation service placement, CRL distribution endpoint, OCSP responder topology. I argue for state-machine-distributed CRL (see WS-C §3).
2. **Week 2:** ADR 020 (L2 Envelope Encryption / DEK Lifecycle). Tier-1. Blocks WS4, WS7 (backup encryption), WS6 (crypto-shredding). Open Questions: KMS backend abstraction, Tenant KEK rotation orchestration, migration sweeper throughput. I argue for AWS KMS + GCP KMS + Azure Key Vault + PKCS#11 from day one (WS-B; position contested by Codex's WS-E — see WS-B §9).
3. **Week 3:** ADR 025 (Audit event model). Tier-1. Blocks WS6. Open Questions: schema versioning, signed-root key separation, break-glass schema. I argue for separate signing key for regulated SKU.
4. **Week 4:** ADR 026 (FIPS 140-3 target). Tier-1. Blocks WS9. Open Questions: aws-lc-rs pinned-version CMVP state, transport TLS plan, entropy statement, lab selection. I argue for `rustls-aws-lc-rs` migration as a Phase 1 deliverable (WS-E).
5. **Week 5:** ADR 021 (Tenant-aware architecture). Tier-2. Blocks WS4/5/6. Open Questions: cross-tenant copy, default-tenant bootstrap.
6. **Week 6:** ADRs 019 (Cipher Selection), 027 (Observability), 029 (Identity/Locality). Tier-2. Batch review + accept.

Two weeks of slack if any ADR needs re-review.

**Exit artifact:** `plans/adr/ACCEPTANCE_LOG_20260422.md` records each week's acceptance with the dependent workstreams unblocked.

### 3.2 IANA Private Enterprise Number registration

**Deliverable:** Filed PEN assignment with IANA; OID frozen in ADR 018 at acceptance.

The PEN registration is paperwork with a calendar cost (typical 4-8 weeks). It starts **Week 1**, in parallel with ADR 018 review, not sequentially. Registration should be under the owning organization's legal entity, not an individual's name.

Once PEN is assigned, the OID hierarchy is:

```
<PEN>.wendelon
    .1 = identity-extension (ADR 018 OID extension)
    .2 = audit-chain (ADR 025; reserved)
    .3 = tenant-admin (ADR 021; reserved)
```

**Opinionated position:** register more OID sub-arcs than we currently need. OID paperwork is cheaper when batched; adding sub-arcs later is possible but messier. Reserve identity (`.1`), audit-chain (`.2`), tenant-admin (`.3`), crypto (`.4`), diagnostic (`.5`) at the same filing. Unused arcs cost nothing.

Every certificate issued before PEN freeze is pre-production. Enforce at issuance: the cert-rotation service refuses to sign until ADR 018 is Accepted with the frozen OID.

### 3.3 CMVP lab engagement

**Deliverable:** Lab contract signed by end of Phase 0, Week 4; engagement kickoff meeting held Week 6.

Lab selection is a procurement decision, not an architectural one. The architect's role here is to scope the engagement correctly. Wendelon is a Level-1 software module; the boundary is the set of code paths that invoke validated primitives. Lab scope must include:

- Security Policy authoring (lab co-authored, architect reviewed).
- Testable boundary definition: at-rest crypto module (Phase 0/1), with transport boundary expansion scheduled at Phase 1 (WS-E §3.1).
- Platform coverage: Linux x86_64 and aarch64, specific kernel-version floors.
- Operational test plan: self-test, KATs, conditional tests.

**Opinionated position:** contract is fixed-scope with fixed milestones, not time-and-materials. Labs that quote T&M on a Level-1 software module are either inexperienced or inflating; the module boundary is bounded.

**Candidate labs:** atsec, Leidos, Acumen, UL. Selection by procurement. Architect-side input: confirm any lab you pick has shipped a CMVP validation for a Rust-language crypto boundary inside the last 24 months. This is not common; lab familiarity with Rust matters because the Security Policy must accurately describe how primitives are called from `aws-lc-rs` into the caller language, and labs less familiar with Rust will produce a policy that's weak or wrong.

### 3.4 KMS vendor procurement

**Deliverable:** Signed enterprise agreements (or SaaS sandbox keys) with AWS KMS, GCP Cloud KMS, Azure Key Vault by end of Phase 0 Week 6. PKCS#11 HSM vendor selected for the regulated SKU; purchase order issued by Phase 1 Week 4.

**Opinionated position:** contract all three cloud KMS providers, not just one. The engineering cost of "we only support AWS for v1" is saved once; the customer-procurement cost of "we only support AWS" is paid every sale. Cloud-KMS providers are commodity; picking one and waiting to do the other two is false economy.

PKCS#11 HSM vendor: Thales Luna or Entrust nShield by default; SoftHSM2 as the software-HSM test backend. Pilot customers procure their own physical HSM; Wendelon's job is PKCS#11 compatibility testing.

### 3.5 CI Program Scorecard wiring

**Deliverable:** `ci/program_scorecard.json` generated on every PR and nightly; dashboard at `ops/dashboards/program_scorecard.json`; Slack alert integration.

Architecture (terse):

- **Sources.** Each row sources from a CI job output. Hot-path benchmarks run in criterion and emit `target/criterion/*.json`. Chaos-matrix runs in `tests/chaos.rs` and emits scenario pass/fail JSON. Fault-injection runs in a separate nightly job and emits iteration counts. Metric-completeness runs a catalog check against `src/metrics/registry.rs`.
- **Aggregation.** A nightly job reads all sources, merges into one JSON blob, compares against per-gate targets, produces green/red per gate.
- **Visualization.** A static HTML dashboard rendered from the JSON; hosted on a stable internal URL.
- **Gate review flow.** Gate review meeting opens with the scorecard shown, green/red on each row. If any row is red, the gate does not pass regardless of what the workstream owners say.

**Opinionated position:** the scorecard is *read-only* to workstream owners. Only the architect lead changes gate target values, and only with a signed Design-Lock entry. This removes the temptation to move the goalpost.

## 4. Work plan (ordered, Phase 0)

| Week | Deliverable | Owner | Exit |
|---|---|---|---|
| 1 | IANA PEN filed | Compliance lead | Filing receipt |
| 1 | CMVP lab RFP issued to 3 labs | Compliance lead | 3 responses by W3 |
| 1 | KMS vendor negotiations opened (AWS, GCP, Azure) | Legal + Architect | 3 parallel tracks |
| 1 | ADR 018 review scheduled | Architect | Week-1 review held |
| 1 | CI scorecard skeleton committed | SRE | `ci/program_scorecard.json` exists, all rows present with "not_yet_wired" status |
| 2 | ADR 018 accepted; OID frozen (pending PEN) | Architect | Acceptance minutes + ADR edit |
| 2 | Benchmark baseline harness committed | SRE + WS2 lead | `ci/baselines/20260508.json` captured |
| 2 | ADR 020 review | Architect | Week-2 review held |
| 3 | ADR 020 accepted | Architect | Acceptance minutes |
| 3 | ADR 025 review | Architect | Week-3 review held |
| 3 | Fault-injection harness skeleton committed | WS1 lead | Harness exists; 10 iterations green |
| 4 | ADR 025 accepted | Architect | Acceptance minutes |
| 4 | ADR 026 review | Architect | Week-4 review held |
| 4 | CMVP lab contract signed | Compliance lead | Countersigned contract |
| 4 | KMS vendor contracts signed | Legal | Executed |
| 5 | ADR 026 accepted | Architect | Acceptance minutes |
| 5 | ADR 021 review + accept | Architect | Acceptance minutes |
| 5 | Design-lock log committed with DL-001..005 | Architect | `plans/gap/design_lock_20260422.md` |
| 6 | ADRs 019, 027, 029 batch review + accept | Architect | Acceptance minutes |
| 6 | CMVP lab kickoff meeting | Compliance + Architect | Kickoff minutes; platform list shared |
| 6 | Phase 0 exit review | Architect + program | Scorecard green on "ADR acceptance", "baseline captured", "calendar items kicked off" |

## 5. Interfaces with other workstreams

- **WS-B (Crypto):** unblocked by ADR 020 acceptance (Week 3) and KMS vendor contracts (Week 4). Can begin design work in parallel; cannot ship provider code before Week 3.
- **WS-C (Identity):** unblocked by ADR 018 acceptance (Week 2) and IANA PEN assignment (when it lands; mid-Phase-1 typical).
- **WS-D (Audit):** unblocked by ADR 025 acceptance (Week 4).
- **WS-E (FIPS):** CMVP lab engagement starts in Phase 0 and runs through Phase 2; lab is a direct dependency of WS-E delivery.
- **WS1/2/3 (Codex-owned runtime workstreams):** baseline capture and fault-injection harness skeleton are WS0 deliverables that WS1/2 consume.

## 6. Exit criteria

1. All eight reviewed ADRs are Accepted. No ADR dependent on Phase-1 work remains Proposed.
2. IANA PEN filed; assignment expected during Phase 1 (ADR 018 retains a frozen-on-assignment clause).
3. CMVP lab contracted and kicked off. First Security Policy draft scheduled.
4. KMS vendor contracts executed for AWS, GCP, Azure; PKCS#11 HSM vendor selected.
5. CI Program Scorecard wired; every row has a measurement source, even if current value is zero.
6. Design-Lock log initialized with at least DL-001 (Stellarator fencing), DL-002 (transport truth-up), DL-003 (audit evidence source of truth), DL-004 (node-replacement contract), DL-005 (first production Root KEK provider).
7. Phase-0 exit review held; architect signs off against scorecard-green rows.

## 7. Risks within this workstream

- **ADR review stalls on cross-ADR consistency concerns.** Mitigation: batch cross-cutting Open Questions into a single review; architect has the tie-breaker vote.
- **IANA PEN assignment delayed beyond Phase 0.** Mitigation: ADR 018 accepts *pending* PEN freeze; cert-rotation service refuses signing until freeze lands.
- **KMS vendor legal negotiations drag.** Mitigation: three parallel tracks from Week 1; if one vendor's legal is slow, the other two still unblock WS-B work.
- **CMVP lab selected turns out to lack Rust familiarity.** Mitigation: RFP explicitly asks for prior Rust-language CMVP validations; tie-breaker is architect review of past Security Policies.
- **Scorecard wiring takes longer than a week.** Mitigation: the scorecard skeleton in Week 1 uses "not_yet_wired" as a legal status for rows; wiring fills in over Phase 0. The *skeleton* must exist Week 1; the *values* fill in by Week 6.

## 8. Opinionated Constraints (adopted framing from Codex's WS-A)

Short, non-negotiable rules for Workstream A execution:

- **No team maintains its own local interpretation of Phase-0 decisions.** The Design-Lock log is the canonical source; hallway decisions are not decisions.
- **"We can clarify this later" is not accepted for any decision affecting correctness, auth, or compliance posture.** It may be accepted for naming conventions or minor ergonomic choices.
- **An ADR is not Accepted if its only hard parts are deferred.** (Borrowed verbatim from Codex.)
- **Calendar-dominant work starts Day 0.** No engineering-readiness precondition gates IANA PEN, CMVP lab, or KMS vendor legal work.
- **The review group signs Tier-1 ADRs; the architect lead signs Tier-2 ADRs.** Phase-0 bandwidth is finite; use the review group where it matters.

## 9. Points of agreement and divergence with Codex's WS-A

Overall: strong alignment. Codex and Claude wrote substantially the same workstream with different emphasis. Explicit notes:

**Agreement:**
- Phase 0 is control surface, not paperwork.
- Calendar-dominant kickoffs start Day 0.
- Design-Lock log is a first-class artifact.
- The four Tier-1 blocking ADRs (018, 020, 025, 026).
- Symbol sweep / dev-insecure / cert-rotation-service are the specific decisions Phase 0 must lock.

**Refinement from Codex:**
- "Not Accepted if hard parts deferred" — adopted verbatim in §2.1.
- Tier-1 vs Tier-2 ADR distinction — adopted in §3.1.
- DL canonical home and ADR-relationship rules — adopted in §2.3.

**Divergence (both positions stand):**
- Codex does not specify the ADR review cadence (I specified one per week).
- Codex does not propose OID sub-arc reservation (I propose 5 arcs at PEN filing).
- Codex is agnostic on CMVP lab selection; I argue for a Rust-familiarity filter.

No outright conflicts; the documents are complementary.

## 10. Open questions (end-of-document, as requested)

These are the questions I want the council to take positions on before Phase 0 exit. Each is a decision that shapes a downstream workstream, and deferring them costs more than answering them wrongly.

1. **OID sub-arc reservation scope.** I propose reserving five sub-arcs (identity, audit-chain, tenant-admin, crypto, diagnostic) at initial filing. Too conservative? Too liberal? Are there sub-arcs we will need that I have not named?

2. **ADR acceptance review group composition.** I proposed 4 people (architect lead, security architect, compliance lead, rotating engineer). Is a separate customer-facing review cycle needed? A procurement reviewer as a permanent member? I lean no — procurement's input is at Gate 3, not per-ADR — but I want the council's read.

3. **Design-Lock ownership when architect lead is absent.** If the architect lead is unavailable for a review, does the security architect have signing authority on DL entries in the meantime? I lean yes with an explicit "pending-ratification" status; argue against.

4. **CMVP lab selection.** Procurement-driven decision, but architect input matters for Rust familiarity. Should we require the lab to have delivered a prior Rust CMVP validation, or is "has delivered validations against C/C++ modules consumed from Rust" sufficient? The narrower requirement may eliminate strong labs; the looser requirement trades off Security Policy quality.

5. **KMS vendor coverage.** I argue for AWS + GCP + Azure + PKCS#11 from day one. Codex's plan leans "cloud KMS first plus a generic enterprise-compatible path" — which might read as two, not four. Where does the council fall on the trade between Phase-1 engineering cost (writing four providers) vs. Phase-3 customer-procurement cost (only supporting one)?

6. **Scorecard target mutability.** I argue for architect-only target changes with a Design-Lock entry. Is there a legitimate case for workstream owners to propose target adjustments mid-gate? I can see the argument for "we learned the baseline was wrong" but I worry about goalpost-moving. Propose the formal escalation path.

7. **Phase 0 slip policy.** If one ADR review falls off the one-per-week cadence, does Phase 0 extend by a week or does the slipped ADR defer to Phase 1 acceptance? I lean "extend Phase 0" — downstream work cannot safely begin without acceptance. Alternative opinions welcome.

8. **What governance does this workstream *not* cover?** I have scoped to ADRs, calendar dependencies, and the scorecard. Operational governance (change management, on-call rotation, incident response) lives in WS7 (Gemini-owned). Security governance (red-team, InfoSec review) is scheduled at Gate 3 (cross-workstream). Is there a governance category I have missed?

The answers to these questions should land in the Design-Lock log by end of Week 2 so Phase 0 execution is not interrupted by meta-decisions.
