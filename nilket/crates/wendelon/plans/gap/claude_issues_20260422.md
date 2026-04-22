# Wendelon Unresolved Issues — Claude's List

**Author / Role:** Claude, as Lead Architect
**Date:** 2026-04-22
**Source:** Re-read of `claude_answers_20260422.md`, `codex_answers_20260422.md`, `gemini_answers_20260422.md`.
**Purpose:** Enumerate what remains genuinely unresolved after the council answered the aggregated questions. This is the Phase-0 Design-Lock meeting agenda.

## Definition of "unresolved"

An issue is listed here if at least one of the following is true:

- **Architect-level disagreement.** Two or three of us landed on different answers. Voting will not resolve this; a named decider must pick.
- **External-dependency contingency.** All architects converged on a direction but the direction depends on a lab finding, upstream maturity, vendor decision, or customer input we do not yet have.
- **Customer-dependent interpretation.** The right answer varies by the first pilot customer's regulatory framework, cloud, or deployment posture.
- **Owner-assignment gap.** The shape of the answer is agreed but the org-chart question (who owns this operationally) is not.
- **Technical specification still missing.** Numbers, thresholds, or exact mechanisms not pinned down.
- **"Out of scope" requiring documentation.** The council said "no" to a customer-visible concern; the "no" needs to be translated into customer-facing language.
- **Cross-workstream dependency.** Another team must make a joint decision with us.

Items that converged to a clean shared position are **not** listed here; see §Council Convergence in `claude_answers_20260422.md` v2 for those.

---

## 1. Architect-level disagreements requiring Design-Lock escalation

Seven items where the council has substantive disagreement. A named decider (architect lead, or whomever the program anoints) must pick.

### 1.1 KMS provider shipping cadence [WS-B Q11]
- **Codex:** AWS for Gate 3, GCP+Azure for Gate 4, PKCS#11 for Gate 5.
- **Claude (v2):** AWS required Gate 3, GCP+Azure target Gate 3 with Gate 4 fallback, PKCS#11 Gate 4.
- **Gemini:** All four in Phase 1.
- **Stakes:** Phase 1 engineering scope; customer-deal coverage at first pilot.
- **My recommendation:** Gemini's aggressive position with AWS as the trait-stabilizing first landing. Pick within two weeks.

### 1.2 Signing HSM per-node vs shared [WS-C Q2]
- **Codex:** Shared HSM cluster.
- **Claude, Gemini:** Per-node HSM with vendor-specific cluster-sharing for Node CA private key.
- **Stakes:** HSM procurement cost (4-6x difference for 3-node cert-service); operational ceremony complexity; single-point-of-failure risk.
- **My recommendation:** Per-node for regulated SKU; shared acceptable as a non-regulated compensating control. Escalate to the security architect for final call.

### 1.3 `tenant_admin` scope grammar [WS-C Q8]
- **Codex, Claude (v2):** Define minimal grammar now (`tenant_id` allowlist or `"*"`).
- **Gemini:** Defer — tenant_admin administers all tenants; scoped admins via separate clusters.
- **Stakes:** Managed-service-partner customer patterns at pilot; multi-cluster operational cost.
- **My recommendation:** Ship the minimal grammar; ~50 lines of dispatcher enforcement. Gemini's "separate clusters" is fine for customers who do not need scoped admins, but the grammar is cheap and unblocks real scenarios.

### 1.4 Universal cert-service vs Tenant CA per tenant [WS-C Q10]
- **Codex:** Not a general tenant CA in v1; Phase 2 ADR extension.
- **Claude (v2):** Universal CA in v1 (moved toward Gemini), Tenant CA hierarchy deferred to Phase 3 for customers demanding cryptographic isolation.
- **Gemini:** Universal CA now, query-session certs under `tenant_user` role.
- **Stakes:** Blast radius of tenant compromise; cert-service scope complexity.
- **My recommendation:** Universal CA for v1 (Gemini's position, now Claude's v2). Codex's "Phase 2 ADR extension" framing still fits — the extension ratifies the universal-CA choice.

### 1.5 Cross-region active replication for 4h RTO [Gemini WS-A Q3]
- **Codex:** 4h RTO from cold restore is "too weak for a serious regulated target"; async replication or warm standby required by regulated pilot/GA.
- **Claude (v2):** Cluster-size-tiered — cold restore ≤10 TB, warm standby 10-100 TB, async replication ≥100 TB.
- **Gemini:** Cold restore + backups sufficient for 4h RTO; active-active is Phase 3 post-GA.
- **Stakes:** Gate 4 regulated-pilot deliverable scope; customer SLA language.
- **My recommendation:** Cluster-size tiering as Claude v2; honest RTO SLA per tier. Async replication for large clusters is Gate 5 commitment.

### 1.6 Buffer pool: measure vs ship-sharded-immediately [Gemini WS-C Q2]
- **Codex, Claude (v1):** Measure first, shard if justified.
- **Gemini:** Ship sharded immediately; global mutex will collapse under concurrent io_uring load.
- **Claude (v2):** Prepare sharded implementation in parallel with io_uring split; ship against measurement.
- **Stakes:** Phase 2 engineering-sequence decision; risk of avoidable regression window.
- **My recommendation:** Prepare sharded pool in parallel (Claude v2 position). Lean toward shipping without the measurement gate if the io_uring split lands and concurrent workload is anywhere near nontrivial.

### 1.7 Soak-duration specifics for SLO promotion [Codex WS-D Q2]
- **Codex:** 4 weekly soaks + 24h each + one 72h release-candidate soak.
- **Gemini:** 14 days sustained mixed load with zero breaches.
- **Claude:** 4 consecutive weeks nightly runs hitting target with ≤5% violation per week.
- **Stakes:** When `TARGET` SLOs promote to `COMMITTED`; customer SLA commitment timing.
- **My recommendation:** SRE lead owns this; my numbers were indicative, not defended. Any of the three specifics is reasonable.

---

## 2. External-dependency contingencies

Seven items where the answer is known but hinges on something we do not yet control.

### 2.1 Shamir library (`vsss-rs`) audit status [WS-B Q1]
- Unanimous choice contingent on crypto review. A pre-Phase-1 audit of `vsss-rs` is required. Budget line item; not yet scheduled.
- **Owner:** Security architect + WS-E compliance lead.

### 2.2 Physical HSM vendor selection [WS-B Q4]
- Thales Luna is all-three preference, conditional on CMVP lab familiarity. Final decision depends on which lab we contract with.
- **Owner:** Procurement + CMVP lab engagement.

### 2.3 CMVP lab selection [cross-cutting]
- Procurement-driven. Architect input: Rust-language familiarity matters. Final decision is business-level.
- **Owner:** Compliance lead + procurement.

### 2.4 `rustls-aws-lc-rs` upstream maturity [WS-E / Codex WS-J Q2]
- Target primary path for regulated SKU transport. Fallback posture defined. The Phase 1 Month 6 decision gate is where we commit.
- **Owner:** Architect lead + compliance lead at Phase 1 Month 6 review.

### 2.5 aws-lc-rs CMVP pinned-version status [ADR 026 Open Question]
- ADR 026 acceptance pending the verification of upstream CMVP status at our pinned aws-lc-rs version. If the upstream cert lapses mid-validation, our submission is blocked.
- **Owner:** Compliance lead; monitored throughout Phase 1-2.

### 2.6 IANA PEN registration timing [WS-A, WS-C]
- ADR 018 freezes OID *pending* PEN assignment. Assignment typically lands Phase 0 Week 4-Phase 1 Month 2. Until frozen, cert-service refuses to sign.
- **Owner:** Compliance lead; filing happens Phase 0 Week 1.

### 2.7 Toron team engagement calendar [WS-C, Gemini WS-B]
- Toron integration requires cross-team engineering work (identity mapping, trace context propagation, QUIC trailer consumption, schema evolution). Calendar alignment not yet confirmed.
- **Owner:** Architect lead + Toron TL + PM.

---

## 3. Customer-dependent interpretation

Four items where the right answer depends on the first pilot customer's regulatory framework, cloud, or posture.

### 3.1 WORM governance vs compliance mode [WS-D Q3]
- Council converged on governance as default, compliance per tier. But SEC 17a-4 strict interpretation, certain defense-industry readings of NIST 800-171, and some healthcare interpretations require compliance mode. Customer attorney input required.
- **Decision trigger:** First regulated customer's compliance team.

### 3.2 Compliance-pack framework scope [Codex WS-J Q4]
- SOC 2 Type 2 + PCI-DSS applicability are universally mandatory for regulated pilot. HIPAA, NIST 800-53, FedRAMP Moderate, ISO 27001 are customer-driven. We do not know the first customer yet.
- **Decision trigger:** First regulated customer identification.

### 3.3 Cloud-KMS priority order [WS-B Q11 cont.]
- Beyond the cadence question (§1.1), the specific *order* of AWS vs GCP vs Azure production-readiness depends on the first customer's cloud. If the first design partner is on GCP, GCP KMS ships before Azure Key Vault.
- **Decision trigger:** First customer cloud.

### 3.4 BYOK timing for first regulated pilot [WS-B Q3]
- Council converged on Phase 2 / Gate 4. But a financial-services customer with mandatory customer-controlled Root KEK may force earlier. Bespoke BYOK engagement is the compensating control; productized BYOK stays Phase 2.
- **Decision trigger:** First regulated customer KMS posture.

---

## 4. Owner-assignment gaps

Five items where the technical shape is agreed but the org-chart question is not.

### 4.1 Identity-mapping owner [Codex WS-I Q1]
- "Toron owns mapping, Wendelon owns enforcement" is the agreed architecture. Which specific TL (Toron team + Wendelon cert-service) signs the jointly-versioned contract? Not named.

### 4.2 Runbook quality ownership post-handoff [Codex WS-H Q4]
- Council agrees: operations owns currency, feature-team owns technical correctness, both signatures required on critical updates. Specific operations-team owners for each runbook not named. Quarterly runbook review meeting not scheduled.

### 4.3 Design-Lock log maintenance [WS-A]
- File location is `plans/gap/design_lock_20260422.md`. Architect lead is the signer. Who maintains currency, handles superseding entries, writes the DL-index, ensures DL-vs-ADR consistency? Operational ownership not named.

### 4.4 Ceremony-host operational role [WS-B, `ops/key_ceremony.md`]
- Escrow ceremony specified; who operates the ceremony room, maintains the HSM fleet, records attestation, schedules quarterly recombine-verify runs? Assumed to be the security-team + compliance-lead but not explicitly assigned.

### 4.5 Jepsen contract management [Codex WS-D Q1]
- Council converged on "in-house checker Phase 1, Jepsen before regulated GA." Who sources the Jepsen engagement (vendor or in-house), manages the contract, reviews the deliverables? Not assigned.

---

## 5. Technical specifications still unspecified

Five items where numbers, thresholds, or exact mechanisms need to be pinned down before implementation.

### 5.1 Orphan-reclaim TTL for failed remote writes [Gemini WS-D Q3]
- "Background sweeper reclaims" is the shape. What TTL? I proposed "bounded by replication factor × sweeper interval"; not a specific number. Default-TTL-seconds needs pinning.

### 5.2 Pending-intent-store fencing threshold [Codex WS-B Q2]
- I proposed 30-second fence window. Codex: "short repeated-error threshold." No specific number agreed. What constitutes "repeated"?

### 5.3 Cert-service Raft quorum size decision per deployment [WS-C Q1]
- 3-node default, 5 as regulated-SKU option. Who decides per-customer deployment? Sales-engineering? Default config? Architect-approved per-deal?

### 5.4 Dedicated background finalization worker scope [Codex WS-B Q4]
- Codex: yes, dedicated subsystem. Claude (me): deferred to runtime lead. Specification of the worker (thread-per-node? pool? queue depths?) not agreed. Runtime lead needs to sign off.

### 5.5 Trace-sampling baseline rate [Codex WS-D Q4]
- Codex: "1-5%" baseline. Claude: "1% default." Gemini: "1% normal." Range is agreed; single default value is not. Does the default differ by compliance tier (regulated-tenant 10% per my WS-D answer)? Needs ratification.

---

## 6. "Out of scope" answers requiring customer-facing documentation

Three items where the council answered "no" but the "no" needs a customer-facing explanation.

### 6.1 mlock memory-dump attack surface [WS-B Q9]
- Council: out of scope for Level-1 CMVP. A regulated customer whose threat model includes process-level-root attackers will ask. Needs a Security Policy statement explaining why Level-1 is sufficient and what Level-2/3 would cost. Not authored.

### 6.2 Per-tenant Root KEK isolation [WS-B Q7]
- Council: post-GA. Customer asking today gets "dedicated single-tenant cluster" as the compensating control. Customer-facing docs need to state this clearly in the pricing / procurement conversation. Not authored.

### 6.3 Backup cross-region replication customer responsibility [WS-B Q10]
- Council: customer owns BYOK cross-region. Customer runbook for setting up multi-region customer-KMS is not written; without it, the "customer owns it" becomes "customer is surprised and unhappy."

---

## 7. Cross-workstream joint decisions

Six items where a decision requires two or more workstream TLs in the same room.

### 7.1 Per-user cert issuance scale [WS-C + Toron]
- Cert-service throughput requirement jumps ~100x to handle Toron per-session certs. Cert-service TL + Toron TL need joint sizing decision. Benchmark target (1000/sec sustained, 5000/sec burst per Claude WS-C §3.6) needs validation against real Toron query rates.

### 7.2 Audit trace correlation across Toron-Wendelon [WS-D + Toron]
- Request-trace correlation agreed. Joint specification of which W3C Trace Context fields propagate, which custom baggage items (`wendelon-query-session-id`), and how Toron's own audit chain references Wendelon audit records. Not authored.

### 7.3 SchemaUpdate wire format [WS-codex WS1 + Toron + WS-D]
- Placeholder `SchemaUpdate` Raft request needs wire-format freezing. Toron owns schema intent, Wendelon persists opaque bytes, but the bytes format is joint. Codex WS-B owns the state-machine side; Toron owns the content side.

### 7.4 Compute-AST operator set [Gemini WS-B + WS-C + Codex WS-C]
- Constrained compute-AST for predicate pushdown (pilot) needs the vetted operator set enumerated. Who enumerates? Toron owns query planning; Wendelon owns execution sandboxing; WPBC (ADR 005) owns the operator vocabulary. Three-party meeting required.

### 7.5 Quota visibility via QUIC trailers [WS-D + WS-codex WS2 + Toron]
- Gemini's QUIC-trailer approach adopted. Joint specification of trailer fields, when they are emitted (success only? on error too?), how Toron consumes them, how they map to SQL result metadata. Not authored.

### 7.6 DR audit-chain continuity across cluster rebuild [WS-D + WS-G]
- `cluster.rebuild` event marks fork point; audit-verify CLI must follow forks. Rebuild choreography (Gemini's WS7 / Codex's WS-H) must emit the event. Joint specification not written.

---

## 8. Silently unaddressed items

Three items that nobody answered but probably should have.

### 8.1 Tenant-cert architecture as ADR 018 extension [WS-C Q10 cont.]
- Universal CA vs per-tenant CA decision is framed but the actual ADR amendment scoping is not drafted. Phase 2 ADR 018 extension work is on the critical path but has no owner yet.

### 8.2 Post-CMVP-certificate boundary-changing PR policy [WS-E]
- I proposed a formal "boundary-changing PR" label with architect sign-off and lab re-engagement. Codex and Gemini didn't address this. It matters at the moment the CMVP certificate issues; we need the policy in place *before* it issues, not after.

### 8.3 Jepsen test catalog [Codex WS-D Q1 cont.]
- "Run Jepsen before regulated pilot / GA" is agreed. Which Jepsen properties? Linearizability? Snapshot isolation? Per-key linearizability? Custom Wendelon-specific properties? Catalog not enumerated.

---

## 9. Items I am intentionally not listing here

To be clear about what I *don't* consider unresolved:

- **The ~40 items in the Council Convergence list** (see `claude_answers_20260422.md` v2). Those are effectively locked.
- **Items where one peer answered and others didn't contest.** Silence is a form of agreement at this council; if nobody pushed back, I treat it as accepted.
- **Items where my own answer was "defer to a named workstream owner."** Those are assigned, not unresolved; the owner will work them in their own time.
- **Items where "low confidence" was flagged but the direction was clear.** A weakly-defended answer is still an answer; it becomes unresolved only if implementation reveals it was wrong.

---

## 10. Recommendation for the program

The 32 unresolved items above fall into a natural prioritization:

- **Phase-0-Week-2 Design-Lock meeting agenda:** §1 items (architect disagreements). Seven decisions; each needs a named decider and one meeting. Target two decisions per meeting; all seven locked by end of Week 6.
- **Phase-0-to-Phase-1 calendar work:** §2 items (external dependencies). Kick off Phase 0; decision gates in Phase 1.
- **Pre-pilot customer-engagement:** §3 items (customer-dependent). Trigger at first-customer-identification, not earlier.
- **Operations-hiring window:** §4 items (owner assignment). As operations team staffs up, fill the named-owner slots.
- **Pre-implementation spec work:** §5 items (technical specs). Pin down before the PR lands.
- **Customer-facing docs track:** §6 items. Author as part of the compliance-pack workstream (WS-E / WS-J).
- **Joint-session scheduling:** §7 items. Two-to-three-hour focused meetings with named attendees.
- **Filled as we go:** §8 items. Surface whenever adjacent work makes them relevant.

This is the Phase-0 meeting agenda. No items here are program-stoppers; all are solvable within the calendar if we treat them as scheduled work rather than "we'll get to it."
