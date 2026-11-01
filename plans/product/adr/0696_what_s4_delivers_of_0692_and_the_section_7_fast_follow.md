# ADR 0696: what the S4 stack delivers of ADR 0692, and the §7 fast follow

Status: proposed r6, 2026-11-01 (Claude), for Codex review. It does two things:
- It records the as-built behaviour of the S4 delivery of ADR 0692.
- It plans the fast follow that implements 0692's undelivered requirements.

Per the operator's disposition (below), S4 ships first, as this ADR's first `impl: adr 0696`. The §7 fast follow is a subsequent `impl: adr 0696`. r5 applies Codex 01a194d6, a packaging correction: 0692 does not land. r6 records the known SQL timeout limitation (Codex 01a195a1). Review history: Codex HOLD 01a1948f; acceptance as a documentation draft, 01a19493; probe evidence source-cleared, 01a194cd.

Relation to ADR 0692: 0692 r7 (accepted 2026-10-26) is the approved design. It is not on main. Its exact text is blob `62c33b41c55621a0389ae7aff160fedc7ca60322` at `plans/product/adr/0692_a_row_is_found_exactly_or_refused_while_storage_moves_it.md` in commit `e507349d535318e49977e7039801394e666aab97`, which is preserved in the S4 history (for example `claude/backup/s4-row03-ready-control-20261031`). This ADR records:
- where the delivered code departs from 0692;
- where the delivery covers cases 0692 left unspecified;
- which approved requirements are not yet delivered, and how they will be.

Old ADRs are never edited: where 0692's text and the delivery differ, this ADR states the delivered behaviour. ADRs 0686–0691 are cited by 0692 only as background or future work. They are unimplemented, not on main, and not part of this ADR.

## Delivered

The delivery implements:
- 0692 §1–§3 and §5.
- Most of §6.
- The parts of §4 and §8 described below.

The delivered mechanisms:
- **Version identity and recovery.**
  - A version is one payload per identity, and persisted locations resolve by identity.
  - Recovery classifies versions by record header.
  - Same-HLC copies are compared by payload digest: equal digests make a copy, unequal digests give a typed `ConflictingVersion` refusal.
  - An unreadable duplicate gives `AmbiguousVersion`.
- **Tier generations.**
  - Each generation keeps its own object and data key.
  - A publication installs pointer and layout history together under the manager `state` lock.
  - A read resolves location and backing in one snapshot, and verifies the decoded frame's `(key, hlc)`.
- **Index continuity.**
  - Index publications are tracked in flight and as orphans.
  - Rebuild is refused on any unresolved mark.
  - Retirement and tier pointer changes are fenced through committed authority.
  - LSM installs carry durable intents (Local/Control, phases Prepared/OutputsPublished, owner tokens).
- **Durability and refusal.**
  - A tier walk refuses any acknowledged offset that is not an accepted record start.
  - A new slab's name is durable before it serves appends.
  - Append reservations quiesce before tier selection.

**Ablation evidence** (packet `ablation-campaign-r1/`, ABLATIONS.md), on the S4 tip:
- Of 33 rows, 31 are positive removals. Each failed its declared control while its named unaffected control passed in the same run.
- Two single-call removals (rows 31 and 32) deliberately stayed green, showing redundancy between two call sites. Removing both (row 30) fails.
- Row 03 was re-run on the successor commit. The other 32 results were carried from the immediate parent under recorded equality evidence (identical kept diffs; the only change was in `tests.rs`).

Each positive removal shows that its mechanism is load-bearing for its paired control. The redundancy is shown only by the combined removal (row 30), not by either single removal. None of this is a correctness proof.

## Changed decisions

The delivered behaviour differs from 0692 r7's text.

| Area | 0692 r7 | Delivered | Ruling |
| --- | --- | --- | --- |
| Old or orphan L2 objects (§8) | delete once definitively rejected, or once applied and unreferenced | no old object is deleted. Each is retained as a durable `(slab, uri)` record, with gauges (count, known bytes, unknown size). Only a candidate that durably never crossed the proposal boundary, and that no current or unapplied pointer references, is deleted. ADR 0693 owns deletion authority. | OPERATOR-DECISION-NO-L2-DELETES (2026-10-27; supersedes Codex seq 16546, 16551); RULING-AMBIGUOUS-CLEANUP (seq 15045) |
| Open-time verification (§8) | lazy, per slab on first access | an eager node-wide readiness gate; lookups and scans are fallible; bootstrap stages committed pointers as unverified | RULING-TIER-AUTHORITY-READINESS (seq 15070); RULING-BOOTSTRAP-PENDING-POINTERS (01a1796b-28a5-7456-8c3c-e6be8e24fd60, seq 15154) |
| Tier publication pairing (§4) | pair by `(tenant, key, hlc)` | pair by class-specific logical owner plus payload digest, all refusals before commit | RULING-INTERIM-S1-S6 S1 (Codex seq 15005); RULING-S1-DECISIONS |
| Maintenance refusal reconciliation (§1) | a follow-up redb delete for refused rows | only authorized outputs publish, in one transaction, with a preflight refusal | review item A9, source approved seq 17371 |

## Additional decisions

These cover cases 0692 r7 left unspecified.

| Area | Delivered | Ruling |
| --- | --- | --- |
| Definitive rejection (§8) | only a refusal before `client_write` counts; followers neither upload nor propose | RULING-B1-OUTCOME-AND-PARTICIPANTS |
| Obsolete stored formats | stores holding class-1 tentatives or class-2 markers refuse to open (Unsupported) | RULING-INTERIM S2/S3 |
| L1 file at a tiered slab id | kept as quarantined evidence and never served; runtime unlinks only owned slab files | RULING-DC87 r2 (Codex seq 14978); RULING-DC87-FIX |
| Snapshot install against tier state | snapshot-authorized retirement, per-slab fences, per-write degrade tokens, a catch-up proof | RULING-SNAPSHOT-RETIREMENT, -METADATA-OBSERVATION, -FENCED-AUTHORITY-APPLY, -OVERNIGHT-20261027, -EIGHT-REDS |
| Missing index and restore | open is split from `initialize_new`; rebuild only under validated restore authority, bound to the restored artifact bytes | RULING-OVERNIGHT-20261027 (DC-146); RULING-RESTORE-IDENTITY |

## Incomplete requirements (delivered by the fast follow)

0692 r7 requires these, and they are not delivered. None is waived: the operator's disposition (below) orders the delivery, it does not drop requirements. The interim rulings are not waivers either: RULING-TIER-AUTHORITY-READINESS requires the complete metrics in commit 5, and RULING-S1-DECISIONS keeps reporting and resume as an open ladder gate.

1. **§7 in full.**
   - The node memory budget with all-or-nothing reservation and `allocation_size` accounting.
   - Immutable budgeted maps, and per-old-slab redirect maps.
   - Proposer tokens through ambiguous commits, and the source test that every `commit_tier_slab` caller holds one.
   - The per-lineage cap.
   - Skipped optional redirects.
   - Typed `Superseded` after old-slab disposal.
   - The §7 metrics: rewrite-refusal and fencing counters, and history and redirect byte gauges.
2. **§4 observer protocol, the remainder.** The delivered observer caches a typed `Fenced` deferral and keeps node readiness degraded (`tier_authority_readiness_reason`). The deferral is retried on:
   - the next apply;
   - snapshot or authority comparison re-staging;
   - each sweeper pass, which re-applies a matching Fenced deferral (`reconcile_tier_slab_at_snapshot_epoch`).

   Not delivered: the specified reporting, and resume on a budget release or L2 recovery as specified. Probe P2 (packet `s7-probes-r1/`) shows one sweeper pass recovering an observer rewrite fence after a real 30 s index-gate timeout. That covers this case only, not first tiering, the other deferral sources, restart, concurrent retry or multinode. The retry runs without a control log index and so skips the control-index bookkeeping (`tier_republication_index`, `cancel_snapshot_retirement_before_control`, `note_tier_republished`). This is registered as an unconfirmed candidate.
3. **Declared controls.**
   - DC-79 and DC-82.
   - Multinode: leader change, a delayed apply after a released token, restart while fenced.
   - Budget, churn and exporter tests.
   - The token source test.
4. **The §8 measurement gate.** It reads a `layout_collision` refusal metric that does not exist.

## Guarantees that cannot be claimed

Without the incomplete requirements, the delivery does not provide:
- **An enforced bound on tier-history memory.** One `TierLayout` per tiered slab keeps every distinct historical location for the process lifetime, with no budget or cap. Probe P1 shows a plateau for one finite fixture: 1000 rows and 9 retention rewrites gave 5400 historical entries plus 100 current locations, estimated at about 0.8 MB installed, not measured as heap. That does not establish a per-node bound, or the absence of growth on every production path. A conditional O(R²)-per-slab bound holds only if every location-changing publication prunes at least one row.
- **Typed refusal under memory pressure.** If memory is exhausted, the process dies rather than refusing.
- **The specified fencing and history reporting.** Readiness reasons, typed refusals and logging do exist.

The delivered refusal mechanisms are evidenced only within their tests and identity assumptions. The delivery does not claim global database safety: ADR 0693's confirmed wrong-data residual (slab ids that are only node-local) remains.

## Disposition

The operator decided on 2026-10-31: the S4 delivery ships without the incomplete requirements, and §7 follows immediately afterwards. The operator's words: "we don't need a month long commit, just fast follow section 7 afterwards".

This is an explicit decision to ship first. It is not a waiver:
- every incomplete requirement above stays an approved obligation of 0692, owned by this ADR's fast follow;
- until the fast follow lands, the guarantees listed above are not claimed.

The decision rests on the assessment and probe evidence in review packet dc54-index-continuity-r1:
- S7-DISPOSITION-ASSESSMENT-r3;
- s7-probes-r1;
- S4's wrong-data fixes (DC-54 and its successors), which would otherwise wait on an availability safeguard.

## Fast follow (a subsequent `impl: adr 0696`)

The fast follow implements 0692 r7 §7 and the §4 observer remainder as approved, with no redesign:
1. **Budget.** The node budget with all-or-nothing reservation and `allocation_size` accounting. Immutable budgeted maps, replacing the per-slab mutable `TierLayout` history where §7 requires it. Per-old-slab redirect maps. The per-lineage cap.
2. **Tokens.** Proposer tokens reserved before proposing and consumed by the observer, through ambiguous commits. A source test that every `commit_tier_slab` caller holds one.
3. **Fencing and resume.** `Fenced { committed: P }` reporting, and single-flight resume on a budget release, sweeper tick or L2 recovery. The existing sweeper retry is kept. The control-index bookkeeping that retry skips (it runs with `last_control_log_index = None`) is reviewed and either fixed or ruled out.
4. **Superseded handling.** Skipped optional redirects, and typed `Superseded` after old-slab disposal.
5. **Metrics.** `wendelon_tier_rewrite_refusals_total{reason}`, `wendelon_relocation_redirects_skipped_total`, `wendelon_tier_publication_fenced_total{reason}`, and gauges for history and redirect bytes. Exporter tests for each.
6. **Controls.**
   - DC-79 and DC-82.
   - Multinode: leader change, a delayed apply after a released token, restart while fenced.
   - Near-budget concurrency, abort and retry accounting, churn and peak live bytes.
7. **The §8 measurement gate**, on the new `layout_collision` metric.

The fast follow carries ablations for each mechanism, under the same rules as the S4 campaign.

## Known limitation: a live SQL materialization timeout under load

The operator explicitly accepted shipping with this unresolved availability and performance risk. That decision is separate from the §7 disposition.

- **Observed failure.** The toron test `adr0094_live_199999_row_chain_materializes_and_reads_back` materializes 199,999 rows through live Wendelon storage. On one three-lane v6 ladder it hit the existing 60 s StatementTimeout; its test binary finished in 62.61 s.
- **Observed success.** On the corrected delivery candidate's ladder it succeeded, and the binary finished in 47.10 s. The two candidates have the same product code.
- **Synthetic load.** A synthetic load-matched comparison did not reproduce the timeout (6 of 6 runs succeeded).
- **Measured cost.** In an isolated debug harness this delivery raises that test's elapsed time from 20.49 s to 25.51 s, about +24.5%.
  - About +2.6 s is a transition at the commit that introduced tier authority and index continuity.
  - About +2.2 s is unattributed.
  - No mechanism is isolated.
- **What is unknown.** The timeout's cause and its failure rate are unknown. It is not established as a flake, as test-only, or as absent in release builds, and no unavoidable durability cost has been shown. No wrong-data defect was demonstrated.

**Follow-up (owned by this ADR, separate from the §7 fast follow).** Profile the materialization and response phases, and both cost increments, keeping the real 60 s bound and the load semantics. Then reduce the cost, or accept it on measured evidence. The test's timeout, concurrency, row count and assertions do not change. No correctness or durability fix is rolled back speculatively.

## Remaining correctness work, independent of the above

- ADR 0693, with B13 as an input.
- B12c.
- R-ACC8-AVAIL.
- DC-84 and the other 0692 residuals.
- The open rows of REGISTER-20261030: R-APPEND, C5, R-WC-AUTHZ, R-WC-DUP, R-DEAD-FEATURE-TESTS, R-DRIFT-IO, R-C6-VERIFIER, R-C8-TOKENLESS.
