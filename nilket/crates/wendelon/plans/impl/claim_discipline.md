# Claim Discipline Register

Date: 2026-04-23
Owner: `WS0 / A`

## Purpose

This file is the standing claim-discipline pass for architecture docs, operator docs, code comments, and release-facing notes.

The rule is simple: a claim must match implemented behavior, gate evidence, and ADR/design-lock status. If those disagree, the claim is retracted or narrowed until the implementation and evidence catch up.

## Review Surface

| Surface | Owner | Current pass status | Required action |
|---|---|---|---|
| ADR text | `A` + ADR owner | `active` | Use `plans/impl/adr_status_matrix.md` before accepting or expanding claims |
| Operator docs and runbooks | `H` + relevant workstream | `active` | Keep runbooks scoped to supported operator paths and recorded drill evidence |
| Roadmap and implementation docs | `A` | `active` | Avoid marking gates complete without scorecard and registry evidence |
| Code comments | local code owner | `active` | Comments must describe current behavior, not target architecture |
| Release-facing notes | `A` + compliance/program owner | `not started` | Deferred until a formal release branch exists |

## Allowed Claim Baseline

| Domain | Allowed claim now | Not allowed without more evidence |
|---|---|---|
| Transport | QUIC-first support through the implemented transport path, with bounded admission controls as the day-1 contract | Auto-adaptive multi-transport support, RDMA support, GSO/`io_uring` QUIC acceleration, or benchmark-backed premium-fabric claims |
| Storage topology | WAL and slab durability classes are the immediate separation boundary; explicit configuration is the premium-hardware day-1 path | Automatic per-device topology discovery or NVMe-saturating performance claims |
| Write correctness | The supported write-outcome vocabulary is ADR `003`; recoverable-pending requires durable pending-finalization intent | Any recoverability claim that does not first persist the pending-finalization intent |
| Custody | `local-sealed` is the serious local baseline; `dev-insecure` is explicit dev/test only; AWS KMS and PKCS#11 seams are tracked by Workstream E artifacts | Generic enterprise HSM support, cloud-provider parity, or custody maturity beyond implemented/provider-documented paths |
| Recovery | Backup/restore and node replacement have documented support paths and an external automation contract | First-party operator automation, five-nines availability, or completed non-author restore drill until evidence is recorded |
| Audit | Local NDJSON hash-chain evidence is the baseline; Kafka is optional distribution unless configured as a canonical projection path | Kafka as a mandatory authoritative audit source or free-form logs as audit evidence |
| Compliance/FIPS | FIPS and CMVP work is planned/tracked; use only controlled pre-validation language | FIPS 140-3 validated, certified, compliant, in validation, or lab-backed claims before the CMVP/lab tracker supports them |
| Toron integration | ADR `034` defines the intended contract boundary | Regulated Toron pilot support until contract tests, identity handoff, tracing, and audit attribution evidence exist |

## Claim Review Checklist

Every claim-bearing change must answer:

1. Which ADR or design-lock entry owns the claim?
2. Is that ADR accepted, proposed with an owner, or deliberately deferred?
3. Which scorecard row or artifact registry entry proves the claim?
4. Does the code path exist now, or is the text describing future work?
5. If the claim is compliance-facing, has the compliance/program owner approved the wording?

## Known Sensitive Phrases

These phrases are not banned, but they require evidence before use:

| Phrase pattern | Required evidence before use |
|---|---|
| `adaptive transport` | More than one implemented transport plus negotiation/fallback tests |
| `RDMA` | Implemented RDMA path, benchmark report, and failure-mode coverage |
| `GSO` or `io_uring QUIC` | Implemented fast path and benchmark report |
| `FIPS compliant`, `FIPS certified`, or `CMVP validated` | CMVP certificate or explicit validation-stage record |
| `five-nines` or `99.999%` | Availability model, recovery automation evidence, drill evidence, and customer-approved support envelope |
| `HSM supported` | Working provider implementation, compatibility matrix, and operator ceremony docs |
| `fully autonomous healing` | First-party operator or equivalent closed-loop automation evidence |

## Current Residual Gaps

- Several ADRs remain `Proposed`; they are allowed only because `plans/impl/adr_status_matrix.md` names owners and blocking milestones.
- External dependency receipts are not repo-visible yet; `plans/impl/external_dependency_tracker.md` tracks the required next actions.
- The H drill report exists as an evidence contract, but the scorecard still marks the real non-author drill as incomplete.
- C benchmark numbers on real hardware and B write-path benchmark delta remain open evidence rows.
