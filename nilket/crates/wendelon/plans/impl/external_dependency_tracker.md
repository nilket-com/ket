# External Dependency Tracker

Date: 2026-04-23
Owner: `WS0 / A`

## Purpose

This file tracks calendar-dominant dependencies that cannot be recovered by coding faster later.

The status below is repo-visible status, not a claim about external paperwork that is not checked in.

## Tracker

| Dependency | Owner | Gate impact | Repo-visible status | Canonical evidence path | Next action | Review cadence |
|---|---|---|---|---|---|---|
| IANA PEN filing and assignment | `A` + `F` + compliance/program lead | `M0` kickoff, `M3` pilot credential freeze | `open` | `plans/impl/external/iana_pen_filing_record.md` | File or record the PEN request, then replace the placeholder actor-claims OID and update ADR `018` plus `plans/impl/workstream_F_claim_oid_note.md` | weekly until assigned |
| CMVP/lab contact state | `A` + `J` + compliance/program lead | `M0` kickoff, `M5` regulated GA | `open` | `plans/impl/external/cmvp_lab_contact_record.md` | Placeholder record exists; replace it with lab shortlist, outreach date, responses, selected lab, and the exact wording allowed for FIPS/CMVP claims | weekly during outreach, monthly after kickoff |
| Custody-provider onboarding | `A` + `E` + legal/procurement owner | `M0` kickoff, `M3` customer pilot custody | `partial` | `plans/impl/external/custody_provider_onboarding_record.md` | Record provider account/legal status for local-sealed, cloud KMS, and PKCS#11/HSM tracks; keep unsupported providers marked unsupported | weekly until pilot provider path is executable |

## Dependency Rules

- A dependency row is not `done` until the evidence path exists and records the current external status.
- A placeholder or internal note is enough for `open` tracking, but not enough for gate closure.
- External receipts should not be embedded in ADRs. ADRs capture decisions; this tracker points to evidence of calendar state.
- If a dependency changes release-facing claims, update `plans/impl/claim_discipline.md` in the same PR series.

## Status Vocabulary

- `open`: work is tracked but no durable external evidence has been recorded.
- `partial`: some implementation or planning artifacts exist, but external onboarding is not fully evidenced.
- `blocked`: an external party is waiting on Wendelon to provide missing material.
- `done`: the evidence path exists and records the completed external milestone.
