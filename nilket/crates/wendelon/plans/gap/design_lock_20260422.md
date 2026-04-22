# Wendelon Design-Lock And ADR Program

Date: 2026-04-22
Author Role: Lead Software Architect
Sources:
- `plans/gap/codex_execution_20260422.md`
- `plans/gap/codex_answers_20260422.md`
- `plans/gap/codex_issues_20260422.md`
- `plans/gap/execution_20260422.md`

## Position

The gap-planning cycle produced enough converged architecture to justify ADR work, but it did **not** produce seventeen deserving new ADRs.

That would be ADR inflation.

The right outcome from this planning round is:

- a small number of **new ADRs** for genuinely missing architectural surfaces
- a short list of **amendments to existing ADRs**
- a separate **design-lock log** for narrow but important implementation decisions

Rule: if a decision is cross-cutting, durable, and likely to survive multiple implementation waves, it deserves ADR weight. If it is narrow, parameter-shaped, or likely to be revised during implementation, it belongs in design-lock instead.

## Recommended Output

### 1. New ADRs To Author

These are the only new ADRs I would add from this cycle.

#### `ADR 030: Deployment Profiles And Support Envelope`

Purpose:

- define the product's support model without creating fake SKU forks
- lock the relationship between:
  - commodity-default deployment
  - premium-hardware day-1 support
  - later regulated hardening

What it closes:

- commodity hardware is the default operating profile
- better hardware is supported on day 1
- day-2 work is tuning and performance claims, not basic compatibility
- support means correct behavior plus documented deployment path plus explicit config

Why this needs a new ADR:

- this is the frame that other storage, transport, and custody decisions now depend on

Suggested file:

- `plans/adr/030_deployment_profiles_and_support_envelope.md`

#### `ADR 031: Production Key Custody Provider Stack`

Purpose:

- define how Wendelon handles root-key custody across deployment profiles

What it closes:

- `local-sealed` as the default serious deployment path
- optional `cloud-kms` hardening tier
- optional `pkcs11/hsm` enterprise tier
- `dev-insecure` explicitly dev/test only
- bootstrap and unlock contract
- provider abstraction boundary

Why this needs a new ADR:

- ADR `020` covers L2 envelope encryption and DEK lifecycle, not the production Root KEK provider stack

Suggested file:

- `plans/adr/031_production_key_custody_provider_stack.md`

#### `ADR 032: Schema Evolution And Control-Ring Contract`

Purpose:

- freeze the durable schema-change contract and remove placeholder ambiguity from customer-facing control paths

What it closes:

- `SchemaUpdate` durable sequencing
- placeholder control-ring structs and wire contracts
- Wendelon ownership of durable schema state
- dependency boundary with Toron for schema intent vs durable sequencing

Why this needs a new ADR:

- there is no current ADR that cleanly owns this surface
- this is too cross-cutting to leave as an implementation note

Suggested file:

- `plans/adr/032_schema_evolution_and_control_ring_contract.md`

#### `ADR 033: Backup, Restore, And Node-Replacement Architecture`

Purpose:

- define the day-1 operational recovery architecture

What it closes:

- authoritative backup and restore model
- cluster-scoped recovery vs node-image folklore
- node-replacement contract
- separation between ADR `001` autonomous eviction and the actual replacement workflow
- commodity default vs richer operator automation later

Why this needs a new ADR:

- the repo has ops docs here, but no architecture-level decision for DR and replacement

Suggested file:

- `plans/adr/033_backup_restore_and_node_replacement_architecture.md`

#### `ADR 034: Toron-Wendelon Integration Contract`

Purpose:

- define the first real cross-system contract between the storage ring and compute ring

What it closes:

- query-session identity handoff
- trace context propagation
- quota visibility contract
- schema intent vs durable schema ownership
- constrained compute/predicate pushdown contract

Why this needs a new ADR:

- this is a real architectural boundary, not just an API detail
- it will otherwise drift independently in both repos

Suggested file:

- `plans/adr/034_toron_wendelon_integration_contract.md`

### 2. Existing ADRs To Amend

These should be revised, not replaced.

#### Amend `ADR 003: Stellarator Model`

Add:

- mandatory pending-intent persistence for the recoverable branch
- concurrent local durable append and remote fan-out
- explicit write-outcome state machine
- leader fencing/degraded behavior on integrity-threatening persistence failures

Reason:

- the current ADR states the broad bifurcated-write idea, but not the now-required recovery semantics

#### Amend `ADR 009: io_uring Threading Model`

Change:

- current "one ring per physical NVMe device" language is stronger than the implemented and now-planned support contract

Replace with:

- commodity default
- premium hardware supported on day 1
- explicit config first
- topology-aware per-device automation later
- WAL/slab durability class split as the real immediate invariant

#### Amend `ADR 011: Auto-Adaptive Multi-Transport`

Clarify:

- what is actually day-1 transport reality
- what remains future transport ambition

Add:

- transport truth-up rule: no claim survives without implementation
- QUIC admission control and overload behavior as part of the supported transport contract

Reason:

- the current language invites over-claiming

#### Amend `ADR 018: Dual-CA mTLS Identity And Authorization`

Add or tighten:

- cert-service scope boundaries
- query-session cert scope
- revocation distribution story
- break-glass scope boundaries
- role/policy layering that can support both simple operation now and stricter enterprise policy later

Reason:

- the planning cycle closed a number of identity and cert-scope questions that should not stay only in markdown side discussions

#### Amend `ADR 025: Audit Event Model And Tamper-Evident Hash Chain`

Add or tighten:

- baseline authoritative audit story
- local NDJSON plus archival shipping as the baseline
- Kafka optional, not required
- data access event types
- authority vs materialization language

Reason:

- audit is now one of the clearer converged surfaces from the planning cycle

### 3. Proposed ADRs To Accept With Targeted Edits

These do not need to be recreated. They need a pass and then acceptance.

#### Accept `ADR 021: Tenant-Aware Architecture From Day One`

Targeted edits:

- ensure it still reads cleanly against the current identity/custody/audit posture

#### Accept `ADR 027: Observability Posture`

Targeted edits:

- align the operator/metrics/traces story with the actual pilot and overload-validation plan

#### Accept `ADR 029: Identity vs Locality`

Targeted edits:

- ensure the naming and durable lookup contracts still align with the current protocol and storage path

## Design-Lock Only

These are real decisions, but they do **not** deserve their own ADRs.

- exact Stellarator fencing thresholds
- exact `FinalizationRecoverablePending` wire-shape and retry metadata
- exact `SmState` replacement mechanism (`watch` vs atomics + immutable snapshot)
- exact `BufPool` sharding trigger
- exact QUIC limits and queue sizes
- exact premium-hardware config knobs
- exact bootstrap order for `local-sealed` unlock paths
- exact break-glass TTL, signature-aging window, and dispatcher restrictions
- exact retention counts and compliance-tier day values
- exact Kafka topic partitioning shape
- exact HSM vendor choice
- exact Shamir library choice
- exact CMVP lab-selection criteria

These belong in this file or in implementation design notes, not in ADRs.

## Deferred ADR Work

These matter, but I would not put them in the first ADR wave from this planning round.

- deeper FIPS and compliance posture refinements in ADRs `019`, `026`
- deeper envelope-encryption/provider refinements in ADR `020`
- later operator/SRE packaging details
- later hardware-acceleration and premium-fabric tuning work

## Authoring Order

This is the order I would actually execute.

1. Amend `ADR 003`
2. Amend `ADR 009`
3. Amend `ADR 011`
4. Accept `ADR 021`
5. Author `ADR 030`
6. Author `ADR 031`
7. Author `ADR 032`
8. Amend `ADR 018`
9. Amend `ADR 025`
10. Author `ADR 033`
11. Accept `ADR 027`
12. Accept `ADR 029`
13. Author `ADR 034`

## Summary Judgment

The planning cycle did produce a good ADR backlog.

It just did **not** produce seventeen good new ADRs.

The correct cut is:

- `5` new ADRs
- `5` ADR amendments
- `3` proposed ADRs ready to accept with edits
- everything else stays in design-lock
