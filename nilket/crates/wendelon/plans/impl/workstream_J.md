# Workstream J: FIPS, Supply Chain, And Release Compliance

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS9`

## Objective

Create a defensible regulated-release path that starts early, tracks the real crypto boundary, and does not confuse engineering intent with certification fact.

## Why Now

This workstream closes late, but it must start early because:

- CMVP and lab timelines are long
- dependency and module-boundary drift compound quickly
- supply-chain artifacts are easiest to add before release habits harden

The key discipline is to keep "FIPS-ready," "in lab," and "certified" distinct at all times.

## ADR Anchors

- ADR `019`
- ADR `020`
- ADR `026`
- ADR `031`

## Scope

- crypto-boundary inventory
- FIPS build-profile discipline
- transport-path truthfulness
- self-tests and symbol-sweep coverage
- SBOM generation
- artifact signing
- supply-chain attestation posture
- compliance-pack skeleton for regulated releases

## Out Of Scope

- custody-provider implementation itself
- broad runtime refactors
- Toron integration

## Dependencies

- `A` for external dependency tracking and claim discipline
- `C` for transport-path truthfulness
- `E` for provider-stack and key-path stability
- `F` and `G` for identity and audit evidence that regulated buyers will ask for

## Entry Criteria

- ADR `026` remains the boundary authority
- dependency inventory is captured
- the program is explicit about which milestones can carry only "in progress" compliance language

## Implementation Slices

### J1. Crypto-Boundary Inventory

Inventory:

- crypto libraries
- FIPS-bound versus non-bound functions
- transport stack posture
- build flags and target platforms

If the inventory is wrong, the evidence package will be wrong later.

### J2. FIPS Build Profile

Implement and stabilize the build profile that can support the regulated path:

- dependency pins
- boundary documentation
- transport-path truthfulness
- configuration restrictions where required

### J3. Self-Tests And Regression Guards

Expand from a symbol sweep only to:

- startup self-test behavior
- KAT or equivalent required checks
- explicit failure mode on self-test failure

### J4. Supply-Chain Artifacts

Add:

- SPDX SBOM generation
- signed artifacts
- attestation outputs
- documented release provenance steps

### J5. Compliance-Pack Skeleton

Build the minimum structured package for regulated conversations:

- architecture and boundary summary
- build-profile description
- evidence index
- artifact inventory
- current certification state language

This is a skeleton first, not the final polished binder.

## Tests And Benchmarks

- symbol-sweep CI job
- self-test success and failure-path tests
- build-profile reproducibility checks where practical
- SBOM generation tests
- artifact-signing verification tests
- release-pipeline dry runs for regulated-profile outputs

## Exit Criteria

- the program can state the regulated crypto boundary honestly
- the FIPS build profile is a real tested path, not a slide-deck idea
- self-tests and supply-chain artifacts are produced automatically enough to be repeatable
- a minimum compliance-pack skeleton exists for regulated pilot conversations

## Artifacts

- crypto-boundary inventory
- FIPS build-profile note
- self-test evidence note
- SBOM and signing outputs
- compliance-pack index

## Deferred Follow-Ons

- final CMVP certificate closure
- fuller regulated GA evidence set
- stronger supply-chain maturity targets once the baseline release pipeline is stable
