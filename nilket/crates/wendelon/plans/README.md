# Plans Directory

This directory contains durable architecture, operations, and evidence documents for the repository. The Rust source and tests are the definitive software specification; markdown here should record durable decisions, operator runbooks, compliance evidence, and external-process state that cannot sensibly live in code.

## Top-level layout
- `README.md`: This file — index and conventions for the `plans/` directory.
- `TEMPLATE.md`: Template for creating new plan documents.
- `adr/`: Architecture Decision Records (numbered 001..N).
- `conventions/`: Recurring style, naming, and design rules; applies to every collaborator (human or AI) editing the repo.
- `governance/`: Claim discipline and external dependency tracking.
- `compliance/`: FIPS/CMVP, crypto-boundary, release evidence, and compliance-pack artifacts.
- `consensus/`: Raft, WAL, snapshot, epoch transition docs (maps to src/consensus/).
- `network/`: Transport, RPC traits, wire protocol, and write flow (maps to src/network/).
- `schema/`: Bytemuck POD layouts and Arrow IPC schemas (maps to src/schema/).
- `storage/`: Storage-engine notes that are not implementation workstream mirrors.
- `ops/`: Operational playbooks, runbooks, observability, testing, storage tiering, durability, and backup/DR.

## Naming Conventions
- Use lowercase, hyphen-separated names for plan files (e.g., `user-authentication.md`).
- Keep files focused: prefer many small, well-scoped documents over a single monolith.

## Adding and moving plans
1. Create or update files within the topical subdirectory that best matches the content.
2. When refactoring, update cross-references to point to the durable destination and avoid keeping duplicate migration copies.
3. Treat temporary gap-analysis and implementation-workstream notes as commit-history material once their durable output has landed.

## Guidance
- `schema/` is the single source-of-truth for on-wire layouts.
- `consensus/` contains durable formats and recovery invariants.
- `network/` contains framing, transport, and RPC interface guidance.
- `ops/` contains reproducible benchmark and operational playbooks.
- `governance/` and `compliance/` contain durable non-code evidence that should survive workstream cleanup.
- `conventions/` contains the style and naming rules every collaborator (human or AI) is expected to follow when editing the repo. Read it before introducing new types, naming new fields, or amending an ADR.
