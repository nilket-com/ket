# Plans Directory

This directory contains planning documents for the repository, organized into topical subdirectories. Each plan file should describe a specific feature, refactor, or project milestone. Use the provided TEMPLATE.md for new plans.

## Top-level layout
- `README.md`: This file — index and conventions for the `plans/` directory.
- `TEMPLATE.md`: Template for creating new plan documents.
- `implementation_roadmap.md`: 10-workstream bridge from accepted ADRs to execution.
- `adr/`: Architecture Decision Records (numbered 001..N).
- `impl/`: Workstream docs (`W1_*.md` .. `W10_*.md`) per the implementation roadmap.
- `consensus/`: Raft, WAL, snapshot, epoch transition docs (maps to src/consensus/).
- `network/`: Transport, RPC traits, wire protocol, and write flow (maps to src/network/).
- `schema/`: Bytemuck POD layouts and Arrow IPC schemas (maps to src/schema/).
- `ops/`: Operational playbooks (observability, testing, storage tiering, durability, etc.).

## Naming Conventions
- Use lowercase, hyphen-separated names for plan files (e.g., `user-authentication.md`).
- Keep files focused: prefer many small, well-scoped documents over a single monolith.

## Adding and moving plans
1. Create or update files within the topical subdirectory that best matches the content.
2. When refactoring, create a short migration README in the destination directory (e.g., `consensus/README.md`) and leave the original file in place until reviewed.
3. Update cross-references to point to the new path (e.g., `plans/consensus/raft-core.md`).

## Guidance
- `schema/` is the single source-of-truth for on-wire layouts.
- `consensus/` contains durable formats and recovery invariants.
- `network/` contains framing, transport, and RPC interface guidance.
- `ops/` contains reproducible benchmark and operational playbooks.

