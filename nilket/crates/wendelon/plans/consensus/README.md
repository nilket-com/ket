# Consensus (plans/consensus)

Contains docs for Raft-based control ring, WAL/snapshot formats, epoch transitions, and leader-side services. Maps to src/consensus/ in the codebase.

Files:
- raft-core.md — WAL, snapshots, recovery invariants
- epoch_transitions.md — epoch advance and lazy migration
- idempotency_cache.md — leader idempotency cache design
- implementation_checklist.md — prioritized tasks
- hashraft.md — architectural overview of the HashRaft fusion
