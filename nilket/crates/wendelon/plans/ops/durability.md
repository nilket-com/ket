# Durability Budget

Wendelon's durability contract is "replay forward from the last durable prefix,
never trust local acceleration state ahead of the verified log." This document
turns that into an operator budget for W1.

## 1. Authority Order

1. The Control Ring WAL and snapshot artifacts are authoritative for replicated
   state.
2. redb is a local restart accelerator. It is never trusted ahead of the
   verified Control Ring recovery point.
3. Slab bytes are authoritative for Data Ring payloads. redb and in-memory
   indexes may be rebuilt from slab bytes plus the recovered Control Ring state.

If redb is missing, corrupt, or behind the recovered Control Ring watermark, the
node must rebuild forward before it serves traffic.

## 2. Recovery Budget Envelope

These are pilot budgets for the baseline node profile used by W1 acceptance:

- 8 vCPU Linux node
- 32 GiB RAM
- local PCIe 4 NVMe
- `io_uring` enabled

The supported recovery envelope is:

- Control Ring snapshot artifact up to 10 GiB
- retained Control Ring WAL tail up to 1 GiB after compaction
- local slab set up to 64 GiB
- redb index file up to 8 GiB

Within that envelope, the recovery budget is:

- soft target: node becomes ready within 30 seconds after an unclean restart
- hard operator threshold: 60 seconds

Crossing the hard threshold is not silent technical debt. It is an operator
signal to compact the Control Ring, reduce retained WAL tail, or investigate the
local NVMe path before expanding production load.

## 3. What Counts Against The Budget

The readiness timer includes:

- WAL discovery and replay to the last valid durable boundary
- streaming snapshot validation and install
- vote / checkpoint artifact recovery
- redb open, validation, and replay-forward catch-up
- slab-manager reopen and tier-pointer reconciliation

The readiness timer does not include:

- background L2 uploads
- tenant sweeper work
- post-ready compaction or benchmarking

## 4. Recovery Rules

- A torn WAL tail is truncated to the last checksum-valid record and recovery
  continues.
- Interior corruption in a sealed WAL segment fails startup loudly.
- A missing directory fsync may legitimately lose a newly rotated segment across
  crash recovery; recovery must still converge from the prior durable prefix.
- Snapshot artifacts are validated before use. A bad snapshot does not authorize
  redb or slab state.
- redb rows are accepted only up to the recovered Control Ring watermark. Any
  gap is replayed forward; any open failure falls back to rebuild.

## 5. Operator Procedure

When a node misses the 30-second soft target:

1. Inspect retained Control Ring WAL bytes and snapshot bytes on disk.
2. Confirm the node is replaying from the expected durable prefix rather than
   carrying an unexpectedly large tail.
3. If redb was rebuilt, treat that as expected degraded recovery, not as a
   correctness failure.
4. If the node exceeds 60 seconds, keep it out of readiness rotation until the
   durable artifact sizes or local storage health are corrected.

## 6. W1 Acceptance Mapping

This document is the budget reference for:

- `plans/impl/W1_foundation_and_storage_seam.md` §3 ADR 017 cold-start budget
- redb replay-forward authority rules from ADR 017
- the W1 CI checks that exercise WAL replay, snapshot recovery, and metadata
  sync behavior
