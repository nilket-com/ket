# ADR 0698: what the section 7 fast follow delivers, and that section 8 failed

Status: accepted as an integration disposition, 2026-11-06 (Claude, Codex), conditional on the final landing checks.

This records what is being integrated to main and what is not established by doing so. It is integration of reviewed corrections, not a release or readiness statement. ADR 0692's section 8 acceptance is FAILED and stays open. Merging does not satisfy it, and no follow-up campaign is scheduled by this record.

## Delivered
- ADR 0696's section 7: budgeted tier history, the charged cross-slab redirect store, typed fence causes, and the lineage census.
- ADR 0697: slabs named by owner and store incarnation, arbitrated terminal transitions, typed operation outcomes with acknowledged windows, reserved slab ids, and recovery of abandoned operations across a reopen.
- The section 8 measurement harness, `s7-measure`, and its fix-only campaign.
- Corrections found by running it: tier rows decode each frame once; the op driver leaves an operation its workflow owns; a table write waits out a refusal past the door; a fence raised during version checks is waited out; a tier retirement disposes of its live run in the same transaction; a retirement waits for an unresolved install naming its slab; readiness does not allocate while no install is unresolved; a tier read's tenant check walks the slab once.
- Two restart and availability corrections found by the compaction workflow test:
  - A GC removal's manifest epoch reaches the disk in the transaction that drops the run, and an install's input runs have durable rows before its intent, so an accepted install is not fenced by a reopen at the one cut tested, `AfterAcceptance`.
  - One LSM job at a time, store-wide, with maintenance refusing while one stands, so an accepted install's prepared epoch is not overtaken.

## Section 8: failed
Both valid blocks failed the RSS predicate, limit 1.10, with everything else passing, 0 workload errors and about 2500 rows/s:
- Block `20261106-b1-fix` on e8e81a0c9: ratio 1.1356.
- Block `20261106c2-b1-fix` on 164ad9266: ratio 1.2466.

No later commit has run section 8. The corrections after those blocks are not shown to change the result. The campaign is fix-only, so there is **no RSS comparison with main**: whether this code uses more or less memory than main is unmeasured.

One allocator diagnostic, not acceptance evidence, reproduced 1.1391 with the workload intact. Sampled requested bytes stayed near 200 MiB against RSS near 1.9 GiB, allocator capacity grew with RSS, and `malloc_trim` after the workload ended took RSS from 1858 to 527 MiB, then 676 MiB 2 s later. That supports substantial returnable allocator retention at that endpoint. It does not establish bounded RSS under load, the source of the growth, or any fix. No trim, tunable, arena limit or allocator change is part of this integration.

## Compaction workflow
One real node compacts real writes through its own services and the ring, collects the inputs, and reads every row back exactly before and after a restart on the same directory, with the manifest epoch not lower. That test is local inputs on one node. The restart aborts tasks and shuts down: it is not a power loss or a full graceful shutdown. Tiered inputs, transport, three nodes and backup are not in it. Section 8 itself never planned an L0 compaction after DC-184, so compaction under that load is unmeasured.

## Limits that integration does not remove
- **Format.** Per ADR 0697 section 7, a store using the encodings from before ADR 0697 is refused at open with a typed error, and has no upgrade path. Such a store must be re-created. Integration authorizes no reset or migration of any store.
- **Root2** remains dormant. This workflow and this acceptance campaign do not establish **backup and restore** readiness. **Three-node** readiness is not established: no harness yet combines three real nodes, service writes, tiering and restart.
- A store already fenced by the defects above is not repaired. A store with two durable LSM intents from before the one-job rule stays fail-closed, as it was.
- A job with an unknown outcome holds other LSM jobs and maintenance off until recovery resolves it. That is deliberate fail-closed ownership, not a time bound. The serialization cost is unmeasured.

## Open and not resolved here
Registered in the defect register, each with its own record:
- DC-178 remainder: other write paths (PK and content-hash admission, WRITE_REQ, TABLE_DELETE_REQ, BULK_WRITE_REQ, COUNTER_ADD_REQ) can still end a stream at the door on a transient refusal.
- DC-181: `unresolved_op_fence` alone does not stop the data door. Neither fixed nor shown reachable.
- DC-183: repeated retention rewrites of the same tiered slab; cost not settled.
- DC-185: a Retired input with no backing after an accepted install; not shown to block other GC.
- DC-186: the retirement records scan. DC-188: a row with no effect stays.
- DC-166, DC-167 and DC-168: parallel-exposed test failures, defect candidates per the test concurrency doctrine.
- Local-only GC, on a store outside a ring, keeps memory-only epoch advances.
- A plan's pre-epoch is read before the merge, so maintenance at section 8's rate may starve L0 compaction. Unmeasured.
- Readiness still allocates per check while any intent is unresolved.

None of these is claimed harmless by this record.
