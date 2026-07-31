# ADR 424 - q18 membership-first composition: the deferred reorder, now on measured ground

## Context

Post-423 board: 17/22 green. ADR 423 designed the membership-first reorder for q18's admitted
chain, then its Phase-0 stop-line fired - the generic membership fold OOMed at the 4 GiB cgroup
(39,017,472 of 59,986,052 rows at the kill) - and the seal pivoted, as pre-authorized, to the
prerequisite: the guarded primitive Int64 grouped-SUM fold. The reorder itself was deferred
**with its design intact** (`plans/adr/423_q18_membership_first_semi_fold.md`: the eight-step
committed sequence, one-shot `Int64PresenceSet`, two-peak lifetime accounting, original-key
ordinal pin, empty-membership short-circuit, direct-filter fences, no-outer-spill mechanism
lock, scheduler receipt, committed/no-late-`None` semantics).

ADR 424 executes that design. Every resource-safety unknown that gated 423 is now banked; the
remaining full-batch and allocator-capacity terms are explicit Phase-0 measurements:

| 423-design unknown | measured fact (423 results) |
| --- | --- |
| membership fold at sf=10 | completes: 59,986,052 rows / 15,000,000 groups, 3.111 s fold / 3.382 s execute; 536,870,912 B completed state, 805,306,368 B transition peak vs the unchanged 1 GiB cap |
| membership cardinality (the LIMIT-100 trap) | 624 qualifying keys at sf=10; 57 at sf=1; zero duplicates/nulls |
| presence-set requested footprint | 1,024 entries + 2,048 buckets = 16,384 bytes; observed allocator capacities are measured in 424 Phase 0, never assumed equal |
| q18's wall | byte-identical reproduction on the 423 binary: `streamed inner join build spill writer` 536,899,968 / 536,870,912, fingerprint `d8b60e3a03bfaf79`, 137,163.7 ms to the wall |

Four pins from this cycle's convergence correct the loose readings of that table:

1. **The 137 s figure is a lower bound, not a total.** It proves 512 MiB of preserved output
   emitted in ~137 s - not that the drive was nearly done; the remaining output fraction is
   unmeasured. The dual projects drive time as `>= ~137 s for the first 512 MiB` and the live
   run settles the total. No green projection is treated as evidence.
2. **Construction-peak inputs are measured batch bytes.** The 4,992-byte key payload is not the
   guard input; Phase 0 measures the actual Arrow `RecordBatch` memory of the post-HAVING
   membership output at sf=1 and sf=10, and the construction-peak guard consumes that number.
3. **424 owns the scheduler/commit receipt.** 423's primitive-fold profile records fold stats
   only. The durable, off-by-default, query-neutral receipt that flushes
   `selected/committed`, `membership_execute_started`, and `outer_writer_constructed=false`
   before membership execution - the accepted-timeout mechanism proof - is new plumbing here.
4. **Baseline threading never shrinks TaskBudget.** The presence-set bytes enter the
   q18-exclusive chain owner's spill/loaded-runtime resident guards as an explicit
   `baseline + local_resident vs unchanged retained-footprint cap` comparison. The task-memory
   value feeding materialization/partition/cache/quota heuristics is untouched; every other
   caller passes a zero baseline.

## Decision

Execute the promoted 423 design with measured parameters. Normative content below restates the
canon where 424 differs or where a pin sharpens it; everything else carries verbatim from the
423 ADR.

### The committed sequence (admitted one-stage chain only)

1. All existing 418 fences plus the direct-filter fences (one LeftSemi key per side,
   `match_filter: None`, `null_safe_keys == false`, identical non-null Int64 key types both
   schemas, one-stage replay-1 chain, terminal metadata shortcut, retained-column/
   grouped-aggregate shape fences). No scan has opened.
2. **Commit**; flush the scheduler receipt (`selected/committed`,
   `outer_writer_constructed=false`). Every later `Ok(None)` is a labeled `InvalidExpression`.
3. Flush `membership_execute_started`; execute the membership subtree - it rides 423's guarded
   primitive fold by construction (its real-plan receipt proved selection at sf=1). On error:
   return it with zero preserved-side scans and no scratch.
4. **Empty membership:** schema-correct zero-row grouped result; no set allocation, no
   preserved preparation or scan. Fold one typed
   `RecordBatch::new_empty(semi_output_schema)` to initialize the grouped output types before
   finalization; do not finalize an untyped empty `group_types` vector.
5. Build the `Int64PresenceSet` (one-shot, guard-first: `row_upper_bound = checked
   sum(num_rows)`;
   `entry_capacity = max(1024, bound).next_power_of_two()` checked with the `u32::MAX` EMPTY
   sentinel preserved; guard requested entries+buckets -> `try_reserve_exact` entries -> guard
   observed entries + requested buckets **before bucket allocation** -> allocate buckets while
   entries live -> guard final observed entries+buckets; duplicate-idempotent; presence-only)
   under the **construction peak** = measured live membership batch bytes + each requested or
   observed allocation state. `try_reserve_exact` capacity equality is never assumed. Drop all
   membership batches only after the final observed-capacity check.
6. Prepare the terminal reoriented runtime with the completed set bytes as the live owner
   baseline, threaded per pin 4 through the q18-exclusive
   `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` owner: both runtime
   preparations, its reservoir helpers, partitioned loaded-build checks, and every
   `SpilledInnerJoinBuild` resident guard it creates. A lost terminal stage is a committed
   error; partial state is cleaned.
7. Drive the chain into the `MembershipFilteredGroupedFold` sink arm: synchronous; probe
   compiled against the **original** `semi_output_schema` / `left_keys` (never the projected
   spill `key_plan`); matching rows retained once (LeftSemi duplicate suppression); the
   survivors are repartitioned with the aggregate's original `partition_count` / `exchange_key`
   exactly as `fold_projected_left_semi_refs` does, and the existing grouped fold consumes each
   non-empty partition. **The semi's outer `SpilledInnerJoinBuild` is never constructed.**
   Append receipt milestones as reached.
8. Record the established replay/counter meanings, clean the runtime unconditionally, finalize.

The materialized/non-chain branch and the `GroupedFold`/`ProjectedSpill` arms stay
byte-identical.

### Phase 0 (re-verification, before code)

Fingerprint + wall reproduction (`d8b60e3a03bfaf79`; 536,899,968 / 536,870,912); census re-run
at HEAD; a temporary/off-by-default exact-membership-subtree harness at sf=1/sf=10 that records
the returned schema, row count, **full `record_batch_memory_bytes` sum**, and primitive-fold
receipt before being removed; the set microfixture (requested vs observed capacities,
construction peak on the real allocator). Generator payload arithmetic cannot supply the
full-batch measurement. All evidence is recorded before code. The OOM stop-line is spent; no
open decision gates the topic.

## Acceptance

- **Primary:** persisted strict q18 sf=10 reaches `pass@compare` (board 18/22), **or** advances
  to a named, reproduced next wall with the `SpillBytes` producer gone - drive-time honesty per
  pin 1 means a >600 s outcome is a legitimate named wall (the scheduler receipt is its
  mechanism proof), not a failure of the cut.
- **Mechanism proof (persisted):** on completion or a typed return, membership scan starts > 0
  and the semi-outer-writer construct counter is exactly zero. On an external timeout, use the
  flushed receipt (selected/committed, membership/chain milestones, and the committed
  `outer_writer_constructed=false` state) plus the direct admission/no-writer locks; do not
  invent unavailable killed-process counters.
- **The full 423 lock set, unchanged** (locks fence the mechanism, not this dataset):
  direct-`Some` + mixed-type/full-row parity incl. duplicate membership keys and boundary
  (exactly-300) exclusion; the membership-first order lock; the empty-set lock; the
  no-outer-spill lock (owner construct counter, inner spill legitimate); presence-set locks
  (layouts, requested-and-observed accounting **without assuming equality**, duplicate
  idempotence, no growth after reservation,
  u32-domain rejection); two-peak guard locks (construction guard with batches live; combined
  set+runtime guard failing where each fits alone); the direct-key fence matrix; committed
  lost-runtime/late-`None` labeled errors; exhaustive cleanup; and a heuristic-invariance lock
  proving the baseline changes resident requested bytes only, not partition/materialization/
  cache decisions. Plus the receipt locks: milestones flush in order, before membership
  execution, off by default, query-neutral.
- **Controls:** the twelve board greens (q03 q07 q09 q10 q11 q12 q13 q14 q16 q19 q20 q22)
  pass@compare with forensic signatures; telemetry meanings preserved.
- **Full engine gate** after fmt; no cap/quota/producer/heuristic change (pin 4); de-branded;
  no `.perf.data`.

## Guardrails

- **Lower-bound honesty on drive time** - no green promise; the live run decides; a timeout
  seals as a named wall with the receipt as proof.
- **Measured batch bytes in the construction guard** - payload arithmetic never substitutes.
- **Baseline never shrinks TaskBudget** - explicit `baseline + local vs cap` only; heuristics
  and all other callers untouched.
- **Original-key ordinals; commit-before-membership-scan; unadmitted paths byte-identical;
  no container refactor** - carried from 423 verbatim.
- **Won ground is a control** - twelve greens re-proven.

## Risks

- **A wall behind the removed writer** (drive time or a downstream limit surfacing past
  512 MiB of output) -> reach honesty; named + receipt-proven is an accepted outcome.
- **Two-peak guards firing on real numbers** -> unlikely at a 16 KB requested set footprint,
  but observed capacities decide and the guards/locks stay - they fence the mechanism.
- **Ordinal drift in the filter** -> the original-key pin + parity locks.
- **Receipt plumbing leaking into hot paths** -> off-by-default env gate, flushed at
  milestones only, query-neutral naming.

## Forward Arc

q18 green closes the quota/ordering thread and leaves q21 (throughput - profiling must first
separate fold time from the six probe stages; the 423 primitive fold is an ingredient, not a
claim), q02 (join-output C-spec), q05 (diagnosis-first OOM; the unguarded-fold class 423 fixed
is a candidate suspect there), and q17 (policy). If q18 instead names a drive-time wall, that
wall lands in the same throughput family as q21 and sharpens the profiling cut's evidence base.

## Grounding evidence (file:line)

Design canon: `plans/adr/423_q18_membership_first_semi_fold.md` + impl plan (committed
sequence, presence set, two peaks, ordinal pin at `streaming.rs:7300-7310`, owner threading at
`try_streaming_reoriented_inner_chain_scan_probe_base_to_sink`). Measurements: 423 results
(membership fold 3.111 s / 536,870,912 B / 805,306,368 B peak; census 624/57 keys, zero
dup/null; presence-set request 16,384 B; wall reproduction 536,899,968 / 536,870,912 @ 137,163.7 ms
with `d8b60e3a03bfaf79`; OOM evidence 39,017,472 rows @ 4 GiB). Execution-order seam:
`streaming.rs:7243/:7359/:7442/:7456`; sink dispatch `:7626-7669`, adaptive `:8025-8073`.
Primitive fold: `exec/aggregate.rs:43` (423). q18 SQL + LIMIT: `toron-tpch/src/lib.rs:162-174`.
