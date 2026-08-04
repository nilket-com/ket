# ADR 423 - q18 membership-first semi fold: stop spilling half a gigabyte before membership

## Context

Post-422 board: 17/22 green. q18 carries two walls: the `SpillBytes` quota
(`streamed inner join build spill writer`, 536,899,968 / 536,870,912 - 29,056 bytes over the
512 MiB writer allowance) and, behind it, a membership side that has **never executed at sf=10**
(418's scan table: membership lineitem = 0 starts, "the preserved projected writer reaches its
cap first").

The council grounding reframed the wall as a **scheduling defect**, the 420/421 pattern applied
to ordering instead of representation:

- **The admitted q18 chain writes first and asks later.** In
  `try_streaming_projected_left_semi_grouped_aggregate` the committed arm prepares the terminal
  runtime, drives the entire preserved chain into `ReorientedChainOutputSink::ProjectedSpill`,
  finishes the outer `SpilledInnerJoinBuild`, and only then calls `execute_bound(right)`
  (`streaming.rs:7243/:7359/:7442/:7456`). The 512 MiB of preserved pages exist only to defer
  probing against the membership result.
- **The membership result is plausibly selective, but not yet measured.** q18's semi filter is
  `o_orderkey IN (SELECT l_orderkey ... HAVING SUM(l_quantity) > 300)`
  (`toron-tpch/src/lib.rs:162-174`) - the extreme tail of order sizes. **Caveat pinned during
  iteration:** the outer `LIMIT 100` makes the published answer count an invalid basis for
  extrapolating cardinality; Phase 0 censuses the pre-limit post-HAVING key count directly at
  sf=1 and sf=10. The mechanism is selected from measured cardinality (thousands is still fine),
  never from the limited answer.
- **The reorder:** execute the independent membership subtree first, hold the post-HAVING keys
  in a budgeted resident presence set, and stream the chain's completed batches through a
  membership-filtered fold arm. The SQL suggests a selective tail, but neither membership nor
  outer-fold cardinality is credited until Phase 0 measures it. **The semi's outer projected
  spill is never constructed.** The quota wall is not shrunk - the writer it fires in is
  removed from the path.
- **The sink seam already exists.** `emit_reoriented_chain_output` dispatches
  `GroupedFold`/`ProjectedSpill` (`:7626-7669`), the adaptive terminal path has the same two-way
  dispatch (`:8025-8073`); a third synchronous membership-filtered fold arm needs no new
  abstraction and no second chain driver.

### The honest hazard this cut must face first

`execute_bound(right)` reaches the membership aggregate through
`try_streaming_scan_grouped_aggregate` (`aggregate.rs:173`), which folds scan batches into
`BTreeMap<Vec<GroupValue>, Vec<AggState>>` (`:194`) with **no task-memory guard in the fold
loop**. At sf=10 it must retain ~15M order-key states before HAVING discards almost all of
them - unaccounted resident growth, 10x the largest completing fold (q13's 1.5M groups).

Phase 0 therefore runs the isolated membership subtree under the persisted sf=10 cgroup first,
and the outcome gates the topic:

- **completion** -> proceed; q18 plausibly reaches comparison in this seal;
- **typed budget error or in-gate timeout** -> proceed; an honest named-wall advance that names
  the primitive grouped-fold follow-on precisely;
- **kernel/cgroup OOM or any unaccounted resident failure** -> **stop and pivot**: shipping a
  reorder that replaces 418's loud quota producer with an ungoverned OOM is a regression in
  honesty. The primitive grouped-fold work becomes ADR 423 and membership-first follows it
  unchanged.

## Decision

### The presence set: 421's lineage, one-shot construction

A local `Int64PresenceSet` beside the chain owner - ADR 421's open-addressed discipline
(u32 buckets at 0.5 load, seeded hasher, linear probing, checked `try_reserve_exact`,
requested- and observed-capacity guards, owner-baseline accounting) **minus** the representative
column, Multi bitset, and dynamic doubling loop. 421 streams an unknown cardinality and must
grow; 423 holds the complete post-HAVING `Vec<RecordBatch>` first, so
`sum(num_rows)` is an exact upper bound known before allocation:

```text
row_upper_bound = sum(membership_batches.num_rows)      # zero -> short-circuit, no set at all
entry_capacity  = max(1024, row_upper_bound).checked_next_power_of_two()
bucket_capacity = entry_capacity x 2
entries: Vec<i64> (8 B)    buckets: Vec<u32> (4 B)
```

Guard the requested entries+buckets total, allocate entries with `try_reserve_exact`, guard its
**observed** capacity, then allocate/resize buckets while the observed entries allocation is
live and guard the final observed total. `try_reserve_exact` is not assumed to return capacity
equal to the request. Insertion may change lengths but must not change either capacity. A
checked target that cannot leave `u32::MAX` available as the EMPTY bucket sentinel rejects
before allocation. Duplicates find the existing entry and consume no slot (grouping should make
keys unique; presence semantics do not rely on it). Lookup is presence-only - no
representative, no key-domain sentinel, no dummy value. **Stop-line:** if sharing the probing
core would require a generic container refactor, keep the implementation local - this is not a
container-abstraction cut.

### Two real peaks, accounted by lifetime

Membership batches are dropped before the inner runtime is prepared, so the owner accounts two
distinct peaks against the unchanged retained-footprint cap - never a fictitious all-live sum:

1. **Set-construction peak:** live membership batch bytes + the complete requested set
   allocation (guard formula identical to 421's, with the "old set" term empty); observe actual
   capacity before crediting anything.
2. **Chain peak:** completed set bytes + every retained byte of the prepared inner runtime -
   with the set bytes threaded as the owner baseline **into** runtime preparation/drive guards
   before they allocate; a post-allocation sum check is insufficient. This baseline is carried
   through the q18-exclusive `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink`
   owner, both terminal/base runtime preparation calls, its reservoir helpers, and every
   `SpilledInnerJoinBuild` resident guard created by that owner. Retained guards compare
   `presence_set_bytes + local_resident_bytes` with the original retained-footprint cap. They do
   **not** reduce the task-memory value used for materialization/partition decisions, and other
   callers retain a zero baseline.

Batch bytes are not charged after the batches drop; their release is not credited before the
set's actual capacities are checked. Both peaks report a membership-first-specific producer
label. The membership aggregate's internal 15M-group map dies inside `execute_bound(right)`
before either peak and is a Phase-0 resource-safety question - this cut does not pretend the
new accounting governs it.

### The exact committed sequence (admitted one-stage chain only)

1. All existing 418 fences plus the direct-filter fences: one LeftSemi key per side,
   `match_filter: None`, `null_safe_keys == false`, identical non-null Int64 key types on both
   schemas (no generic/nullable/composite/cast widening), the one-stage replay-1 chain, terminal
   metadata shortcut, retained-column/grouped-aggregate shape fences. No scan has opened.
2. **Commit.** Every later `Ok(None)` is a labeled `InvalidExpression`.
3. Execute the membership subtree. On error: return it - zero preserved-side scans, no scratch.
4. **Empty membership:** finalize a schema-correct zero-row grouped result (folding a typed
   empty semi-input once if group types require it) - no set allocation, no preserved
   preparation, no preserved scan.
5. Otherwise build the presence set under the construction peak; drop all membership batches.
6. Prepare the terminal reoriented runtime with the completed set bytes as its live owner
   baseline. A lost terminal stage is a committed error; partial runtime/spill state is cleaned.
7. Drive the chain into the membership-filtered fold arm. **Probe the original
   `semi_output_schema` / `left_keys` ordinals** - 418's `key_plan` is compiled against the
   projected spill schema (`streaming.rs:7300-7310`) and would address the wrong columns on the
   full chain batch. Retain matching rows once (LeftSemi duplicate suppression), invoke the
   existing grouped fold. The semi's outer `SpilledInnerJoinBuild` is never constructed.
8. Record the established replay/counter meanings, clean the runtime unconditionally, finalize.

The materialized/non-chain branch and both original sink arms stay byte-behavior unchanged.

### Phase 0 - the decision table (before the production cut)

| probe | required evidence | decision |
| --- | --- | --- |
| exact q18 plan | one Int64 non-null key per side, null-safe false, replay-1 chain + terminal shortcut | mismatch -> stop/re-scope; no data-derived eligibility |
| sf=1 + sf=10 census (bounded generator pass, NOT `strict_q18_oracle(10)` and not inferred from the timed run) | pre-limit post-HAVING rows, duplicate/null counts, output-batch bytes, projected set capacities | insufficient retained headroom -> stop/re-scope Fork C |
| isolated sf=10 membership subtree under the persisted cgroup | elapsed, peak RSS, terminal status, completed groups on return or last durable progress on timeout | complete / typed error / in-gate timeout -> continue; **OOM -> pivot topic** |
| set microfixture | requested vs actual capacity; construction peak | allocator surprise or implicit growth -> stop, fix accounting |
| runtime owner | set + retained runtime bytes, baseline charged pre-allocation | combined crossing -> loud Memory error, never fallback |

A timed-out isolated run must be distinguishable from a hang and leave no scratch. Because an
externally killed process cannot return final in-memory counters, the temporary Phase-0 harness
must durably checkpoint scan-open/fold progress; completed-group count is required on
completion, while a timeout records the last durable input-row/group progress rather than
inventing a final count.

The census itself is O(1) resident state: `tpchgen` emits every order's lineitems contiguously
and `OrderGenerator::make_order_key(index)` is monotonic. Stream one current order key and
checked quantity sum, assert each next key is greater, and count sums strictly above 300.
Do not call the existing sf=10 q18 oracle for this measurement: it constructs order and
quantity hash maps and would turn the census into a second large-memory experiment.

## Acceptance

- **Primary:** persisted strict q18 sf=10 either reaches `pass@compare`, or advances to the
  **measured membership-fold wall** (typed producer or in-gate timeout, byte/time-recorded) with
  the `SpillBytes` producer gone. An ungoverned OOM is not an acceptable outcome (stop-line).
- **Mechanism proof (persisted):** on completion or a typed return, membership scan starts > 0
  and there are **zero writes to the semi's outer projected writer specifically** (narrow
  counter/hook) - not `spill_bytes == 0`, since inner stages may legitimately spill. If the
  accepted outcome is external timeout, a flushed off-by-default scheduler receipt must prove
  that the membership-first arm committed and began membership execution before any outer
  writer was constructed; the direct scan-order/no-writer locks remain the counter proof. Do
  not claim final in-memory counters from a killed process.
- **Eligibility-discriminating locks:** direct-`Some` + mixed-type/full-row parity incl.
  duplicate membership keys; the membership-first order lock (membership storage error opens
  the membership table, zero preserved tables, empty scratch); the empty-set lock (typed empty
  output, zero preserved scans, zero set allocation); the direct-key fence matrix
  (nullable/null-safe/non-primitive/composite/missing-metadata/replay-2/longer-chain -> `None`
  pre-I/O); presence-set locks (entry/bucket sizes, requested-and-observed capacity accounting
  without equality assumptions, duplicate idempotence, no growth after reservation,
  u32-domain rejection); the two-peak guard locks
  (construction guard with batches live; combined set+runtime guard that fails when either
  alone would fit); committed lost-runtime/late-`None` labeled errors; exhaustive cleanup.
- **Controls:** the twelve board greens (q03 q07 q09 q10 q11 q12 q13 q14 q16 q19 q20 q22)
  pass@compare with forensic signatures (q11: 166,552,400); telemetry meanings preserved.
- **Full engine gate** after fmt; no cap/quota/producer change on unadmitted paths; de-branded;
  no `.perf.data`.

## Guardrails

- **The OOM stop-line** - a controlled wall may seal; an unaccounted one pivots the topic.
- **Measured cardinality only** - the LIMIT-capped answer never sizes anything.
- **One-shot guard-first reservation** - no doubling loop, no implicit growth, no container
  refactor.
- **Lifetime-honest accounting** - two peaks, baselines threaded pre-allocation, no all-live
  fiction, no early release credit.
- **Original-key ordinals** - the filter compiles against `semi_output_schema`/`left_keys`,
  never the projected spill plan.
- **Commit-before-membership-scan** - no late `None`, no fallback, no silent rescan.
- **Unadmitted paths byte-identical** - the materialized branch and both sink arms unchanged.
- **Won ground is a control** - twelve greens re-proven.

## Risks

- **The membership fold walls (typed/timeout)** -> accepted named advance; it names the
  primitive grouped-fold follow-on with measured evidence.
- **The membership fold OOMs** -> stop-line; topic pivots to guarded/primitive grouped-fold
  work with this design intact behind it.
- **Membership cardinality larger than headroom** -> Fork C re-scopes; no silent spill path.
- **Probe-side ordinal drift** -> the original-key pin + parity locks.
- **A wall behind the wall** (the chain's own drive at sf=10 with the fold live) -> reach
  honesty; the 418 pipeline already proved the chain side to 512 MiB of output.

## Forward Arc

q18 green (or its measured membership wall) leaves q21 (throughput - profiling must first
separate fold time from the six probe stages; **no q21 speedup claim enters this dual**), q02
(join-output C-spec), q05 (diagnosis-first OOM - note the unguarded-fold class found here is a
candidate suspect), and q17 (policy). The primitive single-Int64 grouped-fold specialization
(the 414/420/421 open-addressed lineage applied to `BTreeMap<Vec<GroupValue>>`) is the named
next mechanism either way: as q18's membership fix if the wall fires, as the speed thread's
opening if profiling elects it.

## Grounding evidence (file:line)

Execution order: `exec/join/streaming.rs:7243` (arm), `:7359` (chain -> ProjectedSpill),
`:7442` (`execute_bound(right)` after finish), `:7456` (probe/fold). Sink seam: `:7626-7669`,
adaptive `:8025-8073`. Projected key plan (the wrong ordinals for direct filtering):
`:7300-7310`. Membership aggregate: `exec/aggregate.rs:173` (recognizer), `:194` (unguarded
`BTreeMap` fold). q18 SQL + LIMIT: `toron-tpch/src/lib.rs:162-174`. Walls: 418/419/420/421
results (SpillBytes row byte-exact; membership 0 starts). Presence-set lineage:
`NeqExistenceSummary` (421; guard-first checked reservation pattern). Census quantity:
the same pre-limit qualifying-order quantity exposed by `Q18Oracle`, measured by the bounded
streaming generator census at sf=1/sf=10 in Phase 0 rather than by constructing that oracle.
