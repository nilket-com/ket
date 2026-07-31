# impl 529 - execution-reachability gate plan

Capability only: run q01-q22 through the production distributed path (coordinator + two
in-process loopback workers) at a disclosed fixed-aggregate envelope, verify byte-identity
against the local oracle arm, classify every wall from typed receipts. No engine behavior
change, no timing claim.

## Immutable inputs

- tree: post-528 main (`2d16e64b` lineage), all activated knobs default on;
- dataset: strict sf=8 persisted Parquet, manifest format 2 — the same
  `build_persisted_perf_query_spec` spec feeds both arms of a cell;
- eligible walls from 528: q15 `fragment_rejected(BroadcastProbeNotStarLinear)` inherited;
  q06 fragments sequential and still runs (executability applies to its single task);
- the substrate template: `ToronWorker::with_partition_store_and_endpoint` on a loopback
  `TcpListener` accept loop (`runtime_filter_pushdown_test.rs:512-542`),
  `StaticWorkerRegistry`, `DistributedQueryCoordinator::new(catalog, registry, memory,
  TcpTransport, distributed_gauge_planner_config(), envelope)` and `execute_query(&logical)`
  (`coordinator.rs:160,395`);
- the envelope (disclosed on every row): per-cell `W = max_stage_width` (`4` for the 20
  parallel cells, `1` for q06); `memory_per_task = 2,147,483,648 B / W`;
  `max_in_flight_tasks = Some(W)`; `spill_bytes_reserved_per_query =
  2,147,483,648 B`; shuffle/in-flight/broadcast dimensions generous, exact values recorded
  at implementation and pinned by the knob-contract test;
- the oracle: the untouched local gauge funnel on the same spec; identity =
  `canonical_arrow_ipc_digest` under one `SortPolicy` + row count + output schema
  fingerprint.

## Step 0 - partition-store grounding (bounded)

The shuffle store choice decides whether sf=8 intermediates live in RAM. Ground
`InMemoryPartitionStore::with_budget` vs `HybridPartitionStore`/`EncryptedSpillPartitionStore`
(`partition_store.rs:304,856,1098`): constructor, budget semantics, spill root. Pick the one
matching production capacity semantics, name it in the row, and record why. This is a
selection among existing stores — building or modifying a store is out of scope.

Grounding verdict: select `InMemoryPartitionStore::with_budget`. It is the store constructed by
the production daemon and fixture worker today (`daemon.rs:245`,
`bin/toron_fixture_worker.rs:26`); selecting a hybrid or encrypted spill store would introduce a
capacity path the production worker does not use. The row records the store budget and the
coordinator/worker `MemoryManager` pool configuration. The fixed 2 GiB statement applies only to
concurrent task-memory allowances; store, transport, result, spill, and broadcast ownership are
separately disclosed.

## Step 1 - execution-census rail

1. Library entry `run_execution_census_query`: build the persisted spec once; arm 1 runs the
   local funnel (oracle digest, row count, schema fingerprint); arm 2 spawns two in-process
   workers with the spec's storage and the chosen partition store, drives
   `DistributedQueryCoordinator::execute_query`, digests under the same policy, tears the
   workers down. Emit one typed `ExecutionCensusRow`: query, classification, both digests,
   row counts, the full envelope verbatim, worker count, transport (`tcp_loopback`), store
   choice, and the typed wall (reason + message) on rejection.
2. Classification derives from receipts: `executes_identical` (all three identity facts
   equal), `executes_divergent` (distributed answered, any identity fact differs),
   `execution_rejected(<reason>)`. Before coordinator execution, repeat ADR 528's typed bind and
   fragment preflight; its `FragmentError` is classified exhaustively by
   `classify_fragment_error`. For admitted plans, classify the exposed `QueryError` outer enum,
   nested `ExecutionError`, and `RuntimeError::TaskFailure`/`TaskFailureKind` exhaustively;
   budget reasons include their typed dimension. `QueryError::Internal` is the recognized
   `query_internal` reason with its message because the coordinator intentionally erases several
   internal source types at that boundary. Nothing lands in an unrecognized bucket and no string
   parsing counterfeits a typed wall. Scale grounding exposed one non-returning production path:
   `InMemoryPartitionStore`'s four-chunk live backlog can block a producer before the scheduler
   starts its downstream stage. A behavior-neutral `LivePartitionBackpressureObservation` counts
   appenders actually awaiting that existing semaphore. A nonzero receipt authorizes
   `live_shuffle_backpressure_stalled` and cancellation for cleanup; no elapsed threshold decides
   the classification and no scheduler/store behavior changes. Worker connection errors are
   ignored only after that typed receipt has armed cancellation cleanup; without the receipt they
   remain harness failures.
3. `execution-census` command mirroring `fragment-census`: q01-q22 or `--query`, JSONL +
   human rows + histogram + summary; exits nonzero only on harness errors, never on an
   honest wall.
4. Tests: two-worker digest parity on small memory fixtures (a grouped aggregate and a join
   at sf=0.01) pinning `executes_identical` mechanics end-to-end; a forced-tiny-budget cell
   pinning a typed `execution_rejected` budget classification; envelope-disclosure and
   vocabulary coherence on the row; the store test pins blocked-appender receipt lifecycle; the
   ADR 528 leak guard extended: no default rail constructs workers or the coordinator, and only
   the census consumes the liveness receipt.

## Step 2 - seal and gates

Commit `impl: adr 529 execution census rail`; the standard full battery with direct exit
statuses (both fmt checks, three `--all-targets` checks, toron lib, full `architecture_test`,
complete `--no-fail-fast`, full `toron-tpch`, `git diff --check`); push; build one release
binary from the sealed identity.

## Step 3 - census run

From the sealed binary: q01-q22, strict sf=8 persisted, full-hash, the disclosed envelope,
join-spill scratch cleared first, one run per cell (identity is deterministic; a transport
flake exposed as `QueryError::Io` retries once and is noted by `distributed_attempts` and
`transport_retry` on the row). Elapsed recorded as diagnostic only; no other wall retries.

## Step 4 - predeclared verdict and receipt

Append the matrix (classification, identity facts, wall reasons) and the wall histogram.
Apply the gate: empty `executes_identical` → close red as comparison precondition; otherwise
name the eligible set for the timing follow-up. Any `executes_divergent` cell is flagged as
a correctness finding that outranks the perf thread. No result authorizes timing, producer
parallelism, or store changes — those are the follow-up decisions this receipt informs.

## Repository gates

As step 2; the battery repeats after any code change made in response.

## Implementation receipt — 2026-07-31

Sealed implementation chain: `a0023ec2` (execution-census rail) → `a7dc0eb3`
(typed live-backpressure receipt) → `4b37d45b` (receipt-armed cancellation cleanup). The release
binary at the final identity ran all 22 cells against strict sf=8 persisted Parquet with full-hash
verification. Artifact:
`target/tpch/adr529_execution_census_4b37d45b/execution_census.jsonl` (22 rows, all
`build_commit=4b37d45b20eb`, all `timing_authority=false`, zero transport retries).

| query | classification | typed reason | local rows | distributed rows | digest | schema | stage widths |
|---|---|---|---:|---:|---|---|---|
| q01 | execution_rejected | live_shuffle_backpressure_stalled | 4 | — | incomplete | — | 1,4,1 |
| q02 | executes_identical | — | 3,747 | 3,747 | equal | equal | 1,1,4,1 |
| q03 | execution_rejected | live_shuffle_backpressure_stalled | 90,442 | — | incomplete | — | 1,1,4,1 |
| q04 | execution_rejected | live_shuffle_backpressure_stalled | 5 | — | incomplete | — | 1,1,4,4,1 |
| q05 | executes_identical | — | 5 | 5 | equal | equal | 1,1,4,4,1 |
| q06 | executes_identical | — | 1 | 1 | equal | equal | 1 |
| q07 | executes_identical | — | 4 | 4 | equal | equal | 1,1,4,4,1 |
| q08 | executes_identical | — | 2 | 2 | equal | equal | 1,1,4,4,1 |
| q09 | executes_identical | — | 175 | 175 | equal | equal | 1,1,4,4,1 |
| q10 | executes_identical | — | 304,540 | 304,540 | equal | equal | 1,1,4,4,1 |
| q11 | executes_divergent | — | 7,508 | 0 | different | equal | 1,1,4,1 |
| q12 | execution_rejected | live_shuffle_backpressure_stalled | 2 | — | incomplete | — | 1,1,4,4,1 |
| q13 | execution_rejected | live_shuffle_backpressure_stalled | 44 | — | incomplete | — | 1,1,4,4,1 |
| q14 | execution_rejected | live_shuffle_backpressure_stalled | 1 | — | incomplete | — | 1,1,4 |
| q15 | execution_rejected | fragment_broadcast_probe_not_star_linear | 1 | — | incomplete | — | — |
| q16 | execution_rejected | live_shuffle_backpressure_stalled | 27,838 | — | incomplete | — | 1,1,4,4,4,1 |
| q17 | executes_divergent | — | 1 | 4 | different | equal | 1,1,4 |
| q18 | execution_rejected | live_shuffle_backpressure_stalled | 100 | — | incomplete | — | 1,1,4,1 |
| q19 | execution_rejected | live_shuffle_backpressure_stalled | 1 | — | incomplete | — | 1,1,4 |
| q20 | executes_identical | — | 1,440 | 1,440 | equal | equal | 1,1,4,1 |
| q21 | execution_rejected | live_shuffle_backpressure_stalled | 100 | — | incomplete | — | 1,1,4,4,1 |
| q22 | execution_rejected | live_shuffle_backpressure_stalled | 7 | — | incomplete | — | 1,1,4,4,1 |

Histogram: `executes_identical=8`, `executes_divergent=2`,
`execution_rejected:live_shuffle_backpressure_stalled=11`, and
`execution_rejected:fragment_broadcast_probe_not_star_linear=1`. Every backpressure row carries
exactly one worker receipt with one appender actually waiting on the live backlog semaphore; the
recorded buffered-chunk totals are respectively 12, 16, 31, 16, 16, 16, 14, 16, 16, 16, and 16
in query order over that wall set. No row has a schema mismatch.

Gate verdict: the nonempty identical set names the only execution-reachable comparison candidates
(`q02,q05,q06,q07,q08,q09,q10,q20`), but **q11 and q17 are correctness findings and therefore
block any timing follow-up** until separately diagnosed and resolved. q11 returns zero distributed
rows versus 7,508 locally; q17 returns four distributed rows versus one locally. The 11-cell
backpressure family is a scheduler/store-liveness backlog, not a request to change the store in
this ADR. No timing, ScanShare, scheduler, or capacity work is authorized by this receipt.

Final gates: both format checks; all three `--all-targets` checks; Toron library (1,461/1,461),
architecture (2,095/2,095), and complete `--no-fail-fast` suite; serialized full `toron-tpch`
(75 library passed/3 ignored, 52 CLI, 21 architecture, 5 Arrow decimal, 176 correctness/2 ignored,
12 decimal, 44 harness, 2 q14, doc tests); `git diff --check`. The two existing Toron warnings
(unused `QueryBudget` import and unused `inner_resident_bytes`) are unchanged.

## Refinement — stall-classification premise guard (post-review)

Review grounding showed the `blocked_appenders > 0` ⟺ non-returning-wait equivalence holds
only because no fragmented census plan has a live-eligible shuffle consumer
(`streamable_unary_plan` admits scan/filter/project/take chains only, so every aggregate,
join, and sort consumer waits for producer seal). Per the review directive, that premise is
now enforced rather than assumed: `FragmentShapeReceipt` carries
`live_eligible_consumer_tasks` (disclosed on both census rows), the execution rail refuses to
classify a stall when the count is nonzero (a loud harness error demanding consumer-dispatch
state in the receipt), and `stall_premise_no_live_eligible_consumers_across_board` pins the
zero count across q01-q22. The sf=8 matrix above is unaffected: all classified cells satisfy
the premise by grounded fact.

Note on identities: the sealed chain was rebased onto `71df5062` (`plan: close the durable
toron dogfood loop`) before publication; `a0023ec2 → a7dc0eb3 → 4b37d45b → 289e1ec4` became
`a7fad8b6 → d64750fb → f8c0da02 → 6b367cb0` with byte-identical crate content. The artifact
directory name and its rows' `build_commit = 4b37d45b20eb` record the pre-rebase build
identity of that identical tree.
