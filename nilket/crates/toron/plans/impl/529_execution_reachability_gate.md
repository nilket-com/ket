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
- the envelope (disclosed on every row): `W = 4`; `memory_per_task = 536,870,912 B`
  (2 GiB / W); `max_in_flight_tasks = Some(4)`; `spill_bytes_reserved_per_query =
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
   `execution_rejected(<reason>)` (coordinator error — classified by an exhaustive match
   over the distributed error surface, reusing 528's `classify_fragment_error` for fragment
   walls and typed budget-dimension slugs for `BudgetExhausted` walls; nothing lands
   unrecognized).
3. `execution-census` command mirroring `fragment-census`: q01-q22 or `--query`, JSONL +
   human rows + histogram + summary; exits nonzero only on harness errors, never on an
   honest wall.
4. Tests: two-worker digest parity on small memory fixtures (a grouped aggregate and a join
   at sf=0.01) pinning `executes_identical` mechanics end-to-end; a forced-tiny-budget cell
   pinning a typed `execution_rejected` budget classification; envelope-disclosure and
   vocabulary coherence on the row; the ADR 528 leak guard extended: no default rail
   constructs workers or the coordinator, census posture stays census-only.

## Step 2 - seal and gates

Commit `impl: adr 529 execution census rail`; the standard full battery with direct exit
statuses (both fmt checks, three `--all-targets` checks, toron lib, full `architecture_test`,
complete `--no-fail-fast`, full `toron-tpch`, `git diff --check`); push; build one release
binary from the sealed identity.

## Step 3 - census run

From the sealed binary: q01-q22, strict sf=8 persisted, full-hash, the disclosed envelope,
join-spill scratch cleared first, one run per cell (identity is deterministic; a transport
flake retries once and is noted on the row). Elapsed recorded as diagnostic only.

## Step 4 - predeclared verdict and receipt

Append the matrix (classification, identity facts, wall reasons) and the wall histogram.
Apply the gate: empty `executes_identical` → close red as comparison precondition; otherwise
name the eligible set for the timing follow-up. Any `executes_divergent` cell is flagged as
a correctness finding that outranks the perf thread. No result authorizes timing, producer
parallelism, or store changes — those are the follow-up decisions this receipt informs.

## Repository gates

As step 2; the battery repeats after any code change made in response.
