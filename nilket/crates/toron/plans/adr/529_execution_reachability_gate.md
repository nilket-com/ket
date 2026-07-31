# ADR 529 - Execution-reachability gate with settled comparison semantics

ADR 528 proved the distributed substrate can *plan* 20 of 22 board cells with intra-query
parallelism (width 4, single-task producers). It also predeclared that no timing arm exists
until three comparison blockers are settled: aggregate budget ownership, scan/producer
parallelism, and dispatcher/transport semantics. ADR 529 settles all three — as semantics,
not machinery — and then runs the gate that stands between "fragments" and any comparison:
does the substrate actually *execute* the board's queries, and does it produce byte-identical
answers?

## The three settlements

1. **Aggregate budget ownership: division at the native owner.** In the distributed substrate
   the task is the budget owner by construction (`TaskBudget::from_query_budget`,
   `budget.rs:314`). ADR 527 forbade dividing the local executor's budget because one task
   owned the whole query; here the aggregate-holding move IS division plus a frontier cap,
   both native: `memory_per_task = envelope / W` and `QueryBudget::max_in_flight_tasks = W`,
   with `W` the census-admitted maximum stage width. Aggregate resident memory is bounded by
   the envelope by construction, receipts disclose both the divisor and the cap, and no new
   ledger is built. The two `_per_query` dimensions (spill, broadcast-replicated) stay
   query-scoped as named; their per-task copy semantics
   (`from_query_budget` hands each task the full allowance) is a disclosed, known distortion
   this gate observes but does not fix.
2. **Producer/scan parallelism: measure the substrate as built.** Single-task producers are
   the fragmenter's current shape; 528's width facts already say scan-side parallelism is the
   first-order gap. This gate runs that shape unmodified. Wiring `ScanShare` into join-input
   producers is a fragmenter mechanism that earns its own ADR only after this gate shows the
   substrate executes at all — an executability wall would make producer work moot.
3. **Dispatcher/transport semantics: the production path, priced not bypassed.** Loopback TCP
   through `NetworkTaskDispatcher` is the substrate's only dispatcher, so it is the gate's
   coordinate, disclosed on every row. No in-process dispatcher is built: it would not be the
   production substrate, and a bypass would make any later comparison a measurement of
   machinery that does not exist. Pricing the transport floor precisely is the timing ADR's
   burden; this gate's rows carry no timing authority at all.

## Contract

1. **Rail:** a new explicit `execution-census` rail runs q01-q22 through the production
   distributed path — `DistributedQueryCoordinator` with the gauge's knobs under the
   distributed posture (`distributed_gauge_planner_config`), two in-process `ToronWorker`s on
   loopback TCP, each holding the same persisted-Parquet storage the local rail scans. No
   scheduler, fragmenter, executor, or coordinator behavior changes.
2. **Identity is the verdict:** for every cell the rail also runs the local gauge funnel on
   the identical spec and requires digest, row count, and output schema equality. The local
   run is the oracle; a distributed answer that differs is `executes_divergent` — the severe
   class that no follow-up may compare or optimize around.
3. **Envelope (disclosed, explicit):** `W = 4` (the census maximum width);
   `memory_per_task = 512 MiB` (2 GiB / W); `max_in_flight_tasks = 4`; spill reservation
   2 GiB per query; shuffle, in-flight, and broadcast dimensions set generously and recorded
   verbatim on every row. A budget wall at this envelope is an honest capacity finding
   classified by its exact dimension, never retried at a larger size mid-census.
4. **Classification vocabulary** (predeclared, receipts only): `executes_identical`,
   `executes_divergent`, `execution_rejected(<typed reason>)` for any distributed-side
   plan/bind/fragment/dispatch/execute wall, with 528's fragment classifications inherited
   where the wall is the fragmenter (q15 stays `fragment_rejected`).
5. **No timing authority.** Elapsed fields are diagnostic. A thread/substrate timing atlas
   is a separate follow-up authorized only over `executes_identical` cells, with thresholds
   predeclared there, and must carry the transport-floor disclosure.

## Predeclared gate

**If `executes_identical` is empty, ADR 529 closes red as a comparison precondition** — the
wall histogram publishes and prioritizes substrate capability work. Otherwise the
`executes_identical` set is the named eligible set for the timing follow-up, and the wall
histogram for the remaining cells is its named backlog. Divergent cells, if any, are
correctness findings that outrank every other thread on this board.

## Acceptance

1. The rail is invoked only by its explicit command; every default rail is byte-identical,
   pinned by test alongside the ADR 528 guard.
2. Both arms of every cell share one spec, one dataset verification, and one digest policy;
   the identity comparison is total (digest, row count, schema fingerprint).
3. The envelope — divisor, frontier cap, and every budget dimension — is recorded verbatim on
   every row; worker count and transport are named on the row.
4. Typed classification is total over the distributed-side error surface; no wall lands in an
   unrecognized bucket.
5. The full q01-q22 matrix with the wall histogram lands in the impl doc. Standard gates pass
   from the final tree.

## Guardrails

- `executes_identical` proves capability at the disclosed envelope only — it is not
  performance evidence, not a scaling claim, and not evidence the envelope is fair to either
  substrate.
- No fragmenter, scheduler, budget-machinery, or worker fix rides this ADR, whatever the
  walls show; the histogram is the evidence for the next decision.
- The local oracle arm runs the untouched default posture; the ADR 528 posture-leak guard
  extends to this rail.
- The frozen 3,554 ms floor and the closed epoch's receipts are not consulted.

## Forward

- `executes_identical` non-empty → the timing ADR: matched-envelope local-vs-distributed
  wall-clock over exactly that set, thresholds and transport-floor pricing predeclared, and
  only then the producer-parallelism (`ScanShare`) decision with its expected-value framed by
  measured stage shares.
- `executes_identical` empty → the wall histogram names the substrate work (partition-store
  capacity, exchange key coverage, budget semantics), each a separately authorized decision.
- Either way the census pair (fragment + execution) becomes the standing substrate
  reachability instrument for every future scheduler discussion.
