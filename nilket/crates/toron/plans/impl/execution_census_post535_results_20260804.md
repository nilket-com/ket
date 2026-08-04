# execution census re-run after ADR 535 — results (2026-08-04)

A re-run of the ADR 529 execution-reachability census at the post-535 tree, on the
unchanged 529 envelope. Capability and identity only: no engine change rides this
document and no row carries timing authority. The purpose is to replace the stale
board — ADR 535 changed the classification boundary for the eleven cells ADR 529
recorded as `live_shuffle_backpressure_stalled`, so grounding a new ADR against the
529 matrix would have grounded against superseded evidence.

## Identity

- tree: `09a6f29f7055` (`evidence: close producer-scoped host panel groups`), clean
  working tree at build time; every row stamps `build_commit=09a6f29f7055` with no
  `-dirty` suffix;
- binary: release `toron-tpch`, rebuilt from that clean tree (`build.rs` re-stamps only
  when `.git/HEAD` or `.git/index` moves, so the stamp was forced rather than inherited
  from cache);
- dataset: strict sf=8 persisted Parquet, manifest format 2,
  `manifest_fingerprint = fnv1a64:0fc5c06d19c5ed23`, lineitem 47,989,007 rows;
  every row records `dataset_verification=full_hash`;
- envelope: unchanged from ADR 529 — `task_memory_envelope_bytes = 2,147,483,648`
  divided by per-cell width `W` (`memory_per_task = 536,870,912` at `W=4`,
  `2,147,483,648` at `W=1`), `max_in_flight_tasks = Some(W)`,
  `spill_bytes_reserved_per_query = 2,147,483,648`, `worker_count = 2`,
  `transport = tcp_loopback`, `partition_store = in_memory`, memory pool 32 retained
  buffers at 536,870,912 B max capacity;
- scratch: `target/tpch/join_spill` absent before the run;
- artifact: `target/tpch/adr535_execution_census_09a6f29f7055/execution_census.jsonl`
  (22 rows). Twenty-one cells were attempted once (`distributed_attempts = 1`); q15 was
  attempted zero times (`distributed_attempts = 0`) because fragmentation rejected it
  before execution. `transport_retry = false` on all 22 rows.

**Envelope-invariance evidence is source-level, not a diff against the 529 seal.** The
ADR 529 receipt names commit `4b37d45b`, which is not a revision in this repository —
it belongs to the frozen pre-migration nilket history. Invariance was therefore
established from the compiled constants (`EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES`,
`EXECUTION_CENSUS_SPILL_BYTES`, `EXECUTION_CENSUS_WORKER_COUNT` in
`crates/toron-tpch/src/lib.rs`, pinned by the knob-contract test in the same file) plus
review of every post-migration commit touching that file: the 528 rail, the 529 rail,
two 529 fixes, ADR 531, and ADR 535. ADR 535's only edit to the census rail added two
classification arms — `FragmentError::UndrainableLiveShuffle` and routing
`QueryError::Fragment` through `classify_fragment_error`. Nothing altered the envelope.
A reader who needs a byte-level equivalence proof against the 529 seal cannot get one
from this repository.

## Raw artifact matrix

Verbatim from the JSONL, before review. The q04 reason is reproduced as the rail emitted
it and is **invalidated** — see Finding 1. The reviewed board follows this table.

| query | classification | typed reason | local rows | dist rows | digest | schema | stage widths |
|---|---|---|---:|---:|---|---|---|
| q01 | executes_identical | — | 4 | 4 | equal | equal | 1,4,1 |
| q02 | executes_identical | — | 3,747 | 3,747 | equal | equal | 1,1,4,1 |
| q03 | executes_identical | — | 90,442 | 90,442 | equal | equal | 1,1,4,1 |
| q04 | execution_rejected | live_shuffle_backpressure_stalled *(INVALIDATED)* | 5 | — | incomplete | — | 1,1,4,4,1 |
| q05 | executes_identical | — | 5 | 5 | equal | equal | 1,1,4,4,1 |
| q06 | executes_identical | — | 1 | 1 | equal | equal | 1,1 |
| q07 | executes_identical | — | 4 | 4 | equal | equal | 1,1,4,4,1 |
| q08 | executes_identical | — | 2 | 2 | equal | equal | 1,1,4,4,1 |
| q09 | executes_identical | — | 175 | 175 | equal | equal | 1,1,4,4,1 |
| q10 | executes_identical | — | 304,540 | 304,540 | equal | equal | 1,1,4,4,1 |
| q11 | executes_divergent | — | 7,508 | 0 | different | equal | 1,1,4,1 |
| q12 | executes_identical | — | 2 | 2 | equal | equal | 1,1,4,4,1 |
| q13 | executes_identical | — | 44 | 44 | equal | equal | 1,1,4,4,1 |
| q14 | executes_identical | — | 1 | 1 | equal | equal | 1,1,4,1 |
| q15 | execution_rejected | fragment_broadcast_probe_not_star_linear | 1 | — | incomplete | — | — |
| q16 | executes_identical | — | 27,838 | 27,838 | equal | equal | 1,1,4,4,4,1 |
| q17 | executes_identical | — | 1 | 1 | equal | equal | 1,1,4,1 |
| q18 | executes_identical | — | 100 | 100 | equal | equal | 1,1,4,1 |
| q19 | executes_identical | — | 1 | 1 | equal | equal | 1,1,4,1 |
| q20 | executes_identical | — | 1,440 | 1,440 | equal | equal | 1,1,4,1 |
| q21 | executes_identical | — | 100 | 100 | equal | equal | 1,1,4,4,1 |
| q22 | executes_identical | — | 7 | 7 | equal | equal | 1,1,4,4,1 |

Raw histogram as emitted: `executes_identical=19`, `executes_divergent=1`,
`execution_rejected:live_shuffle_backpressure_stalled=1`,
`execution_rejected:fragment_broadcast_probe_not_star_linear=1`. The
`live_shuffle_backpressure_stalled=1` entry is the invalidated q04 row and must not be
quoted as a stall count.

## Reviewed board

- **19 `executes_identical`** — q01-q03, q05-q10, q12-q14, q16-q22.
- **1 `executes_divergent`** — q11.
- **1 fragment-rejected** — q15 (`BroadcastProbeNotStarLinear`, inherited from ADR 528,
  never attempted).
- **1 invalidly classified** — q04. Its emitted stall label carries no evidentiary weight;
  an independently grounded, admission-dependent liveness defect nevertheless sits beneath
  it, established by the controls in Finding 1 rather than by its census row.

## What changed against ADR 529

ADR 535 cleared **ten of the eleven** stalled cells. q01, q03, q12, q13, q14, q16, q18,
q19, q21 and q22 moved from `execution_rejected(live_shuffle_backpressure_stalled)`
straight to `executes_identical` — byte-identical Arrow IPC digests, equal row counts,
equal schema fingerprints, no intermediate divergence. No cell regressed and no cell newly
rejected on budget, so post-535 execution completed within the unchanged `2 GiB / W`
envelope. The artifact records no per-stage publication mode, so it cannot evidence which
path any given cell took; a claim that sealed materialization replaced live pacing in a
specific cell would need a field this census does not emit.

Four cells changed stage shape, all attributable to ADR 531's gather rather than to 535:
q06 `1` → `1,1`, and q14, q17 and q19 `1,1,4` → `1,1,4,1`. Each is a global (no-GROUP-BY)
aggregate acquiring the gather exchange and its single-task terminal fold. q17 was
already re-censused to `executes_identical` under ADR 531 and holds.

The identical set is now 19 cells: q01-q03, q05-q10, q12-q14, q16-q22.

## Finding 1 — q04: an unsound classification over a real admission-bound liveness defect

q04 reports `execution_rejected(live_shuffle_backpressure_stalled)` reproducibly (5/5 in
isolation from the sealed binary, `live_partitions = 8`, `blocked_appenders = 1-2`,
`buffered_chunks` 28-34, `live_eligible_consumer_tasks = 0`, distributed arm ending in
7-13 ms). **That row is not valid evidence of a stall** — the mechanism producing it is
defective, as shown below. Discriminating controls run afterwards established that a
genuine engine liveness defect does sit underneath it, but the census row is not what
proves it.

Two independent defects in the census apparatus produce the label:

**1. The eligibility counter cannot see the consumer that is actually live.**
`live_eligible_consumer_tasks` (`crates/toron-tpch/src/lib.rs:4882`) counts a task only
when `is_streamable_shuffle_consumer(&task.spec)` holds — the unary
scan/filter/project/take shape. ADR 123 (multi-upstream live-input dispatch for
streamable-both consumers) deliberately dispatches a *join* consumer against live refs,
through `LiveInputStorageScan`, whenever every upstream is a streamable producer stage —
covering all 8 HashPartitioned join kinds. Such a consumer is live-eligible by design and
is invisible to this counter. q04's graph (`1,1,4,4,1`: two streamable scan producers into
a join consumer) is precisely the ADR 123 shape. Its `0` therefore means "the counter
cannot see it," not "no live consumer exists," and the ADR 529 premise guard — which
refuses stall classification only when the counter exceeds zero — never arms. This is the
shape-dependent soundness hazard the ADR 529 review already recorded as unpinned; q04 is
its first real instance.

**2. Cancellation at the first blocked append cannot prove persistence.**
ADR 124 bounds unread live backlog per partition and specifies that `append_live` *awaits
capacity* once the bound is reached, pausing the upstream scan. A producer awaiting
capacity while a live consumer builds is the designed steady state of the 123/124 pair,
not a deadlock. The census arms cancellation on the first nonzero blocked-appender
receipt and tears down within milliseconds, so it never distinguishes ADR 124 pacing from
a genuine non-progressing cycle. A stall claim requires an uncancelled observation; this
census has none.

An earlier draft of this receipt attributed q04 to a gap in ADR 535's multi-upstream
drainability arm and asserted that resolver and backstop were wrong together. **That
attribution is withdrawn as unsupported.** The `all(producer_is_live)` arm in
`consumer_supports_live_inputs_when` (`crates/toron/src/distributed/scheduler.rs:1119`)
is the ADR 123 path operating as designed, not an oversight. Applying the unary
`is_streamable_shuffle_consumer` predicate to the multi-upstream arm would seal every hash
join and effectively retire ADR 123 — an architecture decision requiring its own ADR and a
full re-census, never a targeted fix.

### Discriminating controls — diagnostic grounding for ADR 536 (2026-08-04, sf=8)

**Status of this evidence.** These runs are diagnostic grounding that scopes ADR 536. They
are not a formal proof of a deadlock; formal proof belongs in 536's failing tests, which
must fail before the fix and pass after it. The controls ran from a deliberately `-dirty`
probe build whose overrides were removed by targeted edit immediately afterwards, so
**neither the probe binary nor a reconstructable patch survives**. No census row in this
receipt comes from that binary. Re-creating the controls requires re-adding three
temporary overrides, described here in full so the runs are reproducible:

1. `max_in_flight_tasks` in `execution_census_distributed_budget_with_memory`
   (`crates/toron-tpch/src/lib.rs`) overridden from an env var instead of `width.max(1)`.
2. The blocked-appender arm of `execute_census_distributed_attempt` (same file) made to
   `continue` instead of cancelling, so the attempt runs on after the first receipt.
3. `live_stage_ids.clear()` before mode assignment in `resolve_shuffle_publish_modes`
   (`crates/toron/src/distributed/fragment.rs`), forcing every producer sealed.

Each control was one invocation of
`toron-tpch execution-census --query q04 --mode strict --sf 8 --data-source persisted
--data-dir target/tpch/data`, under an external `timeout -s KILL 300` for the uncancelled
arms.

| control | cancellation | `max_in_flight` | publication | result |
|---|---|---:|---|---|
| census default | armed | 4 | live | `live_shuffle_backpressure_stalled` at 7-13 ms |
| admission 6 | armed | 6 | live | `live_shuffle_backpressure_stalled` |
| admission 8 | armed | 8 | live | `live_shuffle_backpressure_stalled` |
| forced sealed | armed | 4 | sealed | **`executes_identical`, 3.6 s** |
| uncancelled | none | 4 | live | **no completion within 300 s** |
| uncancelled | none | 5 | live | **no completion within 300 s** |
| uncancelled | none | 6 | live | **`executes_identical`, 13.7 s** |

**The classification mechanism is unsound, proven directly.** At admission 6 uncancelled, a
blocked-appender receipt appears at 7.6 ms and the query then completes with equal digests
in 13.7 s. A receipt therefore occurs in a configuration that is entirely healthy, so
`blocked_appenders > 0` is not proof of a non-returning wait. The cancelled admission-6 and
admission-8 rows are consequently **false stalls** — the rail cancelled on a transient
receipt. Every cell whose live consumer is an ADR 123 join is exposed to this, which is a
wider blast radius than q04 alone.

**An admission-dependent liveness defect nevertheless sits underneath.** What the runs
establish is bounded and should be stated exactly: uncancelled at admission 4 and at
admission 5, q04 **did not complete within 300 s**; at admission 6 it completed in 13.7 s
with equal digests. No task-dispatch or partition-state samples were captured, so these
runs do not themselves prove zero progress, a permanent drainage state, or a cycle that
could never resolve — only non-completion within the bound, against a working
configuration that finishes in 13.7 s.

The structural reading that makes those observations coherent — and which ADR 536 must
confirm or refute with instrumented tests — is that q04 requires both single-task producers
plus the entire four-task consumer stage resident together: admission 5 admits only 3 of the
4 consumer tasks, leaving a live partition without its consumer. Because a producer can
block on *any* undrained partition, the conservative requirement is the full resident set,
six. The width-5 negative is what keeps this from being a threshold read off a single
successful run, but it remains a hypothesis about mechanism rather than a measured internal
state.

**This is an engine liveness defect, not a census artifact.** The envelope is the declared
operating contract; `max_in_flight_tasks = 4` is a legal admission cap. The engine resolved
`Live` for a graph whose live component cannot be hosted under that cap, and then deadlocked
rather than degrading. Selecting a publication mode the admission bound cannot support is an
engine defect regardless of which harness observes it.

Two working configurations exist today: forced `Sealed` at W=4 (3.6 s) and live publication
at admission 6 (13.7 s). Both return byte-identical results, so q04 carries no correctness
divergence.

### Forward — ADR 536, admission-safe live publication

The decision must be graph- and admission-aware, never a blanket join seal:

> live residency requirement > `max_in_flight_tasks`
> → resolve `Sealed`
> → or synchronously reject an externally forced unsafe `Live` graph

At W=4 q04 must resolve `Sealed`; at a proven-sufficient width it may remain `Live`. The
resolver and the pre-dispatch validator must consume the *same* admission-aware predicate,
so they cannot disagree. Applying the unary `is_streamable_shuffle_consumer` predicate to
the multi-upstream arm is explicitly not the fix — it would seal every hash join and
effectively retire ADR 123.

The census corrections ride alongside: the eligibility counter must recognise ADR 123
multi-upstream live consumers, and the classifier must stop cancelling on a first blocked
append. Until both land, no cell with a 123-shaped consumer can receive a sound stall
classification. Completed byte-identical rows remain valid regardless.

**Blast radius is not enumerable from this artifact.** The JSONL stores stage task widths
but not upstream edges, publication modes, or multi-upstream eligibility, so which of the
19 identical cells run an ADR 123 consumer cannot be determined from it. Inference from
widths alone would be speculation. That enumeration belongs in ADR 536's Phase 0, and the
missing facts should become durable census fields.

## Finding 2 — q11 is unchanged and is the sole divergence

q11 remains `executes_divergent` with local 7,508 rows against distributed 0, digests
different, schema fingerprints equal. Reproduced 3/3 across the full census and two
isolated reruns, with stable plan fingerprints (`local=0d79358a330f29f1`,
`distributed=d92f8e52c0f62554`) and stage widths `1,1,4,1`. ADR 535 neither helped nor
harmed it, which is expected: the constant-key HAVING-scalar class has nothing to do with
publication mode. The cell is a clean, cheap (~1.5 s distributed) reproduction for the
ADR that targets it.

## Verdict

The ADR 529 gate stays green — `executes_identical` is non-empty and now covers 19 of 22
cells. The remaining correctness frontier is exactly one cell, q11, so an ADR targeting
the constant-key HAVING-scalar divergence can proceed against current evidence.

q04's original census row is invalid evidence and its `live_shuffle_backpressure_stalled`
label must not be quoted. The controls nevertheless establish a real engine liveness defect
underneath it: an admission-dependent live-path deadlock at the canonical W=4 envelope,
with no correctness divergence. It is not an ADR 535 regression — the earlier attribution to
535's multi-upstream drainability arm is withdrawn above — and it must not be folded into
the q11 ADR. q15 remains an inherited ADR 528 fragmenter wall
(`BroadcastProbeNotStarLinear`) and is untouched by this census.

**The 19-cell identical set stands.** Those cells reached `executes_identical` on positive
evidence — equal digests, equal row counts, equal schema fingerprints. The counter defect
can manufacture a false *stall*; it cannot manufacture a completed byte-identical result.

Sequencing: **ADR 536 takes q04** (admission-safe live publication, with the census
counter and classifier corrections riding alongside), **ADR 537 takes q11** (the stable
constant-key HAVING-scalar divergence, whose evidence is independent and sound). This exact
census re-runs after 536; the predeclared expected board is 20 `executes_identical`, q11
`executes_divergent`, q15 `execution_rejected`.

Timing over the identical set remains unauthorized by this document. Every row carries
`timing_authority=false`, elapsed is diagnostic only, and the distributed arm pays TCP
loopback on a shared machine — the distributed/local elapsed ratios in the artifact
(q07 29.6 s against 4.6 s, q01 23.2 s against 5.5 s) measure the census substrate, not
the engine, and must not be quoted as a scaling result.
