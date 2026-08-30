# ADR 580 - Consuming join inputs without concatenating them

**Status:** Open. Measurement authorized; the structural change is not, and this
record predeclares what would authorize it.

## Context

ADR 554 closed as an attribution question and its successor evidence
(`plans/impl/0554_join_output_materialization_results.md`) carried a perf line it
explicitly said it did not measure. That line grew: brackets, a per-side split, a
zero-copy slice fast path, an index-build deferral, a substitution that was
measured and reverted. Useful work, but it accumulated under a closed record with
no decision boundary of its own. **ADR 554 is frozen at `0ab7abf3`.** This record
owns what remains, and it begins where that evidence ends rather than restating
it.

What that evidence establishes, on the last authoritative run (`e45f5e75`,
`adr554_index_deferral_sf8.jsonl`, SF 8):

| named subterm | board ms |
|---|---:|
| join **input** concatenation | **7,100.6** |
| join output gathering | 3,923.6 |

Input concatenation is now the larger half by a factor of 1.8, and nothing has
taken it apart. Its top cells are q09 2,334.7, q08 1,731.1, q17 900.2, q05 691.5,
q21 445.5, q02 437.2. q17 is the discriminating cell: 900.2 ms of input concat
against 3.1 ms of output gather, so anything measured there is the input side
alone.

Four charged sites produce it, both `concat_or_empty` pairs in
`execute_hash_join` and `execute_hash_join_into_global_aggregate`. Each turns a
`&[RecordBatch]` into one `RecordBatch` before the join does anything else.

**How large the change is, is itself a question — and the first draft of this
record got it wrong.** It asserted that consuming inputs directly means
`(batch, row)` or global row addressing, reopening the selection vocabulary and
the gather machinery in `exec/join/output_materialization.rs`, slice fast path
included. The code says otherwise for the probe side. `execute_hash_join` already
returns `Vec<RecordBatch>`, and `probe_in_memory_join_rows` already takes ONE
probe batch and produces batch-LOCAL `u32` row indices. A join that probes one
source batch at a time therefore needs no new addressing at all: each probe batch
produces its own selection over its own rows, materializes into its own output
batch, and the whole-source slice rule applies per probe batch exactly as it does
today.

What such a design does need is join-wide state where the semantics are
join-wide: `build_match_bitmap` is built per probe call today and would be hoisted
to the join, with unmatched-build emission deferred until the last probe batch.
That affects the left-preserving kinds — `LeftOuter`, `LeftSemi`, `LeftAnti` —
and nothing else; `Inner` and the right-preserving kinds are batch-local as they
stand. `execute_hash_join_into_global_aggregate` is the cleanest case of all: it
is Inner-only and folds each batch immediately, so there is no output selection to
carry across batches.

So the addressing change is a possible LATER design, not a premise. The smaller
one comes first and gets priced first.

## Decision

### 1. Measure the parent by side and shape before designing anything

The instrument is four nested children —
`join_input_{left,right}_{single,multi}_batch_micros`, charged from the same clock
read as their parent — plus, per side, invocation counts, batch/row/slice-byte
totals and a bounded batch-count histogram.

The side split alone was built first (`0ab7abf3`) and **could not have answered
this record's own gate**: it recorded task totals with no invocation count, so it
could not say whether a task's batches arrived in one wide concatenation or many
narrow ones, and it charged single-batch wrapper work to the same counter as
multi-batch copying. Both gaps are repaired before the first run rather than
discovered in its numbers.

No design work precedes that run, and no design is justified by the 7,100.6 ms
scalar alone — a parent counter locates time, it does not name a mechanism.

### 2. The measurement decides which of four projects this is, by predeclared rule

- **Single-batch wrapper overhead.** If the time sits on concatenations of ONE
  batch, it is not copy cost at all: arrow's one-batch path is already a zero-copy
  slice (`benches/join_materialization_shape.rs`), so what remains is O(columns)
  wrapper allocation, previously measured as negligible. That finding **stops**
  this record's structural project — a multi-batch design cannot remove time that
  is not spent copying — and hands back an investigation, not a cut: a wrapper
  cost large enough to see would contradict the bench and has to be explained
  before anything is built on it.
- **Build-heavy, multi-batch.** Left side dominant: direct multi-batch build.
  The build side is read once into a hash structure, so it is the cheaper half to
  serve.
- **Probe-heavy, multi-batch.** Right side dominant: direct multi-batch probe.
  This is the larger change — the probe loop is the hot path and emits the
  selection, so it forces the addressing change through `JoinOutputRow` and the
  gather.
- **Both material.** The API change is scoped once and serves both, rather than
  twice.

### 3. Thresholds, fixed now

A side justifies the addressing change only if its **MULTI-BATCH child** carries
**≥ 500 ms** board-wide (≈7% of the parent). Not the side total: single-batch time
is wrapper overhead a multi-batch design cannot reach, so counting it toward the
gate would let the instrument argue for a project it has no evidence for.

The histogram is the second condition, and it is about ownership rather than
frequency: the cells carrying that ≥ 500 ms must show concatenations of more than
one batch actually occurring, not merely a task total of many batches spread over
many single-batch calls.

Below 500 ms on both multi-batch children, this record stops. A side that is
large but single-batch does not authorize multi-batch work and does not authorize
a copy elision either — there is no copy.

### 4. Batch-local probing is the first design, and it is priced before it is built

The candidate, in the order the engine already runs:

1. Concatenate and build the left side once, as today.
2. Iterate the right side's 4-7 batches.
3. Per batch: derive keys, probe, materialize — all with batch-local `u32` rows.
4. Carry `build_match_bitmap` across batches and emit unmatched build rows after
   the last one, for the left-preserving kinds only.
5. In `execute_hash_join_into_global_aggregate`, fold each batch immediately.

**The bench prices the whole replacement pipeline, not index arithmetic.** ADR
554's §9 recorded a forecast from a bulk-construction bench falsified in direction
by the engine (+92.0 ms measured against 190-306 ms forecast saved) precisely
because the bench measured a component rather than the path. So both arms run end
to end:

- **current**: `concat` -> keys -> probe -> gather (or fold);
- **candidate**: per-batch keys -> probe -> gather (or fold), no concat.

Held identical across arms: build table, schemas, row counts, selectivity, match
filters. Shaped like the cells that carry the time — q09, q08 and q17 scale, which
is 4-7 probe batches over 50-115 M rows and 2-7 GB.

Reported per arm: phase times, **output batch count** (the candidate returns more,
smaller batches, and the downstream consumer pays for that), and **peak memory**
(the candidate never holds the concatenated copy, which is the second reason to
want it and is currently unmeasured).

The gross removable term is the whole 6,424.8 ms — it is spent inside
`concat_batches` and a design that does not call it does not spend it, whatever
the time is made of. The load-bearing unknown is what the per-batch path costs in
its place, and that is what this bench measures. Whether the 3.85 GB/s copy rate
has a satisfying explanation is a separate curiosity and does not gate the
decision.

## Acceptance

- **The four children partition their parent exactly.** `left_single +
  left_multi + right_single + right_multi == parent` on every node, not `<=`.
  Every charged concat adds to the parent and to exactly one child from one clock
  read, so a shortfall means a site skipped its child — the only way this split
  can silently lose a side or a shape.
- **The time is split by shape, not only by side.** A single-batch concatenation
  is arrow zero-copy wrapper work; a design that consumes batches directly cannot
  remove it. An instrument that could not separate the two would let wrapper
  overhead read as evidence for an addressing change, which is the specific
  mistake this record was written to avoid.
- **Shape is reported per side, in all three quantities.** Batches, rows and
  bytes. Batches alone cannot distinguish a large copy from many small ones;
  bytes alone cannot say whether re-chunking is available. Bytes are slice sizes,
  never allocation sizes.
- **Shape is recorded per INVOCATION, not per task.** Invocation counts and a
  bounded batch-count histogram, because a task total of 400 batches is either one
  400-batch concat or 400 single-batch ones and those point at opposite projects.
  A task-aggregated total cannot tell them apart — the same aggregation trap this
  line already paid for once, recorded in section 4 of ADR 554's evidence.
- **The histogram reconciles.** Bucketed invocations plus empty ones equal the
  invocation count, so a concat cannot go unrecorded in the distribution while
  still charging time.
- **Any implemented change carries a correctness rail comparable to the slice
  equivalence check**: an opt-in diagnostic that materializes both ways and
  compares `ArrayData`, run at least once over the full cohort before the change
  is claimed. A wrong `(batch, row)` translation returns the right row count with
  the wrong rows, which no gate on this board would otherwise catch.
- **The spill decision is unchanged or shown to be unchanged.** Today
  `estimated_build_hash_bytes` is computed on concatenated key arrays;
  `estimated_build_hash_bytes_for_batches` already exists for the multi-batch
  case. A change that alters which queries spill has changed the experiment, and
  its result is not comparable to this board.
- **Peak memory is reported, not assumed.** Removing a concatenation removes a
  second full copy of the input, which is the strongest argument for the change
  and is currently unmeasured. A claim of memory benefit needs an instrument, or
  it is not made.
- **The route is recorded.** Which path a join took — concatenating or direct —
  is a task-local receipt, so a null result can be told apart from a path that
  never ran. Absence is not zero.

## Guardrails

The concatenating path remains the fallback for every shape the direct path does
not serve, and stays byte-identical while it does. This is a performance change
to a correct engine, not a rewrite.

No fixture-derived or bench-derived figure is carried into a board claim. The
board says what the board measured.

Nothing here reopens ADR 554's attribution, and no result from this record
revises it. That record is closed and frozen; this one may cite it.

The nested-children accounting rule is unchanged: children of a named remainder
subterm never join `recompute_framework_overhead`, because the parent already
left the residual.

## Risks

The measurement may find the cost uniformly spread across both sides with
multi-batch inputs everywhere, which authorizes the largest version of the change
on the thinnest per-cell evidence. If so, the first implementation is a build-side
prototype behind a route receipt, measured before the probe side is touched.

The addressing change touches the gather machinery this line just carved and
stabilized. A `(batch, row)` selection invalidates the slice fast path's
whole-source restriction as currently written, and re-deriving it per input batch
is a second design problem inside the first. That risk is a reason to prefer the
build side first, not a reason to avoid the project.

The single-batch case may dominate, in which case this record's structural
project evaporates and its remaining content is a small copy-elision cut. That is
a good outcome measured early rather than a wasted record.

## Forward

Prior evidence: `plans/impl/0554_join_output_materialization_results.md` — the
output-gather line in full, including the aggregation trap (§4) and the falsified
substitution forecast (§9). This record does not restate it and does not rewrite
it.

The first act under this record is a run of the instrument already built, on a
clean board, with the partition identity checked before any figure is read.
