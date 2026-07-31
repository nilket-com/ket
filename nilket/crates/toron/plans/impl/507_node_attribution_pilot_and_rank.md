# impl 507 — per-node attribution: pilot, extension, and the ranked target

ADR 507 steps 3–6. The step-1 contract and step-2 determination landed in `a55f791c`. This receipt
records the instrument that pilots and extends the contract across the composed-gap set, and the
ranked attribution that selects the first optimization target. No optimization mechanism ships — the
cycle ends here with an instrument and a ranking, as ADR 507 requires.

## The grounded reframe (step 3 discovery)

Tracing all seven candidates' execution paths overturned the framing. There are already **two**
fused-loop profilers (`ChainStageProfiler`, `LocalJoinPhaseAccumulator`), and the dormant one covers
none of the set. But the decisive finding is that **six of the seven cells execute primarily as a
tree of `ExecutionPlan` nodes through `execute_bound` recursion**, not as a fused loop — the
fused-loop handlers cover only sub-aggregates within the tree. The universal uncovered layer is not
per-lifecycle; it is the generic per-node executor tree.

So the step-3 question — does one abstraction span both mechanism classes? — has a clean **yes**, at a
granularity the original framing did not name: **per plan node**. A spill-heavy cell and a
compute-heavy cell differ only in *which node kind* carries the exclusive time and bytes, within one
node-kind vocabulary. The fused-loop profilers refine specific nodes beneath this layer; they do not
compete with it.

## The instrument

`crates/toron/src/exec/node_attribution.rs`: a per-node profiler wired into `execute_bound` as a
single `Drop`-guard line. It times each node **exclusively** (inclusive elapsed minus children's
inclusive, via a LIFO frame stack) and attributes spill to the node that produced it (task-telemetry
delta, children subtracted). It emits through the step-1 contract: exclusive per-node time reconciles
to the execute total with an explicit residual, and spill is a flow domain reconciling to the
whole-query counter.

Design notes that matter:
- **Drop-guard, not an async wrapper.** An early `return` inside a match arm still closes the span;
  the node dispatch is byte-for-byte untouched, so every architecture guard keyed to `execute_bound`
  (ADR 036/037/235) keeps passing. Zero cost disabled: `enter` takes no lock and samples no clock.
- **The accounting is a pure, tested core.** `NodeAccounting::exit` takes each node's measured
  inclusive span and spill and derives the exclusive figures — no clock, no telemetry — so the nesting
  arithmetic is exercised by deterministic unit tests (nested tree, non-zero root spill baseline,
  interleaving clamp), not only by end-to-end runs. The `Drop` guard supplies the measured values.
- **Attributes time and spill, not row flow.** A Drop-closed span cannot see a node's output, and the
  reconciliation needs neither.
- **The interleaving boundary is explicit.** Exact under sequential await (the single-node perf path).
  Concurrent children would interleave frames and drive an exclusive figure negative; the profiler
  clamps and counts `interleaved_frames`, and the contract's reconciliation is the backstop.

## What "reconciles" means here — two bands, one of them telescoping

Be precise about the residual, because the naive reading overstates it. The profiler measures the
**`execute_bound` root span**, which begins inside `execute_bound` and is a *subset* of the perf row's
`execute_ms`. There are two time bands:

- **in-tree**: node exclusive spans sum to the root span. This residual is near-zero *by telescoping*
  — each exclusive is `inclusive − children`, so the sum collapses to the root's inclusive span. It is
  a real check, but of **internal consistency** (no frame lost or double-counted; zero interleaving),
  not of completeness against the query. The instrument enforces it through the contract and refuses
  an inverted band.
- **outer**: root span ⊆ `execute_ms`. This is the honest, non-telescoping residual, and it is **not
  zero**. `execute_ms` is a *harness* measurement the profiler cannot see, so this band is reconciled
  reader-side, where the profile meets the perf row (`node_attribution_outer_reconciliation` in the
  perf child, folded into the row beside the profile). It has a numeric residual, a **declared 10 ms
  boundary** — `execute_ms` starts immediately before `execute_bound` and stops immediately after, so
  this residual is *not* binding (binding completes before the timer starts); it is executor
  construction — building and entering the boxed future before the root span opens — plus timer-edge
  framework at both ends, which is fixed and sub-millisecond, so a
  residual over 10 ms means the node tree failed to account for the query — and a pass result.

The **byte axis is enforced, not observed**: `write_profile` reconciles per-node exclusive spill (a
flow domain) against the root spill delta through the contract and emits `unattributed_bytes`; boundary
zero, since integer exclusive spills telescope to the delta exactly. A mismatch would fail the profile.

Both readers **fail closed**. The outer reconciliation refuses to publish a pass it cannot justify: an
absent or non-numeric span, a non-finite or negative `execute_ms`, or an inverted band (span exceeding
`execute_ms` — physically impossible, so a broken measurement) each report `within_boundary: false`
with a reason and no residual. The subtraction is checked, never saturating: saturating would turn an
inverted band into a zero residual and a silent pass. The harness also **clears the profile path before
each run**, matching the adjacent diagnostic ledgers — the profiler's write is best-effort by design
(its errors are swallowed so a diagnostic failure cannot mask a query result), so without the removal a
run that failed to emit would attach the previous run's profile, and a stale same-query profile would
reconcile inside the boundary as a false pass.

## Pilot and extension (steps 3–5): the contract holds on every cell

One final instrument, one vocabulary, run across all seven cells at strict sf=8, 2 GiB budgets, with
both reconciliations enforced and the outer band joined from the perf row. Every cell: in-tree residual
0 (telescoping consistency), byte `unattributed_bytes` 0 (enforced), zero interleaved frames, and an
outer residual of 88–128 μs — all within the 10 ms boundary by a factor of ~80–130×. The mechanism
classes are not two vocabularies; they are one vocabulary with the weight on different nodes.

| cell | execute ms | in-tree resid | outer resid | within 10 ms | spill unattr | interleaved | dominant node(s) |
|---|---:|---:|---:|:--:|---:|---:|---|
| q02 | 2,675 | 0 | 110 μs | yes | 0 | 0 | sort 41%, join 31% |
| q08 | 7,982 | 0 | 111 μs | yes | 0 | 0 | aggregate 96% |
| q09 | 13,032 | 0 | 110 μs | yes | 0 | 0 | aggregate 95% |
| q17 | 35,693 | 0 | 128 μs | yes | 0 | 0 | aggregate 100% (623 MB spill) |
| q18 | 19,430 | 0 | 88 μs | yes | 0 | 0 | join 65%, exchange 17% |
| q20 | 9,476 | 0 | 101 μs | yes | 0 | 0 | aggregate 61%, join 33% |
| q21 | 17,935 | 0 | 98 μs | yes | 0 | 0 | aggregate 100% |

All seven from the same final enforced binary (`node_attribution` + `node_attribution_outer_reconciliation`
folded into each perf row). The outer residual is near-constant (~100 μs) regardless of cell size —
fixed framework overhead, not a share of the query — so the node tree accounts for `execute_ms` to
within ~100 μs on a 2.7 s cell and a 35.7 s cell alike.

## Overhead (step 4): within noise

Same-binary profiler on/off pairs at sf=8. q20: on 9,398 ms vs off 9,518 ms. q17: on 34,898 ms vs off
36,003 ms. In both the profiled run is *faster* than the unprofiled one — the overhead is below
run-to-run variance, i.e. not measurable. The boundaries declared before measuring — in-tree time
residual 0 and byte `unattributed_bytes` 0 — are met exactly, so no tolerance is being spent on those;
the outer time residual (~100–170 μs) is recorded as its own quantity, not folded into a zero claim.

## The ranked target (step 6): the aggregate operator, not scan encoding

Exclusive time summed by node kind across the seven cells:

| node kind | exclusive time (approx) | share |
|---|---:|---:|
| **aggregate** | **~81 s** | **~76%** |
| join | ~16 s | ~15% |
| exchange | ~5 s | ~5% |
| sort | ~1 s | ~1% |
| scan / filter / other | ~2 s | ~2% |

**The composed gap concentrates in the aggregate operator** — roughly 5× the join share, and an order
of magnitude above scan. Five of seven cells (q08, q09, q17, q20, q21) are aggregate-dominated; q18 is
join-dominated and q02 is sort/join-dominated.

This selects the first optimization target from attribution, not gap size, exactly as ADR 507 step 6
requires — and it is a genuinely different target than the campaign's prior lean. The encoded-column
hypothesis targeted the scan; scan is ~1% of the composed gap. The aggregate operator — the grouped
and global fold, and for the spill-heavy cells the spilling replay machinery inside it — is where the
time is. Two layers compose here: per-node attribution says *aggregate*; the existing fused-loop
profiler refines *which part* of the aggregate (for q21, 76% is `inner_replay_1`).

## Forward

The next ADR is the aggregate-operator optimization cycle, scoped by this attribution: a
compute-bound grouped/global fold path (q08/q09/q20, zero spill) and a spill-bound replay path
(q17/q21, hundreds of MB in one node). It opens against measured shares, not a presumed mechanism.
Encoded columns remain a candidate only where scan share is shown to matter, which this board says is
nowhere in the current set. The join/sort-dominated cells (q18, q02) are a distinct, smaller second
front. The sf=8 board stays council grounding, not a labelled baseline; a clean re-pin precedes any
movement claim.
