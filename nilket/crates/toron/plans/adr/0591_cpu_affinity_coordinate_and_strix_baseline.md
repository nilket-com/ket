# ADR 591 - CPU affinity as a recorded coordinate item, and strix's sealed baseline

**Status:** PROPOSED. Successor to ADR 590, which closed without a sealed
board and with the finding this record acts on.


## Context

ADR 590 froze strix's configuration, ran ADR 583's board three times, and
sealed none: two boards were refused by the stability gate on DuckDB q11,
whose one high sample per board (132 ms against a 107-110 ms band) a
forty-sample diagnosis examined under three placements. The i9-14900K is
hybrid: restricted to its eight P-cores (`/sys/devices/cpu_core/cpus` =
`0-15`) the cell repeats to 2.8%; restricted to its sixteen E-cores
(`16-31`) it runs 1.78x slower, equally repeatable; unrestricted it spans
106-132 ms. That establishes substantially better repeatability under the
P-core restriction and slower execution under the E-core one. It does not
establish that the unrestricted outliers were runs that migrated to an
E-core, exclude a thermal or timer contribution to them, or show an
effect in Toron's cells (worst spread 0.078 across three boards, sums
within 0.14%); those need placement evidence that was not gathered.
Scheduling across the two core kinds is the supported hypothesis, and it
is enough to motivate a coordinate item, because a board is refused by
its least repeatable cell and the P-core restriction is the one
condition under which that cell repeated.

ADR 583 recorded its own lesson about controls that were documented but
not serialized: the DuckDB thread pin was an environment setting the rows
recorded as null until 583 made it a header and row field with a
provenance and a gate. A P-core pin applied by `taskset` from outside
would repeat that gap: a pinned board and an unpinned one would be
indistinguishable by their artifacts. slim's i9-12900K is hybrid as well
(8 P + 8 E); ADR 583's board sealed unpinned there and its cells carried
the same hazard without tripping.

Disabling the E-cores in firmware would also remove the effect, but it
changes the host for every other lane, is not visible in the artifact
either, and is less recoverable than a mask. It is not chosen.


## Decision

**The harness applies and records CPU affinity, and the board gate
requires it.** `TORON_TPCH_CPU_AFFINITY=<cpulist>` (the kernel's cpulist
syntax, e.g. `0-15` or `0-7,16-23`) is read once at harness start; the
harness normalizes it and applies it with `sched_setaffinity` on the
main thread at the start of the synchronous `main`, before the Tokio
runtime is built. On Linux affinity is a per-thread attribute; a new
thread inherits its creator's mask, and a process created by `fork` or
`clone` inherits the creating thread's mask, which is why the mask must
be in place before the runtime's worker threads exist (the harness
builds four before `run`) and before any child is spawned. The mask is
read back with `sched_getaffinity` immediately after it is applied and
must equal the normalized request exactly: Linux applies the request
intersected with the CPUs the caller may use (the online set and any
cpuset), and a request that was silently narrowed is refused before any
query runs, with both cpulists named, as is an unparseable request. The
harness records three fields in the `perf` artifact header and on every
row: `cpu_affinity_requested` (the normalized cpulist), its provenance
(`explicit` when the variable was set, `default` otherwise, in which case
the requested value is null and nothing is applied), and
`cpu_affinity_observed` (the sampling thread's mask from
`sched_getaffinity`, read again at each sample, as a cpulist). The DuckDB
row's observed value is the launching thread's mask at that sample;
DuckDB inherits it as a child by Linux's rule above, and the record says
so rather than claiming a measurement inside DuckDB.

**Candidate authority requires an explicit, honoured affinity, bound to
the header.** The gate adds to ADR 583's conditions:
`cpu_affinity_requested_provenance = explicit` in the header; and on
every row, `row.cpu_affinity_requested = header.cpu_affinity_requested =
row.cpu_affinity_observed`, else `cpu_affinity_mismatch:<engine>:<query>`
naming which of the three disagreed. Row-local equality alone would let a
row whose requested and observed fields were both changed to another
mask pass; the binding to the header is what makes the row's claim
checkable. Two
boards are comparable only with equal `cpu_affinity_requested`; a board
without the field (every board before this record, ADR 583's included) is
refused as incomparable with the item named, which leaves the ADR 583
comparison exactly where ADR 590 put it: table-to-table, host-to-host,
explanatory. The gate does not know what a P-core is; the choice of set
is the coordinate's, recorded in the results, and the gate enforces that
the set requested is the set that ran.

**strix's coordinate is ADR 590's freeze plus the P-core set.** Everything
ADR 590 froze stays as recorded (headless, `gdm` disabled, `powersave` with
EPP `performance`, turbo on, SMT on, THP `madvise`); the affinity is
`/sys/devices/cpu_core/cpus`, `0-15`, both hyperthreads of every P-core,
for the whole harness process tree, both engines. The results document
quotes the sysfs value beside the requested cpulist. SMT stays on because
ADR 590 froze it on and one change per record is the rule; whether the
sibling thread matters is a later question, if it is ever a question.

**Then ADR 583's board, run once on strix under that coordinate, sealed.**
Same recipe, same DuckDB bytes, same dataset, same budgets and pins, same
integrity gate plus the affinity conditions; the host record taken and
hashed as ADR 590 did, in a sibling directory, output directory fresh. The
sealed board is strix's canonical baseline, and the arc's next owner is
the query with the largest absolute Toron-minus-DuckDB gap on it. ADR
590's unsealed boards rank q21 first (6,865 ms) and q09 second (5,474 ms);
that is a hypothesis the sealed board confirms or replaces.

**The affinity is not a general recommendation for slim.** slim runs
nothing for this record. A future board on any hybrid host pins, because
the gate now requires it; that is the whole point of making it a
coordinate item rather than a strix habit.


## Acceptance

- A cpulist round-trips through parse and normalize (`0-15`, `0-7,16-23`,
  `3,1,2` → `1-3`); empty, malformed, and out-of-range lists are refused
  with the reason named, before any query runs.
- After the harness applies an explicit affinity on the main thread,
  the mask is observed on a Tokio worker thread of a runtime built
  afterwards and on an executed child process (`sched_getaffinity` from a
  child, or `/proc/<pid>/status` `Cpus_allowed_list`), not only on the
  calling thread; the tests use a subset of the current mask so they hold
  on any host. A request that Linux would narrow (a CPU outside the
  caller's allowed set, simulated by requesting the current mask plus a
  CPU not in it, or an offline id) is refused with both cpulists named,
  even though `sched_setaffinity` itself may succeed.
- The `perf` header and every row carry the three fields; with the
  variable unset the requested value is null, the provenance `default`,
  the observed value the inherited mask, and nothing is applied.
- The board gate refuses a header whose provenance is not `explicit`
  (`authority_condition_false:cpu_affinity_requested_provenance`); a row
  whose observed value differs from its request; and a row whose
  requested and observed values agree with each other but not with the
  header (`cpu_affinity_mismatch:<engine>:<query>` in both cases, naming
  the disagreeing field); the discriminating fixtures alter exactly one
  row each, the second altering both of its fields to another mask.
- Two artifacts with different requested affinities are refused as
  incomparable with the item named; one without the field is refused with
  the field named as absent.
- The strix board seals with final authority: 264 samples, 44 stable
  summaries, every row `full_hash` with the dataset fingerprint, DuckDB at
  one thread by staged hash, parallelism 1, affinity `0-15` requested and
  observed everywhere; archived executables, an evidence tag, a sealed
  manifest.
- The results document quotes the host record and its hashes, the
  per-cell table with both aggregates against ADR 583 labelled host-to-host
  and against ADR 590's third board labelled unsealed, and names the next
  owner by strix's absolute gap.
- Plan commit, then one implementation/results/closure commit carrying the
  harness change, its tests, the run, and the closure; the evidence tag
  names the measured tree.


## Guardrails

- The harness change is limited to reading, applying, and recording the
  mask and gating on it. No engine code changes.
- The mask is applied by the harness, never by an external `taskset`, so
  the artifact is the authority on what ran.
- Build on mini, measure on strix, nothing else on strix in the window,
  the freeze re-verified before the run, as ADR 590 prescribes.
- One board. If it is refused, the refusal is retained and reported and
  the record closes on it, as ADR 590 did; no rerun-until-sealed.


## Risks

- Toron's in-process threads, the runtime's workers, and DuckDB are all
  confined to sixteen logical CPUs. Whether any engine work would have
  used more is not established by the parallelism pin, which governs
  partition work only; the confinement is part of the coordinate and
  measured as such, and any future coordinate records its own affinity.
- The sealed board's evidence tag must reach the canonical remote from a
  host without a GitHub key: sealing on strix against a local bare
  remote, then fetching the tag to slim and pushing from there. The
  sealed manifest and archive stay on strix, which is where the arc's
  evidence now lives; the results document names the paths.


## Forward

- The sealed board names the next owner by absolute gap; attribution on
  strix precedes any cut, with ADR 584/585's q21 drivers only if q21 is
  the selection.
- Host identity in the run manifest remains forward work; the affinity
  fields in the artifact are the first host-shaped coordinate item and the
  pattern for the rest.
