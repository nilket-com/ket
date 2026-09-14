# ADR 588 - q21 existence-summary insert: one walk per miss

**Status:** PROPOSED. Successor to ADR 587. The first behaviour-changing cut
of the q21 arc since ADR 580: on a miss, the existence summary reuses the
bucket its first probe already found instead of hashing and walking a
second time, unless a growth operation ran in between, in which case it
re-probes. Accepted only on digest parity and a paired net execution gain
at the sealed coordinate; otherwise reverted and closed. The order-aware
build stays a separate candidate and is not part of this record.


## Context

ADR 587 resolved the row loop outside growth as the owner of q21's semi
insert: 1,122.8 median-scaled ms, 8.75% of a 12,827.1 ms unprofiled execute
median, with 907.6 ms beside it on the anti alias. Its counters settled
what the loop does: on the semi, 12,004,370 lookups met an existing key
4,370 times and appended 12,000,000 times; the sampled classes put 93% of
the row loop's CPU in hash-and-probe in every pair of three runs. Growth
closed below the floor at 450.5 ms gross. The two candidates ADR 587 named
for the row loop were the second walk on a miss and an order-aware build;
this record takes the first and leaves the second where it is.

The second walk is a fact of the code, not a hypothesis. `insert` first
calls `lookup_entry_index`, which hashes the key and walks the bucket array
from the home bucket until it meets an empty bucket or a matching entry,
and returns the entry if there was one. On a miss it returns `None` and
discards the bucket the walk stopped at. The append path then, after
growing the table if the entry array is full, calls `find_bucket`, which
hashes the same key and walks the same array to the same empty bucket, and
writes the new entry's index there. Every one of the 12,000,000 appends on
the semi and 11,002,698 on the anti does this; ADR 587's `probe_steps`
counted both walks, 17.9 M extra steps over 24.0 M walks on the semi.

What the second walk costs is not known and the plan says so. The first
walk is the one that takes the cache miss on a random bucket of a 134 MB
array; the second walk, microseconds later, hashes again (a few
nanoseconds) and reads the same bucket, which is now in the first-level
cache, plus whatever chain the first walk read. Its cost may be a small
fraction of the row loop, and ADR 587's sampled classes cannot separate the
two walks because they share every source line. ADR 587 bounded the
candidate by count, roughly half the walks; by cost it is bounded only by
the row loop itself, and the honest expectation is a gain well under that.
This is why the record is written as a measurement with a cut inside it
rather than as an optimisation: the cut opens only if the paired
measurement establishes the gain, and it is reverted if it does not.

One invariant governs the cut. A growth operation reallocates the entry
array, copies it, and rebuilds the bucket array at twice the size with
every entry rehashed; a bucket index found before growth names a slot in
an array that no longer exists. The reuse is therefore correct only when no
growth ran between the first walk and the append, and after growth the key
must be probed again in the new array. The first insert into an empty
summary is the degenerate case: the lookup returns `None` without walking
because there are no buckets, growth allocates the first 1,024 entries,
and the probe that follows is the first walk, not a second.


## Decision

**The cut.** `lookup_entry_index` becomes a probe that returns what the
walk found: the bucket it stopped at, and the entry there if any. On a hit
nothing changes: the value is observed, the last-key cache is set. On a
miss the append path checks whether the entry array is full; if it is, it
grows and then probes the new bucket array afresh; if it is not, it writes
the new entry's index into the bucket the first probe returned. The
uninstrumented and instrumented monomorphizations of the loop both take
the cut; ADR 587's counters keep their meaning, with `probe_steps` now
counting the single walk on an ordinary miss and the re-probe only after
growth, and ADR 587's `appended`, `lookup_found`, and `cache_hits` unchanged
in value. No other part of the summary, its growth policy, its hashing, the
repartition, or any route changes. The order-aware build is not touched
and not measured here.

**The invariant, tested at the summary.** Four paths, each with a test
that would fail if the cut were wrong. Reuse: on a miss with room in the
table, exactly one walk runs, the entry lands in the bucket that walk
returned, and `find_bucket` afterwards returns that same bucket. Growth,
made discriminating: the test fills the table to its limit, probes the
next key against the pre-growth array and records the bucket that probe
returns, then chooses that key so that its bucket in the doubled array
differs from the recorded one (searching keys until the two differ, which
the doubled mask guarantees for some key), inserts it so that growth runs
mid-batch with the stale index in hand, and asserts three things: the
entry sits at the post-growth bucket, that bucket is not the recorded
pre-growth one, and `find_bucket` on the new array returns it. Merely
forcing growth would let a stale-index reuse pass by coincidence; the
asserted difference is what makes the test fail if the index were reused.
Empty table, separately: the first insert into a summary with no buckets
returns from the lookup without walking, grows, and then performs exactly
one actual walk, the probe of the fresh array; the walk counter reads one,
not two, and the entry is where `find_bucket` says. Existing key: a key
already present is found on the first walk, observed, not appended, and
still found after a later growth. A `cfg(test)` walk counter on the summary
pins one walk for an ordinary miss, two for a growing miss on a non-empty
table, one for the empty-table insert, and one for a hit. The existing
equivalence tests that probe a recognized summary against its residual twin
pass untouched, since they exercise the same build.

**Value gate: digest parity.** Every treatment row's `digest` must equal
every control row's, and both must equal the sealed ADR 583 board's q21
Toron digest, pinned in full in the driver and compared byte for byte:

```
ArrowIpcDigest([253, 211, 102, 221, 152, 120, 53, 233, 227, 131, 103, 248,
                201, 39, 69, 110, 137, 182, 55, 137, 86, 129, 82, 251, 87,
                153, 11, 247, 152, 140, 107, 240])
```

which ADR 586's and ADR 587's rows also carried. A mismatch on any row ends
the record with the cut reverted; there is no partial parity.

**Net gain: paired executables, not a runtime switch.** The measurement is
two executables at the sealed ADR 583 coordinate, both built and staged
through `toron-bench build` on the `release` profile, the sealed recipe:
the control from the tree without the cut, the treatment from the tree
with it and nothing else. The driver, `toron-tpch adr588-q21-second-walk`,
runs under the treatment's manifest and takes `--control-manifest`; it
verifies both staged executables against their manifests before any child
runs, spawns control children with the control's executable and manifest
in their environment, and stamps every row with the identity it ran under
and the manifest path it was launched with.

**Cross-executable identity gate.** Two executables are comparable under
this record only when everything but the cut is the same, and the driver
checks that rather than assuming it from adjacency. Both manifests must
carry the same build recipe hash (profile, features, target, rustc,
rustflags, and `Cargo.lock` all agree; a lock that moved between the trees
is a refusal, not a footnote), the same dataset manifest fingerprint, and
the same measurement settings on every row: memory per task, spill
reserve, partition parallelism, mode, scale factor, data source, and
`diagnostics_disabled = true`. The only fields allowed to differ are the
source tree id and the executable and harness hashes, and they must differ
in the expected direction: exactly two distinct executable hashes across
the rows, each matching its manifest, and the treatment tree must be the
control tree plus the cut alone, which the record shows by citing the two
commits and the diff between them. Every row reports the manifest path it
was launched with, and the evaluator refuses a control row that names the
treatment manifest or the reverse. Phase 0 (one unprofiled warmup and one sample on the control,
tripwire at 10% from ADR 586's 13,128.5 ms), one unprofiled warmup on each
executable, then five adjacent alternating pairs, control-treatment,
treatment-control, and so on, both arms unprofiled: no node profile, no
sampler, `diagnostics_disabled = true` on every row. q21 alone, strict SF=8,
persisted Parquet, explicit 2 GiB task memory and spill reserve, partition
parallelism 1, empty dedicated spill scratch, full-hash dataset
verification, the deployment stopped for the run and restarted after it, a
fresh output directory. There is no runtime switch between the two paths:
the treatment is exactly the code that would ship, and the control is
exactly the code that shipped, so the engine carries no dead path after the
record closes either way.

**Acceptance rule, stated before the run.** For pair i, `gain_i =
execute_control_i - execute_treatment_i` in milliseconds, and `range_control
= max(execute_control) - min(execute_control)`. The cut is **retained** only
if the median of `gain_i` over five pairs exceeds `2 * range_control` and
every `gain_i` is strictly positive; a pair with zero gain fails the rule
exactly as a negative one does. A median gain inside twice the control
range, or any pair where the treatment was not faster, is a gain the
measurement cannot distinguish from its own noise; the record then reports it as **not
established**, reverts the cut in the same commit, and closes the
candidate. There is no floor other than the noise: a small but established
gain is retained, a large but unestablished one is not. The rule is fixed
here and is not revisited after the numbers are in. The record also
reports the control median beside ADR 586's 13,128.5 ms `release` off
median as drift, never netted, and the treatment median beside the control.

**Mechanism check with the existing instrument, refreshed for the
treatment tree, reported and not gating.** ADR 587's source-line classes
are constants pinned to `streaming.rs` by anchor tests; the cut moves and
changes lines inside `insert_with` and `lookup_entry_index_with`, so the
classes are re-pinned to the treatment tree before the check runs, with
the anchor test passing on it and the new ranges recorded in the receipt
beside the old. ADR 587's driver is then run once on a `profiling` build
of the treatment tree at the same coordinate, with its own phase 0 against
ADR 587's 12,827.1 ms. Its walk denominator is redefined for the single
walk and stated in the receipt. `lookups` keeps its meaning: every row
that was not a cache hit, counted before the lookup runs, so it includes
the one attempt per summary against an empty table that returns without
walking. A new `empty_table_lookups` counter records those attempts, and
`growth_reprobes` counts the probes of a fresh array after growth,
including the first probe after the initial allocation. Then

```
walks = lookups - empty_table_lookups + growth_reprobes
```

counts actual walks: the found rows' single walk, the ordinary misses'
single walk, and the post-growth re-probes. The formula is pinned by the
`cfg(test)` walk counter, which counts real walks independently: on every
test fixture the two must agree exactly. `probe_steps` is reported per
walk against that denominator, and ADR 587's 0.74 per walk is restated
against ADR 587's own exact denominator, `2 * appended + lookup_found -
empty_table_lookups`, beside it; the subtraction leaves the rounded 0.74
unchanged, and the two ratios are not compared as one. The check reports
`semi_insert_rows_excluding_grow` against 1,122.8 ms, the sampled
hash-and-probe share against 93%, and `appended`, `lookup_found`, and
`cache_hits` unchanged in value. It is a check that the cut did what it
says, not a second measurement of the gain; the gain is the paired
`release` run's, and the two are reported side by side without netting.

**What this record does not do.** It does not change the hash, the load
factor, the growth policy, the recognizer's hint, the repartition, or the
build's dependence on source order; the order-aware build stays a separate
candidate with its own record if it opens. It does not measure the gain on
the `profiling` build or with any diagnostic on. It does not retain a cut
whose gain is not established, however plausible.


## Acceptance

- The cut lands first with its tests: reuse (one walk, entry in the
  returned bucket, `find_bucket` agrees); growth mid-batch with a key whose
  pre-growth and post-growth buckets are asserted to differ (stale index
  unused, entry at the post-growth bucket, `find_bucket` agrees); the
  empty-table first insert (one actual walk after growth); existing key
  (found, observed, not appended, still found after growth); the walk
  counter pinning one, two, one, and one; ADR 587's counter tests updated
  for the single walk with their hand-computed values re-derived; ADR
  587's line classes re-pinned to the treatment tree with the anchor test
  passing; the summary equivalence tests unchanged and passing.
- The driver verifies both staged executables against their manifests,
  applies the identity gate (same recipe hash, same dataset fingerprint,
  same measurement settings on every row, exactly two executable hashes
  each matching its manifest, every row naming the manifest it ran under),
  requires `diagnostics_disabled = true` on every row, refuses any digest
  that differs byte for byte from the pinned sealed q21 digest, applies
  the phase-0 tripwire, and applies the acceptance rule as written with
  both outcomes tested, including a positive median with one negative
  pair, a positive median with one zero pair, and a median inside twice
  the control range.
- One control executable and one treatment executable, each staged once
  on the `release` recipe, both hashes reported beside `b436a843`,
  `03daafed`, and `3f3a5e2c`; the control median reported against 13,128.5
  ms as drift.
- Per-pair table of control, treatment, gain, and digest; medians, ranges,
  `2 * range_control`, and the verdict in one sentence.
- The mechanism check's receipt reported beside ADR 587's terms and
  counts with the walk denominator `lookups - empty_table_lookups +
  growth_reprobes` stated, pinned against the test walk counter, and the
  two per-walk ratios kept apart, in words, without netting into the gain.
- The verdict's consequence in the same commit: the cut retained, or the
  cut reverted with the record closed as not established; the arc adds no
  stamp-only commit.
- Results cite both source tree ids and the diff between them, both
  executable hashes, the shared recipe, the dataset manifest, and the
  artifact directory.


## Guardrails

- The bucket from a probe is reused only when no growth ran after it; any
  growth forces a re-probe. This is the invariant, and it is tested at the
  summary, not inferred from the perf rows.
- No runtime switch, feature, or environment variable selects the old
  path; control and treatment are two executables.
- Digest parity on every row is a precondition of any verdict, not a
  finding beside it.
- The gain is read from unprofiled `release` arms only; the `profiling`
  mechanism check never enters it.
- A gain that does not clear twice the control range in the median, or is
  zero or negative in any pair, reverts the cut. Plausibility is not
  evidence, and the rule is not revised after the run.
- Two executables compare only under the identity gate: one recipe, one
  dataset, one set of measurement settings, two trees differing by the cut
  alone.
- The local deployment stays running until the measurement and is stopped
  only for it. Partition parallelism stays 1 for every child.
- `b436a843`, `03daafed`, and `3f3a5e2c` are measured by nothing under
  this record.


## Risks

- The second walk may be nearly free, since its bucket is cache-hot: the
  gain may then be tens of milliseconds and inside the noise, and the
  record closes the candidate with the cut reverted. That is a result, and
  it hands the row loop to the order-aware build.
- The control range at this coordinate has been 172–733 ms across the last
  four runs, so the bar for an established gain is host-dependent; a run
  on an unquiet host raises it, and phase 0 does not see load that begins
  after it. The deployment stays up until the run, and the run waits for
  a quiet host, as ADR 587's third run did.
- A wrong reuse would corrupt the table silently on a growth boundary;
  the mid-batch growth test and digest parity are the two independent
  checks against it, one at the summary and one at the query result.
- Two executables from two trees differ by the cut alone only if nothing
  else moves between them; adjacency and different hashes do not establish
  that, so the identity gate checks the recipe and settings and the record
  cites the diff between the two commits.
- The re-pinned line classes may mis-bin a moved line if the anchors are
  set carelessly; the anchor test on the treatment tree is what catches
  it, and the receipt records the ranges used.


## Forward

If retained, q21's row loop is shorter by the established gain and the
next cut on this path is the order-aware build, which the counts already
justify examining (12,000,000 appends, 4,370 found) and which would
subsume the single walk; the repartition candidate from ADR 586 is
governed by the same order question. If not established, the candidate
closes and the order-aware build is the successor directly. Either way,
`cleanup` at 567.3 ms and `runtime_prep_inner_1` at 1,654.0 ms stay open
as their own candidates, and the ADR 587 instrument stays available for
pricing whichever cut comes next.
