# ADR 588 - q21 existence-summary insert, one walk per miss: results

Measured 2026-09-14 at the sealed ADR 583 coordinate as two `release`
executables on one recipe: the control from the tree with this record's
driver and counters, the treatment from that tree plus the cut alone. Every
gate passed. **All five observed gains were positive, 131 to 295 ms, median
182.2 ms, 1.41% of the control median; the predeclared rule required the
median to exceed twice the control range, 353.0 ms, and it did not, so no
improvement is established.** The cut is
reverted in the closing commit and the candidate is closed. The mechanism
check on a `profiling` build of the treatment tree confirms the cut did
what it claimed: the probe-step count halved, the walk denominator matched
the counters exactly, and the row loop shortened by the same ~1.4% the
release pairs showed. No engine behaviour changes remain in the tree.

## Identity and coordinate

| identity | control | treatment |
|---|---|---|
| source tree | `e0cc2ff6827f19c58e98e8c403698ef11afcc6a9` | `3d6709a1cba156d836be229cf08de38a0cfc820e` |
| commit | `aa6e40dd` (driver and counters) | `b0b5b0d2` (the cut, its tests, re-pinned line classes) |
| executable | `d2f90eeb74e2e1e4713fbf17991f1fc153d3792dd073f0f3a843b3db3ae43d00` | `f64f4577077037a45f7b06ee8cec3f1444743928fa37a995bcf5c2ecc27fb823` |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` (release) | same |
| dataset manifest | `fnv1a64:0fc5c06d19c5ed23` | same |

The two trees differ by `git diff aa6e40dd b0b5b0d2` alone: the probe that
returns its bucket, the miss path that reuses it unless growth ran, the
four invariant tests, the walk-counter pins, `find_bucket` scoped to tests,
and the ADR 587 line classes re-pinned to the moved lines (263 insertions,
83 deletions across `streaming.rs` and `main.rs`). The recipe hash agrees
on profile, features, target, rustc, rustflags, and `Cargo.lock`. Earlier
executables `b436a843`, `03daafed`, and `3f3a5e2c` ran under nothing here.

Coordinate: q21 alone, strict SF=8 over persisted Parquet, explicit 2 GiB
task memory and spill reserve, partition parallelism 1, full-hash dataset
verification, an empty dedicated spill scratch (`target/tpch/adr588-join-spill`,
empty after every run), the local deployment stopped for each run and
restarted after it, both arms unprofiled with `diagnostics_disabled = true`
on every row. Artifacts: `target/tpch/adr588-q21-final/` (paired run),
`target/tpch/adr588-q21-mechanism-check/` (ADR 587 driver on the treatment),
and `target/tpch/adr588-q21-rejected-identity/` (a rejected rehearsal, below).
All unsealed, no candidate authority.

## Digest parity

Every one of the fourteen journaled rows (two phase-0 rows on the control,
one warmup per executable, ten pair rows), control and treatment alike,
carried the sealed ADR 583 q21 Toron digest byte for byte:

```
ArrowIpcDigest([253, 211, 102, 221, 152, 120, 53, 233, 227, 131, 103, 248,
                201, 39, 69, 110, 137, 182, 55, 137, 86, 129, 82, 251, 87,
                153, 11, 247, 152, 140, 107, 240])
```

## Phase 0 and the pairs

Phase 0 on the control: warmup 12,939.7 ms, sample 12,964.1 ms, −1.25% from
ADR 586's 13,128.5 ms reference, not tripped. One unprofiled warmup on each
executable (control 13,044.6, treatment 13,030.5 ms), then five adjacent
pairs alternating control-treatment, treatment-control, and so on, on a
quiet host (load 0.58, package 47 °C at start).

| pair | order | control ms | treatment ms | gain ms | gain |
|---:|:---:|---:|---:|---:|---:|
| 0 | C, T | 13,003.1 | 12,820.9 | +182.2 | +1.40% |
| 1 | T, C | 12,921.2 | 12,790.2 | +131.0 | +1.01% |
| 2 | C, T | 12,967.4 | 12,816.8 | +150.6 | +1.16% |
| 3 | T, C | 12,955.6 | 12,766.8 | +188.8 | +1.46% |
| 4 | C, T | 13,097.7 | 12,802.5 | +295.2 | +2.25% |

Control median **12,967.4 ms**, range 176.5 ms; treatment median 12,802.5
ms, range 54.1 ms. Median gain **182.2 ms**, 1.41% of the control median;
every pair strictly positive.

## The rule, applied as written

Retained only if the median gain exceeds `2 × range_control` and every pair
is strictly positive. Required: 353.0 ms. Median: 182.2 ms. **Not
established**: the second condition holds and the first does not. The rule
was fixed in the record before the run and is not revisited on seeing that
all five observed gains were positive; five positive pairs at half the
required size is exactly the case the rule was written to refuse, and the
record's own expectation was that the second walk, re-reading a cache-hot
bucket, might cost little. The consequence is the cut reverted in the
closing commit and the candidate closed.

The control median is 161.1 ms (1.23%) faster than ADR 586's `release` off
median of 13,128.5 ms on a recipe whose lock differs by `toron`'s `libc`
dependency; reported as drift, never netted.

## The rejected rehearsal, and what it measured by accident

The first paired run was refused by the row identity gate: every control
row reported `evidence_identity: invalid` because the child had been
launched from the treatment executable with the control manifest in its
environment. The driver had resolved the executable from the harness's own
environment, which `toron-bench run` sets to the treatment, and the
per-child override reached only the child's identity check. The runner now
spawns the arm's executable explicitly and refuses a child whose reported
hash is not its arm's; the fix is in the driver commit, and both
executables were rebuilt from the corrected trees.

The refused run is retained because it happens to be an A/A: five pairs of
the treatment executable against itself, on the same coordinate and host.
Its "gains" were −12.9, −59.9, +223.0, +9.3, and +62.7 ms, median +9.3 ms,
against a control range of 189.6 ms. It is an illustration of this
coordinate's pair-to-pair variability with no treatment present, and
nothing more: it was not part of the plan, it did not calibrate the rule,
which was declared before either run, and it carries no authority over
the verdict. It is reported because a single A/A pair moving 223 ms is a
useful picture of why a rule keyed to the control range refuses a 182 ms
median, not because the verdict rests on it.

## Mechanism check on the treatment tree, reported and not gating

ADR 587's driver ran once on a `profiling` build of the treatment tree
(executable `865e447c…`, tree `3d6709a1`, recipe `a1fad89d…` as ADR 587's),
with its line classes re-pinned to the moved lines (anchor test passing;
ranges recorded in each pair's bins), phase 0 at 12,586.9 ms against ADR
587's 12,827.1 (−1.87%, not tripped), perturbation 1.96%, all profiles and
residuals complete, zero lost samples. The cut did what it claimed:

| quantity | ADR 587 (two walks) | treatment (one walk) |
|---|---:|---:|
| `lookups` / `lookup_found` / `appended` (semi) | 12,004,370 / 4,370 / 12,000,000 | identical |
| `empty_table_lookups` / `growth_reprobes` | 1 / 15 (control semantics) | 1 / 15 |
| `probe_steps` (semi, pair 0) | 17,864,353 | 8,932,745 |
| walk denominator | `2·appended + found − empty` = 24,004,369 | `lookups − empty + reprobes` = 12,004,384 |
| `probe_steps` per walk | 0.744 | 0.744 |
| `semi_insert_rows_excluding_grow` | 1,122.8 ms (8.75%) | 1,056.2 ms (8.32%) |
| `semi_insert_grow` | 450.5 ms | 457.2 ms |
| `anti_insert_rows_excluding_grow` | 907.6 ms | 826.8 ms |
| rows-loop `hash_and_probe` share (semi) | 93.4% | 92.8% |
| estimated coverage (semi) | 0.894 | 0.888 |

The walk denominator equals the counters' arithmetic exactly and the
per-walk step ratio is unchanged at 0.744 under either denominator, which is
what one expects when the second walk repeated the first's steps: halving
both the steps and the walks leaves the ratio. Growth is untouched, as the
cut promised. The row loop shortened by 66.6 ms scaled on the semi and
80.8 ms on the anti, 147 ms together, against the release pairs' 182 ms
median gain; the two are measured on different profiles and different
runs and are reported beside each other, not netted. The three-term owner
rule still names `semi_insert_rows_excluding_grow` (1,056.2 ms against a
500 ms floor); the row loop remains the owner after the cut, which is the
other reason the record does not fight the rule: the second walk was a
small part of it.

## Implementation and verification

Two driver checks were tightened under review after the runs, in the
closing commit: parity, coordinate, identity, launch, and dataset
validation now run through one validator on every child the moment it
returns, phase 0 and warmups included, so a failing early row stops the
run and is journaled as the record promised; and the identity gate now
binds each row's launched manifest path to its arm's, and each row's
child-reported dataset (`dataset_verification`, `dataset_manifest_fingerprint`,
`data_source`, the latter two added to the `perf-run-one` row for this) to
the dataset the driver verified. Tests cover a warmup row failing parity,
a swapped manifest, a swapped executable, a wrong identity, a wrong
fingerprint, an unverified dataset, a wrong data source, diagnostics on,
and a wrong coordinate, at the validator and through the evaluator. The
fourteen retained rows all pass the tightened parity, coordinate, identity,
launch-path, and `dataset_verification = full_hash` checks; they predate
the two added child fields and so would be refused by the tightened
evaluator for their absence, not for a mismatch. The driver verified the
dataset (`fnv1a64:0fc5c06d19c5ed23`, full hash) before any child ran and
passed the same `--data-source parquet` to every child; the verdict was
computed by the evaluator as it stood and stands as recorded. No run was
repeated for these fixes.

Driver commit `aa6e40dd`: `empty_table_lookups` and `growth_reprobes`
counters with a test-only walk counter pinning the walk formula; ADR 587's
driver reports both walk denominators per summary; the ADR 588 driver
loads and verifies both manifests, applies the manifest gate (one recipe,
one profile, two trees, two executables, no untracked inputs) and the row
gate (every setting equal, `diagnostics_disabled`, byte-exact digest, each
row's identity equal to its arm's manifest and its launch), runs phase 0
on the control, warmups, five alternating pairs, and the predeclared rule
with zero-gain pairs failing. Tests: the manifest gate's five refusals; the
retained case, a negative pair, a zero pair, and a median inside twice the
range; digest, identity, launch, setting, and diagnostics refusals; the two
walk denominators kept apart on a fixture.

Cut commit `b0b5b0d2`: `probe_with` returns the bucket and the entry; the
miss path reuses the bucket unless growth ran and re-probes after growth;
four invariant tests (reuse with one walk and `find_bucket` agreeing; a
growing miss with a key whose pre- and post-growth buckets are asserted to
differ, landing at the post-growth bucket and never the stale one; the
empty-table first insert walking once after its allocation; an existing
key found on one walk, observed, not appended, and found again after
growth); walk counter pins 1, 2, 1, 1; ADR 587's counter tests re-derived
for the single walk.

Closing commit: `git revert b0b5b0d2`, this record, and the ADR's status.
Both measured trees stay in history and are cited above.

Ladders on both measured trees (`RUST_MIN_STACK=67108864`): fmt clean;
control tree Toron library 1,784, treatment 1,788; architecture 2,151;
integration 3,649; toron-tpch 586 passed, 6 ignored on each; 587 on the closing tree.
