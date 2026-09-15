# ADR 589 - phase 2: the order-aware build, results

Measured 2026-09-14 at the sealed ADR 583 coordinate as two `release`
executables on one recipe: the control from the tree with phase 1's census,
the finish accounting, the admission gate, and the paired driver; the
treatment from that tree plus the order-aware build alone. Every gate
passed. **Retained: every pair was faster with the cut, by 1,684.6 to
1,819.0 ms, median 1,753.4 ms, 13.7% of the control median, against a
required 172.5 ms; every row carried the sealed q21 digest byte for byte.**
That result belongs to the tree it was measured on, `737c54cd…`. Two
review rounds then tightened the memory and index-width contracts, so the
closing tree `8d752ee0…` was built and measured again on 2026-09-15
against the same recorded control under the same gates, and is retained
in its own right: median gain 1,733.0 ms, 13.9% of its control median,
against a required 372.8 ms, every pair positive, digest parity on every
row (section "The closing tree, measured").
The mechanism check on a `profiling` build of the treatment shows the cut
did exactly what it claimed: twelve million fast-path appends with zero
probe steps and zero growth events, every recurrence found in its
partition's sorted list, and the once-built index costing 302 ms in the
finish. q21's runtime-prep owner is now `runtime_prep_inner_1`.

## Identity and coordinate

| identity | control | treatment |
|---|---|---|
| source tree | `1956335572f751a72f7e4389aae298e223557d37` | `737c54cdce76969bdeded9b568d6f5a5565c372c` |
| commit | `8eecba1d` (finish accounting, admission gate, paired-driver plumbing) | `15d889df` (the cut, its tests, re-pinned line classes) |
| executable | `f1991c91a6c3ae3a6315fbdb0184d1cec511ddc1163e29e674bb27e2da6438ba` | `f64e1de44546bd23661add2d4218791781ec54294482976047f590b524495b75` |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` (release) | same |
| dataset manifest | `fnv1a64:0fc5c06d19c5ed23` | same |

The closing tree, measured separately against the same control:

| identity | closing treatment |
|---|---|
| source tree | `8d752ee08625e1357bb6b3c3fb2e5de07db09ef6` |
| commit | `4cc5d651` (the cut with both review rounds folded, before this measurement was folded into the record; the closing commit differs from it in this file and the ADR's status line alone) |
| executable | `285641fd5c704ed69d857f64810c9da4711045b8c8cb87517c4141f222492dec` |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` (release), same |
| dataset manifest | `fnv1a64:0fc5c06d19c5ed23`, same |

The two trees differ by `git diff 8eecba1d 15d889df` alone: the build
state and the ordered row path, the once-built index, the guarded
reservations, the fifteen summary and writer tests, and ADR 587's line
classes re-pinned with `ordered` and `finish` classes (1,151 insertions, 57
deletions across `streaming.rs` and `main.rs`). The recipe hash agrees on
profile, features, target, rustc, rustflags, and `Cargo.lock`, and is the
recipe ADR 588's arms were built on. Earlier executables ran under nothing
here.

Coordinate: q21 alone, strict SF=8 over persisted Parquet, explicit 2 GiB
task memory and spill reserve, partition parallelism 1, full-hash dataset
verification, empty dedicated spill scratch (`target/tpch/adr589-join-spill`,
empty after every run), the deployment stopped for each run and restarted
after it, both arms unprofiled with `diagnostics_disabled = true` on every
row, on a quiet host (load 0.33, package 47 °C at start). Artifacts:
`target/tpch/adr589-q21-final/` (paired run on `737c54cd…`),
`target/tpch/adr589-q21-final-closing/` (paired run on the closing tree
`8d752ee0…`), `target/tpch/adr589-q21-mechanism-check/` (ADR 587's driver
on the `737c54cd…` treatment), and the retained rehearsals
`target/tpch/adr589-q21-rejected-dataset-binding/` and
`target/tpch/adr589-q21-mechanism-check-tripped-hashed-reference/`. All
unsealed, no candidate authority.

## Digest parity and row validation

Every one of the fourteen journaled rows (two phase-0 rows on the control,
one warmup per executable, ten pair rows) carried the sealed ADR 583 q21
Toron digest byte for byte, reported `dataset_verification = full_hash`,
`dataset_manifest_fingerprint = fnv1a64:0fc5c06d19c5ed23`, and `data_source
= parquet`, and reported the identity, manifest path, and executable of
its own arm, checked by the one validator on every child as it returned.

## The rejected rehearsal

The first paired run was refused by that validator on its very first
child: the control's phase-0 warmup reported `fnv1a64:57c8f4f5…` where the
driver had verified `fnv1a64:0fc5c06d…`. The plumbing that ADR 588's review
had added to the `perf-run-one` row wired the wrong field: the persisted
query spec's `export.manifest_fingerprint` is a per-query derivation
(`query=…;dataset=…;tables=…`), not the dataset's manifest fingerprint. The
spec now carries the dataset's own fingerprint and the row reports it; the
fix belongs to the harness and rides the plumbing commit, so both
executables were rebuilt from the corrected trees (the superseded
manifests are retained as `adr589-*-rejected-dataset-binding.manifest`).
The validator did its job: a binding that could not have been checked
before was checked, and the run stopped on the first row.

## Phase 0 and the pairs

Phase 0 on the control: warmup 13,648.1 ms, sample 13,462.5 ms, +3.82% from
ADR 588's control median 12,967.4 ms, not tripped. One unprofiled warmup on
each executable (control 13,500.7, treatment 11,464.4 ms), then five
adjacent pairs alternating control-treatment, treatment-control, and so on.

| pair | order | control ms | treatment ms | gain ms | gain |
|---:|:---:|---:|---:|---:|---:|
| 0 | C, T | 12,802.0 | 11,027.9 | +1,774.1 | +13.86% |
| 1 | T, C | 12,815.2 | 10,996.3 | +1,819.0 | +14.19% |
| 2 | C, T | 12,856.1 | 11,102.7 | +1,753.4 | +13.64% |
| 3 | T, C | 12,828.9 | 11,088.8 | +1,740.1 | +13.56% |
| 4 | C, T | 12,769.9 | 11,085.2 | +1,684.6 | +13.19% |

Control median **12,815.2 ms**, range 86.3 ms; treatment median
**11,085.2 ms**, range 106.4 ms. Median gain **1,753.4 ms, 13.68% of the
control median**; every pair strictly positive.

## The rule, applied as written

Retained only if the median gain exceeds `2 × range_control` and every
pair is strictly positive. Required: 172.5 ms. Median: 1,753.4 ms, ten
times the bar. Every pair positive. **Retained.** The rule is the one ADR
588 declared, fixed before this run and not revisited.

The control median is 152.2 ms (1.17%) faster than ADR 588's control
12,967.4 ms on the same recipe with the census and finish plumbing added
to the unprofiled path (a partition index carried per batch and a finish
term published at zero); reported as drift, never netted. Peak RSS rose
from 1,288 MiB on the control to 1,363–1,372 MiB on the treatment, about
80 MiB, inside the 2 GiB coordinate; the build's peak point is the fill,
when the entries, the pairs array, and the bucket array are all live.

## The closing tree, measured

The reviewed fixes after the first run (the guarded maxima and list-header
vectors, the obtained-capacity re-checks, hashbrown's real footprint, the
overlap peak, `once_built_index_sizing`) changed no fast-path instruction
in intent, but intent is not a measurement: the retained result was the
`737c54cd…` executable's, and the closing tree is a different executable.
So the closing tree was built as `adr589-treatment.manifest` (the
measured one renamed to `adr589-treatment-measured-737c54cd.manifest`,
retained) and run through the same verb against the same recorded
control `adr589-control.manifest`, on 2026-09-15 at the same coordinate
(deployment stopped, load 0.50, package 49 °C at start, the spill scratch
empty after the run), with the identity gate, the validator on every
child, the phase-0 tripwire, and the retention rule unchanged.

Phase 0 on the control: warmup 12,634.3 ms, sample 12,493.9 ms, −3.65%
from ADR 588's control median 12,967.4 ms, not tripped. Warmups: control
12,618.3 ms, treatment 10,863.9 ms. Then the five alternating pairs.

| pair | order | control ms | treatment ms | gain ms | gain |
|---:|:---:|---:|---:|---:|---:|
| 0 | C, T | 12,564.6 | 10,768.8 | +1,795.8 | +14.29% |
| 1 | T, C | 12,466.5 | 10,819.4 | +1,647.1 | +13.21% |
| 2 | C, T | 12,378.2 | 10,639.7 | +1,738.5 | +14.05% |
| 3 | T, C | 12,387.3 | 10,654.4 | +1,733.0 | +13.99% |
| 4 | C, T | 12,500.9 | 10,777.3 | +1,723.6 | +13.79% |

Control median **12,466.5 ms**, range 186.4 ms; treatment median
**10,768.8 ms**, range 179.7 ms. Median gain **1,733.0 ms, 13.90% of the
control median**; required 372.8 ms; every pair strictly positive.
**Retained**, under the rule as written, on the closing executable.

All fourteen rows carried the sealed q21 digest byte for byte, reported
`full_hash`, the dataset fingerprint `fnv1a64:0fc5c06d19c5ed23`,
`parquet`, `diagnostics_disabled = true`, and the executable and manifest
of their own arm (`f1991c91…` on every control row, `285641fd…` on every
treatment row). Peak RSS: control 1,290–1,291 MiB, treatment 1,367–1,370
MiB, the same 80 MiB as before. Spill bytes 27,242,808 on every row of
both runs.

The two runs agree within their own noise: the control ran 348.7 ms
(2.7%) faster on the second night and the treatment 316.4 ms (2.9%)
faster, host drift on both arms alike, and the gains sit at 13.7% and
13.9% with overlapping pair ranges (+1,684.6..+1,819.0 and
+1,647.1..+1,795.8). Neither run is netted against the other; each stands
on its own executable. The mechanism check below was run on the
`737c54cd…` profiling executable and is explanatory for that executable;
it was not rerun; the closing tree's release pairs are the measurement
that clears it.

## The mechanism check, reported and not gating

ADR 587's driver ran on a `profiling` build of the treatment tree
(executable `6fcd2ab5…`, tree `737c54cd`, recipe `a1fad89d…` as ADR 587's).
Its phase 0 first ran against the reference the record named, a hashed
build's 13,008.9 ms from the census, and tripped at −15.25%: the
treatment's own gain read as a host shift, which the tripwire cannot tell
apart. That run is retained as a rehearsal. The check was rerun against
the treatment's own unprofiled median from the release pairs, 11,085.2
ms, which is the reference a check of the treatment needs: −0.71%, not
tripped; perturbation 2.49%, accepted; every profile and residual
complete; outer residual 0.94–1.00 ms; zero lost samples; 148,000–150,000
samples per pair.

The counters, identical in every pair:

| | semi | anti |
|---|---:|---:|
| `ordered_fast` | 12,000,000 | 11,002,698 |
| `appended` | 12,000,000 | 11,002,698 |
| `fallback_found_sorted` | 4,370 | 3,155 |
| `lookup_found` | 4,370 | 3,155 |
| `fallback_found_overflow` / `fallback_appended_overflow` | 0 / 0 | 0 / 0 |
| `probe_steps` / `growth_reprobes` / `grow_events` | 0 / 0 / 0 | 0 / 0 / 0 |
| admission | admitted, Exchange op 0, scan column 0 | admitted, Exchange op 1, scan column 0 |
| census `below_max` / `found_elsewhere` | 0 / 0 | 0 / 0 |

Every miss took the fast path; every recurrence the cache lost was found
by binary search in its partition's sorted list; nothing reached the
overflow map; the index was neither probed nor grown during the build.
The census on the treatment agrees with phase 1's to the row, and its
opening rule still opens.

The terms, median-scaled ms, beside ADR 587's:

| term | ADR 587 (hashed) | treatment (order-aware) |
|---|---:|---:|
| `semi_write_insert` (per-batch) | 1,583.0 | 343.4 |
| `semi_insert_rows_excluding_grow` | 1,122.8 | 328.8 |
| `semi_insert_grow` | 450.5 | 0.0 |
| `semi_insert_finish` (new) | — | 302.3 |
| `semi_insert_total = write_insert + finish` | 1,583.0 | 645.7 |
| `anti_write_insert` | 1,368.1 | 285.5 + 277.4 finish = 562.9 total |
| `runtime_prep_semi_write` | 2,199.1 | 952.8 |
| `runtime_prep_anti_write` | 1,857.1 | 790.3 |
| `runtime_prep_inner_1` | 1,654.0 | 1,671.4 |
| `cleanup` | 567.3 | 561.0 |

The finish, per summary and pair (semi, pair 0): `index_pairs` 31.1 ms,
`index_sort` 220.0, `index_alloc` 37.4, `index_fill` 13.6, `build_cleanup`
4.8; the sort of twelve million pairs is three quarters of it, and the
sequential fill is 14 ms where the hashed build's twelve million random
probes were over a second. `insert_finish` reconciles in the runtime-prep
band as `semi_finish` and `anti_finish` (residual 19.8 ms), and the
insert totals fell by 937 ms (semi, from 1,583.0) and 805 ms (anti, from
1,368.1), 1,742 ms together, against the release pairs' 1,753 ms median
gain; two profiles, two runs,
reported beside each other and not netted. The work was removed, not
moved: the finish that replaced the probes and the growth is a fifth of
what they cost.

The three-term owner rule on the treatment returns `below_floor_no_mechanism`
(rows-excluding-growth 328.8 ms against a 500 ms floor); the row loop is
no longer an owner. The sampled classes inside what remains of it are
reported as unresolved: `other` leads (57%), then `shared` (26%) and
`ordered` (17%), with `hash_and_probe` at zero in every pair; with the
probes gone the loop is arrow column access, the census's own compare,
and vector pushes, most of which resolve outside `streaming.rs`. The
estimated coverage fell to 0.67 (semi) and 0.65 (anti) from ADR 587's
0.89. The rows windows are now 23,436 intervals of about 14 µs each
against a 100 µs sampling period, which limits what the estimate can say
at this window length; that limit is stated, and it does not establish
why the coverage fell. The discrepancy is unexplained, as the record
requires, and no cause is attributed.

## The owner after the cut

ADR 585's rule on the treatment's receipts names `runtime_prep_inner_1`,
the partitioned `orders` build for replay 1, at 1,671.4 ms (15.3% of a
10,953 ms off median), ahead of `runtime_prep_semi_write` at 952.8 ms.
ADR 586's rule inside the semi write names `semi_write_repartition` at
606.4 ms over `semi_write_insert` at 343.4 ms: the Exchange, whose
partition index the writer now consumes to drive the build, costs more
than the insert it feeds. Phase 1 measured that every observed global
restart occurs at a partition transition and every observed recurrence at
a same-partition boundary straddle; what a build without the Exchange
would see remains unmeasured. The Exchange stays ADR 586's candidate,
gross with net unpriced, now with a smaller insert beside it.

## Implementation and verification

Plumbing commit `8eecba1d` (the control): `runtime_prep_{semi,anti}_finish`
as runtime-prep children in ADR 585's band with `insert_total` in ADR
587's, zero-call finish terms accepted as facts, `insert_finish` and its
windows on the instrument; the admission gate `order_aware_admission`
tracing both ordinals to the scan through exactly the Projects between
them and the scan, recorded in the receipt as `{semi,anti}_write_admission`;
the writer passing each batch's partition on both paths; the
`dataset_manifest_fingerprint` and `data_source` fields on the
`perf-run-one` row bound to the persisted dataset; an `adr589-q21-order-aware`
verb on the paired driver, which is ADR 588's driver under its own kind.
Tests: the receipt band with finish terms (reconciled, over-residual as a
defect, missing as a defect); admission fixtures (a), (b), (c) the decoy,
(d) computed key, (e) no Exchange, (f) two Exchanges; ADR 585's term
universe of fifteen.

Cut commit `15d889df` (the treatment): `SummaryBuild::{Hashed, Ordered}`;
`for_existence_plan` choosing the build by admission; `insert_ordered` with
the fast path and the exact fallback (binary search of `ordered[p]`, then
the overflow map, new below-max keys into the overflow map only);
`finish_index` with the pairs array, in-place sort, one bucket allocation
at the hashed build's final capacity, the monotone unwrapped-cursor fill
with a debug assertion below `2N`, and the cleanup; every allocation
guarded before it is made at the capacity obtained with overlap counted
(`reserve_entries_exact`, the partition lists, the overflow map's reserve,
the pairs array, the bucket array), the pairs accounted while live, the
peak observed at the fill; probing an unfinished ordered build asserted
against. Tests: a boundary straddle found without appending and marked
multi-valued; a maximum per partition with a below-max key on one
partition and a found key on another; descending keys into the overflow
map with the sorted list still sorted and the maximum unchanged;
multi-key bits equal to the hashed build's on every path; randomised
mixed-order input on three hash-routed partitions equal to the hashed
build in entries, representatives, multi bits, probe results, table size,
and every entry found by a fresh walk, with probe steps beside the grown
table's; end-of-array collisions wrapping into occupied low buckets with
absent-key probes ending at empty slots; several high-home groups
continuing through one wrap with fill slot visits at most `N + pairs`; an
empty build's index finding nothing; the three budget refusals (overflow
growth, pairs array, bucket array) each before the allocation with the
summary intact; five disjoint ordered finish windows summing to the term
and none on a hashed build; the writer taking the hashed build when
admission is refused and the ordered build, fast path only, when admitted.
ADR 587's line classes re-pinned with `ordered` and `finish` classes and
the anchor test passing; the binner's rows classes extended so its
identities still hold.

Two contracts were tightened under review after the runs, in the closing
commit, without changing what the measured tree does on the fast path.
Memory: the per-partition maxima and list-header vectors are now guarded
before they grow; a partition list's and the overflow map's obtained
capacity is re-checked after the reservation, and the overflow map is
priced at hashbrown's real footprint (one slot and one control byte per
bucket at a power-of-two count above 8/7 of the capacity, plus a control
group), since a reservation of 16 obtains 28 slots at 32 buckets; every
one of these allocations updates the peak, and the peak it records is the
overlap: the bytes accounted before the reservation plus the obtained
allocation, since a growing vector or map holds its old storage while the
new one is filled, so the pre-reservation accounting is preserved and the
reservation is added on top rather than netted. Tests: a cap between the
naive request and the real footprint refuses before the allocation with
the map still empty, and the same insert is admitted and accounted at the
real footprint; a first-seen partition's vectors are refused under a
tight cap; a growth on each of the three paths (partition vectors,
partition list, overflow map) records a peak equal to the old storage
plus the new, above the final accounted bytes.
Index width: the once-built index is sized by `once_built_index_sizing`,
which refuses more than 2^31 entries, the count at which the doubled
bucket array's home buckets would no longer fit the pair's `u32`, and the
pair's fields are converted with checked conversions; a test sizes the
boundary at 2^31 and 2^31 + 1 without allocating. The `737c54cd…` executable
predates these fixes; its runs at twelve million entries and an empty
overflow map exercised neither path, and its retention rests on that tree
as recorded. The closing tree carries them and was measured again
("The closing tree, measured"), so the retained candidate is the
executable that ships.

Ladders (`RUST_MIN_STACK=67108864`): fmt clean; control tree Toron library
1,790 and toron-tpch 589; treatment tree Toron library 1,802,
architecture 2,151, integration 3,649, toron-tpch 589 passed, 6 ignored;
closing tree Toron library 1,806, architecture 2,151, integration 3,649,
toron-tpch 589.

The result and the cut land together; the closing commit carries this
record and the ADR's status; the arc adds no stamp-only commit.
