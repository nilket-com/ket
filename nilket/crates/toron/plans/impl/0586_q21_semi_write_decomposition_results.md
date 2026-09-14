# ADR 586 - q21 semi-summary write decomposition results

Measured 2026-09-14 at the sealed ADR 583 coordinate on one instrumented
executable that ran phase 0 and both arms of every pair. Phase 0 did not
trip. The perturbation gate accepted the profile at 0.47%. Every band
reconciled exactly in every pair, every residual met its bound, and the
five-term owner rule resolved a single owner inside the semi write:
**`semi_write_insert`, 1,610.7 median-scaled ms, 12.27% of the unprofiled
execute median**, leading `semi_write_repartition` (636.0 ms) by 974.7 ms
against a 172.2 ms off-arm range. The insert child is not itself removable;
the successor splits it before any writer change. The repartition child
clears the floor as gross removable work (636.0 ms > 500 ms) but is not the
owner, and its net is unpriced here. No engine route, admission, or writer
changed.

## Identity and coordinate

| identity | value |
|---|---|
| sealed baseline executable | `b436a843b107b2f385d5e7c2486c41b1d1f89acfdb4af75f8a4793c823a05ee4` |
| ADR 585 instrumented executable | `2119272652c4c81f9e2ee9b45bda6236a59cf452ad2c3d1e37b4d5ce4485b444` |
| instrumented source tree | `1d99858f8addc4622b2497d23a601e6e6ec7f5ff` (commit `4326eb73`) |
| instrumented executable | `03daafed26923778cc7011512d18bc0d30a43ac19056de004f1a3611ab571ace` |
| build recipe | `54800bbb3059c3f2b142c048e4b49815ec26ec1dc4b4cfeb2efa5b88189ec771` |
| dataset manifest | `fnv1a64:0fc5c06d19c5ed23` |

The recipe differs from ADR 585's `fcdf9ca1…` only in the `Cargo.lock`
hash; the rustflags hash (`3d1cfdc9…`), profile (release), features (none),
target, and rustc (1.95.0) are identical. The lock moved with eleven days of
unrelated work on the tree, so ADR 585's row is not a same-recipe control
for this run; the distance is reported and never netted. Neither earlier
executable ran under this record.

The coordinate was q21 alone, strict SF=8 over persisted Parquet, explicit
2 GiB task memory, explicit 2 GiB spill reserve, partition parallelism 1,
full-hash dataset verification, an empty dedicated spill scratch
(`target/tpch/adr586-join-spill`, empty after the run), and the local
deployment stopped for the measurement and restarted after it. A polariton
health sampler kept retrying its `nilket toron sql` insert against the
stopped server every few minutes with a 30 s timeout; it was left running
and is the one known non-idle process during the run. Artifacts are under
`target/tpch/adr586-q21-final/` (`samples.jsonl`, `summary.json`,
`profiles/pair-NN-on.json`). The run is unsealed and carries no candidate
authority.

## Phase 0

| step | execute (ms) |
|---|---:|
| host-check warmup | 13,471.1 |
| host-check sample | 13,352.7 |
| reference (ADR 585 off median) | 13,958.6 |

Movement −4.34%, threshold 10%: **not tripped**. The sample entered no
statistic.

## Perturbation and outer boundaries

One further unprofiled warmup (13,235.1 ms), then five adjacent pairs in
alternating off/on order.

| pair | order | off ms | on ms | move |
|---:|:---:|---:|---:|---:|
| 0 | off, on | 13,111.8 | 13,150.0 | +0.29% |
| 1 | on, off | 13,128.5 | 13,057.4 | −0.54% |
| 2 | off, on | 13,239.1 | 13,222.7 | −0.12% |
| 3 | on, off | 13,066.9 | 13,128.8 | +0.47% |
| 4 | off, on | 13,190.5 | 13,031.7 | −1.20% |

Off median **13,128.5 ms**, range 172.2 ms (1.31%); on median 13,128.8 ms,
range 191.0 ms. Median absolute movement **0.47%**, below the 25% rejection
threshold and below ADR 585's 1.16%. The on and off medians differ by 0.3
ms, so the instrument's cost at this coordinate is inside the pair noise;
that is the only claim made about it.

The off median is 830.1 ms (5.95%) faster than ADR 585's 13,958.6 ms, beyond
either cohort's range, on a recipe whose lock differs. It is reported as
drift plus recipe distance, not netted, and it is not evidence of an engine
change: no route changed and the scaled shares below sit within a point of
ADR 585's.

Every on arm carried a distinct profile path; every off arm carried neither
a profile nor a receipt and retained `diagnostics_disabled = true`. ADR
507's 10 ms outer boundary held in every pair (outer residual 190–203 µs).

## The three ADR 585 bands, re-reported

All three sums reconciled exactly in every pair. Residual bounds
(`overhead + 5% handler`) ranged 651,557–718,293 µs; the runtime-prep
residual was 17,106–17,417 µs and the lifecycle residual 696–776 µs in every
pair.

| term | this run (ms) | ADR 585 (ms) | delta |
|---|---:|---:|---:|
| `runtime_prep_semi_write` | 2,251.6 | 2,323.1 | −71.5 |
| `runtime_prep_anti_write` | 1,869.8 | 1,954.0 | −84.2 |
| `runtime_prep_inner_1` | 1,711.3 | 1,810.2 | −98.9 |
| `cleanup` | 573.1 | 586.8 | −13.7 |
| `runtime_prep_anti_read` | 506.2 | 528.6 | −22.4 |
| `runtime_prep_semi_read` | 374.6 | 386.2 | −11.6 |
| `runtime_prep_residual` | 17.3 | 17.6 | −0.3 |
| `base_prep` | 2.2 | 2.4 | −0.2 |
| `runtime_prep_inner_2` | 1.5 | 1.4 | +0.1 |
| `lifecycle_residual` | 0.8 | 0.8 | 0.0 |
| `finalize` | 0.6 | 0.6 | 0.0 |
| `outside_handler_span` | 0.4 | 0.4 | 0.0 |
| `outer_residual` | 0.2 | 0.2 | 0.0 |

Every term moved by less than this run's 172.2 ms off range and less than
ADR 585's 247.4 ms; the two runs agree at the noise boundary. The
thirteen-term rule on this run's receipts again names `runtime_prep_semi_write`
(17.15% of off execute; ADR 585: 16.64%), leading `runtime_prep_anti_write` by
381.8 ms. `drive_inclusive` is 5,829.6 ms against ADR 585's 6,305.7 ms and the
non-drive sum 7,308.8 ms against 7,651.3 ms; the drive term moved by more
than the range, in the direction of the run's overall 5.95% distance, and is
not a candidate under either record. The shared-source pairwise removable
read is 374.6 ms, still below the floor.

## The fourth band

Both write halves reconciled exactly (children + residual = write term) in
every pair. The decoded op lists are the run's first finding: the **semi
summary carries one op, `exchange`**, and the anti summary two, `filter` and
`exchange`. Neither carries a `Project` op: on this route the key columns
come straight out of the scan projection, so the key-program child is
absent in both summaries by plan shape, with zero passes, not by
measurement. The filter child is absent from the semi summary the same way.
Each summary saw **5,859 source batches**; every present op and the insert
counted one pass per batch.

Raw on-arm microseconds; `bound` is `overhead + 5% write`.

| pair | semi write | filter | key program | repartition | insert | residual | bound | ok |
|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 0 | 2,240,583 | 0 | 0 | 629,680 | 1,607,724 | 3,179 | 150,216 | yes |
| 1 | 2,242,466 | 0 | 0 | 632,046 | 1,607,162 | 3,258 | 112,123 | yes |
| 2 | 2,260,029 | 0 | 0 | 648,035 | 1,608,723 | 3,271 | 113,001 | yes |
| 3 | 2,248,911 | 0 | 0 | 650,427 | 1,595,266 | 3,218 | 174,326 | yes |
| 4 | 2,224,477 | 0 | 0 | 628,374 | 1,592,859 | 3,244 | 111,224 | yes |

| pair | anti write | filter | key program | repartition | insert | residual | bound | ok |
|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 0 | 1,858,434 | 72,384 | 0 | 422,825 | 1,356,708 | 6,517 | 131,108 | yes |
| 1 | 1,859,728 | 72,388 | 0 | 422,570 | 1,358,374 | 6,396 | 92,986 | yes |
| 2 | 1,892,375 | 72,634 | 0 | 437,426 | 1,375,741 | 6,574 | 94,619 | yes |
| 3 | 1,878,188 | 72,797 | 0 | 436,362 | 1,362,406 | 6,623 | 155,790 | yes |
| 4 | 1,869,551 | 72,857 | 0 | 425,252 | 1,364,873 | 6,569 | 93,478 | yes |

The write residuals (3.2 ms semi, 6.6 ms anti) are the batch unwrap and loop
overhead the record predicted, two orders of magnitude inside their bounds.

The instrument's clock count was smaller than the record projected, because
the record misread ADR 585's "~23,000 `Instant` reads per query" as 23,000
source batches. With 5,859 batches per summary, the semi summary added two
clock pairs per batch (one op pass, one insert loop) and the anti three, about
58,600 additional reads per query rather than 370,000. The perturbation
gate priced them regardless.

## Five-term owner result

Floor `max(500 ms, 2 × 172.2) = 500 ms`. Median-scaled milliseconds over the
five pairs:

| term | median scaled ms | share of off execute | clears floor |
|---|---:|---:|:---:|
| `semi_write_insert` | 1,610.7 | 12.27% | yes |
| `semi_write_repartition` | 636.0 | 4.84% | yes |
| `semi_write_residual` | 3.3 | 0.02% | no |
| `semi_write_filter` | 0.0 | 0.00% | no |
| `semi_write_key_program` | 0.0 | 0.00% | no |

**`semi_write_insert` is the owner**: its 974.7 ms lead over the repartition
child exceeds the 172.2 ms tie range. No residual leads. The per-pair scaled
insert values were 1,587.7–1,615.9 ms; the repartition values 627.9–648.8 ms.

The anti write, reported beside it and not ranked:

| term | median scaled ms | share of off execute | clears floor |
|---|---:|---:|:---:|
| `anti_write_insert` | 1,365.8 | 10.40% | yes |
| `anti_write_repartition` | 430.4 | 3.28% | no |
| `anti_write_filter` | 72.7 | 0.55% | no |
| `anti_write_residual` | 6.6 | 0.05% | no |
| `anti_write_key_program` | 0.0 | 0.00% | no |

The same shape holds: the insert owns roughly three quarters of each write
half, the repartition the rest, the anti's date filter costs 72.7 ms for a
full pass over its alias. Together the two inserts are 2,976.5 scaled ms,
22.7% of q21's execute, on one code path.

## Removable pricing and the successor

The insert child is not itself removable: it is the summary being built. The
record therefore opens no mechanism on it and names the successor as the
next split at the same coordinate, inside `NeqExistenceSummary::insert`:
consecutive-key cache hits, hash-and-probe for keys the cache does not
absorb, and growth. That split is what tells the writer's successor whether
the cost is in the hash, in the probe's memory traffic over a table of tens
of millions of entries, or in the resize.

The repartition child is priced as gross removable work at 636.0 ms, above
the floor: the writer never reads the partition index, so the op's own cost
could be skipped. Its net is unpriced here, because the order it imposes is
what the insert's consecutive-key cache sees, and this run cannot say how
often that cache hits with and without it. A repartition cut remains a
candidate for a successor that measures net execution improvement in paired
off/on runs beside value parity; it does not open on this evidence.

`cleanup` (573.1 ms) and `runtime_prep_inner_1` (1,711.3 ms) stay open as
their own candidates, unchanged by this record.

## Implementation and verification

The write loop accumulates its four children only when a caller passes
clocks or a stage handle is present, so an unprofiled arm never reads a
child clock; ADR 585's unconditional read/write clocks are untouched. The
children publish through the existing inclusive-lifecycle term path with
their pass counts as `calls`, and the op list as an invocation diagnostic.
The receipt's fourth band computes both write residuals by checked
subtraction and reports `instrument_defect` on underflow, a missing child,
or a missing op list. The driver runs phase 0 with an explicit reference,
refuses to proceed when tripped, and evaluates the pairs through the ADR 585
three-band evaluator (now parameterised by row tags, its behaviour
unchanged) before applying the fourth band, the five-term rule with its
three outcomes, and the pricing.

Tests on the measured tree: four engine accounting tests (full chain with
one pass per batch per child; absent filter listed absent with zero passes;
a filter that empties the stream leaves downstream passes at zero; the
unprofiled path writes every row), three receipt tests (fourth band with
pass counts and op lists, children exceeding the write term as a defect,
missing child or op list as a defect), and eight driver tests (tripwire on
both sides of 10%, resolved owner with gross repartition pricing, tie within
the off range, below-floor with no mechanism, insert not removable and other
owners unpriced, an incomplete write residual withholding the owner, a
leading residual at the rule, and refusal of missing pass counts, op lists,
or broken algebra). ADR 585's driver tests pass unchanged on the shared
evaluator; its receipt test's fixture gained the fourth band's terms because
the receipt now requires them, and its assertions on the three bands are
unchanged.

Ladder on the measured tree (`RUST_MIN_STACK=67108864`, as `ci/nilket-check.sh`
sets it): `cargo fmt --all -- --check` clean; Toron library 1,777 passed;
Toron architecture 2,151 passed (one guard caught a query-number reference
in a test comment during development; reworded); Toron integration and
doctests all passed; toron-tpch 568 passed, 6 ignored.

The result and its instrumentation land together; there is no stamp-only
commit.
