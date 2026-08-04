# ADR 417 results - scan-output accumulator eligibility restoration

Base: `3bda38c plan: adr 417 scan output accumulator family`

## Phase 0 - persisted sf=10 classification (recorded before the cut)

All four rows were reproduced through the exact `perf-run-one --mode strict --sf 10 --data-source parquet` path under
the fixed 4 GiB systemd cap. An off-by-default diagnostic at the bind/execute seam captured the bound plan used by
that same invocation; it was removed before the implementation cut. The captured fingerprints exactly matched the
fingerprints in the corresponding failing perf rows. Scan progress artifacts identified the concrete overflowing
scan rather than inferring it from SQL.

| query | plan fingerprint | overflowing scan and projection | immediate consumer | enclosing recognizer | exact miss |
| --- | --- | --- | --- | --- | --- |
| q13 | `ac3784c060c3c226` | `orders`, scan 3, `[o_orderkey,o_custkey,o_comment]` / source ordinals `[0,1,5]` | `Filter(EvaluateInToron) -> Exchange -> LeftOuter` right/probe input | `try_streaming_left_outer_grouped_aggregate` | recognizer enters, but its streamed-spilled preserved-side arm requires `retained_columns.len() < left.width`; join projection pruning leaves the customer side as the one required key, so retained width equals input width and execution falls through to vanilla materialization |
| q17 | `97ca111db986c14d` | `lineitem`, scan 1, `[l_quantity,l_extendedprice,l_partkey]` / `[0,1,7]` | `Exchange -> Inner Join(0)` left/probe input | `try_streaming_correlated_scalar_left_outer_global_aggregate` | correlated recombine requires `Exchange(Filter(Join(0)))`; the current persisted plan has the equally valid bare `Exchange(Join(0))` alias |
| q18 | `d8b60e3a03bfaf79` | `lineitem`, scan 3, `[l_quantity,l_orderkey]` / `[0,8]` | `Exchange -> Inner Join(1)` right/build input | projected LeftSemi grouped fold plus reoriented-inner-chain machinery | the grouped LeftSemi recognizer materializes its entire inner-chain preserved side before estimating/spilling it; no existing alias composes the reoriented inner chain into the semi grouped fold |
| q21 | `e2e25953a6a3fefd` | `lineitem`, scan 2, `[l_orderkey,l_suppkey,l_commitdate,l_receiptdate]` / `[8,9,10,11]` | `Filter(EvaluateInToron) -> Exchange -> Inner Join(0)` right/probe input | `try_streaming_inner_prefix_semi_anti_grouped_aggregate` | the chain extractor requires `Exchange(Filter(Join(1)))` and `Exchange(Filter(Join(2)))`; the current plan has bare `Exchange(Join(1))` and `Exchange(Join(2))`, so it rejects before the already-supported base scan is streamed |

The pre-cut byte rows were independently reproduced:

| query | requested | available | over | execute ms | spill bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| q13 | 1,073,760,840 | 1,073,741,824 | 19,016 | 1,010.1 | 0 |
| q17 | 1,073,767,936 | 1,073,741,824 | 26,112 | 1,005.4 | 0 |
| q18 | 1,073,742,336 | 1,073,741,824 | 512 | 7,404.9 | 18,862,208 |
| q21 | 1,073,754,240 | 1,073,741,824 | 12,416 | 1,584.4 | 0 |

The artifact-backed parent paths were:

```text
q13 Aggregate <- Exchange <- LeftOuter(Exchange(Scan customer[0]),
     Exchange(Filter(Scan orders[0,1,5])))
q17 Aggregate <- Filter <- LeftOuter(Exchange(Inner Join(0)(
     Exchange(Scan lineitem[0,1,7]), Exchange(Filter(Scan part[0])))), scalar grouped fold)
q18 Aggregate <- Exchange <- LeftSemi(Exchange(Inner Join(1)(
     Exchange(Inner Join(0)(customer,orders)), Exchange(Scan lineitem[0,8]))), grouped membership)
q21 Aggregate <- Exchange <- LeftAnti(Exchange(LeftSemi(Exchange(Inner Join(2)(
     Exchange(Inner Join(1)(Exchange(Inner Join(0)(supplier,filtered lineitem)), filtered orders)),
     filtered nation)), lineitem membership)), filtered lineitem anti-membership)
```

### History audit

**q13 / ADR 369.** ADR 369's streamed-spilled LeftOuter grouped arm and its strict-less-than width fence are both
unchanged since `f4cb6a8`. The retained ADR 369 result proves q13 passed persisted sf=10 through that arm. ADR 397
(`2d83f49`) then introduced required-column projection through joins; the current artifact proves that this reduced
q13's preserved customer input to the sole join/group key. That turns ADR 369's projection-benefit check from true to
false even though the same spill-backed execution is still required to avoid scan materialization. This is lost
eligibility, not a new topology; ADR 397's pruning remains intact and the executor fence is restored for the
all-columns-retained case.

**q21 / ADR 372.** Retained rows through ADR 386 use fingerprint `a1bf4abc376f6ec4` and prove q21 advanced beyond scan
output to the semi/anti existence-set producer (ADR 372 later removed that producer). The current fingerprint is
`e2e25953a6a3fefd`, and the current artifact proves the semi/anti topology is still present but its two intermediate
inner joins are no longer wrapped in executor-evaluated filters. Thus today's scan is a lost-eligibility regression,
not a new downstream scan. Retained artifacts do not isolate the exact shape-changing commit: the narrowest proven
interval is after the ADR 386 artifact and by the ADR 415 sweep. The implementation restores the filter-optional alias
without guessing a more precise historical attribution.

### Cut selection

The implementation cap selects one bounded eligibility family: optional-filter/bare-join aliases feeding existing
streamed join machinery. It also restores q13's already-proven ADR 369 spill-backed path without introducing another
execution mechanism. q18's inner-chain-under-semi composition is structurally separate and remains a named follow-on;
no generic pull executor or scan spill is introduced.

## Implementation and post-cut evidence

The cut is eligibility-only and feeds existing mechanisms:

- the ADR 369 LeftOuter grouped spill arm now accepts an all-columns-retained preserved scan. The binder's narrower
  projection is kept; the one-column customer scan is written through the same projected spill API and the orders
  probe still comes from `bound_scan_request`;
- the correlated-scalar recognizer accepts both `Exchange(Filter(Join(0)))` and `Exchange(Join(0))`, passing the
  optional predicate into the existing chunk visitor;
- the semi/anti chain extractor accepts filtered or bare replay-1/replay-2 joins, and its base visitor likewise
  accepts a filtered or bare replay-0 join. Present filters are still evaluated in the same position.

No second executor mechanism, generic pull interface, scan spill, cap change, or binder rollback was introduced.

### Per-cell primary outcomes

| query | post-cut outcome | mechanism proof | correctness status |
| --- | --- | --- | --- |
| q13 | **pass@compare sf=10**; driver reached 46 rows in 129,386.6 ms, 9,152,992 spill bytes, 956,532 KiB max RSS | scan progress shows exactly one customer scan and one orders scan; orders `[0,1,5]` no longer enters the scan accumulator | persisted strict sf=10 compare passed in 153,259.2 ms |
| q17 | **reach_only sf=10**; one row in 56,867.4 ms, 961,584 spill bytes, 1,110,240 KiB max RSS | scalar lineitem `[0,7]`, pushed part `[0]`, and preserved lineitem `[0,1,7]` all complete through the streamed correlated recombine | sf=10 remains `q17_sf10_oracle_policy_gap`; supported sf=0.02 strict compare passed in 809.0 ms as secondary evidence |
| q18 | unchanged scan-output wall; bounded follow-on | current persisted plan needs composition of a reoriented inner chain as the preserved input of the grouped LeftSemi path, which is a separate mechanism from the optional-filter aliases selected here | pre-cut wall remains reproduced at `1,073,742,336 / 1,073,741,824`; named follow-on candidate ADR 418, not claimed by 417 |
| q21 | **advanced to named next wall**: `semi/anti existence-set spill resident state`, `1,073,742,536 / 1,073,741,824`, in 9,078.7 ms | progress reaches the lineitem `[8,9]` existence scan after the orders/nation stages; the former initial lineitem scan accumulator is gone | strict sf=0.01 compare passed in 804.1 ms; no sf=10 correctness claim because the named next wall precedes output |

q21's result recovers historical ground: the post-cut producer is the same resident-state producer recorded after the
earlier semi/anti work, not the regressed leaf scan producer. q17 is deliberately not labeled sf=10 pass@compare even
though the reach driver returned a digest.

### Parity and ScanRequest contract

Three materialized-vs-streamed parity locks cover every admitted alias:

- `streaming_scan_left_outer_grouped_aggregate_keeps_all_preserved_columns` covers q13's one-column preserved side,
  pre-join scan filter, matches, and null-fill;
- `correlated_scalar_bare_join0_stream_matches_materialized_recombine` covers q17's bare Join(0) correlated recombine;
- `streaming_inner_prefix_semi_anti_bare_join_alias_matches_materialized` covers q21's bare intermediate and base
  joins, residual-bearing semi/anti stages, and grouped fold.

All three use the production executor for the streamed plan and a shape-fenced materialized plan for the baseline.
They are eligibility-discriminating rather than digest-only: the q17 lock invokes the correlated recognizer directly
and requires `Some(output)`, while the q13 and q21 locks require nonzero streaming spill telemetry before executing
their materialized twins. A future recognizer miss therefore fails locally instead of letting both twins fall back.
The bypasses call the existing visitors, which obtain every scan through `bound_scan_request`; they do not reconstruct
requests. The existing `bound_scan_request_carries_every_bound_scan_field` test remains the all-observable-field lock
for projection, pushed predicate, row limit, identity, profile hint, tenant/session/query/read bound, runtime filter,
trace, morsel, and downstream cancellation.

## Controls and verification

All nine required spill-bearing green controls passed persisted strict sf=10 compare under the 4 GiB cap:

| query | compare | forensic perf status | spill bytes |
| --- | --- | --- | ---: |
| q03 | pass | pass | 233,338,304 |
| q07 | pass | pass | 134,732,480 |
| q09 | pass | pass | 108,992,256 |
| q10 | pass | pass | 215,703,848 |
| q12 | pass | pass | 159,510,976 |
| q14 | pass | pass | 36,464,000 |
| q16 | pass | pass | 60,078,400 |
| q19 | pass | pass | 51,471,552 |
| q20 | pass | pass | 256,104,392 |

The required forensic bands are exact; q20's fixed-cap residency watch also remains green.

Verification passed:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron streaming --lib                 # 43 passed
scripts/cargo-cgroup test -p toron join --lib                      # 195 passed
scripts/cargo-cgroup test -p toron --test architecture_test        # 2,079 passed
scripts/cargo-cgroup test -p toron --no-fail-fast                  # passed, all targets
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

The feature-expanded `toron-tpch` suite passed, including its 41-test harness target. No test target reported a
failure. `exec.rs` remains inside its architecture ratchet at 7,499 / 7,500 lines.

## Seal audit

- Phase 0 was recorded before the implementation and is backed by the exact persisted bind fingerprints and scan
  progress paths.
- q13 independently passes persisted sf=10 compare; q17 independently reaches output with its policy gap stated and
  supported-scale secondary compare; q21 independently reaches a named and reproduced next wall; q18 is explicitly
  the structurally separate, implementation-cap follow-on.
- Every admitted alias has materialized-vs-streamed parity and uses the shared `bound_scan_request` constructor.
- No memory/spill cap, multiplier, query scope, binder projection, or oracle policy changed.
- No scan spill without a puller and no partial generic pull executor landed; no C-spec was required for the selected
  bounded aliases.
- The temporary bound-plan diagnostic was removed. Core executor source contains no ADR 417 or qNN branding.
- No `.perf.data` is tracked. q02/q22 (join-output/writer residency) and q05/q11 (residency/oracle) remain untouched
  named families.
