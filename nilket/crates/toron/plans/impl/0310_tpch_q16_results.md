# ADR 310 results — tpch.q16 reachability

## Measured outcome

q16 reaches execute in both modes.

```text
q16 dev    sf=0.01 pass@execute digest=ArrowIpcDigest([164, 183, 72, 40, 46, 101, 81, 163, 233, 202, 133, 17, 254, 134, 141, 48, 25, 145, 205, 121, 138, 19, 238, 208, 235, 133, 137, 224, 90, 240, 14, 132])
q16 strict sf=0.01 pass@execute digest=ArrowIpcDigest([164, 183, 72, 40, 46, 101, 81, 163, 233, 202, 133, 17, 254, 134, 141, 48, 25, 145, 205, 121, 138, 19, 238, 208, 235, 133, 137, 224, 90, 240, 14, 132])
```

Measured CLI runtimes under `scripts/cargo-cgroup` were slow but finite: dev ~75.9s, strict ~101.3s at sf=0.01.
The identical digest matches the ADR's mode-agnostic expectation: q16 projects strings, `p_size`, and an integer
count, not the strict-only Decimal columns.

**Correction (ADR 310 review):** that slowness is a **materialized cartesian, not "just slow."** The built binary
measured ~8.25 GB RSS at sf=0.01, scaling O(n²) (join memory 82 MB → 8 GB for 10× data) = `partsupp × part` (~16M
rows). The `p_partkey = ps_partkey` equality is **stranded** above the deferred `NOT IN` (`DeferredExists`) node,
so `recognize_cross_equi_joins` never keys the comma join — the ADR 282 / q17 pattern recurring. The `sf=0.0001`
test scale masked it. **q16 reaches execute but is NOT keyed; see ADR 311.**

For the permanent harness test, q16 is pinned at sf=0.0001 to avoid making `cargo test -p toron-tpch` inherit a
multi-minute q16 run:

```text
q16 dev/strict sf=0.0001 pass@execute digest=ArrowIpcDigest([11, 131, 186, 222, 220, 230, 55, 14, 13, 119, 182, 8, 125, 144, 17, 27, 122, 90, 109, 138, 119, 159, 236, 203, 211, 161, 87, 98, 116, 195, 227, 178])
```

## Verify-first finding

The first q16 probe did not pass: it blocked at plan with `unsupported_correlated_exists`. The raw wall was the
uncorrelated `NOT IN` subquery body:

```sql
ps_suppkey NOT IN (
  SELECT s_suppkey
  FROM supplier
  WHERE s_comment LIKE '%Customer%Complaints%'
)
```

`IN` already routed uncorrelated subquery predicates through the normal filtering pipeline. `NOT IN` still routed
the same inner-only predicate through the older EXISTS classifier, which rejects unqualified column references such
as `s_comment`. The implementation extends the filtering-pipeline path to uncorrelated `NOT IN` as well; correlated
`NOT IN` remains on the existing classifier/decorrelation path.

Pinned by:
- `exists_decorrelation_test::uncorrelated_not_in_with_unqualified_inner_residual_filters_subquery`
- `harness::q16_executes_in_both_modes_at_tiny_scale`

## Fixture facts for q16 correctness

The sf=0.01 fixture produces q16 result groups, so the reachability pass is not empty. However,
`%Customer%Complaints%` matches no supplier comments at sf=0.01, so the live default-scale reachability row does not
exercise an actual anti-join exclusion. The future q16 correctness oracle must either run at a scale where at least
one supplier is excluded or add synthetic coverage for the exclusion branch.

Pinned by `harness::q16_default_fixture_is_nonempty_but_anti_join_is_not_live`.

## Board impact

q16 reaches `pass@execute`, **but via a cartesian (not keyed)** — so it is **not yet a true reach-17 cell**. The
keyed green is earned by **ADR 311** (push the outer equality through the deferred `NOT IN` node). Until 311 lands,
treat the board as **16 keyed-reach + q16 executes-but-cartesian**.

```text
keyed reach: q01/q02/q03/q04/q05/q06/q07/q08/q09/q10/q12/q13/q14/q17/q20/q22  (16)
q16: pass@execute but cartesian — keyed green pending ADR 311
```

Correctness remains 16/22; no correctness oracle was added in this cut.

## Forward arc

**ADR 311** keys q16's comma join (the ADR 282 pattern for `DeferredExists`); once it lands q16 is a real reach-17
cell and its correctness oracle is the next rail catchup. The remaining reach frontier is q11/q15/q18/q19/q21.
