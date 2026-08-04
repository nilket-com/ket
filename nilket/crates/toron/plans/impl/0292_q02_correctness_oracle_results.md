# ADR 292 - Results: q02 correctness oracle

## Outcome

Strict q02 is now answer-checked:

```text
tpch_correctness query=q02 sf=0.01 mode=strict phase=compare status=pass reason=none
```

The `q2` CLI alias also routes to the same q02 correctness attempt and records `query=q02`.

This adds the eighth correctness cell. Reachability is unchanged.

## Phase-0 Schema Measurement

The strict q02 output schema was measured live and pinned:

```text
s_acctbal  Decimal128(15, 2)
s_name     Utf8
n_name     Utf8
p_partkey  Int64
p_mfgr     Utf8
s_address  Utf8
s_phone    Utf8
s_comment  Utf8
```

A schema drift is `fail@compare`. The test suite explicitly rejects the stale `p_partkey Int32` variant.

## Oracle

`strict_q02_oracle` traverses the generators directly:

- `region`: keep `EUROPE`;
- `nation`: keep nations in the EUROPE region;
- `supplier`: keep suppliers in those nations and retain raw `s_acctbal` plus projected supplier fields;
- `part`: keep `p_size = 15` and `p_type LIKE '%BRASS'` via a suffix helper;
- `partsupp`: for eligible parts and EUROPE suppliers, compute the per-part raw `MIN(ps_supplycost)`;
- emit every supplier/part row whose raw `ps_supplycost` equals that part's EUROPE-scoped minimum.

Measured non-vacuity at `sf=0.01`:

```text
rows          = 4
matched_parts = 4
min_tie_parts = 0
```

Because the live fixture has no min-cost tie, the tied-minimum policy is covered by a synthetic comparator test:
dropping one same-part min supplier is `fail@compare`.

## Comparator

`compare_strict_q02` asserts:

- measured schema exactly;
- no NULL in any projected cell;
- answer equality as a full-row multiset, not a projected-key map;
- exact raw `i128` equality for `s_acctbal`;
- exact string and `i64` equality for the remaining columns;
- emitted SQL order respects `s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC`, without pinning hidden
  tie-breaks beyond those keys.

## Tests

Added focused tests for:

- strict q02 end-to-end `pass@compare` at `sf=0.01` on the large-stack path;
- non-vacuous oracle data at the default correctness scale;
- measured schema drift rejection, including `p_partkey Int32`;
- `%BRASS` suffix semantics;
- Decimal, string, and `p_partkey` mismatches;
- dropped min-tie supplier rejection;
- extra, missing, and unexpected duplicate row rejection;
- NULL-cell rejection for all eight output columns;
- mixed-direction order violations.

## Matrix

Correctness now has eight strict pass cells:

```text
q01 strict=pass@compare
q02 strict=pass@compare
q03 strict=pass@compare
q05 strict=pass@compare
q06 strict=pass@compare
q12 strict=pass@compare
q14 strict=pass@compare
q17 strict=pass@compare
```

Reachability remains:

```text
q01/q02/q03/q04/q05/q06/q10/q12/q14/q17/q20: dev=pass@execute strict=pass@execute
```

## Forward

q02 closes the wide stored-column projection plus correlated-MIN correctness class. The next cycle can return to
reachability breadth, with q07/q09 still the likely frontier candidates.
