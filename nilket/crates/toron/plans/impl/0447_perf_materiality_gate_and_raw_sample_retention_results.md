# ADR 447 results — perf materiality rail and raw sample retention

## Implementation receipt

The implementation adds rail-wide raw per-run samples, separates the existing
one-sided `slower_than_baseline` alarm from materiality, and provides a
deterministic paired Hodges–Lehmann estimator. `REGRESSION_THRESHOLD` and
`MATERIALITY_FLOOR` remain distinct named constants (both currently `0.10` by
policy); the estimator does not read the regression alarm.

The first q05 attempt after the source edit is excluded: it failed with a
Parquet projection error (`project index 8 out of bounds, max field 4`) before
producing a result. The harness composition was corrected, rebuilt, and the
following cohort was run from the corrected release binary.

## q05 characterization cohort

Strict sf=10, persisted Parquet, seven measured children plus one warmup,
fresh spill directory, release profile. The measured execution samples (ms,
in run order) were:

```
110158.488554, 115719.988335, 112485.082764, 110007.589363,
108743.558102, 110821.927186, 106635.973057
```

All seven children passed. Median is 110158.488554 ms, minimum 106635.973057
ms, maximum 115719.988335 ms, and the existing range spread is 0.08246314.
The sorted-sample IQR is 3741.524662 ms (IQR/median 0.03396492); the minimum
is 3.1977% below the median and the maximum 5.0486% above it. Every child
returned five rows, the same Arrow digest, the same schema fingerprint, and
3365847016 spill bytes.

The summary now retains the exact samples as `execute_ms_samples` with each
run index. The regression alarm remains unchanged and one-sided; this receipt
does not reinterpret the cohort as a materiality decision, and it does not
re-judge ADR 443, 444, or 446.

## Runtime-filter corroboration

A separate diagnostic run with the Parquet scan progress ledger counted
59,986,052 rows on each of the three unfiltered lineitem scans and 17,893,398
rows on the runtime-filtered scan. The filtered count is the measured
runtime-filter result; it is not inferred from spill bytes. This diagnostic run
is separate from the seven-child timing cohort.

The seven-child timing receipt reports 3365847016 task spill bytes. No speed or
survival claim is made from this characterization cohort. The paired comparison
design remains available through `TORON_TPCH_TORON_BIN`; no comparison-tree
paired cohort was run here.

## Verification status

The invalid pre-fix attempt is not evidence. The corrected implementation's
focused estimator/binary-override tests pass, and the q05 cohort is fully
pass/digest-stable. Full repository gates must be run on the final implementation
commit; no benchmark baseline is moved by this receipt.
