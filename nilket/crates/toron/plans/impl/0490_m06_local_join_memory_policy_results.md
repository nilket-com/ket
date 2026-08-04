# ADR 490 implementation receipt — ADMITTED, corrected

This receipt supersedes the earlier "not admitted" version of this file (committed at
`528bbab5`). That conclusion was wrong: it was drawn from a single probe row whose
binary predated the candidate commit, while two preserved candidate-window runs
(`target/tpch/adr490_local_admission_debug{,2}`) already showed the local shape
binding and winning. The candidate `faea4b31` was reinstated (`5adb615e`) and the
apparatus was hardened so this failure mode is now structurally impossible.

## What the stale probe hid

The 10:31 probe row (`144a330ac4a17555`, 20,597 ms) ran a binary built before the
admission reader existed; the receipt banked it as a policy decline. The 10:34/10:35
runs on the candidate build had already produced fingerprint `01b38b33108cf032` at
14,972 / 15,166 ms with the standing digest. Two hardening consequences shipped:

- Every perf row now carries `build_commit` (compile-time git stamp, `-dirty` aware)
  and `binary_modified_unix` (mtime of the executing binary).
- Admission is no longer a bare bool: the binder records a per-join
  `LocalHashJoinAdmission` receipt on the optimizer trace, surfaced through
  `bind_with_local_join_admissions` and serialized into every toron perf row as
  `local_join_admissions`, e.g. `admitted(estimate=1440000000,budget=2147483648)`
  or `declined(no_memory_budget)`. A cell can never again *infer* the decision.

## Matched alternating A/B (rebuilt, identity-verified)

Coordinate: 2 GiB task, 2 GiB spill reserve, sf=8, strict, parquet, release,
`build_commit=cca75de6f9c1-dirty` on every row (the dirty delta is the A/B toggle
itself). Arms alternate A,B ×3; the control arm withholds only the planner policy
input (`TORON_TPCH_LOCAL_JOIN_ADMISSION=0` → `declined(no_memory_budget)`), leaving
the execution budget identical.

| arm | fingerprint | execute (3 runs) | median | peak RSS | spill |
|---|---|---|---:|---:|---:|
| exchanged (control) | `144a330ac4a17555` | 21,037 / 20,617 / 20,751 ms | 20,751 ms | ~477 MiB | 88,559,536 B |
| local (admitted) | `01b38b33108cf032` | 15,603 / 14,929 / 15,018 ms | 15,018 ms | ~477 MiB | 88,559,536 B |

**Effect: −27.6% execute median.** All six runs pass with the identical
ArrowIpcDigest and one-row output. Peak RSS is flat — the 1.44 GB conservative
estimate admits under 2 GiB while observed residency barely moves.

The byte-identical whole-query spill across both shapes shows that spill does not
originate from the join exchange the local shape removes; `spill_bytes != 0` was
never a valid admission-failure signal (`spill_scope=whole_query_total`). Attributing
those bytes to their producing operator needs operator-scoped spill telemetry —
deferred.

## Fixes that rode the reinstatement

- Physical probe-linearity: admission previously tested `right_probe_linearity`
  after the SwappedRightBuild operand swap, i.e. the build side. A named
  `physical_probe_linearity` is now selected by placement. (Benign for m06 — both
  sides scan-linear — wrong in general.)
- `gauge_planner_config()` no longer `.ok()`-swallows budget parse errors; a
  malformed `TORON_TPCH_MEMORY_PER_TASK` aborts the bind loudly instead of silently
  disabling the policy input while the row reports an explicit budget.

## Kept rails

- `estimated_hash_join_build_bytes` remains the shared raw estimate; the m06
  `12,000,000 × (112 + 8) = 1,440,000,000` boundary test remains green.
- ADR 489's coordinator clear and three-worker `JoinInputNotExchanged` negative rail
  are untouched; no local shape reaches distributed fragmentation.
- The binder-root architecture bound moved 6,105 → 6,120 for the receipt recording.

## Forward

- Operator-scoped spill attribution for the residual 88.5 MB (lineitem-side or
  aggregate), which is now the dominant known unknown on m06's remaining gap.
- The exchange/materialization regime still owns most of the distance to the
  indexed-SQLite reference; local admission recovers the single-node share of it.
