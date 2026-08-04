# impl 496 — m06 fused join phase ledger (grounding results)

Grounding for the m06 CPU-attribution ADR. No engine decision is made here; this lands the
diagnostic phase ledger and records what it measured.

## The instrument

`TORON_LOCAL_JOIN_PHASE_PROFILE_PATH` arms a process-global accumulator over the fused streaming
inner-join + global (ungrouped) aggregate path — `try_streaming_collapsing_global_aggregate` →
`try_visit_streaming_inner_join_scan_probe_chunks` → selection materialize/fold. It splits the one
fused pass into five buckets and always publishes attributed-vs-total so any un-timed span reads as
an honest residual, not false precision. It writes no wire/cluster struct. The harness folds the
ledger into the single `perf-run-one` row, alongside the identity/receipt fields already there
(`build_commit`, `plan_fingerprint`, `digest`, `output_row_count`, `local_join_admissions`,
`spill_bytes`, `join_spill_observations`).

Path correction found during grounding: m06 is `COUNT(l_orderkey)` with **no GROUP BY** — a scalar
aggregate. It does **not** use the grouped chain path (that is m05's), and it is **not** un-fused
(there is no 48M-row `Vec` of join output). It runs fully fused: each lineitem probe batch is
scanned, keyed+probed against the 12M-row orders build, reoriented-materialized, and folded into the
COUNT before the next batch is pulled.

## Rails verified

- **Off by default, negligible when disabled.** A/B interleaved under identical load: instrumented
  (env unset) median ~4,348 ms vs baseline (pre-instrumentation binary) median ~4,350 ms —
  statistically identical. An earlier apparent gap was pure load drift, ruled out by interleaving.
- **Identical output when disabled.** Same `digest`, `output_row_count=1`, `plan_fingerprint`; no
  `local_join_phase_ledger` field present.
- **Enabled adds ~0 over disabled** (uncontended single-thread lock), so the ledger's
  `total_execute_micros` tracks the true wall.
- **One ledger row per execution** carrying `build_rows`, `probe_rows`, `matched_rows`,
  `drive_completed`, `total_execute_micros`, `attributed_micros`, `unattributed_residual_micros`,
  and the five phases.

## Coordinate

Strict, sf=8, persisted parquet, release, 2 GiB task / 2 GiB spill reserve, 1 warmup + 5 measured.
`build_commit=e96d6a32beaa` (clean). Zero spill, in-memory build (`admitted`,
`estimate=1,440,000,000` ≤ `2,147,483,648`). build_rows=12,000,000, probe_rows=matched_rows=
47,989,007 (every lineitem row matches its order — the FK).

## Attribution (share of wall, medians over 5 runs; ~7.4% residual)

| phase | share | absolute |
|---|---:|---:|
| probe_key (hash lookup + key equality) | ~55% | ~2,090 ms |
| build (hash table from 12M orders) | ~22.7% | ~878 ms |
| probe_scan (lineitem decode) | ~9.8% | ~389 ms |
| output_materialize | 3.9% | ~151 ms |
| aggregate_fold | 1.2% | ~48 ms |
| residual (build-side scan branch + framework) | 7.4% | ~287 ms |

Shares held within ~2 points across all 5 runs. The residual is dominated by the orders build-side
scan on the physical-scan-build branch, which the ledger deliberately leaves un-bucketed rather than
mislabel; the ADR treats it as build-adjacent.

## What the evidence says

The pre-grounding hypothesis — "the probe materializes full join-output batches only to feed a COUNT
that needs no payload" — is **refuted**. Output-materialization is 3.9% and the fold 1.2%; eliding
the payload would recover ~5% at most. The wall is the **Int64 hash-join machinery**: probe_key
(~55%) plus build (~22.7%) ≈ 78% of execution. `output_materialize`'s ~151 ms over 48M rows is
already cheap, and `aggregate_fold`'s ~48 ms confirms the count itself is not the cost.

The deciding ADR opens against `probe_key` — the hash + bucket lookup + key-equality over 48M Int64
probes against a 12M-entry table — with `build` as the secondary target. Before choosing the
optimization, one further decomposition is warranted (hash cost vs cache-missing bucket chase vs
equality), because the fix differs: a specialized Int64 join-key path (parallel to ADR 495's compact
Int64 group path) helps hashing/equality, whereas a memory-bound bucket chase points at layout.
