# LSM Compaction Runbook

ADR: `plans/adr/055_lsm_leveled_compaction_substrate.md`

## When to Use This

Use this runbook when L0 debt grows, read amplification rises after sustained
writes, retired slabs remain pinned, or an LSM compaction invariant alarm fires.

ADR 055 compaction is local to a storage locality. V1 has four compaction
levels, `CompactionLevelIndex::L0` through `L3`. It rewrites live slab data into
fresh slab files, commits a manifest epoch transition, and later lets GC delete
unpinned retired inputs.

## Metrics

Primary throughput and result signals:

```text
wendelon_lsm_compactions_total{level,result}
wendelon_lsm_compaction_input_slabs{level}
wendelon_lsm_compaction_output_bytes{level}
wendelon_lsm_compaction_duration_millis{level}
```

Level pressure and GC signals:

```text
wendelon_lsm_level_slab_count{level}
wendelon_lsm_level_total_bytes{level}
wendelon_lsm_pending_delete_slabs
wendelon_lsm_retired_slab_pinned_bytes
```

Invariant signals:

```text
wendelon_lsm_digest_invariance_violations_total{kind}
```

Labels are intentionally bounded. Do not add tenant, table, subscription,
slab id, job id, path, or error-string labels to `lsm_*` metrics.

## First Checks

1. Check `wendelon_lsm_level_slab_count{level="0"}` and
   `wendelon_lsm_level_total_bytes{level="0"}`. Sustained growth means the
   picker or worker is not keeping up with writes.
2. Compare `wendelon_lsm_compactions_total{result="ok"}` with
   `result="manifest_race"`. Occasional races are expected; a rising rate means
   compaction is repeatedly planning from stale manifest epochs.
3. Inspect `wendelon_lsm_pending_delete_slabs`. A rising value with low retired
   pinned bytes points to local deletion errors. A rising value with high retired
   pinned bytes points to readers or streams holding slab pins.
4. Check `wendelon_lsm_retired_slab_pinned_bytes`. This should drain after
   catchup readers finish and point reads release their guards.
5. If `wendelon_lsm_digest_invariance_violations_total` increments, stop
   treating the issue as capacity pressure and follow the invariant section
   below.

## Result Labels

| Label | Meaning |
|-------|---------|
| `ok` | Compaction committed and advanced the manifest epoch. |
| `manifest_race` | Manifest changed between plan and commit; the next picker tick should replan. |
| `sidecar_digest_mismatch` | Rebuilt sidecar state did not match the cold rebuild, or a sidecar-covered input reached an unimplemented V1 swap path. |
| `visible_digest_invariance` | Visible table-row digest changed across compaction; this is page-worthy. |
| `aborted` | Worker aborted before a more specific V1 reject code was available. |

## Triage

For L0 backlog:

- Confirm the node is the locality leader; non-leaders must not run local
  compaction workers.
- Confirm the worker is not paused by node-local maintenance config.
- Check whether `manifest_race` dominates. If so, reduce competing slab rewrite
  churn before tuning compaction thresholds.
- If compactions are `ok` but L0 still grows, lower `max_l0_slab_count` only
  after measuring write amplification with `benches/lsm_compaction.rs`. In V1
  this threshold is compiled into `LsmConfig`; changing it requires a code
  change rather than a `wendelon.toml` edit.

For pending delete growth:

- If retired pinned bytes are nonzero, identify long-lived reads or CDC catchup
  work. ADR 055 does not add an LSM-specific relocation table; old
  `RecordLocation` users must be protected by pins, existing non-LSM relocation,
  or sidecar replacement.
- If pinned bytes are zero, inspect local filesystem errors for slab unlink
  attempts and verify compaction temp files are not accumulating.
- Do not delete manifest-referenced slab files manually. Let the GC sweep
  transition retired runs to delete-pending and then remove physical files.

For sidecar digest mismatch:

- Treat this as fail-closed. Indexed query planning should fall back rather than
  serving from suspect sidecars.
- Confirm whether the input run had `sidecar_coverage != none`. Until the full
  sidecar rebuild/swap path is wired, such inputs must reject before commit.
- Rebuild secondary-index coverage from retained slabs instead of editing
  catalog or manifest rows by hand.

For visible digest invariance:

- Page on-call. A visible-domain mismatch means compaction would change logical
  table contents.
- Keep compaction paused for the affected locality or table scope until the
  pre/post cold-rebuild inputs are captured.
- Preserve the input slabs, output temp files if present, manifest epoch, and
  audit record before retrying.

## Temp Files and Recovery

Compaction temp files use the `compaction-temp-<job>-slab_<id>.wnd` prefix. On
startup, unreferenced temp files should be removed before serving. Final output
slabs must only become visible after manifest validation and install.

If temp files accumulate while the node is running, check for repeated writer
errors or invariant rejects. Temp files alone are not proof that the manifest
committed.

## Tuning

Tune in this order:

1. Validate no invariant or sidecar mismatch is firing.
2. Check whether worker pause or leadership explains the backlog.
3. Measure picker, survivor-elision, and output-planning costs with
   `cargo bench --bench lsm_compaction -- --quick`.
4. Adjust `max_l0_slab_count`, `lsm_l0_max_age_millis`, and
   `lsm_l1_slab_size_target_bytes` conservatively. These are V1 `LsmConfig`
   defaults, not exposed `wendelon.toml` fields; changing them requires a code
   change. Smaller output targets reduce individual job latency but increase
   manifest and file-count pressure.
5. Keep V1 concurrency at one worker per locality.

## Escalation

Escalate when visible digest invariance fails, sidecar digest mismatches repeat
after coverage rebuild, pending delete bytes grow while no readers are pinned,
or L0 remains above threshold while successful compactions are committing.
