# ADR 381 - symbol-retaining spill read/copy profile (q03/q07, diagnostic-only)

## Context

ADR 380 proved the time cluster is **I/O/copy-bound, not CPU-bound** (CPU parallelism gave no speedup; the class-level
profile named spill read + copy + alloc + worker coordination) - but the **release binary lacked Rust symbols**, so no
exact Toron frame. **381 is diagnostic-only**: a **symbol-retaining** profile of q03/q07 that names the **exact** spill
read/copy/alloc frame, required **before** any read-path optimization (382). **No engine change.**

## Decision

### Phase 0 - HARD symbol gate (before any sampling)
Symbol retention is **verified, not assumed**. Build `CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false`,
then **gate**:
```
readelf -S target/release/toron-tpch | rg 'debug|symtab'
nm -C target/release/toron-tpch | rg 'read_page|decode_record_batch_ipc|open_in_place|read_all_batch_pages'
```
If the `.debug_*`/`.symtab` sections are absent **or** the spill-read functions don't resolve by name, **STOP and fix the
profile build** - do **not** collect another coarse profile. The 380 no-symbol gap must not recur.

### Phase 1 - profile (373 discipline) + name the exact frame
- Profile **inside the fixed-cap `systemd-run --scope`** (not the wrapper); `perf record -F 99 --call-graph dwarf`,
  bounded **sample window** (`-D` skip startup + `timeout`); q03 and q07 at sf=10 persisted (need not complete).
- **Hybrid-CPU event hygiene:** the ADR 380 sample on this Alder Lake host split samples into `cpu_atom/cycles` and
  `cpu_core/cycles`, and `perf report` defaulted to one event. 381 must either record an explicit event that reports as
  one stream, or run/report all recorded events explicitly. The results doc must state which event stream is used so the
  sample percentages are not accidentally one-core-class-only.
- Render + **`perf report --stdio -g`** to recover the inlined Toron caller (the folded collapse flattens release
  frames - 375 lesson).
- Produce the ranked CPU-cost table with **exact Toron frames**: which of `.to_vec()` (decrypt-output copy,
  spill.rs:337/950/1565) / `decode_record_batch_ipc` / `read_plain_at` (page read) / RecordBatch alloc dominates, with
  sample %, for q03 and q07. That decides 382's lever.

### Phase 1 - non-spill control (artifact guard)
Profile **one non-spill control** (q06 or q14) with the **same build settings**. Its profile must NOT be dominated by
the spill read/copy frames - this proves the spill-read cost is real, not a harness/runtime sampling artifact (startup
hashing, allocator baseline, perf overhead).

### Phase 2 - parallelism side table (not the deliverable)
`TORON_PARTITION_PARALLELISM=1` vs default wall-time + computed K on q03/q07. If default parallelism is neutral/negative
on these I/O-bound cells, record it - a **follow-up** adjusts the engagement policy. The knob stays as-is here.

## Acceptance
- **Symbol gate passed before sampling** (`readelf`/`nm` evidence in the results doc); no coarse re-profile if symbols
  are missing.
- **Exact Toron spill-read/copy/alloc frame named** for q03/q07 (sample %, the dominant of decrypt-copy / IPC-decode /
  page-read / RecordBatch-alloc) - the primary deliverable; **required before 382**.
- **Non-spill control (q06/q14) profiled, same build** - spill-read frames do not dominate it (artifact guard).
- **Profiled inside `--scope`**, bounded window labeled "sample window" not "query runtime".
- **Perf event stream stated** (explicit event or all-events report); no single hybrid CPU event accidentally used as
  the denominator.
- **Parallelism side table** (`=1` vs default + K) recorded with the engagement-policy follow-up if neutral/negative.
- **No engine change**; `--no-fail-fast` green (unchanged); de-branded; no qNN; large `.perf.data` uncommitted.

## Guardrails
- **Symbol retention is a hard gate** - `readelf`/`nm` verify before sampling; stop + fix the build otherwise.
- **Diagnostic-only** - name the frame; the read-path optimization is 382.
- **Non-spill control** - q06/q14 same build; distinguishes spill cost from sampling artifacts.
- **Profile the workload not the wrapper** - perf inside `--scope`; `perf report -g` for inlined callers.
- **Parallelism check is a side table; no engine change; --no-fail-fast; de-branded; no qNN; no committed perf.data.**

## Risks
- **Symbols still absent** - the hard gate stops the cut before a coarse profile; fix `release` profile settings first.
- **Artifact mistaken for spill cost** - the non-spill control catches it.
- **Inlined frame unresolved** - `perf report -g` + the candidate shortlist (decrypt-copy/decode/read/alloc) anchor it.
- **Over-reach into optimization** - 381 names only; 382 reduces.

## Forward arc
381 (exact spill-read/copy/alloc frame named, symbol-verified, control-validated) -> **382 (reduce it: eliminate the
`.to_vec()` decrypt-output copy / decode from the in-place buffer / reuse a read buffer - the 375 write-path discipline
on the read path)** -> re-measure q03/q07 -> the rest of the time cluster. Separately, the parallelism engagement-policy
follow-up if the side table shows net-neutral/negative.
