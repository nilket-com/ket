# ADR 384 - q19 time-wall diagnostic (symbol-retaining, read-count-proven, control-compared)

## Context

q19 is the board's largest anomaly: **33m51s vs DuckDB 161ms (~12,000x)**. 373 (class-level) put it at **50.34% spill
read/decrypt/decompress + 41.61% kernel/system, RSS stable ~406 MB**. Low RSS + high spill-read + high kernel is
**unlike** the q03/q07 spill-floor (hold ~512 MB, process once), so q19 is a **re-read pathology candidate** (the same
spill read repeatedly) - but **that is a claim that needs counted evidence, not a flamegraph-shape inference**. 384 is
**diagnostic-only**: name the exact frame, **prove or disprove repetition with a hard read-count artifact**, classify,
and name the ADR 385 cut. **No engine change** (any instrumentation off-by-default + removed before seal).

## Decision

### Phase 0 - HARD symbol gate (381 discipline, before sampling)
Build `CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false`, then gate:
```
readelf -S target/release/toron-tpch | rg 'debug|symtab'
nm -C target/release/toron-tpch | rg 'read_page|read_batch_page|read_plain_at|decode_record_batch_ipc|probe_partitioned'
```
Absent symbols / unresolved fns -> STOP, fix the build; no coarse profile.

### Phase 1 - profile + the read-count HARD ARTIFACT
- Profile inside `systemd-run --scope` fixed 4 GB, `perf record -F 99 --call-graph dwarf -D <skip-startup>`, bounded
  sample window (q19 won't complete - **sample window**, not runtime), persisted sf=10; `perf report -g` for inlined
  callers. Name the **exact dominant Toron frame**.
- **Repetition is proven, not inferred:** flamegraph shape names the hot stack but cannot prove "same pages read
  repeatedly". Establish read-count with **one of**:
  - `perf stat` / `strace -c` **syscall counts** (e.g. `pread64` count vs unique-page count). Syscall counts alone
    prove read volume; they prove repetition only when paired with a measured unique-page denominator from spill
    metadata/instrumentation, or
  - a **temporary/off-by-default page-read counter keyed by spill page/file id** (reads-per-page; any page read >1 =
    repeated).
  **If neither artifact is obtained, classify as "spill-read dominant, repetition unproven" - NOT re-read pathology.**

### The control table (the discriminator)
| query | dominant hot frame | RSS | spill_bytes | syscall/read pattern (reads vs unique pages) | timeout/window |
| --- | --- | ---: | ---: | --- | --- |
| q19 | (measure) | ~406 MB | (measure) | (counted) | sample window |
| q03 | `RecordBatchDecoder::next_buffer` | (measure) | (measure) | (counted) | - |
| q07 | `take_bytes`/`partition_bucket_*`/`__memmove` | (measure) | (measure) | (counted) | - |

### Classification + the ADR 385 branch
- **Repeated-read (proven):** q19 re-reads the same spill pages -> **385 = re-read elimination cut**.
- **Same floor, larger volume:** q19 dominated by the same decode/once-read frames as q03/q07 -> **385 = spill-format/
  read-path substrate (not query-specific)**.
- **Join-algorithm / OR-branch blowup:** quadratic/non-partitioned probe or the 3 OR-branch predicate evaluation ->
  **385 = the named join/filter algorithm cut**.
- **Spill-read dominant, repetition unproven:** read-count not obtained -> named honestly; 385 starts by obtaining it.
- **Harness/sampling artifact:** validate against a non-spill control.

## Acceptance
- **Symbol gate passed before sampling** (`readelf`/`nm` evidence).
- **Exact q19 dominant Toron frame named.**
- **Read-count hard artifact:** unique-once vs repeated **proven by syscall counts or a per-page-id counter**, or
  explicitly recorded as **repetition unproven** (no inference-only re-read claim).
- **Control table filled** (q19 vs q03/q07: frame, RSS, spill_bytes, read pattern, window).
  The read-pattern column must use the same counted evidence for each comparable row; old q03/q07 flamegraphs may be
  reused for hot-frame context, not for read repetition.
- **Classification + one named ADR 385 cut** per the branch above, with evidence.
- **No engine change** (instrumentation off-by-default, removed before seal); `--no-fail-fast` unchanged; de-branded;
  no qNN; large `.perf.data` uncommitted; sample window labeled.

## Guardrails
- **Read-count is a counted artifact, not a flamegraph inference** - syscall counts or a per-page-id counter; else
  "repetition unproven".
- **Hard symbol gate before sampling** (381).
- **Diagnostic-only** - name + classify + branch 385; no fix here.
- **Control-compare to q03/q07** - distinguish re-read pathology from inherent floor.
- **Profile the workload not the wrapper** (`--scope`); sample window != runtime; instrumentation off-by-default +
  removed; no engine change; --no-fail-fast; de-branded; no qNN.

## Risks
- **Re-read claimed on flamegraph shape alone** -> the hard read-count artifact gates it; "unproven" if absent.
- **Symbols absent** -> hard gate stops; fix the build.
- **Sample window misses steady state** -> delayed `-D` window past startup; steady CPU required.
- **q19 is the inherent floor** -> named (same as q03/q07, larger volume), honest.
- **Harness artifact** -> non-spill control.

## Forward arc
384 (q19 classified with a counted read-count, exact frame, control table, named 385 branch) -> 385 (the branch-named
cut: re-read elimination / spill substrate / join-algorithm) -> rest of the time cluster (q17/q21) -> memory-tail
(q02/q08/q09/q18/q22) -> parallelism engagement-policy follow-up. q19's 12,000x gap is too large to leave unclassified.

## Grounding evidence (file:line)
373 q19: spill read/decrypt/decompress 50.34% + kernel 41.61%, RSS ~406 MB, 33m51s no row. q03/q07 floor signatures
(381). read path: `read_page`/`read_batch_page`/`read_plain_at` spill.rs (the per-page-id counter site). q19 =
discounted-revenue, 3 OR branches (lineitem x part). 381 method: symbol gate + `--scope` + delayed window + `perf
report -g`.
