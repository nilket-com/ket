# ADR 612 - the grouped row loop's cost across fold builds: diagnostic results

Run 2026-09-30 on strix, single-process diagnostics with the host quiet,
at the ADR 608 coordinate. There is no token, no pair, and no latency
claim. Every figure below is diagnostic.

**Closed as limited and incomplete.** The plan's central measure was
per-symbol and per-line CPU attribution **within `row_lookup_update`**.
It was not delivered. The read-out summed `perf` samples over the whole
process, with no selection of the stage's time window or of the
dominant fold's call stack. Stage-window attribution is therefore
**unavailable** in this record. The symbol table below is labelled as
whole-process samples. No sum of it is compared with the stage, no
claim is made about where the stage's increase sits, and no candidate
is ranked. No cut is proposed.

What the record does establish:
- **The ordering check.** Each profiling build's five-run median
  `row_lookup_update`, read from node attribution rather than `perf`,
  keeps the release builds' order, with non-overlapping ranges.
- **One static fact.** `find_bucket_in`'s machine code is identical
  across the three builds.


## Identity

| build | tree | profiling executable SHA-256 | `.text` bytes |
|---|---|---|---|
| ADR 610 control (`8d73e650`) | `0a6f91c85e5b526dd9e9514ab05a8f6457a31732` | `bcfb8540bad7d2d61286b61ba6485291b70a2c6e85aef51229d43da92ad1ba1f` | 30,182,791 |
| ADR 611 control (`1dddc7fe`) | `7ad8356e4191a274e4b71ec85b93beb84e0ed1a8` | `2b83f8adc62d00665b64663d1ea08048000e8ea90a0a721d99dad89c66c11476` | 30,223,111 |
| ADR 611 treatment (`18372d0c`) | `884255bad69420604d6f62d2420ea8f229de0f9b` | `94a99ab1ef3989f6b22cc36ea883f7f28dbd4dc40aadab5de382049ae12882e8` | 30,223,047 |

- **The builds:** profiling builds (release plus `debug = 1`,
  unstripped), made on bigfoot from clean trees with no warnings, and
  hash-verified on strix. The artifacts are under
  `target/tpch/adr612-diagnostic/`.
- **The scripts:** `adr612_run.sh` (SHA-256 `87dc7c58…`) and
  `adr612_read.py` (`952b4bd7…`), with the read-out in `readout.json`
  (`d4ee372f…`).
- **The host:** load 0.18 before the runs; the spill scratch was empty
  afterwards.

## The gates

Every run was gated:
- its digest and plan fingerprint equal ADR 608's sealed q13 cell;
- exactly one `scan_grouped` aggregate invocation;
- the kernel receipt's dominant fold (11,869,667 rows) runs the expected
  shape: row-at-a-time for the two row builds, and two-phase for the
  ADR 611 control.

**All 15 CPU runs and all 3 page-fault runs passed.** None was excluded.

**Peak RSS was not recorded.** A direct `perf-run-one` of the profiling
executable writes `peak_rss_kib: null`: the harness fills it only under
`toron-bench`. The 13 MB difference comes from ADR 611's paired rows,
not from this record.

## The ordering check (node attribution; established)

| build | q13 dominant-fold shape | median `row_lookup_update` ms | five-run range ms |
|---|---|---|---|
| ADR 610 control | row-at-a-time, two-phase dead | 1,531.5 | 1,529.9–1,536.5 |
| ADR 611 control | two-phase | 1,687.5 | 1,678.2–1,847.7 |
| ADR 611 treatment | row-at-a-time, two-phase live | 2,681.3 | 2,664.9–2,692.0 |

The medians keep the release order, and adjacent ranges do not overlap.
These durations come from the node-attribution stage, independent of
the `perf` samples.

## CPU samples: whole-process only

`perf record -F 999 -e cpu-clock`, five runs per build. Samples are
summed by period over the **entire process**, as ms per run. They are
not limited to `row_lookup_update`, nor to the dominant fold. q13 also
has a second, small grouped fold (an `uncovered_generic_fold`
invocation, about 12–19 ms of exclusive time). Whole-process samples
include every stage and both folds. The dataset hash, `stable_file_hash`,
is omitted from the table, but other samples from outside the stage are
not removed.

| leaf symbol (whole process) | ADR 610 control | ADR 611 control | ADR 611 treatment |
|---|---|---|---|
| `group_table::find_bucket_in` | 717.3 | 1,335.1 | 1,590.6 |
| fold function (`_shaped` in ADR 610, `_configured` in ADR 611) | 767.6 | 89.9 | 1,034.8 |
| `fold_kernel::apply_typed_kernel` | — | 263.7 | — |
| `__memmove_avx_unaligned_erms` | 85.9 | 93.1 | 86.1 |

**What these shares can and cannot say.** They show where the whole
process's sampled CPU landed. They are not a decomposition of
`row_lookup_update`, and they are not summed or compared against it.
Without stage-window or call-stack selection, this record cannot say
where the stage's increase lies. That includes whether it lies in the
dominant fold's lookup.

**Why `find_bucket_in`'s share can differ with identical code.** Each
group table seeds its own `ahash` `RandomState` (`RandomState::new()` in
`from_lookup`), and `find_bucket_in` loops once per probe. Different
seeds, collision layouts, or probe lengths can execute a different
number of the same instructions on the same rows. So probe work, and
cache, TLB, or heap-placement effects, are each possible. None is
measured here.

**Source lines.** Whole-process attribution of the fold function's lines
is mostly unattributed inlined code, on line 0: 586 ms/run in the ADR
610 control and 755 ms/run in the ADR 611 treatment. The attributed
lines are the loop's per-row `match state`, at 12.8 ms and 18.2 ms. No
per-batch template or eligibility line appears among the attributed
lines. That is a weak observation under the same whole-process limit.

## Static evidence (clues, not proof)

- **`find_bucket_in`:** out of line in all three builds, 1,704 bytes,
  with an identical address-normalized disassembly hash (`a10ab501…`).
  Its **static codegen does not differ**. How many of its instructions
  execute per lookup (probes) can differ, and was not measured. Its
  start address is 64-byte aligned in both controls and 32 mod 64 in
  the treatment.
- **The fold function:** 23,707 bytes in the ADR 610 control; 35,663
  and 35,622 bytes in the ADR 611 control and treatment, where the
  two-phase code is live.
- `entry_or_init` and `entry_index_or_init` have no separate symbols in
  any build. Whether the hot loop's lookup is inlined into the hot range
  is **unresolved**: the disassembly with DWARF source-address evidence
  the plan requires was not produced.

## Page faults

`perf record -e page-faults -c 1`, one run per build. The table shows
where faults landed in that run, whole-process. It does not show what
caused a peak difference.

| leaf | ADR 610 control | ADR 611 control | ADR 611 treatment |
|---|---|---|---|
| total faults | 85,874 | 84,519 | 86,154 |
| `__memset_avx2_unaligned_erms` | 7,392 | 7,578 | 14,769 |
| `grow_table` | 13,888 | 13,888 | 13,888 |
| `finalize_bound_grouped_aggregate` | 14,044 | 14,043 | 14,043 |
| `build_primitive_key_rows` | 14,062 | 14,062 | 14,062 |
| `GroupKeyStorage::push` | 5,993 | 5,993 | 5,993 |

The treatment's `memset` faults are about 7.2 k pages above the other
builds', with the caller unattributed (no call graph). It is a named,
unresolved difference.

## Candidates: not ranked

The plan asked for evidence per candidate from stage-attributed samples.
Without stage attribution, this record ranks none:
- codegen or inlining of the fold body;
- per-batch template and eligibility work;
- allocation or code paths;
- ADR 610's second scattered pass;
- lookup probe work, or cache, TLB, and placement effects.

It rules out only one thing: a static difference in `find_bucket_in`'s
machine code. ADR 610's second-pass reading stays **unresolved**:
neither supported nor disproved here.

## Consequence

- **Closed as limited and incomplete. No cut is proposed.**
- A follow-up diagnostic, if pursued, needs its own plan and must first
  deliver what this one did not:
  - **stage-attributed samples:** tied to the dominant fold's
    `row_lookup_update` by timestamps, by per-invocation stage windows,
    or by call-stack selection, demonstrated on q13;
  - **per run, with q13's identity:**
    - the existing grouped-fold probe histograms (`hash_probes` and the
      maximum probe length, under
      `TORON_GENERIC_GROUPED_FOLD_PROFILE_DIR`);
    - `perf stat` instruction, cycle, cache, and dTLB counters;
    - a call-graph recording of the `memset` faults;
  - **peak RSS,** recorded through the harness.
- Main is as ADR 611 closed it: `TwoPhase`, `AllTyped`. ADR 610's q13
  regression stays open.
