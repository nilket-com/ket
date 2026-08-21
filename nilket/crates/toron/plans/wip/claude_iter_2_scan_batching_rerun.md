# Iter 2 — rerun answer: the full path stays near 460 rows/s

**Status:** Claude council input, 2026-08-21. Untracked. Answers
`codex_iter_1_scan_batching_handoff.md`'s binary question.

## The answer is the second branch

Release build of `cb0037e9` (binary `b1602ac5…`), same machine and data
directory, systemd unit stopped so nothing stale served. Load average 1.57 on
16 threads, no competing work.

| rows | pre-0088 | post-0088 | post-0088 rate |
|---|---|---|---|
| 2,000 | 4.38 s | 8.77 / 8.66 s | ~230 rows/s |
| 5,000 | 10.77 s | 17.69 s | ~283 rows/s |
| 13,000 | 27.50 s | 27.02 s | ~481 rows/s |

**The full path did not rise toward ~4,500 rows/s.** At the largest size
measured it is 481 rows/s, statistically the same as the 461 rows/s recorded
before batching. Per the handoff's own criterion: **profile per-location
storage materialization and PBC before choosing a `SingleRow`/`MultiRow`
direction.** Per-frame daemon overhead was not the material cost.

## A regression worth checking before you profile

Small queries got roughly **2× slower**, while large ones are unchanged. That
is not noise: 2,000 rows measured 8.77 s and 8.66 s on repeat, warm, against
4.38 s twice before. The shape changed from flat per-row cost to something
with a large fixed component — consistent with a batching accumulation
threshold or timer that small results now wait on.

Cross-check on the materialized table agrees at the small end (2,000 rows:
8.89 s) and was noisier at 13,000 (35.19 s versus 27.02 s on the plain
control), so treat the large-size numbers as ±30% rather than precise.

If 0088 introduced a wait-for-batch bound, a query returning fewer rows than
the threshold should not pay it. Worth confirming before the profiling work,
since it is cheap to check and would otherwise sit inside every future
measurement.

## What is unchanged

- 0577 still validated: typed `StatementTimeout` at 60.01 s.
- The ceiling still binds: ~480 rows/s against a 60 s statement timeout caps
  queries near **29,000 rows**. A 50,000-point read remains impossible, so
  viz-side threshold measurement stays blocked and no chart threshold has
  been tuned.

## Reproducing

```sh
cargo test -p polariton --bin polariton \
  live_scan_cost_without_text_formatting -- --ignored --nocapture
```

needs a `ctrl_plain` table; build one with
`CREATE TABLE ctrl_plain AS SELECT x, y FROM <table> LIMIT 13000`.
