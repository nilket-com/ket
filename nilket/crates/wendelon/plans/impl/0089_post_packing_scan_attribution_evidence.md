# ADR 0089 — post-packing scan attribution

Where the scan spends its time *after* locality-prefix packing and read
coalescing. ADR 0089 moved the per-row budget by roughly 150x, and a 150x move
retires the profile that justified it: the pre-0089 attribution measured a
2.08 ms/row budget, so every component it excluded "by measurement" at under one
percent was excluded against a budget 150 times larger than today's. A component
that was 0.3% then can be 45% now. None of those exclusions may be carried
forward, and this run replaces them rather than refining them.

## Apparatus

Isolated single-node deployment — never the shared local one, whose contention
would land inside the measured window. Own config, own PKI, data dir on real
ext4 (not tmpfs: `O_DIRECT` reads do not amortize across repeats, and the medium
alone is worth ~1.7x). Ports 7021 / 7451-7453 / 7762.

Binary `nilket` at `87a05eb9` plus the working-tree phase-trace instrumentation,
release, built `--features toron/otlp-http`. The OTLP feature is not optional
plumbing: the Wendelon phase trace is gated on a *sampled* table-query carrier,
Toron only mints one when `telemetry::enabled()` and head sampling fires, and
that path is feature-gated. `sampling_rate_per_million = 1000000` with the
exporter aimed at a dead endpoint — the carrier bit is the point, not a
collector, and export failures are warned and dropped.

Driver: `live_post_packing_scan_attribution` (polariton adapter). Materializes
through the terminal `ket` route, so the fixture carries real ADR 0090 partition
stamps and therefore real ADR 0089 packed records, then takes exactly one full
scan per cardinality — one sampled query, one trace line, no ambiguity about
which line belongs to which read.

## Premise: the scan is in the packed regime

Checked every run, never inferred. If this fails the run measures a different
engine than the one the record is about.

| rows | distinct_records | coalesced_hits | max_resident |
|---|---|---|---|
| 1,000 | 1 | 999 | 1 |
| 13,000 | 4 | 12,996 | 4 |
| 200,000 | 49 | 199,951 | 8 |

`distinct_records == ceil(rows / 4096)` at every rung, `decodes + hits == rows`
at every rung. The 13,000 rung reproduces the `distinct_records = 4` this ADR
banked.

## The attribution

200,000 rows, fresh deployment, `evidence`-profile client, quiet machine. Phases
sum to 461.7 ms against a 535.1 ms client wall — **86.3% of the wall is inside
the scan loop**, so the owner is here and not in transport or the client.

| phase | ns/row | share |
|---|---|---|
| materialize | 475 | 20.6% |
| descriptor match | 425 | 18.4% |
| index entries | 326 | 14.1% |
| accumulator push | 315 | 13.7% |
| projected read | 221 | 9.6% |
| physical read | 173 | 7.5% |
| PBC | 139 | 6.0% |
| stats prune | 74 | 3.2% |
| synthetic probe | 68 | 2.9% |
| decode | 43 | 1.9% |
| placement | 29 | 1.3% |
| cache lookup | 20 | 0.9% |
| **total** | **2,308** | |

**Everything this ADR amortized is now 10.3% combined** (physical read, decode,
cache lookup). That answers the question the original Forward asked: splitting
physical read from decrypt is chasing at most 7.5%, and it is not where the
remaining budget lives.

## The largest single mechanical defect

`descriptor match` is 18.4%, and it is almost entirely avoidable.
`table_record_matches_descriptor` opens with

```rust
if batch.schema().as_ref() != &descriptor.arrow_schema()? {
```

and `arrow_schema()` is `decode_arrow_schema_ipc(&self.arrow_schema_ipc)` — it
**decodes an Arrow IPC schema blob out of bytes on every call**. A 200,000-row
scan performs 200,000 IPC schema decodes of a value that is invariant across the
entire loop.

The same function opens with `batch.num_rows() != 1`. That guard is why the
whole tail is structurally per-row: the descriptor check cannot see a record, so
the record must be slivered before it is reached.

## Control: is `accumulator push` server work or client backpressure?

`ScanPhase::AccumulatorPush` wraps `response_batches.push(...).await`, which is
exactly where a slow consumer's backpressure would land and be misread as server
cost. The banked "an optimized client makes zero difference" result was measured
at 481 rows/s and expires by the same argument as every other pre-0089
exclusion, so it was re-run rather than inherited.

Both arms back to back on one quiet machine, same deployment, 200,000 rows:

| phase (micros) | debug client | `evidence` client | delta |
|---|---|---|---|
| accumulator push | 71,558 | 72,140 | +0.8% |
| materialize | 101,354 | 102,096 | +0.7% |
| descriptor match | 92,226 | 92,717 | +0.5% |
| index entries | 93,050 | 92,541 | −0.5% |
| scan wall (ms) | 598.2 | 594.4 | −0.6% |

The control could have failed and did not: client optimization level moves no
phase outside noise. `accumulator push` is genuinely server-side.

## `index entries` is state-dependent, and the others are not

Across five runs on one deployment that accumulated dropped-but-not-yet-reclaimed
fixtures (ADR 0095 reclaims asynchronously), `index entries` rose 65,299 →
93,050 micros (+43%) while `materialize` moved +5.6% and `descriptor match`
+7.7%. Wiping the data dir returned it to 65,299.

So `locality_candidates_for_tenant` scales with accumulated tenant index state,
not just with rows returned. Its 14.1% share above is the *floor*, measured on a
clean deployment; a long-lived tenant pays more. Any future run that quotes this
phase must say what state the deployment was in.

## Open, and not to be quoted until run

This run reads 200,000 rows at ~374,000 rows/s and 13,000 at ~328,000 rows/s,
reproducibly across fresh deployments. The record banks the 13,000 rung at
0.18 s (~72,000 rows/s), and the viz heavy-churn exhibit reports ~72,000-76,000
rows/s flat. **This run is 3.5-5x faster than both, and that gap is unexplained.**

Untested candidates, in no order: a fresh deployment against a shared one
carrying accumulated LSM and MVCC state; this two-`Float64` `ket.range()`
fixture against `ctrl_plain`; `87a05eb9` against `9e773b28`, which spans ADRs
0090-0096 and the staged maintenance publication. Separately, viz's figure is
"read + prepare" and bundles `create_authored_plot`, so it was never purely a
scan rate.

Until one run settles this, neither ~76,000 nor ~374,000 rows/s should be quoted
as the scan ceiling. The attribution above is unaffected — it is a decomposition
of this run's own wall, and every share is internally consistent with it.

`projected read` at 9.6% is most likely work spent deciding *not* to take the
projected path (`read_projected_record` performs a real read attempt before
returning `None`), inferred from the materialize arithmetic rather than measured.
It needs a counter before anyone acts on it.

## After the cut: the descriptor schema hoisted out of the loop

The schema is now decoded once per query and handed to the matcher; callers that
cannot supply one (CDC, aggregate, point read) pass `None` and keep the previous
behavior byte for byte. Same apparatus throughout: same fixture, same
`evidence`-profile client binary, fresh data dir per run, quiet machine. Only
the daemon binary changed (`3ca38ae4` -> `996ed943`).

200,000 rows. Pre-cut is the fresh-deployment run above; two post-cut runs on
separate fresh deployments.

| phase (micros) | pre | post 1 | post 2 | change |
|---|---|---|---|---|
| **descriptor match** | **85,006** | **6,770** | **6,560** | **−92%** |
| materialize | 94,929 | 97,380 | 94,634 | −0.3% |
| index entries | 65,299 | 62,623 | 60,434 | −7.4% |
| accumulator push | 63,077 | 66,661 | 64,066 | +1.6% |
| projected read | 44,169 | 43,946 | 43,896 | −0.6% |
| physical read | 34,564 | 22,856 | 24,499 | −29% |
| PBC | 27,702 | 31,197 | 30,519 | +10% |
| stats prune | 14,842 | 16,200 | 15,091 | +1.7% |
| synthetic probe | 13,554 | 14,393 | 13,673 | +0.9% |
| decode | 8,638 | 9,347 | 9,634 | +12% |
| placement | 5,873 | 7,211 | 6,264 | +6.7% |
| cache lookup | 4,018 | 4,258 | 4,066 | +1.2% |
| **scan-loop total** | **461,671** | **382,842** | **373,336** | **−17% / −19%** |

Client wall 535.1 -> 451.2 / 441.5 ms. Scan rate 373,750 -> 443,292 / 453,026
rows/s, **+19% / +21%**.

`descriptor match` falls 425 -> 34 ns/row. The residual is the structural
`Schema` equality still performed per row plus the identity-policy arm; it is no
longer a decode.

### The control is the phases that should not have moved

This is one binary against another, so the untouched phases carry the
attribution: if they moved as much as the treated one, the run would be
measuring the machine rather than the change. They did not. `materialize` moved
−0.3% and `projected read` −0.6% against `descriptor match`'s −92% — more than
two orders of magnitude apart. `physical read` (−29%), `decode` (+12%) and
`PBC` (+10%) are the small or IO-bound phases whose pre-cut run-to-run spread
was already this wide; none is large enough in absolute terms to account for the
total.

The premise held across both runs: `distinct_records = 49`,
`coalesced_hits = 199,951`, unchanged. The scan is still in the packed regime,
so this is the same measurement, not a different one.

### What this does not settle

The 3.5-5x gap against the banked ~72,000-76,000 rows/s is untouched by this cut
and remains open — both arms above sit on the fast side of it. The cut is
attributed against its own baseline, which is the only claim it needs.

Next largest, on the post-cut profile: `materialize` at 25% and
`accumulator push` at 17% of a 1,867 ns/row scan. `materialize` is the one the
`batch.num_rows() != 1` guard in the matcher still forces to stay per-row.
