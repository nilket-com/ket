# ADR 620 - replay 2's generic join representation: audit results

Run 2026-09-30 on strix, as two separated systemd units: the session (a
bound-plan export and five armed runs), then the benchmark. It is a
diagnostic: no token, no pair, no cut, and no saving is projected.

**The representation is verified** from the current executable's
bound-plan export:
- replay 2 builds `partsupp` (6,400,000 rows) on the right side of the
  chain;
- its key is two columns, `ps_suppkey` then `ps_partkey`, both Int64 and
  non-nullable;
- the join is Inner and not null-safe, so the key plan is forced to
  `Generic`.

**The SF8 envelopes** are medians of five complete runs:
- `prep_table_build`: 789.3 ms;
- `suffix_probe`: 1,228.7 ms, of which payload take is 111.6 ms;
- `cleanup_build`: 556.6 ms.

Build plus cleanup:
- **per run,** 1,339.9–1,352.8 ms, **with a median of 1,345.2 ms**;
- the **sum of the two stage medians** is 1,345.9 ms.

Either is larger than the probe envelope, 1,228.7 ms. **These are
envelopes, not recoverable time.**

**The controlled synthetic comparisons** are observed paired
**differences**, not measured amounts of removed work or a pure ranking
of allocation costs. They were taken at the SF8 shape, paired within
each of five independent repetitions, and all five are positive for
each:

| comparison | median ms | range |
|---|---|---|
| C1, build: owned − inline keys | 198.3 | 192.7–321.0 |
| C2, drop: owned − inline keys | 461.0 | 459.7–474.6 |
| C3, probe: fresh key `Vec` − reused scratch buffer | 126.9 | 109.2–130.1 |

**A provisional candidate is named: O1, build-key storage only.** It
stores the keys inline, fixed-width, for `Generic` plans whose key
columns are all fixed-width and not null-safe. **Both of the successor's
arms keep production's per-probe fresh-key encoding and allocation.**
Only the lookup adapts to the new storage. The successor's pair decides
the real benefit.


## Identity

| item | value |
|---|---|
| plan | `plans/adr/0620_replay_two_generic_representation_audit.md` (`a2bbf300`) |
| implementation | branch `adr620-impl`, `0ccd7d59`, tree `48438736…`: test-only additions to `crates/toron/src/exec/join/build_rows/join_table_size_scaling.rs`. No measured-path code changes |
| measured executable | ADR 618's `5eadef2d…`, manifest `adr618.manifest` |
| benchmark binary | release lib tests `a02b48c9…` |
| ladder (bigfoot, committed tree) | fmt clean; `toron` 46 suites, all passing; `toron-tpch` 0 failing; the four fixtures, including `owned_inline_and_borrowed_key_forms_share_bytes_hash_and_lookups`, pass. The one warning, `adr589_plain_write`, is pre-existing |
| scripts | under `target/tpch/adr620-scripts/`: `adr620_common.sh` (`0d9fddc6…`), `adr620_session.sh` (`36e31704…`), `adr620_census_read.py` (`eb101fbf…`), `adr620_envelopes_read.py` (`6f398730…`), `adr620_bench.sh` (`2afb7e8c…`), `adr620_validate.py` (`8e8f0c69…`), `adr620_bench_read.py` (`7e2da738…`), `adr619_validate.py` (`a7b7832b…`) |
| offline fixtures | `adr620_census_read_test.py` (`c061e265…`), `adr620_envelopes_read_test.py` (`bd4ac464…`), and `adr620_validate_test.py` (`5c5e37ce…`), all passing locally and on strix |
| artifacts | export `structural_census.json` (`0270f12d…`); `representation.json` (`b62b126e…`); `envelopes.txt` (`1fb8c075…`); `session.log` (`8ab82a68…`); `bench.jsonl` (`682de18b…`); the benchmark read-out (`bcb7c9ab…`); `host.txt` (`176169a4…`) |

**The review record.** Codex held the plan twice before its commit:
- **Share transfer:** the plan had applied benchmark fractions to query
  spans. That was removed, and the query spans are named as envelopes.
- **Key evidence:** a truncated exchange-key receipt was replaced by a
  same-source bound-plan export, with the executor's route rule applied
  to it.

It held the implementation twice before any measurement:
- **The validators:** the exact output set (rejecting extras and
  booleans), persisted checksum and shape evidence, strict census and
  envelope gates with negative fixtures, and exact non-boolean integer
  counts.
- **The runner:** it clears all inherited `TORON_*` variables, and
  requires valid sibling counters.

Every correction came before measurement.


## Step 1: the representation

**The export.** The harness's `structural-census` for q09 at SF8 ran
untimed on `5eadef2d…`, anchored to ADR 613's sealed board
(`perf_board.jsonl`, `2f029448…`). It exited 0, with isolation clean,
and wrote `structural_census.json` (`0270f12d…`). Its `source` reads
"explain binder (bind_with_explain_summary), no execution".

**The reader** (`adr620_census_read.py`) applied the reoriented chain's
own mapping, `extract_reoriented_inner_chain` and
`reoriented_chain_continuation`, at the source of `3a782d97`.
**It returned verified, with no reasons:**
- **Plan fingerprint:** `5b3e374cd2d74c4a`, equal to the timed rows' SF8
  fingerprint.
- **Replay 2's join:** Inner, `HashPartitioned`, `match_filter` null, and
  `null_safe_keys` false.
- **Build side:** `left` continues the chain (an Exchange over the join
  with replay 1), so `right` is the build.
- **Build keys:** `right_keys` `[1, 0]` resolve to `ps_suppkey` (Int64,
  non-nullable) and then `ps_partkey` (Int64, non-nullable).
- **Table:** the build scan (`local_table_id` 4, projection `[0, 1, 3]`)
  maps uniquely to `partsupp` through the SF8 manifest, which is bound by
  its fingerprint (`fnv1a64:0fc5c06d…`). That table has 6,400,000 rows.
- **Corroboration:** the export's join-order edge #2 names
  `dim=partsupp` with those key columns.

**Build rows, distinct keys and duplicates** come from the join-build
profile (step 2), identical in all five runs: 6,400,000 build rows,
6,400,000 distinct keys, and 0 duplicate keys.
- **Null exclusions are unavailable.** The static inference is that both
  key columns are non-nullable in the bound schema.


## Step 2: the envelopes

Five armed q09 SF8 runs, with node attribution and the join-build
profile only. **Every run is complete, and none is withheld.** Each run
passed all of:
- the row checks: digest, fingerprint, dataset, coordinate, and
  `full_hash` verification;
- the node tree reconciling;
- a single invocation on `reoriented_inner_chain_grouped`, with ADR 618's
  top-level topology;
- top-level reconciliation and reconciled refinements. Strict status is
  `false` in every run, the known missing replay-2 suffix sub-stages;
- all six replay-2 stages present;
- exactly one generic build record, with the expected counts, equal to
  the representation's table rows;
- exactly one replay-2 probe profile, with 2,610,723 input and matched
  rows and a payload time.

**Per run** (ms):

| run | execute | prep_build | scan decision | input materialize | **table build** | **suffix_probe** | payload take | **cleanup_build** |
|---|---|---|---|---|---|---|---|---|
| 1 | 6,922.8 | 930.7 | 62.4 | 50.1 | 789.3 | 1,225.6 | 111.1 | 555.9 |
| 2 | 6,962.3 | 935.8 | 62.9 | 51.6 | 792.6 | 1,228.7 | 111.6 | 560.3 |
| 3 | 6,944.0 | 932.2 | 64.6 | 51.8 | 787.0 | 1,226.2 | 111.6 | 556.2 |
| 4 | 6,951.2 | 926.5 | 63.4 | 51.4 | 782.9 | 1,229.6 | 112.0 | 557.0 |
| 5 | 6,940.1 | 938.0 | 64.4 | 51.5 | 790.4 | 1,229.9 | 111.2 | 556.6 |

**Medians** [min, max]:

| stage | ms |
|---|---|
| execute | 6,944.0 [6,922.8, 6,962.3] |
| `prep_table_build` | **789.3** [782.9, 792.6] |
| `suffix_probe` | **1,228.7** [1,225.6, 1,229.9] |
| payload take | 111.6 [111.1, 112.0] |
| `cleanup_build` | **556.6** [555.9, 560.3] |

The profile's `build_micros` agrees with `prep_table_build` within 1
microsecond in every run (runs 1, 2, and 4 differ by 1 µs). That is
consistent with two timer readings of the same interval.

**Context.** The execute medians are 188.8 ms above ADR 618's timed
median (range +167.6 to +207.1). That is not apportioned.

**Envelopes, not recoverable time:**
- `cleanup_build` encloses the runtime's cleanup;
- `suffix_probe` includes selection, match expansion, output, and gaps
  beyond the unrecorded lookups.


## Step 3: the controlled synthetic comparisons

The test-only benchmark ran at the SF8 shape: 6,400,000 two-Int64 keys,
with the production table validated against the SF8 receipt in every
repetition, and 2,610,723 all-present probes.
- It was pinned to CPU 4, a `cpu_core` thread whose sibling CPU 5 was
  read from sysfs, with the observed mask 4.
- The sibling was busy for 0 of 8,998 jiffies.
- Exactly one test was executed, in 89.87 s.
- The output passed the exact-set validator: 135 records, every lookup's
  checksum equal to its expected value, and no refusal.
- **All three tables coexist during the lookups,** unlike a sole
  production table in a query.

**The fixture proves parity:** identical encoded bytes; identical owned,
inline, and borrowed hashes under one seed; and exact counts for
successful and absent borrowed lookups on both prototype tables. Each
paired build clones one hash state.

**Per variant** (median, and range over the five repetitions or the 25
passes):

| variant | ms | range |
|---|---|---|
| build: owned `Vec` keys (prototype) | 651.4 | 648.6–773.1 |
| build: inline `[u8; 18]` keys (prototype) | 454.8 | 452.1–456.6 |
| build: production | 745.4 | 733.2–844.1 |
| drop: owned | 467.5 | 465.7–480.4 |
| drop: inline | 6.1 | 5.6–7.1 |
| drop: production | 566.8 | 565.2–567.7 |
| lookup: fresh `Vec` on the owned table | 753.2 (288.5 ns/probe) | 736.4–755.6 |
| lookup: scratch on the owned table | 627.3 (240.3 ns/probe) | 620.7–663.4 |
| lookup: scratch on the inline table | 338.8 (129.8 ns/probe) | 337.7–352.5 |
| lookup: production | 777.5 (297.8 ns/probe) | 755.8–778.9 |

**The variance sources.** The between-repetition spread (independent
builds, whose hash states and layouts differ) is 1.1–17.1 ms for the
lookups. The within-repetition median range is 1.3–26.7 ms.

**The paired comparisons**, with all five pairs positive for each:

| comparison | ms | range | what differs, and what remains |
|---|---|---|---|
| **C1** build: owned − inline | **198.3** | 192.7–321.0 | a heap key per row and entry; the inline build also copies from a reused scratch buffer. Entry size and layout remain |
| **C2** drop: owned − inline | **461.0** | 459.7–474.6 | freeing 6.4 M per-entry key buffers (the inline drop is 6.1 ms) |
| **C3** probe: fresh − scratch, on the owned table | **126.9** | 109.2–130.1 | per-probe buffer allocation, reallocation, and free work. The table and lookup are identical |
| **Layout, for context:** owned − inline, both scratch | 287.8 | 284.0–289.3 | the stored key's indirection and layout at lookup |

**The production reference, descriptive only.** Production has an
independent hash state and a different value and key representation
(`JoinKey::Generic` and `BuildRowList`, not `u32`). Its distance from
the owned prototype is:
- build: +89.3 ms;
- drop: +99.2 ms;
- probe: +24.3 ms.

These are descriptive differences between different representations,
with independent hash states. They are not a seed-controlled difference,
an allocation decomposition, or a measure of the prototypes' fidelity.


## Step 4: the choice

**The ranking, stated side by side:**
- **The observed absolute SF8 envelopes:** build plus cleanup has a
  per-run median of 1,345.2 ms, and its stage medians sum to 1,345.9 ms.
  The probe is 1,228.7 ms. The build side is the larger either way.
  These are envelopes, not recoverable time.
- **The observed synthetic paired differences,** which are not
  transferred to the query:
  - for build-key storage, C1 is 198.3 ms and C2 is 461.0 ms. Their sum,
    659.3 ms, is a sum of two separate paired-difference medians, not one
    measurement. C1 also changes layout and the encoding
    implementation, and C2 compares whole-table drops across two layouts
    as well as with and without per-entry buffers;
  - for per-probe allocation, C3 is 126.9 ms.
  - The two target different operations: insertion and freeing, against
    probe encoding.
- **Feasibility.** O1, as scoped here, changes only the `Generic`
  table's key storage, for plans whose key columns are all fixed-width
  and not null-safe:
  - The probe path keeps production's per-row `join_key_bytes`
    encoding and its fresh allocation.
  - Only the lookup adapts: the probe's encoded bytes are compared
    against the inline stored key.
  - The inline layout may still change lookup cost (the layout
    comparison was 287.8 ms). The pair measures that, and it is not
    described as an allocation decomposition.
  - O2 (scratch or stack probe encoding) and O3 (a dedicated two-Int64
    plan) are left for later records.
- **Uncertainty.**
  - The prototypes are synthetic, with `u32` values and three coexisting
    tables.
  - The allocator behaves differently in the query.
  - The envelopes are not recoverable time.

**The verdict: provisional candidate O1, build-key storage only.** On
these grounds:
- the key is verified;
- the build-plus-cleanup envelope is the larger;
- as scoped, O1 changes only the build's key storage. Both arms keep
  the per-probe fresh-key encoding and allocation, so it does not
  overlap with O2.

Nothing is projected. **The successor record measures this narrowly
specified storage change with a controlled pair** at ADR 613's
coordinate, q09 deciding by ADR 588's rule, with its own plan.


## Consequence

- **No cut in this record.**
- **The successor:** an O1 plan (build-key storage only, the probe
  unchanged), one token, and a q09 pair. It follows
  the plan/implementation cadence, with its own identity gates and
  breadth guard.
- The SF8 campaign's owner choice remains by absolute cost.
