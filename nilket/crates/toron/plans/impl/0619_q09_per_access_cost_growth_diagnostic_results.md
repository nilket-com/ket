# ADR 619 - q09's per-access cost growth: results

Run 2026-09-30 on strix: the event preflight, the counter session, and
the table-size benchmark, each as its own separated systemd unit. It is a
diagnostic: no token, no pair, no cut, and no saving is projected. Each
reading is consistent, inconsistent, or unresolved with respect to the
narrowly stated hypothesis. Neither measurement separates a single
explanation.

**The counters (process-scoped) are consistent with the hypothesis.**
Two of Toron's measured events per visited row are approximately flat
across the scales:
- instructions, 898 → 891 → 888;
- the dTLB-load event, 238 → 236 → 235.

LLC loads per row are not flat: 4.05 → 4.39 → 4.81, about +9% at each
step. Toron's cycles per row rise ×1.12 and then ×1.14, and its cycles
per instruction rise 0.596 → 0.675 → 0.772. Over the same span:
- LLC load misses per row rise ×1.42 and ×1.53;
- dTLB load misses per row rise ×1.57 and ×1.29;
- DuckDB's cycles per row rise only ×1.03 and ×1.04, and its misses per
  row rise at most ×1.12.

These are whole-process counts over the named scope, not stage
attributions, and not query-only miss rates. No stall event was
collected. Rising cycles per instruction and rising LLC and dTLB load
misses are consistent with the memory-hierarchy hypothesis. They are
not a direct stall measurement or a causal decomposition.

**The table-size benchmark (test-only, restricted table API) gives
mixed results:**
- **A similar order, for replay 2's build:** the synthetic two-Int64
  `generic_hash` build shows per-row growth of ×1.20 and ×1.27 across
  the three sizes. q09's replay-2 build grew ×1.12 and ×1.27 per row.
  This supports replay 2's build as a candidate. It does not isolate a
  size-only cause, because:
  - the synthetic table's match to the current bound key is unverified;
  - independent builds realize different hash states and layouts;
  - the q09 comparator is one armed run per scale (ADR 618).
- **Not reproduced, for the Int64 lookups:** in isolation, replay 1's
  observed mixture grows only ×1.03 and ×1.05, and replay 3's successful
  lookups ×1.045 and ×1.03. Inside q09 they grew ×1.21 and ×1.27
  (replay 1) and ×1.21 and ×1.11 (replay 3). **q09's larger Int64
  lookup growth was not reproduced in this restricted benchmark, and the
  reason remains unresolved.** Candidates include:
  - the query context: the streamed `lineitem` batches and the other
    tables;
  - the probe order.

  The limitations: the benchmark's timed scope excludes the query's key
  and window preparation, and it is a test binary with its own
  compilation and code generation.
- **Mean probe slots, for the synthetic Int64 tables and probe sets,
  are approximately unchanged across sizes:** about 1.25 slots per hit
  and 1.62 per miss for the replay-1-sized table, and 1.28 per hit for
  the replay-3-sized table. This does not establish q09's production
  probe distribution, and it does not rule out changes in its tails.
  Generic probe work remains unavailable.


## Identity and method

| item | value |
|---|---|
| plan | `plans/adr/0619_q09_per_access_cost_growth_diagnostic.md` (plan commit `54652855`) |
| implementation | branch `adr619-impl`, commit `dea2417e`, tree `486aba60…`: the test-only module `crates/toron/src/exec/join/build_rows/join_table_size_scaling.rs`, declared `#[cfg(test)]` from `build_rows.rs`. No measured-path code changes |
| measured executable (counters) | ADR 618's `5eadef2d…`, manifest `adr618.manifest` |
| benchmark binary | the release build of `toron`'s lib tests from that tree, `a613f2ba…`, staged and hash-checked |
| ladder (bigfoot, committed tree) | fmt clean; `toron` and `toron-tpch` all suites 0 failed; the three fixtures pass. The one lib-test warning, `adr589_plain_write`, is pre-existing on main |
| scripts | under `target/tpch/adr619-scripts/`: `adr619_common.sh` (`dd34df8d…`), `adr619_preflight.sh` (`fd0a2afa…`), `adr619_select_events.py` (`b97a4584…`), `adr619_counters.sh` (`d9e55e86…`), `adr619_check_row.py` (`6c58ccb5…`), `adr619_duckdb_script.py` (`ce1ea65c…`), `adr619_bench.sh` (`b7a1cd22…`), `adr619_validate.py` (`a7b7832b…`) with offline fixtures `adr619_validate_test.py` (`55e52c1b…`, all passing), `adr619_counters_read.py` (`de2dedaf…`), `adr619_bench_read.py` (`fada7da8…`), and the pre-plan extraction `adr619_pre_extract.py` (`38987e6a…`) |
| read-outs | `adr619-preflight/events_selected.json` (`3a4146c9…`); `adr619-counters/readout.txt` (`2c4a6cff…`), `session.log` (`98259d8f…`); `adr619-bench/readout.txt` (`34c03c4e…`), `bench.jsonl` (`c2d7812d…`), `host.txt` (`f125f866…`) |

**The review record.** Codex held the implementation twice before any
strix run, and the corrections are in the scripts above:
- the benchmark's full libtest name, and an exact-output gate;
- shape validation against the hash-cited receipts, with a refusal
  fixture;
- probing only after validation;
- isolation and whole-tree P-core confinement in every launch;
- rc captured immediately (a `$(basename …)` before `$?` had recorded the
  wrong status);
- verification gates that stop the session;
- event selection computed from the preflight;
- a counted zero treated as valid evidence;
- exact per-run revalidation in the readers.

A first ladder failed two architecture guards: `adr350`, because comments
named the query number, and `adr352`, because the file sat under the
join root. They were fixed by rewording the comments and moving the
module under `build_rows`. The module and test names are semantic, per
the user's preference.

**The replay-2 key: a stated limitation.** ADR 604's structural census
(`structural_census.json`, `208d32e7…`, from an earlier executable)
records the cell's join #2 as `dim=partsupp`, with right keys
`ps_partkey` and `ps_suppkey`, both Int64 in the schema. The
benchmark's generic case is therefore **a synthetic two-Int64-column
`generic_hash` table** that reproduces replay 2's receipts exactly at
all three sizes. It is **not** verified as a match to the current bound
key, and no current-key gate is claimed as passed.


## The event preflight

It ran each `cpu_core` event alone, then the full set of six, around one
SF8 q09 run per engine.
- All 14 launches exited 0, with isolation verified, and the unit's
  stderr is empty.
- The observed mask was 0-15 (`cpu_core` is 0-15, `cpu_atom` 16-31);
  `perf_event_paranoid` is 2, and perf is 7.0.14.
- **Every run was identity-valid.** The Toron rows pass the row checker
  at manifest-only, and the DuckDB outputs pass the fingerprint,
  read-back, and digest checks.
- **All six events were selected:** cycles, instructions, LLC loads and
  misses, and dTLB loads and misses, all user-only. Each counted with a
  positive run time at a 100.00% running fraction on both engines, alone
  and together.

So the session used one simultaneous set, with no splitting.


## The counter session

The sequence was:
1. full-hash verification of all three datasets before and after the
   session, outside `perf stat`: every row passed, with `full_hash` and
   the gated identity;
2. 30 measured runs in the declared balanced order, each after its own
   untimed warm-up of the same engine and scale. Every Toron row was
   revalidated at manifest-only (a named departure), and every DuckDB
   output was revalidated.

**No run was withheld.** Every group has five valid runs, and every event
ran at 100%.

**The scopes:**
- Toron: the `toron-bench` launcher and the staged `toron-tpch`
  process, from startup through the digest.
- DuckDB: the CLI process alone.

The elapsed medians (the launch interval) were:
- Toron: 6.92 / 15.34 / 34.47 s at SF8 / SF16 / SF32;
- DuckDB: 1.81 / 3.51 / 7.12 s.

**Per visited row** (each engine's own counter: Toron's ledger raw rows
69,669,032 / 139,348,665 / 278,720,576; DuckDB's
`OPERATOR_ROWS_SCANNED` 68,069,032 / 136,148,665 / 272,320,576). These
are medians of five runs.

| quantity | Toron SF8 → SF16 → SF32 | ratios | DuckDB SF8 → SF16 → SF32 | ratios |
|---|---|---|---|---|
| instructions | 898.4 → 891.0 → 888.0 | 0.99, 1.00 | 243.8 → 242.5 → 242.4 | 0.99, 1.00 |
| cycles | 535.6 → 601.3 → 685.4 | 1.12, 1.14 | 116.7 → 119.6 → 124.1 | 1.03, 1.04 |
| CPI | 0.596 → 0.675 → 0.772 | | 0.479 → 0.493 → 0.512 | |
| LLC loads | 4.05 → 4.39 → 4.81 | 1.09, 1.09 | 0.83 → 0.95 → 1.14 | 1.14, 1.20 |
| LLC load misses | 0.895 → 1.268 → 1.937 | 1.42, 1.53 | 0.406 → 0.438 → 0.492 | 1.08, 1.12 |
| LLC miss / load | 0.221 → 0.288 → 0.403 | | 0.489 → 0.463 → 0.432 | |
| dTLB loads | 238.0 → 236.1 → 235.3 | 0.99, 1.00 | 56.8 → 56.7 → 56.6 | 1.00, 1.00 |
| dTLB load misses | 1.582 → 2.483 → 3.201 | 1.57, 1.29 | 0.462 → 0.485 → 0.499 | 1.05, 1.03 |

**Reading, consistent with the hypothesis:**
- Toron's instructions and dTLB-load events per row are approximately
  flat, and its LLC loads per row rise about 9% per step.
- Its cycles per instruction rise, and its LLC and dTLB load misses per
  row rise much faster than DuckDB's.
- No stall event was measured.

The counts include startup, manifest-only dataset opening, planning, and
digesting. Their per-row normalization is not a query-only rate, and
nothing here assigns them to a stage.


## The table-size benchmark

- It ran pinned to CPU 4 (a `cpu_core` thread; its sibling CPU 5 was
  read from sysfs), with the observed allowed mask 4.
- The sibling's busy share over the 260.8 s run was 0 of 26,082 jiffies.
- The runner confirmed exactly one test executed and passed. Its output
  passed the exact-set validator: 60 build records, each matching its
  receipt with no refusal, and 300 lookup records with hits plus misses
  equal to probes.
- No process remained.

**The two uncertainty sources, reported separately:**
- **the between-build spread** (independent builds, whose hash state
  and layout differ): at most 0.29 ns/probe for Int64 lookups, and 3–13
  ns for the generic lookups;
- **the within-build range** (repeated probes of one table): at most
  0.27 ns for Int64 lookups, and 0.4–5.2 ns for the generic lookups.

**Medians, per operation:**

| case | keys (SF8 / SF16 / SF32) | lookup ns/probe | ×16/8, ×32/16 | build ns/row | ×16/8, ×32/16 | mean probe slots (hit, miss) |
|---|---|---|---|---|---|---|
| replay 1, observed mixture (5.44% hits) | 87,125 / 173,930 / 348,036 | 9.65 → 9.96 → 10.43 | 1.033, 1.047 | 9.83 → 10.24 → 11.10 | 1.042, 1.084 | (1.25, 1.62) at every size |
| replay 1, successful only | same | 6.85 → 7.41 → 10.03 | 1.082, 1.353 | 9.70 → 10.12 → 11.07 | 1.043, 1.095 | (1.25, none) |
| replay 3, successful only | 12 M / 24 M / 48 M | 31.12 → 32.50 → 33.48 | 1.045, 1.030 | 34.49 → 35.41 → 36.09 | 1.027, 1.019 | (1.28, none) |
| replay 2, synthetic generic | 6.4 M / 12.8 M / 25.6 M | 295.7 → 314.7 → 351.5 | 1.064, 1.117 | 116.7 → 140.0 → 177.7 | 1.200, 1.269 | not observable |

**Build outliers.** The replay-1 observed-mixture case, the first case
the measurement runs, has one build near 22 ns/row at each size (21.75,
22.09, and 22.88), against medians of 9.8–11.1. The other cases have no
such outlier. They are retained, and not explained here.

**In q09, per operation** (ADR 618's armed runs, one per scale, from the
hash-cited extraction):

| operation | SF8 → SF16 → SF32 | ratios |
|---|---|---|
| replay 1 lookup | 9.91 → 11.99 → 15.19 ns per probe | ×1.21, ×1.27 |
| replay 3 lookup | 28.54 → 34.40 → 38.31 ns per probe | ×1.21, ×1.11 |
| replay 2 build | 130.5 → 146.5 → 185.9 ns per row | ×1.12, ×1.27 |

These are stage times divided by operation counts. They are not the
benchmark's operations: they include the query's key preparation and
windows. They are compared only as growth ratios.

**Reading:**
- **Replay 2's build:** the synthetic generic build shows per-row growth
  of a similar order across the three sizes, ×1.20 and ×1.27, against
  q09's ×1.12 and ×1.27. That supports a candidate. It is not an
  isolated size-only cause (the limits are stated above).
- **Int64 lookups: q09's larger growth was not reproduced in this
  restricted benchmark.** In isolation, replay 1's mixture grows ×1.03
  and ×1.05, and replay 3 ×1.045 and ×1.03. The reason remains
  unresolved. Candidates include the query context and the probe order;
  the timed-scope and test-binary limits apply.
  - The **successful-only** replay-1 lookup does grow ×1.35 from SF16 to
    SF32. No mechanism is assigned to it: its table's 25 MB is
    index-accounted bytes, not a measured hot footprint or an effective
    cache capacity.
  - q09's replay 1 is miss-dominated, and the observed-mixture case
    barely grows.
- **Mean probe slots,** for the synthetic Int64 tables and probe sets,
  are approximately unchanged across sizes. That is not q09's
  production probe distribution, and it does not rule out changes in
  its tails.


## Consequence

- **No cut is proposed.**
- The counters make the memory-hierarchy reading the leading candidate
  for q09's scaling. Toron's instructions per row are approximately
  flat, while its cycles per instruction and its LLC and dTLB load
  misses per row rise.
- **Replay 2's generic build** shows per-row growth of a similar order
  in the synthetic table. A replay-2 representation audit is a candidate
  successor, with its own plan and pair at the SF8 coordinate. It keeps
  two separate things apart:
  - the build's heap-allocated key per entry;
  - the probe's per-row key allocation.
- **q09's larger Int64 lookup growth** was not reproduced in this
  restricted benchmark. A successor that isolates it (for example,
  lookups with concurrent streaming, or in `lineitem`'s probe order) is
  its own record.
- The SF8 campaign continues to choose its next owner by absolute cost
  from the sealed board.
