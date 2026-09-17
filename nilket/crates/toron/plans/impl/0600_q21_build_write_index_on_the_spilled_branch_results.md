# ADR 600 - q21's replay-1 build write: the compact resident index on the spilled branch: results

Measured 2026-09-16 on strix: one paired attempt under ADR 595's
separated launch, ADR 597's twelve-row driver, and ADR 599's declared
gate procedure (a 5 s wait between the launch environment's admission
and the gate's reading, threshold and load bound unchanged). The gate
admitted; twelve rows ran; every row's q21 digest equals the sealed
digest. **Verdict, by ADR 588's rule as ADR 597 fixed it: `retained`.**
Every pair was positive — gains of 532.5, 525.1, 599.9, 559.7, and
609.8 ms, median **559.7 ms (6.23% of the control median)** — against
a bar of 191.8 ms (2 × the 95.9 ms control range). The one call stays
in the closing tree. The mechanism check on the treatment (ADR 596's
driver, after the verdict) shows the index the cut built: `index_mode
= compact`, `index_variant = int64`, 5,845,938 entries, resident peak
268,506,120 bytes, the index child at 303.9 median-scaled ms and
`inner_1_build_write` at 688.7 — beside ADR 596's 482.4 / 879.6 and
ADR 599's 489.6 / 778.5, historical readings from other executables.
The lookup counts the record asked for are **absent** on this check:
the counter's reporting half was on a handler the measured chain does
not route through; the closing tree carries the fix and a structural
fixture, and the counts themselves remain unmeasured under this record.

## Identity

| identity | value |
|---|---|
| control executable | `fe5825bd38efd8f86bdfac4b4bf51978bcd732f04cc445d66ce2baf7a7b5b1d0`, tree `5d03ac4269e8e0f47996fcb5d093c0d9e330e001`, commit `1f1fa967` (tag `artifact/adr600-control`: this record's facts, counters, and fixtures; the spilled branch as it was) |
| treatment executable | `ad888863ab3f7f2d209821276b6496ecb0dab8de194a49c39d1d60f9b9d35728`, tree `f9f92866997d1f234f52d2fd2105eaceb1cbf4d9`, commit `950e42d7` (tag `artifact/adr600-treatment`: the control tree plus the one call `spilled.use_compact_resident_index()` in the spilled branch of `prepare_reoriented_inner_join_runtime_with_scan_build` — 2 insertions in one hunk of `exec/join/streaming.rs`, one of them the call) |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580`, both arms; ADR 588's manifest gate admitted the pair (same recipe, different trees, different executables, no untracked inputs) |
| closing tree | the treatment tree with this document, the ADR's status, the call site's comment, the lookup publication as one collection from both handlers, the three-valued reading, and their fixtures; not re-measured under this record |
| launcher | `toron-bench` `23267312…` (ADR 598's, unchanged since), which verified 99,411,432 bytes in 45,658 µs under mask `0-31` |
| sealed baseline (bound by content) | `950ebc4b…` on tree `a962f3d8…`, artifact `d03935eb…`, q21 median 9,126.9 ms, affinity `0-15`, dataset `fnv1a64:0fc5c06d19c5ed23` |
| driver | `adr597-q21-exchange-skip` (ADR 597's two-executable driver, by reference; the treatment is the launching manifest) with `--gate-settle-seconds 5`; unit `adr600-compact-index.service` under `user@1002.service/app.slice`, `Linger=yes`; `Type=exec`, `RuntimeMaxSec=10min`, `TimeoutStopSec=30s`, `KillMode=control-group`; timer `OnActiveSec=120`, `AccuracySec=1s` |
| build host / run host | bigfoot / strix |

## The launch, the gate, and the window

| instant (CDT) | what |
|---|---|
| 22:32:50 | preflight: the completion hook's datagram from strix reached the workstation's listener (+10 ms) |
| 22:34:41.117 | `ADR595_LAUNCH_UNIX_MS`, launch session 2343, token `5fd777c5ddcf07c8482d70d7`, delay 120 s; the session exited (logout 22:34:41.161) |
| 22:34:51.96–52.05 | an operator-side check session (2344) read the timer's state — active, next elapse in 1 min 49 s, `AccuracyUSec=1s` — because the launch record's own `systemctl show` had run before the transient units were registered and printed an empty next-elapse and the 1 min default; the session closed 110 s before the trigger, inside the separation interval and outside the window; not part of the procedure, recorded here |
| 22:36:41.000 | timer trigger; the launcher's hash ended at 22:36:41.955 |
| 22:36:41.957 | `at_entry`: **46 °C**, one P-core at 5.70 GHz, 2 ms after the hash's end; then the scope and the launch environment's admission (driver start 22:36:41.966) |
| 22:36:41.97–22:36:46.97 | the declared wait: requested 5 s, performed, elapsed 5,000.2 ms on the monotonic clock |
| 22:36:46.966 | `at_gate`, the first act after the wait and the one reading judged: **38 °C**, every masked core at 800 MHz but core 8 at 1.30 GHz, load 0.00 against the 0.3 bound — **admitted**; `hash_end_to_gate_ms` 5,011.0 |
| 22:36:49.431 | first child's launch (the control's warmup); separation margin 128.3 s from the launcher's logout |
| 22:39:03.140 | last child's return; journal collected `collected`, 19 entries: session 2343's close, anacron's run, session 2344's open and close, one `rtw88` and one wireless `UFW BLOCK` entry in the separation interval; one `UFW BLOCK` (22:37:50, during pair 1's control child) and no session event in the window — **`verified` / `verified`** |
| 22:39:05.179 | completion hook: `result=success exit_code=exited exit_status=0`; datagram received at +4 ms |
| 22:39:16 | read-out session 2345; unit and timer inactive; spill scratch empty; package 42 °C, load 0.87 |

The entry reading was above the threshold (46 °C) and the gate reading
after the wait below it (38 °C); both are observations, as ADR 599
reported them, with nothing established about why they differ.

## The pairs

Warmups: control 8,925.2 ms, treatment 8,389.8 ms. Every row `pass`,
digest `ArrowIpcDigest([253, 211, 102, 221, …])` equal to the sealed
digest on all twelve, every row launched from its arm's executable and
validated as it returned.

| pair | order | control ms | treatment ms | gain ms | gain % |
|---:|---|---:|---:|---:|---:|
| 0 | C,T | 8,928.9 | 8,396.4 | 532.5 | 5.96 |
| 1 | T,C | 8,941.7 | 8,416.6 | 525.1 | 5.87 |
| 2 | C,T | 9,024.7 | 8,424.8 | 599.9 | 6.65 |
| 3 | T,C | 8,979.5 | 8,419.8 | 559.7 | 6.23 |
| 4 | C,T | 8,984.6 | 8,374.7 | 609.8 | 6.79 |

Control median **8,979.5 ms**, control range **95.9 ms** (8,928.9 to
9,024.7; the cutoff ratio 0.0107, within the 4% bound), treatment
median 8,416.6 ms. **Bar `2 × 95.9 = 191.8 ms`; median gain 559.7 ms;
every pair positive; `retained`.** The control median sits 147.4 ms
(−1.61%) from the sealed q21 median and 7.3 ms (−0.08%) from ADR 596's
off median, observed distances. Readings in run order: package
83–90 °C at the children's start readings, 68–85 °C at end;
involuntary switches 60–87 and voluntary 11,728–11,733 on both arms;
inter-child gaps 4.0–6.8 ms. The children's peak RSS, as the harness
reads it on every row: control 1,391,664–1,398,756 KiB, treatment
1,013,624–1,021,876 KiB — measured on both arms, reported, not netted
against the ledger's accounting below.

## The mechanism check, after the verdict

ADR 596's driver (`adr596-q21-build-write`, its own off/on arms) on the
treatment executable, launched under the same procedure at 22:39:56
(session 2351, token `bce3d2cb…`; entry 38 °C with one P-core at
5.70 GHz, gate 38 °C after the 5,000.2 ms wait, load 0.27 — admitted),
eleven rows, separation 127.8 s and window `verified` (one wireless
`UFW BLOCK` entry, no session event), off median 8,405.7 ms, off range
104.7 ms (1.25%), on median 8,575.6 ms (perturbation 2.02%, accepted),
sixth and seventh bands consistent on every pair, residuals complete,
`facts_vintage adr597`:

| fact, treatment (measured) | value | ADR 599 treatment (pass-through, historical) | ADR 596 (split, historical) |
|---|---:|---:|---:|
| exchange handling / partitions | split / 4 | pass_through / 4 | split / 4 |
| batches read / written | 1,465 / 5,860 | 1,465 / 1,465 | 1,465 / 5,860 |
| chunks / pages | 5,860 / 5,860 | 1,465 / 1,465 | 5,860 / 5,860 |
| plaintext / ciphertext bytes | 27,148,512 / 27,242,272 | 24,301,512 / 24,324,952 | 27,148,512 / 27,242,272 |
| index entries, mode, **variant** | 5,845,938, **compact, int64** | 5,845,938, legacy, — | 5,845,938, legacy, — |
| `resident_state_peak_bytes` | **268,506,120** | 514,525,240 | not recorded |
| `writer_peak_bytes` | 5,128 | 17,160 | not recorded |
| `scratch_plaintext_capacity_peak` | 8,960 B | 33,152 B | not recorded |
| `scratch_refs_capacity_peak` | 1,930 entries | 7,962 entries | not recorded |
| `scratch_releases` | 0 | 0 | not recorded |
| replay 1 `lookups_counted` / hits / misses | **absent** (the check's receipt wrote `false`, corrected below) | — | — |

The resident peak is consistent with the wrapper's accounting from
obtained capacities: 2²⁴ buckets × 4 B and 2²³ entries × 24 B are
268,435,456 B, and the remaining 70,664 B is the overflow heap, the
reserve, and the plaintext peak as the guard sums them. The peak on
the same branch under the legacy map, measured on ADR 599's treatment
executable on another run, was 514,525,240 B; this record's control
arm ran unprofiled, so its peak was not measured. Every other row of
the table is what the ledger recorded on this check; the pass-through
column is a different cut on a different executable, reported as
context.

Terms, median-scaled ms, treatment against ADR 596's split and ADR
599's pass-through (historical, other runs, never netted):
`inner_1_build_write` **688.7** (879.6; 778.5); `build_write_index`
**303.9** (482.4; 489.6); `build_write_scan_wait` 139.1 (179.0; 178.1;
open 0.23, poll 138.9); `build_write_repartition` 64.9 (split; 0 under
pass-through); `build_write_finish` 53.9 (13.0; 13.4); `build_write_encode`
50.6 (51.4; 42.0); `build_write_page_write` 31.6 (33.7; 18.7);
`build_write_key_extract` 28.5 (28.4; 31.5); `build_write_residual`
14.2 (14.0; 4.5); `build_write_create` 0.3; filter and project 0. The
finish child reads 12.9–72.4 ms raw across the five pairs, a wider
spread than the other children's; a reading, not attributed here. The sixth band's other
children as before (partition sizing 264.2, sizing scan 198.1). ADR
594's rule on these rows resolves `inner_1_build_write` (688.7 ms,
8.19% of the off median) and ADR 596's returns
`below_floor_no_mechanism` with the index at 303.9 against a 500 ms
floor.

**The absent lookup counts.** The record specified the counter's
lifecycle: enabled under the lifecycle handle, counted in `lookup`,
published at cleanup per replay. The enabling call sits in the shared
`prepare_reoriented_inner_join_runtime_with_scan_build` and ran (the
same handle carried `inner_1_facts` and `build_write_facts` onto every
row); the publication sat in the cleanup loop of
`try_streaming_reoriented_inner_chain_grouped_aggregate` only, and the
measured chain routes `inner_prefix_semi_anti_grouped`, whose cleanup
loop published nothing. The receipt therefore carries no
`inner_replay_lookups` diagnostic. The receipt as built then read that
absence as `inner_1_lookups_counted: false` with null hits and misses
— which conflated an unpublished counter with a disabled one, and this
run is the case that shows the difference: counting was enabled,
publication was missing. The closing tree corrects both halves.
Publication: the handlers publish one collection
(`{"replays": [{replay_id, counted, hits, misses}, …]}`) through
`publish_lookup_counts(handle, &runtimes)` ahead of the cleanup loop in
both handlers that open a `cleanup` stage — one value, because the
ledger keeps one diagnostic per name and a replay-at-a-time publication
would have left only the last spilled replay (the first fix published
that way). Reading: `inner_1_lookups_counted` is three-valued — `true`
with the counts, `false` when the build reported counting disabled,
`null` when no publication carried replay 1 (unknown, which is not
disabled) — and the counts are null unless counted. Fixtures: two real
spilled runtimes of one chain (replay 1 counting through its own
lookups, replay 2 never enabled) published as the handlers publish
them, attached through the ledger's own by-name method, and read
through the real receipt as replay 1's counts; the same runtimes
published a replay at a time read as unpublished; the receipt on
counted-with-zero-lookups, disabled, unpublished, and a publication
without replay 1; and the structural check that every handler opening
a `cleanup` stage publishes before it cleans up. The counts on this
route are unmeasured under this record; the original mechanism
evidence stands with this limitation, and this record launches nothing
further.

## Implementation and verification

Engine: `SpilledBatchIndex::variant_name`, `index_variant` on the
ledger (set after every page from the build's own answer, `none` when
no page was written) and on the receipt as `build_write_index_variant`
with the variant-vs-mode rule (`legacy` ⇒ `hash`; `compact` ⇒ `hash`,
`int32`, or `int64`; `none` ⇒ `none`); `enable_lookup_counting` /
`lookup_counts` on `SpilledInnerJoinBuild`, hits and misses counted in
`lookup` only when enabled, enabled on the profiled arm after the
build's creation; the `inner_replay_lookups` diagnostic and the sixth
band's `inner_1_lookups_counted` (three-valued) / `inner_1_lookup_hits` /
`inner_1_lookup_misses` from replay 1's entry in the collection; `build_write_index_variant`
in the pair bands and the three lookup fields in the sixth band of
`toron-tpch`'s ADR 594/596 evaluators. Fixtures: parity legacy against
compact on every variant — `Int64`, `Int32` (the probe key typed like
the build key, which is what selects the primitive plan), and `Utf8`
(the `Hash` variant), nulls under both null-safety settings (indexed
on neither under the non-null-safe plan, on both under the null-safe
one), repeated keys within and across batches (at least 50 per
variant), an absent key, growth across pages, each asserting the
variant the build reports; the accounting fixture (accounted bytes =
bucket length × 4 + entry capacity × 24 + the overflow heap, over
17,000 keys, buckets a power of two at least twice the keys) with
cleanup on a budget refusal; the counts enabled only when asked and
counted through the real lookup path; the receipt fixture on the
variant and the lookup fields, absent when the diagnostic is absent;
the tpch bands carrying both. (Review round, this record: the first
accounting fixture omitted the overflow heap and the first parity
fixture typed the probe key `Int64` for every variant, so the `Int32`
case took the generic plan; both corrected before the build.)

Ladder on bigfoot (`RUST_MIN_STACK=67108864`, fmt clean), on the
control tree and the treatment tree alike: Toron library 1,821,
architecture 2,151, integration 3,649; toron-tpch library 241 (+4
ignored), `toron-tpch` binary 132, `toron-bench` 9, integration 261 (+2
ignored). The closing tree adds the publication fixtures (library
1,823).

Evidence on strix: `target/tpch/adr600-q21-compact-index/`
(`samples.jsonl` `58b2dfcb2629e1eb1…`, `summary.json`
`63a5e0a44e3f0e2da…`, `profiles/`), `target/tpch/adr600-mechanism-check/`
(`samples.jsonl` `aa6d71e2cc2acf0ce…`, `summary.json`
`4a92d9d6b3f519b07…`, `profiles/`), and
`target/tpch/adr600-strix-host-record/` (the paired attempt's
`completion.marker` `798ffb3f…`, `driver_stdout.log` `2894c034…`,
`host_config_before.txt` `6191ebac…`, `host_after.txt` `b3c188f3…`
under `paired-attempt/`; the mechanism check's `completion.marker`
`d9f345fb…`, `driver_stdout.log` `095ecf74…`,
`host_config_before_mechanism.txt` `f24db920…`,
`host_after_mechanism.txt` `24ca7eb9…`; `launch.sh` `95c8c203…`,
`launch_mechanism.sh` `836ec435…`, `snapshot.sh` `fc6022e8…` — ADR
599's copy, whose two directory lines named ADR 599's directories on
the paired attempt's snapshot and were repointed before the mechanism
check; `tool_versions_run_host.txt` `d607c81f…`).

## Closure

ADR 600 closes `retained` under the rule it inherited: the cut gained
on every pair, a median 559.7 ms and 6.23% of q21, against a bar of
191.8 ms on this run's control range. The one call stays; the facts,
the fixtures, and the counter remain, the counter's reporting half now
one collection on every handler that reaches cleanup, and its absence
read as absence. The mechanism check says what
the cut did — the `int64` variant over 5.85 million entries in
268.5 MB of accounted residency, the index child at 303.9 ms and the
build write at 688.7 on the treatment's own profile — and the pairs
say what it was worth at q21's scale on this run; neither is a claim
the rule did not make, and the historical readings beside them are
context from other executables. What the record asked for and did not
get is replay 1's lookup counts, absent for the reason stated and
unmeasured here. The next candidate on this child, as the record's
forward section put it, is the once-built index (ADR 589's pattern),
with its own ledger term for the finish.
