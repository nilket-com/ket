# ADR 601 - q21's replay-1 build write: the compact index built once, at finish: results

Measured 2026-09-17 on strix: one paired attempt under ADR 595's
separated launch, ADR 597's twelve-row driver, and ADR 599's declared
gate procedure (a 5 s wait between the launch environment's admission
and the gate's reading, threshold and load bound unchanged). The gate
admitted; twelve rows ran; every row's q21 digest equals the sealed
digest. **Verdict, by ADR 588's rule as ADR 597 fixed it: `retained`.**
Every pair was positive — gains of 142.8, 148.2, 150.4, 109.4, and
155.2 ms, median **148.2 ms (1.84% of the control median)** — against
a bar of 59.3 ms (2 × the 29.7 ms control range). The one token stays
in the closing tree. The mechanism check on the treatment (ADR 596's
driver, after the verdict) read the index the cut built —
`index_build = finish`, 5,845,938 entries, no repeated key, 2²⁴
buckets, no growth past the reservation, resident peak 207,482,040
bytes — with `build_write_finish_index` at 110.9 median-scaled ms,
the per-page index child at 37.9, `inner_1_build_write` at 535.9, and,
for the first time, replay 1's lookup counts: 14,656,608 hits and
15,686,933 misses. That check's off arm ranged 680.6 ms (8.54%), over
ADR 593's 4% bound, so ADR 596's owner rule on it closed on
variability; its facts and terms are reported as readings.

## Identity

| identity | value |
|---|---|
| control executable | `38e2c42ec735fc7d46cb7ba376d16da274203901e849d694ad5ad9c76dfbf85f`, tree `10d139c918e6e45e651269d7713b0659d860b061`, commit `9e53dde8` (tag `artifact/adr601-control`: this record's index states, terms, facts, and fixtures; the branch's form `SpilledIndexForm::Incremental` — ADR 600's index as retained) |
| treatment executable | `39c94f6e2d6e2707d581f08f8a5499e284829e8f86b2849a972755ebe7f61ae0`, tree `5ef74af8904042b06e447d153ab8da25d947c3c1`, commit `ef41bdd7` (tag `artifact/adr601-treatment`: the control tree with the one token `SpilledIndexForm::FinishBuilt` at the branch's call site — 1 insertion, 1 deletion in `exec/join/streaming.rs`) |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580`, both arms; ADR 588's manifest gate admitted the pair (same recipe, different trees, different executables, no untracked inputs) |
| closing tree | the treatment tree with this document, the ADR's status, and the call site's comment; not re-measured under this record |
| launcher | `toron-bench` `23267312…` (ADR 598's, unchanged since), which verified 99,338,824 bytes in 46,507 µs under mask `0-31` |
| sealed baseline (bound by content) | `950ebc4b…` on tree `a962f3d8…`, artifact `d03935eb…`, q21 median 9,126.9 ms, affinity `0-15`, dataset `fnv1a64:0fc5c06d19c5ed23` |
| driver | `adr597-q21-exchange-skip` (ADR 597's two-executable driver, by reference; the treatment is the launching manifest) with `--gate-settle-seconds 5`; unit `adr601-finish-built.service` under `user@1002.service/app.slice`, `Linger=yes`; `Type=exec`, `RuntimeMaxSec=10min`, `TimeoutStopSec=30s`, `KillMode=control-group`; timer `OnActiveSec=120`, `AccuracySec=1s` |
| build host / run host | bigfoot / strix |

## The launch, the gate, and the window

| instant (CDT) | what |
|---|---|
| 08:19:50 | preflight: the completion hook's datagram from strix reached the workstation's listener (+35 ms) |
| 08:20:01.593 | `ADR595_LAUNCH_UNIX_MS`, launch session 2377, token `c0731433e614bd650d644832`, delay 120 s; the session exited (logout 08:20:01.641); no other session on strix before the trigger |
| 08:22:01.000 | timer trigger; the launcher's hash ended at 08:22:01.952 |
| 08:22:01.954 | `at_entry`: **50 °C**, one P-core at 5.70 GHz and one at 1.46, 2 ms after the hash's end; then the scope and the launch environment's admission (driver start 08:22:01.963) |
| 08:22:01.96–08:22:06.96 | the declared wait: requested 5 s, performed, elapsed 5,000.4 ms on the monotonic clock |
| 08:22:06.963 | `at_gate`, the first act after the wait and the one reading judged: **37 °C**, every masked core at 800 MHz, load 0.16 against the 0.3 bound — **admitted**; `hash_end_to_gate_ms` 5,010.7 |
| 08:22:09.433 | first child's launch (the control's warmup); separation margin 127.8 s from the launcher's logout |
| 08:24:14.741 | last child's return; journal collected `collected`, 7 entries: session 2377's close, one wireless `UFW BLOCK` and one `rtw88` entry in the separation interval; one `UFW BLOCK` (08:23:21, during pair 2's control child) and no session event in the window — **`verified` / `verified`** |
| 08:24:16.784 | completion hook: `result=success exit_code=exited exit_status=0`; datagram received at +35 ms |
| 08:24:47 | read-out session 2378; unit and timer inactive; spill scratch empty; package 43 °C, load 0.72 |

The entry reading was above the threshold (50 °C) and the gate reading
after the wait below it (37 °C); both are observations, as ADRs 599
and 600 reported them, with nothing established about why they differ.

## The pairs

Warmups: control 8,013.3 ms, treatment 7,877.9 ms. Every row `pass`,
digest `ArrowIpcDigest([253, 211, 102, 221, …])` equal to the sealed
digest on all twelve, every row launched from its arm's executable and
validated as it returned.

| pair | order | control ms | treatment ms | gain ms | gain % |
|---:|---|---:|---:|---:|---:|
| 0 | C,T | 8,058.9 | 7,916.0 | 142.8 | 1.77 |
| 1 | T,C | 8,065.6 | 7,917.4 | 148.2 | 1.84 |
| 2 | C,T | 8,085.2 | 7,934.8 | 150.4 | 1.86 |
| 3 | T,C | 8,058.2 | 7,948.9 | 109.4 | 1.36 |
| 4 | C,T | 8,055.6 | 7,900.4 | 155.2 | 1.93 |

Control median **8,058.9 ms**, control range **29.7 ms** (8,055.6 to
8,085.2; the cutoff ratio 0.0037, within the 4% bound), treatment
median 7,917.4 ms. **Bar `2 × 29.7 = 59.3 ms`; median gain 148.2 ms;
every pair positive; `retained`.** The control — ADR 600's retained
index on this record's tree — sits 1,068.0 ms (−11.70%) from the
sealed q21 median, 920.6 ms from ADR 600's control median (8,979.5)
and 499.2 ms from ADR 600's treatment median (8,416.6), observed
distances between runs on different days and executables, not netted.
Readings in run order: package 84–91 °C at the children's start
readings, 69–88 °C at end; involuntary switches 67–78 on eleven rows
and 332 on the treatment's warmup; voluntary 11,730–11,733 on both
arms; inter-child gaps 5.5–7.0 ms; peak RSS 1,009,960–1,022,660 KiB
on both arms.

## The mechanism check, after the verdict

ADR 596's driver (`adr596-q21-build-write`, its own off/on arms) on the
treatment executable, launched under the same procedure at 08:25:03
(session 2382, token `98758acc…`; entry 47 °C with one P-core at
5.44 GHz, gate 38 °C after the 5,000.2 ms wait, load 0.16 — admitted),
eleven rows, separation 127.8 s and window `verified` (one wireless
`UFW BLOCK` entry during pair 0's off child, no session event), off
median 7,970.5 ms, **off range 680.6 ms (8.54%, over the 4% bound:
the off rows read 8,154.9, 7,892.3, 8,572.9, 7,970.5, 7,974.4, and
7,968.0)**, on median 8,112.9 ms (perturbation 1.79%, accepted),
sixth and seventh bands consistent on every pair, residuals complete,
`facts_vintage adr601` (the treatment executable's label for the fact set the closing tree names `index_build`; see the revalidation below). ADR 596's owner rule on these rows returns
`closed_on_variability` and ADR 594's `below_floor_no_mechanism`; the
facts and the terms below are what the ledger recorded on the run,
reported as readings under that variability:

| fact, treatment (measured) | value | ADR 600 treatment (incremental, historical) |
|---|---:|---:|
| exchange handling / partitions | split / 4 | split / 4 |
| batches read / written, chunks / pages | 1,465 / 5,860, 5,860 / 5,860 | the same |
| plaintext / ciphertext bytes | 27,148,512 / 27,242,272 | the same |
| `index_mode` / `index_variant` / **`index_build`** | compact / int64 / **finish** | compact / int64 / incremental |
| `index_expected_rows` / `index_entries` | 5,845,938 / 5,845,938 | — / 5,845,938 |
| `finish_index_entries` / `_duplicates` / `_buckets` | **5,845,938 / 0 / 16,777,216** | — |
| `index_capacity_growths` | **0** | — |
| `resident_state_peak_bytes` | **207,482,040** | 268,506,120 |
| `writer_peak_bytes`, scratch capacities, releases | 5,128, 8,960 B / 1,930 entries, 0 | the same |
| replay 1 `lookups_counted` / hits / misses | **true / 14,656,608 / 15,686,933** | absent |

The resident peak is the plan's arithmetic to the byte: 5,845,938 × 24
= 140,302,512 entry bytes at the reservation's capacity (the estimate
exact, no growth) plus 2²⁴ × 4 = 67,108,864 bucket bytes, plus the
5,128-byte writer peak and the 65,536-byte page reserve; no key
repeated, so no overflow list. Replay 1's probe made 30,343,541
lookups against this build, 48.3% of them hits — a count, reported;
replay 2's spilled build was not counted (the lifecycle handle covers
replay 1).

Terms, median-scaled ms, treatment against ADR 600's treatment
(historical, another run and executable, never netted):
`build_write_scan_wait` 134.3 (139.1; open 0.23, poll 134.1);
**`build_write_finish_index` 110.9** (—; raw 112.1–113.1 ms across the
five pairs); `build_write_repartition` 64.9 (64.9); `build_write_finish`
63.2 (53.9; raw 44.1, 46.7, 64.3 on three pairs and 323.8, 387.2 on
pairs 1 and 0); `build_write_encode` 50.8 (50.6); **`build_write_index`
37.9** (303.9; raw 38.0–40.1); `build_write_key_extract` 31.7 (28.5);
`build_write_page_write` 30.5 (31.6); `build_write_residual` 13.8
(14.2); `build_write_create` 0.25; `build_write_reserve` 0.01 (raw
9–14 µs); filter and project 0. **`inner_1_build_write` 535.9** (688.7;
ADR 596's 879.6; raw 523.0–545.2 on three pairs, 819.9 and 878.7 on
the two whose finish read 324 and 387 ms). The sixth band's other
children as before (partition sizing 262.5, sizing scan 197.8). The
finish term, `set_len`, `fdatasync`, and the guard together, is the
one that spread; the finalization pass beside it read within 1 ms
across the pairs.

## Implementation and verification

Engine (`exec/join/spill.rs`): `PrimitiveIndexBuild::{Incremental,
Collecting, Finalized}` on the primitive index; `reserved(expected_rows)`
(the entry vector obtained at that capacity, accounted as obtained, no
buckets); `collect` (the append without hashing, growth counted and
accounted after from the obtained capacity); `finalize` (the bucket
array at `finish_index_plan`'s count — checked, allocation-free,
refusing an entry count at the `u32` sentinel or an unrepresentable
array — accounted as obtained, then the cursor pass: a new key moves
down to the cursor, a repeated key's ref onto the earlier entry's
overflow list in insertion order, the vector truncated, its capacity
kept); on the build, `use_compact_resident_index_built_at_finish(key_plan,
null_safe_keys, expected_rows, budget)` (admitted plans only, before
the first page, the planned entry bytes guarded at site `reserve`
before the allocation) and `finalize_index(budget)` (the planned bucket
bytes guarded at site `finish_index` before the allocation, the build
cleanable on a refusal); a lookup while collecting, `finish` on a
collecting index, and a second finalization refused by name; every
writer entry (`write_batch_chunks_ledgered`,
`write_batch_chunks_coalesced_profiled`, `write_preserved_batch_chunks`,
`write_projected_batch_chunks_ledgered`,
`write_projected_batch_chunks_coalesced_profiled`) refusing a write to
a finalized build before any page I/O or early return, the build left
as it was — the first-page reconstruction, which would otherwise have
replaced a finalized empty index with an incremental one on a later
keyed write, never reached. (Review round: the first cut checked only
at the insertion; the boundary check was added.) The ledger: thirteen children —
`build_write_reserve` and `build_write_finish_index` — the facts
`index_build`, `index_expected_rows`, `index_capacity_growths`,
`finish_index_entries`, `finish_index_duplicates`,
`finish_index_buckets`, settled after finalization
(`observe_finish_built_index`) so an input with no keyed page still
names its form. The receipt (`exec/node_attribution.rs`): the `index_build`
facts vintage, keyed on that fact, under which the two terms are
required and, without which, neither carried nor asked for (the
retained ADR 596–600 receipts revalidate as before); the rules
`reserve` and `finish_index` calls 1 iff `index_build = finish` and 0
with zero time otherwise, `finish ⇒ compact ∧ variant ∈ {int32,
int64}`, `entries + duplicates = index_entries`, `buckets` a power of
two at or above twice the entries; `index_capacity_growths` required
numeric on either form; the four finish-only facts
(`index_expected_rows`, `finish_index_entries`, `_duplicates`,
`_buckets`) required present — numeric on the finish-built form, null
on the incremental one — a missing field or another shape a defect.
(Review round: the first cut required only the three totals under
`finish` and rejected only a non-null entry count on the incremental
form; the shape rules were added.) The retained receipts revalidated
under the tightened rules without a run (`adr596-reevaluate` over the
mechanism checks): ADR 601's, ADR 600's, and ADR 599's five pairs each
consistent as retained and with the derivation; ADR 600's and 599's
with no differences, ADR 601's with exactly one — the vintage label,
`adr601` as the treatment executable wrote it against `index_build` as
the closing tree names the same fact set (the review round renamed the
label and the identifiers around it for the repository's naming rule,
which bars record numbers from code names; the harness keys the two
terms on the `index_build` fact itself, so the retained rows still
evaluate over thirteen terms); ADR 596's as its closure recorded them —
consistent with the derivation, `batches_limit_discarded` and
`batches_admitted` predating the rules. The branch (`exec/join/streaming.rs`): `SpilledIndexForm`
chosen at the call site ahead of every clock — `FinishBuilt` on an
admitted plan reserves under the reserve clock and finalizes under the
`finish_index` clock, closed before `finish` opens; any other plan, and
`Incremental`, take ADR 600's call with neither clock. `toron-tpch`: the
bands carry the two terms and six facts under the `index_build` vintage and
rank thirteen rule terms, eleven otherwise.

Fixtures (nine new): the pass under a seeded hasher — seven keys
chosen to collide on the last bucket, probe past the array's end and
wrap, and repeat after their first occurrence's slot was moved and a
dropped slot reused — every lookup returning the right refs in order,
equal to the incremental index's, the buckets pointing only below the
cursor; the states refusing a second finalization and a write after
it, an empty collection finalizing to one bucket, a short reservation
growing with the growth counted; the plan's arithmetic refusing the
sentinel and `usize::MAX` without allocating; through the real write
path, finish-built against incremental on `Int64` and `Int32` — refs
and their order for every key, at least 50 repeated keys, nulls never
indexed, an absent key, growth across pages, both clocks once, the
accounting from obtained capacities; a finalized build — empty,
all-null, and populated — refusing a keyed, an empty, and a projected
write at the boundary with rows, pages, spill bytes, form, entries,
accounting, and lookups unchanged, then cleaned up; the unadmitted
fallback (`Utf8`,
null-safe `Int64`) taking the incremental hash index with both new
children at zero calls and zero time and no finalization facts; the
accounting on every path — a short estimate growing (counted), a long
one keeping its capacity, a duplicate-heavy input truncated to ten
entries with 5,990 on overflow lists, all-null input finalizing to one
bucket, the bucket allocation refused at `site=finish_index` on a
40 KB budget with the pages written and the build cleaned up, the
reservation refused at `site=reserve` on 8 KB before any page; a
collecting index refusing lookups and finish; the receipt under the
`index_build` vintage with a breaking case per rule and an earlier vintage
neither carrying nor asked for the terms (carrying them without
`index_build` a defect); the tpch bands by vintage.

Ladder on bigfoot (`RUST_MIN_STACK=67108864`, fmt clean; the release
and library builds without warnings, the test build with one
pre-existing warning on `adr589_plain_write` that predates this
record — the first cut's test build also carried two unused-`mut` and
one unused-method warning of its own, since removed), on the control
tree and the treatment tree alike: Toron
library 1,831, architecture 2,151, integration 3,649; toron-tpch
library 241 (+4 ignored), `toron-tpch` binary 133, `toron-bench` 9,
integration 261 (+2 ignored).

Evidence on strix: `target/tpch/adr601-q21-finish-built/`
(`samples.jsonl` `040d3083d…`, `summary.json` `6711e7570…`,
`profiles/`), `target/tpch/adr601-mechanism-check/` (`samples.jsonl`
`2756caba7…`, `summary.json` `e9d787908…`, `profiles/`), and
`target/tpch/adr601-strix-host-record/` (the paired attempt's
`completion.marker` `61e60c59…`, `driver_stdout.log` `d0e4c398…`,
`host_config_before.txt` `2a7018f6…`, `host_after.txt` `ad16cbc9…`
under `paired-attempt/`; the mechanism check's `completion.marker`
`ca3ba480…`, `driver_stdout.log` `c65d6fda…`,
`host_config_before_mechanism.txt` `0e1990b9…`,
`host_after_mechanism.txt` `97852d0d…`; `launch.sh` `c7d587d8…`,
`launch_mechanism.sh` `daf9af70…`, `snapshot.sh` `a160a365…`,
`tool_versions_run_host.txt` `d607c81f…`).

## Closure

ADR 601 closes `retained` under the rule it inherited: the cut gained
on every pair, a median 148.2 ms and 1.84% of q21, against a bar of
59.3 ms on this run's 29.7 ms control range. The one token stays; the
finish-built index is the spilled branch's index. The mechanism check
says what the cut did — the reservation exact, no growth, no repeated
key, 207.5 MB accounted where the incremental index held 268.5, the
per-page index child at 37.9 ms and the finalization at 110.9 beside
it, the build write at 535.9 — under an off arm that ranged past the
variability bound, so those terms are readings and not an owner
finding; the pairs say what the cut was worth at q21's scale on this
run. Replay 1's lookups are on the record for the first time:
30.3 million against this build, 14.7 million hits. The next candidate
on replay 1, as the record's forward section put it, is the probe side
— `refs` allocating a `Vec` per hit, 14.7 million times here, and the
hash function — grounded on these counts.
