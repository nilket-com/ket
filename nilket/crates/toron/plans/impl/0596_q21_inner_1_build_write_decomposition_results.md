# ADR 596 - q21's replay-1 build write on strix: lifecycle decomposition of `inner_1_build_write`: results

Measured 2026-09-16 on strix under ADR 591's coordinate, ADR 595's
separated launch, and this record's seventh band. **The run's off range
was 324.1 ms, 3.61% of its 8,986.8 ms median, inside the 4% cutoff; the
launcher's logout lies 122.9 s before the first child and the window
carried no login-session event; the sixth band's rule again resolves
`inner_1_build_write` (879.6 median-scaled ms, 9.79%); and the seventh
band's rule, over the eleven terms inside it, returns
`below_floor_no_mechanism`: the leading child, `build_write_index`, reads
482.4 median-scaled ms (5.37% of q21, 54.9% of the build write) against
a floor of 648.2 ms (2 × 324.1), and it is below the 500 ms floor as
well, so no range would have carried it.** The facts settle the shape of
the write: the build's scan input carries an Exchange of four
partitions and no filter, the rows are written whole (no projection),
the 1,465 scan batches become 5,860 pieces, chunks, and pages of about
1,000 rows, every chunk keyed, and the index is the legacy one. No
owner is promoted; the shape names the successor.

## Identity

| identity | value |
|---|---|
| source tree (instrumented) | `c0097a48b2a4383af082e7d446041eedf4fd16a2`, commit `3de8e04d` (the closing commit differs in this document, the ADR's status, and the review-round changes to the receipt's rules and the re-evaluation named below) |
| instrumented executable | `2a7cad0d9338d95ba3ce91691f525e60fae4c9ebe9ed574f993b181c98792e55`, both arms of every pair |
| build manifest | `evidence/build/adr596-instrumented.manifest` (`f2944bf2…`; bigfoot and strix, staged under `~/work/toron-evidence/staged`, hash verified on strix) |
| closing executable (re-evaluation only) | `fa023fb56e6adc0abdda4d14668541b257ed9acbbd5a7e104990123cf0955b94` on tree `5ef3c064…`, commit `1b9bf9a6` (the closing commit differs from it in this document alone), `evidence/build/adr596-closing.manifest`; no measurement was made with it |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` |
| sealed baseline (bound by content) | `950ebc4b…` on tree `a962f3d8…`, artifact `d03935eb…`, q21 median 9,126.9 ms, affinity `0-15`, dataset `fnv1a64:0fc5c06d19c5ed23` |
| driver unit | `adr596-build-write.service` under `user@1002.service/app.slice`, Unix session id 407116, audit session id 2143, `Linger=yes`; `Type=exec`, `RuntimeMaxSec=10min`, `TimeoutStopSec=30s`, `KillMode=control-group`, `KillSignal=15`, `FinalKillSignal=9`; timer `OnActiveSec=120`, `AccuracySec=1s` |
| start gate | package 45,000 millic, load 0.08 |
| build host / run host | bigfoot / strix |

## The launch, verified

| instant (CDT) | unix ms | what |
|---|---:|---|
| 11:50:32.500 | 1789577432500 | `ADR595_LAUNCH_UNIX_MS`, taken before `systemd-run`, launch session 2255 (load 0.00, package 40 °C at staging) |
| 11:50:32.516 | 1789577432516 | `systemd-run` returned (timer started; `NextElapseUSecRealtime` empty for a monotonic `OnActive` timer, the 120 s delay recorded) |
| 11:50:32.539 | 1789577432539 | `Session 2255 logged out` (logind, boot `882b9409…`) |
| 11:52:32 | 1789577552000 | timer trigger as `LastTriggerUSec` reports it (second resolution) |
| 11:52:32.964 | 1789577552964 | driver start; gate read: 45 °C, load 0.08 |
| 11:52:35.401 | 1789577555401 | first child's launch (the warmup) |
| 11:54:42.503 | 1789577682503 | last child's return |
| 11:54:44.583 | 1789577684583 | completion hook: `result=success exit_code=exited exit_status=0`, token `77042a2d602d9f6e282b1572` |
| 11:54:44.621 | 1789577684621 | datagram received on the workstation (38 ms), sender 192.168.0.8 |
| 11:54:59.909 | 1789577699909 | read-out session 2256 opened; unit and timer inactive; package 47 °C; spill scratch empty |

Boot id bound (launcher and driver on `882b9409-f9d4-4b4e-b2ca-bffc1de1f891`).
Journal collected once after the last child from the launch instant −1 s
to the last return +1 s: `collected`, 7 entries, 0 malformed. In the
separation interval: session 2255's close, deactivation, logout, and
removal (11:50:32.537–32.539), one `rtw88_8821au … failed to get tx
report from firmware` (11:51:46.6) and one wireless `UFW BLOCK`
(11:51:52.7), both on the unused wireless interface and both before the
first child. In the measurement window: one wireless `UFW BLOCK`
(11:53:57.7, in child 7's window and child 8's join by its 1 s margins)
and nothing else. **Separation: the logout precedes the first child by
122,861.2 ms; `verified`. Window: zero session entries; `verified`.**
The recorded verdicts are these; nothing was re-evaluated.

## The run

Warmup (off) 8,931.7 ms. Pairs, off / on, movement: pair 0 (off, on)
8,899.3 / 9,046.2, +1.65%; pair 1 (on, off) 8,966.6 / 9,112.8, +1.63%;
pair 2 (off, on) 9,023.7 / 9,141.6, +1.31%; pair 3 (on, off) 8,986.8 /
9,108.8, +1.36%; pair 4 (off, on) 9,223.4 / 9,530.8, +3.33%. Off median
**8,986.8 ms**, off range **324.1 ms** (8,899.3 to 9,223.4), on median
9,112.8 ms. Perturbation gate: median `|m_i|` 0.0163, accepted (ADR
595's instrument, without this band, read 0.0170 on its run; the
seventh band's clock reads — one per build for create, open, and finish,
1,466 polls, 1,465 repartitions, and four per page over 5,860 pages,
about 26,400 pairs per on child — are not distinguishable in that
figure, and the residual below says what they cost inside the term).
Every profile complete, the seven identities reconciled on every on
row, the sixth and seventh bands' facts consistent on every pair. The
off median sits 140.1 ms (−1.5%) from the sealed q21 median and 19.1 ms
(−0.2%) from ADR 595's, observed distances.

**Variability: 0.0361 against the 0.04 cutoff. Within bound.** The
range is pair 4's: its off arm at 9,223.4 ms is 200 ms above the next
slowest, and its on arm carries a 410.5 ms `finish` (12–18 ms on the
other four pairs). Neither is attributed here; the pair's readings
(below) show nothing the other pairs lack.

The sixth band, as context: `runtime_prep_inner_1` 1,346.7 median-scaled
ms (14.99%); `inner_1_build_write` **879.6** (9.79%), `inner_1_partition_sizing`
263.8, `inner_1_sizing_scan` 199.6, residual 0.04; branch `spilled`,
one partition, one pass, 5,845,938 rows, 27,242,272 spill bytes on
every pair; ADR 594's rule resolves the build write, as ADR 595 read it
(876.4).

### The seventh band, per pair (µs)

| pair | `inner_1_build_write` | create | scan open | scan poll | filter | repartition | project | key extract | encode | page write | index | finish | residual | bound |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 875,458 | 209 | 238 | 181,557 | 0 | 67,233 | 0 | 27,758 | 52,060 | 33,884 | 485,762 | 12,472 | 14,285 | 190,646 |
| 1 | 880,444 | 307 | 242 | 181,672 | 0 | 65,427 | 0 | 29,186 | 52,200 | 35,034 | 490,304 | 12,090 | 13,982 | 190,259 |
| 2 | 891,060 | 269 | 235 | 183,151 | 0 | 65,528 | 0 | 31,593 | 52,176 | 33,591 | 496,732 | 13,116 | 14,669 | 162,450 |
| 3 | 894,785 | 402 | 247 | 183,321 | 0 | 68,374 | 0 | 28,318 | 52,667 | 36,025 | 493,208 | 18,011 | 14,212 | 166,717 |
| 4 | 1,284,720 | 311 | 248 | 181,941 | 0 | 65,560 | 0 | 29,371 | 52,418 | 34,781 | 495,495 | 410,517 | 14,078 | 371,655 |

The identity `sum(eleven) + residual = inner_1_build_write` holds on
every pair; every residual is inside its bound (`overhead + 5%`), at
1.1–1.6% of the term; no residual leads.

### The facts, identical on every pair

| fact | value |
|---|---|
| filter present | `false` (the writer saw no predicate; the scan delivered 5,845,938 rows, the count the sizing scan reports, so `o_orderstatus = 'F'` is applied below the writer) |
| exchange | `true`, 4 partitions |
| projected | `false` (rows written whole) |
| termination | `stream_end` (a final `None` observed; 1,466 polls for 1,465 batches) |
| index mode | `legacy` |
| batches read / filtered empty / written | 1,465 / 0 / 5,860 (each batch split four ways by the Exchange, every piece non-empty) |
| rows read / kept | 5,845,938 / 5,845,938 |
| chunks / unkeyed / pages | 5,860 / 0 / 5,860 (one chunk per piece: the pieces average 998 rows, well under the 8,192-row chunk) |
| plaintext / ciphertext bytes | 27,148,512 / 27,242,272 (+93,760 = 5,860 pages × 16 tag bytes; the ciphertext equals the sixth band's spill bytes) |
| index entries / compactions | 5,845,938 / 0 |
| calls | create 1, scan open 1, scan poll 1,466, filter 0, repartition 1,465, project 0, key extract 5,860, encode 5,860, page write 5,860, index 5,860, finish 1 |

### The rule

Owner floor `max(500 ms, 2 × 324.1) = 648.2 ms`. Median-scaled terms,
`scan_wait` ranked once (its two components beside it, never in the
ranking):

| term | median-scaled ms | share of off median | share of build write | clears floor |
|---|---:|---:|---:|:---:|
| `build_write_index` | 482.4 | 5.37% | 54.9% | no |
| `build_write_scan_wait` (open 0.24 + poll 178.8) | 179.0 | 1.99% | 20.4% | no |
| `build_write_repartition` | 64.7 | 0.72% | 7.4% | no |
| `build_write_encode` | 51.4 | 0.57% | 5.8% | no |
| `build_write_page_write` | 33.7 | 0.37% | 3.8% | no |
| `build_write_key_extract` | 28.4 | 0.32% | 3.2% | no |
| `build_write_residual` | 14.0 | 0.16% | 1.6% | no |
| `build_write_finish` | 13.0 | 0.14% | 1.5% | no |
| `build_write_create` | 0.3 | 0.00% | 0.0% | no |
| `build_write_filter` | 0.0 | — | — | no |
| `build_write_project` | 0.0 | — | — | no |

**Outcome: `below_floor_no_mechanism`, leading term `build_write_index`
at 482.4 ms.** No child clears 648.2 ms, and the leader would not clear
500 ms on a zero range either. No residual leads. No owner, no pricing
(`owner_removable_pricing.status = no_owner`), no cut. ADR 595's
precedence was applied and did not withhold (both intervals verified).

What the terms say about the write, as arithmetic from the counts and
not as attribution or per-operation latency: the index child, 482.4 ms
over 5,845,938 entries and 5,860 pages, is 82.5 ns per entry and 82 µs
per page of aggregate child time — the span covers the insert loop and,
per page, the compaction check, the resident guard, and the page
bookkeeping, so it is not a measured insert latency; the scan wait,
179.0 ms over 1,466 polls, is 122 µs per poll of the caller's wall wait
on a stream whose read and decode this instrument does not decompose;
the repartition child, 64.7 ms over 1,465 batches, is 44 µs per batch
(11 ns per row) to hash every key into four buckets that are then
written to one file; encode 8.8 µs and page write 5.7 µs per 4.6 KB
page (the seal and the write of 27.2 MB in 33.7 ms); key extraction
4.8 µs per chunk; finish 13 ms for `set_len` and `fdatasync` of the
27 MB file on four pairs, and 410 ms once. The shape is a large index
child over a spread of small per-page children. On the page count, two
figures kept apart: the observed one, 1,465 batches read → 5,860 pieces
→ 5,860 chunks → 5,860 pages, because the Exchange's four-way split
precedes the per-batch chunking and every piece is under 8,192 rows;
and the packing lower bound, `ceil(5,845,938 / 8,192) = 714`, which is
what globally packed 8,192-row pages would number and which this
writer does not produce across incoming batches on any path. The
observed count is the fact; the bound is arithmetic, not an
amplification this record attributes to the Exchange.

## The readings, in run order

| child | arm | execute ms | temp start→end °C | P-core clocks at start (MHz) | at end | ctxt vol/invol | reading µs | gap ms | journal |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| warmup | off | 8,931.7 | 85→80 | 800–5,900 | 800–5,994 | 11,733/76 | 373/277 | — | 0 |
| 1 | off p0 | 8,899.3 | 88→78 | 800–5,900 | 800–5,978 | 11,733/94 | 400/257 | 6 | 0 |
| 2 | on p0 | 9,046.2 | 89→72 | 800–5,900 | 800–5,405 | 11,732/76 | 400/245 | 6 | 0 |
| 3 | on p1 | 9,112.8 | 90→80 | 800–5,900 | 800–5,691 | 11,733/79 | 380/279 | 7 | 0 |
| 4 | off p1 | 8,966.6 | 90→77 | 800–5,900 | 798–5,700 | 11,732/68 | 280/250 | 7 | 0 |
| 5 | off p2 | 9,023.7 | 89→87 | 800–5,820 | 800–5,968 | 11,733/74 | 370/254 | 7 | 0 |
| 6 | on p2 | 9,141.6 | 91→83 | 800–5,897 | 800–5,483 | 11,733/79 | 260/262 | 7 | 0 |
| 7 | on p3 | 9,108.8 | 91→83 | 800–5,810 | 799–5,845 | 11,730/85 | 366/260 | 6 | 1 |
| 8 | off p3 | 8,986.8 | 91→83 | 800–5,895 | 800–5,900 | 11,733/69 | 366/269 | 6 | 1 |
| 9 | off p4 | 9,223.4 | 90→79 | 800–5,800 | 800–5,657 | 11,733/68 | 380/348 | 7 | 0 |
| 10 | on p4 | 9,530.8 | 90→81 | 800–5,813 | 800–5,541 | 11,732/75 | 435/266 | 7 | 0 |

Endpoint temperatures 85–91 °C at start readings and 72–87 °C at end
readings; one P-core thread at 5.4–6.0 GHz at each endpoint;
involuntary switches 68–94; reading cost 260–435 µs; inter-child gaps
6–7 ms; the one journal entry in a child's window is the wireless `UFW
BLOCK` at 11:53:57.7. The limits ADR 593 stated apply. Pair 4's readings
are inside every band the other pairs occupy; its 200 ms off excess and
410 ms finish are recorded, not explained.

## Review round: two receipt gaps, and the retained receipts re-validated

Review found two gaps in the receipt's rules as first cut. (1) A valid
row-limit run failed consistency: the writer's own fixture reads three
batches but writes two, because the third poll's batch is discarded
whole by the limit already met, and the rules expected every read batch
to reach the filter, the Exchange, and the writer. The ledger now
records `batches_limit_discarded` and `batches_admitted` (= read −
discarded); the filter, repartition, and batches-written rules are
against the admitted; `stream_end` requires zero discarded and
`row_limit` exactly one; and the writer's real fixtures — projected with
a filter, unprojected with an Exchange, an all-unkeyed chunk, the
row-limit termination, the exact-limit stream end, and the unsupported
preserved-rows shape — are each passed through the receipt's own
validator (`revalidate_build_write_band`, the band's rules over an
invocation as the profile stores it) and come back consistent, shape
`supported`, residual as constructed, or `unsupported` where they
should. The fixture's comment claiming the third batch is never read is
corrected: it is read and discarded. (2) An inactive child could carry
time: the rules checked call counts but did not require zero
microseconds where the count is zero, so residual time moved into an
absent filter or projection reconciled. The rule is now `calls == 0 ⇒
micros == 0` for every child, with the converse (calls whose durations
round to zero) still valid and a fixture for each: time in the absent
filter, projection, repartition, and a page child with no page.

**The retained receipts under the tightened rules.** `adr596-reevaluate`
now re-runs every on-row receipt through the band's current rules and
reports it beside the evaluator (`receipt_revalidation`). Run on strix
from the closing executable (`fa023fb5…`, report
`adr596-strix-host-record/reevaluation_under_review_round2.json`,
`bbf7d29ee2bcf35e52942445298d5433f0e033218ba8762543173cf72f70b6b1`;
the first round's report from `5a7dc83f…`, before the derivation was
narrowed to the missing fields, is retained beside it as
`reevaluation_under_review_round1.json`, `f91571fd…`, with the same
verdicts on every pair since both fields were absent):
the evaluator returns the retained outcome unchanged
(`below_floor_no_mechanism`, `build_write_index` 482.4 ms, the same
eleven medians, separation and window `verified`, cutoff `within_bound`
at 0.0361); every pair's residual and shape re-validate identical; the
`calls == 0 ⇒ micros == 0` rule passes on every retained receipt
(filter, project, and repartition carry zero time where they carry
zero calls); and the one rule the retained receipts cannot answer is
named on every pair: `fact batches_limit_discarded is missing`, because
the measured executable did not record it. The report states that
plainly (`all_facts_consistent_as_retained = false`,
`facts_predating_the_rules = [batches_limit_discarded, batches_admitted]`)
and then applies, as a second and separately labelled pass, the
derivation the measured code path implies — the discard counter is
incremented only on the branch that sets `termination = row_limit`, and
every retained receipt records `stream_end`, so the discarded count is
0 and the admitted count is the read count — to the missing fields
only (`derived_fields` names them; a recorded value is never rewritten,
and a fixture for each partially missing case shows a recorded discard
or a recorded short admitted count surviving into the rules and failing
them), under which every pair is consistent
(`all_facts_consistent_with_derivation = true`; both fields were absent
on every retained pair). The retained `summary.json` and
`samples.jsonl` are not rewritten; no measurement was repeated.

## Implementation and verification

Engine: `BuildWriteLedger` (`exec/join/spill.rs`), eleven children by
`BuildWriteChild`, path facts and counts, `begin`/`finish` taking the
clock only when a ledger is present; threaded as `Option<&mut>` through
`write_streaming_scan_inner_build_batches` (the loop rewritten around
an explicit poll so a final `None` and a row-limit exit are named as
`BuildWriteTermination`), `write_streaming_scan_inner_build_batch`,
`write_batch_chunks_ledgered` and `write_projected_batch_chunks_ledgered`
(the unledgered names call them with `None`), and `write_page_batch_inner`;
`try_compact_near_cap_resident_index` now reports whether it compacted;
armed on replay 1's spilled branch by the ADR 594 handle and published
once as `build_write_*` terms and a `build_write_facts` diagnostic;
`None` on every other route. Receipt: `build_write_decomposition_band`
(`exec/node_attribution.rs`) after the sixth band, absent when never
armed, a defect on any branch but `spilled`, with the residual by
checked subtraction, the nineteen facts, `build_write_shape`, and the
consistency rules conditional on the facts (polls against the
termination, filter and repartition and project calls against their
presence, key extraction against chunks, the page children against
pages, pages against chunks less unkeyed, batches written against
batches kept, rows and ciphertext against the sixth band).
Harness: `adr596-q21-build-write` (ADR 595's driver under this record's
spec and tags), `evaluate_adr596_pairs` running the sixth band's
evaluation first under the same tags, then the eleven-term rule with
`scan_wait = scan_open + scan_poll` ranked once and its components
reported beside the table, ADR 594's rule, and the gross pricing;
`adr596-reevaluate` (the ADR 595 re-evaluation generalized over the
evaluator, plus the receipt revalidation above).

Tests: the receipt band with a reconciled band, a child whose calls
round to zero, a row-limit band with its discarded batch, an unarmed
profile, children exceeding the term, a partial band, a band on the
partitioned branch, and one case per consistency rule (including time
in each inactive child and each discarded-batch rule) plus the
unsupported shape; the writer through the real path — projected with a
filter to the stream's end, unprojected with a one-partition Exchange
and no filter, an all-unkeyed chunk (key extraction counted, no page,
no index mode), a row-limit termination with no final poll and one
discarded batch, an exact limit that still observes the stream's end,
and the preserved-rows shape reported unsupported — each also run
without a ledger, preparing the same rows, pages, and spill bytes and
recording nothing, and each passed through the receipt's validator; the
receipt revalidation on fixtures as retained and with facts predating
the rules; the evaluator on fixtures
returning `resolved` (on a page child and on the scan wait's sum),
`below_floor_no_mechanism`, `withheld_instrument_incomplete` (a leading
residual inside its bound; inconsistent facts; the seventh band absent),
`withheld_incomplete_profiles` (a residual past its bound),
`withheld_unsupported_path` (the partitioned branch; an unsupported
writer shape), the pricing on the resolved child, and ADR 595's
precedence over this record's outcome.

Ladder on bigfoot (`RUST_MIN_STACK=67108864`, fmt clean, at the
measured tree): Toron library 1,813, architecture 2,151, integration
3,649; toron-tpch library 241 (+4 ignored), `toron-tpch` binary 116,
`toron-bench` 9, integration 261 (+2 ignored); at the closing tree the
same with the binary at 117. Two line-bound tests
caught the first two cuts of the same code — `exec/join.rs` at its
3,465-line bound and the ADR 587 line-class table on `streaming.rs` —
and the imports were placed so neither file's measured lines moved; the
ladder was re-run in full each time and the executable rebuilt from the
final tree.

Evidence on strix: `target/tpch/adr596-q21-build-write/` (`samples.jsonl`
`8446cb172d03faa96…`, `summary.json` `8d14275d571e8d4e6…`, `profiles/`)
and `target/tpch/adr596-strix-host-record/` (`host_config_before.txt`
`dbc6412b…`, `host_after.txt` `070248d9…`, `completion.marker`
`38a485cc…`, `driver_stdout.log` `4b9c2730…`, `launch.sh` `5ce5b9c1…`,
`snapshot.sh` `a0d4f364…`, `tool_versions_run_host.txt` `d88e2dc9…`,
`reevaluation_under_review_round1.json` `f91571fd…`,
`reevaluation_under_review_round2.json` `bbf7d29e…`).

## Closure

ADR 596 closes below the floor: the build write's eleven children are
read, reconciled, and consistent on every pair, and none of them owns
it. The leading child, the legacy index over 5.85 million entries, is
482.4 ms (54.9% of the write, 5.37% of q21); the scan wait is 179.0 ms; the
rest are under 65 ms each. The shape the record names for its
successor: an index child that is the largest single piece of q21's
replay-1 preparation still unaccounted to a mechanism, below the floor
as a term but with a candidate already on the books — ADR 589's
once-built index, which replaced this same legacy pattern on the
semi-join summary — and, beside it, the fact that the build's scan
input is an Exchange of four partitions feeding a one-partition build:
its split precedes the chunking, the 1,465 batches become 5,860 pages
of about a thousand rows, and every key is hashed into a bucket the
one-file write does not use. Whether either is a cut is a paired question
under its own record with its own control; nothing opens under this
one. The separated launch held: separation and window verified from the
driver's own collection, at the second use of the procedure.
