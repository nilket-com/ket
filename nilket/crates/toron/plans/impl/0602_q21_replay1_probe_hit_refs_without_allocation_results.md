# ADR 602 - q21's replay-1 probe: a hit's refs without an allocation: results

Measured 2026-09-29 on strix: one paired attempt under ADR 595's
separated launch, ADR 597's twelve-row driver, and ADR 599's declared
gate procedure (a 5 s wait between the launch environment's admission
and the gate's reading, with the threshold and load bound unchanged).
The gate admitted, twelve rows ran, and every row's q21 digest equals
the sealed digest. **Verdict, by ADR 588's rule as ADR 597 fixed it:
`not_established`.** Every pair was positive, with gains of 57.2, 55.3,
34.3, 99.2, and 4.3 ms and a median of **55.3 ms (0.70% of the control
median)**. The bar was 75.4 ms (2 × the 37.7 ms control range), and the
median does not exceed it. The token reverts to `Collected` in the
closing tree. The mechanism check on the treatment, run after the
verdict, read `lookup_form = inline` and **14,656,608 inline hits
against 14,656,608 hits**: every hit on replay 1 returned its ref
without an allocation, as the plan's arithmetic said it would on a
build with no repeated key. Removing those allocations did not clear
the bar. The five positive pairs are readings, not a retained
improvement.

## Identity

| identity | value |
|---|---|
| control executable | `66813843eef9d7a7edc94fda969017c00610915d05535b47077ae4fdebe8bc14`, tree `5c7f4d82a87dab58d11e89757e942d40491fd2ca`, commit `e13b43df` (tag `artifact/adr602-control`: this record's implementation with `SpilledLookupForm::Collected` at the spilled branch's call site) |
| treatment executable | `85d9048e1df0cd6618414ec3537e68f52472942b09567394643c5f6defed3238`, tree `ba945a670af7a5cca22faa51e340dae97924a792`, commit `d78136ca` (tag `artifact/adr602-treatment`: the control tree with the one token `SpilledLookupForm::Inline`; 1 insertion, 1 deletion in `exec/join/streaming.rs`) |
| manifests | control `87d722619f0d38f2…`, treatment `d55491d799f40cb9…` (`target/tpch/evidence/build/adr602-{control,treatment}.manifest`) |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580`, both arms; rustc 1.95.0 (`59807616e`); ADR 588's manifest gate admitted the pair |
| closing tree | the control tree with this document, the ADR's status, and the call site's comment; not re-measured under this record |
| launcher | `toron-bench` `23267312…` (ADR 598's, unchanged), which verified 99,360,136 bytes in 45,094 µs under mask `0-31` |
| sealed baseline (bound by content) | ADR 591's board, `perf_board.jsonl.evidence.sealed` `19c8a91a…`, dataset `fnv1a64:0fc5c06d19c5ed23` |
| driver | `adr597-q21-exchange-skip` (by reference; the treatment is the launching manifest) with `--gate-settle-seconds 5`; unit `adr602-lookup-inline.service`, `Type=exec`, `RuntimeMaxSec=10min`, `TimeoutStopSec=30s`, `KillMode=control-group`; timer `OnActiveSec=120`, `AccuracySec=1s`; `TORON_TPCH_CPU_AFFINITY=0-15`, 2 GiB task memory and spill reserve, partition parallelism 1 |
| build host / run host | bigfoot / strix |

## The launch, the gate, and the window

| instant (CDT) | what |
|---|---|
| 07:56:33 | preflight: a completion-hook datagram from strix reached the workstation's listener (+75 ms) |
| 07:56:50.507 | `ADR595_LAUNCH_UNIX_MS`, launch session 2925, token `a158f9e28552921d0a48cce0`, delay 120 s; the session exited |
| 07:58:50.000 | timer trigger; the launcher's hash ended at 07:58:50.953 |
| 07:58:50.96–07:58:55.96 | the declared wait: requested 5 s, performed, 5,000.2 ms elapsed on the monotonic clock |
| 07:58:55.96 | `at_gate`: **37 °C**, every masked core at 800 MHz, load 0.27 against the 0.3 bound, **admitted**; `hash_end_to_gate_ms` 5,010.9 |
| 07:58:58.428 | first child's launch; separation margin 127.9 s from the launcher's logout, `verified` |
| 08:01:04.995 | completion hook: `result=success exit_code=exited exit_status=0`; datagram received at +77 ms |

The window's journal was `collected`, with no session event: `verified`.

## The pairs

Warmups: 7,904.1 and 7,870.6 ms. Every row `pass`, validated against
its arm's executable and the sealed digest.

| pair | control ms | treatment ms | gain ms | gain % |
|---:|---:|---:|---:|---:|
| 0 | 7,943.2 | 7,885.9 | 57.2 | 0.72 |
| 1 | 7,941.9 | 7,886.6 | 55.3 | 0.70 |
| 2 | 7,943.0 | 7,908.7 | 34.3 | 0.43 |
| 3 | 7,979.6 | 7,880.4 | 99.2 | 1.24 |
| 4 | 7,942.8 | 7,938.5 | 4.3 | 0.05 |

Control median **7,943.0 ms**, control range **37.7 ms**; treatment
median 7,886.6 ms. **Bar `2 × 37.7 = 75.4 ms`, median gain 55.3 ms,
every pair positive: `not_established`.** Pair 3's control row, at
7,979.6 ms, is the range; without it the other four control rows span
1.3 ms. That is a reading, not grounds to change the rule. The control
sits 25.6 ms from ADR 601's treatment median (7,917.4 ms, the same
index form on another day and executable), an observed distance that
is not netted.

## The mechanism check, after the verdict

ADR 596's driver (`adr596-q21-build-write`) on the treatment executable
ran under the same procedure: launch 08:01:47 (session 2929, token
`cda01e709b0635ad6ab6773e`), gate at 37 °C after a 5,000.2 ms wait,
load 0.09, admitted; separation 128.2 s and window both `verified`
with no session event. Off median 7,958.1 ms, **off range 292.9 ms
(3.68%, within ADR 593's 4% bound)**, on median 8,071.1 ms, perturbation
accepted; every build-write and inner-1 fact and residual consistent
and complete. The owner rules returned `below_floor_no_mechanism`
(ADR 594's four-term owner `inner_1_build_write` at 525.9 median-scaled
ms against a 585.9 ms floor) and `unresolved_tie_within_off_range`
(ADR 585's fifteen-term owner `runtime_prep_inner_1` at 981.4).

| fact, treatment (measured) | value | ADR 601 treatment (historical) |
|---|---:|---:|
| **`lookup_form`** | **inline** | — |
| replay 1 `lookups_counted` / hits / misses | true / 14,656,608 / 15,686,933 | the same |
| **replay 1 `lookup_inline_hits`** | **14,656,608** | — |
| `index_mode` / `index_variant` / `index_build` | compact / int64 / finish | the same |
| `index_entries` / `index_expected_rows` / growths | 5,845,938 / 5,845,938 / 0 | the same |
| `resident_state_peak_bytes` | 207,482,040 | 207,482,040 |

Every hit came back as `One`, as it must on a build with no repeated
key, so the cut did what the plan said on every hit. The observed
median paired difference was 55.3 ms, under the bar.

The fifteen-term medians, median-scaled ms, as readings: runtime prep
of inner 1 981.4, semi write 846.8, anti write 699.1, anti read 388.8,
semi read 278.5, semi finish 248.1, anti finish 229.2, cleanup 29.6,
and every other term under 18.

## Implementation and verification

Engine (`exec/join/spill.rs`): `SpilledLookupForm {Collected, Inline}`
and `SpilledLookupMatches {Miss, One, Many}`, with `len()`, a test-only
`is_empty()`, and an exact-size, fused by-value iterator.
`SpilledBatchRefList::matches(form)` returns `One` only under `Inline`
with no overflow, else the collected `Vec`. The primitive and wrapper
index lookups take the form; the `Hash` variant and the legacy index
return their cloned `Vec` as `Many`, or `Miss`. On the build: the form
defaults to `Collected` on every constructor; `use_lookup_form` refuses
`Inline` without the compact index; `lookup` returns the owned value
and, under counting, counts inline hits on `One`. The six production
callers of `SpilledInnerJoinBuild::lookup` compile unchanged. The branch
(`exec/join/streaming.rs`) sets the form beside ADR 601's index form,
cleaning up on a refusal; the lookup-count collection carries `form` and
`inline_hits`. The receipt (`exec/node_attribution.rs`) carries
`inner_1_lookup_form` and `inner_1_lookup_inline_hits` from replay 1's
entry and checks them. An absent form carries no inline count. A present
form is `collected` or `inline`, with `inline` only on the compact index.
Counted, both counts are numeric, with no inline hits under `collected`
and no more inline hits than hits under `inline`; not counted, the inline
count is null. (Review round: the first cut treated a malformed present
form as absent and let a counted `inline` receipt pass without its
count; the rules above and their breaking fixtures were added.)
`toron-tpch`: the bands carry both facts.

Fixtures (four new, three extended): both forms on `Int64` and `Int32`
through the real write path, covering a unique key (`One` against the
one-element `Many`), a key repeated across pages (`Many` in insertion
order), an absent key and a mismatched variant (`Miss`), and counts of
three hits, two misses, and two inline hits against zero. Also the
legacy index refusing `Inline`, and the compact wrapper's `Hash` variant
under `Inline` still returning `Many`; the iterator's exact size hint
and fusing on every shape; and the receipt with both good shapes, a
receipt from before the form, and one breaking case per rule. The
existing lookup fixtures now compare explicit shapes, the ADR 600
collection fixture carries the two new keys, and the tpch band fixture
reads both facts.

Ladder on bigfoot (`RUST_MIN_STACK=67108864`, fmt clean on `toron` and
`toron-tpch`; the one pre-existing test-build warning on
`adr589_plain_write`), on the control tree and the treatment tree alike:
Toron library 1,836, architecture 2,151, and every other Toron suite
green; toron-tpch library 241 (+4 ignored), `toron-tpch` binary 133,
`toron-bench` 9, integration 261 (+2 ignored). The four `evidence::tests`
seal fixtures make signed commits in temporary repositories. They failed
under a non-interactive session with no SSH agent ("incorrect
passphrase"), and passed on a rerun of the toron-tpch suites with the
agent forwarded, on both trees. That is the build host's environment,
not the code.

Evidence on strix: `target/tpch/adr602-q21-lookup-inline/`
(`samples.jsonl` `ae3ec561f…`, `summary.json` `3bf6aaa12…`, `profiles/`),
`target/tpch/adr602-mechanism-check/` (`samples.jsonl` `d572bb916…`,
`summary.json` `a1363ab1f…`, `profiles/`), and
`target/tpch/adr602-strix-host-record/` (`launch.sh` `fbce624fa…`,
`launch_mechanism.sh` `d1ad22b3c…`, `snapshot.sh` `51b208557…`,
`host_config_before.txt` `bf1ee27db…`, `host_config_before_mechanism.txt`,
`driver_stdout.log` `0ceaabf10…`, `host_after.txt`). The scripts are
ADR 601's with the record's names, with two corrections: the launch
script's timer read names this unit's timer (ADR 601's named a stale
one), and the snapshot takes the output directory it checks as an
argument. **Two departures from ADR 601's record-keeping:** the paired
attempt's completion marker and driver logs were not moved aside before
the mechanism check, so `completion.marker` holds the mechanism check's
record and `driver_stdout.log`/`driver_stderr.log` hold both runs,
appended. The original paired marker was overwritten; its record
survives as the datagram the workstation's listener received, preserved
verbatim:
`adr595 token=a158f9e28552921d0a48cce0 result=success exit_code=exited exit_status=0 unix_ms=1790686864995 received_unix_ms=1790686865072 sender=192.168.0.8`. There was also no read-out snapshot
between the two runs; one `host_after.txt` (47 °C, load 0.49, spill
scratch empty) follows both.

## Closure

ADR 602 closes `not_established` under the rule it inherited. The cut
gained on every pair, a median of 55.3 ms, against a bar of 75.4 ms on
this run's 37.7 ms control range. The token reverts to `Collected`; the
implementation, the facts, and the rules stay, so a later record can
measure the form again or build on the owned lookup value. The mechanism
check says the allocation was removed on all 14,656,608 hits; the
observed median paired difference was 55.3 ms, which this rule does not
retain. This allocation cut did not clear the retention bar at this
coordinate. The fifteen terms, as readings, put runtime preparation of
inner 1 and the semi and anti writes first; the refreshed board and its
attribution select the next owner. The campaign's next record refreshes ADR 591's
sealed board on this tree, as the plan's forward section set out, and
the owner after it is chosen by absolute recoverable time on the
refreshed board.
