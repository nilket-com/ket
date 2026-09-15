# ADR 592 - q21 attribution on strix's sealed coordinate: results

Measured 2026-09-15 on strix under ADR 591's coordinate (ADR 590's freeze,
affinity `0-15`), one instrumented release executable built on mini from
this tree, both drivers bound to ADR 591's sealed manifest by its contents
and every child validated as it returned. **Phase 1 (ADR 584's rule)
resolves `outside_drive` as q21's owner on strix: 4,416.3 median-scaled
ms, 48.93% of a 9,024.9 ms off median, on an accepted profile (median
absolute movement 0.004) with every profile complete.** **Phase 2 (ADR
585's rule over the fifteen-term band) does not resolve the envelope:
`runtime_prep_inner_1` leads at 1,334.5 ms (14.79%) and clears the floor,
but `runtime_prep_semi_write` at 849.6 ms lies within the off range
(609.0 ms) of it, and the rule reports the tie unresolved.** Under the
record's transition rule, phase 1's owner is the envelope and phase 2's
finding is that the envelope's owner is unresolved between those two
terms; nothing is promoted. On slim the same rule had returned
`runtime_prep_inner_1` outright (ADR 589, 1,671.4 ms against a 500 ms
floor); on strix that remains a hypothesis, now with a named rival.

## Identity

| identity | value |
|---|---|
| source tree (instrumented) | `ff0b1c550ee7483b6682fcf82cb84e43d94511ac` (= `10abb590`, the plumbing before this record was folded in; the closing commit differs in the plan documents alone) |
| instrumented executable | `84da5eaa08f4a565e726c0d6ff0762194373260443e8cdb0e1418dc86f4f65ea`, both arms of every pair, both phases |
| build manifest | `target/tpch/evidence/build/adr592-instrumented.manifest` (mini and strix; staged hash verified on strix) |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` (slim's) |
| sealed baseline (bound) | manifest `target/tpch/adr591-strix-board/perf_board.jsonl.evidence.sealed` (`19c8a91a…`), tree `a962f3d8…`, executable `950ebc4b…`, DuckDB `c478c0f4…`, artifact `d03935eb…` re-hashed and equal, q21 Toron median 9,126.9 ms, its digest, dataset `fnv1a64:0fc5c06d19c5ed23`, affinity `0-15` explicit |
| build host / run host | mini / strix |

The sealed executable `950ebc4b…` is the historical reference; every
child row named `84da5eaa…` as the executable that ran, the instrumented
manifest as the one it was launched from, and `84da5eaa…` as the launched
hash, and carried the sealed q21 digest, `full_hash` on the baseline's
dataset fingerprint, `parquet`, the sealed coordinate fields, affinity
`0-15` / `explicit` / `0-15`, and diagnostics disabled on the off arm and
enabled on the on arm (22 rows, both warmups included). The ADR 585
summary's `adr584_instrumented_executable_sha256` field still reports
ADR 585's slim constant `a173e825…`; it is a label from that record, not
this run's, whose phase-1 executable is `84da5eaa…` above.

## The coordinate, verified

Freeze re-verified before each phase from the snapshots: headless
(`multi-user.target`, `gdm` inactive and disabled, no `gnome-shell`, only
the launching SSH session), `intel_pstate` `powersave` with EPP
`performance`, turbo and SMT on, THP `madvise`, `perf_event_paranoid = 2`,
`/sys/devices/cpu_core/cpus` = `0-15`, spill scratch empty, output
directory absent. Phase 1 started 14:28:48 CDT at load 0.00 and 42 °C and
ended 14:31:16 at 68 °C (read immediately after); phase 2 started
14:32:16 at load 0.23 and 41 °C and ended 14:34:38 at 46 °C. Host record
on strix under `target/tpch/adr592-strix-host-record/` (SHA-256):

| file | sha256 |
|---|---|
| `phase1_host_config_before.txt` | `93fec870869dc3c889cbebeacb94ea73c25a48a8317de89984d0d02e8ada0f78` |
| `phase1_host_after.txt` | `da9ad79773a3cb5083d612ae49ddc24ddf3f0da6a7d45e64f285cbf9ee01102f` |
| `phase1_stdout.log` | `815230734b0717d7c89055b3c74bd8144d80722012db4d45c6f8a421af652557` |
| `phase2_host_config_before.txt` | `5e4ed12e3caae6db3f933f82faca4e724b1f90181beca996bb6b164cf104b738` |
| `phase2_host_after.txt` | `7d5da9e7e664a929ba9d3d2dfe0ae39adc97c7d44e44de1d2b94c4ad00a4a878` |
| `phase2_stdout.log` | `82c3c90397958416f99684125734d897485bdcdc89931d05068dd628ea198821` |
| `tool_versions_run_host.txt` | `d88e2dc951f15e4c64bf2f6550b699e6543a8a30c19403f4bb35f0b60981acfd` |

Artifacts: `target/tpch/adr592-q21-attribution/` and
`target/tpch/adr592-q21-decomposition/` (each `samples.jsonl`,
`summary.json`, `profiles/`), unsealed, no candidate authority.

## Phase 1: ADR 584's attribution

Warmup (off) 9,001.5 ms. Pairs, off then on in milliseconds, movement
`m_i`: pair 0 (off, on) 9,016.5 / 9,053.0, +0.40%; pair 1 (on, off)
9,019.7 / 9,046.2, +0.29%; pair 2 (off, on) 9,024.9 / 9,091.5, +0.74%;
pair 3 (on, off) 9,032.1 / 9,062.9, +0.34%; pair 4 (off, on) 9,052.2 /
9,113.4, +0.68%. Off median **9,024.9 ms**, off range **35.7 ms**
(relative spread 0.004). Perturbation gate: median `|m_i|` **0.004**
against 0.25, accepted. Every profile complete (in-drive residual within
its bound, `outside_drive` non-negative on every pair). The off median
sits 102.0 ms (−1.1%) from the sealed q21 median 9,126.9 ms, an observed
distance that carries host and time variability as well as the
plumbing's difference.

Owner floor `max(500 ms, 2 × 35.7) = 500 ms`.

| term | median-scaled ms | share of off median | clears floor |
|---|---:|---:|:---:|
| `outside_drive` | 4,416.3 | 48.93% | yes |
| `inner_replay_1` | 2,101.4 | 23.29% | yes |
| `inner_replay_0` | 1,248.3 | 13.83% | yes |
| `base_scan` | 503.3 | 5.58% | yes |
| `inner_replay_2` | 402.3 | 4.46% | no |
| `in_drive_residual` | 178.5 | 1.98% | no |
| `semi_replay_3` | 98.2 | 1.09% | no |
| `anti_replay_4` | 77.0 | 0.85% | no |
| `grouped_fold` | 1.6 | 0.02% | no |

**Owner: `outside_drive`, resolved**, 2,314.8 ms ahead of `inner_replay_1`
against a 35.7 ms range. On slim ADR 584 read `outside_drive` at 7,887.0
ms, 55.16% of a 14,298 ms off median, with `inner_replay_1` next at
3,321.7 ms (23.23%); on strix the order is the same and the shares are
48.93% and 23.29%. Host-to-host, tree-to-tree context: the preparation's
share fell across the two hosts and the six ADRs between the trees, and
which of those moved it is not established here.

## Phase 2: ADR 585's decomposition

Warmup (off) 9,368.0 ms. Pairs: pair 0 (off, on) 8,955.8 / 9,051.7,
+1.07%; pair 1 (on, off) 9,023.8 / 9,130.2, +1.18%; pair 2 (off, on)
9,014.2 / 9,127.4, +1.26%; pair 3 (on, off) 9,103.0 / 9,504.4, +4.41%;
pair 4 (off, on) 9,564.8 / 9,451.0, −1.19%. Off median **9,023.8 ms**,
off range **609.0 ms** (relative spread 0.067). Perturbation gate: median
`|m_i|` **0.012**, accepted. Every profile complete; the three bands
reconciled on every on row (`outer_residual` 0.9 ms, `lifecycle_residual`
0.7 ms, `runtime_prep_residual` 18.4 ms median-scaled). Off median
103.1 ms (−1.1%) from the sealed row.

The last two pairs ran slower on both arms (pair 3's on arm 9,504.4 ms,
pair 4's off arm 9,564.8 and on arm 9,451.0 ms) than the first three,
and the warmup was slow too; phase 1, two minutes earlier under the same
coordinate, held a 35.7 ms range across its five off samples. No cause is
attributed: the snapshots show nothing changed on the host between the
phases, the package was at 41 °C at phase 2's start and 46 °C after it,
and no placement, timer, or thermal reading was taken during the run.
The range is what it was, and the rule is applied to it.

Owner floor `max(500 ms, 2 × 609.0) = 1,218.0 ms`.

| term | median-scaled ms | share of off median | clears floor |
|---|---:|---:|:---:|
| `runtime_prep_inner_1` | 1,334.5 | 14.79% | yes |
| `runtime_prep_semi_write` | 849.6 | 9.42% | no |
| `runtime_prep_anti_write` | 701.7 | 7.78% | no |
| `cleanup` | 424.7 | 4.71% | no |
| `runtime_prep_anti_read` | 391.2 | 4.34% | no |
| `runtime_prep_semi_read` | 279.3 | 3.09% | no |
| `runtime_prep_semi_finish` | 247.7 | 2.75% | no |
| `runtime_prep_anti_finish` | 228.3 | 2.53% | no |
| `runtime_prep_residual` | 18.4 | 0.20% | no |
| `runtime_prep_inner_2` | 3.6 | 0.04% | no |
| `base_prep` | 2.0 | 0.02% | no |
| `outer_residual` | 0.9 | 0.01% | no |
| `lifecycle_residual` | 0.7 | 0.01% | no |
| `finalize` | 0.5 | 0.01% | no |
| `outside_handler_span` | 0.3 | 0.00% | no |

`drive_inclusive` 4,532.6 ms median-scaled beside phase 1's drive
(9,024.9 − 4,416.3 = 4,608.6 ms), the non-drive envelope 4,487.2 ms
beside phase 1's `outside_drive` 4,416.3: two runs, two instruments,
reported beside each other and not netted. The shared-source candidate's
removable read, priced pairwise as ADR 585 fixed, is 279.3 ms, below the
floor: closed below floor here as it was on slim.

**Envelope owner: unresolved.** `runtime_prep_inner_1` clears the floor
and no other term does, but the rule's tie condition holds: the leading
term's margin over `runtime_prep_semi_write` (484.9 ms) is inside the off
range (609.0 ms), and the rule reports that as `unresolved_tie_within_off_range`
rather than breaking it. No residual leads, so the instrument is not the
finding.

Beside slim's readings (ADR 585 on the pre-586 tree; ADR 589 on the
order-aware tree), host-to-host, tree-to-tree:

| term | strix (this record) | slim ADR 589 | slim ADR 585 |
|---|---:|---:|---:|
| `runtime_prep_inner_1` | 1,334.5 | 1,671.4 | 1,810.2 |
| `runtime_prep_semi_write` | 849.6 | 952.8 | 2,323.1 |
| `runtime_prep_anti_write` | 701.7 | 790.3 | — |
| `cleanup` | 424.7 | 561.0 | — |
| `runtime_prep_semi_finish` | 247.7 | 302.3 | — |
| `runtime_prep_anti_finish` | 228.3 | 277.4 | — |

## The selection under the transition rule

Phase 1 names `outside_drive`, so q21's owner is the envelope, and the
envelope's owner is whatever phase 2 returns; phase 2 returns an
unresolved tie. **q21's owner on strix is therefore unresolved between
`runtime_prep_inner_1` and `runtime_prep_semi_write`**, with
`runtime_prep_inner_1` the leading term. slim's `runtime_prep_inner_1`
is not confirmed and not refuted: it leads on strix too, by 484.9 ms,
under a range that does not let the rule call it. Nothing is promoted,
no term is chosen by preference, and no optimization opens under this
record.

## Implementation and verification

Plumbing, in the harness only: `adr592_load_sealed_baseline` reads the
sealed run manifest, requires `sealed` with final authority, re-hashes
the artifact against the manifest's `artifact_sha256`, and reads the
header's dataset fingerprint and explicit affinity and the passing q21
Toron cell's median and digest; `adr592_bind_driver_coordinate` requires
the process affinity to be explicit and the baseline's and the loaded
dataset to be the baseline's; `adr592_validate_child_row` is the one
validator both drivers apply to every child as it returns (warmup
included), checking pass status, q21 on Toron, the sealed coordinate
fields, the arm's expected `diagnostics_disabled`, the affinity triple,
the digest, `full_hash` on the expected fingerprint, the data source,
the executable identity against the instrumented manifest, and the
launch manifest and launched hash; `adr592_admit_child_row` stamps the
launch identity, journals a failing row, and stops the driver before
another child. Both drivers accept `--sealed-manifest` and stamp the
bound baseline, the affinity, and the off arm's distance from the sealed
median on their summaries; without the flag they report the slim
constant as before, which this record does not use. Tests: the binding
admits the fixture and names each mismatch (artifact hash, unsealed,
not final, no q21 Toron cell, failing cell, missing digest, missing or
default affinity, no authority); the driver coordinate refuses a
different or default affinity and a different dataset; the child
validator admits an off and an on row and refuses nineteen single-field
defects by name, the arm's diagnostics expectation included.

## Review round, and the retained rows under the tightened plumbing

Two gaps were closed under review after the run, in the closing commit.
First, a child that did not pass used to be returned as an error by the
launchers before the validator saw it, so it was never journaled; the
launchers now return the row as it is, the validator refuses it, and the
driver journals it with its refusal (`adr592_refusal`) and stops. The
paired-children loop both drivers share (`adr592_run_paired_children`)
takes its launcher as a parameter, so the contract is tested on the
driver path: a failing warmup is journaled with its refusal and no
second child launches; a pair row observed on another mask stops the run
after the three admitted rows before it; a complete run journals eleven
admitted rows in the alternating order. Second, the baseline loader now
binds the sealed artifact's coordinate, not only its dataset and
affinity: mode, data source, scale, both budgets with their provenance,
and the parallelism pin with its provenance must be this record's, and
the artifact header's own executable and tree identity must be the
manifest's, whose labels the summary reports; a fixture per field, absent
and other, and three identity fixtures.

The two retained journals were re-examined under the tightened plumbing
on strix by an executable built from the closing code on mini
(`1a7de226d1069ee070f3c67085574d8b2e88c24aeb52ce2715233d0f229cfa37`,
manifest `evidence/build/adr592-revalidate.manifest`) through a new
`adr592-revalidate` verb, which loads and binds the sealed baseline with
the tightened loader, reads the instrumented manifest, and runs the child
validator over every journaled row with the arm's diagnostics
expectation: phase 1, 11 rows, all admitted; phase 2, 11 rows, all
admitted; baseline `950ebc4b…`, instrumented `84da5eaa…`, journals
unchanged (`2c9eab41…`, `718d6f3b…`). Reports retained beside the host
record: `phase1_revalidation_under_tightened_gate.json`
(`e7641fee9a0f3890d1f798cca2bfcb708a18dd0ed6af676935ed53743bfa623c`) and
`phase2_revalidation_under_tightened_gate.json`
(`e678a06b8274612f5b050f0974d7d006369f3b365643dabc56593ef6b902eb43`).
The unresolved result is unchanged and no board or pair was rerun.

Ladder on mini (`RUST_MIN_STACK=67108864`, fmt clean), passing with the
ADR 538 atlas-driver exclusion tracked under ADR 591's finding: Toron
library 1,806, architecture 2,151, integration 3,649; toron-tpch library
231 (+4 ignored, 1 excluded), `toron-tpch` binary 102, `toron-bench` 9,
integration 261 (+2 ignored).

## Closure

ADR 592 closes with q21's owner on strix unresolved between
`runtime_prep_inner_1` (leading, 1,334.5 ms) and `runtime_prep_semi_write`
(849.6 ms) under a 609.0 ms off range, phase 1 having resolved
`outside_drive` (48.93%) cleanly.

The successor is not another identical decomposition run: a new number
does not license rerunning until an owner resolves. What is known about
the range is concrete enough to instrument. The slow samples were the
last two pairs and the warmup of a run that began one minute after phase
1 ended with the package at 68 °C, on one continuously loaded P-core;
phase 1's five off samples, two minutes earlier, spanned 35.7 ms. The
hypotheses that fit are thermal or frequency (a sustained-load clock
limit late in the run), a background event in the window (the host's
timers include `sysstat-collect` every ten minutes and `fwupd-refresh`;
neither was checked against the sample times), and a scheduling effect
inside the P-core set (the pin excludes the E-cores, not migration among
P-cores); none is supported or excluded by evidence taken here. The
successor record therefore adds per-child host readings to the driver
rows before it measures anything: package temperature and the P-cores'
current frequency at the child's start and end, the child's voluntary and
involuntary context-switch counts, and the system journal's events in
the child's window; it predeclares that a run whose off range exceeds a
stated bound closes on those readings, not on an owner; and only then
runs ADR 585's decomposition once. If that run resolves the envelope, the
owner it returns names the cut's record; if it returns
`runtime_prep_inner_1`, that term has no lifecycle decomposition yet and
gets one before any cut. Nothing else under this number.
