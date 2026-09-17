# ADR 599 - q21's replay-1 build write: the Exchange-skip experiment relaunched under a declared gate procedure: results

Measured 2026-09-16 on strix: one paired attempt under ADR 595's
separated launch, ADR 597's twelve-row driver, and this record's
declared gate procedure (a 5 s wait between the launch environment's
admission and the gate's reading, threshold unchanged). The gate
admitted; twelve rows ran; every row's q21 digest equals the sealed
digest. **Verdict, by ADR 588's rule as ADR 597 fixed it:
`not_established`.** Every pair was positive — gains of 82.2, 189.4,
139.6, 138.4, and 152.0 ms, median **139.6 ms (1.54% of the control
median)** — but the control range was 233.7 ms, so the bar was
467.4 ms, and the median does not clear it. The treatment call site is
reverted in this commit; the candidate is closed under this record
with its gain on the record. The mechanism check on the treatment
(ADR 596's driver, after the verdict) shows the cut doing what it was
built to do: pass-through admitted on every pair, no repartition,
1,465 pages in place of 5,860, `inner_1_build_write` at 778.5 ms
against ADR 596's 879.6.

## Identity

| identity | value |
|---|---|
| control executable | `511aa6118c5ec0c79fd5c02ef1bc4cd8ea3fb94f772e8c7f0217792bc9139d44`, tree `1c42b6a0bc82ccaf53007745db16eda34674c3e9`, commit `e6f23902` (this record's implementation; the spilled branch at `Split`) |
| treatment executable | `49791068e36f4a3046e9f295f52a106717ee0e2771682d5f2d2305595a1b16d5`, tree `38d4f266823e413fa856e01140ae30653d5e1f2b`, commit `63e8a81f` (the control tree plus the one call site at `PassThrough`: the diff is 4 insertions and 5 deletions in one hunk of `exec/join/streaming.rs`, one of them the argument) |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580`, both arms; ADR 588's manifest gate admitted the pair (same recipe, different trees, different executables, no untracked inputs) |
| closing tree | the control tree with this document, the ADR's status, and the call site's comment updated; the treatment call site is not in it |
| predecessors | ADR 597's `bebcf790…` / `64c2e713…`, never run; rebuilt here so the driver carries ADR 598's stamps |
| launcher | `toron-bench` `23267312…` (ADR 598's, unchanged since), which verified 99,406,024 bytes in 45,711 µs under mask `0-31` |
| sealed baseline (bound by content) | `950ebc4b…` on tree `a962f3d8…`, artifact `d03935eb…`, q21 median 9,126.9 ms, affinity `0-15`, dataset `fnv1a64:0fc5c06d19c5ed23` |
| driver | `adr597-q21-exchange-skip` (ADR 597's, by reference) with `--gate-settle-seconds 5`; unit `adr599-exchange-skip.service` under `user@1002.service/app.slice`, `Linger=yes`; `Type=exec`, `RuntimeMaxSec=10min`, `TimeoutStopSec=30s`, `KillMode=control-group`; timer `OnActiveSec=120`, `AccuracySec=1s` |
| build host / run host | bigfoot / strix |

## The launch, the gate, and the window

| instant (CDT) | what |
|---|---|
| 19:52:50 | preflight: the completion hook's datagram from strix reached the workstation's listener |
| 19:52:57.292 | `ADR595_LAUNCH_UNIX_MS`, launch session 2314, token `94b3817446b08ce7ed4e2b04`, delay 120 s; the session exited (logout 19:52:57.407) |
| 19:54:57 | timer trigger; the launcher's hash ended at 19:54:57.953 |
| 19:54:57.955 | `at_entry`: **50 °C**, one P-core at 5.70 GHz, 2 ms after the hash's end; then the scope and the launch environment's admission (driver start 19:54:57.963, 963 ms after the trigger's second) |
| 19:54:57.96–19:55:02.96 | the declared wait: requested 5 s, performed, elapsed 5,000.2 ms on the monotonic clock, nothing of the driver's own in it |
| 19:55:02.963 | `at_gate`, the first act after the wait and the one reading judged: **38 °C**, every masked core at 800 MHz, load 0.29 against the 0.3 bound — **admitted**; `hash_end_to_gate_ms` 5,010.0 |
| 19:55:05.444 | first child's launch (the control's warmup); separation margin 128.0 s |
| 19:57:24.809 | last child's return; journal collected `collected`, 8 entries, 0 malformed: session 2314's close, deactivation, logout, and removal in the separation interval; three wireless `UFW BLOCK` / `rtw88` entries, no session event, in the window — **`verified` / `verified`** |
| 19:57:25.863 | completion hook: `result=success exit_code=exited exit_status=0`; datagram received at +8 ms |
| 19:57:47 | read-out session 2315; unit and timer inactive; spill scratch empty; package 45 °C, load 0.82 |

**The endpoint observations, causality unestablished.** The entry
reading was above the threshold (50 °C) and the gate reading after the
wait below it (38 °C) with the load 0.01 under its bound; the gate
admitted. `at_entry` is taken before the scope and the launch reads,
so it is not what the old placement's gate would have read, and one
run says nothing about why the two readings differ or whether the wait
was what separated them; the record reports them and the verdict. The
mechanism check's driver, launched the same way 61 s after the read-out
session, read 48 °C at entry and 40 °C at its gate, load 0.05.

## The pairs

Warmups: control 8,996.6 ms, treatment 9,160.4 ms. Every row `pass`,
digest `ArrowIpcDigest([253, 211, 102, 221, …])` equal to the sealed
digest on all twelve, every row launched from its arm's executable and
validated as it returned.

| pair | order | control ms | treatment ms | gain ms | gain % |
|---:|---|---:|---:|---:|---:|
| 0 | C,T | 9,219.5 | 9,137.3 | 82.2 | 0.89 |
| 1 | T,C | 9,251.9 | 9,062.5 | 189.4 | 2.05 |
| 2 | C,T | 9,022.9 | 8,883.4 | 139.6 | 1.55 |
| 3 | T,C | 9,018.2 | 8,879.8 | 138.4 | 1.53 |
| 4 | C,T | 9,088.3 | 8,936.3 | 152.0 | 1.67 |

Control median **9,088.3 ms**, control range **233.7 ms** (9,018.2 to
9,251.9; the cutoff ratio 0.0257, within the 4% bound), treatment
median 8,936.3 ms. **Bar `2 × 233.7 = 467.4 ms`; median gain
139.6 ms; every pair positive; `not_established`.** The control median
sits 38.6 ms (−0.4%) from the sealed q21 median and 101.5 ms (+1.1%)
from ADR 596's off median, observed distances. Readings in run order:
package 84–91 °C at children's start readings, 71–89 °C at end;
involuntary switches 68–102; inter-child gaps 10–80 ms; the treatment
rows' voluntary context switches 2,941–2,943 against the control rows'
11,732–11,733 (a count, reported, not netted).

## The mechanism check, after the verdict

ADR 596's driver (`adr596-q21-build-write`, its own off/on arms) on the
treatment executable, launched under the same procedure at 19:58:28
(session 2321, token `b9288598…`), gate admitted after the wait, eleven
rows, separation 128.3 s and window `verified`, off range 80.6 ms
(0.91%), perturbation 0.94% accepted, seventh band consistent on every
pair with `exchange_handling = pass_through`, `pass_through_requested`
true, no refusal, and `facts_vintage adr597`:

| fact, treatment (measured) | value | ADR 596 (split, historical) |
|---|---:|---:|
| batches read / written | 1,465 / 1,465 | 1,465 / 5,860 |
| chunks / pages | 1,465 / 1,465 | 5,860 / 5,860 |
| repartition calls | 0 | 1,465 |
| plaintext / ciphertext bytes | 24,301,512 / 24,324,952 | 27,148,512 / 27,242,272 |
| index entries, mode | 5,845,938, legacy | 5,845,938, legacy |
| `writer_peak_bytes` | 17,160 | not recorded |
| `scratch_plaintext_capacity_peak` | 33,152 B | not recorded |
| `scratch_refs_capacity_peak` | 7,962 entries | not recorded |
| `resident_state_peak_bytes` | 514,525,240 | not recorded |
| `scratch_releases` | 0 | not recorded |

Terms, median-scaled ms, treatment against ADR 596's split (historical,
another run, never netted): `inner_1_build_write` **778.5** (879.6);
`build_write_index` 489.6 (482.4); `build_write_scan_wait` 178.1
(179.0; open 0.24, poll 177.9); `build_write_encode` 42.0 (51.4);
`build_write_key_extract` 31.5 (28.4); `build_write_page_write` 18.7
(33.7); `build_write_finish` 13.4 (13.0); `build_write_residual` 4.5
(14.0); `build_write_create` 0.3; filter, repartition, project 0. The
sixth band's other children as before (partition sizing 262.8, sizing
scan 196.7). ADR 594's rule on these rows resolves `inner_1_build_write`
and ADR 596's returns `below_floor_no_mechanism` with the index at
489.6 against a 500 ms floor. The memory facts are the treatment's,
measured on this run; the split path's capacities and residency were
never recorded (ADR 596's executable predates the facts and this
record's control arm ran unprofiled), and are reported as unavailable,
not inferred. `resident_state_peak_bytes` is the legacy index's
accounted bytes over 5.85 million entries plus the 64 KB reserve and
the 17 KB plaintext peak; `scratch_refs_capacity_peak` is a capacity in
entries of `(JoinKey, u32)`, as the ledger records it.

## Implementation and verification

`--gate-settle-seconds` on the lifecycle driver (`adr599_parse_gate_settle`:
default 0; missing value, non-integer, negative, or over 600 s refused,
never defaulted, the raw text kept on the refusal), the wait placed
after the launch environment's admission and before the gate's reading
with `gate_settle` (requested seconds, `wait_performed`, monotonic
start, end, elapsed) on the summary and on `refusal.json`, and the
driver's `sleep` injectable beside its scope and gate
(`LifecycleHostProbes`). Fixtures: the flag's parsing in each case; the
order scope → launch admission → wait → gate recorded by injected
probes on the separated driver with a 5 s setting, the refusal at the
gate carrying the performed wait beside the gate's reading; the default
performing no wait and recording `wait_performed: false` with a zero
elapsed; a failed launch admission invoking neither the sleep nor the
gate, its refusal recording the setting, the wait not performed, and
absent gate evidence; a bad value refused with the raw setting before
anything runs; a scope refusal before the wait. (Review round: the
first cut reported `wait_performed: true` for the zero default and
proved the order on a non-separated driver; both corrected, the
launch admission made injectable for it.) Nothing else changed: the
engine, the admission, the facts, the driver, the contract, and the
rule are ADR 597's.

Ladder on bigfoot (`RUST_MIN_STACK=67108864`, fmt clean), on the
control tree and the treatment tree alike: Toron library 1,817,
architecture 2,151, integration 3,649; toron-tpch library 241 (+4
ignored), `toron-tpch` binary 131, `toron-bench` 9, integration 261 (+2
ignored).

Evidence on strix: `target/tpch/adr599-q21-exchange-skip/`
(`samples.jsonl` `85394c285e7f6357d…`, `summary.json` `2c8ce427aa0d75888…`),
`target/tpch/adr599-mechanism-check/` (`samples.jsonl`
`764b00048fc9b78f9…`, `summary.json` `b5843d41f94ea0def…`, `profiles/`),
and `target/tpch/adr599-strix-host-record/` (the paired attempt's
`completion.marker` `f7ca53e8…`, `driver_stdout.log` `088c38d7…`,
`host_config_before.txt` `b6332872…`, `host_after.txt` `9195c3e7…`
under `paired-attempt/`; the mechanism check's `completion.marker`
`20972d08…`, `driver_stdout.log` `3bb3122a…`,
`host_config_before_mechanism.txt` `c70cc183…`, `host_after_mechanism.txt`
`af494f8f…`; `launch.sh` `3f035e0d…`, `launch_mechanism.sh` `9ccf7e63…`,
`snapshot.sh` `bc9dcc2d…`, `tool_versions_run_host.txt` `d607c81f…`).

## Closure

ADR 599 closes `not_established` under the rule it inherited: the cut
gained on every pair, a median 139.6 ms and 1.54% of q21, and the rule
asks for 467.4 ms on this run's control range. The treatment call site
is reverted in this commit; the parameter, the admission, the facts,
the fixtures, the driver, and the gate procedure's flag remain. The
mechanism check says what the cut did — one page per batch, no
repartition, 2.9 MB fewer ciphertext bytes, the write 101 ms shorter
on the treatment's own profile than on ADR 596's — and the paired
pairs say what it was worth at q21's scale on this run; neither is a
claim the rule did not make. The declared gate procedure admitted the
run on the first attempt with both endpoint readings on record and
nothing established about them. The candidate stays closed under this
record; the legacy-index candidate is its own.
