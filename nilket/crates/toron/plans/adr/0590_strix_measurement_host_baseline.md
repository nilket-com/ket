# ADR 590 - strix as the Toron measurement host: configuration freeze and sealed baseline board

**Status:** CLOSED, NOT SEALED. The freeze was verified and recorded, three
full boards ran on strix (results in
`plans/impl/0590_strix_measurement_host_baseline_results.md`), and none is
a candidate: one refused for a non-fresh output directory, two for the
stability gate on DuckDB q11, whose single high sample per board a
forty-sample diagnosis examined under placement restrictions on the
hybrid i9-14900K (P-core restricted: repeats to 2.8%; E-core restricted:
1.78x slower; unrestricted: 106-132 ms). Finding: the cell repeats under
the P-core restriction and not reliably without it, so thread placement
is a coordinate item on this host; scheduling across core kinds is the
supported hypothesis, not a demonstrated cause. No tuning was applied under this record. Successor: ADR
591, which records affinity in the artifact and pins the P-core set.
Successor coordinate to ADR 583; that board stays sealed as history and is
not re-derived.


## Context

Every Toron measurement since ADR 521 ran on slim, which is also the
operator's interactive workstation and the home of the `nilket-local`
dogfood deployment. The arc's controls managed that by stopping the
deployment, waiting for a quiet host, and pairing every treatment with an
adjacent control, and the rule held; but the workstation was locked out for
every window and the operator's own activity was the largest uncontrolled
source of drift (the ADR 589 rerun saw both arms move 2.8% between nights).
The workload has outgrown a shared machine.

Four other hosts are available and idle. The fleet default from 2026-09-15:
slim is interactive only; strix (192.168.0.8, i9-14900K, 24 cores / 32
threads, 36 MiB L3, 123 GiB, 3.6 TB NVMe) measures Toron; mini (i9-14900,
61 GiB) builds Toron and runs its ladders; bigfoot (i9-12900, 123 GiB) is
Polariton's; nano is Rune's. These are defaults, not a scheduler: a
distributed benchmark reserves its members together for its window.

Setup state, already done on the non-root side and verified: strix and
mini carry rustc 1.95.0 pinned by a `rustup override` on the checkout
(slim's `stable` is 1.95.0); the checkout is pushed from slim over the LAN
(`fd0fe81d`, tree `df8aed7b…`); slim's cargo linker configuration (clang +
mold) is byte-identical on all four hosts; `clang 21.1.8`, `mold 2.40.4`,
binutils 2.46, glibc 2.43, kernel `7.0.0-31-generic` agree on slim, mini,
and strix; `perf_event_paranoid = 2` is persisted on strix, mini, and
bigfoot; the strict SF=8 dataset is on strix at slim's absolute path
(the dataset manifest embeds absolute paths and the same `$HOME` resolves
them), verified `full_hash` against `fnv1a64:0fc5c06d19c5ed23`. A release
build on mini reproduces slim's recipe hash `9af51cbc…` exactly. That
proves agreement on what the recipe records (profile, features, target,
rustc, rustflags, `Cargo.lock`); it does not prove identical linker or
system-library versions, which are captured separately above, nor a
bit-reproducible binary, which is unchecked: executables are identified by
their own hash, never by the recipe. One q21 smoke run on strix through
`toron-bench run` passed with the sealed digest and full-hash
verification; it establishes execution and value parity on strix and
nothing about performance.

Choosing strix is a new measurement coordinate. Moving slim into a closet
would not by itself have been; changing hardware or relevant configuration
is. ADR 583's board, the 584-589 controls, and the 589 owner selection
(`runtime_prep_inner_1`) are slim facts; none carries over.


## Decision

**strix's configuration is frozen before its first measurement, and the
freeze is the coordinate.** Headless: default target `multi-user.target`,
the display manager stopped and disabled (`gdm` inactive, so it cannot
respawn a session), and the currently running GNOME session terminated;
verified before any run as no graphical login session in
`loginctl list-sessions` (no session of type `x11` or `wayland`, no seat
session on a tty), no `gnome-shell` process, and `gdm` inactive. The
operator's SSH sessions, including the one that launches the run, are
allowed; a PTY session is not a graphical one. Disabling the boot target
or terminating one session does not on its own keep the display manager
from returning; the check covers all three. CPU policy retained as found and recorded precisely:
`intel_pstate`, governor `powersave`, energy-performance preference
`performance`, turbo on (`no_turbo = 0`, `hwp_dynamic_boost = 0`), SMT on,
5.7 GHz max, THP `madvise`, default mitigations, microcode `0x133`. No CPU
tuning is added before repeatability has been seen; a later change to any
of these is a new coordinate under its own record.

**Build on mini, measure on strix, never both on one host in one window.**
The executable, its build manifest, and the staged DuckDB are produced on
mini by `toron-bench build --profile release --duckdb-bin <duckdb 1.5.4>`
and copied to strix at the same absolute paths; `toron-bench run` verifies
the staged hashes before launching. During a measurement window strix runs
nothing else: no compilation, no ladders, no other agent's jobs; the
quiet-host gate (load below 0.8, no process above 5% CPU) is checked before
each run as on slim.

**The baseline is ADR 583's board, run once on strix, sealed.** q01 through
q22, Toron then DuckDB per query, 1 warmup plus 5 measured samples per cell,
strict SF=8 persisted Parquet, explicit 2 GiB task memory and 2 GiB spill
reserve, `TORON_PARTITION_PARALLELISM=1`, DuckDB 1.5.4 at one thread from
the staged executable (`c478c0f4…`, the same bytes as ADR 583's control),
diagnostics disabled, an empty dedicated join-spill scratch before and
after, sealed through `seal-evidence`. The same full-board integrity gate
applies unchanged.

**The host record is part of the evidence.** The raw configuration
snapshot taken immediately before the run (the items above plus
`/proc/cmdline`, the login-session listing, the display-manager state, the
user-service count, and the load and package temperature at start and end)
and the tool-version snapshots of the build host and the run host are
retained as files beside the board's artifacts, and their SHA-256 hashes
are recorded in the results document next to the executable hashes; the
results document also quotes them readably. That gives the baseline an
auditable host record without a manifest-schema change; writing host
identity into the run manifest is named below as forward work.

**Comparison with ADR 583 is host-to-host and explanatory only.** The two
boards share the DuckDB bytes and the dataset fingerprint and differ in
host and Toron tree; the per-cell table is reported side by side with both
identities, and no Toron change is inferred from it. strix's board becomes
the canonical baseline for the arc's next owner selection, which is by
absolute Toron-minus-DuckDB gap on strix; the slim candidate is a
hypothesis strix's attribution must confirm.

**Repeatability is read from the board's own spread**, per cell (min, max,
spread ratio, and the stability gate), and stated as the host's first
repeatability reading; it is not compared to slim's and no tuning follows
from it under this record.


## Acceptance

- Before the run, the recorded configuration matches the freeze item for
  item, the session listing shows no graphical login session, no
  `gnome-shell` process exists, and `gdm` is inactive; the results
  document quotes the check, and SSH sessions are not counted against it.
- The build manifest on strix verifies (staged Toron and DuckDB hashes) and
  its recipe hash is `9af51cbc…`; tool versions on mini and strix are
  recorded and agree.
- All 264 samples pass, all 44 summaries are stable, every row carries the
  dataset fingerprint under `full_hash`, `diagnostics_disabled`, the
  parallelism pin with an empty observation list, and DuckDB rows carry
  thread setting `1`, version 1.5.4, and the staged hash.
- The board seals with final authority: archived executables, an evidence
  tag, and a sealed manifest.
- The results document reports both aggregates and the per-cell table
  against ADR 583's sealed table, labelled host-to-host, and names the next
  owner by strix's absolute gap.
- The raw configuration and tool-version snapshots are retained beside the
  board's artifacts and their hashes appear in the results document.
- Plan commit, then one implementation/results/closure commit; the evidence
  tag names the measured tree; nothing else lands under this number.


## Guardrails

- Root-only steps (the default target, stopping and disabling `gdm`,
  terminating the session) are the operator's; the record states what was verified, not what was requested.
- No engine or harness code changes ride this record. If the board cannot
  run without one, that is a finding, and the change gets its own commit
  under the appropriate record before this one measures.
- slim runs nothing for this record. Cross-host bit-reproducibility of a
  build stays unchecked and is stated as such.


## Risks

- A single board is one reading of strix's repeatability; the arc's paired
  rule (median gain above twice the control range, every pair positive)
  remains the retention test, and the range is measured per ADR, not
  assumed from this board.
- A graphical session may return (autologin, a display manager re-enabled
  by an update); the pre-run check is repeated before every window, not
  once.
- Thermal behaviour under a 22-query board is unmeasured on this cooler;
  package temperature is recorded at start and end.


## Forward

- Host identity in the run manifest (hostname, CPU model, kernel, microcode,
  governor, EPP, turbo, SMT, memory) so a sealed board names its host by
  content rather than by prose; a schema bump with pinned-bytes tests.
- Attribution of whichever query has the largest absolute Toron-minus-DuckDB
  gap on strix's board, before any cut; ADR 584/585's q21 drivers apply
  only if q21 remains the selection, since slim's owner does not carry
  over.
- Distributed boards: define the workload and the metric first
  (fixed-workload latency, throughput, or scaling efficiency; latency is
  not compared against a sum of single-node latencies), and record each
  node's hardware, data placement, and network configuration. The per-host
  record this ADR introduces is the member-level input; heterogeneous
  members are the intended case, not a defect.
