# ADR 592 - q21 attribution on strix's sealed coordinate

**Status:** CLOSED, OWNER UNRESOLVED. Measured on strix under ADR 591's
coordinate with both drivers bound to the sealed baseline (results in
`plans/impl/0592_q21_attribution_on_strix_results.md`). Phase 1 resolves
`outside_drive` as q21's owner (4,416.3 ms, 48.93%, accepted profile,
35.7 ms off range). Phase 2 leaves the envelope unresolved:
`runtime_prep_inner_1` leads at 1,334.5 ms and clears the floor, but
`runtime_prep_semi_write` (849.6 ms) lies within the 609.0 ms off range
of it and the rule reports the tie. Nothing promoted; slim's
`runtime_prep_inner_1` stays a hypothesis with a named rival. Successor to
ADR 591, whose sealed board names q21 the next owner on strix by absolute
gap.


## Context

ADR 591 sealed strix's baseline (tree `a962f3d8…`, tag
`evidence/a962f3d80f63-d03935eb49cc`): q21 leads by 6,916.9 ms of absolute
Toron-minus-DuckDB gap (9,126.9 against 2,210.0 ms), 1,398.9 ms ahead of
q09. The coordinate is ADR 590's freeze plus the P-core affinity `0-15`,
applied by the harness and recorded on every row.

Everything known about where q21's time goes was measured on slim. ADR 584
resolved `outside_drive` as the owner (7,887.0 ms, 55.16%); ADR 585
decomposed it and named `runtime_prep_semi_write` (2,323.1 ms; ADR 587
later read it at 2,199.1 ms); ADRs 586-589 decomposed and then cut the
semi write, and ADR 589's receipts
named `runtime_prep_inner_1` (1,671.4 ms, 15.3% of the off median) as the
runtime-prep owner after the cut, with `semi_write_repartition` (606.4 ms)
above the insert it feeds. Those are slim facts on slim's trees; on strix
`runtime_prep_inner_1` is a hypothesis, and the arc does not carry an
owner across hosts. The attribution is repeated here, on the sealed
coordinate, with the same instruments and the same rules.

The two drivers exist and run at this tree. ADR 584's
`adr584-q21-attribution` reconciles the chain-stage profile to the drive
with `outside_drive` as a named term and ranks nine terms; ADR 585's
`adr585-q21-decomposition` reads the inclusive lifecycle receipt and ranks
the runtime-prep envelope, whose band ADR 589 extended with the
`runtime_prep_semi_finish` and `runtime_prep_anti_finish` terms and the
`insert_total` identity, so the receipt already carries the finish the
order-aware build moved out of the row loop. Both drivers check the
coordinate they were written for (2 GiB budgets, parallelism 1, a staged
executable, a quiescent host, full-hash data) and neither knows about the
affinity coordinate, checks the sealed digest on its rows, or names a
sealed baseline other than slim's `b436a843…`, which is a constant in the
code.


## Decision

**The coordinate is ADR 591's, unchanged, and the drivers must see it.**
Headless as ADR 590 froze it, `powersave` with EPP `performance`, turbo
and SMT on, `TORON_TPCH_CPU_AFFINITY=0-15`, the freeze re-verified and
the host snapshot taken and hashed before each run, the build on mini and
the runs on strix with nothing else on strix in the window. Before either
driver runs a child, it refuses unless the process's affinity coordinate
is explicit and equal to the verified baseline's (below). Every child,
the warmup included, is validated as it returns and before another child
is launched: its row must pass with `q21` on `toron`; carry
`cpu_affinity_requested`, `cpu_affinity_requested_provenance = explicit`,
and `cpu_affinity_observed` equal to the driver's requested cpulist; the
sealed q21 digest byte for byte; `dataset_verification = full_hash`, the
baseline's dataset fingerprint, and `data_source = parquet`; the sealed
coordinate fields (mode, scale, budgets, parallelism); diagnostics
disabled on the off arm; and the identity of the executable that actually
ran, the manifest it was launched from, and the launched executable hash,
each equal to the current instrumented executable's manifest, not to the
sealed one, which is the historical reference. A child that did not pass
is returned to the validator as it is, never swallowed by the launcher;
a failing row is journaled with its refusal and the run stops there; ADR 588's validator is
generalized so both drivers share it, and the driver records the
coordinate in its summary. That is the plumbing this record adds, with a
discriminating fixture per refusal.

**The sealed baseline is bound by its contents, not by its labels.** Both
drivers accept `--sealed-manifest PATH` and this record requires it. The
driver reads the sealed run manifest, requires `seal_state = sealed` and
`final_authority = true`, resolves the artifact it names, hashes it, and
refuses unless the hash equals the manifest's `artifact_sha256`. It requires
the artifact's header to be this record's coordinate (strict mode,
parquet, scale 8, both 2 GiB budgets and the parallelism pin with
explicit provenance) and its own executable and tree identity to be the
manifest's, since the manifest's labels are what the summary reports.
From the verified artifact it reads the header's dataset fingerprint and
affinity (`cpu_affinity_requested`, explicit) and the q21 Toron cell
summary (present, passing, its median and its digest), and binds the
driver's coordinate to them: the process affinity must equal the baseline's, every
child's dataset fingerprint and digest must equal the baseline's, and the
baseline median is the reference the off-arm distance is stated against.
The summary records the baseline's source tree, executable, DuckDB, and
artifact hashes and the values it bound to. Without the flag the drivers
keep reporting slim's constant as before, which is a default this record
does not use. The instrumented executable is built once from this tree
through `toron-bench build` with the release profile and runs both arms
of every pair, as ADR 584 fixed. Its unprofiled arm's median is reported
as an observed distance from the sealed q21 median (9,126.9 ms); that
distance includes host and time variability as well as the executable's
changes, and is not read as the cost of the plumbing.

**Phase 1: ADR 584's attribution, as written.** One unprofiled warmup,
then five adjacent pairs alternating off-on, on-off, off-on, on-off,
off-on; the off arm with the chain profiler and the diagnostic ledger
disabled, the on arm with both enabled, each on child with its own
receipt path recorded in its row. The perturbation gate (median
`|m_i| > 0.25` rejects the profile), the completeness bounds (in-drive
residual within the pair's overhead plus 5% of the drive; `outside_drive`
non-negative), the nine-term universe (seven stages, the in-drive
residual, `outside_drive`), the scaling
`scaled_t_i = term_t_on_i / (execute_on_i × 1000) × execute_off_i`, the
median over five pairs, and the owner floor
`max(500 ms, 2 × range_off)` with ties inside `range_off` unresolved: all
ADR 584's, applied unchanged.

**Phase 2: ADR 585's decomposition, as written, over the fifteen-term
band, of the work outside the drive.** Phase 2 runs after phase 1 in every
outcome except a rejected or incomplete phase-1 profile, which closes the
record. It attributes the envelope ADR 585 named, not q21 as a whole: its
winner is the envelope owner. The overall owner is chosen by phase 1's
rule: if phase 1 names `outside_drive`, phase 2's envelope owner is q21's
owner; if phase 1 names a drive stage, that stage is the owner and phase
2's winner is reported as the envelope's owner beside it, context and not
a replacement; if phase 1 names the in-drive residual, the finding is an
incomplete instrument, and phase 2 is reported as context. The same
warmup and pair structure; the inclusive lifecycle
receipt with its three bands reconciled and every residual recorded; the
candidate universe is the runtime-prep children including ADR 589's two
finish terms and the residual, `base_prep`, `finalize`, `cleanup`,
`outside_handler_span`, `lifecycle_residual`, and `outer_residual`;
`drive_inclusive` reported for consistency and not a candidate; the same
scaling, statistic, and floor; a residual that leads is an incomplete
instrument, not an owner. The mechanism floor is applied to any removable
subterm a term suggests, priced pairwise as ADR 585 fixed, and reported;
no mechanism opens under this record.

**The owner is chosen from strix's evidence.** The record names one of:
the owner the rules return; an unresolved tie, when the leading terms lie
within `range_off` of each other, reported as such and not broken; no
term clearing the floor; or a rejected or incomplete profile. slim's chain (584's `outside_drive`, 585's
`runtime_prep_semi_write`, 589's `runtime_prep_inner_1`) is reported
beside strix's terms as host-to-host, tree-to-tree context, never as a
prior the result must match; where strix disagrees with slim, the
disagreement is the finding. `runtime_prep_inner_1` is confirmed only if
ADR 585's rule on strix's receipts returns it.

**No optimization.** No engine route, admission, build, writer, or
instrument semantics change under this record. If a driver cannot run
without an engine change, that is a finding and the change gets its own
record first.


## Acceptance

- The shared child validator refuses a row that did not pass, is not q21
  on Toron, lacks the affinity fields, carries `default` provenance or an
  observed mask differing from the driver's request, lacks the sealed
  digest or carries another, is not `full_hash` on the baseline's dataset
  fingerprint or not `parquet`, is off the sealed coordinate, or names an
  executable, manifest, or launched hash other than the current
  instrumented executable's; each names the row and the field, the
  failing row is journaled with its refusal, and no further child is
  launched; a driver-path test with an injected launcher proves a failed
  warmup is journaled and no second child launches. The driver refuses to
  start under a default affinity coordinate or one differing from the
  baseline's. Each refusal has a fixture that changes one thing, the
  warmup row included.
- `--sealed-manifest` is required; the driver refuses an unreadable,
  unsealed, or non-final manifest, an artifact whose hash differs from
  the manifest's, an artifact without a passing q21 Toron cell, a dataset
  fingerprint or a digest differing from the children's, an affinity
  differing from the process's, a header at another coordinate (mode,
  source, scale, budgets, parallelism, each with its provenance), and a
  header whose identity is not the manifest's, each with the reason
  named; fixtures cover a mismatched artifact hash, a mismatched dataset
  fingerprint, a mismatched affinity, every coordinate field other and
  absent, and the identity. The summary records the bound values.
- Phase 1 journals eleven rows (one warmup, five pairs) on one instrumented
  executable, every row validated, the perturbation and completeness
  gates reported as ADR 584 defined them, and the nine-term table with its
  verdict.
- Phase 2 journals eleven rows likewise, the three bands reconciled on
  every on row, the fifteen-term table with its verdict, and the mechanism
  floor applied to any suggested subterm; the results state the phase-1
  owner, the phase-2 envelope owner, and the overall selection under the
  transition rule, including an unresolved tie where one occurs.
- The results document reports the instrumented identity beside the sealed
  one and its off-arm distance from the sealed q21 median, the host record
  and its hashes, both tables with slim's ADR 584/585/589 figures beside
  them labelled host-to-host, and the owner or the finding.
- Plan commit, then one implementation/results/closure commit; the
  measured executable's tree is named; nothing else lands under this
  number.


## Guardrails

- The owner rules, floors, gates, and scaling are ADR 584's and 585's
  verbatim; this record adds validation and identity plumbing only.
- A rejected profile or an incomplete instrument closes the record on that
  finding; no rerun-until-accepted. A retained rehearsal that was refused
  is kept under a renamed directory.
- Build on mini, measure on strix, the freeze verified before each window,
  the deployment on slim untouched, as ADRs 590 and 591 prescribe.


## Risks

- The 0.25 perturbation gate was calibrated on slim; a faster host may
  show a larger relative instrument overhead. If the profile is rejected,
  the flush cadence is the next instrument change under its own record,
  as ADR 584 wrote, not a relaxed gate.
- The instrumented executable is this tree's, which is ADR 591's sealed
  tree plus this record's plumbing; the off-arm distance from the sealed
  row is observed and reported, and it carries host and time variability
  as well as that difference.


## Forward

- The owner the rules return names the successor: a decomposition of its
  lifecycle if it is a runtime-prep term without one (`runtime_prep_inner_1`
  has none), ADR 586's semi-write decomposition if it is the semi write
  again, or a stage-level record if a drive stage leads.
- Host identity in the run manifest remains forward work; the affinity
  fields the drivers now validate are the pattern.
