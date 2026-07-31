# No planning-phase names in code

Do not name Rust types, fields, methods, variables, tests, metric
labels, or any code identifier after a *planning-step number or letter*
from an ADR. Names must describe what the thing **does** semantically,
not where it sits in someone's writeup.

This is a sibling to [no-version-suffixes.md](no-version-suffixes.md):
both rules forbid ephemeral planning vocabulary from surviving into
shipped code. Version suffixes pin namespace to a hypothetical future
revision; phase letters pin namespace to an ADR's chronological
narrative. Both rot. Code identifiers must be stable and self-
explanatory; planning artifacts are written for *one moment* and the
codebase has to outlive that moment.

## Rule

Banned in code identifiers (any language, any file):

- Phase letters: `phase_a`, `phase_b`, `phase_c`, `phase_d`, `PhaseB`,
  `PhaseCFetch`, `phase_c_fetch`, `phase_c_batches`,
  `apply_phase_b_masks_to_projected_row_groups`,
  `zip_phase_b_and_phase_c_projected_row_group`.
- Phase numbers: `phase_0`, `phase_1`, `Phase3Plan`.
- Iteration tags: `iter1`, `iter_two`, `draft_v1`, `final_pass`.
- Workflow tags: `wip_*`, `tmp_*`, `temp_*`, `staging_*`,
  `pre_promotion_*`, `post_promotion_*`.
- Sprint / planning-cadence tags: `sprint3`, `q3_plan`,
  `roadmap_step_14`, `step8`.
- Generic ordinals masquerading as semantic names: `step_a`,
  `pass2`, `round3`. (Ordinals are fine when they reflect a real
  semantic ordering — `level_2_storage_tier` is OK because L2 is
  the *thing*, not a planning step.)

What to use instead — describe the function:

- The row-group fetch planner's stages get **semantic** names:
  - `phase_a` → `directory_probe` (metadata + directory read);
  - `phase_b` → `predicate_evaluation` / `predicate_fetch` /
    `survival_mask_derivation`;
  - `phase_c` → `projection_fetch` / `projected_fetch`;
  - `phase_d` → `encoding_materialize` / `page_materialize`.
- Specific identifiers from the existing leak:
  - `phase_b_fetch` → `predicate_fetch`;
  - `phase_c_fetch` → `projection_fetch`;
  - `phase_c_batches` → `projection_batches`;
  - `apply_phase_b_masks_to_projected_row_groups` →
    `apply_survival_masks_to_projection_batches`;
  - `zip_phase_b_and_phase_c_projected_row_group` →
    `zip_predicate_and_projection_columns`;
  - metric label `phase_b` → `predicate`;
  - metric label `phase_c` → `projection`.

ADR / impl-plan prose may still describe stages in chronological
order ("the planner's first stage probes the directory, the second
evaluates predicates, …"), but even there prefer the semantic name
on first reference and drop the phase letter entirely once the
sequencing is established. **Code follows the prose only when the
prose used semantic names.**

## Why

Phase letters and similar planning-cadence tags describe *the
narrative of a writeup at the moment it was authored*. Three failure
modes follow:

1. **They rot the moment the planner shape changes.** ADR 056 named
   stages A/B/C; ADR 057 added stage D. If a future ADR reorganizes
   the planner into two stages or six, every `phase_*` identifier in
   code lies. Semantic names ("predicate_fetch", "projection_fetch")
   describe behavior; behavior survives planner reshapes.
2. **They force ADR archaeology to read code.** Reading
   `apply_phase_b_masks_to_projected_row_groups` requires knowing
   what Phase B is, which requires opening ADR 056. Reading
   `apply_survival_masks_to_projection_batches` requires nothing.
   Identifier-as-explanation is a load-bearing readability invariant.
3. **They suggest temporal ordering as the primary semantic axis,
   when functional purpose is what callers care about.** A test named
   `phase_c_ranges_exclude_buffers_already_fetched_by_phase_b`
   describes plumbing; a test named
   `projection_ranges_exclude_buffers_already_fetched_by_predicate`
   describes the invariant under test. Failure-mode names that read
   like requirements catch regressions faster.

The general principle: **name by what, not by when**. "When" lives
in commit messages, ADR text, changelogs, and review threads. Code
identifiers are the *what* — and they are the durable interface
every future reader pays the cost of.

## How to apply

- **New code:** never accept `phase_*` / `iter*` / `wip_*` /
  `tmp_*` / planning-step ordinals as identifiers. If a name needs
  context to be understood, the name is wrong.
- **Reviewing diffs:** flag any new `phase_*` identifier and ask for
  the semantic rename before merge. The author may need help
  picking the semantic name; a one-sentence "what does this thing
  *do*?" usually produces it.
- **Existing code:** a sweeping rename pass to remove the current
  `phase_a` / `phase_b` / `phase_c` / `phase_d` identifiers (in
  `src/storage/row_group.rs`, `src/storage/manager.rs`,
  `src/metrics/registry.rs`, the row-group test files, and the
  observability docs) is planned work; treat it as expected, not
  surprising. Until the rename lands, new code must not extend the
  leak.
- **ADR / impl-plan drafts in `plans/wip/`:** must use semantic
  names for new identifiers being introduced. Referring to
  *existing* `phase_b_fetch` etc. by current name is fine for
  accuracy; proposing new code that compounds the leak is not.
- **Metrics:** label values like `phase_b` / `phase_c` count as
  identifiers. They appear on operator dashboards and runbooks.
  Same rule applies; same rename pass.

## Escape hatch

If a name truly is "stage N of an algorithm with no better
description" — rare, but possible — say so explicitly with a
comment, and keep the ordinal narrow (`stage_2_of_radix_sort`,
not `phase_b`). The convention is against ordinals smuggled in as
semantic names, not against ordinals as ordinals when the algorithm
literally has numbered stages.
