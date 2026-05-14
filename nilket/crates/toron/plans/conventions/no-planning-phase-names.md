# No planning-phase names in code

Do not name Rust types, fields, methods, variables, tests, metric
labels, or any code identifier after a *planning-step number or
letter* from an ADR or impl plan. Names must describe what the
thing **does** semantically, not where it sits in someone's
writeup.

This is a sibling to [no-version-suffixes.md](no-version-suffixes.md):
both rules forbid ephemeral planning vocabulary from surviving
into shipped code. Version suffixes pin namespace to a
hypothetical future revision; phase numbers and gate ordinals pin
namespace to an ADR's chronological narrative. Both rot. Code
identifiers must be stable and self-explanatory; planning
artifacts are written for *one moment* and the codebase has to
outlive that moment.

## Rule

Banned in code identifiers (any file):

- Phase numbers / letters: `phase_0`, `phase_1`, `Phase2Plan`,
  `phase_a`, `PhaseBLowering`.
- Hard-gate ordinals as prefixes: `g16_`, `g17_`, `gate_17_`,
  `hard_gate_3`.
- Iteration tags: `iter1`, `iter_two`, `draft_v1`, `final_pass`.
- Workflow tags: `wip_*`, `tmp_*`, `temp_*`, `staging_*`,
  `pre_promotion_*`, `post_promotion_*`.
- Audit-item tags as identifiers: `p0_3_default`, `P05Check`.
- Generic ordinals masquerading as semantic names: `step_a`,
  `pass2`, `round3`. (Ordinals are fine when they reflect a real
  semantic ordering — `stage_0` for a fragmenter's first
  distributed stage is OK because the stage *is* the thing, not a
  planning step.)

What to use instead — describe the function. An impl plan's
"Phase 2 — `Sort` IR + binder" produces `LogicalPlan::Sort`, an
`ExecutionPlan::Sort` variant, and a binder arm — **not** a
`phase_2_sort` anything. A "Phase 1 — clause accounting" produces
`lower_query_and_lower_select_have_no_rest_pattern` — a test
named for the invariant it pins, not `phase_1_test`.

ADR / impl-plan prose may still describe work in phases ("Phase 1
lands the clause-accounting refactor; Phase 2 adds the operator")
— that is the *narrative*, and the narrative is allowed to be
chronological. Code follows the prose only through the *semantic*
names the prose uses, never the phase numbers.

## Why

Phase numbers, gate ordinals, and iteration tags describe *the
narrative of a writeup at the moment it was authored*. Three
failure modes follow:

1. **They rot the moment the plan shape changes.** An impl plan
   names work Phase 1 / 2 / 3 / 4; a re-scope merges two phases
   or splits one. Every `phase_*` identifier in code then lies.
   Semantic names describe behavior; behavior survives re-scopes.
2. **They force ADR archaeology to read code.** Reading
   `g17_join_ir_surface_has_single_join_variant` requires knowing
   what hard gate 17 was, which requires opening the ADR. The
   name *after* the `g17_` prefix is already semantic — the
   prefix is pure planning-cadence noise. Identifier-as-
   explanation is a load-bearing readability invariant.
3. **They suggest planning sequence as the primary semantic
   axis, when functional purpose is what callers care about.** A
   test named for the invariant it protects catches regressions
   faster than one named for its slot in a gate list.

The general principle: **name by what, not by when**. "When"
lives in commit messages, ADR text, and review threads. Code
identifiers are the *what* — the durable interface every future
reader pays the cost of.

## How to apply

- **New code:** never accept `phase_*` / `g<NN>_` / `iter*` /
  `wip_*` / `tmp_*` / audit-item ordinals as identifiers. If a
  name needs the ADR open to be understood, the name is wrong.
- **Reviewing diffs:** flag any new planning-ordinal identifier
  and ask for the semantic rename before merge. The author may
  need help picking the name; a one-sentence "what does this
  thing *do*?" usually produces it.
- **Existing code:** the `g16_` / `g17_` / `g18_` / `g19_`
  gate-prefixed test names in `tests/architecture_test.rs` are
  the current instance of this leak — the name after the prefix
  is already semantic, so a rename pass dropping the `g<NN>_`
  prefix is expected, not surprising. Until it lands, new tests
  must not extend the leak.
- **ADR / impl-plan drafts in `plans/wip/`:** must use semantic
  names for new identifiers being introduced. Describing the work
  as "Phase 2" in prose is fine; proposing a `phase_2_*`
  identifier is not.

## Comments are prose, not identifiers

A `// P0.3:` comment that annotates *where in the code an audit
decision landed* is prose, not an identifier — it is the
established Toron impl-plan pattern (ADR 010 onward) and is
explicitly fine. The convention bans planning ordinals as
*identifiers* (the names the compiler and grep see), not as
references inside comments or doc text.

## Escape hatch

If a name truly is "stage N of an algorithm with no better
description" — rare, but possible — say so explicitly with a
comment, and keep the ordinal narrow (`stage_2_of_radix_partition`,
not `phase_b`). The convention is against ordinals smuggled in as
semantic names, not against ordinals as ordinals when the
algorithm literally has numbered stages.
