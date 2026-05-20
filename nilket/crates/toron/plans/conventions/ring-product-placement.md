# Ring-product placement: where an ADR's docs live

An ADR's home directory is decided by **what kind of product it
is**, not by **which council process drafted it**. Ring-specific
products live in their own ring's ADR sequence; workspace-shared
products live in the Toron sequence acting as the aggregator. The
council process runs out of `crates/toron/plans/wip/` regardless —
process locality is not placement.

This convention promotes the framework seeded in
[../brainstorms/006_ring_products_vs_development_cycles.md](../brainstorms/006_ring_products_vs_development_cycles.md).
It was adopted after a concrete misfile: `nilket wendelon inspect`
was drafted as Toron ADR 049, then relocated to Wendelon ADR 071
because the deliverable is a storage-ring feature.

## The matrix

Classify every design cycle on two axes — product **scope**
(Interior / Exterior) and technical **nature** (Substrate /
Composition):

| | **Substrate** (the "How") | **Composition** (the "What") |
| :--- | :--- | :--- |
| **Interior** (workspace-shared) | **Extraction / Refactor.** Shared crates consumed by both rings. *e.g.* `tls-pki` (ADR 047), `observability-http` (ADR 048). | **Coordination / Glue.** Unify the product surface, hide complexity. *e.g.* the `nilket` unified entrypoint / dispatcher. |
| **Exterior** (ring-specific role) | **Engine / Protocol.** Build a ring's truth, scale, capability. *e.g.* Toron compute daemon (ADR 042), QUIC listeners (ADR 044); Wendelon storage hierarchy (ADR 056-070). | **Feature / Surface.** Deliver operator-facing verbs and parity. *e.g.* `nilket toron inspect` (Toron ADR 046), `nilket wendelon inspect` (Wendelon ADR 071), `sql` runner (Toron ADR 043). |

- **Interior** = consumed by both rings / workspace-shared (a new
  `crates/<shared>` crate, or cross-ring glue).
- **Exterior** = belongs to one ring's role (its code lands in
  that ring's crate; its server-side parent ADR is in that ring's
  sequence).

## Rule

- **Interior products** (Substrate or Composition) file in
  `crates/toron/plans/adr/`, which acts as the **workspace
  aggregator**. Guards/fixtures land where the shared crate +
  its primary consumer live.
- **Exterior products** "return home" to the owning ring's ADR
  sequence:
  - Toron-ring → `crates/toron/plans/adr/` (Toron's home sequence
    *is* the aggregator location — see the nuance below).
  - Wendelon-ring → `crates/wendelon/plans/adr/` (e.g.
    `071_nilket_wendelon_inspect_real_wiring.md`). Guards/fixtures
    land in `crates/wendelon/tests/`.
- **Placement follows code-locality + server-side parentage, not
  process-locality.** If all the code lands in one ring's crate
  and the surface it builds on was specced in that ring's
  sequence, the ADR belongs to that ring — even when the
  Claude/Codex/Gemini council ran from `crates/toron/plans/wip/`.
  The other ring's roadmap carries a **cross-reference**, never a
  duplicate artifact. One owner; no dual-home.

## Why

The two rings are independent products on disjoint hardware
(see the cross-ring orchestration boundary). Their ADR sequences
are their design archaeology. Filing a storage-ring feature in
the compute ring's sequence:

- splits a feature from its server-side parent ADR (the
  `nilket wendelon inspect` client's `/ready` target is Wendelon
  ADR 027 observability posture — they should be discoverable in
  one sequence);
- turns the compute ring's sequence into a catch-all, eroding the
  separation of concerns that lets each ring evolve its own
  cadence;
- misleads renumbering and ownership (Wendelon already had its
  own ADR 049 — content-hash identity; a second "049" would have
  collided).

The aggregator role is real and useful for Interior products
(shared substrate genuinely belongs to neither ring alone), but
it must not leak into Exterior products.

## Examples already in the codebase

Correctly placed (consistent with the rule across the whole
ADR 040-onward arc):

- **Interior Substrate → Toron aggregator:** `tls-pki`
  (ADR 047), `observability-http` (ADR 048). New workspace crates
  consumed by both rings; guards in
  `crates/toron/tests/architecture_test.rs`.
- **Interior Composition → Toron aggregator:** the `nilket`
  unified entrypoint / reservation-dispatch substrate (ADR 040).
- **Exterior Substrate → Toron home:** compute daemon (ADR 042),
  SQL listener (ADR 043), authenticated QUIC listeners (ADR 044),
  observability listeners (ADR 045).
- **Exterior Composition → Toron home:** CREATE OR REPLACE TABLE
  (ADR 041), `nilket toron inspect` (ADR 046).
- **Exterior Substrate → Wendelon home:** observability posture
  (Wendelon ADR 027), content-hash identity (Wendelon ADR 049),
  the storage hierarchy (Wendelon ADR 056-070).
- **Exterior Composition → Wendelon home:** `nilket wendelon
  inspect` (Wendelon ADR 071).

The one misfile (corrected): `nilket wendelon inspect` was
drafted as Toron ADR 049 because the council + roadmap that
selected it live under `crates/toron/plans/`. It was relocated to
Wendelon ADR 071 (`git mv` adr+impl, renumber, guards/fixtures →
`crates/wendelon/tests/`, both roadmaps reconciled with the Toron
side cross-referencing).

## The aggregator nuance

`crates/toron/plans/adr/` does double duty: it is **both** Toron's
ring-home sequence (for Toron Exterior products) **and** the
workspace Interior aggregator (for shared substrate + coordination).
There is no separate workspace-level sequence today, so Toron-as-
aggregator is the convention.

This is a deliberate, low-cost choice — not an invariant. If the
Interior surface grows enough that overloading Toron's sequence
becomes confusing, a future move could introduce a true
workspace-level sequence (e.g. `crates/nilket/plans/adr/`) that
owns Interior products, leaving each ring's sequence purely
Exterior. Until that pressure surfaces, Interior products file in
the Toron sequence.

## The Leap Rule

Genuinely new capabilities are often **Substrate Leaps disguised
as Composition tasks**: they look like a parity feature ("add
tenant flags to the CLI") but actually require designing the
Interior "How" (isolation logic) first. Multi-tenant routing — the
current Toron ADR 049 lead candidate — is the canonical example:
the per-tenant catalog isolation + scoped-catalog decorator +
mTLS tenant extraction is an Interior/Exterior Substrate design
that must precede any Exterior tenant-flag surface. Classify such
candidates as Substrate, not Composition, and scope a Phase-1
interior cut before the user-facing verbs.

## How to apply in review / topic selection

At ADR topic selection and at promotion:

1. Classify the candidate on the matrix. Name the quadrant in the
   ADR's Category line.
2. **Interior** → Toron aggregator. **Exterior** → owning ring's
   sequence. If the code will land entirely in one ring's crate,
   that ring owns the ADR regardless of which wip dir the council
   ran in.
3. If a candidate looks like Composition but needs new interior
   mechanism, apply the Leap Rule and treat it as Substrate.
4. Risk lens: Substrate cycles carry **integration risk** (gate
   on byte-identical behavior preservation + guard stability);
   Composition cycles carry **parity risk** (gate on faithfully
   mirroring a user-facing contract across rings).
5. A storage-ring noun as the *subject* of a Toron ADR title (or
   vice-versa) is a placement smell — re-classify before drafting,
   not after promotion.

## Architectural enforcement

Placement is **review-time discipline**, not a source-text guard —
subject classification isn't mechanically checkable. The
enforcement is: classify on the matrix in the ADR's Category line,
and treat the ADR 049→071 relocation as the documented precedent
for what a misfile costs (renumber, guard/fixture move, dual
roadmap reconciliation). Each ring's `tests/architecture_test.rs`
still owns its own ADR guards, which keeps the *implementation* of
an Exterior ADR anchored in the right crate even though the
*placement* of the doc is a review judgment.

## When this changes

The rule holds while the workspace has two product rings and one
shared aggregator-by-convention. It is revisited if a third ring
appears, or if Interior surface area justifies a dedicated
workspace-level sequence (then Interior products move there and
each ring's sequence becomes purely Exterior).

## See also

- [../brainstorms/006_ring_products_vs_development_cycles.md](../brainstorms/006_ring_products_vs_development_cycles.md)
  — the seed framework (the 2x2, the Placement Rule, the Leap
  Rule) this convention promotes.
- [no-project-name-in-protocols.md](no-project-name-in-protocols.md)
  — the brand/protocol-timescale split; same separation-of-
  concerns instinct at the identifier layer.
- [no-version-suffixes.md](no-version-suffixes.md),
  [no-planning-phase-names.md](no-planning-phase-names.md) —
  process-artifacts (V1, phase names) don't belong in durable
  surfaces; here, process-locality doesn't decide durable ADR
  placement.
