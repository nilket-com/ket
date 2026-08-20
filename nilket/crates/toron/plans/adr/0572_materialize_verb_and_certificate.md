# ADR 572 - The materialize verb and the birth certificate

**Status:** Proposed 2026-08-20. The Toron companion PRODUCT-0137 §6 names,
implemented against that contract, KET-0006's `boson` crate
(`CanonicalRelationExpression`, `2ac6d5e3`), and Wendelon ADR 0087's ruling
on the attachment's residence, bound, and atomic admission.

## Context

TORON-0569's generated relations execute and reproduce but cannot be kept.
PRODUCT-0137 gives `materialize` its contract — a fresh, named, Toron-owned
table born with an immutable birth certificate, atomic with visibility —
and both prerequisites now exist: the canonical expression format is
implemented in `shared/boson`, and Wendelon 0087 rules where the
certificate lives (a dedicated per-object immutable attachment catalog),
what bounds it (`MATERIALIZATION_CERTIFICATE_MAX_BYTES`, refused at
`Provision`), and how it is admitted (riding `CtasSagaEntry` from
`Provision`, transferred to the attachment catalog inside the single
`Publish` apply).

The load-bearing reuse fact: the CTAS saga already is 0137 §2's
"certificate-first with total failure" shape, and Toron's coordinator
already drives it end to end, including the recovery re-entry in
`finish_ctas_after_rows_committed`. The companion adds a verb, a
translation, and a rider — not a write path.

## Decision

### 1. The Rune spelling is a terminal `materialize(name)` verb

`ctx.range(n).…, .materialize("name")` ends a Rune relation program with a
materialization statement. The name follows PRODUCT-0134 §4 verbatim:
trimmed exact comparison, empty and all-whitespace refuse, no folding or
normalization — carried by citation, since Rune and `TableRef` already
preserve strings exactly. There is no SQL spelling in this cut; CTAS keeps
its own verb and its own semantics untouched.

### 2. Translation to the canonical form happens at authoring, and it is
the admission gate

The verb translates the dataframe's plan into `boson`'s
`CanonicalRelationExpression` **directly — no mirror struct, no
`LogicalPlan` serialization** (0137's rejected list, honored structurally:
the certificate's authoritative bytes are `encode_v1`'s output and nothing
else). The admissible shapes are exactly boson v1's: a generated range
plus at most one projection whose expressions map onto the closed
vocabulary (`Ordinal`, numeric literals, `Cast`, `Add`, `Sub`, `Mul`,
`Div`, `Floor`, `SeededUnitInterval`). A plan outside that vocabulary
refuses **at authoring** with a directive naming the boundary — before
planning, before any write. Seed witnesses are extracted from the encoded
expression, never author-supplied, and marked non-exhaustive per 0137 §3.

### 3. The certificate rides the existing CTAS plan node as a tail field

`LogicalPlan::CreateTableAsSelect` gains one `#[serde(default)]` tail
field: the optional encoded certificate. SQL CTAS never sets it; the
materialize verb always does. When present, the coordinator's CTAS path:

- provisions with `CtasExpectedBinding::Absent` and forces
  `if_not_exists: false` — the fresh-only decision, structural;
- carries the certificate into `CtasSagaEntry` at `Provision` (Wendelon
  0087 §3), where an oversized payload refuses whole before
  `row_write_id` dispatches;
- publishes through the unchanged single apply, which transfers the
  certificate into the attachment catalog with the name binding — one
  state transition or neither.

The write path inherits TORON-0569's fences verbatim: coordinator-only,
no ordering promise, no distributed writes; the ceiling is stated, not
promised around.

### 4. The receipt is routine status

Success reports the table name and certificate confirmation through the
existing status lane; failures — collision, refused name, inexpressible
plan, oversized certificate, engine or write error — surface as ordinary
diagnostics. No viewer, no browser, no provenance panel (0137 §7).

## Acceptance

- `ctx.range(4).materialize("t")` creates a table addressable as
  `ctx.table("t")` and from SQL, whose rows equal the live relation's, and
  whose attachment decodes via `boson::decode_v1` to exactly the
  expression the verb encoded — round-tripped, not assumed.
- A projection using the full closed vocabulary (ordinal arithmetic,
  `floor`, `seeded_random`) materializes; seed-position literals appear as
  witnesses; a computed seed does not, and the witness list says so.
- A plan outside the vocabulary — a filter, a join, a non-vocabulary
  scalar — refuses at authoring with the directive; nothing plans.
- A name collision refuses; `if_not_exists` is unreachable from the verb;
  the created table is ordinary (no special kind) and drops normally,
  destroying its attachment with it.
- The fault-injection gate, extending the existing saga recovery fixtures:
  a failure injected between `RowsCommitted` and `Publish` leaves the name
  unbound and no rows visible; the resumed publish binds table and
  certificate together. In no interleaving is a table name-resolvable
  without its attachment.
- Plain SQL CTAS behavior is byte-identical: no certificate, no admission
  change, existing suites untouched.

## Guardrails

The certificate's bytes come from `boson::encode_v1` alone; a guard scans
the materialize path for `LogicalPlan` serialization and for any second
encoder. The vocabulary boundary is boson's `validate_v1`, not a
Toron-side list that could drift from it.

## Risks and deferrals

Distributed generated-table writes, an SQL spelling, richer receipts, and
any second attachment kind are deferred with their own records. A future
boson v2 widens the vocabulary by its own record; the verb inherits it by
consuming the crate, not by local extension.

## Forward

Implementation order: Wendelon 0087's attachment catalog and saga field
first (the `wendelon_client` surface), then this verb against it, then the
fault-injection gate over the combined path.
