# ADR 569 - Stable range relations and seeded random values

**Status:** Proposed 2026-08-20. Promoted from the council dual after iter-3
review, with the two endorsed contract corrections incorporated.

## Context

Toron's Rune relation API starts at `ctx.table(name)`. It can transform,
join, aggregate, and window a relation, but it cannot create rows. That leaves
performance fixtures, cancellation exhibits, and large data demonstrations to
hand-authored inserts or imported data.

`ExecutionPlan::EmptyRelation` and `SystemTable` prove that a relation source
need not be a storage scan. Neither represents virtual cardinality, so a range
is not honestly encoded as either one. The scalar registry also distinguishes
`Deterministic` from `PerCall`; `RANDOM` is deliberately the latter and
evaluates fresh OS entropy per row. Planner legality reads this distinction
today, so changing `RANDOM` would alter shipped rewrite semantics.

The required guarantee is not output order or bytes. Distributed execution has
no order contract without `Sort`. A generated relation must instead reproduce
the same multiset of `(ordinal, value)` pairs across repeated execution and
worker/exchange placement. `distributed::determinism` already defines the
engine's comparison mechanism: canonical full-row lexicographic normalization,
canonical batches, and Arrow IPC digest.

Materializing the relation, cataloguing its birth certificate, artifact byte
reproducibility, CSV ingest, and an ingestion wire frame are separate work. In
particular, current `INSERT INTO ... SELECT` is coordinator-only and
non-fragmentable, so it cannot support a claim of distributed generated-table
writes in this record.

## Decision

### 1. One virtual Rune range relation

`ctx.range(count)` is a Rune-only relation source. `count` is an `i64`:
negative values refuse at the Rune binding boundary; zero yields the declared
one-column schema and no rows. It emits the ordinary, non-null `Int64` column
`ordinal`, with values `[0, count)`.

`ordinal` is a real schema column. It follows the normal binder, projection,
alias, join, and collision rules; it is not a magic expression and there is no
ordinal-only binder path.

The IR uses a dedicated virtual range execution node rather than materialized
system-table rows. Every traversal that treats `EmptyRelation` and `SystemTable`
as leaves must consciously include the range node. The node publishes exact
row-count, NDV, and ordinal min/max facts at birth. Those facts are true by
construction, and only plans containing the new node can observe them.

There is no SQL table-function spelling: Toron has no SQL `FROM` table-function
seam. This is an absence, not a preference; adding such a grammar is deferred.

### 2. One deterministic scalar is the generation vocabulary

The scalar registry admits `SEEDED_RANDOM(seed, key)` and Rune spells it as
`seeded_random(seed, key)`. Both arguments are `Int64` expressions; the result
is `Float64` in `[0, 1)`, non-null for non-null arguments — nulls propagate
per the declared registry behavior, the catalog's convention. Restricting the
typed signature to `Int64` keeps the initial key encoding exact and bounded
while still allowing the ordinary ordinal and its arithmetic transforms, as
well as correlation through another integer expression. Broad type hashing is
a separate admission decision, not an implicit consequence of accepting an
expression.

The function is `Deterministic`. It is functional, never stateful:
`value = mix64(mix64(seed_bits) ^ key_bits)`, where `seed_bits`/`key_bits`
are the arguments' two's-complement `u64` bit patterns and `mix64` is
SplitMix64's finalizer (the specified shift-xor-multiply sequence, wrapping).
Mixing the seed **before** combining destroys additive structure — `(s+1,
k-1)` cannot reproduce `(s, k)`'s stream — and the asymmetry makes
`(seed, key)` and `(key, seed)` distinct. The high 53 bits of the result
divide by `2^53` into `[0, 1)`. The composition, input representation, and
conversion are semantic contract, with golden witnesses; no worker-local RNG,
partition index, or evaluation order contributes.

Users compose existing scalar expressions over the unit interval: multiply/add
for ranges, `floor(u * k)` for buckets, and `CASE` for weighted categories.
This record adds no named distribution catalogue.

### 3. Placement-invariant logical reproducibility

For the same source cardinality and authored expressions, the same seed/key
inputs produce the same multiset of output rows on repeated execution and under
different worker and exchange placement. The verification oracle is equality of
`canonical_arrow_ipc_digest(..., SortPolicy::FullRowLex)` across those outputs.

This deliberately does **not** promise raw batch order, raw IPC bytes,
materialized-table bytes, or output order. An explicit `Sort` remains the only
order contract. The later materialization record decides whether it needs a
stronger artifact guarantee.

### 4. Existing `random()` remains the nondeterministic sibling

`RANDOM` / Rune `random()` remains its current `PerCall` scalar, id, spelling,
comment, and OS-entropy evaluation behavior. It is not reseeded, aliased to, or
implemented through `SEEDED_RANDOM`.

The registry now makes the intended pair explicit: `random()` is a
non-deterministic convenience scalar; `seeded_random` is the reproducible
generator. Their distinct metadata continues to govern existing optimizer
legality rather than concealing it behind one changing name.

### 5. Generation ends at relation production

This record does not materialize a generated relation, name or create a table,
write a catalog birth certificate, introduce client or wire protocol changes,
or change `INSERT INTO ... SELECT` placement. It is an engine relation source
and scalar only.

## Acceptance

- `ctx.range(0)` returns an empty relation with exactly the non-null `ordinal:
  Int64` schema; negative cardinality refuses at the declared boundary.
- Ordinals are zero-based, exact through the accepted `i64` domain, and bind as
  ordinary columns through projection, aliasing, and integer expressions.
- Rune `seeded_random(lit(seed), ordinal)` and SQL `SEEDED_RANDOM(seed,
  ordinal)` bind to the same registry identifier and produce fixed golden
  `Float64` witnesses in `[0, 1)`.
- Repeated and placement-varied executions have equal canonical full-row IPC
  digests; unnormalized row/batch order is not asserted.
- Exact range row-count, NDV, and min/max facts reach the optimizer fact carrier
  and match the source cardinality.
- A deterministic rewrite/CSE probe preserves seeded values, proving the
  registry declaration rather than trusting it.
- Existing `random()` remains `PerCall` and retains its current behavior.

## Guardrails

The architecture guard pins the scalar registry pair: `SEEDED_RANDOM` is
`Deterministic`; `RANDOM` remains `PerCall`. It also pins the SplitMix64
reference witnesses and rejects an RNG-state or OS-entropy call in the seeded
evaluator.

Range's facts are exact only for its `ordinal` output. Do not infer facts for
derived projections or for a seeded-random output merely because the source is
known; that would turn one node's construction facts into a general statistics
inference system.

## Rejected

- **Reseed or redefine `random()`:** changes existing planner legality and
  leaves users unable to request actual entropy.
- **Stateful per-worker seeded RNG:** its output changes when placement changes.
- **Opaque generator relation or ordinal-only expression:** hides an ordinary
  column from the binder and forces every useful derivation into a growing
  operator vocabulary.
- **Named distribution family in this cut:** each distribution's parameters,
  support, and statistical claim is its own admission decision.
- **Generic key hashing:** demands a canonical encoding contract across scalar
  types; `Int64` is enough to prove the mechanism without smuggling that work.
- **Materialization or `INSERT INTO ... SELECT` distribution:** a different
  catalog/write-path contract, currently coordinator-only.

## Forward

The following PRODUCT record may spell materialization of a Rune-defined
relation, add its catalog birth certificate, and decide what reproducible
artifact means. It must not assume this ADR supplies table creation, ordering,
or a distributed write path.

Named distributions, wider seeded-key types, a SQL table-function grammar,
CSV ingest, and a distributed Arrow-IPC ingest/write path are separately
deferred.
