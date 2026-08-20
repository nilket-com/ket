# ADR 0137: the durable materialization contract

Status: accepted 2026-08-20. Implementation is blocked on the shared
canonical-expression contract and the named Toron companion (§6). This record
gives PRODUCT-0002 §3's `materialize`
verb its contract: a Rune-defined relation becomes a fresh, named, Toron-owned
table carrying the §4 birth certificate, with generated relations —
TORON-0569's range and seeded-random contract — as the first consumer. It is
the viz-side half of the fifth arc's split.

It decides the certificate's content, the fresh-table rule, the identity
discipline, the refusal rules, the host receipt, and the explicit split
between this contract and its named Toron companion — and it names **two**
prerequisites, not one: the canonical-expression contract record, which must
exist before the companion can implement anything. It does not decide
table-creation policy beyond schema derivation, ordering, distributed writes,
an append or bind path, the Rune spelling of the verb, the catalog
implementation, or anything TORON-0569 fences. It examines DB-02's trigger
and finds it does not fire.

## Context

TORON-0569 landed the fifth arc's engine half: a virtual range relation with
an ordinary `Int64` ordinal, `seeded_random` as a `Deterministic` scalar with
a pinned composition and golden witnesses, and placement-invariant pair
multisets proven by canonical digests. A generated relation now exists,
executes, and reproduces — but it cannot be kept. Reusing a fixture means
re-running its program.

PRODUCT-0002 already owns the vocabulary this record instantiates. §3 names
`materialize` — "creates Toron-owned durable data" — as one of five explicit
operations that "'Save this dataframe' must never silently choose among." §4
sets the certificate's bar: "Durable bytes may not outlive all record of how
they were produced," enumerates its content — the versioned relation
expression, input identities, schemas, source fingerprints or revisions, and
the execution coordinates required to interpret them — and assigns ownership:
"Toron's catalog owns the immutable birth certificate attached to
Toron-materialized data."

Toron's catalog cannot hold one today. Its only provenance is fact-grade —
`UniqueKeyProofProvenance` on key proofs — and its table descriptors are
schema-and-statistics carriers from `wendelon_client`. A certificate
attachment is new catalog surface, and PRODUCT-0003 fences Polariton from
implementing relation verbs at all: "Polariton owns only the terminal view
envelope." So both the verb's spelling and the certificate's storage are
Toron work, and this record's job is to be the contract they implement — the
PRODUCT-0107/TORON-552 precedent with the order reversed: the product record
specifies; the companion realizes.

Two honesty constraints frame the scope. TORON-0569's write path is
coordinator-only — `INSERT INTO ... SELECT` is explicitly non-fragmentable —
so materialization has a throughput ceiling this record inherits and must not
promise around. And the huge fixtures that motivated generation do not need
materialization at all: a generated relation executes live, at any
cardinality, every time. **Materialize serves reuse, not scale.**

## Decision

### 1. Materialize creates; it never binds, appends, or replaces

Materialization produces a **fresh, named, Toron-owned table** whose entire
content is the materialized relation's output. A name collision **refuses** —
the create-never-clobbers discipline of PRODUCT-0134 §4 and PRODUCT-0136,
now applied a third time rather than re-derived.

Binding an existing table is rejected, not deferred as a nicety: §4's
totality clause decides it. A certificate is total only when the table was
born from the materialization it describes. An existing target has rows the
certificate cannot cover, and covering them would require per-row provenance
— a different and much larger contract. Append is ingest's cousin, not
materialize's variant.

A materialized table is thereafter an **ordinary Toron table**: addressable
as `ctx.table(name)`, queryable from SQL, subject to every existing rule.
There is no "generated table" kind; the certificate distinguishes provenance,
never type.

### 2. The certificate is atomic with the table's visibility

§4's totality rule has a temporal edge this contract must close: **no
committed table may exist, for any observable interval, without its immutable
certificate.** The companion satisfies this one of two ways, and must name
which:

- **one atomic admission** — table visibility and certificate publication
  commit together in the catalog, indivisibly; or
- **certificate-first ordering with total failure** — if the certificate
  cannot be recorded, the materialization fails whole: the name stays
  unbound, and no durable rows are observable through any read path.

There is no third state. A table without a certificate is not a degraded
success to repair later; it is the §4 violation this record exists to
prevent. The companion's implementation plan must carry a **fault-injection
acceptance gate**: a failure injected between the durable write and the
certificate publication leaves the name unbound and no rows visible, proven,
not assumed.

### 3. The certificate instantiates PRODUCT-0002 §4, slot by slot

For a materialized generated relation, §4's enumeration resolves exactly:

- **The canonical relation definition** — the range-plus-projections program
  in its canonical serialized form. This is the certificate's authority;
  everything else is derived from or names it.
- **Input identities: none.** Generation is the case where this set is
  empty, stated as a first-class fact rather than an omitted field. This is
  precisely why generated relations are the contract's first consumer: the
  certificate is complete by construction.
- **The derived schema**, as produced by the relation.
- **The execution coordinates, as public contract identities — never an
  engine build string and never private plan structures.** Three named
  parts: the **canonical definition's versioned expression format** — the
  neutral versioned relation expression of PRODUCT-0002 §6, which **does not
  exist yet**: no shared canonical-serialization surface is implemented in
  Polariton, Toron, or a shared crate today, and PRODUCT-0003 assigns the
  neutral contract to the shared analytics layer, not to Toron's private
  `LogicalPlan`. This record cites the format; a prerequisite record must
  define and implement it (§6). The companion must not serialize
  `LogicalPlan` or any internal IR as the canonical form, and equally must
  not invent the format itself — a quietly invented serialization would be
  the shared contract, unrecorded; the **public Rune compatibility coordinates** of
  PRODUCT-0003 — the request-protocol, language-surface, and
  sandbox-contract version triple current at materialization; and the
  **generator-contract identity by record citation, `TORON-0569`**, whose
  pinned formula and golden witnesses make the citation stable across
  builds. A superseding generator record mints a new identity rather than
  mutating this one.
- **Literal seed witnesses — optional, extracted, and explicitly
  non-exhaustive.** TORON-0569's `seeded_random(seed, key)` accepts
  ordinary `Int64` *expressions*, so a seed need not be a literal at all.
  The certificate therefore carries a witness list of the seed-position
  literals extractable from the canonical form — derived at materialization,
  never author-supplied — and the list is marked non-exhaustive by
  contract: a computed seed appears only in the definition. The canonical
  definition remains the sole complete authority; the witnesses exist so
  fixture tooling can read the common case without parsing, and they can
  never disagree with the definition because they have no independent
  source.

The certificate is immutable from birth, per §4. It is not a second editable
copy of the program; the product workspace keeps the editable definition,
exactly as §4 divides the ownership.

### 4. Identity is PRODUCT-0134 §4's discipline, verbatim

The table name is user-chosen, compared exactly after trimming leading and
trailing whitespace; empty and all-whitespace names refuse; no case folding,
Unicode normalization, or similarity rule applies. This is the third surface
to carry that identity rule, and it adopts the existing one rather than
minting a third variant.

The rule is compatible with Toron's identifier handling as it stands — SQL
lowering preserves `Ident.value` and `TableRef` stores the string exactly —
and the companion should carry that citation as grounding rather than
re-deriving the compatibility claim.

### 5. The schema is derived wholly from the relation

Materialization derives every column name and type from the relation's own
schema. It authors nothing: no additional columns, no type overrides, no
constraints, no keys, no defaults. Whatever DDL vocabulary Toron's
`CREATE TABLE` supports or refuses is untouched — this record needs only
"the relation's schema becomes the table's schema," and decides nothing
larger about table creation.

### 6. The split is explicit: contract here, surface in the companion

**This PRODUCT record** defines the durable-materialization contract — the
fresh-table rule, atomic certificate publication, certificate content,
identity, refusals — and the host receipt of §7.

**A prerequisite canonical-expression record** — prior to or paired with the
companion — defines and implements the public canonical relation-expression
format this certificate stores: PRODUCT-0002 §6's neutral versioned
expression, living in the shared analytics layer per PRODUCT-0003 and
KET-0005's interchange-crate boundary, consumable by both heads and by
Toron. Until it exists, the certificate's first slot has no lawful
serialization.

**A named TORON companion record**, negotiated in that lineage's council
idiom, adds what Toron lacks: the catalog's certificate surface and the
materialize verb's Rune spelling, implemented against this contract **and
consuming the canonical-expression contract — companion implementation
halts until that record exists and is implemented.** Its write path inherits
TORON-0569's fences unchanged: coordinator-only writes as the stated
ceiling, no ordering promise, no distributed generated-table writes.
Repairing that ceiling is future Toron work with its own record, not a
promise either record makes.

Acceptance of this record does not wait for either prerequisite;
implementation waits for both, in their stated order. That is the fourth
arc's dependency shape with one more link in the chain, named rather than
discovered mid-implementation.

### 7. The host receipt is routine status, and no more

A successful materialization reports through Polariton's routine `status_msg`
lane: the new table's name and confirmation that its materialization
certificate was recorded. Failures — collision, refused name, engine or
write error — surface as diagnostics through the existing paths.

No richer display ships in this cut: no certificate viewer, no materialized-
table browser, no provenance panel. Those are display surfaces over catalog
data that the companion has not yet created, and each would be its own
record against real catalog reads.

### 8. DB-02's trigger is examined and does not fire

The workspace artifact store's if-asked trigger asks for a persistence
requirement that store alone would satisfy. This record's persistence lives
in **Toron's catalog**, where §4 assigns it — not in a product workspace
store. DB-02 therefore stays if-asked, untouched, and this record may not be
cited as its trigger.

## Rejected

- **Binding or appending to an existing table.** Mixed provenance breaks §4's
  totality; per-row provenance is a different contract.
- **Overwrite on collision, or an overwrite prompt.** The operation carries
  the intent; create refuses, as it does in the two records this discipline
  is adopted from.
- **An engine-version string as the generator identity.** Unstable across
  builds and names nothing a reader can consult; the contract citation is
  the identity.
- **Author-supplied seed metadata.** A seeds field that can disagree with the
  definition is a second source of truth; extraction has no independent
  source and cannot drift.
- **Treating the seed witnesses as exhaustive.** 0569 seeds are expressions;
  an exhaustive claim would be false the first time a seed is computed, and
  tooling built on it would silently miss seeds.
- **A committed table without its certificate, however briefly.** §4's
  totality has no grace interval; certificate-later designs turn a crash
  into a permanent provenance hole.
- **Serializing private `LogicalPlan` as the canonical definition.** The
  canonical form is PRODUCT-0002 §6's neutral versioned expression; internal
  IR is neither versioned for this purpose nor a public contract.
- **Letting the companion invent the canonical format.** Whatever
  serialization the companion chose would *be* the shared contract, made
  without a record in the wrong lineage; the format is defined once, in the
  prerequisite record, where both teams can hold it.
- **A "generated table" type or flag.** Provenance lives in the certificate;
  a type would leak it into every consumer's dispatch.
- **Deciding the Rune spelling here.** PRODUCT-0003 fences relation verbs out
  of Polariton; the spelling belongs to the Toron companion.
- **Storing the certificate in a workspace artifact store.** Contradicts §4's
  ownership assignment and would fire DB-02 for a consumer that does not
  need it.
- **Any ordering, byte-reproducibility, or distributed-write promise.**
  TORON-0569 §3 and its Grounded limits govern; a stored table's stronger
  artifact guarantees are future work over the canonical-digest vocabulary.
- **A richer receipt.** Certificate viewers and provenance panels are display
  records over catalog reads that do not exist yet.

## Consequences and sequencing

**The canonical-expression contract is the next negotiation, before or
beside the companion.** It is shared-layer work — the natural home is the
interchange boundary KET-0005 drew — and it is the first record the fifth
arc has forced into existence rather than merely consumed, which is evidence
the arc is doing its paradigm-setting job.

**The TORON companion follows**: the catalog certificate surface and the
materialize verb, implemented against §§1–6 and consuming the
canonical-expression contract, inheriting 0569's fences. Both records should
travel to the db-team council with the same handoff mechanics the
0565-drafted/0569-promoted record used, with the halt explicit: no companion
implementation before the canonical format exists.

**Polariton's implementation is small and waits on the companion**: the
receipt wording of §6 and whatever thin surface the companion's verb needs
reflected host-side. No Polariton code lands before the companion promotes.

**The fifth arc closes on three artifacts, not two**: generation
(TORON-0569, landed); the **implemented canonical-expression contract** (§6's
prerequisite — without it the certificate's first slot has no lawful
serialization, so no materialization can exist); and durable reuse (this
contract plus its companion, which consumes the second). CSV import remains
held behind the separate Arrow-IPC ingest contract and now inherits both a
settled materialization contract and the shared expression format when that
work begins, which strengthens the sequencing argument this arc was chosen
on.

**Deferred, each requiring its own record:** append/bind with per-row
provenance; certificate display surfaces; stronger stored-artifact
reproducibility over the canonical-digest vocabulary; distributed
materialization writes; materializing non-generated relations with non-empty
input identities — the certificate slots exist for them, but their source
fingerprints and revision semantics deserve their own examination against
live sources.
