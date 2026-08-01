# ADR 0002: local Toron is the default data workspace

Status: accepted 2026-08-01. This PRODUCT record spans Polariton, the shared
relation boundary, Toron, and Wendelon. It names the destination posture; it
does not claim that every release gate has already passed or authorize an
all-at-once migration.

This record supersedes KET-0005 sections 3 and 4 only where they make SQLite
the destination scratch default and require an in-process, Wendelon-free
`ToronScratch` topology before Toron can become the default. KET-0005's crate
boundary, dependency guards, Arrow-as-interchange rule, capability-over-brand
rule, broad source support, and assignment of analytical execution to Toron
remain in force.

## Context

Polariton's intended center is not SQL. It is a Rune-native environment for
dataframes, relations, and plots, with local durability available without
connection ceremony. SQL remains an important compatibility and diagnostic
surface, but it is not the product language.

The first Toron dogfood loop proved that a real local Toron/Wendelon deployment
can be launched, discovered, queried, stopped, recovered, and used for plots.
It also proved why default status must be staged: overnight use found transport
teardown noise, idle write amplification, incomplete snapshot identity, and a
global-sort liveness cycle that the existing test corpus had not exposed.
Those findings are arguments for progressive promotion and continued
dogfooding, not for a different destination.

The product therefore needs a stable vocabulary before the plotting contract
can be designed. A durable plot cannot be specified honestly until the
relation it references has explicit identity, residence, provenance, and
temporal semantics.

## Decision

### 1. Toron is the local workspace; other systems are sources and peers

The destination default is a local Toron workspace. BigQuery, Postgres,
MySQL, Snowflake, SQLite, Parquet, and other systems remain supported as data
sources, pushdown targets, execution peers, and export destinations. They are
not removed when Toron becomes the default.

At the product-composition boundary, Polariton selects local Toron as its
default workspace. Below that boundary, logic asks for capabilities such as
streaming, pushdown, mutation, persistence, cancellation, and snapshot
pinning; it does not branch on product or engine brands. “Default workspace”
is a composition policy, not permission to couple editor or document-engine
crates to Toron internals.

`ToronScratch` is not the destination noun. Scratch implies disposability,
while the workspace supports both ephemeral relations and durable Toron-owned
data. User-facing and product-level code should converge on **local workspace**
or **local Toron workspace**. This record alone authorizes no mechanical
rename.

### 2. Source, relation, and view are the product nouns

- A **source** identifies where data originates and which capabilities are
  available there.
- A **relation** is the typed relational value Rune transforms and an engine
  executes, caches, or materializes.
- A **view** presents a relation as a table, plot, export, or other interactive
  product.

SQL is one way to construct or inspect a relation. It is not the relation IR
and must not constrain Rune's public algebra.

### 3. Relation lifetime is three independent axes

No single `External | Volatile | Durable` state may collapse these questions.
Defaults are specified per axis:

| axis | states | default |
|---|---|---|
| **definition lifetime** | session-scoped or saved | session-scoped |
| **data residence** | source-owned/non-resident, evictable cache, or Toron-materialized | source-owned or non-resident |
| **temporal binding** | live or pinned | live |

The axes compose. A saved definition need not materialize its data; a
materialized relation may be referenced live or at a pinned revision; a live
reference can point to Toron-owned data.

The product grammar exposes separate explicit operations, whatever exact Rune
spelling is later selected:

- **save** persists an editable relation definition in the product workspace;
- **cache** creates a reconstructible acceleration artifact;
- **materialize** creates Toron-owned durable data;
- **pin** binds a reference to an exact source or Toron revision; and
- **export** creates an external artifact such as Parquet.

“Save this dataframe” must never silently choose among those operations.

Saved editable Rune definitions are product-workspace artifacts. Toron does
not acquire ownership of Rune source or presentation semantics merely because
it is the default data workspace.

### 4. Materialization carries an immutable birth certificate

Materializing a relation snapshots its defining relation expression into the
new relation's immutable provenance even when the user's editable Rune buffer
is session-scoped or unsaved. Durable bytes may not outlive all record of how
they were produced.

The birth certificate includes the versioned relation expression, input
identities, schemas, source fingerprints or Toron revisions, and the execution
coordinates required to interpret them. It is provenance, not a second
editable copy of the user's program.

The product workspace owns the editable definition. Toron's catalog owns the
immutable birth certificate attached to Toron-materialized data. The two may
share a versioned expression format without sharing mutation authority.

Temporal binding belongs to a reference edge, not the data node. A live plot
or downstream relation resolves the current revision; a pinned reference
names an exact revision. Revision advancement drives generation-stamped,
newest-wins refresh. Stale results never overwrite a newer published view.

### 5. Cache is never authority

A cache entry derives its identity entirely from the relation definition,
source fingerprints, temporal binding, and relevant execution version. It is
evictable at any time, cannot establish provenance, and cannot survive an
identity or fingerprint mismatch. Correctness may not depend on cache
residency.

### 6. The relation expression is neutral and versioned

Rune lowers to a neutral, versioned relation expression owned by the shared
analytics contract, not to Toron's private planner IR and not semantically to
SQL text. The expression admits multiple lowering targets:

1. a deliberately disposable stage-zero SQL-text adapter for the surface that
   is already dogfooded;
2. a future Toron-native plan protocol; and
3. backend-dialect lowering for safe pushdown into external execution peers.

Replacing the SQL-text adapter must not change Rune programs. Unsupported
operations receive typed capability refusals; lowering never silently changes
semantics to fit a backend dialect.

The shared contract remains thin. Toron owns query planning, joins,
aggregation, and analytical execution. The shared layer owns relation
identity, expression and capability contracts, streaming result products, and
presentation-facing specifications—not a competing execution engine.

### 7. Parquet has distinct workflow verbs

- **scan** evaluates Parquet lazily without implying import;
- **cache** accelerates a reconstructible scan;
- **materialize** ingests a result into a Toron-owned relation; and
- **export** writes a new Parquet artifact.

Provenance records fingerprint strength. Immutable object versions, trusted
content digests, and equivalent coordinates can provide strong pins; path,
size, and modification time may provide only a weak change detector. The UI
must not present weak evidence as immutable identity.

If a saved pinned view can no longer verify its source, viewing fails visible:
render the last stamped result when one exists and display an explicit stale
or unverifiable banner. Writes, recovery, and materialization still fail
closed when their required identity cannot be established.

### 8. Durable plots preserve definition separately from binding

A durable plot is a saved `PlotSpec` referencing relation identity and
provenance, not merely a bitmap.

Durable PlotSpecs are product-workspace artifacts. Toron may execute their
relations and retain materialization provenance, but plot semantics do not
become database-core catalog semantics.

- A **live durable plot** retains its specification and follows the current
  revision of a named relation.
- A **pinned durable plot** retains its specification and an exact relation or
  source revision.
- An **exported image** is a presentation artifact, not an executable plot.

Plot refresh reuses the established coalescing contract: newest request wins
per relation/view, results carry generations or revisions, and at most one
unavoidable execution remains in flight. A later plotting ADR defines
`PlotSpec`, scale, theme, guide, interaction, and execution-policy details
against this relation contract.

### 9. Availability is promoted in stages

The destination default does not require a one-shot launch:

1. discover and attach to an already-running local profile through its
   versioned runtime and readiness contracts;
2. offer visible, attributed startup while preserving process ownership;
3. remember an explicit user's local-workspace preference; and
4. make lazy local Toron availability the release default after the product
   acceptance checklist passes.

The checklist retains KET-0005's intent while superseding its topology test:
the supported local profile may use Toron, Wendelon, QUIC, and TLS. Promotion
depends on experienced behavior—bounded startup and memory, durable restart
and upgrade, cancellation and clean shutdown, streaming results, a green Rune
workspace acceptance corpus, honest capability refusal, and repeated idle and
active dogfood soaks. Rune coverage is measured against the named, versioned
**Rune workspace acceptance corpus**, seeded with the dogfood loop's create,
insert, aggregate, order, update, delete, drop, and durability-across-restart
workflow families and extended only by recorded diary evidence. Promotion
requires that corpus to be green; “sufficient” is not an unrecorded judgment.
Default status is earned by product evidence, not by being in-process.

Polariton never stops a deployment it does not own. Auto-start and lifecycle
policy advance only through the staged path above.

### 10. Zero-copy is latitude, not a boundary

Arrow remains an interchange representation rather than a compulsory
in-memory model. Implementations should avoid unnecessary materialization and
may reuse or lease buffers where ownership and cancellation are explicit, but
literal cross-process zero-copy is not a product contract.

For large visualization, the preferred optimization is often to transfer less:
Toron computes bounded aggregates, bins, tiles, or level-of-detail products
near execution and Polariton renders them progressively. Shared memory or
other lease machinery must be justified by measured seam costs and must not
erode the public client boundary.

## Preserved boundaries

- Analytics never enters `libaryon`; Baryon does not inherit database or
  connector machinery.
- Polariton depends on Toron through the guarded public client and future thin
  shared contracts, never server internals.
- Toron owns analytical execution. Product-side crates own composition,
  interaction, and presentation.
- Capability queries govern behavior. Cargo features and distributions govern
  availability only.
- Arrow is interchange, not an analytics monolith or compulsory residence.

## Rejected

- **SQL as Rune's semantic IR:** preserves SQL's constraints beneath a new
  syntax and makes a disposable adapter permanent.
- **One dataframe lifetime enum:** conflates saving a definition,
  materializing data, and pinning time.
- **Ambient durability:** creates catalog, recovery, retention, and disk costs
  for intermediate values the user did not ask to keep.
- **Cache as durable evidence:** makes an eviction policy part of correctness.
- **Every durable plot is pinned:** prevents saved live dashboards; binding is
  an independent reference property.
- **Every durable plot is live:** silently changes saved evidence when sources
  advance.
- **Mandatory in-process Toron:** confuses deployment topology with product
  behavior and discards the working local profile.
- **Mandatory shared-memory interchange:** couples process lifetime and memory
  ownership before measurements justify it.
- **Dropping external connectors after promotion:** removes acquisition paths,
  execution peers, and correctness oracles.

## Consequences and sequencing

- Local Toron is the named destination default, reached progressively rather
  than asserted complete today.
- Rune dataframe work targets the three-axis relation contract. SQL remains a
  stage-zero lowering and diagnostic surface.
- The plotting contract is blocked behind acceptance of this record. It must
  reference relation identity as definition + residence + provenance +
  binding + stamped result.
- The next plotting record decides PlotSpec and Rune ergonomics; it does not
  reopen workspace posture or relation lifetime.
- The storage, versioning, synchronization, and project scope of saved
  product-workspace artifacts are deliberately undecided here. A separate
  record must decide the workspace artifact store before any saved-definition
  or durable-PlotSpec feature ships.
- Existing operational work—snapshot recovery, idle-write posture, sort
  completion, and local deployment hardening—continues independently and
  supplies promotion evidence.
- No implementation, dependency, auto-start, or migration change lands merely
  because this proposed record exists. Each staged change receives its own
  plan and acceptance evidence.
