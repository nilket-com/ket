# ADR 0005: one workbench, many sources

Status: accepted 2026-07-31, converged three-way (user, Claude, Codex).
Scope is workspace-wide; the ADR series lives here by the ADR-0002
convention. This record decides a **boundary** and a **default** — both
reversible by packaging profile. It deliberately does not decide a
company.

## Context

Polariton began as a data workbench (DataGrip × JupyterLab); Baryon was
born from it when no off-the-shelf editor could execute selected SQL; the
adjacent `../nilket` workspace (Toron: distributed compute/SQL; Wendelon:
distributed storage; ~530k LOC) is a serious backend laboratory whose SQL
surface, type depth, optimizer, and benchmark maturity are — by its own
reachability record — early. Three product visions were considered:
(1) two products (Toron-default vs broad); (2) one workbench with many
connectors, Toron privileged; (3) one lean Toron/Wendelon-only client.

## Decision

**Option 2 — with the reframing that dissolves the choice: options 1 and
3 are limits of option 2, not alternatives.** Capability profiles over
one source tree make option 1 a distribution decision available any
quarter, and option 3 the asymptotic state as Toron passes promotion
gates and connectors demote to optional features. What is decided today:

### 1. The crate boundary

| layer | owns |
|---|---|
| `libaryon` | documents, commands, panes, UAST — the headless editor engine, unchanged |
| **shared interchange/plot-contract crate** (working name open; `boson` suggested — the exchange particle, for an interchange boundary) | data-source contracts, Arrow record-batch streams, result products, `PlotSpec`/`PlotThemeSpec`/`PlotExecutionPolicy` — not analytical execution |
| `baryon` | terminal head; *optional* plot-raster consumer |
| `polariton` | graphical workbench head |
| Toron / Polars | optional execution engines behind the seam |
| connectors | optional data sources behind the seam |

Dependency direction: heads depend on `libaryon` and the interchange crate;
the interchange crate depends on **neither** editor crate. Analytics never
enters `libaryon`; editor semantics never enter the interchange crate.
Heads compose the two (e.g., bridging `UiPalette` chrome roles with
`PlotThemeSpec` happens head-side or in a thin bridge, never by merging
the palettes).

**PRODUCT-0001 ownership clarification:** almost all analytical execution —
query planning, joins, aggregation, and transforms — belongs in Toron. The
shared crate is a thin contract/adapter boundary, not a second analytics
engine. Libaryon's text indexes are document infrastructure, not precedent for
ad hoc data analytics. A future workbench library may own composition,
interaction, and presentation without acquiring Toron's execution role.

### 2. The interchange seam

Bounded, cancellable **Arrow record-batch streams** are the product
boundary: `DataSource → stream → optional transform engine →
ResultProduct → table/plot/export`. Arrow is the **interchange
representation, not a compulsory in-memory representation**: engines keep
native layouts internally; conversion happens at the streaming seam only.
Polars becomes one optional consumer/transform engine behind it — the
path that obviates it without a rewrite cliff.

### 3. Capability profiles, not brands

Profiles (e.g., *Polariton*: broad connectors, SQLite scratch default,
Toron available; *Nilket Studio*: same application, Toron prominent,
optional local Toron/Wendelon lifecycle) are **packaging presets over
runtime capability queries** — streaming, pushdown, cancellation,
mutation — asked of each source at runtime. Product logic never asks
which branded distribution is running; `#[cfg]` selects what ships, never
how logic branches. Cargo features and adapter registries suffice; a
stable plugin ABI waits until distribution size or release cadence proves
it necessary.

### 4. Toron: privileged, not exclusive — with a written promotion gate

SQLite remains the instant scratch default until **all** of:

1. a supported in-process `ToronScratch` API over its in-memory storage;
2. no QUIC, TLS, Wendelon, or daemon required;
3. editor startup does not notice it;
4. DDL/DML/query parity against Polariton's existing scratch acceptance
   corpus;
5. bounded memory, cancellation, clean shutdown, Arrow streaming;
6. honest refusal for unsupported SQL.

Until then Toron is an explicitly selected connection. External adapters
are retained deliberately: they are acquisition paths *and correctness
oracles* — the proving ground that disciplines the laboratory into
product slices, as Baryon disciplined libaryon.

## Rejected

- **Option 3 now**: couples the product's usefulness to Toron's SQL
  completeness, removes the DataGrip half, eliminates the easiest
  bring-your-data path, and discards reference oracles. Risk
  concentration, not leanness.
- **Two forked UI products**: settings, Rune APIs, plotting, fixes, and
  docs diverge immediately. One tree, profiles.
- **Polars as the product boundary**: inverts the obviate-Polars
  trajectory already on record.
- **Analytics inside `libaryon`**: dependency inversion; burdens the
  terminal head; erodes the headless-document definition defended all
  week.

## Consequences

- **The dependency direction is guard-enforced**
  (`impl/dependency-boundary-guards.md`): manifest denylists in the audit
  family keep Arrow and the analytics/connector stack out of `libaryon`
  (and mostly out of `baryon`), with amendment-of-this-ADR as the sole
  exception path. The recorded fantasy exception — semantic-indexing
  machinery someday wanting columnar data — points the *other* way:
  libaryon exposes harvest hooks; the analytics crate consumes them.
- Polariton's existing closed adapter surface migrates toward the seam
  **incrementally** — this is a migration, not a greenfield.
- **A thermal benchmark fixture is created now** (before any tile
  engine): today's scatter kernels' point-count thresholds and
  frame/scan costs, recorded so future kernel work cannot silently move
  the baseline while the multiresolution architecture waits, and so the
  eventual engine has a before-number.
- ADR 0006 (the plotting contract) is next, placing `PlotSpec`,
  `PlotThemeSpec`, and `PlotExecutionPolicy` in the shared crate with the
  cascade algebra and interactive-override policy.
- Sequence thereafter: theme boundary → PlotSpec/resolver + golden
  fixtures → Rune lowering → kernel migration → evidence-gated
  multiresolution plot engine.
