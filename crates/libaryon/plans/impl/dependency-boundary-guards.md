# Guards for the ADR 0005 dependency boundary

Scope: enforce the product-boundary dependency direction structurally, in
the established audit-test family. Small pair. Deliverable is one `impl:`
commit reviewed against this plan.

## The rule being enforced

From ADR 0005: `libaryon` is the headless document engine and gains no
analytics machinery; heads depend on `libaryon` and the (future) shared
analytics crate; the analytics crate depends on no editor crate. The
loudest concrete consequence: **`libaryon` must not depend on Arrow — or
any of the analytics/connector stack.** Today that is true by accident;
after this pair it is true by construction.

## Guards

1. **`libaryon` manifest denylist** (architecture test, scanning its
   `Cargo.toml` plus the workspace root manifest): scan ordinary and
   target-specific `[dependencies]`, `[dev-dependencies]`, and
   `[build-dependencies]` tables. Resolve `workspace = true` entries through
   `[workspace.dependencies]`, including `package =` renames there. Forbid
   crates whose resolved package names match
   `arrow*`, `parquet*`, `polars*`, `datafusion*`, `duckdb*`, `*sqlite*`,
   `libsql*`, `sqlx*`, `*postgres*`, `mysql*`, `odbc*`, `connectorx*`,
   `gcloud*`/`*bigquery*`, `*snowflake*`, and — once it exists —
   the shared analytics crate itself (`boson` is guarded as the reserved
   working name; the analytics crate's birth commit must update this name if
   the final choice differs). The assertion message cites ADR
   0005 and states the only exception path: *amend the ADR first, then
   the denylist* — a dependency addition without an ADR amendment must be
   un-mergeable, not merely questionable.
2. **`baryon` manifest denylist**: the same list, minus the shared
   analytics crate (ADR 0005 explicitly permits baryon as an optional
   plot-raster consumer of it later). Guard lives beside baryon's
   existing architecture test.
3. **Birth-certificate requirement** (recorded here, activated when the
   analytics crate is created): its own test scaffold must ship, from its
   first commit, the reciprocal guard — no dependency on `libaryon`,
   `baryon`, or `polariton`. The plan for that crate's creation must
   reference this clause; a crate born without its guard fails this
   plan's review checklist retroactively.
4. **Scanner correctness**: both architecture suites compile the same small
   pure helper from workspace test support (section-aware,
   comment-tolerant, no TOML dependency). It matches dependency names,
   `package =` renames, target-specific tables, and workspace inheritance.
   Unit tests use synthetic manifests, including both rename evasions:
   `foo = { package = "arrow" }`, and local `foo.workspace = true` resolving
   to workspace `foo = { package = "arrow" }`.

## Non-goals

- Transitive-dependency policing via `Cargo.lock` — the ADR decides the
  *direct* boundary; transitive graphs of vetted deps are not the threat
  model. Revisit only if a vetted dep starts smuggling an engine.
- Guarding `polariton` — it is a head and may depend on everything.
- Workspace-wide generic dependency lint frameworks (`cargo-deny` etc.) —
  the audit-family test is dependency-free, self-documenting, and cites
  the ADR in its failure message; that is the house style. A future
  `cargo-deny` adoption may subsume these guards; it does not replace the
  ADR-citation requirement.

## Review checklist

- [ ] Denylists live in the audit family; failure messages cite ADR 0005
      and the amendment-first exception path.
- [ ] All three dependency kinds and their target-specific forms scanned;
      local and workspace `package =` renames caught; scanner unit-tested
      against synthetic manifests.
- [ ] Baryon variant permits the future analytics crate by name or by
      documented allowlist slot.
- [ ] Birth-certificate clause recorded; ADR 0005 gains a one-line
      enforcement note pointing here.
- [ ] Zero new dependencies; zero behavior change; suites green.
