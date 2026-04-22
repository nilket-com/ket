# Simulator Seed Registry

Frozen simulator seeds live in this directory as TOML files named `{issue_number}.seed`.

Example:

```toml
issue = 1234
scenario = "coverage_ladder"
seed = 424242
ops = 10000
introduced_at = "2026-04-21"
notes = "Short note describing the invariant or regression."
```

Rules:

- The filename stem must match `issue`.
- `scenario` must name a supported simulator scenario.
- `introduced_at` must use `YYYY-MM-DD`.
- Frozen seeds are deleted only by explicit PR, with the retirement recorded in `tests/seeds/RETIRED.md`.

The weekly `seed-retention-advisory.yml` workflow scans this registry and the
chaos registry to produce an advisory retirement report; the workflow never
removes seeds on its own.
