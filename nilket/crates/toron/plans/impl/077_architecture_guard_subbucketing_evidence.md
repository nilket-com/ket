# ADR 077 Implementation Evidence: Architecture Guard Sub-Bucketing

**Date:** 2026-05-23
**Scope:** Test-only refactor of the Toron `architecture_test` corpus. No
production, manifest, dependency, Wendelon, SQL, storage, wire, telemetry, or
audit behavior change.

## Shape

The two ADR 076 jumbo modules were converted to semantic directory modules:

- `tests/architecture/distributed_execution/`
  - `mod.rs`
  - `tenant_admission.rs`
  - `descriptor_sync.rs`
  - `cancellation.rs`
  - `sql_ingress.rs`
  - `worker_lifecycle.rs`
  - `scheduler_dispatch.rs`
  - `resource_governance.rs`
- `tests/architecture/storage_wendelon_boundary/`
  - `mod.rs`
  - `ddl_admin.rs`
  - `row_identity.rs`
  - `dml_delete_update.rs`
  - `returning.rs`
  - `descriptor_invalidation.rs`
  - `wendelon_catalog.rs`

`tests/architecture/mod.rs` keeps the existing `pub(crate) mod
distributed_execution;` and `pub(crate) mod storage_wendelon_boundary;`
declarations. Rust module resolution now targets each directory's `mod.rs`.

## Leaf Conservation

Source-derived duplicate-preserving leaf-name check:

```text
before=945 after=950 missing=0 added=5
added:
  adr077_architecture_command_remains_stable
  adr077_architecture_corpus_source_recurses_subdirectories
  adr077_leaf_names_conserved_after_subbucketing
  adr077_no_architecture_module_exceeds_line_ceiling
  adr077_second_level_modules_are_semantic_not_adr_numbered
non_adr077_added=[]
```

Every pre-move leaf is still present; the only additions are the five ADR 077
structure guards.

Moved-block comparison against `HEAD`:

```text
distributed_execution old=262 new=262 missing=0 added=0 changed_after_rstrip=0
storage_wendelon_boundary old=276 new=276 missing=0 added=0 changed_after_rstrip=0
```

## Line Ceiling

Largest post-split architecture modules:

```text
2175 sql_planner_surface.rs
1688 storage_wendelon_boundary/dml_delete_update.rs
1674 observability_audit.rs
1599 network_tls.rs
1489 storage_wendelon_boundary/ddl_admin.rs
1243 distributed_execution/resource_governance.rs
```

All non-`support` architecture modules are below the 2500-line ceiling.

## Commands

```text
cargo fmt --package toron
cargo test -p toron --test architecture_test --no-run
cargo test -p toron --test architecture_test adr077_
cargo test -p toron --test architecture_test adr076_
cargo test -p toron --test architecture_test
cargo clippy -p toron --test architecture_test -- -D warnings
git diff --check
```

Results:

```text
cargo test -p toron --test architecture_test --no-run: pass
cargo test -p toron --test architecture_test adr077_: 5 passed, 945 filtered out
cargo test -p toron --test architecture_test adr076_: 7 passed, 943 filtered out
cargo test -p toron --test architecture_test: 950 passed
cargo clippy -p toron --test architecture_test -- -D warnings: pass
git diff --check: clean
```

## Notes

- `architecture_corpus_source()` and `source_files()` were not changed; the
  existing recursive `collect_rs_files()` path picks up the new subdirectories.
- The new `mod.rs` files reexport their semantic submodules to preserve existing
  wildcard guard-call imports.
- The split was mechanical: test names, test bodies, and test attributes were
  preserved.
