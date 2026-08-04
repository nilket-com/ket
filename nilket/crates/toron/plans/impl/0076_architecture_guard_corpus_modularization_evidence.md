# ADR 076 Evidence: Architecture Guard Corpus Modularization

**Date:** 2026-05-23
**Commit base:** `a0b4d6b plan: adr 076 architecture guard corpus modularization`
**Scope:** Test-only architecture guard refactor. No production source, manifest,
dependency, Wendelon, storage, wire, telemetry, or audit behavior change.

## Baseline

Captured before moving `tests/architecture_test.rs`:

```text
cargo test -p toron --test architecture_test -- --list
cargo test -p toron --test architecture_test
```

Result:

```text
baseline leaf-name count: 938
baseline full run: 938 passed; 0 failed
```

The `-- --list` output was normalized by taking the final `::` path segment for
each `: test` row, sorting the result, and preserving duplicates.

## Post-Move Leaf Conservation

Captured after the module split:

```text
cargo test -p toron --test architecture_test -- --list
```

Result:

```text
post-move leaf-name count: 945
missing baseline leaf names: 0
new leaf names: 7
```

The only added leaf names are the ADR 076 guards:

```text
adr076_architecture_command_remains_stable
adr076_architecture_modules_are_semantic_not_adr_numbered
adr076_architecture_test_is_only_a_harness
adr076_corpus_content_guards_read_the_architecture_corpus_not_the_harness
adr076_existing_adr_guard_leaf_names_are_preserved
adr076_no_production_runtime_or_dependency_change
adr076_support_helpers_stay_test_local
```

This satisfies the duplicate-preserving leaf-name conservation gate:
`post = baseline + adr076_*`.

## Test Commands

```text
cargo test -p toron --test architecture_test
```

Result:

```text
945 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

```text
cargo test -p toron --test architecture_test adr074
```

Result:

```text
19 passed; 0 failed; 926 filtered out
```

```text
cargo test -p toron --test architecture_test adr075
```

Result:

```text
3 passed; 0 failed; 942 filtered out
```

```text
cargo test -p toron --test architecture_test adr076
```

Result:

```text
7 passed; 0 failed; 938 filtered out
```

```text
cargo clippy -p toron --test architecture_test -- -D warnings
```

Result:

```text
Finished `dev` profile [unoptimized + debuginfo]
```

```text
git diff --check
```

Result:

```text
clean
```

## Structural Evidence

- `crates/toron/tests/architecture_test.rs` is a one-line harness:
  `mod architecture;`.
- `crates/toron/tests/architecture/mod.rs` wires one support module and nine
  semantic guard modules.
- Shared architecture-test helpers now live in
  `crates/toron/tests/architecture/support.rs`, including
  `architecture_corpus_source()`.
- Self-inspecting Toron architecture guards now read the architecture corpus
  instead of the harness; Wendelon architecture-test reads remain Wendelon-scoped.
- The new ADR 076 guards pin the harness shape, semantic filenames, corpus-reader
  retarget, leaf-name preservation, test-local support, stable command, and no
  production/dependency change.
