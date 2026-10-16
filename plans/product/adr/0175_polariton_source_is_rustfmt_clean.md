# ADR 0175: Polariton source is rustfmt clean

Status: accepted 2026-10-16. Repository-health lane; independent of ADR 0174,
which restores the architecture guard. `ci/check.sh` begins with
`cargo fmt --check`. Since ADR 0171 and ADR 0173, Polariton's committed source
fails that check, so the gate stops at its first step before any test runs.

## Decision

### 1. Format Polariton with the workspace rustfmt configuration

Run `cargo fmt -p polariton` and commit the result. The change is mechanical:
it touches `src/gui/state.rs`, `src/gui/state/local_rune_tests.rs`,
`src/main.rs`, `src/rune_local.rs`, `src/rune_presentation.rs` and
`tests/product_0057_render_evidence.rs`, and nothing else. No rustfmt
configuration or toolchain change is part of it.

### 2. Evidence

The implementation commit shows:

- `cargo fmt --check -p polariton` passes;
- the formatted tree builds every Polariton target with no new warnings;
- a token comparison against this plan's parent finds only rustfmt's
  behaviour-preserving rewrites: whitespace and line breaks, trailing commas in
  split lists, sorted `mod` declarations, and braces added or removed around a
  single-expression closure body or `match` arm;
- every Polariton test target passes.

## Consequences

The formatting commit touches recently changed Rune and MCP files, so open
work in those files rebases over whitespace. No behaviour changes. Keeping the
check green afterwards is each change's responsibility, as it already is for
the other crates.
