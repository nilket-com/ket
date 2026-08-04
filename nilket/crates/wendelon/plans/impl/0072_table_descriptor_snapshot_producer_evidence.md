# ADR 072 implementation evidence

ADR 072 is implemented as a Wendelon Control Ring read-only descriptor
snapshot producer. It adds the 0x3C/0x3D descriptor-get wire pair, a
leader-only authorized handler over the applied schema catalog, aggregate
outcome metrics, a client helper with leader-hint retry, and the ADR 072
architecture guards.

## Commands

- `cargo fmt --all` - pass.
- `cargo fmt --check` - pass.
- `git diff --check` - pass.
- `cargo check -p wendelon` - pass.
- `cargo test -p wendelon descriptor_get_ --lib` - pass, 11 focused
  descriptor-get unit tests.
- `cargo test -p wendelon client_get_table_schema_descriptor --lib` -
  pass, 3 focused client helper tests.
- `cargo test -p wendelon adr072 --test architecture_test` - pass, 19
  ADR 072 architecture/convention guards.
- `cargo test -p wendelon --test architecture_test` - pass, 67 full
  Wendelon architecture guards.
- `cargo test -p wendelon client_get_table_schema_descriptor --no-run` -
  pass, full target build for the client-focused test filter.
- `cargo clippy -p wendelon --all-targets -- -D warnings` - pass.
- `cargo test -p wendelon` - pass, full Wendelon package suite.

## Notes

- The `--all-targets` clippy gate surfaced pre-existing test and bench
  nits outside the ADR 072 production surface. They were cleaned in the
  same cycle as behavior-neutral gate work.
- No Toron production code changed. The Toron bind-miss consumer remains
  the next Toron ADR.
