# Continuous refactoring

Refactoring cleanup is allowed to travel with an ADR when it is
small, mechanical, behavior-neutral, and directly adjacent to the
surface already being changed. The codebase is still pre-V1 in
these areas; preserving stale names, stale helper shapes, or
stale test paths just because a previous ADR mentioned them
creates unnecessary fossil layers.

## Rule

- If an ADR changes a protocol/config/test surface, update nearby
  names and paths to the current conventions in the same cycle.
- Do not keep aliases, compatibility structs, duplicate test
  targets, or old command names for pre-V1 surfaces.
- Update promoted docs, evidence files, architecture guards, and
  command snippets when a mechanical rename changes the public
  spelling of a local target.
- Keep the cleanup bounded. If the refactor reaches unrelated
  modules or changes behavior, split it into a separate ADR or
  explicit cleanup commit.

## Examples

Brand and path cleanup belongs with the cycle that is already
touching the relevant surface:

- `crates/toron/tests/toron_config_test.rs` becomes
  `crates/toron/tests/config_test.rs`.
- `crates/toron/tests/toron_sql_listener_test.rs` becomes
  `crates/toron/tests/sql_listener_test.rs`.
- `crates/toron/tests/toron_tls_listener_test.rs` becomes
  `crates/toron/tests/tls_listener_test.rs`.
- `crates/toron/tests/toron_daemon_test.rs` becomes
  `crates/toron/tests/daemon_test.rs`.
- `crates/toron/tests/toron_observability_listener_test.rs`
  becomes `crates/toron/tests/observability_listener_test.rs`.

The crate path already supplies the Toron namespace. Repeating
the project name in each test target adds noise and makes command
snippets age poorly.

## Review posture

Ask three questions:

1. Is the refactor mechanically tied to the touched surface?
2. Does it preserve behavior and test coverage?
3. Were all local references updated in code, plans, evidence,
   and architecture guards?

If yes, keep the cleanup in the current cycle. If no, either
scope it down or make it an explicit follow-up.

## See also

- [no-project-name-in-protocols.md](no-project-name-in-protocols.md)
  — the naming rule this convention keeps applying as code moves.
- [no-premature-backward-compat.md](no-premature-backward-compat.md)
  — why pre-V1 old shapes should be removed rather than retained.
