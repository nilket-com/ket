# ADR 0712: test-only WAL page writers are test-only

Status: proposed, 2026-11-10 (Claude, Codex). Implementation lands with this plan. Unblocks the operator's §8 campaign decision (2026-11-10, option 1). Review record: `reviews/s8-campaign-20261110/`.

## Why
- **The campaign fails closed on main.** The frozen §8 launcher (`nilket/scripts/s7-measure/run.sh`) accepts only a campaign release build with `build_warnings == 0`. The campaign build of main 0e83f5560 (binary f499db72…) has two `dead_code` warnings, so run.sh refused block 1 before anything ran.
- **The two warnings.** `serialize_entry_page` and `serialize_checkpoint_page` (`consensus/raft_storage.rs`) are gated `#[cfg(any(test, feature = "test-support"))]`. Since ADR 0699 (2aacd38cd), production writes through the versioned `serialize_entry_page_as`, and the two functions' only callers are `#[cfg(test)]` modules (`raft_storage/read_only.rs` `fixture`, `root2_carrier/wal_tests.rs`, `raft_storage/decision_mode_tests.rs`).
- **Why only the campaign sees them.** The campaign builds toron-tpch's `s7-measure` feature, which enables `wendelon/test-support`, so the functions compile there with no caller. Both are private, so no other crate can reach them.
- **Why the 2026-11-06 builds were clean.** They predate ADR 0699 (0 warnings).

## Decision
**Both functions are gated `#[cfg(test)]`.** Nothing else changes:
- No behaviour or format change.
- The harness, the manifest, the overlay and the launcher's zero-warning precondition are unchanged.

## Acceptance (finite)
1. `cargo check -p wendelon --features test-support` and `cargo check -p wendelon --tests` are warning-free.
2. The campaign build (`build-arms.sh fix`, campaign mode, the reviewed overlay 5fafd987…) of the tip records `build_warnings == 0`.
3. The checkpoint for this change is a compile and build checkpoint, sized to it, not a full test-suite run. Both helpers stay compiled under `cfg(test)` exactly as before, and no test semantics or acceptance predicate is waived. On the final source it consists of: `cargo fmt --check`; warning-free `check -p wendelon --features test-support` and `check -p wendelon --tests`; the commit policy, signature and tree checks; and the rebuilt campaign binary recording `build_warnings == 0` on its own.

## Not established
- Anything about §8 itself. It stays FAILED until its frozen campaign decides it.
