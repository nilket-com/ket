# Complete the PRODUCT-0005 discovery proofs

Scope: close the five gaps between PRODUCT-0005's planned test suite
(`0005_discover_and_attach_local_workspace.md`) and the suite that landed in
`88c35581`. Production behavior is correct and stays unchanged. This plan adds
tests, permits two narrow private seams where deterministic synchronization
requires them, and amends one architecture guard that would otherwise block a
legitimate test dependency. Deliverable is one `test:` commit.

No contract, state, ordering, public API, or lifecycle change. If a proof cannot
be written without changing behavior, the finding is escalated rather than
absorbed.

## Why these five

Four of the five gaps sit on code paths added in response to the two review
rounds that preceded implementation: identity re-observation, the
`Stale`/`Corrupt` arms widened in round two, and the Polariton attach polarity.
Late-arriving contracts arrived without their proofs. That is the pattern this
plan closes, and the reason the two most load-bearing proofs come first.

## Seams

Both seams are private functions in `nilket-local`, exercised from the crate's
inline `mod tests`. Neither adds a public item, so no consumer can reach them and
the existing leak guards keep holding.

1. **Injectable health.** `discover_blocking_root` (`lib.rs:400`) and
   `discover_async_root` (`lib.rs:346`) call `toron_client` inline at `:413` and
   `:359-362`. Extract the probe into a parameter — a `&dyn Fn(SocketAddr) ->
   Result<(), String>` for the blocking path and its async counterpart — with the
   current call sites becoming thin wrappers that pass the real probe. The retry
   arm (`Confirmation::Changed if !retried`) must stay in the shared body; the
   point of the seam is to test that logic, not to duplicate it.
2. **Observation pause.** Add a private `observe_identity_with(root, pause: &dyn
   Fn())` invoking `pause` inside the lock-observation step, with
   `observe_identity` becoming a wrapper passing a no-op. This exists solely so
   the race assay can hold the observer mid-observation deterministically.

Both wrappers must be trivial enough that the tested path and the production path
cannot diverge. The lifecycle guard at `lib.rs:932` already forbids the strings
that would matter; it keeps applying to the production section unchanged.

## 1. Identity stability, both signs

Currently untested: `Confirmation` handling at `lib.rs:363-372` and `:415-424`,
and `Evidence::StableIdentity` at `:369` and `:421`.

Using seam 1, the injected probe mutates the profile mid-flight by rewriting
`runtime.json` with a new `session_nonce` and PID before returning:

- **False `Ready`** — probe returns `Ok(())` after substituting the session. The
  result must not be a `Ready` pairing the pre-health identity with the
  post-health session. One substitution re-classifies against the new profile
  state.
- **False `Unhealthy`** — probe returns `Err` after substituting the session. The
  result must not be `Unhealthy` naming the superseded identity, which would blame
  a newly started session for its predecessor's silence. This is the sign the
  round-two review added and the one a success-only test would miss.
- **Churn** — probe substitutes on both the first and second attempt. Result is
  `Unverifiable { StableIdentity }`, proving exactly one retry, not a loop.
- **Stability positive control** — a probe that mutates nothing yields `Ready`,
  proving the assertions above fail for the right reason rather than because
  discovery never returns `Ready` under a seam.

Run this against the blocking path and the async path. They carry independent
copies of the retry arm (`:418` and `:366`), so a single-path test would leave one
copy unproven.

Additionally, unit-test `confirm_identity` directly for its `Stable`, `Settled`,
and `Changed` arms, so a future refactor that inlines it keeps the classification
under test.

## 2. Barrier-synchronized race assay

Currently untested: the property that observation acquires no lock. The only
`Barrier` in the tree is TORON-532's pre-existing
`reset_preserves_the_locked_inode_and_excludes_a_competitor`
(`nilket/src/local.rs:823`), which proves something else.

The observer, never the helper, holds the lock in the control branch. An acquiring
helper that already held it would starve acquisition under both implementations
and prove nothing. Two branches over one acquisition helper, differing only in
observation strategy:

- **safe** — using seam 2, the real observation pauses at the barrier holding no
  `flock`; the helper's acquisition must succeed;
- **control** — a test-local observer that acquires `flock` on `local.lock` and
  then pauses at the same barrier; the same helper must receive `EWOULDBLOCK`.

Both the helper and the control observer use `libc::flock` with `LOCK_EX |
LOCK_NB` directly in test code. This is what `fs2::FileExt::try_lock_exclusive`
compiles to on Unix, so the semantics are identical, and it avoids adding `fs2` to
a crate whose dependency guard forbids it. `libc` is already a dependency, and the
guard's `libc::` occurrence count applies only to the production section.

The control must also assert the product consequence, not merely the syscall
result: with the control observer holding the lock, a real `nilket local` startup
in that position dies with the `report_existing` diagnostic. This test belongs in
the `nilket` crate, which owns both `open_lock` and the supervisor, and it uses
the **real** `try_lock_exclusive` acquisition path rather than a replica — a
concurrent `blocking_discover` must never make that acquisition fail.

## 3. `Stale::OwnerGone` and `Stale::PidRecycled`

Constructed at `lib.rs:616` and `:626` with no fixture. `PidRecycled` is the arm
defending against the identity-from-insufficient-bits class, so it is not
optional coverage.

- **`PidRecycled`** — write a manifest naming the live test process's own PID with
  a deliberately wrong `process_start_token`, holding the lock from that same
  process. The owner is alive, the lock matches, and only the token disagrees.
  Fully deterministic.
- **`OwnerGone`** — spawn a trivial child, reap it, and write a manifest naming
  its PID. Because PID reuse between reaping and observing would silently convert
  this into a different state, the fixture asserts `/proc/<pid>` absence as an
  explicit precondition and fails with a clear message rather than proceeding on a
  recycled PID.

Both fixtures assert the exact `StaleCause`, not merely `Stale`.

## 4. `Corrupt { ProfileRoot }`

`symlinked_identity_artifact_is_corrupt_and_not_followed` (`lib.rs:888`) covers a
symlinked *marker*; the root arms are unexercised. Three fixtures, each asserting
`Artifact::ProfileRoot`:

- root is a symlink to a real directory;
- root exists as a regular file rather than a directory;
- root holds generated content — `config/`, `pki/`, `data/`, or `logs/` — with no
  marker and a free lock, the marker-less-layout branch added in round two.

The third needs a companion negative: an empty root, and a root holding only
`local.lock`, must remain `Absent`. Without it the fixture cannot distinguish the
branch from a blanket refusal.

## 5. Polariton attach polarity and async render

`Message::LocalWorkspaceDiscovered` is dispatched at `gui/state.rs:290` and
handled at `:952`, with nothing asserting what the handler must not do.

- **Attach polarity** — drive the handler with every non-`Ready` variant and prove
  by counter that no client is constructed, no adapter stage advances to
  connected, and the Connect dialog does not open. A `Ready` positive control must
  increment the same counter, so the test cannot pass by being blind.
- **No endpoint fallthrough** — `Corrupt`, `Incompatible`, and `Unverifiable`
  never produce a connection attempt against the previously hardcoded endpoint.
  The architecture guard at `tests/architecture_test.rs:115` proves the constant is
  absent from source; this proves the behavior.
- **Async render** — the first frame is produced with discovery outstanding. Build
  the state, render before any `LocalWorkspaceDiscovered` arrives, and assert a
  frame is produced and no discovery result is awaited on the startup path.
- **Masquerade** — after a non-`Ready` discovery, the status surface names the
  observed state and SQL dispatch records `"scratch"` provenance rather than a
  workspace identity. `sql_connection_prefers_external_and_falls_back_to_scratch`
  (`adapters/driver.rs:354`) stays as-is: scratch remains available and this test
  covers the labeling that keeps it honest, which is the distinction the ADR
  exists to enforce.

## Guard amendment

The dependency assay at `lib.rs:979-989` scans the entire `Cargo.toml` text, so
any `fs2` entry trips it regardless of table. That is stricter than PRODUCT-0005
requires: the record forbids production dependencies, not test ones. This plan
does not need `fs2`, but the guard's imprecision would misfire on a future
legitimate dev-dependency and teach the next author to weaken it wholesale.

Make it table-aware: scan only the `[dependencies]` section, with two positive
controls — a synthetic production `fs2` entry must fire, and a synthetic
`[dev-dependencies]` `fs2` entry must not. The forbidden list and the failure
message citing PRODUCT-0005 are unchanged.

## Non-goals

- No production behavior, ordering, state, cause, or public API change.
- No new public item; both seams are private and unreachable from consumers.
- No change to the existing guards' scope beyond the table-awareness fix, and no
  relaxation of the forbidden dependency list.
- No new live-gated test; existing `#[ignore]` coverage is unchanged.
- No `ket local` work, and no answer to `Absent`.

## Review checklist

- [ ] Identity stability covers false `Ready`, false `Unhealthy`, double-change
      churn, and a no-mutation positive control, on both the blocking and async
      paths.
- [ ] `confirm_identity` is unit-tested across `Stable`, `Settled`, and `Changed`.
- [ ] The race assay has both barrier branches, with the observer holding the
      control's `flock`, and asserts the `report_existing` product consequence
      against the real supervisor acquisition path.
- [ ] `Stale::OwnerGone` and `Stale::PidRecycled` assert exact causes;
      `OwnerGone` guards its PID-reuse precondition.
- [ ] Three `Corrupt { ProfileRoot }` fixtures plus the `Absent` negatives.
- [ ] Polariton attach polarity has a `Ready` positive control; async render and
      masquerade labeling are proven.
- [ ] The dependency guard distinguishes `[dependencies]` from
      `[dev-dependencies]`, with a positive control for each.
- [ ] Both seams are private, trivially thin, and absent from public API surface.
- [ ] Full validation green on both workspaces; the live deployment is untouched.
