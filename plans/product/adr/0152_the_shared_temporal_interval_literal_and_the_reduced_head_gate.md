# ADR 0152: the shared temporal interval literal and the reduced head gate

Status: accepted 2026-09-03, proposed and revised the same day after
implementation found two more ungated test surfaces and a host-toolchain
red in the cross-target loop, and after review found that a check-only gate
cannot see test-code rot. Polariton's reduced head — the build with the
`toron` feature off, which `docs/ket.md` names as the deliberate
non-dogfood configuration and PRODUCT-0063 gate 16 promised would keep
checking and testing — does not compile. This record moves the one type the
plot kit borrowed from the Toron client into the shared literal crate that
PRODUCT-0129 already reserved for it, closes the feature-gated import
leak, and makes the reduced head a gated configuration so it cannot rot
silently again. It changes no default-feature behaviour, no Rune spelling,
no plot pixel, and no saved-session byte.

## Context

### The reduced head is fiction today

`cargo check -p polariton --no-default-features` fails with twenty-three errors.
The count was sixteen before PRODUCT-0149 through PRODUCT-0151 landed on
2026-09-02: the two original defects are unchanged, and a third arrived
with the PRODUCT-0151 implementation, which is the rot this record exists
to stop. They collapse into three:

1. `crates/polariton/src/plot/common.rs:25` imports
   `toron_client::TemporalInterval` unconditionally while `toron-client` is
   an optional dependency behind the `toron` feature. The missing type
   cascades into nine pattern and dereference errors in the temporal tick
   selector directly below it. The same unconditional import appears in
   `rune_presentation.rs:41` and `gui/state.rs:72`. Ten errors.
2. `crates/polariton/src/gui/state.rs:67` gates `use std::sync::Arc` behind
   the `toron` feature while eleven uses of `Arc` in the same file are
   unconditional. Eleven errors.
3. `crates/polariton/src/editor.rs:451` and `:1426` — the selection-context
   input-identity map and `bind_rune_artifact`, added by PRODUCT-0151 —
   name `rune_relation_envelope::RelationLineage` unconditionally while
   `main.rs:18` gates that module behind the `toron` feature. Two errors.

Nothing in the local gate builds the reduced head. `ci/check.sh` runs the
workspace with default features and cross-checks three Windows and macOS
targets, also with default features. PRODUCT-0063 gate 16 recorded that the
reduced suite passed on 2026-08-09; no gate has asked since.

Implementation found the reduced test target broken further back than the
binary. Eleven Rune-view tests in `gui/state.rs`, from PRODUCT-0082's box
surface through PRODUCT-0148's candlestick surface, call `rune_view_outcome`
and its companions, which are themselves gated behind the `toron` feature,
without carrying the gate — forty-nine errors in the reduced test build.
Once that target compiled, the PRODUCT-0073 discovery test drove the binary
through `--session list`, which the reduced head refuses by PRODUCT-0063's
own gate 16, so that test target lacked its `required-features` gate too.
They are the same defect kind as the `Arc` import and the lineage pair, and
decision 3 closes them the same way.

Implementation also found that the cross-target loop is red on this host
in both heads for a reason outside this record: `aws-lc-sys`, reached
through Polars' object-store client, cannot be cross-compiled by the host C
compiler. That is release-artifact evidence, recorded here and owned there.

### Why the reduced head matters now

The local Toron workspace is Linux-only: Wendelon depends on `io-uring`, and
Polariton's `toron` feature pulls in `nix`. Any Polariton that ships on macOS
or Windows is the reduced head, so a broken reduced head blocks every
cross-platform release before packaging, signing, or a storefront can be
discussed. The plot kit's dependence on the Toron client is also the first
leak that a later, iced-window-free plot crate would have to remove; this
record removes it now, where it is cheapest.

### The type was placed deliberately, and the placement reserved this exit

`TemporalInterval` is a std-only enum with two variants, `FixedNanos(i64)`
and `CalendarMonths(i32)`, plus its parser and error
(`nilket/crates/toron-client/src/temporal_interval.rs`, 102 lines). It is
one spelling contract with two consumers: Toron's SQL planner lowers
interval literals through it, and Polariton's `tick_every` surface
(PRODUCT-0130) parses the same grammar. PRODUCT-0129 put the carrier in
`toron-client` so both sides share one grammar, and wrote as its first stop
condition: if the carrier cannot live in `toron-client`, "create a dedicated
shared literal crate."

That crate already exists. `shared/boson` is the dependency-free
interchange crate PRODUCT-0001 §4 sanctions for "a genuinely jointly-owned
abstraction," Toron already depends on it, the product dependency guard
permits Polariton to depend on it, and its own boundary forbids Rune,
Arrow, Polars, engine, and editor dependencies — so the literal cannot
acquire weight there.

## Decision

### 1. The temporal interval literal moves to `boson`

`TemporalInterval`, `TemporalIntervalError`, `parse`, and
`from_amount_and_unit` move verbatim from `toron-client` to `shared/boson`.
Their grammar, variants, and error text do not change. `toron-client`
re-exports both types from `boson` at the same paths, so Toron's planner and
every existing `toron_client::TemporalInterval` import outside Polariton
compile unchanged.

### 2. Polariton imports the literal from `boson`, never from the client

`boson` becomes a non-optional Polariton dependency, and `toron-client`
gains `boson` as a dependency for the re-export; both workspace lockfiles
change, and no cycle results because `boson` stays dependency-free. The
three unconditional
`toron_client::TemporalInterval` imports become `boson::TemporalInterval`.
No plot, presentation, registry, or settings code may name `toron_client`
outside the `toron` feature after this record; the architecture test below
enforces it.

### 3. Feature closure is restored where it broke

`gui/state.rs` imports `Arc` without a feature gate. The PRODUCT-0151
lineage pair is closed the way the crate already closes gated state:
`#[cfg(feature = "toron")]` on the `rune_input_identities` field, its
initializer, `bind_rune_artifact`, and the test that exercises them,
matching the pattern at `editor.rs:1090`. `RelationLineage` is not lifted
out of its module; it is Rune-envelope data and belongs where it is. Any
other gap of these two kinds that the reduced build reveals is corrected
the same way, in this cut, without widening scope.

### 4. The reduced head joins the local gate

`ci/check.sh` runs `cargo check --locked -p polariton --no-default-features`
beside its existing steps, and the same reduced check is added to the
Windows and macOS cross-target loop, since the reduced head is the only head
those targets can ship. The reduced unit tests join the gate too, as
`cargo test --locked -p polariton --no-default-features --bin polariton`,
because a check compiles no `#[cfg(test)]` code and the rot this record
found was in test code. The `--bin` scope mirrors what `scripts/cargo-ctrl`
already runs for the workspace, `--workspace --lib`, which — Polariton being
a binary crate — runs none of Polariton's tests on the default head at all.
That default-head gap and the absence of integration targets from the gate
on both heads are pre-existing and named here, not owned here.

### 5. What this record does not do

It does not decide macOS or Windows viability — a cross-target `cargo
check` is not a build, a link, or a package, and that evidence belongs to
the release-artifact record. It does not extract a plot crate, does not
touch the temporal axis authority, the tick selector, or the `tick_every`
surface, and does not add or remove any Rune member.

## Acceptance gates

1. **Both heads compile.** `cargo check -p polariton` and `cargo check -p
   polariton --no-default-features` both succeed natively. The three
   cross-target checks are run in both configurations and their outcome is
   reported with a control: if the reduced head fails a target, the default
   head must fail that target identically, otherwise the failure is this
   record's to own.
2. **Both suites pass.** The default and reduced Polariton test suites pass
   under `--no-fail-fast`, with one named exception under a control: the
   `result_artifact_ui` trybuild target fails identically in both heads
   because its path-included fixture cannot resolve the selection import
   PRODUCT-0151 added, and this record touches neither the fixture nor
   either module. That target is PRODUCT-0151's to repair. The reduced
   suite still refuses `--session` as PRODUCT-0063 requires, exercised by
   the binary's own `cfg(not(feature = "toron"))` unit test.
3. **Toron is untouched.** `nilket/crates/toron` compiles with no source
   change; the SQL planner's `LoweredIntervalLiteral` alias still resolves
   through `toron_client`.
4. **Grammar parity.** `boson`'s parser tests are the moved tests, byte for
   byte, and a Polariton test parses the PRODUCT-0130 exhibit intervals to
   the same variants as before.
5. **The leak cannot return.** A Polariton architecture test is mechanical
   and never infers `cfg` scope from text: it fails on any `toron_client`
   mention at all under `src/plot` and in `rune_presentation.rs`,
   `presentation_registry.rs`, and `settings_writeback.rs`, which today name
   the client only for the temporal literal; and it fails on
   `toron_client::TemporalInterval` in `gui/state.rs`, whose one other
   client use is a legitimately gated Toron operation. Whether remaining
   gated uses are gated correctly is the reduced compilation's proof, not
   the test's.
6. **The gate can fail.** Reintroducing the unconditional import on a
   scratch branch makes `ci/check.sh` fail at the reduced step.
7. **No pixel, spelling, or byte moves.** The SVG oracle corpus, the Rune
   spelling tables, and the saved-session schema are unchanged.

## Guardrails and stop conditions

1. If the literal needs anything beyond `std` to move, stop: `boson`'s
   boundary is the point, not an obstacle.
2. If a reduced-build error is not one of the three named defects, a
   wrongly gated import, or an ungated use of a gated module, stop and
   report it; it is a separate defect with its own owner.
3. If the reduced test suite fails for a reason other than a missing
   feature gate, stop and report rather than gating the test.
4. Two commits: this record, then one `impl:` commit carrying every source,
   manifest, test, and gate change together.

## Risks

- **Silent grammar fork.** Mitigated by moving the parser rather than
  copying it and by re-exporting from the client, so there is one
  implementation with two paths to it.
- **Cross-target checks may fail for reasons this record does not own.**
  They run today with default features only; the reduced configuration may
  surface a platform-specific dependency. That is evidence for the
  release-artifact record, not scope for this one, and is reported as such.

## Forward

This is the prerequisite cut of the web and cross-platform programme. Its
successors are a published-dashboard package record, a release-artifact
contract record whose acceptance needs real macOS and Windows builds of
the reduced head, and, later, a plot-crate extraction for which this
record's guard is the first boundary. None of them is owned here.
