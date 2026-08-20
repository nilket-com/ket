# PRODUCT-0137 evidence — the Polariton half

Implementation of PRODUCT-0137 §7 against
`plans/product/impl/0137_the_durable_materialization_contract.md`, landing as
one commit under the pair convention (the ADR is `d759a488`).

Verification at close: `cargo test -p polariton --bin polariton` — **965
passed, 0 failed, 8 ignored** (twice, no flake); `--test architecture_test` —
**98 passed**; no new clippy findings in any changed file.

This closes the fifth arc's last artifact. The chain that produced it:
TORON-0569 (generation) → KET-0006 `boson` (canonical expression) →
WENDELON-0087 (certificate attachment) → TORON-0572 (materialize verb) →
TORON-0574 (receipt and protocol bump) → this record.

## Gate ledger

### Stage 1 — the terminal-relation route

- `rune_view_compiler.rs` recognizes a top-level
  `StaticProvenance::Relation { root: RelationRoot::Ket, .. }` as an
  execution carrying zero presentation slots, so a terminal `ket` relation
  reaches Toron instead of producing `NoTerminalView`.
- `state.rs`'s `NoTerminalView | Parse(_)` fallback is **untouched**, as the
  plan requires: teaching it to detect a `ket` shape would have needed the
  source inspection the record forbids. Recognition lives in the compiler, so
  the shape never reaches that arm.
- `workspace` roots keep today's behavior —
  `product_0137_terminal_workspace_relation_is_not_stolen_from_the_local_host`
  asserts `workspace.result(...)`, `workspace.cell(...)`, and
  `workspace.table(...)` all still refuse here and therefore still reach the
  local host.
- The structural guard
  (`product_0137_terminal_route_is_structural_and_names_no_relation_verb`)
  asserts two literal claims against the compiler's *production* source: the
  branch selects on `root: RelationRoot::Ket`, and `materialize` appears
  nowhere in it. Compiler unit coverage uses a generic `ket.table(...)`
  fixture so the guard cannot fail on its own test text.

### Stage 2 — the closed success kind

- `RuneDispatchSuccess::{Relation(Arc<ResultArtifact>), Materialized(..)}`
  replaces the bare artifact; `RuneDispatchFailure` is unchanged.
- `MaterializationReceipt` is built at the adapter boundary from
  `MaterializeOutcomeResponse`, so `toron_client` types do not leak past
  `adapters/`. Namespace travels beside the table name, not dotted into it.
- The Toron adapter matches **both** `RuneQueryResult` variants with no `_`
  arm, so a future variant fails the build rather than being silently
  rendered as one of these two. The `Batches` path keeps its prior
  nullability policy and error text verbatim.

### Stage 3 — the receipt reaches the host and renders

- `RuneRelationCompletion::{Views, Materialized}` is a closed enum on the
  completion message rather than a view set with an optional receipt — the
  same reasoning that produced TORON-0574's sum type, and the reason no
  consumer has a `None` to misread as "nothing happened."
- §7's routine status is emitted on the `status_msg` lane with `status_error`
  cleared: the resolved table **named beside** its namespace — `table
  "fixture" in namespace "public"`, never `public.fixture` — plus rows
  written and the certificate's recorded format version. The separation is
  the contract: `TableRef::new` does not split a qualified string, so a
  dotted render would report an identity that cannot round-trip into
  `ctx.table(...)`.
  `product_0137_materialization_renders_a_routine_receipt` asserts the
  separated form *and* rejects the dotted one, and asserts
  `status_df_size` stays `None` — a materialization never populates the
  grid.
- A terminal relation that returns **rows** refuses with terminal-view
  guidance rather than dropping them
  (`product_0137_terminal_relation_with_rows_refuses_rather_than_dropping`).
  Only `Materialized` is a complete success on that route.

## Deviations and judgment calls

1. **PRODUCT-0084's architecture guard was updated, with its claim intact.**
   It pinned `Result<Arc<ResultArtifact>, RuneDispatchFailure>` literally.
   The guard now asserts the new signature *and* that the relation half is
   `Relation(Arc<ResultArtifact>)` — so 0084's actual claim, that the
   artifact is the single truth boundary for rows, is still enforced in the
   shape 0137 gave it. The citation is in the test.
2. **A `#[cfg(test)]` `expect_relation` accessor** was added to
   `RuneDispatchSuccess`, and an `expect_views` helper beside the state
   tests. Production consumers match both variants explicitly; only tests
   that mean "I expect rows" unwrap, and they say so. Six live-Toron test
   sites use the former.
3. **The receipt test primes the scheduler** through
   `RuneViewScheduler::submit`, as production does, rather than reaching past
   it — a completion is only ever delivered for a program the scheduler
   started, and the assertion inside `complete` enforces that.
4. **`RuneRelationCompletion` carries no `Debug`.** The refusal test unwraps
   its error with a `match` instead of `expect_err`. Deliberate: the
   completion can carry a receipt, and receipts should not transit debug
   formatting by default.

## Guards verified at close

- `crates/libaryon`, `crates/polariton/src/rune_presentation.rs`, and
  `shared/boson` all have **empty diffs** — the presentation VM is untouched,
  and the structural compiler change is confined to `rune_view_compiler.rs`.
- `boson` is absent from Polariton's manifest; the certificate's format
  version is reported as recorded and never decoded host-side.
- No Rune source is parsed to detect a materialization: the only signal is
  the typed wire outcome.
- Existing view-set behavior is unchanged — the view-set tests pass
  untouched, with only their completion unwrapping adjusted.

## Left open, correctly

Certificate viewers, provenance panels, and materialized-table browsers
(§7's "no richer display in this cut"); a `workspace`-rooted terminal route;
materializing non-generated relations; and everything the Toron-side records
defer.
