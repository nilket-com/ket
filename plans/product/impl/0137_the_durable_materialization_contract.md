# Implement the durable materialization contract — Polariton half

Scope: implement PRODUCT-0137 §7, the host receipt, now that both stated
prerequisites exist (KET-0006 `boson` at `2ac6d5e3`; the Toron companion as
WENDELON-0087, TORON-0572, and TORON-0574 with its client fix). Polariton
gains a closed success kind for Rune dispatch that carries a materialization
receipt, maps `toron_client::RuneQueryResult` in the Toron adapter, opens the
narrow terminal-relation route without which a materialize cannot reach Toron
at all, and emits the §7 routine status. It adds no relation verb, no
certificate display, and no host surface beyond the route and the receipt's
own type.

The work lands as **one implementation commit** — the pair convention, with
PRODUCT-0137's ADR already committed at `d759a488`:

```text
impl: adr 0137 polariton materialization receipt
```

Evidence lands in `0137_the_durable_materialization_contract_evidence.md`
within the same commit. The stages below are work order and gates.

**The build is currently broken by design.** The db team landed TORON-0574's
client fix and deliberately left `crates/polariton/src/adapters/toron/mod.rs`
failing at `result.batches` (`dispatch_rune`, line 145: `no field 'batches'
on type RuneQueryResult`), rather than mapping a materialization to an empty
relation. Their reasoning is recorded and correct: an empty relation reads as
"nothing happened," and the natural retry collides with the fresh-only table
the first run created. Restoring the build is this commit's floor, not its
purpose.

## Halt rule

PRODUCT-0137 §7 permits a routine `status_msg` naming the resolved table and
confirming the certificate was recorded, carried by a typed receipt. Stop and
return to PRODUCT-0137 or a new record if implementation requires:

- parsing Rune source to detect a materialization (PRODUCT-0003 fences
  Polariton from relation verbs — the receipt is producer-authored or it does
  not exist);
- a certificate viewer, provenance panel, or materialized-table browser
  (§7's "no richer display in this cut");
- decoding the certificate bytes host-side — Polariton renders
  `certificate_format_version` as reported and does not read `boson`;
- rendering a materialization as an empty relation, for the reason above;
- any change to `crates/libaryon` or to the certificate/verb contracts of the
  Toron-side records.

**The presentation compiler is not fenced, but the allowance is narrow.**
PRODUCT-0137's Consequences already anticipate "whatever thin surface the
companion's verb needs reflected host-side," and Stage 0's routing finding
proves a terminal-relation route is that surface. The compiler may gain
**structural recognition of a `ket`-rooted terminal relation only** — such an
expression with no view root is routed to Toron rather than refused, while
every other root keeps today's behavior. It may **not**
learn `materialize`, or any relation verb, as a word: recognizing a verb
would be the PRODUCT-0003 violation this record exists to avoid. If the route
cannot be expressed without naming a verb, halt.

### Where work may land

- `crates/polariton/src/adapters/common.rs` — the success kind for
  `dispatch_rune` and the host receipt type;
- `crates/polariton/src/adapters/toron/mod.rs` — mapping
  `RuneQueryResult::{Batches, Materialized}` at the one break point;
- `crates/polariton/src/gui/state.rs` — the relation-dispatch outcome, its
  message payload, and the §7 status emission; and
- `crates/polariton/src/gui/messages.rs` — the closed completion outcome; and
- `crates/polariton/src/rune_view_compiler.rs` and
  `rune_relation_envelope.rs` — the `ket`-rooted terminal-relation route
  only, under the halt rule's narrow allowance. `rune_presentation.rs` is
  not touched.

## Stage 0 — census (established during planning)

- **`dispatch_rune` has exactly one production caller**:
  `dispatch_rune_relation_with_palettes` (`state.rs:2089`), reached from the
  plain Rune buffer path through the presentation scheduler and completing as
  `Message::RunRuneViewSetResult`.
- **Materialize is unreachable from notebook Rune cells.**
  `run_rune_markdown_cell` executes on the local `rune_host`, which exposes
  `workspace.result`/`workspace.cell` and no `ctx` — the range/materialize
  vocabulary is Toron's server-side engine API. **`run_plain_rune_code` is not
  reduced-head only** — the function carries no `cfg`, and only its direct
  call from `run_rune_code` is `#[cfg(not(feature = "toron"))]`. In the Toron
  build it is still reached, through the `NoTerminalView | Parse(_)` fallback
  at `state.rs:3668`. That distinction is what makes the routing gap below
  real rather than theoretical. **The receipt therefore lands on the relation
  dispatch path, not the notebook path**, and PRODUCT-0136's notebook "no product" refusal is a
  different rule on a different path that this commit does not touch.
- `RuneDispatchResult` is
  `Result<Arc<ResultArtifact>, RuneDispatchFailure>` — a success type with no
  case that can carry a receipt.
- `rune_view_outcome_with_palettes` (`state.rs:2175`) immediately calls
  `result.projection()`, so a receipt must be handled **before** it, never by
  synthesizing an artifact to feed it.

- **Routing gap, closed during review — a materialize cannot reach Toron
  today.** A buffer with a relation but no view root fails
  `transform` with `RuneEnvelopeError::NoTerminalView`
  (`rune_relation_envelope.rs:885`), and `state.rs:3668` routes that error —
  *inside the toron build* — to `run_plain_rune_code`, the local host. That
  host has no `range` and no `materialize`, so the script fails there. No
  view-less `TransformedRuneRelation` exists, and `ctx.`-spelled source is
  unrecognized earlier still, Polariton's vocabulary being `workspace.`.
  This is a routing contract gap, not a census unknown: **Stage 1 must open
  the route or nothing downstream can ever execute.**

## Stage 1 — the terminal-relation route

- The presentation compiler gains a **terminal-relation route rooted at
  `ket`**: a top-level `RelationRoot::Ket` expression with no view root
  becomes a transformed relation carrying **zero presentation slots**,
  dispatched to Toron unchanged — recognized in the compiler, so it never
  reaches the `NoTerminalView` path at all. Recognition is structural — "this is a
  `ket` relation" — and never lexical: no verb name appears in the compiler.
- **`workspace` terminal behavior is unchanged.** A generic
  any-root rule would steal plain local-Rune programs —
  `workspace.result("sales")`, `workspace.cell("trips")` — which today fall
  through `NoTerminalView` to the local host **by design**, and which have no
  Toron meaning. `ket` is the correct root: it already exists as
  `RelationRoot::Ket` with its own entrypoint prefix
  (`rune_view_compiler.rs:420, 1049`), is shadow-aware (`:804`), and has no
  local-host meaning to displace.
- **`state.rs:3668` is not changed.** Teaching the fallback to detect a
  `ket` shape would require exactly the source inspection this plan forbids.
  Instead the compiler recognizes a terminal `ket` relation and returns a
  transformed relation, so that shape **never produces `NoTerminalView`**.
  The existing fallback keeps its behavior verbatim for every genuine
  `NoTerminalView` and every `Parse(_)`.
- **The non-materialize case must refuse, not silently drop.** A terminal
  relation that returns `Batches` — rows, no views — is a script that
  computed something and asked for nothing. It refuses with the existing
  "add a terminal view" guidance rather than discarding the rows quietly.
  Only a `Materialized` outcome is a complete success on this route.

### Gates

- A relation-only buffer reaches `dispatch_rune` instead of the local host.
- **The terminal-root branch is structural.** "Contains no relation-verb
  vocabulary" cannot be asserted literally — the compiler already contains
  `table` and other relation words. The gate is therefore two specific
  claims: the new branch consumes
  `StaticProvenance::Relation { root: RelationRoot::Ket, .. }`
  (`rune_view_compiler.rs:129`) and inspects no method or verb name in
  reaching its decision; and `materialize` appears nowhere in the compiler's
  implementation. The guard asserts against the implementation, not its own
  fixtures — a textual check whose test file contains the spelling it
  excludes fails on itself. Compiler unit coverage therefore uses a generic
  `ket.table(...)` terminal relation, and the end-to-end materialize fixture
  lives in the adapter and state tests.
- `workspace.result(...)` and `workspace.cell(...)` still reach the local
  host unchanged — asserted, since that is the behavior the narrowed root
  exists to preserve.
- A relation-only buffer returning rows refuses with the terminal-view
  guidance; nothing is dropped.
- Every existing view-set shape routes exactly as before, and `Parse(_)`
  still falls back to the local host.

## Stage 2 — the closed success kind

- Replace `RuneDispatchResult`'s success with a closed enum: a relation
  artifact, or a materialization receipt. The failure half
  (`RuneDispatchFailure`) is unchanged.
- Add the host receipt type carrying the four reported facts — resolved
  namespace, resolved table name, certificate format version, rows written.
  It is built from `MaterializeOutcomeResponse` at the adapter boundary so
  the wire type does not leak past `adapters/`.
- Map both `RuneQueryResult` variants in the Toron adapter's `dispatch_rune`.
  `Batches` keeps today's `from_arrow_ipc` conversion exactly; `Materialized`
  becomes the receipt. This restores the build.

### Gates

- The adapter maps both variants; no `_ =>` arm hides a future one.
- The artifact path is byte-identical in behavior to pre-break: same
  nullability policy, same conversion error text.

## Stage 3 — the receipt reaches the host and renders

- The relation-dispatch outcome becomes a **closed enum** —
  `Views(RuneRelationViewSetOutcome) | Materialized(receipt)` — rather than an
  optional receipt field on the existing outcome. Same reasoning that
  motivated TORON-0574's sum type: an outcome that can carry both is an
  outcome that can carry neither honestly, and the fake-empty-artifact trap
  reappears wherever a `None` has to be interpreted. A receipt never passes
  through `rune_view_outcome_with_palettes`.
- Emit PRODUCT-0137 §7's status in the routine `status_msg` lane, naming the
  resolved table and confirming the certificate was recorded, and reporting
  rows written. `status_error` stays empty on success; PRODUCT-0136's
  `ExportReceipt` rendering is the established shape to follow.
- A receipt-only success is a **complete** success: no view panels are
  created, no grid is populated, and nothing reports as empty.

### Gates

- A materialize receipt renders the resolved identity, the format version,
  and rows written in `status_msg`, with `status_error` empty.
- The status names the table such that the reported identity is the one
  `ctx.table(...)` accepts — namespace carried beside the name, not dotted.
- A relation view set with views is unaffected: existing view-set tests pass
  unchanged.
- A materialization never populates the result grid and never reports an
  empty relation.

## Cross-stage guards

- **No Rune parsing anywhere.** The only signal is the typed wire outcome.
- **No certificate decoding.** The format version is rendered as reported;
  `boson` is not a Polariton dependency and must not become one.
- **The existing relation path is untouched in behavior** — every stage
  re-runs the full suite, and the view-set tests are the control.
- **`crates/libaryon`, `rune_presentation.rs`, and `shared/boson` have empty
  diffs.** The presentation *VM* is untouched; the structural compiler change
  of Stage 1 is in scope and deliberately excluded from this guard.

## Evidence and review checklist

- [ ] Census: one `dispatch_rune` caller; materialize unreachable from
      notebooks; the routing gap and its closure recorded.
- [ ] A relation-only buffer reaches Toron; the compiler names no verb.
- [ ] A relation-only buffer returning rows refuses rather than dropping.
- [ ] Both `RuneQueryResult` variants mapped; build restored.
- [ ] The GUI completion outcome is a closed enum, not an optional field.
- [ ] Receipt renders resolved identity, format version, rows written, in the
      routine lane.
- [ ] No grid population and no empty-relation report on materialization.
- [ ] Existing view-set and notebook paths unchanged.
- [ ] `boson` absent from Polariton's manifest; libaryon,
      `rune_presentation.rs`, and `shared/boson` diff-empty.
- [ ] `workspace`-rooted local-Rune programs still reach the local host.
- [ ] Full suite and architecture tests green; counts reported passed and
      ignored separately; the known 0063 flake noted if seen.
