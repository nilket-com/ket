# ADR 574 - The materialization receipt and its protocol bump

**Status:** Proposed 2026-08-20. Settles the Toron half of the receipt the
viz-team council converged on (`plans/wip/claude_iter_0..2_receipt.md`),
against ADR 0572's `materialize` verb and Wendelon ADR 0087's attachment.

## Context

ADR 0572 gave `materialize(name)` a write path but no outcome: a successful
materialization returns the CTAS shape, which is empty batches, so the caller
learns nothing beyond the absence of an error. PRODUCT-0137 §7 asks for
routine status confirming the table and its certificate.

The council settled the payload. What it could not settle, and deliberately
handed back, is Toron's: the response variant is a Toron wire surface, and
carrying it requires a version decision that only this ring can make.

The grounded mechanics, because the obvious reading of them is wrong:

- `SqlMessage` is append-only by convention, and the comment in
  `toron-client/src/protocol.rs` says why — postcard encodes discriminants by
  declaration order, so appending keeps existing variants' numbers stable.
  That is **encoding** stability. It is not peer compatibility: a build that
  does not know a discriminant fails to decode it.
- `SqlProtocolVersion` has exactly one variant, `Current`. The exact-match
  checks in `wire.rs` and `toron-client/src/client.rs` are real code but
  cannot currently discriminate anything, because there is only one value to
  hold. The frame version is not what carries version identity today.
- What does carry it is the Rune request: `ExecuteRuneRequest` sends three
  `u16` axes — request-protocol, language-surface, sandbox-contract — and
  `rune_version_skew` (`daemon.rs`) refuses on exact inequality of any one,
  naming the axis, the requested value, and the supported set.

So an appended success variant, shipped without a version move, would reach
an older peer as a postcard decode error on an unknown discriminant: the
least legible failure available.

## Decision

### 1. The request-protocol axis moves, and that is what keeps skew legible

`RuneRequestProtocolVersion::CURRENT` increments. The language-surface and
sandbox-contract axes do not: a response-shape change alters neither the Rune
language nor the sandbox contract, and moving them would misreport what
changed.

The bump is not bookkeeping — it is the mechanism. Because
`rune_version_skew` runs on the **request**, a peer mismatch is refused
before any new-shaped response is written, in both directions: an old client
against a new server, and a new client against an old server, both fail on
the `RequestProtocol` axis with the requested and supported values named.
Without the bump, the same mismatch surfaces as a decode error with no axis,
no versions, and nothing to act on.

This is a **lockstep bump**, not an append-only extension. Toron and
Polariton ship from one repository and `nilket local` supervises matched
processes, which is what makes lockstep acceptable here; the record says so
plainly rather than implying a compatibility window this protocol does not
have.

### 2. Skew is emitted by Toron and mapped by Polariton

Toron owns producing `RuneFailureKind::VersionSkew(RuneVersionSkew { axis,
requested, supported })` at its boundary. Polariton's adapter maps it onto
its own `RuneDispatchFailureKind::VersionSkew`
(`crates/polariton/src/adapters/`), which is that ring's surface and not this
record's to specify. Both types exist; this record cites the `toron-client`
one because that is the layer Toron owns.

No new failure kind is introduced. The bump makes an existing, already-mapped
failure reachable for a new reason.

### 3. The receipt is derived from the publish result, not from a later read

The payload is `{ table_name (resolved), certificate_format_version,
rows_written }`.

- **`table_name`** is the resolved identity — the published entry's
  `descriptor.namespace` and `descriptor.table_name`, not the bare string
  `materialize(name)` lowered through `TableRef::new`. The receipt names
  something the user can paste back into `ctx.table(...)`.
- **`certificate_format_version`** is read from the **attached bytes**, not
  from this build's `boson::V1_FORMAT_VERSION`. Reporting what the build
  would have written rather than what was recorded turns evidence back into
  assumption.
- **`rows_written`** is named for what it is. Every other `Complete {
  row_count }` on this protocol means rows delivered to the caller; a CTAS
  execution returns empty batches, so a field named `row_count` here would
  invite exactly the wrong reading.

All three facts must be taken from the `Publish` response, in the same
operation that publishes. This is a correctness requirement, not a
preference: `CtasSagaCatalog::prune_terminal_detail` does not trim detail
from a terminal entry, it removes the entry whole, taking certificate bytes,
row count, and descriptor with it. A receipt that re-reads the saga to
assemble itself works in every normal-flow test and finds nothing on an aged,
resumed saga — which is precisely the path
`finish_ctas_after_rows_committed` runs after a coordinator restart.

### 4. The receipt is a typed result, not an optional collector

`execute_query_with_context` returns `Vec<RecordBatch>`, and the receipt's
facts exist only inside the coordinator's CTAS path, so something must carry
them out. `ExecutionContext::task_telemetry` is the house pattern for
execution-produced facts — an optional shared collector the caller installs —
and it is **rejected here**.

A collector would be an `Option` that is structurally always populated on the
materialize path, so an unfilled one could only mean a broken invariant.
That is the same defect the council rejected in `certificate_recorded: bool`,
one layer down: a field whose empty state cannot occur without something
being wrong should not be representable. Telemetry may be absent by design;
a result may not.

The receipt is therefore returned as a typed value on the path that produces
it, so a materialize execution that published cannot fail to carry one.

### 5. Two things stated so they are decisions, not omissions

**The attachment invariant is conditional.** Wendelon 0087 does not
guarantee that a successful `Publish` attaches a certificate — plain SQL CTAS
publishes through the same apply with none, by design. It guarantees that *a
`Publish` whose saga entry carried a certificate attaches it inside that same
apply, or the publish does not happen*. The materialize verb always
provisions with a certificate, so the receipt's existence does imply
attachment; stated unconditionally, a later reader would conclude plain CTAS
violates an invariant it was never subject to.

**Seed witnesses are omitted.** `seed_witnesses` exists on the producer side
and marks its list non-exhaustive when a seed is computed rather than
literal, so it could be carried. It is not, in this cut: §7 asks for routine
status and no more, and witness provenance belongs to the viewer §7 defers.

## Acceptance

- A `materialize` that publishes returns a receipt whose `table_name` round-
  trips: feeding it back to `ctx.table(...)` resolves the created table.
- `certificate_format_version` equals the version decoded from the bytes the
  attachment catalog holds — proven by reading the attachment, not by
  asserting the build constant against itself.
- `rows_written` equals the rows the source produced, on a case where that
  count is neither zero nor the outer cardinality, so a wrong-source
  substitution fails.
- The resume control: a materialize whose publish is re-entered through
  `finish_ctas_after_rows_committed` still produces a receipt with the same
  three facts, on a saga aged past `CTAS_SAGA_DETAIL_RETENTION_MS`. This
  fails against any implementation that re-reads the saga entry.
- A peer sending a stale `request_protocol_version` is refused with
  `VersionSkew` naming the `RequestProtocol` axis, the requested value, and
  the supported set — asserted in both directions, and asserted to occur
  before any response body is written.
- The other two Rune axes are unchanged, asserted by value, so a future
  reflex bump of all three fails here.
- Plain SQL CTAS is unchanged: no receipt, no new response variant on its
  path, existing suites untouched.

## Guardrails

A guard pins that the new `SqlMessage` variant is appended at the tail, that
no existing variant's declaration position moved, and that
`RuneRequestProtocolVersion::CURRENT` advanced in the same cut that added it
— a variant added without the bump is the exact failure this record exists to
prevent, so the two facts are asserted together rather than separately.

## Risks and deferrals

Mixed-version operation is not supported and is not made to work here; the
lockstep constraint is stated, not engineered around. A richer receipt, seed
witnesses, and any provenance surface remain with the viewer PRODUCT-0137 §7
defers. Polariton's rendering of the receipt is that ring's decision.

## Forward

If a second response-shape change follows, the question this record leaves
open is whether the request-protocol axis should carry a supported *range*
rather than a single `CURRENT`, which is what would turn lockstep into a
window. That is a protocol decision with its own record, and nothing here
forecloses it.
