# ADR 0087 - The materialization certificate attachment

**Status:** Proposed 2026-08-20. Rules on the three questions Toron's
PRODUCT-0137 companion framing posed
(`plans/wip/toron_materialization_certificate_framing.md`); the framing's
constraints and its rejected descriptor-field shortcut are incorporated by
reference, and Toron's fresh-only decision receives its concurrence in §4.

## Context

A table materialized by Toron's materialize verb must carry an immutable
birth certificate — `boson`'s `CanonicalRelationExpression` v1 bytes plus
scalar coordinates — for as long as the table exists, admitted in the same
act that makes the name resolvable (PRODUCT-0137 §2), never edited (§3),
and living in neither the schema descriptor (generation-advancing, the
opposite lifetime) nor the CTAS saga alone (`prune_terminal_detail` ages
terminal entries; the saga is the certificate's carrier, not its home).

The grounding fact this ruling leans on: `CtasSagaMutation::Publish` is
applied as **one replicated state-machine transition** — snapshot
resolution, epoch/phase/binding validation, `publish_provisional`, and the
phase flip happen inside a single raft apply. Atomicity here is not a lock
discipline to be careful about; it is the apply boundary itself.

## Decision

### 1. Residence: a dedicated immutable per-object attachment catalog

The certificate lives in a new consensus-state catalog keyed by
`(TenantId, TableObjectId)`, holding exactly one immutable record per
materialized object. It is created **only** inside the `Publish` apply,
read by object id (provenance inspection is the rare path; lifetime
correctness is the design axis), and removed **only** in the apply that
finalizes the object's destruction through the existing drop
prepare/finalize fences — the attachment must never survive its object,
and the destruction test asserts the pair leaves together.

This introduces the per-object immutable attachment as a concept scoped to
**this one kind**. It is deliberately not a general metadata slot: a
second attachment kind is its own admission decision, exactly as the range
node's facts fence kept one node's construction facts from becoming a
statistics system.

### 2. Bound and refusal: a named constant, refused at Provision

`MATERIALIZATION_CERTIFICATE_MAX_BYTES = 64 * 1024`. Boson v1 certificates
measure tens to a few hundred bytes; 64 KiB is generous headroom for the
scalar coordinates while staying far inside `MULTI_ROW_MAX_RECORD_BYTES`'s
8 MiB write discipline. An oversized certificate refuses the
materialization **whole, at `Provision`**, via a new
`CtasSagaReject::CertificateTooLarge` — before `row_write_id` is
dispatched, so no completed write is wasted and no partially-certified
state can exist. Refusal-whole is the only answer compatible with 0137
§2's totality.

### 3. Atomic admission: ride the entry, transfer inside the Publish apply

The certificate enters `CtasSagaEntry` as a new `#[serde(default)]`
optional field on `Provision` — the ADR 0085 durable-format discipline:
existing durable entries decode as `None`, and CTAS's own verb never sets
it. The materialize verb requires `Some`. The `Publish` apply reads the
certificate from the resolved snapshot and writes the attachment catalog
record **in the same apply** that calls `publish_provisional` — table and
certificate become visible in one state transition or neither does. Every
existing `Publish` reject (`StaleOwner`, `RowsNotCommitted`,
`BindingChanged`, `ForeignProvisionalObject`) leaves nothing visible,
unchanged. Recovery needs no carve-out: a crash between `RowsCommitted`
and `Publish` resumes by re-issuing the same mutation against the
still-live entry.

### 4. Concurrence with Toron's fresh-only decision

Materialize provisions `CtasExpectedBinding::Absent` only and forces
`if_not_exists: false`; `RetiringOld` is unreachable from the verb and a
certificate never needs retirement alongside a replaced object. No
saga-surface change is required for it. Concurred.

## Acceptance

- The attachment catalog admits exactly one record per object, created in
  the `Publish` apply; a second admission for the same object is rejected
  — immutability is structural, not policed.
- Fault injection per phase edge over the existing saga recovery fixtures,
  with `RowsCommitted → Publish` as the load-bearing case: in no
  interleaving is a table name-resolvable without its attachment, and the
  resumed publish binds both together.
- An oversized certificate refuses at `Provision` with
  `CertificateTooLarge`; nothing is dispatched, nothing is visible.
- A pre-0087 durable `CtasSagaEntry` decodes with `None` and CTAS's own
  verb publishes exactly as before — the format gate per ADR 0085.
- Dropping a materialized table removes the attachment in the destruction
  apply; no orphaned attachment survives, and no attachment disappears
  while its object lives.

## Guardrails

The attachment record is written once and never mutated: the catalog
exposes no update path, so "immutable" is the absence of an API rather
than a convention. The one-kind fence holds until a second kind arrives
with its own record.

## Forward

Toron's companion ADR proceeds against this ruling: the materialize verb,
direct `boson` consumption, and the fault-injection proof named above.
