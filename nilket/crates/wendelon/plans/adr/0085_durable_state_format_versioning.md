# Durable state format versioning

## Status

Proposed 2026-08-15. No implementation.

## Context

A local Nilket profile that last wrote on 2026-08-08 refuses to start under a
binary built from current `main`:

```
wendelon: bootstrap failed: snapshot artifact .../data/wendelon/snapshot.wnd
decode failed: snapshot postcard decode failed: Hit the end of buffer, expected
more data
```

**This is not corruption.** `decode_snapshot_reader` verifies the body digest
against `SnapshotHeader.state_sha256` *before* it decodes, and the failure comes
from the decode that follows. The body is byte-for-byte what the writer produced.

**The cause is a field.** `SmState` gained `ctas_sagas: CtasSagaCatalog` in
`aec63cdf` (ADR 0548, CTAS staged publication). The stored snapshot carries the
preceding seventeen fields; the current binary expects eighteen. Postcard is not
self-describing, so the decoder walks off the end of a perfectly intact payload
and reports exhaustion. The `#[serde(default)]` attributes on several `SmState`
fields do not rescue this — postcard has no way to signal that a trailing field
was absent, so the default never gets a chance to apply.

**Nothing gates the shape.** `SnapshotHeader` (`schema/core.rs`) is eighty fixed
bytes holding indexes, term, epoch, HLC, digest, and a checkpoint length, with
`_pad: [u8; 4]` left over and no format version. `WALRecordHeader` is the same
story — `record_type: u8` plus `_pad1: [u8; 7]` — and its log-entry body is
`postcard(Entry<WendelonTypeConfig>)`, so the write-ahead log carries the
identical exposure with a longer retention horizon than any snapshot.

**The existing compatibility mechanism does not cover this.**
`validate_protocol_window` lives in `network/wire.rs` and governs which peers may
speak to each other; `upgrade_compatibility`'s rolling-restart coverage exercises
that path. It says nothing about the shape of state already on disk, so a rolling
restart onto a binary with a wider state machine passes every protocol check and
then fails at bootstrap.

This surfaced while trying to verify ADR 0084 against a real profile. It is not
that record's defect: `SlabManager::open` runs before `WendelonLogStore::open`
during node bootstrap, and slab recovery had already completed when this fired.

## Decision

1. **Durable postcard-encoded state carries an explicit format version**, written
   into reserved header padding. `SnapshotHeader` spends one byte of `_pad` and
   `WALRecordHeader` one byte of `_pad1`. Both headers keep the fixed sizes their
   static assertions pin, so no reader's framing changes. Version zero means
   "written before this record", which is what every existing artifact already
   reports, since that padding is zeroed.

2. **An unrecognized version is refused with a diagnostic that names the format.**
   The error states the artifact path, the version found, the version range the
   binary accepts, and that the artifact is intact but shaped for a different
   build. An opaque decoder message is the defect this record exists to remove.

3. **Format mismatch and corruption are distinct errors.** A digest mismatch and a
   version mismatch get separate text, because they call for opposite operator
   responses: one is a damaged file, the other is a serviceable file against the
   wrong binary. Conflating them is what made this incident cost a code read.

4. **Adding a field to durable state is a version bump, and a test enforces it.**
   The control decodes a pinned prior-version artifact and asserts the refusal
   names the version. A test that only round-trips current-version state passes
   against this defect and is not the control.

5. **No migrations in this cut.** Making a superseded snapshot readable is
   separate work. The version gate is its precondition, and nothing here
   forecloses it.

## Rationale

The padding already exists and is already size-asserted, so the correct fix costs
no format change and no reader churn — the cheapest version gate available is the
one that fits in bytes nobody is using.

Refusal beats migration here because a migration is speculative until someone
needs a superseded profile readable, while the diagnostic gap is concrete and
already cost real time. What went wrong was not that the data was unreachable; it
was that an intact file and a damaged one produced indistinguishable errors, and
telling them apart required reading the decoder to learn that the digest check
runs first. Legibility is the whole return on this record.

The WAL is in scope because it shares the encoding and the absent gate, and it
outlives snapshots. Fixing only the artifact that happened to fail would leave the
larger one carrying the same defect.

## Consequences (The "Negatives")

Version-zero artifacts stay unreadable. This record makes the failure legible, not
recoverable; an operator holding a pre-versioning profile still resets it or keeps
the older binary. That is a real limit and it is chosen, not overlooked.

Each header's reserved padding shrinks by one byte and stops being free for
whatever else might have claimed it.

Every future field addition to durable state now needs a deliberate bump. That
cost is the point — the defect was that widening the state machine required no
thought at all, and the resulting breakage surfaced two builds later on a machine
nobody was watching.

## Open Questions

Whether other durably encoded payloads share the exposure — the persistent local
index rows, secondary-index segments, and sketch state are separately encoded and
were not examined here. Each should be checked before this record is called
complete, and any that qualify are in scope for the same gate.

Whether any non-development profile predates a durable-state field addition. That
answer decides whether a version-zero migration is ever worth building, and it is
a question about deployments rather than about code.
