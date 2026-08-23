# ADR 0092 - Consensus body compatibility: versioned WAL and snapshot replay

## Context

ADR 0085 gave the two consensus artifacts a format version: a byte in
`WALRecordHeader` and one in `SnapshotHeader`, version zero meaning "written
before versioning", anything newer than the build refused by name. It
deliberately stopped there — no migrations — because at that moment no
superseded shape existed and refusal was the honest contract.

A superseded shape exists now. ADR 0087 appended `certificate:
Option<Vec<u8>>` to the CTAS `Provision` mutation (a WAL body, via
`WendelonRequest::CtasSaga`) and to `CtasSagaEntry` plus the attachment catalog
in `SmState` (the snapshot body), bumping both formats 1 → 2. The record of
what every version means is now:

- **v0**: pre-0085, shape unknowable from the artifact alone.
- **v1**: 0085's shape — no `Provision` certificate, no attachment catalog.
- **v2**: current. ADRs 0088–0091 changed **no** consensus body shape: 0090's
  partition carriers live in the redb index (length-gated V5) and Arrow frames
  (self-describing, presence-based), its consensus additions
  (`PartitionTransfer`, `partition_span`, `partition_transfer_for`) are derived
  values that are never serialized, and rows never enter the raft log at all.
  The `SchemaUpdate` variant's inner payload is separately versioned
  (`decode_schema_update_envelope_v1`) and unchanged. v2 is therefore
  unambiguous — verified by a serde-reachability census of the 0088–0091
  diffs, not assumed.

**What replay actually does today, measured.** A faithful v1 `Provision` body
(the current encoding minus its trailing `None` discriminant, restamped v1,
hashes rebuilt) driven through `WendelonLogStore::open` fails at open with:

```
wal <path> interior corruption at offset 0: log entry postcard decode failed:
Hit the end of buffer, expected more data
```

Pinned by `adr0092_v1_ctas_provision_wal_record_replay_behavior`. Three
defects live in that one line:

1. **The node refuses to boot.** Body decode is eager during the open scan, so
   one v1 `Provision` entry in the un-purged tail makes the whole profile
   unstartable — the recovery-time failure mode, invisible in normal
   execution.
2. **A known shape is reported as damage.** The record's own header declares
   version 1; the binary writes 2 and knows exactly what changed between them.
   The snapshot path already handles this honestly — its decode failure at a
   lower-but-recognized version names both versions and the no-migration
   stance. The WAL path special-cases only version zero; v1 gets the bare
   sentence and the `interior corruption` classification 0085 Decision 3
   forbids.
3. **Refusal-not-misapply is luck, not contract.** The failure is a clean
   end-of-buffer only because 0087's field is the *last* in the postcard
   stream. A future widening in the middle of a body would over-consume its
   neighbour's bytes and could decode successfully into wrong values — a
   silent misapply into replicated state. Nothing checks the trailing-only
   discipline today.

The exposure window is not speculative. The un-purged tail routinely spans
tens of thousands of entries across restarts (the live soak profile holds
`purged 19999, applied 23187`), so any upgrade with CTAS activity in that
window hits defect 1. The soak profile crossed the 0087 boundary intact only
because its first CTAS ever ran on 2026-08-23, after the upgrade — the bomb
was real and unarmed. The snapshot carries the same exposure with a shorter
horizon: a v1 `SmState` under the current build is refused (by name, at
least), and a node whose newest snapshot is v1 cannot regenerate it without
booting.

Both artifacts share one boundary and would share the mechanism that closes
it; splitting them would build the version-scoped decode machinery twice.

## Decision

1. **A superseded body version is decoded by its own shape and upconverted,
   not decoded by the current shape.** Replay dispatches on the version the
   header declares. For v1, a frozen shadow decodes the old shape and a total
   conversion produces the current type. The conversion is complete, not
   nominal: `Provision` gains `certificate: None`; `SmState` gains the empty
   materialization catalog **and every entry of a non-empty
   `CtasSagaCatalog` is converted, each `CtasSagaEntry` gaining
   `certificate: None`** — the entry's certificate field sits mid-stream
   inside the catalog map, so an empty-catalog conversion would be a lie that
   only an active-saga snapshot exposes. All conversions are semantically
   exact: they state what every v1 writer meant, so this is replay, not
   guesswork.

   What gets frozen is the **changed container spine**, because derived Serde
   cannot substitute a leaf inside a derived container: the WAL side freezes
   the `WendelonRequest` enum spine — every discriminant, in order — so its
   `CtasSaga` arm can carry the v1 mutation shape; the snapshot side freezes
   the `SmState` field spine and the CTAS catalog/entry containers. Unchanged
   payload types are referenced from the frozen spine, not copied. Shadows
   live in version-scoped modules (`wal_body::v1`, `snapshot_body::v1`),
   never as version-suffixed type names.

2. **Upconversion is offered only where it is total.** A future version whose
   old shape cannot be converted without invention gets a refusal, and the
   refusal is the WAL twin of the snapshot sentence: the artifact is intact,
   written at version N, this build writes M, and what the operator can do
   about it. The classification rule is stated narrowly: **a hash-matched,
   recognized-version decode mismatch is never classified as
   `interior corruption`.** The corruption classification legitimately keeps
   covering digest and checksum failures, structural framing contradictions,
   coordinate mismatches between header and decoded entry, and invalid record
   types — those are evidence of damage or of a writer this build cannot
   attribute, and renaming them would blunt a diagnostic that is currently
   telling the truth.

3. **Superseded shapes are pinned as bytes, not as code.** Each version's
   control decodes a stored byte fixture — the probe's hand-built v1 page
   becomes the first — so refactoring shared types cannot silently "repair" a
   frozen shape. A fixture the current encoder can regenerate is not a
   fixture.

4. **The current shape is pinned by a golden fixture corpus, and changing it
   forces the bump.** A single canonical golden cannot see every reachable
   change — appending an unused enum variant leaves every existing encoding
   byte-identical. The pin is therefore a corpus plus an exact-set guard: one
   golden encoding per `WendelonRequest` variant and per changed container
   (`SmState`, `CtasSagaEntry`), paired with a variant-count guard in the
   ADR 540 totality style so an appended variant fails the set check even
   though no stored bytes move. Any change the corpus or the guard catches is
   re-pinned in the same cut as a
   `WAL_FORMAT_VERSION`/`SNAPSHOT_FORMAT_VERSION` bump, a frozen fixture of
   the outgoing version, and its replay control — the documented, enforced
   procedure. This is the check-that-can-fail 0085 Decision 4 wanted; the
   existing prior-version refusal controls test the gate, this tests the
   discipline.

5. **New fields append at the stream tail.** The golden pin makes a mid-stream
   widening visible, and review refuses it: a tail addition fails loudly under
   a wrong-version decode, a mid-stream addition can misapply silently. The
   discipline is stated here once and enforced by Decision 4's procedure.

## Acceptance

- A stored v1 `Provision` WAL page replays, and the recovered state machine is
  byte-equal to the state a current-encoding `Provision` with `certificate:
  None` produces. The control drives both through real recovery and compares
  the resulting states, not just the decode.
- A stored v1 page of an unchanged-shape variant (a lease entry) replays
  identically to its v2 encoding — the dispatch must not disturb bodies whose
  shapes never differed.
- A v1 snapshot decodes via the shadow shape and boots; a profile holding a v1
  snapshot *and* a v1 WAL tail boots to the state a full replay produces.
- The v1 snapshot fixture carries an **active saga** — a non-empty
  `CtasSagaCatalog` with an in-flight entry — and the conversion is judged on
  that entry's fields, `certificate: None` included. An empty-`SmState`
  fixture would pass a conversion that never touches catalog entries and is
  not the control.
- A version with no registered decoder and no total conversion is refused with
  the named sentence: version found, version written, artifact intact, no
  `interior corruption` in the text. The existing newer-than-build refusals
  stand unchanged.
- The golden corpus and exact-set guards exist for both artifacts: every
  `WendelonRequest` variant and every changed container has a pinned encoding,
  and the variant-set guard fails on an appended variant whose addition moves
  no stored bytes. Their discriminating run: the v1 fixture decoded through
  the *current* shape still produces the exact error the grounding probe
  pinned — proving the dispatch, not a widened tolerance, is what makes v1
  readable.
- The grounding probe's assertions flip from pinning the defect to pinning the
  contract, in the same cut — and its v1 page construction is replaced by
  **immutable stored v1 bytes**. The current-encoder truncation was the right
  tool for grounding, but as a permanent control it would track the current
  encoder's drift; the stored fixture cannot.

## Guardrails

- Version-scoped shadow modules are reachable only from the replay dispatch;
  an architecture guard fails if `wal_body::v1` or `snapshot_body::v1` is
  named anywhere else. Frozen shapes are not an API.
- No `#[serde(default)]` is load-bearing for postcard compatibility anywhere
  in a consensus body; the attribute stays where JSON/dev tooling wants it but
  the versioned dispatch is the only compatibility mechanism.
- The versioned decode paths consume their payload exactly
  (`take_from_bytes` + empty-remainder check, as the snapshot body already
  does); the WAL body decode adopts the same check for every version including
  current.
- Raising a format version without landing the outgoing version's frozen
  fixture and replay control in the same cut is refused in review; the corpus
  re-pin (or the exact-set guard, for changes no stored encoding can see)
  makes the omission visible.

## Risks and deferrals

- **Shadow drift.** A frozen module that references shared unchanged types
  inherits their future changes. The byte fixtures are the defense: they fail
  the moment a referenced type's encoding moves, which is exactly when the
  frozen module must fork its own copy.
- **openraft coupling.** The v1 dispatch must reconstruct
  `Entry<WendelonTypeConfig>` from a shadow request; if `Entry`'s own postcard
  layout ever shifts under an openraft upgrade, the fixtures catch it, but the
  repair is ours, not upstream's.
- **`pending_finalizations.redb`** is the third 0085 surface and is unchanged
  since; it stays under 0085's refusal contract and is out of scope here by
  name.
- The v0 contract is untouched: pre-versioning artifacts decode by the current
  shape with 0085's explanatory failure text, because nothing can know their
  true shape.

## Forward

This closes the recovery-time boundary the storage side already closed
piecemeal — the V5 index length gate, the frame placement cross-check, the
schema-update envelope — with one uniform contract for the consensus bodies:
every widening is versioned, every superseded version is either exactly
replayable or exactly refused, and the discipline that keeps that true is a
test, not a review habit. The next body widening (the first candidate is
whatever ADR carries rebalancing transfer execution) pays one golden re-pin
and one fixture instead of an upgrade outage.
