# ADR 0086: extensible authored-family colour lineage

Status: proposed 2026-08-12. This record keeps authored mark family in the
categorical-colour lineage key, but stops making every new authored family a new
session-schema version. `polariton_session` becomes version 3 and carries a
bounded, opaque authored-family identifier. Versions 1 and 2 retain their
closed vocabularies. Version 3 readers preserve valid family identifiers they
do not understand, so a later mark can earn its own spelling without forcing
version 4 merely because it allocates categorical colour.

This is a substrate record. It adds no Rune word, makes no renderer reachable,
chooses none of the remaining marks' spellings, and changes no resolved colour
or pixel.

## Context

The plot-catalog restoration census in
`plans/wip/plot-catalog-restoration.md` separates two colour problems. Heatmap
and bubble encode a quantitative value through a continuous ramp and allocate no
categorical identity. Pie, violin, radar, radial dial, parallel coordinates, and
funnel encode categories and therefore need the continuation state PRODUCT-0007
§8 established.

That continuation is currently:

```text
ColorIdentityKey { field, layer: LayerIdentity, category }
ColorLineageKey  { field, layer: LayerIdentity }
LayerIdentity    { ordinal, plot_type }
```

The runtime definitions are at `plot/spec/mod.rs:705,1150-1160`. The family is
load-bearing. PRODUCT-0007 §8 says a lineage is keyed by resolved field and
layer identity; PRODUCT-0055 §4 makes orientation a rendering member rather
than a new lineage; PRODUCT-0064 §3 applies the same rule to grouped versus
stacked bars; and PRODUCT-0070 §4 gives area its own authored family. Removing
family from the key here would silently overturn all four decisions. A mark
changed at the same view and layer ordinal would inherit another mark's
allocation history merely because it reused the same field name.

Persistence introduces a different problem. PRODUCT-0062 projects the
renderer-facing `PlotType` into the closed DTO enum `AuthoredFamily`
(`session_persistence.rs:423-436`). PRODUCT-0070 added `Area`, advanced
`SCHEMA_VERSION` from 1 to 2 (`:20-21`), and made the version meaningful by
rejecting an area family when the envelope says version 1 (`:447-465`). The
runtime-to-durable conversion at `:698-712` refuses every other plot type.

Histogram and box plot avoided that path because they allocate no categorical
colour. The six categorical catalog marks cannot. Adding six enum variants now
would make version 3 understand this known batch, but the next colour-bearing
mark would recreate the same migration question. Removing family from identity
would avoid the migration by destroying the semantic partition it protects.

The useful cut is neither. Family remains part of colour lineage, while version
3 changes its durable representation from a closed enum to a bounded opaque
identifier. An older version-3 binary need not render a future family in order
to preserve its continuation state. The payload owner already retains colour
meanings independently of whether a panel is open (PRODUCT-0062 §4), so opaque
preservation is a real operation rather than a theoretical compatibility claim.

## Decision

### 1. Authored family remains part of categorical colour identity

Categorical identity continues to be keyed by:

```text
(resolved field, authored family, layer ordinal, category value)
```

Family is not removed, inferred from the category field, or replaced by the
view identity. `LocalViewIdentity` selects the per-view `ColorIdentity`; family
and ordinal partition the lineages inside it. Those are different levels of
identity and neither substitutes for the other.

The discriminating fixture is asymmetric. Under the same view, field, five-slot
palette, and ordinal, family A first sees `[A, B, C]` and therefore assigns
`A=0, B=2, C=4`; family B first sees `[C, A]` and independently assigns
`C=0, A=4`. If family is omitted, B inherits A's `C=4, A=0` history and the two
categories arrive in the opposite positions. A fixture in which both families
first see the same ordered domain is invalid evidence: both designs produce the
same colours.

Changing only a rendering member continues to preserve lineage. Vertical and
horizontal bars are authored family `bar`; grouped and stacked bars are
authored family `bar`; any later member of an existing family must make the same
decision in its own record. A genuinely new authored mark begins a new lineage.

### 2. Colour lineage gets an authored-family type independent of `PlotType`

The colour maps stop storing the renderer vocabulary indirectly. They use:

```text
AuthoredFamilyId(private bounded token)

ColorLayerIdentity {
    ordinal: usize,
    family: AuthoredFamilyId,
}
```

`ColorIdentityKey` and `ColorLineageKey` carry `ColorLayerIdentity`.
`BoundChannel.layer` may keep the existing `LayerIdentity { ordinal,
plot_type }` as its resolution-phase carrier; one conversion authority projects
that authored layer into `ColorLayerIdentity` before a categorical allocation
key is formed. A renderer `PlotType` does not enter `ColorIdentitySnapshot` or
the persistence DTO.

This split states what PRODUCT-0062 already argued but its type did not encode:
`PlotType` is a freely changing renderer catalog, while authored family is
semantic lineage. It also lets a decoded version-3 identity retain a family the
running binary does not yet know, because no conversion back into `PlotType` is
required merely to hold or re-encode it.

There is one conversion authority for live authored layers. It maps today's
families exactly:

```text
Scatter                         -> scatter
Bar | HorizontalBar             -> bar
Line                            -> line
StackedArea                     -> area
```

Live allocated identifiers are enumerable even though the version-3 grammar is
open. `AuthoredFamilyId::ALL` is the exhaustive catalog of named family
constants the running binary can originate. Today it contains exactly
`scatter`, `bar`, `line`, and `area`. Its entries are unique. Every target of
the live conversion authority appears in `ALL`, and every entry in `ALL` has a
canonical authored-family representative in that conversion. The architecture
gate reads the named constant declarations and the `ALL` catalog as source
shape, following PRODUCT-0075's `PlotType::ALL` precedent, so declaring a
constant without cataloguing it cannot pass merely because no runtime consumer
uses it yet.

`ALL` is **not** the set of strings version 3 can decode. §3 deliberately
admits valid opaque tokens absent from it. It is the set the current binary can
allocate from live authored source. Keeping those two sets distinct is what
makes forward preservation possible without making production token creation
unreviewed.

The `HorizontalBar` input preserves the compatibility accepted by
PRODUCT-0062's rotation gate; production channel resolution continues to carry
`Bar` as PRODUCT-0055 requires. `StackedBar` and
`HorizontalStackedBar` do not gain a second normalization escape: PRODUCT-0064
requires their layer identity to remain `Bar` before this boundary, so seeing a
stacked renderer member here invokes `unreachable!` with a message naming
PRODUCT-0064's authored-family invariant. Every other renderer member without a
registered authored categorical family invokes the same form with a message
naming the missing live conversion. This is not a recoverable data refusal:
channel resolution has already admitted and constructed a supposedly authored
colour-bearing layer, so arrival exposes a Polariton phase-boundary bug rather
than bad user data.

No identifier for pie, violin, radar, radial dial, parallel coordinates, or
funnel is chosen here. Each surface record owns its authored spelling and adds
one named family constant, one `ALL` entry, and one live conversion. Version 3
makes that additive code change cease to be a persistence-schema migration.

### 3. Version 3 families are bounded canonical tokens

The durable family field remains a JSON string but is decoded through
`AuthoredFamilyId`. A valid token is 1 through 64 ASCII bytes and matches:

```text
[a-z][a-z0-9_]*
```

Empty, over-length, non-ASCII, uppercase, leading-digit, whitespace, hyphen,
dot, and other punctuation forms refuse. Validation happens before a token can
enter either runtime colour map. The inner string remains private; production
call sites use named constants, while the decoder uses the checked parser.

The token is opaque after validation. A version-3 reader does not require it to
appear in the running binary's known-family constants. Unknown-but-valid is a
state to preserve, not a family to render, infer, alias, or silently map to a
known mark.

This is not an unbounded string escape from schema discipline. The grammar and
length are the version-3 schema. Changing either, assigning a second meaning to
an existing token, or renaming a token already emitted is a schema decision.

### 4. Versions 1 and 2 keep their closed meanings

`polariton_session` writers publish schema version 3. The current reader accepts
versions 1, 2, and 3 with different family admission rules:

| envelope version | admitted family tokens |
|---|---|
| 1 | `scatter`, `bar`, `line` |
| 2 | version 1 plus `area` |
| 3 | every token satisfying §3 |

A wire number is not a code vocabulary. Production code represents the three
rows as a semantic admission policy with names equivalent to:

```text
InitialClosedFamilies
AreaClosedFamilies
ExtensibleFamilies
```

The wire constants may carry numeric values 1, 2, and 3, and the codec entry
point may continue to accept `schema_version: u32`. No Rust type, enum variant,
function, fixture, test, or comment is named `V1`, `V2`, `V3`, `version_one`,
`schema_two`, or another number-as-era synonym. In particular, this change does
not create `PayloadV1`, `PayloadV2`, or `PayloadV3` types. The values select a
family-admission policy; they do not create three implementations of the
payload.

The existing `FIRST_SCHEMA_VERSION`, every `schema_two` persistence-test name,
the `v1` fixture variable, and session-specific assertion prose such as
`"schema v1"` and `"publishes schema v2"` are retired in this cut. The
lower-bound constant, PRODUCT-0070 migration gate, PRODUCT-0075/0079/0082 owner
gates, and the related editor assertions are renamed for their semantic roles.
`SCHEMA_VERSION` remains valid because it names the current owner protocol
rather than an era-specific implementation. Numeric wire values stay confined
to the protocol constants, envelope comparison, and codec fixtures.

This naming constraint is scoped by construction to
`session_persistence.rs` and `editor.rs`, the two files containing the owner
implementation and its persistence assertions. It is not a repository-wide
rule with exceptions. BigQuery's generated `storage::v1` module lies outside
the two-file instrument because it is upstream API identity, not terminology
introduced for this schema.

A version-1 payload relabelled to contain `area` still refuses. A version-1 or
version-2 payload containing a valid future token still refuses. A version-3
payload containing that token decodes and preserves it. This replaces
`Payload::validate_family_vocabulary`'s area-only special case with an explicit
version table; it does not let the new parser retroactively widen old schemas.

Existing v1 and v2 payloads restore with every panel mapping, group mapping,
allocator mark, allocation space, palette position, and family meaning
unchanged. An actual version-2 binary refuses a version-3 envelope through the
existing unsupported-schema check (`session_persistence.rs:217-226`). That is
honest backward incompatibility at the owner boundary, not silent loss.

Writers always declare version 3, even when a payload contains only the original
three families. The version names the grammar the writer is entitled to emit,
not an optimization based on current contents; PRODUCT-0070 §5 already settled
that rule for version 2.

### 5. Unknown v3 families are preserved end to end, not interpreted

A valid unknown family must survive:

```text
owner bytes
  -> DurableMeanings::decode(3, ...)
  -> restored ColorIdentity
  -> runtime meanings
  -> checkpoint capture
  -> owner bytes
```

with its family token, field, ordinal, categories, positions, and allocation
space unchanged. Closing its panel does not discard it; removing its producer
document does, following PRODUCT-0062's existing colour lifetime exactly.

Preservation grants no renderer or language authority. The binary does not
create a kernel, accept a Rune word, resolve a new channel register, or show a
legend because an opaque family exists in saved data. If source asks an older
binary for a mark it cannot compile, that source refuses through the ordinary
presentation path while its already-stored colour continuation remains intact.

This property is why an open version-3 token is materially different from
adding six enum variants. It makes a future family forward-preservable by an
older v3 binary rather than merely understood by the one release that predicted
it.

### 6. Allocation semantics do not change

`PalettePosition`, `ColorAllocationSpace`, palette-length partitioning,
first-complete-domain spreading, maximin extension, capacity refusal, and
atomic proposal/commit remain byte- and behaviour-identical. This record
changes one component's type, not how a position is chosen.

In particular:

- the same field and family continue the same lineage across filtering,
  sorting, panel close/reopen, and process restart;
- changing the categorical field starts a new lineage;
- changing authored family starts a new lineage;
- changing only an accepted geometry member does not; and
- an unknown preserved family consumes no position in a different family.

No category from heatmap or bubble enters these maps. Their quantitative colour
scales remain prerequisite B in the restoration plan and are not made
categorical to reuse this work.

### 7. The DTO remains explicit and renderer-free

Serialization stays owned exclusively by `session_persistence.rs`. Runtime
plotting types do not derive `Serialize` or `Deserialize`. The DTO keeps:

```text
DurableLayer {
    ordinal: u64,
    family: String, // decoded and encoded through AuthoredFamilyId
}
```

It contains no `PlotType`, kernel name, composition member, channel binding,
resolved colour, or renderer settings. The architecture gate that currently
requires `enum AuthoredFamily` is superseded: it instead requires the bounded
token parser, forbids `PlotType` in the DTO region, and proves the sole serde
owner remains `session_persistence.rs`.

The numeric conversion checks remain: `ordinal` must fit `usize`, palette
positions and allocation lengths retain their existing bounds, duplicate
assignments and spaces refuse, and every assignment still requires its matching
allocation space.

### 8. This record changes no authored or rendered surface

The six catalog marks remain producerless after this record. Existing scatter,
bar, line, and area figures resolve to the same exact assignments and draw the
same operations. Histogram and box plot retain empty colour footprints. Heatmap
and bubble remain continuous-ramp kernels outside categorical identity.

The only externally visible change is the owner schema version written at the
next checkpoint. Existing sessions remain readable; a pre-v3 application
honestly refuses a newly written v3 owner rather than pretending it can preserve
the open-family contract.

## Implementation sequence

### Stage 0 — bank the closed v2 state

Before changing types or the version, capture:

- canonical v1 bytes containing scatter, bar, and line;
- canonical v2 bytes containing scatter, bar, line, and area;
- the v1-area refusal and the future-version refusal;
- a runtime continuation fixture whose two families have asymmetric first
  domains, proving the family component changes allocation; and
- exact resolved assignments for existing scatter, bar, line, and area paths.

The asymmetric fixture must be banked before the key changes. A fixture whose
families happen to receive identical positions cannot prove the family remains
in the key.

### Stage 1 — split renderer layer from colour lineage

Add `AuthoredFamilyId`, its exhaustive live `ALL` catalog, and
`ColorLayerIdentity`; move both colour-map keys and their snapshots to the
latter; add the one live conversion authority and its explicit `unreachable!`
boundary; and keep all allocation behaviour green before changing persistence.
Existing rendered operations and Stage-0 assignments must remain identical.

### Stage 2 — install the version-3 codec

Replace the closed DTO enum with the checked token, advance
`SCHEMA_VERSION` to 3, install the three semantic admission policies behind the
wire-number boundary, and add an unknown valid family round-trip. Read the two
closed historical schemas through their exact rules. Do not add any catalog
mark mapping in this stage, and do not introduce number-named Rust items.

### Stage 3 — prove preservation through both persistence directions

Carry an unknown v3 family through decode, runtime re-keying, capture, and
re-encode; exercise panel close/reopen and producer removal; run the complete
session suite; then update the restoration plan to name PRODUCT-0086 as
prerequisite A's persistence foundation. No screenshot is required because no
reachable pixel moves.

## Acceptance gates

1. **The banked v2 state is discriminating.** Canonical v1/v2 payloads and the
   v1-area refusal are captured before Stage 1. Over a five-slot palette the
   colour fixture banks family A as `A=0, B=2, C=4` and family B as `C=0, A=4`;
   removing family yields `C=4, A=0` for B and therefore fails.

2. **Family remains a lineage dimension.** Same view, field, ordinal, palette,
   and category under two authored families produce independent allocation
   histories; rerunning each reproduces its own positions. A control with the
   same family proves ordinary continuation still occurs.

3. **Geometry members do not fork family.** Vertical/horizontal and
   grouped/stacked bar controls share the `bar` lineage exactly, while bar and
   area with the same field/categories do not. A test that merely compares the
   durable strings is insufficient; resolved positions after asymmetric history
   must distinguish both cases.

4. **Colour identity no longer stores renderer vocabulary.** The two internal
   colour keys and their persistence snapshots carry `ColorLayerIdentity`;
   neither contains `PlotType`. `BoundChannel` may retain its resolution-phase
   `LayerIdentity`, proving the change is scoped rather than a renderer-wide
   rename.

5. **Token validation is exact.** Valid boundary fixtures of lengths 1 and 64
   enter; empty, 65-byte, non-ASCII, uppercase, leading-digit, whitespace,
   hyphen, and dot fixtures each refuse with a family-attributed reason. Every
   refusal has a valid neighbouring twin so rejecting all decoded strings cannot
   satisfy the gate.

6. **The version table has three real meanings.** V1 accepts only scatter/bar/
   line; v2 additionally accepts area; v3 accepts a valid unknown token. Area
   relabelled v1, and the unknown token relabelled v1 or v2, refuse. Merely
   accepting schema versions 1 through 3 against one open parser fails this
   gate.

7. **Wire numbers do not become code vocabulary.** The codec selects semantic
   `InitialClosedFamilies`, `AreaClosedFamilies`, or `ExtensibleFamilies`
   policy; it does not define versioned payload types or number-named branches.
   A source-shape gate reads exactly `session_persistence.rs` and `editor.rs`.
   For Rust identifiers it splits on underscores and rejects a component
   `v1`/`v2`/`v3`, or adjacent components `version` or `schema` followed by
   `one`/`two`/`three`. For comments and string literals it applies the same
   case-insensitive word-token rule, including punctuation-separated forms such
   as `schema-v2`. It does **not** reject the digits `1`, `2`, or `3`, or the
   neutral words `version` and `schema`. It proves the old
   `FIRST_SCHEMA_VERSION`, `v1` fixture variable, all four existing
   `schema_two` owner-test names, and the editor's `schema v2` prose are gone.
   Its positive half asserts that `SCHEMA_VERSION = 3` and codec fixtures using
   `decode(1, ...)`, `decode(2, ...)`, and `decode(3, ...)` remain, so a rule
   that removes the wire numbers to pass cannot satisfy the gate. Generated
   external API paths require no carve-out because no third file is scanned.

8. **Existing sessions migrate losslessly.** Banked v1 and v2 owner payloads
   decode under the v3 binary and reproduce every mapping, mark, field,
   category, position, and allocation space. Re-encoding changes no meaning.

9. **A v2 reader refuses v3 honestly.** The existing bounded schema check,
   exercised with maximum supported version 2, refuses a version-3 envelope.
   It may not accept the envelope while dropping or remapping unknown families.

10. **Unknown v3 family state is forward-preserved.** A synthetic valid token
   absent from every named live constant, with both assignments and an
   allocation space, survives decode -> runtime join -> capture -> encode. The
   output retains the exact token and positions. Mapping it to `scatter`,
   dropping it, or refusing it each fails.

11. **Colour lifetime is unchanged.** In the gate-10 fixture, closing the panel
    retains the opaque colour meaning and reopening recovers it; removing the
    producer document drops it. This proves preservation uses PRODUCT-0062's
    real lifetime rather than a test-only DTO round-trip.

12. **There is one live conversion authority.** Every categorical allocation
    key formed from a `BoundChannel` routes through the same authored-family
    conversion. A source guard forbids local `PlotType`-to-token matches in
    resolvers and persistence. Synthetic direct constructors remain test-only.

13. **The live family catalog is exhaustive and injective.** A source-shape
    architecture gate compares every named `AuthoredFamilyId` constant with
    `AuthoredFamilyId::ALL`; omission from either side fails. All `ALL` tokens
    are distinct, every live conversion target belongs to `ALL`, and canonical
    authored-family representatives map one-to-one onto it. The explicitly
    accepted `Bar | HorizontalBar -> bar` member normalization is tested
    separately and is not misreported as two authored families.

14. **Forbidden renderer members hit the pinned invariant boundary.** Passing
    `StackedBar` and `HorizontalStackedBar` to the live conversion panics through
    `unreachable!` with PRODUCT-0064 named in the message; an unrelated
    unregistered member names the missing authored categorical conversion. A
    valid `Bar` twin returns `bar`, so a function that panics for every input
    cannot pass.

15. **Allocation behaviour is byte-identical.** Existing explicit and legacy
    palette gates reproduce their exact positions, extension order, capacity
    refusals, and atomic failure. The only serialized byte expected to differ
    at publication is the owner version outside the unchanged v1/v2 payload
    meanings.

16. **The DTO remains strict and bounded.** Unknown fields, duplicate keys,
    trailing content, oversized owner bytes, invalid numeric conversions,
    assignments without spaces, and invalid family tokens refuse. `PlotType`
    and renderer settings remain absent from the DTO region, and serde remains
    confined to `session_persistence.rs`.

17. **Continuous colour does not enter categorical identity.** Heatmap and
    bubble create no `ColorIdentity` assignment, allocation space, family token,
    or durable colour footprint. This record does not satisfy CI-02 or shrink
    its acceptance surface by relabelling quantitative values as categories.

18. **No surface or pixel moves.** No new presentation constructor, free
    function, `FigureElementKind`, authored channel form, or production
    `PlotType` producer appears. Existing operation banks and the full suite
    remain unchanged; histogram and box plot continue to serialize panel
    meaning with empty colour relations.

## Stop conditions

> If an unknown valid v3 family cannot be retained and re-encoded without
> converting it to a renderer `PlotType`, stop. Adding six closed enum variants
> would conceal that the proposed open grammar is not actually implemented.

> If any v1 or v2 payload accepted by its original schema cannot restore every
> meaning unchanged, stop. Catalog restoration is not authority to strand
> existing sessions.

> If separating `ColorLayerIdentity` requires changing resolved colours,
> palette allocation, renderer operations, or any Rune surface, stop. Those are
> mark records, not persistence substrate.

> If version 3 cannot bound and validate family tokens while preserving unknown
> valid ones, stop and return to a closed-vocabulary migration. An unbounded or
> lossy escape hatch is worse than repeated explicit versions.

## Rejected

- **Remove authored family from colour identity:** merges allocation histories
  for unrelated marks at the same layer ordinal and overturns PRODUCT-0007,
  PRODUCT-0055, PRODUCT-0064, and PRODUCT-0070.
- **Add six `AuthoredFamily` enum variants to version 3:** handles the current
  list and recreates the migration for the next colour-bearing mark. It also
  chooses six durable spellings before their surface records earn them.
- **Serialize `PlotType`:** makes saved user data depend on a renderer catalog
  whose variants may be added, renamed, split, or retired without authored
  meaning changing; PRODUCT-0062 rejected that coupling explicitly.
- **Map the six marks onto `Scatter`, `Bar`, `Line`, or `Area`:** avoids a
  version by making semantically unrelated marks inherit one another's colour
  history.
- **Accept an arbitrary `String`:** permits empty, confusable, unbounded, and
  accidental identifiers into a durable semantic key. Version 3 is open to
  future canonical tokens, not schema-free text.
- **Wait for the first mark and migrate one family at a time:** repeats the
  persistence decision in every surface record and turns the known catalog
  restoration dependency into version churn. The concrete six-family demand
  is what earns the extensible substrate now.

## Consequences

Authored family remains a real part of categorical meaning, but it ceases to be
a closed list embedded in every session-schema version. Version 3 pays one
migration to make that distinction structural. Future surface records still
decide their words, channels, cardinality, colour consumer, guide, and exhibit;
adding their canonical family identifier no longer changes the persistence
grammar.

No catalog mark becomes authorable here. The immediate next record remains the
continuous sequential/diverging authority or the first categorical mark's
substrate, according to the restoration plan. What changes is that neither must
smuggle a persistence design into its surface work.

PRODUCT-0062 §5's closed durable vocabulary is superseded for version 3 only.
Its reasons for excluding `PlotType`, preserving family and ordinal, validating
positions, and refusing malformed data remain in force. PRODUCT-0070 §5's v2
migration remains the exact compatibility rule for old payloads; version 3
changes how future family growth is represented, not what versions 1 and 2
mean.
