# ADR 0091: the continuous colour authoring surface

Status: proposed 2026-08-12. PRODUCT-0089 built a continuous colour substrate
with no way to reach it: the only production job origin is `Sequential`, the
diverging ramp slot is unreachable, and no anchor can be authored. This record
adds the Rune words, the stable ramp-name grammar they create, the partial-slot
cascade carrier they need, and the anchor-provenance path from authoring to
planning — then retires the tripwire that has been holding the substrate shut.

It closes ledger item **CI-02** for continuous colour and **splits status out**
as a residue, for the reason in §10.

**It depends on the heatmap and bubble surfaces** (`plot-catalog-restoration.md`
steps 6–9), which must land sequential-only first. Without them every word here
would be observationally inert — the exact defect §10 uses to refuse a status
word. An earlier draft of this record proposed the words with no consumer, one
section away from making that argument.

## Context

### What exists, surveyed 2026-08-12

**One colour word, and it is categorical.** `palette(...)` registers as
`__palette` (`rune_presentation.rs:396,632`), is enforced as at most one per
figure (`:2093`), resolves through `CategoricalPaletteSelection::from_theme_name`
(`spec/mod.rs:920`) against **libaryon theme identities**, and defaults to
`"viridis"` (`theme.rs:269`).

**No quantitative mark has a Rune constructor.** The registry holds `__bar`,
`__line`, `__scatter`, `__area`, `__histogram`, `__box`
(`rune_presentation.rs:384-408`). Heatmap, bubble, and hexbin are absent.

**One job origin, and it is sequential.** The automatic channel path pins
`color_job: None` (`spec/mod.rs:1540`) and all three quantitative kernels request
`ContinuousScaleIntent::SEQUENTIAL`. PRODUCT-0089 gate 19 made this structural:
an architecture gate asserts **per kernel** that production contains neither
`ColorJob::Diverging` nor `ContinuousAnchor::Authored`
(`architecture_test.rs:464-465`).

**Ramp slots are not individually settable.** `PlotSettingsLayer` carries
`theme: Override<PlotThemeSpec>` (`theme.rs:192`) — atomic. Setting one slot by
constructing a whole theme would overwrite chrome and the other three ramps when
`apply_layer` runs (`theme.rs:336`).

**`ColorTheme` has no name grammar.** 37 variants and `ALL` (`colors.rs:7,50`),
`Display` in PascalCase (`colors.rs:164`), and **no** `FromStr`. There are no
snake_case identifiers and no parser.

**A ramp name does not determine a job, today, by default.** `"viridis"` is
simultaneously the default categorical palette identity (`theme.rs:269`) and the
default sequential ramp (`theme.rs:63`).

**Status has no consumer.** `status_ramp` is declared (`theme.rs:52`), defaults to
`RedGreen` (`:66`), reachable through `ramp(job)` (`:74`) — and no kernel ever
requests `ColorJob::Status`. `spec/mod.rs:2591` already maps
`Categorical | Status => ScaleFamily::CategoricalColor`.

## Decision

### 1. Three concepts, three words, no inference between them

| concept | word | level |
|---|---|---|
| categorical palette | `palette(name)` — **unchanged** | figure |
| which ramp fills the sequential slot | `sequential_palette(token)` | figure |
| which ramp fills the diverging slot | `diverging_palette(token)` | figure |
| whether a mark's job is diverging, and its anchor | `diverging()` / `diverging_at(v)` | mark |

Selecting a *ramp* and selecting a *job* are different acts, and no word does
both.

### 2. A ramp token never infers a job

`diverging_palette("viridis")` is legal and means "put viridis in the diverging
slot." `sequential_palette("blue_white_red")` is the legal mirror. The token
identifies a ramp; the slot is chosen by which word receives it.

This is PRODUCT-0089 §2's rejected classification reappearing one layer up. That
record refused to classify the 37 members as intrinsically sequential or
diverging because `PlotThemeSpec` already answers by slot; the same trap is more
tempting in spelling, and the shipped default already disproves the inference —
`"viridis"` occupies the categorical and sequential slots simultaneously.

### 3. The ramp-token grammar is a new public namespace

`sequential_palette` and `diverging_palette` accept **tokens**, not `ColorTheme`
values, and this record creates that namespace.

- **The catalog is all 37**, mechanically snake_cased from the existing
  PascalCase `Display` (`colors.rs:164`) — `BlueWhiteRed` → `blue_white_red`.
  Curating a subset is rejected: it decides which ramps are "real," which is a
  second decision, and strands the rest unreachable.
- **One conversion authority** owns token → `ColorTheme`. Not a match arm
  duplicated per word.
- **Unknown tokens refuse**, naming the offending token at its span.
- **Completeness and injectivity are both gated.** Every variant has exactly one
  token; no two variants collide after casing. Mechanical derivation guarantees
  neither — two PascalCase names can snake_case to the same string — so the
  injectivity gate is not ceremony.

**These tokens outlive `ColorTheme`.** CI-03 retires the enum; the tokens are the
public contract and must survive it, with `ColorTheme` demoted to their temporary
backend. Stated here because the alternative — treating tokens as a rendering of
whatever the enum happens to be called — makes CI-03 a breaking change to authored
figures.

**They are a different namespace from `palette(...)`.** That word resolves
libaryon theme identities (`spec/mod.rs:920`); these resolve ramp tokens. No
cross-resolution, and no shared resolver — both accept strings, which makes a
shared entry point the obvious wrong shortcut.

### 4. Slot overrides join the cascade beside the atomic theme

`PlotSettingsLayer` gains two fields beside `theme` (`theme.rs:192`):

```rust
pub sequential_ramp: Override<ColorTheme>,
pub diverging_ramp:  Override<ColorTheme>,
```

- **Within a tier, a slot override beats the atomic `theme`.** More specific wins.
  Setting one slot therefore no longer overwrites chrome or the other three ramps
  at `apply_layer` (`theme.rs:336`).
- **Across tiers, the existing precedence is unchanged** — the slot fields resolve
  through the same order as every other layer field.
- **The graphical-layer tier is inapplicable**, matching §4's principle that the
  *figure* chooses which ramp fills each slot, and following the precedent at
  `theme.rs:271` where that tier is already intentionally inapplicable to some
  settings.

PRODUCT-0089 §2's `ContinuousRamps` carrier then delivers the resolved slots
unchanged. No new tier, and no per-mark ramp override — a mark chooses its *job*;
the figure chooses which ramp fills each *slot*.

### 5. The mark word selects the job and carries the anchor

Two spellings, mapping 1:1 onto the substrate:

| spelling | `ContinuousScaleIntent` |
|---|---|
| *absent* | `{ Sequential, None }` |
| `diverging()` | `{ Diverging, DefaultZero }` |
| `diverging_at(v)` | `{ Diverging, Authored(v) }` |

**The surface cannot express an invalid intent.** PRODUCT-0089 §6 refuses
`Sequential` with any anchor, but no word produces that pair: an anchor arrives
only through `diverging_at`, which sets the job in the same act. The substrate
rule remains as a defence for non-Rune construction; from Rune it becomes
unreachable rather than merely rejected.

`diverging_at(v)` inherits 0089 §6's authored-anchor rules and §7's degenerate
rule unchanged. This record supplies the origin; it does not restate the policy.

### 6. The anchor's provenance is carried, span and all

PRODUCT-0089 §3 deliberately left this path unbuilt, and it is this record's real
cost rather than a risk to note. The complete path:

- `AuthoredPlotSpec` already has `color_job`; the automatic path discards it
  (`spec/mod.rs:1540`). For heatmap and bubble it stops discarding.
- `AuthoredPlotSpec` gains the anchor, since no anchor field exists anywhere.
- `ResolvedPlotSpec` carries both onward as `ContinuousScaleIntent`.
- Continuous intent travels through the sink that retains mark origin
  (`rune_relation_envelope.rs:276`); the record states what the other sink
  (`:121`) does with a figure that carries intent, rather than leaving it to
  whoever hits it first.

**The `diverging_at(...)` span is retained to planning.** PRODUCT-0089 §6's
anchor refusals are **domain-dependent** — out-of-domain, boundary, zero-width —
so they surface at planning, long after authoring. Without span retention the
diagnostic points at the whole mark or the whole figure, when the author's error
is one number in one modifier.

PRODUCT-0090 §2 declined spans because bubble was producerless and automatic
resolution keeps only field names. That reasoning does not transfer: an authored
mark has a call site, so the span exists and only has to survive the distance.

### 7. Cardinality and collisions, refused at the second spelling

| rule | on violation |
|---|---|
| at most one `sequential_palette` per figure | refuse at the **second** spelling's span |
| at most one `diverging_palette` per figure | refuse at the **second** spelling's span |
| `palette` may coexist with either or both | — |
| `diverging()` and `diverging_at(...)` are one mutually exclusive family, at most once per mark | refuse at the **second** spelling's span |

Refusing at the *second* span rather than the first is deliberate: the second
spelling is the one the author added, and pointing at the first tells them to
delete the line they meant to keep. This mirrors the existing one-categorical-
palette rule (`rune_presentation.rs:2093`).

### 8. Only heatmap and bubble receive the job word, for two reasons

Hexbin gets no job word because it **has no Rune surface** (steps 6–9 cover
heatmap and bubble only) and because it **can never diverge** — domain
`0..max_count`, counts non-negative by construction, settled by PRODUCT-0089 §9.

Both reasons are recorded because the first expires. When hexbin does get a
surface, the second must still refuse it a job word, and a record citing only
availability would read as though the omission were incidental.

The other eight catalog marks are categorical; `palette(...)` remains their word.

### 9. Gate 19's tripwire is retired deliberately, as a named step

`architecture_test.rs:464-465` asserts no production `ColorJob::Diverging` and no
production `ContinuousAnchor::Authored`. Landing §5 makes both false.

The assertion is **replaced, not deleted**: it becomes a gate that the only
production origins are the ones this record authors — heatmap and bubble, through
`diverging()` and `diverging_at`. Its purpose, that no origin appears without a
record deciding when divergence is meant, survives narrowed.

This is Stage 4, an explicit step, so it cannot be discovered as a broken build
and repaired by whoever is unblocking themselves.

### 10. Status splits out, and is not spelled here

Status gets no word. It has **no consumer** — no kernel requests
`ColorJob::Status`, and `spec/mod.rs:2591` already routes it to
`ScaleFamily::CategoricalColor`.

Spelling it now would author a setting nothing can observe, and its gates would be
vacuous: no fixture distinguishes the figure with the word from the figure
without it. That is the vacuity failure this arc has caught repeatedly, and it
would be introduced deliberately.

The residue's trigger is the **first real status consumer**, not "when
convenient."

### 11. What this record does not do

No status spelling (§10). No per-mark ramp override (§4). No new ramp *variants* —
the 37-member catalog is unchanged, though §3 gives it a public naming surface it
did not have. No change to `palette(...)`. No categorical work: a continuous scale
allocates no `ColorIdentity`. No anchor policy — that is 0089 §§6–7, consumed
here. **CI-03** stays open.

## Implementation sequence

**Prerequisite — the heatmap and bubble surfaces**, sequential-only
(`plot-catalog-restoration.md` steps 6–9). 0091 does not begin until both land.

**Stage 0 — bank.** Every heatmap, bubble, and hexbin figure once the surfaces
exist. All are sequential; all must stay byte-identical when the new words are
absent.

**Stage 1 — the token grammar.** §3's catalog, conversion authority, and
diagnostics, with no word consuming it yet.

**Stage 2 — the slot words and their carrier.** §4's `PlotSettingsLayer` fields
and precedence, then `sequential_palette` and `diverging_palette`. The sequential
slot becomes observable here; the diverging slot is authorable but still
unreachable.

**Stage 3 — the job word and the anchor carrier.** §§5–6, including span
retention.

**Stage 4 — retire the tripwire.** §9. A named step, not a fix-up.

**Stage 5 — reconcile the ledger.** CI-02 closed for continuous colour; the status
residue created with its first-consumer trigger.

## Acceptance gates

1. **Every token resolves, and the catalog is complete.** All 37 `ColorTheme`
   variants have a token, derived mechanically; `ColorTheme::ALL` is covered with
   no variant unreachable.
2. **Tokens are injective.** No two variants produce the same token. Mechanical
   snake_casing does not guarantee this, so the gate is real rather than
   ceremonial.
3. **An unknown token refuses at its span**, naming the token. Paired with a
   valid twin, since a resolver refusing everything passes the first half.
4. **Ramp tokens and palette identities do not cross-resolve.** A libaryon theme
   identity accepted by `palette(...)` but absent from the ramp catalog refuses
   in `sequential_palette`, and vice versa. Both directions, since one shared
   resolver would pass either alone.
5. **The sequential slot word is observable.** `sequential_palette("magma")`
   changes heatmap's mark colours and gradient guide from the viridis bank.
   Landing in Stage 2, before any job word exists.
6. **A slot override does not disturb the rest of the theme.** Setting only
   `sequential_palette` leaves chrome, the categorical ramp, the diverging ramp,
   and the status ramp exactly as the resolved theme had them. This is what §4's
   partial carrier buys; constructing a whole `PlotThemeSpec` fails it.
7. **Slot precedence within a tier.** A tier carrying both an atomic `theme` and a
   slot override resolves to the **slot override**.
8. **Slot precedence across tiers.** Set at `application_theme`, overridden by the
   Rune-authored tier, overridden by `pane_override` — the pane value wins. Three
   tiers, because Rune can only write the authored one and a two-tier gate cannot
   show the authored tier losing.
9. **The graphical-layer tier is inapplicable.** A slot override placed there does
   not take effect, matching §4.
10. **The diverging slot word reaches the diverging ramp.** With
    `diverging_palette("green_purples")` and a `diverging()` mark, marks and guide
    use GreenPurples — not the sequential slot, not the default BlueWhiteRed.
11. **A token does not infer a job.** `diverging_palette("viridis")` with a
    `diverging()` mark renders viridis *anchored*; `sequential_palette("blue_white_red")`
    with a plain mark renders BlueWhiteRed *unanchored*. Both halves, since either
    alone is satisfied by an implementation that reads the name.
12. **`diverging()` anchors at zero.** On a `-50…100` domain the neutral lands at
    data value **0** — PRODUCT-0089 gate 1, now reached from Rune rather than by
    test injection.
13. **`diverging_at(v)` anchors at `v`.** `diverging_at(25.0)` on the same domain
    puts the neutral at 25 — distinguishable from gate 12's zero and from the
    unanchored midpoint.
14. **Absence is sequential.** A heatmap or bubble with no job word yields
    `{ Sequential, None }` and a byte-identical figure to Stage 0.
15. **An authored-anchor refusal names the modifier, not the mark.**
    `diverging_at(500.0)` on a `-50…100` domain returns 0089 §6's refusal through
    planning with a span covering **`diverging_at(500.0)`**, and no render occurs.
    The span assertion is the point: a diagnostic naming the whole mark passes a
    refusal-only gate and still fails the author.
16. **Zero-width domains discriminate provenance.** `diverging_at(0.0)` on a
    constant heatmap refuses per 0089 §7; `diverging()` on the same data renders.
17. **Invalid intents are unspellable.** No combination of words produces
    `Sequential` with an anchor — asserted over the registered surface, since the
    claim is about what can be written rather than what is rejected.
18. **Cardinality refuses at the second span.** Two `sequential_palette` calls,
    two `diverging_palette` calls, and `diverging()` with `diverging_at(...)` on
    one mark each refuse, each naming the **second** spelling. `palette` alongside
    both slot words succeeds.
19. **Hexbin has no job word.** Asserted as an absence in the registered word
    table, not as a runtime refusal — §8's claim is that the word does not exist
    for it.
20. **The tripwire is replaced, not removed.** Production `ColorJob::Diverging`
    origins are exactly heatmap and bubble; production `ContinuousAnchor::Authored`
    origins are exactly `diverging_at`. A third of either fails.
21. **No status word exists.** The registered word table contains no status
    spelling, so §10's split cannot erode by someone adding it for symmetry.
22. **`palette(...)` is unchanged.** Every banked categorical figure is
    byte-identical and the one-palette-per-figure rule still holds.
23. **No categorical identity is allocated.** Resolving a diverging heatmap or
    bubble against a known `ColorIdentity` snapshot returns it unchanged.

## Stop conditions

> If the heatmap and bubble surfaces have not landed, do not start. Every gate
> from 5 onward needs a mark that can be authored, and adding a constructor here
> to unblock a gate makes this three records.

> If `sequential_palette` and `diverging_palette` cannot install into
> `PlotSettingsLayer` without a new cascade tier, stop. §4's claim is that the
> cascade already exists; a new tier makes this a cascade record.

> If the `diverging_at` span cannot reach planning, stop and record the carrier
> gap rather than degrading the diagnostic to mark or figure scope. §6 exists
> because a domain-dependent refusal is the one case where the author cannot see
> the fault from the source alone.

> If any gate here needs a status fixture to be meaningful, stop — §10 is wrong
> and status has a consumer nobody surveyed.

> If retiring the tripwire cannot be done without permitting an origin this record
> did not author, stop. §9 narrows the assertion; it does not drop it.

## Supersession

Supersedes no record. Depends on the heatmap and bubble surfaces. Consumes
PRODUCT-0089 §§1–7 unchanged and supplies the origin its §3 deliberately omitted.
Replaces the architecture assertion added by 0089 gate 19. Closes **CI-02** for
continuous colour and opens a status residue. Leaves **CI-03** open and better
posed, since two of the four ramp slots gain authored consumers and §3's tokens
give CI-03 a public contract to preserve.

## Rejected

**One word with a job argument — `palette(name, job)`.** Attractive as a single
concept with a smaller surface. Rejected because it invites the name to carry the
job in practice: authors write `palette("blue_white_red", diverging)` and then
omit the second argument, which must then be inferred from the first.

**Infer the job from the ramp token.** Attractive because most diverging ramps are
recognizably diverging. Rejected because the shipped default disproves it —
`"viridis"` occupies two slots at once — and because it is exactly the
classification PRODUCT-0089 §2 rejected, one layer up.

**Curate the token catalog to a sensible subset.** Attractive because 37 tokens
include ramps nobody should use for a continuous scale. Rejected because deciding
which ramps are "real" is a second decision this record has no evidence for, and
it would strand the remainder unreachable from Rune while still present in the
enum.

**Derive tokens from `Display` at call time rather than pinning a catalog.**
Attractive because it cannot drift. Rejected because it makes the public token
namespace a shadow of an internal enum's naming, so CI-03's retirement of
`ColorTheme` would silently rename authored figures' palettes.

**Share one string resolver with `palette(...)`.** Attractive because both accept
names. Rejected because they are different namespaces — libaryon theme identities
versus ramp tokens — and a shared resolver would make a categorical palette name
silently valid in a ramp slot.

**Set slot ramps by constructing a whole `PlotThemeSpec`.** Attractive because the
carrier already exists. Rejected because `apply_layer` (`theme.rs:336`) installs
the spec wholesale, so setting one slot would overwrite chrome and the other three
ramps with whatever the constructing code happened to default them to.

**Give hexbin a job word that always refuses.** Attractive for uniformity across
the quantitative marks. Rejected because a word existing only to be rejected
implies hexbin might diverge under some data, when its domain makes that
impossible.

**Spell status now, for symmetry.** Attractive because the theme slot exists and
the omission looks arbitrary. Rejected because nothing consumes
`ColorJob::Status`, so the word would be observationally inert and its gates
vacuous. Spellings are earned by use.

**Delete the tripwire rather than replacing it.** Attractive because the assertion
becomes false the moment §5 lands. Rejected because its purpose survives this
record intact; only its scope narrows.

## Consequences

The substrate becomes reachable. PRODUCT-0089's diverging path, built and gated
entirely under test injection, gets its first production origin, and the
neutral-at-zero behaviour that PRODUCT-0087 and PRODUCT-0088 banked as a defect
becomes something an author can ask for.

Polariton acquires a public ramp-name namespace it did not have. That is the
record's most durable output and its least visible: 37 tokens that must survive
CI-03's retirement of the enum beneath them, because authored figures will
reference them.

CI-02 closes for continuous colour, leaving status correctly posed — a slot with a
default, a ramp, and no consumer, waiting on the first thing that needs it rather
than on a spelling decision.

The three-word separation costs surface area against a single parameterized form.
That is the price of keeping ramp identity and colour job independent — the
distinction PRODUCT-0089 spent its §2 establishing at the type level, which a
convenient spelling would have quietly undone.
