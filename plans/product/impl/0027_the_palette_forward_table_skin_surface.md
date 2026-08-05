# The palette-forward table skin surface

Scope: implement PRODUCT-0027 only. Deliverable is one combined ADR/plan commit,
one indivisible implementation-and-tests commit, one evidence commit, and one
documentation-only closure commit that pins the evidence commit's hash. The
implementation commit must introduce `.duotone()` and flip named `.skin(name)`
to PaletteForward together. No production or test cleanup commit follows it.

## Halt rule

Stop on any libaryon or PRODUCT-0026 substrate change; table role, projection
policy, contrast, or alpha change; bare/SQL/Markdown default change; plot skin or
data-color syntax; `UiPalette`; installed `../uat/test9.rn` edit; table geometry;
execution; transport; panel identity; placement; dashboard; or a need to split
the modifier and default flip across commits, flags, dead grammar, or cleanup.

## Stage 0: feasibility and historical-proof findings

### 0a. The surface has one completion switch

`rune_view_outcome` in `gui/state.rs` is the sole production name-to-skin caller.
It currently maps `RuneTableSink.skin_name` to explicit Duotone and leaves absent
names as `TableSkin::DEFAULT`. Replacing the envelope carrier with the bounded
PRODUCT-0027 intent gives this one match three exhaustive cases:

```text
Default                         -> TableSkin::DEFAULT
Theme(name, PaletteForward)     -> explicit PaletteForward
Theme(name, Duotone)            -> explicit Duotone plus advice policy on failure
```

No editor handoff, message, `TableArtifact`, renderer, or lifecycle type needs
to change. If another production skin-selection site appears during the census,
halt and update the record rather than allowing two surface defaults.

### 0b. The existing chain parser can flatten order without a new grammar level

`parse_modified_table` already walks the complete postfix call chain into a
closed register before constructing `RuneTableSink`. Add one `duotone` register
slot, validate all slots after traversal, and construct the bounded intent once.
Both source orders therefore lower identically and no modifier node or source
order survives. `table().duotone()` fails because the collected slots cannot
construct `Theme { name, projection }`, not because an invalid normal-form value
was built and checked later.

The normal-form projection enum remains local to the envelope module. Importing
`gui::table_skin::TableSkinProjection` into the envelope triggers the halt.

### 0c. The diagnostic matrix is reachable through registered names

Use PRODUCT-0026's real public resolver rather than reconstructing failures:

```text
yellows        Duotone fails, PaletteForward succeeds
dark2_s1       Duotone fails, PaletteForward fails
unknown name   identity resolution fails before either projection exists
```

Factor the head policy around an injected resolver closure in ordinary tests.
The production closure is exactly `TableSkin::from_theme_name`; the assay logs
requested projections and returned results. This makes call count and order
observable without a mock palette, public constructor, copied color policy, or
change to PRODUCT-0026. Assert:

- ordinary PaletteForward success/failure and unknown identity call once;
- explicit-Duotone unknown identity calls once;
- explicit-Duotone unsuitability probes PaletteForward exactly once afterward;
- every probe path still returns the requested Duotone failure, never the
  alternative skin.

### 0d. The indivisible property is proved from Git history

The combined ADR/plan commit is the intended parent `P` of the implementation
commit `I`. Before implementation, record `P = git rev-parse HEAD` in the work
log and verify that its production tree has the PRODUCT-0026 state:

```text
envelope: no duotone register member
head:     named skin explicitly selects Duotone
```

After `I` lands, evidence inspects the commit object rather than the final tree:

```text
git rev-parse I^
git diff --name-status I^ I
git diff --unified=80 I^ I -- \
  crates/polariton/src/rune_relation_envelope.rs \
  crates/polariton/src/gui/state.rs
```

The parent must equal `P`. That single diff must add `.duotone()` recognition,
the bounded projection intent, and the head's explicit PaletteForward mapping.
Neither half may exist in `P`.

An evidence commit cannot record its own hash without a self-reference. The
sequence therefore names two documentation points:

```text
E = implementation evidence commit
C = documentation-only closure commit that records E
```

After `E` lands, capture its exact hash and evaluate
`git diff --name-only I..E`. The range must contain only the evidence document;
no production or test file may appear. Commit `C` then records `E`, the exact
command/result, accepts the ADR, and advances the corpus. The durable claim is
“at commit E, the immutable range I..E contained only documentation,” not a
command whose `HEAD` changes meaning as later records land. A final-tree
architecture assertion remains a useful reintroduction guard, but does not
discharge the historical gate.

### 0e. The installed fixture is intentionally ahead

`../uat/test9.rn` already contains `bar(...).skin("greens")`. PRODUCT-0008
therefore refuses the complete buffer before dispatch, as PRODUCT-0027 §8
records. Do not edit, parse around, or cite its stale window as surface evidence.
Use focused in-repo source strings for table projection and lifecycle gates.

## Stage 1: land the entire surface atomically

This is the only implementation-and-tests commit.

### 1a. Replace the envelope carrier by construction

Add a private/source-normal-form projection intent with exactly PaletteForward
and Duotone, and a `RuneTableSkinIntent` equivalent to:

```text
Default | Theme { name, projection }
```

Make `RuneTableSink` require that intent. Extend the table modifier parser with
zero-argument, duplicate-free `.duotone()`. Collect `skin` and `duotone` in any
order, validate the complete register, then construct exactly one intent.

Add contextual assays for direct/named positions, reversed order, compiled
default, invalid arity, missing identity, duplicates, malformed receivers,
unknown/cross-register methods, relation sugar, and calls outside proven view
positions. Type-oriented PRODUCT-0025 assertions may be updated to the new
normal form; behavioral expectations may change only where PRODUCT-0027
supersedes the named-skin default and register vocabulary.

### 1b. Flip the head in the same diff

In the same unstaged change and commit, exhaustively map the normal-form intent:

- `Default` to `TableSkin::DEFAULT`;
- themed PaletteForward to explicit `TableSkinProjection::PaletteForward`;
- themed Duotone to explicit `TableSkinProjection::Duotone` and the §5 advice
  policy.

Factor the resolver-policy helper from Stage 0c. Do not move the surface default
into `TableSkin`, add a one-argument helper, use `Default` on either projection
enum, or probe a second mechanism for ordinary PaletteForward.

Keep the existing Duotone factual string as the prefix of an advised failure.
For `yellows`, append a deterministic sentence stating that the default
PaletteForward projection succeeds and to remove `.duotone()`. For `dark2_s1`,
return the requested Duotone diagnostic with no alternative sentence. Unknown
identity under either spelling retains the existing unknown diagnostic and one
resolver call. Never turn a successful probe into output.

### 1c. Prove the complete surface and unchanged lifecycle

In the same commit, add all executable and architecture coverage:

- exact direct substrate equality for `terrain_rs10` in both modes;
- exact PaletteForward `yellows` completion and seven-ratio validity;
- truthful `yellows.duotone()` advice with no artifact;
- registered `dark2_s1` both-failure and no-suggestion behavior;
- resolver call sequence/count for every diagnostic branch;
- same-result sibling completion, stale prior artifact retention, and atomic
  frame/skin replacement;
- named and anonymous panel reuse with unchanged group layout;
- no dashboard and no panel-chrome change;
- the complete PRODUCT-0026 oracle/census and PRODUCT-0023/0025 regressions;
- architecture guards that keep the envelope independent of GUI types, the
  substrate API explicit/default-free, SQL/Markdown compiled-default paths
  unchanged, and `.skin` absent from every plot-mark register.

Run `git diff --check`, the focused PRODUCT-0023 through PRODUCT-0027 tests, and
the architecture suite before committing. Stage every production and test
change together and commit once:

Commit: `impl: make palette-forward the Rune table skin default`.

PRODUCT-0027 gates: 1–12.

## Stage 2: verify, pin the evaluation point, and close without touching code

Immediately record implementation commit `I`, verify `I^ == P`, and perform the
Stage 0d historical diff assay before another commit is made. Then run:

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo test -p libaryon
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```

Write `plans/product/impl/0027_the_palette_forward_table_skin_surface_evidence.md`
with the parent/implementation hashes, implementation-diff findings, diagnostic
call traces and strings, normal-form/register evidence, lifecycle results,
twelve-gate ledger, permitted-file census, and full verification. Commit that
file alone as evidence commit `E`:

Commit: `evidence: record the palette-forward table skin surface`.

Capture `E = git rev-parse HEAD`, run the exact immutable `I..E` file census,
and update the evidence document with `E` plus the command/result. Accept the
ADR and advance the corpus to v0.27 in documentation-only closure commit `C`.
Update the existing executable table-skin row to state
PaletteForward-by-absence and explicit `.duotone()`; do not add or accept a
plot-skin row and do not edit `../uat/test9.rn`.

After `I`, commits `E` and `C` may change only:

```text
plans/product/adr/0027_the_palette_forward_table_skin_surface.md
plans/product/impl/0027_the_palette_forward_table_skin_surface_evidence.md
plans/product/corpus/rune_workspace_corpus.md
```

Commit subject: `evidence: close the palette-forward table skin surface`.

Commit body: `Records I..E because E cannot contain its own hash.` This sentence
is required: it makes the extra closure commit self-explaining and warns future
history cleanup that squashing `C` into `E` destroys the recorded property.

## Gate ledger

| PRODUCT-0027 gate | stage |
|---|---|
| 1 | 1 |
| 2 | 1 |
| 3 | 1 |
| 4 | 1 |
| 5 | 1 |
| 6 | 1 |
| 7 | 1 |
| 8 | 1 |
| 9 | 1 |
| 10 | 1 |
| 11 | 1 |
| 12 | 1 |

Every gate lands in the one implementation commit. Stage 0 establishes how the
historical property will be observed; Stage 2 records evidence only and may not
repair code or tests.

## Permitted implementation files

```text
crates/polariton/src/rune_relation_envelope.rs
crates/polariton/src/gui/state.rs
crates/polariton/tests/architecture_test.rs
```

Colocated test modules in the two production files are permitted and must land
with their production changes. The ADR, plan, later evidence, and corpus are
documentation. Any need to edit `gui/table_skin.rs`, libaryon, GUI messages,
editor/table artifact or renderer code, plot files, configuration, execution,
protocol, placement, dashboard, or `../uat/test9.rn` triggers the halt rather
than expanding this list.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo test -p libaryon
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```
