# ADR 0028: optional categorical theme palettes

Status: proposed 2026-08-05. This record decides one thing: a resolved libaryon
theme may expose a second, optional, naturally ordered categorical palette
beside its existing syntax-oriented twenty-color palette. It preserves every
existing theme, syntax, table-skin, and modifier result; adds no plot projection,
capacity rule, diagnostic, or Rune surface; and reserves PRODUCT-0029 for stable
categorical plot positions.

## Context

Libaryon is the color-theme authority. `Theme::try_new(name)` owns one compact
identity language: canonical names and aliases plus `r`, `c`, `C`, and `sN`
modifiers. PRODUCT-0024 made each resolved theme retain the exact twenty-color
sequence assigned to `THEMED_CATEGORIES`, and PRODUCT-0025/0026 made tables
project that sequence into artifact skins.

That sequence is correct for syntax and tables. It is not necessarily the
natural data-palette sequence from which a curated theme originated.

`tab20c` demonstrates the distinction. Its declarations contain Matplotlib's
natural order `c01..c20`, grouped as four blues, four oranges, four greens, four
purples, and four dark accents. Its syntax projection deliberately distributes
those colors across token roles:

```text
Keyword  <- c01
String   <- c09
Comment  <- c20
Type     <- c02
Function <- c10
...
```

PRODUCT-0024 then collects values in `THEMED_CATEGORIES` order, so the existing
resolved palette begins `c01, c09, c20, c02, c10`. That is not corruption. Four
adjacent blue token roles would make poor syntax highlighting; the permutation
is an artifact-specific projection doing its job.

Nor can a natural categorical palette be recovered by deduplicating syntax
colors. `dark2` declares the eight ColorBrewer data colors as named values, then
adds an anonymous ninth neutral exclusively for `Whitespace`. Dedupe would
promote editor furniture into a data color and manufacture intent that the
declaration does not express.

Some curated families have canonical categorical answers: `tab20`, `tab20b`,
`tab20c`, `dark2`, `kelly`, and `prism`. Others such as `nord`, `dracula`, and
`gruvbox` are syntax themes with no accepted data-palette design. Requiring a
categorical palette from every curated family would turn this substrate record
into twenty-one unreviewed aesthetic decisions.

Continuous families are different. Their natural ordered palette is already the
twenty samples produced after clipping and before assignment to syntax roles;
the existing syntax-oriented palette and the new categorical palette therefore
contain the same values and order for those families.

The governing boundary is the one established by PRODUCT-0023 through
PRODUCT-0027: a lower layer reports measurable capability and values; an
artifact-owning head composes policy. Libaryon may report that a theme has no
categorical palette or that an available palette has length `m`. It does not
know a bar, geometry limit, domain cardinality, color identity, or user-facing
diagnostic.

## Decision

### 1. One theme identity may resolve two authoritative sequences

`Theme` retains its existing public `syntax_colors` and existing
`ResolvedThemePalette` unchanged. It additionally exposes:

```text
categorical_palette() -> Option<&ResolvedCategoricalPalette>
```

The two sequences are sibling projections of one resolved identity:

- `palette()` is the existing complete twenty-position sequence projected into
  syntax and consumed by table skins;
- `categorical_palette()` is the optional natural data-color sequence, in its
  declared order and natural length.

Neither sequence is reconstructed from the other. The historical generic name
`ResolvedThemePalette` and `Theme::palette()` remain source-compatible; this
record does not rename or reinterpret their accepted behavior.

### 2. Categorical availability is optional and independent of family

Every continuous family has a categorical palette containing its twenty
resolved samples. A curated family has one only when its `ThemeSpec` explicitly
declares a natural sequence.

The initial curated capability set is exactly:

| family | natural length |
|---|---:|
| `tab20` | 20 |
| `tab20b` | 20 |
| `tab20c` | 20 |
| `dark2` | 8 |
| `kelly` | 20 |
| `prism` | 20 |

Every other curated family returns `None`. `None` is not a theme-resolution
error and does not weaken syntax or table capability.

`ThemeFamily::Curated` does not imply categorical availability. Family
describes how syntax colors are produced; categorical capability is a separate
declared fact. No caller infers the latter from the former.

### 3. A resolved categorical palette is non-empty by construction

`ResolvedCategoricalPalette` is immutable, ordered, and structurally non-empty.
It exposes length, indexed observation, and ordered iteration without exposing
mutation or a public constructor that can create an empty value.

Its length is natural rather than padded to twenty. It contains no repeated
syntax-role assignments, background, foreground, body-text, whitespace, or
artifact-role label unless that color is deliberately part of the declared data
palette.

Libaryon does not call a short categorical palette insufficient. `dark2` having
eight colors is a fact. PRODUCT-0029 may combine that fact with a Polariton
geometry and domain to decide whether a requested plot is possible.

### 4. Curated declarations are explicit and single-source

For each capable curated family, the natural palette is declared directly in
source. Existing syntax fill functions consume the same color constants rather
than retaining a second copied list. `tab20c` therefore has one `c01..c20`
declaration used by both its natural sequence and its deliberate syntax-role
mapping. `dark2` has one eight-color declaration; its whitespace neutral remains
outside it.

No deduplication, token-order inversion, variable-name reflection, luminance
selection, hue clustering, or family-name special case reconstructs a natural
sequence at runtime.

### 5. Modifiers apply independently to each projection extent

`Theme::try_new` parses the identity and modifiers once. It then applies the
accepted operation to each available authoritative sequence:

- `r` reverses each sequence;
- `sN` rotates each sequence right by `N mod that sequence's length`;
- `c` and `C` retain their existing continuous sampling effect and therefore
  change both continuous projections identically;
- curated `c` and `C` remain the preserved no-op for both projections;
- an absent categorical palette remains absent under every modifier.

Length-dependent rotation is deliberate:

```text
dark2_s1   syntax rotates 20; categorical rotates 8
dark2_s20  syntax rotates 20 to identity; categorical rotates 8 by 4
terrain_s12 both projections rotate 20 by 12
```

`_s1` means rotate the sequence being projected by one position. It does not
promise one universal permutation over differently sized artifacts.

### 6. Existing resolution remains byte-identical

For every existing canonical name, alias, and modifier expression:

- `syntax_colors` is byte-identical to PRODUCT-0024's checked-in oracle;
- `Theme::palette()` is byte-identical and retains its exact equality with
  `THEMED_CATEGORIES` assignment;
- every PRODUCT-0025/0026 table-skin success, failure, role, alpha, ratio, and
  diagnostic is unchanged; and
- `Theme::try_new` accepts and refuses the same strings with the same existing
  error text.

The new optional palette has no production consumer in this record. Its
presence cannot change syntax highlighting, editor colors, or a table skin.

### 7. Libaryon reports facts, not plot policy

This record adds no categorical-capacity error. The complete libaryon result is:

```text
unknown identity                         -> existing Theme::try_new Err
known identity, no categorical palette   -> None
known identity, categorical palette      -> Some(non-empty sequence with len)
```

PRODUCT-0029 owns palette-position assignment, geometry and palette limits,
combined capacity facts, refusals, and diagnostics. Libaryon gains no `bar`,
`plot`, `geometry`, `domain`, `ColorJob`, `ColorIdentity`, or artifact-capacity
vocabulary.

## Acceptance gates

1. `ResolvedCategoricalPalette` cannot represent an empty sequence through any
   production constructor and exposes immutable length, indexed colors, and
   ordered iteration.
2. Every continuous canonical family resolves `Some` with exactly twenty colors.
   Exactly the six declared curated families resolve `Some` at the lengths in
   §2; every other curated family, including `nord`, resolves `None` while
   remaining a valid syntax theme and table skin.
3. Base `tab20c` exposes exactly `c01..c20` in Matplotlib order, while its
   existing `Theme::palette()` retains the distinct syntax-role order beginning
   `c01, c09, c20, c02, c10`.
4. Base `dark2` exposes exactly its eight named ColorBrewer colors in declared
   order and excludes the whitespace neutral. Its existing twenty-position
   palette and syntax assignments remain unchanged.
5. `tab20`, `tab20b`, `kelly`, and `prism` expose their complete declared
   natural sequences exactly, with their syntax-role projections unchanged.
6. For every categorically capable family, `_r` reverses the categorical
   sequence exactly. For a categorically absent curated family, `_r` remains
   absent while its existing syntax behavior remains unchanged.
7. `_s1` proves independent projection rotation. `dark2_s20` leaves the existing
   twenty-position palette unchanged and rotates the categorical sequence by
   four; `terrain_s12` rotates both equal twenty-position sequences by twelve.
8. Continuous `c` and `C` produce equal syntax-oriented and categorical
   sequences from the accepted clipped samples. Curated `c`/`C` remain no-ops
   on present or absent categorical capability and on existing syntax behavior.
9. PRODUCT-0024's complete syntax snapshot and palette/category equality suite,
   plus PRODUCT-0026's 8,480-expression table-skin oracle, pass unchanged.
10. Canonical names, aliases, case handling, modifier parsing/order, accepted
    expressions, refusal strings, `ThemeFamily`, and existing public accessors
    remain unchanged. No second modifier parser exists.
11. No Polariton production source, plot type, capacity rule, diagnostic, Rune
    surface, table policy, `UiPalette`, execution, transport, panel, placement,
    or dashboard behavior changes.

## Stop condition

Stop if implementation requires changing an existing syntax color, retained
palette position, table-skin outcome, name, alias, modifier parse, modifier
behavior, or error string; assigning categorical intent to an undeclared curated
family; inferring a natural sequence; padding or cycling a short palette;
changing `ThemeFamily`; adding artifact vocabulary or capacity policy to
libaryon; adding a plot consumer; or changing Rune, execution, transport,
panels, placement, dashboard, or table behavior.

The expected trigger is discovering that one of the six initial curated
sequences cannot be single-sourced without altering its syntax projection. In
that case work stops and the declaration is revised; byte parity is not traded
for convenience.

## Consequences

- The compact theme language can serve syntax, tables, and categorical plots
  through distinct authoritative projections without making any projection an
  inference from another.
- Canonical plotting palettes regain their natural order and length while
  deliberate syntax permutations remain intact.
- Syntax-only curated themes remain valid and acquire no unreviewed data-viz
  design by implication.
- Rotation is projection-local and length-dependent, a visible consequence of
  natural palette length rather than an inconsistency in name resolution.
- PRODUCT-0029 receives only measurable facts: optional colors and length. It
  owns every geometry, identity, capacity, and diagnostic decision.

## Rejected

- **Use `Theme::palette()` for categorical plots:** it is intentionally ordered
  for syntax roles and scrambles palettes such as `tab20c` for data use.
- **Redefine `Theme::palette()` as natural order:** breaks PRODUCT-0024's
  palette/category equation and every accepted table-skin projection.
- **Deduplicate syntax colors:** promotes syntax-only neutrals, loses declared
  order, and invents a palette for families that have none.
- **Require every curated family to declare a categorical palette:** turns a
  mechanical exposure record into twenty-one aesthetic design decisions.
- **Treat every curated family as categorical:** confuses construction family
  with an independently declared capability.
- **Pad short palettes to twenty:** manufactures capacity by repetition and
  hides the natural length PRODUCT-0029 needs to report honestly.
- **Apply shifts modulo twenty universally:** makes `dark2` behave as though it
  had twelve duplicate categorical positions and loses projection-local meaning.
- **Return an error for absent categorical capability:** makes a valid syntax
  theme fail during identity resolution for an artifact it was never asked to
  support.
- **Add capacity errors to libaryon:** geometry and domain cardinality are
  Polariton facts; libaryon reports only capability and length.

