# PRODUCT-0025 implementation evidence

Recorded 2026-08-05 after three implementation commits and complete
cross-feature verification. Rune table sinks can now select any suitable
libaryon theme identity with `table().skin(name)`. Libaryon remains the only
authority for names, aliases, modifiers, and resolved palette order; Polariton
owns the validated projection from that palette into table-interior roles.

## Stage 0 findings and landed policy

The dogfood expression is `terrain_rs10`, not a normative special case. It
resolves through `Theme::try_new` as the ordinary terrain palette with reverse
and right-shift-by-ten modifiers. Its deterministic indexed contrast pair is
palette positions 9 and 10:

```text
darker anchor  #333399
lighter anchor #FFFFFF
contrast       10.124026087717501:1
```

The accepted projection contract is the four constraints in PRODUCT-0025 §2.
The following exact mapping is the landed, tunable Polariton policy—not Rune or
libaryon semantics:

```text
body background       darker anchor                         #333399
header background     8% p[2] into darker anchor            #2F3F97
gutter background     6% p[3] into darker anchor            #303B9A
even-row background   3% p[0] into darker anchor            #343898
odd-row background    7% p[1] into darker anchor            #313E96
gridline              p[4], alpha 0.25                      #00A9CE
cell text             lighter anchor                        #FFFFFF
header text           lighter anchor                        #FFFFFF
type text             first ordered 4.5:1 header match      #5DDE78
gutter text           lighter anchor                        #FFFFFF
selection fill        p[5], alpha 0.18                      #009BF6
scrollbar thumb       p[6], alpha 0.60                      #0A83E9
```

Encoded-sRGB interpolation is used for opaque tints. Interaction fills retain
their declared alpha. PRODUCT-0023's exact post-composite assay gives:

```text
cell / even row             9.699402088387815:1
cell / odd row              9.291850309460296:1
header / header background  9.215605551928507:1
type / header background    5.347313350977389:1
gutter / gutter background  9.468500117805823:1
cell / selected even row    7.929932506425018:1
cell / selected odd row     7.603379704659476:1
```

All seven exceed `4.5:1`. The production test assayed base, reverse, and
shift-by-one expressions for all fifty-three registered families: 117 of 159
project successfully and all seven ratios pass; 42 resolve as syntax themes but
refuse honestly as unsuitable table skins. The flat registered `black` palette
is the stable unsuitable control at `1.0:1`.

The normal form needed no color carrier. `RuneTableSink` holds only
`Option<String>`, and the existing per-sink completion boundary converts that
identity into `RuneRelationSinkOutput::Table(TableSkin)`. The existing
`TableArtifact { frame, skin }` and stale-on-failure behavior already provide
atomic retention.

## Gate ledger

1. **One theme authority.** `TableSkin::from_theme_name` calls only
   `Theme::try_new` and reads its `ResolvedThemePalette`. Architecture coverage
   rejects a Polariton catalog, suffix parser, `THEME_SPECS` production use,
   active `UiPalette`, or editor-theme coupling.
2. **Complete ordered projection.** Every one of the twelve required roles is
   assigned. `terrain_rs10` resolves through the mapping above, and role
   sentinels plus base/reverse/shift inequalities prove that ordered positions
   remain observable beside the contrast anchors.
3. **Validation is unavoidable.** Every successful registered projection runs
   all seven PRODUCT-0023 checks using the skin's declared selection alpha. The
   117-result census is non-vacuous and no success path bypasses validation.
4. **Diagnostics distinguish capability.** An unknown expression produces
   `UnknownTheme`; the resolved flat `black` palette produces
   `UnsuitableTheme` with name, weakest role, observed ratio, and threshold.
   Neither path falls back or changes libaryon theme validity.
5. **Permutation is visible.** `terrain`, `terrain_r`, and `terrain_s1` yield
   three distinct complete skins while preserving every contrast gate.
6. **Defaults remain exact.** Bare Rune tables carry `TableSkin::DEFAULT`.
   PRODUCT-0023's draw oracle remains unchanged, and architecture assertions
   keep SQL-result and Markdown-cell table construction explicitly defaulted.
7. **The envelope carries identity only.** Direct and named-view positions
   flatten to one `RuneTableSink` retaining the exact authored string. The
   production envelope contains neither `TableSkin` nor a resolved palette.
8. **The register is closed and atomic.** Invalid arity, nonliteral names,
   duplicates, unknown/cross-register methods, and malformed receivers all
   refuse the complete outer sink span.
9. **Recognition stays contextual.** The same call outside a proven view-sink
   position remains ordinary Rune, and relation `r.table()` sugar remains the
   unmodified default intent.
10. **Completion stays shared and per-sink.** Styled table, default table,
    unsuitable table, and plot outputs over one relation retain the same result
    `Arc`. Suitability failure marks only its destination stale and preserves
    the prior complete frame/skin pair.
11. **Skin is content, not placement.** A skin-only rerun reuses the same
    `HostPanelId`; the lifecycle assay preserves panel count and ordinary host
    placement and confirms that no dashboard is opened.
12. **The high-density fixture closes as one program.** The exact installed
    `test8.rn` source shape parses into two origins and completes its
    `terrain_rs10` table, headed layered line/scatter plot with both mark
    modifiers, default second table, emoji heading, and styled bar sink. The
    test supplies local frames rather than requiring a sibling checkout or live
    Toron service; the installed file remains the real-window aesthetic oracle.

## Commit and scope trace

- `8e251bfb` — PRODUCT-0025 record and implementation plan;
- `5a526200` — deterministic projection, diagnostics, and complete validation;
- `f8884673` — contextual closed table modifier register and identity-only
  normal form;
- `4ba40bb3` — resolved-skin completion, atomic lifecycle, authority guard, and
  exact high-density fixture assay.

Production changes stayed within the permitted Polariton table-skin, Rune
envelope, completion message/state, and editor handoff files. No libaryon,
table-geometry, SQL/Markdown authoring, plot, configuration, transport, panel
protocol, placement, or dashboard production path changed. The installed
`../uat/test8.rn` changed only its dogfood expression to `terrain_rs10`.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
310 passed; 5 live-service tests ignored; 25 architecture tests passed

cargo test -p libaryon
1424 passed; 22 ignored; 14 architecture tests and quit-latency test passed

cargo check -p polariton --no-default-features
passed (the existing feature-disabled styled-plot warnings remain)

cargo build -p polariton
passed

git diff --check
passed
```

All twelve PRODUCT-0025 gates are discharged. The mapping is semantically
accepted; whether `terrain_rs10` has the desired shared vibe in a real window is
deliberately left to the installed dogfood fixture rather than encoded as a
false automated aesthetic claim.
