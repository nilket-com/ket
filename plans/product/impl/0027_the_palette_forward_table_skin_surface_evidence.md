# PRODUCT-0027 implementation evidence

Recorded 2026-08-05 after the indivisible implementation commit and complete
cross-feature verification. Named Rune table skins now select PRODUCT-0026's
`PaletteForward` projection by absence; `.duotone()` preserves explicit access
to the accepted two-anchor treatment. Bare tables retain the compiled default.

## Historical acceptance assay

The plan and implementation anchors are immutable commit objects:

```text
P  7fec9c9377c663cba035980a6068a4414e15e96c
I  74d004720183fd37e96fcc8dfb1b82372b80e197
I^ 7fec9c9377c663cba035980a6068a4414e15e96c
```

Thus `I^ == P`. The parent contains no `"duotone" =>` table-register member and
no `RuneTableSkinProjection`. Its single production named-skin completion at
`gui/state.rs:810` explicitly calls
`TableSkin::from_theme_name(&name, TableSkinProjection::Duotone)`.

The exact `I^..I` file census is:

```text
M  crates/polariton/src/gui/state.rs
M  crates/polariton/src/rune_relation_envelope.rs
M  crates/polariton/tests/architecture_test.rs
```

That one diff introduces all of the following together:

- the zero-argument, duplicate-free `"duotone" =>` table-register member;
- the closed envelope projection intent and bounded named-theme carrier;
- PaletteForward for an unmodified named `.skin(name)`;
- Duotone for the same identity with `.duotone()`;
- the truthful, non-fallback advice policy and all executable coverage.

No flag, dead grammar, earlier production commit, or later code cleanup divides
the modifier from the default flip. The private carrier is named `NamedTheme`
rather than bare `Theme`: this keeps the implementation inside PRODUCT-0027's
permitted Polariton files and avoids colliding with PRODUCT-0024's architecture
guard for construction of libaryon's distinct `Theme` type.

The evidence commit cannot contain its own hash. The documentation-only closure
commit records the exact evidence hash and the immutable `I..E` file census.

## Normal form and register

The envelope normal form is equivalent to:

```text
RuneTableSkinIntent =
    Default
  | NamedTheme {
        name: String,
        projection: PaletteForward | Duotone,
    }
```

It contains no GUI substrate type, resolved palette, color, `TableSkin`, or
modifier-order list. The parser collects `skin` and `duotone` before constructing
the intent, so these accepted spellings flatten identically:

```rune
table().skin("terrain_rs10").duotone()
table().duotone().skin("terrain_rs10")
```

Direct and named-view positions are covered. Missing identity, duplicate
modifiers, nonzero arity, malformed receivers, unknown table methods, and
cross-register methods refuse at the outer sink span. Relation sugar is
unchanged, and the same calls remain ordinary Rune outside a proven view-sink
position.

## Resolution and diagnostic traces

An injected ordinary resolver assay observes the exact call sequence without a
mock palette or copied color policy:

```text
Default                         []
yellows PaletteForward          [(yellows, PaletteForward)]
yellows Duotone                 [(yellows, Duotone), (yellows, PaletteForward)]
terrain_rs10 Duotone            [(terrain_rs10, Duotone)]
unknown PaletteForward          [(unknown, PaletteForward)]
unknown Duotone                 [(unknown, Duotone)]
dark2_s1 PaletteForward         [(dark2_s1, PaletteForward)]
dark2_s1 Duotone                [(dark2_s1, Duotone), (dark2_s1, PaletteForward)]
```

The requested result remains authoritative in every branch:

- bare `table()` performs no theme lookup and equals `TableSkin::DEFAULT`;
- ordinary `yellows` equals direct PaletteForward resolution;
- explicit-Duotone `terrain_rs10` equals direct Duotone resolution;
- explicit-Duotone `yellows` keeps the complete failed-role diagnostic and adds
  `; the default palette-forward projection succeeds; remove .duotone()`;
- an unknown name is reported unchanged after one call and gets no projection
  advice;
- `dark2_s1.duotone()` reports Duotone facts after the failed PaletteForward
  probe, with no suggestion of the broken alternative;
- ordinary `dark2_s1` reports PaletteForward facts after one call and never
  probes or recommends Duotone.

No successful advice probe becomes content. A failed destination has no partial
artifact and retains any prior `TableArtifact { frame, skin }` stale.

## Lifecycle and compatibility evidence

Focused table-only source exercises the accepted surface without editing the
ahead-of-language `../uat/test9.rn`. It completes default, PaletteForward, and
Duotone table sinks over shared stamped results. A lifecycle assay then changes
the same view from explicit Duotone to implicit PaletteForward and proves:

- the same destination and producer group are reused;
- frame and skin replace atomically on success;
- a later unsuitable PaletteForward skin retains the prior pair stale;
- sibling destinations still complete independently over the same result;
- group geometry and dashboard state remain unchanged.

The full PRODUCT-0026 8,480-expression oracle and two-mode census remain green.
PRODUCT-0023 draw/contrast behavior and PRODUCT-0025 identity/register/lifecycle
coverage remain green except for the named-skin default and table-register word
that this record explicitly supersedes. Architecture coverage keeps the local
intent independent of GUI types, keeps the substrate API explicit, preserves
SQL and Markdown compiled-default paths, and confirms that no plot-mark register
accepts `skin` or `duotone`.

## Gate ledger

1. **Three exact meanings.** Bare tables equal the compiled default; named
   absence equals direct PaletteForward; explicit `.duotone()` equals direct
   Duotone.
2. **Positions and order.** Direct and named sinks accept canonical and reversed
   modifier order with identical normal form, identity, and skin.
3. **Closed contextual register.** Invalid combinations refuse atomically;
   outside positions and relation sugar retain their prior Rune meanings.
4. **Bounded carrier.** Only default or a name plus one local closed projection
   is representable; no substrate value or source-order chain crosses the
   envelope.
5. **PaletteForward `yellows`.** Completion equals PRODUCT-0026 exactly and all
   seven visible ratios remain at least `4.5:1`.
6. **Truthful Duotone repair.** `yellows.duotone()` fails without content and
   names the verified working removal.
7. **Unknown identity.** Either spelling performs one lookup, preserves the
   unknown-name diagnostic, and offers no projection advice.
8. **Both-failure branch.** `dark2_s1.duotone()` keeps Duotone facts without a
   false suggestion; ordinary `dark2_s1` keeps PaletteForward facts without a
   reverse probe.
9. **Identity and lifecycle.** Projection changes reuse placement and replace
   the frame/skin pair atomically; failure preserves the previous pair stale.
10. **Indivisible history.** The immutable `P`, `I^`, and `I` assay above proves
    the grammar, normal form, and default flip landed in one implementation
    commit.
11. **Substrate and prior surfaces.** PRODUCT-0026's oracle/census and the
    applicable PRODUCT-0023/0025 regressions are green.
12. **Unchanged systems.** Libaryon, SQL/Markdown defaults, rendering geometry,
    chrome, plots, dashboard, execution, transport, identity, and placement did
    not change; plot marks still refuse `skin`.

## Scope trace

Implementation changes are confined to the plan's three permitted files:

```text
crates/polariton/src/gui/state.rs
crates/polariton/src/rune_relation_envelope.rs
crates/polariton/tests/architecture_test.rs
```

No libaryon, table substrate, renderer, editor handoff, plot, protocol,
configuration, execution, placement, dashboard, or installed UAT file changed.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
318 passed; 5 live-service tests ignored; 27 architecture tests passed

cargo test -p libaryon
1424 passed; 22 ignored; 14 architecture tests and quit-latency test passed

cargo check -p polariton --no-default-features
passed (the existing feature-disabled styled-plot warnings remain)

cargo build -p polariton
passed

git diff --check
passed
```

One unrelated libaryon event-bus timing assay timed out during an intermediate
full run. Its isolated rerun passed, followed by the complete clean result
recorded above. All twelve PRODUCT-0027 gates are discharged at implementation
commit `I`; the closure commit records `E`, accepts the ADR, and advances the
corpus without touching code.
