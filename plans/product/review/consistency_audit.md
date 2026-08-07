# Consistency audit: TABLE artifacts vs PLOT CHROME vs PLOT DATA

Sources: `/home/me/work/ket/plans/product/adr/` — 0007, 0008, 0010–0040, all read in full.
Column key: **TABLE** = table artifact skins (0023–0027, 0035); **CHROME** = plot chrome, axes,
grid, ticks, bounds, skins (0031–0034, 0036–0039); **DATA** = marks, layers, modifiers,
categorical palettes (0013–0022, 0028–0030). 0040 (binding resolution) cuts across all three.

## Section 1 — Behavior matrix

### Q1. Invalid / unknown authored input

| Question | TABLE | CHROME | DATA |
|---|---|---|---|
| Unknown theme/skin name | Refuse, destination-local, previous frame+skin retained stale (0025 §3 `UnknownTheme`, §6–7; 0035 §3) | Refuse, destination-local (0032 §5 `UnknownTheme`; 0034 Decision: "Unknown theme identity … remain errors") | Refuse, destination-local (0030 §8: "unknown identity reports libaryon's existing name failure") |
| Valid theme, insufficient contrast | **Renders**, contrast is a measured inert fact (0035 §1, §4; supersedes 0026 §5–6, 0027 §5) | **Renders**, measured inert fact (0034 Decision; supersedes 0032 §5, 0033 gate 6) | No contrast concept; "valid but ugly renders" (0030 §4, gate 8) |
| Malformed selector syntax (arity, non-literal, duplicate, unknown modifier) | Lowering-time refusal at outer sink span, atomic (0025 §4–5; 0027 §2–3) | Lowering-time refusal at own span (0032 §1; 0036 §4; 0038 §3; 0039 §6) | Lowering-time refusal at modified mark, atomic chain (0018 §2–4; 0020 §2; 0022 §2) |
| Scope of an envelope (lowering-time) refusal | Whole view program refuses pre-dispatch, last good panels stale (0008 §3 "partial lowering is prohibited"; 0027 §8 test9.rn) | Same (0008 §3) | Same (0008 §3; 0018 §2 "no partial view survives") |
| Scope of a resolution-time failure | Only that destination; siblings over the same origin complete (0025 §6) | Only that destination (0032 §9) | Per-sink, sibling views render (0007 §2; 0030 §8) |
| Out-of-range numeric value | n/a (string selector only) | Refuse at lowering, never clamp: bounds non-finite/min>=max (0036 §4), step ≤ 0 (0038 §3), minor count negative/fractional (0039 §3) | Refuse at lowering, never clamp: point_size 0.25..=8.0 (0017 §1, 0018 gate 3), stroke_width (0019 §1), bar_width (0,1] (0021 §1, 0022 gate 4) |
| Negative-literal diagnostic phase | n/a | Grammar accepts one unary negation, **value** refuses, diagnostic names the value not the grammar (0038 §2; 0039 §3) | **Unspecified** — 0018 §3 says the diagnostic "reports the accepted domain," but whether unary negation is parsed-then-value-refused or refused at grammar is not stated |
| Clamping anywhere | Never (0026 §4 "no … clamping"; 0035 §1) | Never (0034; 0036 §6 prohibits minimum-span ε; 0037 rejects truncating/thinning) | Never (0018 §3 "neither clamps nor interprets") |
| Capacity exhaustion | n/a (twelve fixed roles) | Tick lattice exceeding declared capacity refuses, attributed (0037 §6; 0039 §5 total majors+minors) | Categorical capacity (geometry and/or palette length) refuses, atomic, both facts reported (0007 §8; 0029 §5) |

### Q2. Valid-but-inapplicable request

| Question | TABLE | CHROME | DATA |
|---|---|---|---|
| Setting that cannot apply to the current body/axis | n/a — a table skin always has a table to dress | **Refuses, per axis, "never silently dropped"** — `x_bounds` on categorical x (0036 §7), `Step` on categorical/band/temporal (0037 §7), minor count likewise (0039 §6) | **Silent no-op** — `palette(...)` on a figure with no categorical consumer "is not a failure and produces no warning" (0030 §4, §8) |
| Categorical palette requested on a theme that lacks one | n/a | n/a | Refuse: distinct capability refusal, not unknown-name and not capacity (0029 §2, §5; 0030 §8; 0035 §3) |
| ...but on a figure with no categorical consumer | — | — | **Unspecified**: 0030 §4 admits an unused palette member; 0030 §8 makes missing capability a resolution failure; no record says which wins for `palette("nord")` over a line-only body |
| Inapplicable theme modifier (`c`/`C` on a curated family) | Silent no-op, inherited (0024 §3) | Silent no-op, inherited (0024 §3) | Silent no-op, inherited (0024 §3; 0028 §5) — explicitly "observed and preserved, not accepted as correct" |

### Q3 / Q8. Theme projection; reactive vs frozen after resolution

| Question | TABLE | CHROME | DATA |
|---|---|---|---|
| Who resolves the theme name | `Theme::try_new` only, at sink completion (0025 §1, §6) | `Theme::try_new` only, head-side before flattening (0032 §2, §4) | `Theme::try_new` + `categorical_palette()` only, at domain resolution (0029 §2; 0030 §2) |
| Editor `:set langtheme` consulted | Never (0025 §1) | Never (0032 §2 — omission is `Inherit`, no secret name) | Never (0030 §2–3) |
| Participates in PRODUCT-0007 §7 five-tier cascade | **No** — binary: `TableSkin::DEFAULT` or authored name (0025 §6; 0027 §1); no application or pane tier exists | Yes: library compatibility → application chrome → authored skin → pane override (0032 §2); pane controls preserved (0031 §6) | Yes: library `viridis` → application → authored figure → pane override (0030 §3); mark settings use all five tiers (0017 §2–3; 0019 §2; 0021 §2) |
| What can restyle an already-completed artifact | Nothing but a successful rerun: skin baked into `TableArtifact{frame,skin}` atomically (0023 §5; 0025 §7) | Pane override restyles live — background/decoration edits are grouped pane overrides (0031 §6); completed plot retains kernel+settings+chrome together across replacement/stale (0031 §8) | Pane override restyles live without data preparation or kernel reconstruction (0017 §5); exact data colors frozen inside `ResolvedPlotSpec` (0029 §1) — settings-reactive, color-identity frozen |
| Frozen at completion (stale retention pairing) | frame + skin as one artifact (0023 §5) | kernel + settings + chrome as one completion (0031 §8); canvas heading atomic with kernel (0016 §9) | resolved colors inside the spec; pinned plots self-contained (0007 §4, §8; 0029 §1) |
| Modifier (`_r`, `_sN`) observability | Observable in **both** projections: PaletteForward (0026 §8) and Duotone (0024 §7 "must prove representative `_r` and `_s1` names produce distinct table skins"; 0025 §2 constraint 2, gate 5) | Ordinary skin: observable (0033, gate 3–4). **`.duotone()`: intentionally collapses** — `tab20c_s5`/`_s8` duotone forms identical (0033 gate 3; 0032 §4) | Always observable; `_s1` proves authority delegation (0029 §7; 0030 gate 6) |

### Q4. Order sensitivity

| Question | TABLE | CHROME | DATA |
|---|---|---|---|
| Duplicate modifier/element | Refuses; never last-wins (0025 §4) | Refuses (0016 §4; 0030 gate 1; 0032 §1; 0036 §2; 0038 §2; 0039 §3) | Refuses; "duplicate refusal … rather than selecting by chain order" (0018 §4, gate 5) |
| Modifier/metadata order | Register order irrelevant: `table().duotone().skin(n)` ≡ `table().skin(n).duotone()` (0027 §2) | Figure metadata order carries no meaning (0016 §2; 0030 §1; 0032 §1) — **except** `.duotone()` is "valid only immediately after a direct figure `skin(string_literal)` member" (0033) | Modifier order semantically irrelevant, chain flattens (0018 §4; 0022 §2) |
| Order that IS meaningful | none | none | Layer list order = paint order, authored, never normalized (0013 §1, §4; 0014 §6) — by design, not a modifier rule |
| Cascade tiers | n/a (no cascade) | Right-biased selection, later tier wins, never multiplies (0017 §2; 0036 Context) | Same (0017 §2; 0019 §2; 0021 §2) |

### Q5. Clamping vs refusal for numeric parameters — see Q1 rows; uniformly refusal, no clamping, no ε thresholds (0036 §6 prohibits; 0037 §6 distinguishes capacity from value threshold). Consistent.

### Q6. Naming / selector grammar

| Question | TABLE | CHROME | DATA |
|---|---|---|---|
| Theme-expression grammar (`greys_rCs2` etc.) | One grammar, libaryon-owned, no Polariton reparse (0025 §1; 0026 §1) | Same (0032 §4) | Same (0029 §2; 0030 §2) |
| Spelling of the selector | Postfix on the sink: `table().skin(name)` (+`.duotone()`) (0025 §5; 0027 §2) | Figure member: `skin(name)` (+postfix `.duotone()` per 0033) (0032 §1) | Figure member: `palette(name)` (0030 §1); mark styling via closed postfix registers (0018 §1) |
| Word reuse across receivers | `skin` = artifact interior treatment (0025) | `skin` = artifact chrome dress (0032 §1); `palette` never chrome (0032 §7) | `palette` = data-color sequence only (0030 §1; 0032 §7) |
| Bound-name (`let s = "nord"`) accepted as selector | **Not covered** by 0040 §6 (register modifier, not a figure element); not named as a gap either | Yes — `skin`, `palette`, bounds, steps, minors all enumerated (0040 §6) | Figure `palette` yes; **mark arguments not covered**, explicitly named a coverage limit (0040 §6) |

### Q7. Defaults

| Question | TABLE | CHROME | DATA |
|---|---|---|---|
| Authored-absent default | `TableSkin::DEFAULT`, exact extraction of legacy constants (0023 §2; 0027 §1) | `PlotChrome::DEFAULT`, exact compatibility extraction; omission deliberately not a named theme "because naming an implicit skin here would change the legacy gridline" (0031 §2; 0032 §2) | Explicit libaryon `viridis`; **deliberately supersedes** legacy twelve-color behavior and gives bare bars band fills (0030 §3) |
| Named-skin absent-projection default | PaletteForward (0027 §1; flip atomic with `.duotone()`, 0027 §6) | Ordered (order-sensitive) per 0033, correcting 0032 §3's set-based default | n/a — one projection (natural categorical sequence, 0029) |
| Must Polariton-chosen default pass its contrast checks | **Yes** — seven checks at 4.5:1 remain binding on `TableSkin::DEFAULT` "and any future colors Polariton selects when the author selects none" (0023 §3; 0035 §2, gate 4) | **No** — default gridline ≈1.66:1 fails the 3:1 criterion and ships, "measured, not falsely certified" (0031 §4); 0034 Context calls this "the right precedent" | **Unspecified** — default `viridis` data colors carry no stated legibility obligation; data-vs-chrome contrast validation is rejected for authored pairs (0032 §6) but the default case is never addressed |

## Section 2 — Candidate seams, ranked

**S1. Inapplicable figure setting: silent no-op (DATA) vs mandatory refusal (CHROME).**
- Rule A: an authored `palette(name)` on a figure whose body has no categorical consumer "is not a failure and produces no warning"; the setting is silently unused (0030 §4, §8, gate 9).
- Rule B: an authored `x_bounds`/`x_tick_step`/`x_minor_ticks` on an axis that cannot accept it must refuse, per axis, and "a bound that cannot apply is never silently dropped. A silently ignored setting is the one failure mode the rope principle does not cover" (0036 §7; 0037 §7; 0039 §6).
- Why a seam: both are figure-wide members in the same metadata group answering "what happens when my setting has no effect on this body?", and 0036 §7 states its never-silent principle in general terms without reconciling 0030's inert palette; 0030 never cites the bounds posture. A reconciliation exists (impossible-request vs defined-but-unused) but no record draws that line. Corollary unspecified cell: `palette("nord")` (no categorical capability) on a consumer-less body — refusal (0030 §8) or inert (0030 §4)?

**S2. Post-completion restyling: plots are pane-override-reactive; tables are frozen.**
- Rule A: a pane-override change restyles a completed live plot without data preparation or kernel reconstruction (0017 §5), and the plot settings pane's two color controls remain live grouped pane overrides (0031 §6).
- Rule B: a table's skin is installed atomically inside `TableArtifact{frame, skin}` and only a successful rerun can replace it; nothing else may restyle a completed table (0023 §5; 0025 §7).
- Why a seam: the same question — "what may change an already-completed artifact's presentation?" — gets opposite answers, and no record acknowledges the difference or says tables should (or deliberately should not) have a pane tier.

**S3. Cascade participation: table skins sit entirely outside PRODUCT-0007 §7's five-tier settings model.**
- Plot chrome cascades library → application → authored → pane (0032 §2); the data palette cascades identically (0030 §3); mark settings use all five tiers (0017 §2–3).
- Table skins are a two-state model — `DEFAULT` or one authored name (0025 §6; 0027 §1) — with no library/application/pane tiers and no `Override<…>` slot.
- Why a seam: PRODUCT-0007 §7 declares one precedence order for presentation settings; two artifact kinds adopt it and the third silently doesn't, with no record justifying the exclusion (0023/0025 simply never mention the cascade). This is the structural cause of half of S2.

**S4. Contrast obligation on Polariton-chosen defaults diverges three ways.**
- TABLE: the default and "any future colors Polariton selects when the author selects none" must pass all seven 4.5:1 checks — "`table()` gets our judgment and must be legible" (0035 §2; 0023 §3, gate 3).
- CHROME: `PlotChrome::DEFAULT`'s gridline measures ≈1.66:1 against a 3:1 criterion and ships anyway (0031 §4); 0034 Context promotes this from documented debt ("gridline contrast debt", 0031 Consequences) to "the right precedent."
- DATA: default `viridis` colors carry no stated obligation at all (unspecified).
- Why a seam: 0031 justifies its own failing default locally (exact pixel-parity extraction; fixing it "needs its own record"), but 0035 §2 — written later, explicitly as "the table half" of a cross-artifact correction — states the who-chose rule generally without carving out the plot default, and 0034 treats the same fact as precedent rather than debt. Two records rely on the same fact with opposite valences and neither reconciles the table-side obligation with the chrome-side exemption.

**S5 — DISSOLVED at the 2026-08-06 review; moved to Section 3, item 16.**

**S6. `.duotone()` attachment and order rules differ across registers.**
- TABLE: register modifier, order-irrelevant — `table().duotone().skin("terrain_rs10")` lowers identically to the canonical order (0027 §2, "accepted evidence that the postfix chain is a flattened register rather than an execution pipeline").
- PLOT: "valid only immediately after a direct figure `skin(string_literal)` member" (0033) — position-fixed, inside a metadata group whose order otherwise "has no meaning" (0032 §1).
- Why a seam: the table record made order-irrelevance an explicit design statement; the plot record imposes positional attachment for the same word without acknowledging the divergence. (Weaker than S5: the receivers differ structurally — `duotone` attaches to `skin(...)` itself on plots — but no record says so.)
- Sharper form of the same seam: 0030 §1 fixes the language's attachment points as exactly three — figure member, mark postfix, table postfix — with a structural rule for which properties go where. 0033's `skin(name).duotone()` is a postfix **on a figure member**: a fourth grammatical position the attachment-point table does not name and no record amends. Either the table needs a fourth row or `.duotone()` needs restating as part of the `skin` member's own grammar.

**S7. Static literal binding coverage skips the table register without naming it.**
- 0040 §1 states the rule over "any recognized presentation position"; §6 implements it for the ten figure elements and names mark arguments as the only coverage limit. `table().skin(name)` — a recognized presentation position taking a string literal (0025 §5) — is in neither list, so `let s = "nord"; table().skin(s)` refuses while `figure([skin(s), …])` resolves.
- Why a (weak) seam: the general rule and the "coverage, not meaning" framing arguably cover it, but the only named example of the boundary is mark arguments; a reader of 0025+0040 cannot tell whether the table selector's exclusion is coverage or decision. Mark-modifier numeric arguments (`.point_size(sz)`) share the same silence.

**S8 (minor, factual). 0036's numeric-literal claim contradicts the modifier records.**
- 0036 Context: "the envelope has no numeric-literal precedent at all — every field it recognizes today is a string." But the envelope has recognized numeric-literal modifier arguments since 0018 §3 ("numeric-literal decoding", accepted 2026-08-04), 0020 §2, and 0022 §1 — all predating 0036 (2026-08-06). Either "field" quietly means figure-element field, or the claim is false; nothing disambiguates. A record-accuracy inconsistency rather than a behavior seam, listed for completeness.

### Record-accuracy findings (text defects, not behavior seams)

**R1. 0035's supersession list is textually incomplete.** 0035 supersedes 0026
§5–§6 and gates 6–7, and 0027 §5 and gates 6/8, "only where they make contrast
an admission condition" — but 0026 **§4** also contains admission language ("if
no completed projection meets `4.5:1`, palette-forward refuses rather than
changing palette positions, clamping, inventing a third ink, or falling back")
and is not named. 0035's gates (1–3) behaviorally cover the case, so the intent
is unambiguous; the supersession section is simply missing a citation.

**R2. 0030's status line still reads "proposed 2026-08-05."** Its evidence file
records the indivisible production commit as landed 2026-08-05 with gate 16
(the real-window fidelity observation) deliberately held open, and 0032, 0036,
and 0038 all build on it as landed. Either the status is stale, or the arc has
one record in a shipped-but-not-accepted state that no other record's status
vocabulary has — worth an explicit decision either way.

### Unspecified cells worth flagging (findings, not seams)
1. `palette(name)` naming a categorical-less theme on a body with no categorical consumer: refuse vs inert (0030 §4 vs §8).
2. Legibility obligation for the Polariton-chosen default data palette (S4, third column).
3. Whether mark-modifier grammar accepts unary negation and where its value refusal phases (0018 §3 silent on the 0038 §2 grammar/value split).
4. Whether an application-theme tier could ever supply a table skin (no record opens or closes it; see S3).

## Section 3 — Acknowledged asymmetries (checked and set aside)

1. **Omitted-skin default (CHROME) vs explicit `viridis` default (DATA).** 0032 §2: "deliberately different from PRODUCT-0030's explicit `viridis` categorical library default, because naming an implicit skin here would change the legacy gridline and other default pixels."
2. **One plot-chrome projection vs two table projections.** 0032 Context + Rejected: plots lack the broad neutral surfaces that justified table PaletteForward; partially superseded by 0033, which documents its own correction and supersession scope.
3. **Chrome's many-to-one under set-preserving modifiers vs order-observable table PaletteForward / data palettes.** 0032 §4 states and justifies it explicitly, including the shared-figure `viridis_r`→`viridis_s5` co-occurrence example. (The *Duotone-specific* residue is S5.)
4. **`.views([])` vs `layers([])` refusing for different reasons.** 0014 §1 explicitly: "but **not** for PRODUCT-0008 §3's reason."
5. **Curated `c`/`C` silent no-op.** 0024 §3: "observed and preserved, not accepted as correct"; 0028 §5 carries it forward with the same flag.
6. **`skin` as table postfix vs figure member; `palette` vs `skin` receiver split.** 0030 §1 (three attachment points table, structural rule) and 0032 §1/§7 justify the word placement and disjointness.
7. **Bar is styleable but not layer-composable.** 0021 §5 and 0022 Context/Rejected state and defend the distinction.
8. **Major ticks authored by step, minor ticks by count.** 0039 §2: "That inversion is structural, not a reversal," with the invariant argument spelled out.
9. **Capacity limit vs value threshold.** 0037 §6 explicitly distinguishes itself from 0036 §6's ε prohibition; 0039 §5 re-cites the same rule.
10. **Length-dependent `sN` rotation (`dark2_s1` rotates 8 while syntax rotates 20).** 0028 §5: "a visible consequence of natural palette length rather than an inconsistency in name resolution."
11. **Atomic `.views` recognition stricter than PRODUCT-0003 free-function retention.** 0008 §3: "The difference is evidence, not appetite."
12. **Chrome label source-current vs canvas completion-atomic.** 0016 §7 states the split and its reason ("chrome labels the current program's view … the canvas labels the plot that is actually drawn").
13. **Duotone and PaletteForward failure payloads differ by mechanism.** 0026 §5 rejects "one generic suitability payload" explicitly.
14. **Bar default geometry change (72%→90%).** 0021 §1 acknowledges and justifies abandoning legacy pixels for an honest scale — the deliberate counter-example to the extraction-parity defaults of 0023/0031.
15. **The calibration seam itself** — table contrast-admission refusal vs plot render-and-measure — acknowledged and repaired by 0035 (Context table, Supersession section).
16. **`.duotone()` order semantics (formerly S5).** Dissolved at the 2026-08-06 review: the rule is uniform — *order is consumed wherever the role set can spend it*. Table Duotone keeps `_r`/`_sN` observable through its **accent roles** ("the same ordered positions, encoded-sRGB tint weights, and explicit alphas populate the same twelve roles," 0026 §2), while its paper/ink anchors are set-based. Plot chrome's eleven roles have no accent vocabulary — everything derives from paper, ink, and alpha hierarchy (0032 §3) — so under the set-based `.duotone()` pair there is nothing left for order to act on. Same rule, different role sets; an unstated mechanism difference, not an inconsistency.

## Section 4 — Review resolutions (2026-08-06)

Positions agreed at the review. These are meeting outcomes awaiting their
records; none is yet normative except the two record-accuracy fixes, which were
applied directly.

- **S1 → documentation seam; behavior stands.** The reconciling principle: *a
  setting that names a structural target refuses when that target cannot accept
  it (`x_bounds` names the x axis); a setting that supplies a vocabulary to
  whatever consumers exist is inert when there are none (`palette` names no
  target).* 0040 makes inertness load-bearing: a shared `let` config block
  spanning bar and line figures requires the palette member to be inert on the
  line body. The unspecified cell resolves as a **two-way split on 0029 §2's
  existing taxonomy**: unknown identity refuses even with no consumer (a typo
  is undefined vocabulary, not valid-but-unused); absent categorical capability
  (`nord`) is consumer-dependent and inert until consumed. Caution for the
  record: this is *not* the 0038 §3 lowering/resolution split — theme identity
  resolves at completion (0025 §1), so both checks are resolution-time; the
  distinction is "when the expression is read" versus "when a consumer asks," a
  sub-phase within resolution.
- **S2+S3 → the one genuine capability gap; fix agreed.** Table skins join
  PRODUCT-0007 §7's cascade: `Override<TableSkinSpec>` in the settings layer,
  authored tier populated by `.skin(name)`, application and pane tiers
  available (staged like 0030 — library + authored producers first, no pane UI
  required to close the seam). Portability survives: 0025 §1's never-consult-
  `:set langtheme` stands; an application tier is Polariton's own default,
  exactly `PlotThemeSpec`'s role for plots. The record **must supersede 0023
  §1's "ready to draw without consulting anything" sentence explicitly** —
  bake the authored-tier resolution into `TableArtifact`, flatten pane
  overrides at draw (0017 §4's shape) — or the atomicity language survives
  verbatim while meaning something different.
- **S4 → honor the obligation; the data column is documented, not closed.**
  Polariton-chosen defaults carry the contrast obligation (0035 §2 stated it
  generally); the 1.66:1 gridline is named extraction-parity debt with a
  repair record owed, and 0034's "right precedent" framing is retired — right
  decision, wrong promotion. The candidate criterion for default data colors
  (band fill vs default canvas at 3:1 non-text) was **measured before
  adoption and fails**: default `viridis` even-spread position 0 (`#440154`)
  is 1.378:1 and position 4 (`#3C4582`) is 2.366:1 against the black default
  canvas; the fresh spread always includes position 0, and a single-category
  domain receives only it. Adopting the criterion would author a rule already
  broken, so the S4 record records the measurement as fact and leaves the
  column measured-but-ungated.
- **S6 → repair by restatement, not a fourth row.** `.duotone()` is restated
  as part of the `skin` member's own grammar — a projection qualifier on a
  member — preserving 0030 §1's three attachment points as the load-bearing
  teachable simplification. The word keeps its two grammatical realizations
  (table register member, skin-member qualifier), now deliberately.
- **S7, S8, R1, R2 → applied 2026-08-06** directly to 0040 §6, 0036 Context,
  0035 Supersession, and 0030's status line (R2 taken as *accepted, with gate
  16 open and named*, with the status vocabulary defined in place).
- **Deferred ledger → full triage** into will do / will do if asked / closed
  won't-do (see `deferred_triage.md`), with two structural requirements: the
  Guide cluster and the persistence cluster triage as one decision each, and
  **every closure lands back in the records** — a one-line Rejected-style
  reason, plus a note in the deferring record's Consequences — so the ledger
  never becomes a second source of truth.
- **Facets** stay untouched until the triage says how much room they have; the
  only item whose answer changes what the other deferrals are attached to.
