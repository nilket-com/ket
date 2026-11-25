# ADR 0013: show Markdown math as Unicode text, lowered in the engine

Status: accepted 2026-11-25. Subjects: `plan: adr 0013 libaryon …`, `impl: adr 0013 libaryon …`.
Prompted by toron brainstorm 011
(`nilket/crates/toron/plans/brainstorms/011_boron_semantic_naming.md`), whose
`$\mathcal{B}_{\text{weak}} \ltimes_K \mathcal{B}_{\text{strong}}$` and `**$\infty$**` table cells
the previews showed as raw TeX. Worked out between Fable and Astra over chatd (conversations
`markdown-math-rendering` and `boron-math-preview`).

## Context

Both heads draw `svp::markdown_blocks`, and that parser ran pulldown-cmark without
`Options::ENABLE_MATH`. So `$…$` was ordinary text, with two effects:

- the preview showed the TeX source, dollars included;
- the interior was open to Markdown's inline rules, in the preview and in the source highlighter
  (`svp::markdown_semantic`, which built the same option set by hand): `_` and `*` inside a formula
  paired up as emphasis and backslash escapes were consumed.

The heads draw runs of styled text. Baryon rasterizes glyphs one at a time with fontdue, with no
shaping and (until now) from one face per run; Polariton hands spans to iced. Neither has a box
model for typeset math, and ADR 0012 sizes table columns by character counts.

## Decision

1. **One dialect, with math.** `markdown_blocks::markdown_parser` builds the parser for the block
   model and for the source highlighter, with `ENABLE_MATH` added to tables, task lists,
   strikethrough and GFM. A formula's interior reaches neither as Markdown.
2. **Math is lowered to one line of Unicode text in libaryon.** A new private module
   `svp::markdown_math` owns `render_math(tex) -> Option<String>`. The block parser appends the
   result as ordinary inline text in the surrounding style, for `$…$` and `$$…$$` alike. No new
   inline style, block type or head API: both heads, the table width choice and wrapping see text.
3. **The subset is what a line of text can say faithfully.**
   - Symbols, one for one: Greek, the usual operators, relations, arrows, quantifiers and
     delimiters, and upright function names (`\infty` → ∞, `\ltimes` → ⋉, `\to` → →, `\ker` → ker).
   - Alphabets: `\mathcal` (script), `\mathbb` (double-struck), `\mathbf` (bold), using the
     Letterlike Symbols characters where the mathematical block has holes (`\mathcal{B}` → ℬ,
     U+212C).
     A character the alphabet lacks stays as written.
   - `\text{…}`, `\textrm`, `\mathrm`, `\operatorname`: literal text.
   - An index (`_`, `^`) becomes Unicode sub/superscript characters only when every character of
     it has one; otherwise it stays linear, parenthesized when it is more than one token
     (`K_i` → Kᵢ, `x^2` → x², `\ltimes_K` → ⋉_K, `x_{i,j}` → x_(i,j)). Superscript letters stop at
     `i` and `n`; the other raised letters are phonetic modifiers few fonts carry. `^\dagger`, `'`
     and `^*` are written inline (a†, f′, A*).
   - Spacing commands and source whitespace become single spaces; `\left` / `\right` are dropped;
     `-` is U+2212.
4. **Anything else keeps the author's source.** `render_math` returns `None` for an unlisted
   command, `\frac`, `\sqrt`, accents, environments, `&`, `\\`, unbalanced braces or an empty
   result, and the block parser then emits the delimiters around the formula text the Markdown
   parser supplied (which table-cell escaping can alter, so it is not always the source bytes).
   Never a partial conversion.
5. **Nesting is bounded.** The lowering recurses once per group or argument and reads arbitrary
   files, so depth is capped at 16 through one function every recursive path uses; deeper input is
   unsupported (step 4).
6. **Baryon falls back per glyph.** For non-ASCII text the preview picks, per character, the first
   face that has the glyph: the run's own face, one installed math face (Noto Sans Math, Cambria
   Math, STIX Two Math, DejaVu Math TeX Gyre, Apple Symbols, DejaVu Sans, in that order), then the
   loaded sans, serif and mono regular faces. Measuring and drawing walk the same runs. Polariton
   needs no change: iced's shaping already falls back to system faces.

## Rationale

- Lowering in the engine is the same argument as ADR 0012: one choice both heads share, and no
  pixels or fonts in libaryon. Text output also keeps ADR 0012's character counts truthful, which a
  typeset box would not.
- Real typesetting needs a box model, math font metrics and shaping in two renderers. The notation
  in the plans is symbols, calligraphic letters and short indices, which Unicode already encodes.
- Refusing whole formulas rather than flattening: `a\frac{b}{c}d` flattened is `ab/cd`, which is a
  different formula. An accent was tried as base plus combining mark; Baryon's unshaped raster drew
  the macron of `\bar{P}` beside the letter, in another face. A reader can read TeX source; a
  reader cannot detect a wrong rendering.
- No single installed face covers the output (on the development host Noto Sans Math lacks ᵢ ₘ †,
  DejaVu Sans lacks 𝟏 and script letters other than the Letterlike ones, Arial lacks ℬ ⋉ ᵢ ₘ), which
  is why Baryon's fallback is a chain per glyph rather than one math face.
- Whitespace is kept, unlike TeX: authors space their operators, and a text line has no math
  spacing to put back.

## Consequences

- `$` now opens math under pulldown-cmark's rules (no whitespace just inside the delimiters).
  `$5 and $10` stays prose and `\$` is always literal; text such as `$x and y$` becomes a formula,
  and a digit may follow the closing dollar (`$x$2` is the formula `x`, then `2`). A probe of the
  tracked Markdown for `$…$` spans with no TeX marker found only real math (`$(B, K]$`, `$O(1)$`,
  `$k=200$`). `docs/baryon.md` documents the escape.
- Seven tracked Markdown files hold 305 math spans as the parser counts them. 298 lower,
  including all 32 in brainstorm 011; the seven that show as source are in wendelon brainstorm
  021: six `\bar{P}` and one `\stackrel{?}{=}`.
  `brainstorm_011_expressions_render` pins each distinct one, `other_plan_expressions_render` the
  notation of the others.
- Tests, libaryon: `the_unsupported_and_the_malformed_are_refused_whole`;
  `nesting_is_bounded_on_every_recursive_path` (each recursive shape renders at the bound and is
  refused one level past it and at 10,000 levels); `every_alphabet_letter_is_an_assigned_character`;
  `boron_math_renders_in_prose_and_bold_table_cells` and
  `math_preserves_literal_markdown_and_unknown_tex` in the block parser (code spans and fences
  untouched, source ranges unchanged); `math_markers_do_not_split_source_emphasis` in the
  highlighter.
- Glyphs depend on installed fonts. A host with no face holding a character draws that head's
  missing glyph; no math face is bundled. Polariton's bundled faces cover part of the output
  (Polariton Plot Sans supplies ℬ but lacks ∞ → ≤ ∪ ⋉ 𝟏; DejaVu Sans Mono lacks ℬ ⋉ 𝟏), and system
  fallback supplies the rest, such as ⋉.
- Not tested in the suite: Baryon's per-glyph fallback and Polariton's shaping, because both depend
  on the host's fonts. They were checked once on the development host with probes that were then
  removed: Baryon drew ℬ ⋉ 𝟏 from Noto Sans Math, ∞ − from Arial and the subscript letters from
  DejaVu Sans Mono; iced drew ℬ from Polariton Plot Sans, ⋉ ∞ from DejaVu Sans and 𝟏 from FreeSans.
- The source pane is unchanged apart from formulas no longer showing false emphasis; math gets no
  highlight category of its own.

Scope: `svp::markdown_math`, the shared parser options, Baryon's glyph fallback and the user doc.
Typeset layout (fractions, roots, accents, matrices), a math highlight category in the source pane,
bundling a math font and display math as its own block are out of scope.
