# Markdown structural topology

Scope: the Markdown provider gains `parse_structural_topology`, provider-kind
names, and trivia classification, so `:uast` cursor-follow works on prose at
section/block granularity. Localized provider work — **but the graft's
byte-partition invariant is load-bearing**, so coverage proofs come first.
Deliverable is one `impl:` commit reviewed against this plan. The pattern
established here is the template for json/html/bash later (out of scope).

## Verified surface (2026-07-29)

`svp/markdown_blocks.rs` already produces range-aware blocks for the raster
preview: `parse_markdown_blocks(&str) -> Vec<MarkdownBlock>`, where every
variant (`Heading{level}`, `Paragraph`, `CodeBlock{info}`, `Quote`,
`ListItem{depth, marker, checked}`, `Rule`, `Table`, `Html`) carries
`source_range: Range<usize>` in bytes. The provider currently inherits the
default `parse_structural_topology() -> None`, so documents remain one
physical token and the inspector honestly reports
`provider topology unavailable`.

The block list is source-ordered but is not necessarily a flat interval
partition. Container constructs can produce laminar ranges: a nested list item
or block inside a quote may be wholly contained by its parent block. V1
deliberately exposes top-level blocks only, so lowering keeps the maximal range
and suppresses contained ranges. A partial/crossing overlap is invalid parser
output and takes the safe flat fallback described below.

## Lowering: two passes

### Pass 1 — byte-exact coverage (the load-bearing one)

Produce a flat, ordered `CoveredUnit { range, provider_kind }` stream that
**partitions `[0..len)` exactly**:

- Validate every parser range as an in-bounds UTF-8 byte interval. Sort
  deterministically by `(start, Reverse(end), original_index)`, retain maximal
  ranges, and suppress only ranges wholly contained by a retained parent.
  Equal ranges keep the first parser block deterministically. Never clamp a
  malformed range: a partial/crossing overlap or out-of-bounds interval takes
  a byte-exact `DOCUMENT → CONTENT` flat fallback and gets a regression test.
- Emit a **gap trivia token** for every byte not inside a block range: blank
  lines between blocks, inter-list spacing, leading whitespace the parser
  excluded, and the trailing gap after the last block (including
  no-trailing-newline files). Empty documents have an empty `DOCUMENT`; a
  non-empty document always has at least one covered unit.
- Known hazards, each with a dedicated fixture: CRLF line endings (`\r` must
  land inside some token), fence terminators (closing ``` line — verify the
  parser includes it in `CodeBlock.source_range`; if not, the gap token
  absorbs it), unterminated fences at EOF, Unicode multi-byte content
  (offsets are already bytes — fixtures with emoji/CJK prove no `char`-count
  arithmetic sneaks in), an empty document, a document of only blank lines,
  and a BOM if the pipeline can deliver one.

### Pass 2 — heading-derived section hierarchy

Fold the covered stream into a tree via a heading stack:

- A `DOCUMENT` root (`RelationalTable`). Preamble blocks before the first
  heading nest directly under it.
- `H1`–`H6` open **section** nodes (`RelationalTable`, provider kind = the
  heading level); an `Hn` closes every open section of level ≥ n. The
  heading's own line is the section's first child block. All following
  blocks nest under the innermost open section — this is what turns
  cursor-follow into a prose breadcrumb (`DOCUMENT → H2 Decision →
  LIST_ITEM`).
- Blocks are `RelationalRow` nodes, each containing one `Token` child
  carrying its bytes (block-granularity v1 — inline structure is a non-goal).
  Gap trivia rows attach to the section active when the gap begins. Therefore
  whitespace immediately before a heading belongs to the closing section (or
  document preamble), while whitespace after the heading line belongs to the
  newly opened section. This “closing side” rule is deterministic and keeps a
  section's first byte equal to its heading's first byte.
- Section node byte extents are the sum of their children by construction —
  never computed independently, so pass-2 cannot break pass-1's partition.

### Provider kinds and trivia

Per-provider u16 namespace (established stage-3 pattern): `DOCUMENT`,
`H1`..`H6`, `HEADING_LINE`, `PARAGRAPH`, `CODE_BLOCK`, `QUOTE`, `LIST_ITEM`,
`RULE`, `TABLE`, `HTML`, `GAP`, `CONTENT`. `CONTENT` is used only by the safe
flat fallback for malformed parser intervals. `structural_kind_is_trivia`
returns true for `GAP` only, so semantic mode hides inter-block whitespace
while raw mode shows the honest partition. `structural_kind_name` roundtrips
all of them (bounds-checked, per the stage-4-era Rust-provider precedent).

## Ordering (Codex amendment — property fixtures before any e2e)

1. **The lowering lands as a pure function with its coverage property.**
   Two checkers are required because `GreenNode` discards absolute offsets:
   an interval checker proves the ordered `CoveredUnit` ranges tile
   `[0..len)` exactly once before conversion, and a Green-tree checker proves
   every interior `byte_len` equals the sum of its children afterward. Merely
   walking Green lengths cannot detect reordered source intervals. Run both
   against every hazard fixture above; the block-type zoo (nested lists,
   tables, quotes containing fences, html blocks); and a property-style
   generator composing random sequences of fragment templates (headings of
   random level, paragraphs, fences with/without terminators, blank-line
   runs, CRLF/LF mixes) — hundreds of composed inputs, seeded and
   reproducible on failure, without adding a production dependency. **No
   provider registration until this suite is green.**
2. Provider wiring: `parse_structural_topology`, kinds, trivia. The existing
   `validate_graft` byte-identity check becomes the runtime backstop for the
   same invariant — a lowering bug now rejects a graft (stage-4 refusal
   surfaces it) instead of corrupting a tree.
3. End-to-end acceptance, exactly as agreed: open
   `plans/adr/0001-ecs-memory-reclamation.md`, place the cursor beneath
   "Decision", and the inspector's cursor-scoped ancestry reads
   `DOCUMENT → H2 → LIST_ITEM` (host-level test asserting the labels appear
   in the panel, in the established `SOURCE_FILE`-test style). Plus:
   cursor-follow moves between sections as the cursor crosses a heading;
   semantic mode hides `GAP`; raw mode shows it.

## Non-goals

- Inline structure (emphasis, links, code spans) — block granularity only.
- List-item nesting by `depth` (flat within their section in v1; the depth
  is preserved in the parser if a follow-up wants the tree).
- Fence-language injection coordination, json/html/bash providers, preview
  changes. The markdown *preview* keeps its own parse — sharing one parse is
  a separate refactor, not smuggled in here.

## Review checklist

- [ ] Coverage property suite exists, passes, and precedes provider wiring
      in the commit's test structure; failures print the seed/input.
- [ ] Nested/container ranges are suppressed only when fully contained;
      crossing ranges fail closed to `DOCUMENT → CONTENT`.
- [ ] Every named hazard has a dedicated fixture.
- [ ] Section stack semantics: preamble, level-skips (H1 → H3), trailing
      sections, heading-only documents all covered.
- [ ] Kinds roundtrip; only `GAP` is trivia; names bounds-checked.
- [ ] The ADR-0001 breadcrumb acceptance test passes verbatim.
- [ ] No preview/rendering behavior change; no shared-parse refactor.
- [ ] Graft acceptance on real plan documents (the repo's own `*.md` corpus
      makes a fine fixture set — every file in `crates/libaryon/plans/`
      must lower, partition, and graft).
