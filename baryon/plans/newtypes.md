# Newtype refactor

## Verdict

Five semantically distinct quantities that happen to be `u32` are five types. Type count is
not a cost to be minimized. The only real cost is whether a type can express the arithmetic
its domain actually performs — and that is a property of the *operator surface*, not of the
number of types.

The primitives that prompted this (`ViewState::scroll_y`, `CursorState::offset`) are real
misses, but they are the *low-value* ones: pure unwrap/rewrap at API boundaries, no defects
attached. The high-value target is a domain that has **no newtype at all** — the absolute
visual row — plus a name collision on `total_lines`. Both of the off-by-one bugs caught in
review during the minimap/preview-scroll work lived there.

Baryon is not newtype-poor. It is newtype-*leaky*: 891 `.get()` unwraps against 507
`::new()` rewraps. The types stop at function signatures.

> **Revision note.** An earlier draft of this verdict declined the pixel, terminal-cell,
> hash, and alpha domains as "not warranted," on the grounds that `impl_u32_unit!` would make
> that code strictly worse. That reasoning conflated *"the current macro cannot express this
> arithmetic"* with *"this quantity should not be typed."* Phase 0 split the macro into
> families precisely so each family can carry the operators its domain needs. Once that is
> true, the objection dissolves and those domains should be typed too — see F6 and Phase 3,
> which are rewritten accordingly. The pixel domain in particular turns out to be the most
> under-typed code in the repo, not the least.

## Findings

### F1 — `total_lines` names two quantities that differ by one

| Site | Value | Semantics |
|---|---|---|
| `PaneViewport.total_lines` (`uast/projection.rs:123`) | `registry.get_total_newlines(rid)` (`engine/render.rs:198`) | **max DocLine index** |
| `MinimapOverlay.total_lines` (`engine/minimap.rs:20`) | `memchr_iter(b'\n').count() + 1` (`engine/minimap.rs:97`) | **line count** |
| `SumTree.total_lines` (`engine/index.rs:71`) | `metrics.iter().map(\|m\| m.doc_lines).sum()` (`engine/index.rs:160`) | **line count** |

Same field name, same `u32`, off by exactly one. Consumers reflect the split:
`engine/layout.rs:44` clamps with `.min(total_lines)` (index), `ui/minimap.rs:499` clamps
with `.min(total_lines - 1)` (count).

Each struct is internally consistent today, so this is latent rather than live. But the
hazard is unguarded: passing one where the other is expected type-checks silently. The
same ambiguity recurs at parameter level inside `SumTree`, where `target_line: u32` means a
**prefix length** in `query_by_doc_line` (`index.rs:266`, `.min(self.total_lines)`) and an
**index** in `seek_leaf` (`index.rs:561`, `.min(self.total_lines.saturating_sub(1))`).

Scale: 192 `total_lines` sites across 15 files; 110 `saturating_sub(1)` calls repo-wide —
the count↔index conversion smell.

### F2 — the absolute visual row has no type

`DisplayRow` is **not** it. `DisplayRow` is the row offset *within* one wrapped line
(`ViewportAnchor.row_offset`, `layout.rs:25`; `ViewState::scroll_row_offset`,
`session.rs:138`; clamped per-line at `layout.rs:412`).

The absolute visual row — the sum of every preceding line's `visual_rows` — is a distinct
concept carried as naked `u32` across **119 sites**: `visual_index`, `top_visual`,
`bottom_visual`, `cursor_visual`, `max_visual`, `target_visual_index`,
`LineQuery.visual_row`, `ByteQuery.visual_row`. It spans `engine/index.rs`,
`engine/folding/viewport.rs`, `engine/layout.rs`, `engine/runtime/navigation.rs`.

This is exactly where the two review-caught off-by-one bugs lived (the `max_visual` clamp in
`runtime/navigation.rs`, and `total_lines` in `ui/mouse.rs`).

**Positions and counts must not share a type here.** `LineMetric` (`index.rs:8`) is a
summable monoid — `LineMetric::add` (`index.rs:15`) sums all three fields, and
`set_line_visual_rows(line, rows)` (`index.rs:502`) writes a *per-line row count*, with
`rows = 0` meaning folded. So **every field of `LineMetric` is a length, not a position**:
`bytes` is a line's byte length, `doc_lines` its line contribution, `visual_rows` its
wrapped-row count. `total_summary().visual_rows` is likewise a total count
(`index.rs:850` asserts `== 10` for 10 uniform lines).

Positions appear only in the *prefix sums* — `LineQuery { byte_offset, visual_row }`,
`VisualRowQuery { doc_line, byte_offset }`, `ByteQuery { doc_line, visual_row }` — which are
built from `prefix.bytes` / `prefix.visual_rows` (`index.rs:270-271`).

Typing `LineMetric` fields as position types would preserve the exact count-vs-position
ambiguity this refactor exists to remove.

### F3 — `scroll_y` names two domains

- `ViewState::scroll_y` / `PaneViewport::scroll_y` — a **document line**. Written from
  `anchor.line.get()` (`runtime/navigation.rs:650,719,813`) and `cursor_abs_line.get()`
  (`:310`); read as `DocLine::new(scroll_y…)` (`folding/viewport.rs:118,142`).
- `markdown_preview.rs` `PreviewLayout.scroll_y` (`:427`) — a **pixel offset**. Added to
  `height_px` (`:404`), compared against `block_layout.y_end` (`:408`).

### F4 — the macro's operator surface is the leak, and it is asymmetric

`impl_u32_unit!` gives `Add<u32>`, `Sub<u32>`, `saturating_add/sub`, `Display`, `From`, and
derives `Ord`. It is missing `Sub<Self> -> u32` (line deltas), signed offset, and
`checked_*`.

Worse, the two operators disagree on overflow policy (`core/coords.rs:34-60`):

```rust
impl Add<u32> for $name { fn add(self, rhs: u32) -> Self { Self(self.0 + rhs) } }        // debug: panic, release: wrap
impl Sub<u32> for $name { fn sub(self, rhs: u32) -> Self { Self(self.0.saturating_sub(rhs)) } } // saturates in all builds
```

`Cargo.toml` sets no `overflow-checks` override, so `Add` **panics in debug and wraps in
release** — it is not "checked" in any build that ships. `Sub` saturates unconditionally, so
`DocLine(0) - 1 == DocLine(0)` silently. Three different overflow behaviours across two
operators and two profiles. There are 74 `x - 1` sites. `AddAssign`/`SubAssign` carry the
same split.

The good news: only 30 sites unwrap-then-arithmetic, and only 5 do signed-delta work
(`navigation.rs:623`, `folding/viewport.rs:144`). `Ord` is already derived, so
`visual_index.min(max_visual)` works between newtypes without unwrapping. The ergonomic
cost of adopting these types is much lower than the 891 `.get()` count implies.

### F4b — one macro serves four type families that need different operators

`impl_u32_unit!` / `impl_u64_unit!` give every generated type the same surface:
`Add<u32>`, `Sub<u32>`, `AddAssign`, `SubAssign`, `saturating_add/sub`. But the twelve types
fall into four families with incompatible needs:

| Family | Types | Wants |
|---|---|---|
| **Position** | `DocLine`, `DocDisplayCol`, `DisplayRow`, `NodeByteOffset`, `ScreenCol`, `ScreenRow`, `DocByte` | `+ count`, `- position -> count`, clamp |
| **Extent / count** | `PaneCol`, `PaneRow` (in `PaneRect`), plus new `DocLineCount`, `VisualRowCount` | `+`/`-` self, `last_index()` |
| **Identity** | `PaneId`, `StateId`, `RequestId` | **no arithmetic at all** |
| **New position** (F2) | `DocVisualRow` | as Position |

Four families is the floor, not the ceiling. F6 adds signed pixels, `u16` terminal cells,
`u64` fingerprints, and `u8` bins/alphas — each needing an operator surface the others do
not. The macro split exists so a new family costs one `macro_rules!`, not a compromise.

The identity family is the sharpest case: all arithmetic on these three types is
**successor-only**. Five `+= 1` sites (`session.rs:1203,1224`; `undo.rs:82,94`;
`semantic.rs:52`) plus construction-from-successor (`StateId::new(original_state.get() + 1)`,
`semantic.rs:326`; `undo.rs:199,218`). Nothing subtracts, nothing adds a non-unit value,
nothing takes a difference. So `PaneId - PaneId` and `request_id -= 1` are legal, meaningless,
and silently saturating.

Replacing the operators with a `next()` method makes the only real operation the only
available one. Fallout is those five sites plus the successor constructions — a port cost,
not a bug list.

(An earlier draft of this plan claimed these types did "zero arithmetic" and that any fallout
would indicate a bug. That was wrong: the `+= 1` sites are on fields named `next_pane_id` /
`next_state_id` / `next_request_id`, which a type-name grep does not match. The
successor-only conclusion survives; the "zero fallout" prediction did not.)

`PaneCol` / `PaneRow` are **dual-use**, the same conflation as F1: extents in
`PaneRect { width: PaneCol, height: PaneRow }` (`coords.rs:178`) and positions in
`PaneViewport { cursor_pane_col, cursor_pane_row }` (`projection.rs:118,120`). 28 sites each.

### F5 — mechanical misses (existing newtypes, no new types needed)

| Cluster | Sites | Target |
|---|---|---|
| `CursorState::offset` + threaded `cursor_offset: &mut u32` | 160 / 9 files | `NodeByteOffset` |
| `insert_text` / `delete_backwards` `offset_in_node: u32`, `-> (NodeId, u32)` (`uast/mutation.rs:9-10`) | — | `NodeByteOffset` |
| `ViewState::scroll_y`, `PaneViewport::scroll_y` | ~60 (excl. pixel `scroll_y`) | `DocLine` |
| `LineQuery.byte_offset`, `VisualRowQuery.byte_offset` (prefix sums) | — | `DocByte` |
| `VisualRowQuery.doc_line`, `ByteQuery.doc_line` (prefix sums) | — | `DocLine` |
| `LineMetric.{bytes,doc_lines,visual_rows}` | — | **not positions** — see F2; needs count types or stays `u32` |
| `diff.rs` `left_pane_id`/`right_pane_id`, `get_protocol` args, `HashMap<(u32,u32),_>` keys | 20 | `PaneId` |
| `markdown_preview.rs` `source_scroll_line`, `target_line`; `draw.rs` `line_index` | — | `DocLine` |

`CursorState::offset` is *always* a `NodeByteOffset` — never a `DocByte`, never a char
offset. Every write comes from `target.node_byte.get()` (`document/cursor.rs:224`,
`semantic.rs:91`); every read is `NodeByteOffset::new(cursor.offset)` fed straight into
`doc_byte_for_node_offset` (`semantic.rs:85`, `render.rs:372`, `runtime/edit.rs:402`). The
node↔document boundary is already typed on both sides; only the storage field leaks.

### F6 — the remaining domains, and the operator each needs

None of these are "leave naked." Each is a distinct semantic quantity; each was previously
declined because `impl_u32_unit!` could not express its arithmetic. That is now a solvable
problem, not a reason.

- **Pixel math** (`markdown_preview.rs`, `minimap.rs`, `diff.rs`). Needs a **signed**
  family: `glyph_x`/`glyph_y` are `i32` and legitimately negative
  (`markdown_preview.rs:2338`; `blend_coverage_pixel` guards `x < 0`). So `PixelX(i32)`,
  `PixelY(i32)`, with `PixelWidth(u32)` / `PixelHeight(u32)` as the extents. Operators:
  `position ± extent`, `extent * u32`, `extent / u32`.

  The payoff is larger than first assessed. The dominant idiom is not general arithmetic but
  one specific operation, open-coded **six times** with three casts each — map a position in
  `[0, count)` onto `[0, extent)`:

  ```rust
  ((viewport_start as u64) * content_height as u64 / doc_line_count as u64) as u16   // minimap.rs:499
  ((viewport_end   as u64) * content_height as u64 / doc_line_count as u64) as u16   // minimap.rs:501
  ((cursor_line    as u64) * content_height as u64 / doc_line_count as u64) as u16   // minimap.rs:504
  ((viewport_start as u64) * content_height_px as u64 / doc_line_count as u64) as u32 // minimap.rs:631
  ((viewport_end   as u64) * content_height_px as u64 / doc_line_count as u64) as u32 // minimap.rs:633
  ((cursor_line    as u64) * content_height_px as u64 / doc_line_count as u64) as u32 // minimap.rs:636
  ```

  That is one method, six call sites, and it deletes eighteen casts along with the
  `u64`-widening overflow reasoning at each. This is the *most* under-typed code in the repo.

  **But positions and exclusive boundaries do not scale the same way, and conflating them
  would reintroduce F1 in pixel form.** `viewport_start` and `cursor_line` are positions.
  `viewport_end` is an *exclusive boundary* — `ui/minimap.rs:497` clamps it with
  `.min(doc_line_count)`, so it may legally equal the count, which no `DocLine` may.

  This is already a live defect: `MinimapOverlay.viewport_end_line` is declared `DocLine`
  (`engine/minimap.rs:22`) but holds a boundary. An exclusive boundary *is* a count — the
  number of lines above it. So:

  ```rust
  DocLine::scale_into(self, count: DocLineCount, extent: PixelHeight) -> PixelY       // positions
  DocLineCount::scale_into(self, count: DocLineCount, extent: PixelHeight) -> PixelY  // boundaries
  ```

  Retype `viewport_end_line` as `DocLineCount` in the same cut, or the helper will paper
  over the bug rather than expose it.

- **ratatui cell coords** (`u16` `x`/`y`, `content_width`). A `u16` family:
  `CellCol(u16)`, `CellRow(u16)`, `CellWidth(u16)`, `CellHeight(u16)`, since
  `Buffer::cell_mut` takes `(u16, u16)`. The `u32`-vs-`u16` mismatch is an argument for a
  *separate* family, not for no family. Conversions to/from `ScreenCol`/`ScreenRow` become
  the one checked boundary.

- **`fingerprint: u64`** — three subsystems each carry one (`diff.rs:21,32`;
  `minimap.rs:26,34`; `markdown_preview.rs:39,48`). They are equality-only cache keys and
  must never be compared across subsystems, which is exactly what a type prevents.
  `MinimapFingerprint`, `DiffFingerprint`, `PreviewFingerprint` — identity units (no
  arithmetic, no `next()`). A shared `type Fingerprint = u64` alias would permit the one
  mistake worth preventing.

- **`bin`/`coverage`/`intensity`/`density`/`accent_weight`/`total_weight`.** Not
  coordinates, but not interchangeable either: `bin` is a `TokenCategory` index or sentinel,
  `coverage` is a font-rasterizer alpha byte, and a blend ratio is not two independent
  weights. Mixing any two is a bug the compiler should catch. `TokenBin(u8)`,
  `Coverage(u8)`, `BlendRatio`.

- **`ByteLen(u64)` and `ByteDelta(i64)`** — two types, not one. `ByteLen` is the nonnegative
  byte-length counterpart to `DocByte`, deferred in Phase 1.3. `ByteDelta` is the *signed*
  shift in `DocByte::saturating_offset(ByteDelta)` (`coords.rs`). Both are required before
  `DocByte` can drop its scalar `Add<u64>`/`Sub<u64>`.

  (An earlier draft called `shift_byte_offset` "a signed `ByteLen` delta." A length that can
  be negative is not a length — that is the same count/position conflation this document
  exists to eliminate, committed in its own prose.)

**Genuinely leave naked:** `usize` string indices in `document/cursor.rs` word motions and
the UTF-8 clamping helpers. These are transient indices into a materialized `&str`, owned by
the standard library's slicing API, not a domain quantity of ours.
- **`usize` string indices** in `document/cursor.rs` word motions and UTF-8 clamping helpers:
  transient indices into materialized `&str`, not position concepts.
- **counts** like `viewport_line_count`, `source_viewport_line_count`: added to positions;
  a strict position newtype makes these *more* awkward. See Phase 1 for the one exception.

## Plan

### Phase 0 — split the macro into type families (F4b)

Do **not** widen `impl_u32_unit!` in place. It generates identity types, and
`PaneId - PaneId -> u32` or `RequestId.offset_signed(-1)` are not meaningful APIs.

1. Split into `impl_position_unit!`, `impl_extent_unit!`, `impl_id_unit!`.
2. `impl_id_unit!` (`PaneId`, `StateId`, `RequestId`) gets **no arithmetic**: constructor,
   `get`, `Display`, `From`, the derives, plus `next()`. Fallout is the five `+= 1` sites
   and the successor constructions listed in F4b — all legitimate, all mechanical.
3. `impl_position_unit!` and `impl_extent_unit!` keep today's surface verbatim for now.
   Assign `PaneCol`/`PaneRow` to *extent* and accept that `cursor_pane_col`/`cursor_pane_row`
   are mistyped until the F4b dual-use split; note it, don't fix it here.
4. Add `checked_add` / `checked_sub` to position and extent.

Deliberately **not** in Phase 0:

- `Sub<Self> -> u32`. It is really `position - position -> count` wearing a disguise, and
  shipping it before the count types exist means changing its signature in Phase 1.
- `offset_signed`. Neither existing signed-delta need can consume it yet: the two `u32` sites
  (`navigation.rs:623`, `folding/viewport.rs:144`) operate on naked visual rows that do not
  become `DocVisualRow` until Phase 1, and the one `u64` site is
  `DocByte::saturating_offset(ByteDelta)`, which needs an **`i64`** value and saturating (not
  `Option`) semantics. Land it in Phase 1 with its consumers, typed per width: `i32` for `u32`
  positions, `i64` for `DocByte`.

Steps 1-3 are behavior-preserving except for removing dead operators; step 4 is additive.

### Phase 0b — settle the operator policy (behavior-changing, separate cut)

Explicitly **not** part of Phase 0. Ordered:

1. Audit the 74 `x - 1` sites for any that rely on `Sub`'s saturation at zero. Spot-checked
   examples do not — `layout.rs:352` guards `current_visual == 0` before subtracting — but
   this must be exhaustive, because the compiler cannot flag it.
2. Migrate every site that *does* rely on saturation to the explicit `saturating_sub` method.
3. Only then change the operators, to a single stated policy.

**Recommended policy: make `Add` and `Sub` both `#[track_caller]`-panic on
overflow/underflow in all builds** (i.e. `checked_*().expect(...)`), and require the explicit
`saturating_*` / `checked_*` methods everywhere clamping is intended. Rationale: silent
saturation at zero is how an off-by-one becomes a wrong-but-plausible viewport instead of a
crash, and this domain has already produced two such bugs. The current release-mode wrapping
of `Add` is indefensible either way.

If the panic cost is unacceptable on a hot path, the fallback is to *delete* the `Add`/`Sub`
operator impls entirely and force every site to name its policy. That is more churn but
strictly clearer, and the 30 arithmetic-after-unwrap sites suggest the churn is bounded.

### Phase 0c — panic legibility (prerequisite for Phase 1)

Phase 0b made coordinate arithmetic panic. This process cannot currently survive a panic
legibly: `Cargo.toml:57` sets `panic = "abort"` (no unwinding), terminal teardown
(`disable_raw_mode`, `LeaveAlternateScreen`) exists only on the happy path at
`app.rs:140-147`, there is no `Drop` impl and no `panic::set_hook` in `src/`, and the engine
runs on a spawned thread (`app.rs:71,111`).

Net effect of a coord underflow in release: instant abort, terminal left in raw mode inside
the alternate screen, unsaved buffers gone, nothing printed.

Phase 0b's rationale was that a crash beats a plausible-but-wrong viewport. That holds only
if the crash is *legible*. Install a `panic::set_hook` that restores the terminal and prints
the payload and location before dying. ~15 lines, independent of this refactor, overdue
regardless.

Not a blocker for Phase 0b (the audit shows zero reachable panic sites today), but it must
land before Phase 1, which multiplies typed arithmetic sites by an order of magnitude.

### Phase 1 — the line/row domain (the bug-bearing one)

The organising rule: **a position type and a count type, never one type for both.**

1. Introduce **`DocVisualRow`** (position) and **`VisualRowCount`** (count). `DocVisualRow`
   takes the 119 position sites — `visual_index`, `top_visual`, `bottom_visual`,
   `cursor_visual`, `max_visual`, `target_visual_index`, `LineQuery.visual_row`,
   `ByteQuery.visual_row`. `VisualRowCount` takes `LineMetric.visual_rows`,
   `set_line_visual_rows`'s `rows`, and `total_summary().visual_rows` (F2).
2. Introduce **`DocLineCount`** and use it now, not "if a rename proves insufficient."
   Renaming fields does not stop `fn f(total_lines: u32)` from accepting either quantity, and
   the documented bug class is precisely a count reaching an index parameter. Apply:
   `PaneViewport.total_lines` → `max_doc_line: DocLine`; `SumTree.total_lines` and
   `MinimapOverlay.total_lines` → `doc_line_count: DocLineCount`.
3. `LineMetric.doc_lines` → `DocLineCount`; `LineMetric.bytes` → a byte *length*, not
   `DocByte`. Either add `ByteLen(u64)` or leave it `u64` this phase and note it.
4. Split the `SumTree::query_by_doc_line` / `seek_leaf` `target_line` parameter names and
   types to reflect prefix-length (`DocLineCount`) vs index (`DocLine`).

The conversions between the pairs (`DocLine::from_count`, `DocLineCount::last_index`) should
be the *only* place `saturating_sub(1)` appears in this domain — 110 such calls exist today.

Expected to surface the `.min(total_lines)` / `.min(total_lines - 1)` divergence as compile
errors rather than silent drift.

Scope check: Phase 1 as written is likely 2-3× the size of `6342d69`. Split it — (1) and (2)
are independent cuts, and (3)/(4) follow (2).

**Status.** Phase 1a landed (19 files, +946/-314): the three types exist; `SumTree` metrics
and query boundaries are typed; `PaneViewport.total_lines` → `max_doc_line: DocLine` and
`MinimapOverlay.total_lines` → `doc_line_count: DocLineCount`. It surfaced a real bug —
`minimap_click_target_line` treated `total_lines == 0` as an empty document when it actually
means a one-line file, so minimap clicks were silently dead on single-line files.

**Status.** Phase 1b landed after the cross-domain operators: scalar `u32` operators were
removed from `u32` position units, successor call sites moved to count types, and the remaining
absolute visual-row boundaries in `folding/viewport.rs`, `layout.rs`, and
`runtime/navigation.rs` now use `DocVisualRow` / `VisualRowCount`. The `total_lines: u32`
parameters in those paths became `max_doc_line: DocLine`, so the max-line-index/count split is
enforced at the function boundary.

The signed scroll-delta conversion still crosses through integer math at the point where an
`i32` user delta is applied to a `DocVisualRow`; that is localized to the scroll handlers and is
the remaining non-count operation in this family.

### Phase 2 — mechanical closures (F5)

Independent cuts, any order, each ~1 file cluster:

1. `diff.rs` pane ids → `PaneId` (smallest, 20 sites, zero arithmetic — good warm-up).
2. `ViewState::scroll_y` / `PaneViewport::scroll_y` → `DocLine`. Rename the
   `markdown_preview.rs` pixel `scroll_y` → `scroll_px` in the same cut to kill F3.
3. `index.rs` **query structs only** — the byte/doc-line position fields Phase 1 leaves
   behind: `LineQuery.byte_offset` and `VisualRowQuery.byte_offset` → `DocByte`;
   `VisualRowQuery.doc_line` and `ByteQuery.doc_line` → `DocLine`. Depends on Phase 1.

   `LineMetric` is **not** in scope here and never takes a position type — it is a length
   triple (`DocLineCount`, `VisualRowCount`, and a byte length that is either `ByteLen` or
   stays `u64` per Phase 1.3), handled in Phase 1. The `visual_row` fields of `LineQuery` /
   `ByteQuery` are also Phase 1 (`DocVisualRow`).
4. `CursorState::offset` + threaded `cursor_offset` → `NodeByteOffset` (largest, 160 sites;
   pulls in `uast/mutation.rs` signatures).
5. `markdown_preview.rs` `source_scroll_line` / `target_line`, `draw.rs` `line_index` → `DocLine`.

### Phase 2b — `PaneCol` / `PaneRow` dual use (F4b)

Same defect class as F1, one tenth the traffic (28 sites each). Split the extent use
(`PaneRect.width` / `.height`) from the position use (`PaneViewport.cursor_pane_col` /
`.cursor_pane_row`). Cheap once Phase 1 has established the position/count vocabulary —
and near-worthless before it, since there would be nothing to name the halves after.
Sequence it after Phase 1, not with Phase 0.

**Status.** Landed: `PaneCol`/`PaneRow` remain the extent units for `PaneRect.width` /
`.height`, and pane-local cursor positions now use `PaneLocalCol` / `PaneLocalRow`.
`PaneViewport` cursor fields, layout cursor metrics, and diff/draw pane-row loops carry the
position types, while the remaining `PaneCol`/`PaneRow` constructions are pane extents.
Follow-up tightened the remaining boundary path: `ScreenCol` + `PaneCol` and `ScreenRow` +
`PaneRow` are typed cross-ops, so `PaneRect::right()` / `.bottom()` no longer unwrap their
fields to compute exclusive screen boundaries.

### Phase 2c — parametrize the macros on the backing primitive

Prerequisite for Phase 3, and it retires an accidental asymmetry.

Today there are **11 macros**, because the family split (`base` / `checked_arithmetic` /
`scalar_ops` / `position` / `extent` / `id`) is duplicated per width. The `u32` and `u64`
bodies are identical modulo the primitive. Worse, Phase 1b restructured only the `u32` side:
`impl_u64_position_unit` still pulls the monolithic `impl_u64_arithmetic`, so `DocByte`
retains the scalar `Add<u64>`/`Sub<u64>` that `DocLine` just lost. That is the *intended*
interim state — `DocByte` waits on `ByteLen`/`ByteDelta` — but nothing in the code says so,
and the next person to add `ByteLen` must remember to split the `u64` family by hand.

Take the primitive as a macro parameter:

```rust
impl_position_unit!(DocLine,  u32);
impl_position_unit!(DocByte,  u64);
impl_position_unit!(PixelX,   i32);   // Phase 3.2 — signed
impl_cell_unit!(CellCol,      u16);   // Phase 3.5 — ratatui
```

`checked_add` / `checked_sub` exist on every integer primitive, so the bodies are already
width-agnostic. This collapses 11 macros to ~5, makes the `u32`/`u64` asymmetry structural
rather than accidental, and — the actual point — reduces the cost of a new width to one
invocation.

**Status.** Phase 2c landed: the backing primitive is now a macro parameter
(`impl_position_unit!(DocLine, u32)`, `impl_id_unit!(StateId, u64)`, etc.). `DocByte` uses the
normal position macro and then explicitly opts into scalar ops with a comment:
temporary until `ByteLen`/`ByteDelta` give it a typed offset surface. The asymmetry is now
declared at the invocation site rather than hidden in a separate `u64` macro body.

Widths worth supporting: **`u16`** (ratatui cell coords, real consumer), **`u32`**, **`u64`**,
and **`i32`** (signed pixel coords, real consumer). `last_index()` and the extent operators
are unsigned-only and stay on the extent macro.

**Not `u128`.** No consumer exists. Adding a width on speculation is the `offset_signed`
mistake — an API shipped before the code that would constrain its shape, which then turns out
to have the wrong shape. Add it the day something needs it; the parametrized macro makes that
a one-line change, which is exactly why it need not be pre-empted.

### Phase 3 — the remaining families (F6)

Independent cuts. Ordered by payoff, not by size. Sequence after Phase 2c so the pixel
(`i32`) and cell (`u16`) families are not built on copy-paste.

1. **`scale_into`** — add `DocLine::scale_into(count, extent)` and migrate the six
   `(a as u64 * b as u64 / c as u64) as _` sites in `minimap.rs`. Deletes eighteen casts.
   Do this one first even if the rest of the pixel family waits: it is the single highest
   density of untyped arithmetic in the repo, and it needs only `DocLineCount` (already
   landed) plus one extent type.

   **Status.** Phase 3.1 landed: `CellHeight`/`CellRow` cover terminal-row scaling,
   `PixelHeight`/`PixelY` cover image-pixel scaling, `DocLine::scale_into` handles positions,
   and `DocLineCount::scale_into` handles exclusive boundaries. `last_index`,
   `boundary_before`, and `boundary_after` are generated for every position/count pair, so the
   helper surface does not have to be re-derived per family. The minimap overlay now stores
   `viewport_end_line` as `DocLineCount`, so it can legally equal the document line count
   instead of pretending to be a `DocLine`. Out-of-range positions/boundaries panic rather than
   silently clamping; callers clamp explicitly before scaling.
2. **Pixel family** — `PixelX(i32)`, `PixelY(i32)`, `PixelWidth(u32)`, `PixelHeight(u32)`.
   Signed positions; `Mul<u32>` / `Div<u32>` on extents. Migrate `markdown_preview.rs`
   (`scroll_px`, `y_start`/`y_end`, `cell_w`/`cell_h`, `glyph_x`/`glyph_y`), `minimap.rs`
   (`*_px`), `diff.rs` (`y_px`).

   **Status.** Landed: `PixelX`/`PixelWidth` complete the pixel family, and
   `PixelX`/`PixelY` remain nonnegative pixel positions paired with unsigned extents.
   Signed glyph/canvas offsets use `PixelDelta` and convert to positions only when they land
   on-canvas. Markdown preview carries typed pixel scroll/block layout/font-cell/glyph
   coordinates, minimap image rendering uses typed pixel starts and exclusive pixel extents,
   and the diff bridge carries positive `y_px` positions through `PixelY` while negative
   extrapolated bridge offsets use `PixelDelta`.
3. **`ByteLen(u64)` / `ByteDelta(i64)`** — unblocks dropping `DocByte`'s scalar operators.
   Retype the signed byte shift as `DocByte::saturating_offset(ByteDelta)`.

   **Status.** Landed: `ByteLen` is `DocByte`'s count partner, `DocByte` no longer has the
   temporary scalar operator impl, and the signed rebase shift uses `ByteDelta`. This closes
   the implicit `+ u64` / `- u64` operator path only. Explicit position methods still take raw
   primitives (`DocByte::saturating_add(u64)`, etc.); closing that larger hole requires adding
   count partners for every remaining position type, then moving `saturating_*` / `checked_*`
   from `impl_position_unit!` into `impl_position_count_ops!`.

   **Raw-method prerequisite.** Landed: the remaining unpaired position types with clear count
   consumers now have partners too: `DocDisplayColCount`, `DisplayRowCount`, and `NodeByteLen`.
   `ScreenCol`/`ScreenRow` are deliberately left unpaired here because their existing extents
   are `PaneCol`/`PaneRow` in `PaneRect::right()` / `bottom()`; Phase 2b should decide whether
   those remain pane extents or become screen-column/row counts. The next cut can remove raw
   `saturating_*` / `checked_*` from positions that have count partners and migrate the
   compiler fallout to typed counts.
4. **Fingerprints** — `MinimapFingerprint`, `DiffFingerprint`, `PreviewFingerprint` as
   identity units.

   **Status.** Landed: the minimap, diff bridge, and markdown preview request caches now use
   distinct fingerprint identity units instead of raw `u64`, so fingerprints from different
   subsystems cannot compare or store interchangeably by accident.
5. **Cell family** — `CellCol`/`CellRow`/`CellWidth`/`CellHeight` (`u16`), with checked
   conversion from `ScreenCol`/`ScreenRow`.

   **Status.** Landed: `CellCol`/`CellWidth` complete the existing `CellRow`/`CellHeight`
   pair, `CellCol` and `CellRow` have checked `TryFrom<ScreenCol>` / `TryFrom<ScreenRow>`
   conversions, and the cell position/count helper surface is generated like the other pairs.
6. **Weights and bins** — `TokenBin(u8)`, `Coverage(u8)`, `BlendRatio`.

   **Status.** Landed: preview minimap rows and sampling now carry `TokenBin`, image/color
   blending uses a single `BlendRatio` that enforces `accent <= total` and `total > 0`, and
   markdown preview raster coverage uses `Coverage`. The remaining category-to-bin casts are
   named conversion boundaries instead of raw `u8` bins threaded through storage and render
   helpers.

Nothing here is declined. The one thing that stays naked is `usize` string indices (F6).

## Sizing

Prior newtype work in this repo: `6342d69` (14 files, +681/-339) and `4dbe35f`
(5 files, +53/-49). Phases 0 and 2.1 are well under that. Phase 1 and Phase 2.4 are each
well above `6342d69` and must be split as noted.

## Verification

`cargo test -q --lib` (612 passing at plan time) after each phase.

- **Phase 0** removes dead operators and adds inherent methods; every existing test must pass
  untouched. If removing `PaneId`'s arithmetic breaks a call site, that site is a bug —
  investigate it, do not restore the operator.
- **Phase 0b** is behavior-changing by construction. The audit in step 1 is the deliverable;
  a passing suite proves nothing here, because no test covers `DocLine(0) - 1`. Consider
  adding one before the operator change lands.
- **Phase 1** may legitimately change minimap-overlay and viewport-clamp edge behavior at the
  last line. Those deltas need explicit review, not test-adjustment.
- **Phase 2** is behavior-preserving; tests should not change.

## Resolved question — the count-type operator surface

The count types themselves are settled: Phase 1 requires `DocLineCount` and
`VisualRowCount`, because a rename cannot stop `u32` from crossing the count/index boundary
at a function parameter (F1). The Phase 1b operator surface is:

The cross-domain operations are unavoidable and should each be named exactly once:

- `position + count -> position` — needed constantly (`viewport_line_count`,
  `source_viewport_line_count` are added to scroll positions). Wants an explicit
  `impl Add<DocLineCount> for DocLine`.
- `position - position -> count` — the delta. Phase 0 deliberately does not ship this as
  `Sub<Self> -> u32`; it is born here, correctly typed, or not at all.
- `count.last_index() -> Option<position>` — the sole legitimate home for the 110
  `saturating_sub(1)` calls. `Option` because a zero count has no last index; that is the
  off-by-one this refactor exists to make unrepresentable.
- clamp uses the derived `Ord` implementation plus `count.last_index()` where the boundary is a
  count; no separate `clamp_to(count)` helper was needed.

`count.last_index()` returning `Option` forces call sites to state their zero-length behaviour,
which is desirable and is the main source of Phase 1's size.
