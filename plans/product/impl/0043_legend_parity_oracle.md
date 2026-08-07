# PRODUCT-0043 legend parity oracle

The first implementation report overstated this artifact. Commit ab06a8a9 was
only a prose census; it did not contain recordings or a recorder. This repair
replaces that claim with the actual corpus and the tests that consume it.

## Provenance

The recordings were generated after the migration work had begun, from a
detached worktree at ab06a8a9 (the pre-migration tree), which is a graph
predecessor of implementation commit 4c736876. The temporary recorder ran
product_0043_record_pre_migration_legend_corpus over representative prepared
data for all sixteen concrete legend-producing kernels, plus the layered and
scatter-line composite paths. Its output is committed in
0043_legacy_legend_corpus.svg.txt; the per-section byte counts and SHA-256
digests are in 0043_legacy_legend_corpus.sha256.

This is the honest form of gate 1: the recording act post-dates the migration
plan, but the recorded renderer tree is independently pinned before the
migration by commit graph order.

## Live checks

product_0043_legend_corpus_covers_every_pre_migration_site routes the same
representative data through the current factory and shared renderer. It checks
every corpus section, replaces all RGB values with a sentinel, canonicalizes
only numeric endpoint formatting (old integral labels versus the typed model's
one-decimal labels), and compares the resulting SVG byte-for-byte. Geometry,
text layout, backing, borders, and mark-sample glyphs are therefore all
covered; only the explicitly accepted color authority change is removed from
the comparison. The line/scatter fixture's partition names are canonicalized
to `ENTRY` because Polars' legacy partition iterator has no ordering contract
and the old recording ran in a separate process; each path/text pairing and
its geometry remain exact.

The companion tests discharge the named differences and special shapes:

- product_0043_bar_legend_uses_the_resolved_band_fill checks the accepted
  artifact-color change directly.
- product_0043_gradient_and_dual_legends_keep_their_structural_geometry checks
  one 50-step gradient for heatmap, hexbin, and histogram, and the two-section
  bubble model with its size sample.
- product_0043_composite_corpus_sections_cover_both_delegations checks the
  layered and scatter-line content seams and their corpus sections.

The comparison is deliberately exact after those mechanical normalizations.
`LegendSample` preserves the old rectangle, line, and point
glyphs rather than flattening them, and the accepted color difference is
asserted separately by the artifact-color test. No geometry or glyph drift is
masked by a signature.
