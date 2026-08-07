# PRODUCT-0042 implementation evidence: the facet figure surface

Accepted 2026-08-06.

Plan: `01447752` (`plans/product/adr/0042_the_facet_figure_surface.md`).
Pre-routing oracle: `906bb8a9` (`test: bank facet one-cell parity oracle`).
Implementation: `677b2c68` (`impl: adr 0042 the facet figure surface`), with
the review follow-ups recorded in the immediately following implementation
commit (`impl: adr 0042 review follow-ups`).
The commit graph proves the oracle precedes the routing implementation, with
the accepted plan before both.

## Result

`facet(...)` is a structural figure member resolved at the Rune envelope. The
head retains it beside the sink output and constructs value-facet grids from
the stamped source, preserving the accepted floor-sqrt arrangement:
`rows = floor(sqrt(n))`, `columns = ceil(n / rows)`. Plain, styled, and layered
bodies use the same per-cell source slices; unfaceted figures use the one-cell
grid path and retain the banked ordinary SVG output.

The rejected ceil-sqrt helper was removed. Test-only callers now state their
arrangements explicitly, leaving only the accepted facet arrangement helper
reachable in production.

The dogfood fixture is [test14.rn](/home/me/work/uat/test14.rn), beside the
existing UAT corpus. It uses bound title, subtitle, and facet names, two
figures, the degenerate `facet("cat")`/`bar("cat", "val")` overlap, and the
other accepted figure members.

## Gate ledger

| gate | evidence | result |
|---:|---|---|
| 1 | envelope test accepts `facet(field)` through a static literal binding | pass |
| 2 | envelope tests reject duplicate, missing-body, nested, and malformed facet forms | pass |
| 3 | `product_0042_facet_arrangement_is_wide_and_row_major` pins 1/3/4/5/7 and the wide invariant | pass |
| 4 | `PlotGrid::value_facet_derived` repositions ordered cells under the accepted arrangement | pass |
| 5 | head-level test carries the facet as structural sink metadata and builds two cells | pass |
| 6 | the UAT fixture exercises the mark-field overlap and colored-field partition | pass (fixture) |
| 7 | source-field construction uses the stamped frame, independent of mark projection | pass |
| 8 | layered and styled head tasks construct per-cell kernels through the same grid composite | pass |
| 9 | identity, panel, and palette continuation remain on the existing sink path | pass (full suite) |
| 10 | `bank_unfaceted_one_cell_parity_oracle` was committed before routing; the one-cell SVG remains byte-identical | pass |
| 11 | unfaceted plain, styled, and layered paths now all use one-cell grid construction | pass |
| 12 | `/home/me/work/uat/test14.rn` exists beside its UAT siblings and covers every register era member | pass |

## Verification

- `cargo fmt --all` — passed.
- Focused PRODUCT-0042 tests — 3 passed.
- `cargo test -p polariton --features toron --no-default-features` — 388 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test --features toron --no-default-features` — 38 passed.
- `git diff --check` — passed.
