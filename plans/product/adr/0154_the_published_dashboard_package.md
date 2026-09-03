# ADR 0154: the published dashboard package

Status: proposed 2026-09-03. Publication lane, first record; it advances the
web corpus rows *shared figure image*, *snapshot dashboard publication*,
and *served snapshot publication*, and no other. PRODUCT-0153 §4 made the
snapshot package the unit of publication and left its format to this
record. This record decides the package's shape, identity, data encoding,
provenance, figure description, rendered fallback, font carriage, the
promotion of the SVG backend to a production renderer under its own
evidence licence, the publish action, the HTML shell, and a deliberately
dumb server for an existing package. It decides no interaction, no hit
metadata, no viewer runtime, no Rune spelling, no live data, no hosting,
and no authentication.

## Context

### The export unit already exists, and it is the right seed

Plot → Export today walks the visible host panels and produces one
`HostPlotExport` per plot panel (`crates/polariton/src/editor.rs:2301`):
the prepared kernel, the settings model — the only settings carried across
that seam, resolved again at the point of use so an export cannot disagree
with its marks (PRODUCT-0139 §6) — the pane's width and height, the render
revision, and, since PRODUCT-0151, the selection revision and the rebuild
recipe so an export behind its selection is rebuilt rather than lied
about. Rendering is exact and headless by PRODUCT-0149 §8. Formats are
SVG, PNG, and AVIF. A package is this unit, made durable, given identity,
joined to its data, and multiplied across the dashboard.

### What the export unit lacks

- **Identity.** An export has a title. PRODUCT-0010 §5 gives a named view a
  durable key — workspace identity, durable document identity, view name —
  and PRODUCT-0151 §4 gives every Rune relation a `RelationLineage`, the
  first workspace call and its literal arguments. Neither reaches the
  export.
- **Data.** An export is ink. `ResultArtifact` holds Arrow IPC where a
  producer supplied it and a Polars frame otherwise, with an IPC bridge
  that exists to feed the CSV writer (`result_artifact.rs:435`), not to
  define a canonical encoding.
- **A durable definition.** PRODUCT-0002 §8 says a durable plot "is a
  saved `PlotSpec` referencing relation identity and provenance, not merely
  a bitmap", and that an exported image "is a presentation artifact, not an
  executable plot". The session owner (`session_persistence.rs`) already
  practises the discipline a durable definition needs: runtime types never
  implement `Serialize`; conversion is explicit; the schema is versioned;
  placement and renderer vocabulary are refused by an architecture test.
  `AuthoredPlotSpec` and `ResolvedPlotSpec` have no `Serialize` today.
- **A safe SVG.** The SVG backend interpolates label text unescaped
  (`plot/export.rs`, `fill_text_raw`) and names no font family, while
  every desktop label is set in the bundled "Polariton Plot Sans", an
  OFL-1.1 Noto Sans derivative renamed so the bundled bytes provably win
  (`fonts/mod.rs`). PRODUCT-0057 licenses the SVG as an oracle, not a
  renderer.
- **A consistency witness.** PRODUCT-0133 §5's `ArtifactToken` is a
  process-local counter, the identity of an in-memory publication. It can
  witness that nothing republished during assembly; it cannot name a file.

### What is already decided elsewhere and is consumed, not restated

TORON-0581's identity discipline: SHA-256 over exact bytes, versioned
schemas with explicit version bytes, sorted lists, one canonical encoding,
identity separate from navigation. PRODUCT-0136's write discipline: an
authored path, create-never-clobbers, a surfaced diagnostic. PRODUCT-0137's
certificate, whose implementation is blocked, as an optional attachment
and never a manufactured one. PRODUCT-0153 §1, §2, and §4 in full.

## Decision

### 1. A package is a directory with a versioned, content-addressed manifest

A published dashboard is a directory. Its root holds `manifest.json`,
schema version 1, listing every other file by relative path, byte length,
and SHA-256, plus the package's own fields. The package digest is the
SHA-256 of the manifest's canonical encoding — sorted keys, no
insignificant whitespace, one newline — and is the package's only
identity. A directory rather than an archive because a directory is
inspectable, diffable, and servable by anything; an archive form is a
later encoding of the same manifest, not a different package.

Manifest fields beyond the file table: package schema version; publisher
name and version; publish time; source-neutral provenance (§4); the figure
list (§5); the dataset list (§3); the font list (§6); and an optional
`certificates` list (§4). Nothing else.

### 2. Runtime types stay unserializable; the package has its own DTOs

Every manifest and figure value is a package-owned data type with explicit
conversion from runtime types, in a new `publication` module beside
`session_persistence`. Runtime plot, editor, and selection types acquire no
`Serialize`. Placement identities, host panel ids, buffer ids, artifact
tokens, and renderer vocabulary cannot enter; the architecture test that
enforces this for the session owner is extended to the package module.

### 3. Data is canonical Arrow IPC, addressed by digest, shared across figures

Every figure's data is an Arrow IPC stream file under `data/`, named by the
SHA-256 of its bytes. The encoding is canonical: one stream, uncompressed,
schema metadata sorted, batches in producer order, written by one writer
with fixed options, so equal record batches produce equal bytes and equal
digests. Arrow-backed artifacts are re-encoded through that writer, never
copied as received; Polars-backed artifacts cross an explicit
package-export conversion, the one seam where a frame becomes IPC for
publication. Figures reference datasets by digest. Two figures over one
relation share one file; the four figures of the distribution showcase are
four references to one digest.

### 4. Provenance is source-neutral, and a certificate is an attachment

Every dataset carries one provenance entry: the producer kind (Toron,
SQLite, PostgreSQL, MySQL, BigQuery, Parquet, Rune-derived), a non-secret
producer label, the relation lineage where the compiler carried one, the
result revision the desktop held, and the publish time. It never carries
an endpoint, a path outside the package, a credential, or an adapter
field value; §8's guard refuses a package that does. When a PRODUCT-0137
certificate exists for a materialized input it is embedded verbatim under
`certificates/` and referenced from the entry; when none exists the slot
is absent. A certificate is never synthesized.

### 5. A figure is identity, definition, presentation, layout, and fallback

Each figure has a manifest entry and a `figures/<id>/` directory. The
entry names: a figure id, stable within the package and derived from the
PRODUCT-0010 §5 durable key when the view is named and from the export
ordinal otherwise, with that distinction recorded; the title and subtitle
from the resolved chrome; the mark family; the dataset digest; and its
layout rectangle in the dashboard's grid, in cells. The directory holds
`definition.json` — the authored plot definition as a DTO, PRODUCT-0002
§8's saved specification — `presentation.json` — the resolved settings
model, theme selection, colour assignments, and domain overrides — and
`figure.svg`, the pre-rendered fallback at the export's width and height.

A figure that is behind its selection is rebuilt through its recipe before
publication, exactly as export does; a figure with no recipe refuses.

### 6. The SVG backend becomes a production renderer, with a licence

Three obligations make the SVG production-grade, each with its own gate:

1. **Escaping.** Text content and every attribute derived from data are
   XML-escaped. The fixture is a label containing `<`, `&`, `"`, and a
   non-ASCII glyph.
2. **Font carriage.** Every `<text>` names `font-family="Polariton Plot
   Sans"`. The package carries `fonts/PolaritonPlotSans-Regular.ttf` and
   its OFL-1.1 licence text, listed in the manifest, and the HTML shell
   declares the face with `@font-face`. Layout never depends on the
   browser's font, by PRODUCT-0117's static ASCII budget.
3. **The licence.** A package SVG rasterized through `resvg` with the
   package's own font file is pixel-equal to the PNG export of the same
   figure, which already takes that path (`PngBackend`). That equality is
   the evidence that "SVG with this font" is a faithful renderer; it says
   nothing about any other font, and PRODUCT-0057's limits on clipping
   and antialiasing under the iced renderer stand unchanged.

### 7. Publish is a desktop action beside Export, with a consistency witness

Plot → Publish… takes an authored directory path and writes a package for
the visible dashboard, under PRODUCT-0136's three floors: the path is
authored, an existing directory refuses, a failed write is a diagnostic.
Assembly captures every figure's `ArtifactToken` first; if any figure's
token differs when its data is read, assembly refuses and reports which
figure republished, and the user retries. Tokens are witnesses only and
never enter the package. On the desktop, a published package is not
opened; a later record decides import. Publishing from a saved session on
the command line, without the GUI, is not decided here.

### 8. The package never carries a secret, and a guard can prove it

A publication guard scans the finished package's manifest, definitions,
presentations, and provenance for every configured adapter field value
marked secure, every saved connection profile secret, and every absolute
path outside the package, and refuses the write on a hit. Its positive
control seeds a known secret into a fixture and asserts the refusal.

### 9. The HTML shell is a document, and a deliberately dumb server serves it

`polariton publish` may also emit `index.html`, a document that lays the
figures out on the dashboard grid with CSS, embeds or references each
`figure.svg`, declares the font face, and contains **no script**. It is
the first web code in the repository, so PRODUCT-0153 gate 3 lands with
it: the product dependency guard gains the Vega, Vega-Lite, and Altair
families for every boundary that renders or views Polariton figures, with
a positive-control specimen, and an architecture test asserts the emitted
shell contains no `<script` element.

`polariton serve <package>` serves an existing package over HTTP on
localhost by default: files by manifest path only, content types by
extension, `ETag` from the manifest digest, no directory listing, no path
outside the package, no execution, no credentials, read-only. It is built
on `hyper` 1, which the lockfile already carries through `reqwest`, and
nothing heavier. It works in the reduced head; so does publish, because
the reduced head has adapters and a GUI and lacks only Toron.

### 10. What this record does not decide

Hit metadata and any interaction; the viewer runtime; an archive encoding;
a Rune `publish` word joining PRODUCT-0136's family; command-line publish
from a saved session; importing a package back into the desktop; live
data authority; hosting, authentication, or any network exposure beyond
localhost; and any change to the desktop plotting contracts.

## Acceptance gates

1. **Digests are stable.** Publishing the same dashboard twice yields
   byte-identical manifests and equal package digests; changing one label
   changes the figure's SVG digest and the package digest and no dataset
   digest.
2. **Datasets are shared and canonical.** The distribution showcase
   publishes four figures over one dataset file; re-encoding an
   Arrow-backed and a Polars-backed artifact of equal content yields equal
   bytes.
3. **The DTO boundary holds.** The extended architecture test fails on any
   `Serialize` derive on a runtime plot, editor, or selection type and on
   any placement or renderer vocabulary in the package module.
4. **Escaping.** The fixture label round-trips through the SVG and an XML
   parser exactly.
5. **Font licence.** The package SVG through `resvg` with the package font
   is pixel-equal to the PNG export of the same figure, for one figure of
   every mark family with text.
6. **Provenance never leaks.** The publication guard refuses the seeded
   secret; every producer kind produces an entry; the certificate slot is
   absent for every producer today.
7. **The witness works.** A token bump between capture and read refuses
   assembly and names the figure.
8. **The shell and the server are dumb.** The shell contains no script;
   the server refuses a traversal path, serves `ETag` from the manifest,
   returns the manifest for the root, and lists nothing.
9. **The anti-fork guard fires.** The Vega-family specimen is refused by
   the dependency guard at the Polariton boundary.
10. **Both heads.** Publish and serve compile and their tests pass in the
    default and reduced heads, under the PRODUCT-0152 gate.
11. **The corpus moves.** The three named rows become executable citing
    the commits; no other row changes.

## Guardrails and stop conditions

1. If a figure DTO needs a placement identity, a buffer id, an artifact
   token, or renderer vocabulary to be complete, stop: the definition is
   wrong, not the guard.
2. If any producer's provenance needs a secret or an endpoint to be
   meaningful, stop and report; §4 does not bend.
3. If the package SVG and the PNG export diverge under §6.3, stop: that is
   a PRODUCT-0057 finding, not a tolerance to widen.
4. If the shell needs script to lay figures out, stop; layout is CSS or
   the grid model is wrong.
5. Recheck the record number before the plan commit; two commits, plan
   then one `impl:` cut, corrections amended under review.

## Risks

- **Definition DTO churn.** The authored definition mirrors a surface that
  grows a member at a time. Mitigated by versioning the figure schema
  separately from the manifest and by deriving the DTO from the
  presentation registry PRODUCT-0141 already makes authoritative, not by
  hand.
- **Canonical Arrow is subtler than it looks.** Dictionary encodings,
  nested types, and metadata order can all break byte equality. Mitigated
  by gate 2's paired fixture and by refusing, with a diagnostic, any type
  the canonical writer cannot make deterministic in this cut.
- **`hyper` as a direct dependency.** It is already in the graph; the risk
  is API surface, not weight. Mitigated by keeping the server to one file
  and one route shape.

## Forward

The package is the boundary every later rung consumes. The next
publication record is hit metadata — PRODUCT-0133's typed hit projected
into the package so a thin layer can answer hover without plot logic —
and it is written only after this package has been used and its gaps
observed, per PRODUCT-0153 §9.
