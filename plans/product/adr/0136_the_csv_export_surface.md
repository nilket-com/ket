# ADR 0136: the CSV export surface

Status: accepted 2026-08-19. This record gives PRODUCT-0002 §3's `export` verb
its first spelling and its first format: a Rune word that writes a named result
as a CSV file. It is the export half of the ratified plan's fourth arc; the
import half is deferred behind a Toron ingest contract this record does not
write.

Amended 2026-08-19, before implementation began: §1's path argument became an
ordinary runtime Rune string — the local host receives values, not source, so
static-literal discipline cannot hold there and belongs to the presentation
envelope if export ever appears in it; §1 scopes the shipping surface to Rune
notebook cells through the local host, since the default build routes plain
Rune buffers through the presentation pipeline this record fences off; §2
requires the receiver to stay artifact-first so the canonical Arrow path is
genuinely direct; §5's success report became an explicit export receipt from
the host to the interface, which had no such channel. No other section
changed.

It decides the authoring surface and its receivers, the canonical
serialization, the value-fidelity rule, the format defaults, and the write
discipline. It does not decide CSV import, any other export format, export of
plot images (PRODUCT-0075's repaired surface stands), options authoring,
overwrite confirmation flows, a file browser, or any menu surface — and it
adds no Polars-facing construction.

## Context

Polariton has no data egress. The word `csv` appears nowhere in the crate; the
only way to get rows out is Ctrl-C over a grid selection, as display-formatted
TSV. For a data workbench this is the last of the four adoption blockers the
2026-08-19 ratification selected, and the only one whose count is still zero.

Three recorded decisions shape where the surface lands.

**Export is already a product verb and a view kind.** PRODUCT-0002 §3 names
`export` one of five explicit relation operations — "export creates an
external artifact such as Parquet" — and §2 lists export among the things a
view presents. This record spells the verb; it does not invent it.

**Rune is the authoring surface.** The product direction is fewer chrome
surfaces, not more: the menu may not survive, and a new `Data` menu would be
new chrome for a capability the product language should own. Exports first
appear in Rune, where `workspace.result("name")` and `workspace.cell("name")`
already hand scripts the published artifacts (`rune_host.rs`,
`RuneCapabilities`).

**Arrow is the boundary; Polars is a compatibility projection.** KET-0005
rejected Polars as the product boundary; PRODUCT-0084 made Arrow authoritative
where it exists. `arrow-csv` ships a writer over the `RecordBatch`es this
crate already speaks, at the pinned Arrow version. A CSV path built on Polars
would be new construction on the seam being removed.

The import half was examined and deliberately split out. A Polars CSV source
adapter would establish the wrong boundary; an interim literal-`INSERT` loader
into Toron has an unattractive size and transaction ceiling (TORON-0030 is
literal-only DML, and `toron-client` carries no batch-ingest frame). CSV
import is CSV-to-Toron materialization — PRODUCT-0002 §3's `materialize`, with
§4's birth certificate naming the file — and it waits for a Toron Arrow-IPC
ingest contract. That is a TORON record first, then a PRODUCT record that
consumes it.

## Decision

### 1. Export is a Rune word over named receivers

```rune
workspace.result("sales").export_csv("/home/me/exports/sales.csv");
workspace.cell("trips").export_csv("trips.csv");
```

The receivers are the artifacts Rune can already address: published results
(`-- @result name`) and named notebook cells (`{#name}`). "The current
result" is a focus concept — it means something only from a menu — and Rune
addresses names, so the receivers are named. A grid-focused export can arrive
with whatever command surface the product later grows; it is not this record.

The path argument is an ordinary runtime Rune string — plain-buffer and
notebook Rune pass runtime values, as every capability word there does. The
product provides no path expansion or templating. A relative path resolves
against the process working directory; an authored absolute path is used as
written. Should export ever appear in the presentation envelope, that
surface's static-literal discipline (PRODUCT-0040/0058/0076) governs it
there.

**The shipping surface is the Rune notebook cell**, executed through the
local host. The default build routes plain Rune buffers through the
presentation pipeline, which this record leaves untouched; a plain
presentation buffer therefore cannot reach `export_csv` under this record,
and extending the envelope is a later record. The notebook host gains the
result snapshots it currently lacks — today it receives only cell
snapshots — and an execution whose only product is an export is valid: the
one-cell-one-plot rule refuses a cell that produces *nothing*, not a cell
that exports.

Export runs host-side against the artifact the host already holds. It adds no
relation verb, no Toron round-trip, and no new engine surface — it is a view
in PRODUCT-0002 §2's sense, presented as a file.

### 2. Arrow CSV serialization is canonical; the Polars projection must match it

Arrow-backed artifacts (Toron, BigQuery) write through `arrow_csv::Writer`
directly. Polars-backed artifacts (SQLite, Postgres, MySQL, Parquet) write
through an internal compatibility projection.

**The projection is held to the canonical output**: an equivalence gate writes
the same logical table through both paths and requires byte-identical CSV.
The projection is an internal detail Polars's eventual removal deletes; the
bytes are the contract.

**The receiver is artifact-first.** Resolving a named receiver must not
eagerly project the artifact through Polars; the projection happens when a
consumer needs one — a plot's `show`, not an export. An Arrow-backed result
exports through the Arrow path without ever materializing a Polars frame,
or the canonical path is direct in name only.

### 3. Full data values, never display formatting

Export writes the data, not the grid. Floats serialize at round-trip
precision, not display rounding; temporal values serialize completely, not as
truncated labels; nulls are empty fields. The clipboard's display-formatted
TSV is a presentation artifact and is not this surface's fidelity rule.

### 4. Defaults only

Header row from the schema's column names; comma delimiter; RFC-4180 quoting;
UTF-8. No delimiter, encoding, quoting, header, or type-formatting authoring
in this record — deferred by design, matching the options-free posture the
Parquet source has survived with.

### 5. The write discipline: three floors

- **The path is authored**, never assumed: no hardcoded name, no implicit
  directory beyond the process working directory for relative paths.
- **An existing file refuses.** Export creates; it never clobbers. The
  refusal is a diagnostic naming the path; the user changes the name. This is
  the same create-never-overwrites discipline as PRODUCT-0134 §4.
- **A failed write is a surfaced diagnostic**, through the Rune diagnostic
  path the word already lives in — never silence.

A successful export reports the path and row count in the routine status
lane, carried by an **explicit export receipt** from the host execution to
the interface — the host has no ambient status channel, and a receipt the
interface renders is one; prose the VM prints is not.

### 6. Missing receivers refuse with the established wording

`workspace.result("absent").export_csv(...)` fails exactly as
`workspace.result("absent")` already fails — the existing unknown-name
diagnostics, with the export adding nothing to name resolution.

## Rejected

- **A `Data ▸ Export result as CSV…` menu.** The menu may not survive, and
  the product language should own exports; a menu item would be new chrome
  for a capability Rune can spell today.
- **Exporting "the current result."** A focus concept with no Rune meaning;
  named receivers are the addressable surface. May return with a later
  command surface.
- **A Polars CSV source adapter.** New construction on the boundary KET-0005
  rejected and PRODUCT-0084 demoted.
- **An interim literal-`INSERT` CSV loader into Toron.** Wrong product
  boundary and an unattractive size/transaction ceiling; import waits for the
  Arrow-IPC ingest contract.
- **Display-formatted export.** Exporting what the grid shows loses data to
  presentation rounding; the clipboard already serves the presentation case.
- **Silent overwrite, or an overwrite-confirmation flow.** The first is the
  chart-export defect; the second is held export-hardening scope. Refusal is
  total and needs no dialog.
- **Options authoring.** Delimiters, encodings, and quote styles multiply
  without a user; the defaults are the interchange baseline.
- **Excel, JSON, or Parquet export formats.** Each is its own record;
  Parquet export is even named by PRODUCT-0002 §7 and still waits for one.
- **Exporting notebook plots or named-registry enumeration.** Out of scope;
  the receivers are the two named-artifact words that exist.

## Consequences and sequencing

**The ratified plan's fourth arc splits**, and
`plans/product/review/deferred_triage.md` § Ratification carries the
annotation beside this record: CSV **export** is selected and specified by
this record, with delivery contingent on this record's implementation plan
completing; CSV **import** is held behind a Toron Arrow-IPC ingest record and
is not schedulable from the Polariton side until that contract exists. Neither
a Polars adapter nor a literal-`INSERT` loader may be offered as an interim.

**The dependency addition is `arrow-csv` at the pinned Arrow version** —
already the crate family Polariton ships. No Polars feature is added; the
Polars `csv` feature specifically must not appear.

**The equivalence gate is the projection's leash.** The byte-identity test
between the canonical Arrow path and the compatibility projection is an
acceptance gate, not an aspiration; when Polars leaves the crate, the
projection and its gate delete together.

**Unannotated ad hoc SQL results are not exportable through this record.**
Only the named receivers are addressable: a result must be published with
`-- @result name`, or produced by a named notebook cell, before it can be
exported. An ad hoc query's grid content has no name and therefore no export
path here — the cost of the Rune-first surface, stated rather than
discovered. Naming the result is the one-line escape.

**Local Rune gains its first filesystem write.** The local presentation VM
already executes user-authored code at the user's own trust level — `init.rn`
runs at startup — and `export_csv` writes only where the authored path
points, refuses existing files, and creates no directories. A broader
filesystem policy for the local VM, if ever wanted, is its own record.

**Deferred by design, each requiring its own record:** CSV import (behind the
Toron ingest contract); Parquet export (PRODUCT-0002 §7); other formats;
options authoring; directory creation; overwrite flows; a grid-focused export
on a future command surface.
