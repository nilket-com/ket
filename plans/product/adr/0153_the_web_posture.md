# ADR 0153: the web posture

Status: accepted 2026-09-03, proposed and revised the same day after review
found the two lanes serialized against their own independence, the
anti-fork guard inconsistent with the adapter it permits, and the browser
boundary drawn wider than the workspace boundary it protects. This PRODUCT
record spans Polariton, the shared
plot boundary, the Toron client, and release. It names the destination
posture for everything the product does on the web — selling the desktop
software, publishing dashboards, viewing them in a browser, and one day
authoring there — and charters the acceptance corpus that measures it. It
decides no mechanism, no package format, no server, no viewer runtime, and
no storefront provider. It is the map; the records it sequences are the
cuts.

## Context

### What exists

Polariton draws every plot through one trait, `PlotBackend`
(`crates/polariton/src/plot/common.rs`), with three implementations: the
production iced canvas, an SVG backend that is both an export format and the
plotting arc's test oracle, and a PNG backend that rasterizes the SVG.
PRODUCT-0057 licenses what that SVG can and cannot prove about production
pixels; PRODUCT-0149 §8 makes exact headless rendering its own contract.
Interaction is arriving on the desktop now: PRODUCT-0133 gave hits a type,
PRODUCT-0150 gave the viewport a gesture whose pan and zoom are pane-tier
state that never reaches Rune, and PRODUCT-0151 gave selection an exact
scalar vocabulary, a closed admission list, and a rebuild recipe beside the
kernel. None of it is finished; the operator's own words are that pan and
zoom are not yet dialed and cross-filtering does not exist.

The data side has its nouns. PRODUCT-0002 makes a local Toron workspace the
authority and gives relations identity, residence, provenance, and lifetime.
PRODUCT-0136 spells `export`; PRODUCT-0137 spells `materialize` with a birth
certificate whose implementation is still blocked; PRODUCT-0133 §5 mints a
process-local `ArtifactToken` for every publication. The Toron client
speaks QUIC with mutual TLS, which no browser can open.

The release side has almost nothing. The code is MIT. Windows and macOS are
cross-checked, never built, linked, or packaged, and PRODUCT-0152 found the
cross-target loop red on this host in both heads because one crypto
dependency reached through Polars cannot be cross-compiled here. The local
Toron workspace is Linux-only, so the only head that can ship elsewhere is
the reduced one PRODUCT-0152 just repaired.

### What the web asks, in the operator's order

1. A web store that sells the desktop products and is itself a software
   product.
2. A sharing and distribution model for Polariton dashboards, with more
   than one delivery mode, a server with or without HTML among them.
3. Some of Polariton in a wasm web application, with the honest admission
   that a browser cannot be a workspace host.
4. A successor to sqlplot — connect, author, plot, share — that reuses the
   plotting kit instead of reaching for a browser charting library.

### The fear this record exists to name

The plotting kit is six months of relation, layout, typography, theme, and
rendering contracts. The failure mode is not that the web is slow to
arrive; it is that the web arrives as a second plotting product whose
figures merely resemble Polariton's because a browser library was the path
of least resistance. That outcome is refused here by name, and every later
web record inherits the refusal.

### The browser's envelope

A browser exposes limited analogues of files, memory, and networking and
possesses none of the authority a Polariton workspace has: no sockets it
chooses, no io_uring, no RDMA, no access to arbitrary host memory, and no
way to open the Toron client's native mutual-TLS QUIC protocol. Whatever
runs there is a viewer or an editor of intent. The workspace stays native,
local or hosted.

## Decision

### 1. The browser is a host, never the workspace

No browser-resident code is ever the authority for a relation's identity,
residence, provenance, or execution. A browser may display a publication,
hold viewer state, edit source, and send intent to a native workspace. It
never receives database or source credentials and never holds the
authority to act as the workspace; it may hold a bounded credential to a
hosted workspace service and may communicate with that workspace only
through its browser gateway, never through Toron's native protocol or
arbitrary sockets. It may not execute adapters or mint durable identity.
This is PRODUCT-0002 §1 restated at the edge.

### 2. Polariton is the sole plot authority on every surface

Polariton remains the only semantic, layout, and rendering authority for
Polariton figures wherever they are shown. SVG, Canvas, HTML, WebGPU,
JavaScript, and wasm are drawing substrates and hosts, never replacement
plot authorities. Vega-Lite and its relatives are not a fallback web
renderer and not a canonical representation of anything this product
publishes; any future compatibility with them is an explicit import or
export adapter that lowers into or projects from Polariton's own
specification, and needs its own record. A rung that can only be reached
by adopting a second plot authority is not reached.

### 3. Two lanes, sharing one discipline

The web programme has two lanes that share nothing but the identity
discipline of TORON-0581 — content-addressed artifacts, versioned
manifests, identity separated from navigation hints — borrowed, not
claimed:

- **Publication:** a dashboard leaves the desktop as a snapshot package and
  is viewed, served, inspected, and eventually interacted with elsewhere.
- **Distribution:** desktop builds are produced, verified, signed, sold,
  and updated. The storefront is exterior to this workspace and lives in
  its own repository; the build, package, verify, and manifest commands live
  here, and provider orchestration stays outside the source tree as
  `ci/README.md` already requires.

### 4. The publication boundary is a snapshot package, and "static" is retired

The unit of publication is a package whose data, provenance, and initial
figure state are immutable and content-addressed, and which carries
Polariton-owned figure identity, the plot specification, the resolved
presentation, references to shared dataset digests, and pre-rendered
fallback assets. It never carries credentials, adapter handles, workspace
paths, or live execution. Its format is the subject of the next record.

Three axes of a publication are independent and are named separately from
now on; the word "static" is not used for any of them:

- **data authority:** snapshot or live;
- **initial rendering:** pre-rendered by native Polariton or rendered in
  the browser;
- **interaction:** passive, locally interactive over existing geometry,
  locally recomputed, or server-backed.

The initial publication rungs are snapshot and pre-rendered. Passive
publication is an admitted rung, not an interaction ceiling.

### 5. Interaction matures rung by rung and follows the desktop

Web interaction consumes the vocabulary the desktop has settled and never
invents a competing one. While PRODUCT-0150 and PRODUCT-0151 are still
moving, the web does not lead them. The rungs, in order: faithful snapshot;
hover and inspection over published hit metadata; local pan, zoom, and
selection as the native contracts stabilize; linked highlighting and
cross-filtering over packaged data; local or server recomputation only when
an interaction demonstrably requires it; live hosted execution much later.

Two kinds of interaction are distinguished at every rung. A **transform**
translates or scales geometry that already exists and needs no plot logic.
A **semantic recomputation** changes axes, bins, aggregation, level of
detail, or hit geometry and needs Polariton's plot logic to run somewhere.
Only the second kind raises the question of where.

### 6. The browser runtime is a non-decision until a rung needs recomputation

Whether recomputation runs as extracted Polariton logic in wasm or as
native Polariton behind a server is not decided here and is not decided by
the package record. It is decided by a later record from controlled probes:
one identical figure and package, three disposable viewers — pre-rendered
SVG with browser-native behaviour, SVG plus a thin script over hit
metadata, and the extracted plot core in wasm — compared on fidelity, font
behaviour, accessibility, startup and bundle cost, data copying,
interaction latency, export, and debuggability, with the apparatus held
still. Every candidate preserves one semantic and package boundary: wasm
requires the plot-core extraction, a script requires durable hit metadata
in the package, a server requires a publication protocol; none acquires
plot-semantic authority.

### 7. Distribution posture

The distribution lane sells official builds, updates, and support of
MIT-licensed software unless a licensing record decides otherwise; that
decision is the operator's and is named here as open, not made. No
artifact is saleable until it is really built, linked, and packaged on its
target and verified by the identity discipline of §3. The cross-target red
PRODUCT-0152 found is diagnosed by an observational probe that traces every
dependency path to the failing crate; any change of TLS provider or
dependency feature that the probe recommends is a security and packaging
decision that belongs to the release-artifact record.

### 8. The web acceptance corpus is chartered

`plans/product/corpus/web_publication_corpus.md` is the living, versioned
acceptance artifact for this programme, in the form PRODUCT-0002 §9 gave
the Rune workspace corpus. Its rows are outcomes, never mechanisms —
"inspectable published dashboard", not "wasm viewer"; "saleable verified
desktop artifacts", not a provider. A row's status changes only with
landed, cited evidence. A rung is not promoted while its row is pending,
and no record may declare a rung reached that the corpus does not show.

### 9. Sequencing

Records are reserved by topic and numbered at commit time, since the last
collision came from numbering early. Each lane has its own sequence, and a
lane never waits on the other.

**Publication lane:**

1. the published dashboard package — snapshot structure and versioning,
   canonical Arrow data and digests, source-neutral provenance with the
   PRODUCT-0137 certificate as an optional attachment, figure specification
   and resolved presentation, font carriage, SVG escaping and its
   production evidence license, headless render and publish, and a
   deliberately dumb server for an existing package;
2. observation of what interaction the shipped package actually lacks,
   then the §6 probes, then the viewer record;
3. a hosted workspace and browser gateway, only if authoring in the
   browser is ever pursued.

**Distribution lane:**

1. the §7 dependency probe, observational, in parallel with the package
   record;
2. the release-artifact contract, informed by the probe;
3. the licensing decision and the merchant-of-record choice, both the
   operator's;
4. the storefront, in its own repository.

### 10. What this record does not decide

The package format, the server, the viewer runtime, the storefront
provider, the licence, the hosted workspace, any Rune spelling, any plot
pixel, and any change to the desktop interaction contracts.

## Acceptance gates

1. **The corpus exists with every publication and distribution rung
   pending**, its deferred-by-design rows each citing the section that
   excludes them, and cites this record as its charter.
2. **Every later web record names its lane and the corpus row it
   advances**, and no row leaves pending without a landed, cited cut.
3. **The anti-fork invariant has a guard that can fail.** The first cut
   that adds web code also guards it: no boundary that renders or views
   Polariton figures may depend on a Vega, Vega-Lite, or Altair package,
   with a positive-control specimen that visibly fires. The existing
   product dependency guard reads Cargo manifests only, so that cut extends
   guarding to whatever manifest and source forms it actually introduces —
   package manifests, import maps, vendored or CDN scripts. A §2 adapter
   lives in its own boundary under a recorded exception the guard names.
4. **The three axes are named, not "static".** No later web record or
   corpus row uses "static" for a publication; each names data authority,
   initial rendering, and interaction separately.

## Guardrails and stop conditions

1. If a rung can be reached only by a second plot authority, stop; §2 is
   the reason the programme exists.
2. If a viewer needs plot semantics the package does not carry, stop and
   extend the package record rather than computing them in the browser.
3. If the browser-runtime question is raised before a rung needs semantic
   recomputation, decline it and cite §6.
4. If a cut sells or ships an artifact that was cross-checked but never
   built on its target, stop; §7 forbids it.
5. Recheck the next record number immediately before every plan commit.

## Risks

- **The map ossifies.** Mitigated by the corpus: rows change with evidence,
  and §9's order is a sequence, not a schedule.
- **Interaction envy.** The desktop's gesture and selection contracts will
  keep moving through this programme; the web's answer is to follow, per
  §5, and to accept a lower interaction rung for longer than feels
  comfortable.
- **The distribution lane is blocked before it starts.** The cross-target
  red is real and unowned; §7's probe exists so the release-artifact record
  starts from a diagnosis rather than a hope.

## Forward

The published dashboard package is next. It is the boundary every later
rung consumes, it needs nothing the desktop does not already have, and it
is the first cut that can prove, in a browser, that a Polariton figure is
still a Polariton figure.
