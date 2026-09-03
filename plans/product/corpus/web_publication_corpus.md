# Web publication acceptance corpus

Version 0.1. Charter: PRODUCT-0153 §8. This is a living, versioned acceptance
artifact — not an ADR. It records the destination ladder for the web
programme's two lanes with honest per-rung status. Rows are outcomes, never
mechanisms. A row's status changes only with landed, cited evidence; rows are
added only by a record that names them. "Pending" rows are the destination
stated in advance, not omissions. No row uses the word "static": each names
its data authority, initial rendering, and interaction separately.

Statuses: **executable** (landed with cited evidence) · **pending**
(destination, not yet implemented) · **proposed** (under consideration; no
accepted record, and not a commitment) · **deferred-by-design** (excluded
deliberately by a cited record).

## Publication lane

| rung | data authority · initial rendering · interaction | status | evidence |
|---|---|---|---|
| shared figure image: one figure leaves the desktop as a faithful Polariton SVG or raster | snapshot · pre-rendered · passive | pending | — |
| snapshot dashboard publication: a package with figure identity, specification, resolved presentation, shared dataset digests, provenance, and fallback renders | snapshot · pre-rendered · passive | pending | — |
| served snapshot publication: an existing package served without credentials, adapters, or execution | snapshot · pre-rendered · passive | pending | — |
| inspectable published dashboard: hover and inspection over published hit metadata | snapshot · pre-rendered · local over existing geometry | pending | — |
| navigable published dashboard: local pan, zoom, and selection consuming the desktop's settled vocabulary | snapshot · pre-rendered · local over existing geometry | pending | — |
| linked published dashboard: linked highlighting and cross-filtering over packaged data | snapshot · pre-rendered · local | pending | — |
| recomputing published dashboard: axes, bins, aggregation, or hit geometry recomputed by Polariton logic, locally or server-backed | snapshot · either · recomputed | pending | — |
| live published dashboard: re-queried on view against a native workspace | live · either · server-backed | pending | — |
| browser authoring: source edited in the browser, executed by a hosted native workspace | live · either · server-backed | pending | — |

## Distribution lane

| rung | status | evidence |
|---|---|---|
| verified Linux desktop artifacts with content-addressed manifests | pending | — |
| verified macOS and Windows artifacts of the reduced head, built and linked on target | pending | — |
| saleable official downloads with entitlement and fulfilment | pending | — |
| update channels for purchased artifacts | pending | — |

## Deferred by design

| family | record |
|---|---|
| a browser library as plot authority, fallback renderer, or canonical publication form | PRODUCT-0153 §2 |
| credentials, adapter handles, workspace paths, or live execution inside a package | PRODUCT-0153 §4 |
| a browser as the authoritative workspace | PRODUCT-0153 §1 |
| deciding the browser runtime before a rung needs semantic recomputation | PRODUCT-0153 §6 |

## Change log

- v0.1 (2026-09-03): chartered by PRODUCT-0153 with every publication and
  distribution rung pending; four families deferred-by-design.
