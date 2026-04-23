# Workstream G Sampling Policy Note

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `G3`

## Purpose

Make the pilot data-access audit policy explicit instead of implying it through whatever the current emit sites happen to do.

## Current Pilot Policy

Current live data-access audit events are not sampled.

Emission policy:

- `data.write`
  - `requested`: `100%`
  - `denied`: `100%`
  - `completed`: `100%`
- `data.write_finalized`
  - `100%`
- `data.read`
  - `denied`: `100%`
  - `completed`: `100%`
- `data.range_query`
  - `denied`: `100%`
  - `completed`: `100%`

## Why This First

The current implementation does not yet have:

- tenant compliance tiers
- per-tenant sampling configuration
- retention math honest enough to justify quietly dropping read-side evidence

So the pilot posture is intentionally simple:

- emit the minimum serious set
- emit it every time
- measure cost before adding policy complexity

That keeps the evidence story honest while the retention and archive layers are still being built out in later `G` slices.

## Fail-Closed Behavior

Dispatcher-visible data-access audit emission is fail-closed on the authoritative local chain.

Current runtime behavior:

- if local NDJSON emission fails for `data.write`, `data.read`, or `data.range_query`
- the request is refused with a top-level I/O error instead of proceeding without baseline evidence

That is deliberate for this pilot posture: the authoritative local evidence path is required, while off-box shipping remains additive.

## Authority Boundary

- dispatcher-emitted `data.write`, `data.read`, and `data.range_query` records are `node_local` authority
- `data.write_finalized` remains `raft_log` authority

This is deliberate:

- actor attribution exists at the dispatcher boundary
- durable write finalization authority exists in the committed control path

## Current Scope Boundary

Only dispatcher-validated external data requests emit the node-local data-access records above.

Explicitly included:

- unary `WRITE_REQ`
- streaming `BULK_WRITE_REQ`
- unary `READ_REQ`
- unary `QUERY_REQ`
- streaming `QUERY_REQ`

Explicitly not included:

- internal replicate / migrate traffic
- orphan-witness recovery pulls
- direct in-process test helpers that bypass the dispatcher

This keeps recovery and replication internals from double-emitting as if they were end-user or operator data access.

## Follow-On

If later work adds tenant compliance tiers or regulated-vs-non-regulated sampling, this file is the place to freeze that policy before implementation lands.
