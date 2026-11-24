# Brainstorm 020: Live memory on the database website

The Wendelon memory investigation is a starting point for product
observability. In development we assemble RSS, allocator readings, storage
counters, and workload logs after each run to see what the database was
doing when memory grew. An operator needs those same signals aligned while
the database is running, plus enough history to investigate an incident
later.

Build a first-party memory view around the questions that came up in those
runs. A live chart near the top of the database website would be its public
face. Visitors could watch a demo cluster take writes, run storage maintenance,
and see how memory changes under a declared workload.

## One set of signals, three uses

- **Development.** Open a captured run and line up memory with ingest and
  storage maintenance. Compare two runs with their workload, build, allocator,
  and node budget visible. Keep detailed allocation profiles and one-off
  probes available when the ordinary signals leave a question unanswered.
- **Operations.** Open the same timeline on a live cluster. Start with
  headroom and recent changes, then inspect the node under pressure. Use
  allocator or owner accounting where available to ask whether live memory
  is growing, or whether the allocator or OS is holding more pages.
- **Public demo.** Publish selected cluster aggregates from a dedicated
  demo deployment. Keep the workload and its current activity in view so
  visitors can judge a rise, a plateau, or a drop in context.

## What the memory view shows

- **Footprint and headroom.** Show total memory charged to the monitored node
  cgroups, their combined configured budget where finite, and the highest
  percentage used by any node. The last figure makes a tight node visible
  even when the cluster has room in aggregate. Include a process RSS series
  with its own label.
- **What the database accounts for.** Plot tier-history and relocation-map
  charges as named series. These are Wendelon's internal budget gauges.
  A later version could add rewrite buffers or live heap bytes if their
  accounting is suitable for continuous collection. Keep those quantities
  out of an RSS breakdown unless their relationship to RSS is established.
- **Work on the same timeline.** Show ingest rate and storage-maintenance
  activity, such as tiering and retention rewrites, so a visitor can tell
  whether a flat line coincides with work continuing or stopping. Include
  the demo's node count, memory budget, and workload.
- **Freshness and coverage.** Include the sample time and how many nodes
  reported. Show a gap or label partial data when a scrape is missing.
  A stale node should never become a zero in the cluster total.

## First version to discuss

A read-only collector could scrape the authenticated Wendelon metrics
listener and collect the OS memory figures from each node. A custom chart
could start with a saved capture from the memory investigation, then take
live samples with the same metric definitions. That would let us refine
the view using real debugging questions before asking operators or
visitors to rely on it.

The public feed would publish a short time series of allowlisted cluster
aggregates. The database metrics endpoint would retain its existing
access controls. The website could continue to serve its last published
view during a collection outage with its age clearly labelled.

The current exporter already has
`wendelon_tier_history_bytes` and `wendelon_relocation_redirect_bytes`.
Check each proposed series against its source before adding it to the public
feed. In particular, verify how the chart handles a missed scrape, a
restarting node, and a single node approaching its budget while the rest of
the cluster stays low.

One design choice is whether to start with a continuously running, repeatable
demo workload or open on the latest live window and let visitors select
recent periods of heavier activity. Both can use the same aggregate feed.
Per-node drill-down belongs in the authenticated operator view.

Observability can be part of the product line with its own interface.
Keep the existing OpenMetrics listener available for integrations and
design the first-party experience without a Grafana dependency. Ownership
of collection, history, and presentation stays open. Toron and Wendelon
can supply telemetry while the collector and interface live elsewhere.

The page demonstrates observed behavior for the declared deployment and
time window. Section 8 and the 1 GiB node budget keep their own qualification
criteria.

[Wendelon observability](../ops/observability.md) ·
[Memory gauge definitions](../../src/metrics/registry.rs) ·
[System visibility brainstorm](012_system_visibility_and_introspection.md)
