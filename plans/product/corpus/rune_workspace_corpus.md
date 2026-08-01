# Rune workspace acceptance corpus

Version 0.2. Charter: PRODUCT-0002 §9. This is a living, versioned acceptance
artifact — not an ADR. It records the complete destination matrix for the Rune
workspace with honest per-family status. A row's status changes only with
landed, cited evidence; the only expansion path for new rows is dogfood diary
evidence. "Pending" rows are the destination stated in advance, not omissions.

Statuses: **executable** (landed with cited evidence) · **pending** (destination,
not yet implemented) · **deferred-by-design** (excluded deliberately by a cited
record).

## Relation construction and transformation

| family | status | evidence |
|---|---|---|
| workspace table lookup (`workspace.table`) | pending — first-contact target | — |
| select / projection | pending — first-contact target | — |
| filter / predicate | pending — first-contact target | — |
| group / aggregate (`group_by` + `summarize`) | pending — first-contact target | — |
| sort | pending — first-contact target | — |
| limit | pending — first-contact target | — |

## Views

| family | status | evidence |
|---|---|---|
| bounded table view (`table(r)`, inherits the 128 MiB `toron-client` collection ceiling) | pending — first-contact target | — |
| experimental bar sink (`plot.bar`) | pending — first-contact target | — |
| experimental scatter sink (`plot.scatter`) | pending — first-contact target | — |
| durable live plot | pending | — |
| durable pinned plot | pending | — |

## Mutation and definition lifecycle

| family | status | evidence |
|---|---|---|
| create | pending | — |
| insert | pending | — |
| update | pending | — |
| delete | pending | — |
| drop | pending | — |
| save (definition) | pending — blocked on artifact-store ADR | — |
| cache | pending | — |
| materialize | pending | — |
| pin | pending | — |
| export | pending | — |

## Durability

| family | status | evidence |
|---|---|---|
| materialized restart durability | pending | — |

## Deferred by design

| family | status | record |
|---|---|---|
| bounded finite `collect` | deferred-by-design | `plans/product/impl/rune_relation_first_contact.md` |

## Change log

- v0.2 (2026-08-01): "streamed table view" corrected to "bounded table view"
  with the inherited 128 MiB ceiling cited, per PRODUCT-0003; rows and
  statuses otherwise unchanged.
- v0.1 (2026-08-01): created with the first-contact plan; all rows pending.
