# Close the semantic algebra

Scope: rename the topology-neutral `SemanticKind::Group` value to the
concrete enum variant `SemanticKind::None`, make it the explicit zero-state
of the semantic-kind column, and make every inspector surface display that
stored truth. This is a semantic-model correction with lifecycle
consequences, not an `Option<SemanticKind>` conversion. Deliverable is one
`impl:` commit reviewed against this plan.

## Algebra

`SemanticKind` is the universal semantic classification carried by every
published UAST node:

```rust
#[repr(u8)]
pub enum SemanticKind {
	None = 0,
	RelationalTable = 1,
	RelationalRow = 2,
	Token = 3,
}
```

`None` is a value, not missing storage. Its interpretation is the identity:

```text
interpret(None(children)) = interpret(children)
interpret(None([]))       = empty
```

That law is already the behavior of the value currently named `Group`.
Semantic-mode inspection projects through it; folding and navigation reject
it as a target; structural interpretation uses its children rather than
assigning language meaning to the container. The node's edges carry the
physical fact that it groups children. The semantic component contributes
no flavor.

The three concentric absences stay distinct:

- `SemanticKind::None`: a node exists but makes no universal semantic claim;
- `ProviderKindId::NONE`: it has no provider-local syntax identity;
- `Option<NodeId>::None`: no node exists.

`Token` remains a positive classification for a genuine text extent. It is
not the default for an object that has not earned a published meaning.

## Verified lifecycle facts (2026-07-30)

- `UastRegistry::kinds` pages currently initialize every untouched slot as
  `SemanticKind::Token`.
- `RegistryChunk::spawn_node` has no implicit published kind: every caller
  passes a kind, and `initialize_reserved_node` writes it before setting
  `slot_published`.
- `scrub_node_side_tables` does not currently reset `kinds` or
  `provider_kinds`. A reclaimed reusable slot therefore retains its previous
  classifications until the next publication overwrites them.
- The bounded reclaim cursor captures a node's children before calling the
  registry reclaim operation. Clearing classification during that operation
  cannot lose traversal.
- Resolver I/O does not interpret semantic/provider kinds. Object reuse still
  waits for I/O-lifetime quiescence and generation validation exactly as
  required by ADR 0002.
- `PagedArray` constructs typed values through its initializer; it does not
  currently expose raw zeroed/decommitted memory.

## Zero-state contract

1. Give `SemanticKind` `#[repr(u8)]`, put `None = 0`, and assign explicit
   discriminants to all four values. This makes zero an honest, stable
   representation rather than relying on declaration order.
2. Initialize untouched semantic-kind pages to `None`.
3. When the bounded reclaim visit makes an orphan inert, replace both its
   semantic kind and provider kind with `None`/`NONE` under the existing
   registry synchronization. Rename the registry operation if needed so its
   name does not claim it only drops payload after it also retires
   classification and object lifetime.
4. A new publication must still receive an explicit semantic kind. Recycled
   initialization overwrites the inert state before `slot_published = true`.
   Genuine roots, physical extents, mutation-born leaves, and test-node
   helpers that allocate actual text nodes remain `Token`.
5. Do not use `None` as a liveness test. `slot_published`, packed object
   generation, and checked `NodeId` dereference remain the only identity
   authority.

This makes the semantic-kind column compatible with a future zero-filled
page representation and prepays one narrow part of ADR 0002 P3. It does
**not** authorize page decommit or discharge its page-ownership,
reader-quiescence, generation-column, or other component-default proofs.

## Structural use

Replace every former `SemanticKind::Group` branch with
`SemanticKind::None`, preserving behavior:

- complete-source graft fan-out nodes are `(None, ProviderKindId::NONE)`;
- semantic UAST inspection projects through them;
- fold and navigation boundaries exclude them;
- structural diff may use them as neutral partition buckets;
- provider fallback matches remain exhaustive;
- Baryon and Polariton graph surfaces retain a neutral visual treatment.

Topology vocabulary remains valid outside the semantic component. Local
names such as `group_depth`, `groups`, or `fanout` may describe edges and
layout. No stored or protocol semantic-kind value may continue to say
`Group`.

## Display honesty

All surfaces render the stored semantic answer:

- text and DOT output use `none`, never `group`, for the semantic value;
- graph-node fallback labels use `none`;
- raw text may append a derived `topology=fanout` marker when a `None` node
  has children, alongside its existing child count and branch storage;
- semantic mode remains transparent and hides neutral fan-out scaffolding;
- frontend fill colors may remain neutral but their match arm is `None`.

Do not store a second origin/group bit merely to preserve the old label.
The fan-out fact is derived from the node's edges and child count.

## Contract documentation

Update the live object-identity documentation to state that a bounded reclaim
visit removes semantic and provider classification as well as payload. A
fully reclaimed object reads as the semantic/provider zero-state until its
slot becomes unpublished or is explicitly republished with a new generation.
Historical implementation plans remain historical.

## Tests and audits

1. Representation: `None as u8 == 0`, all discriminants are pinned, and
   `size_of::<SemanticKind>() == 1`.
2. Untouched page slot: test-only inspection observes `None`, not `Token`,
   without publishing the slot.
3. Reclaim transition: a published nonzero semantic/provider pair becomes
   `None`/`NONE` during the bounded reclaim visit.
4. Reuse transition: the same slot is republished with the requested
   non-`None` kind and current generation; no inert classification leaks into
   the new object.
5. Structural graft: fan-out nodes are `(None, NONE)`, preserve bounded
   descent, and remain transparent to semantic inspection and folding.
6. Display: raw text and DOT say `none`; raw fan-out rows derive
   `topology=fanout`; no inspector output says `group` as a semantic value.
7. Diff and frontend graph regression tests remain green with exhaustive
   matches updated.
8. A source audit rejects `SemanticKind::Group` in current Rust sources and
   rejects using `SemanticKind::None` as a substitute for NodeId currency.
9. Full libaryon, Baryon, and Polariton suites plus strict Clippy pass.

## Non-goals

- No optional semantic-kind column.
- No topology, graft, folding, diff, or provider-taxonomy redesign.
- No page decommit, raw-zero allocation, id/lifetime change, or new stored
  origin component.
- No rewrite of historical plan documents that accurately record the old
  name.

## Review checklist

- [ ] The four-value algebra is explicit and `None = 0`.
- [ ] Every published node still receives an explicit kind.
- [ ] Untouched/reclaimed storage is semantically and provider neutral.
- [ ] `None` is never used to decide whether a NodeId is live.
- [ ] Fan-out behavior is unchanged but stored/displayed semantic truth is
      `none`.
- [ ] Inspector/DOT/frontends do not re-smuggle `group` as semantic meaning.
- [ ] ADR 0002 P3 benefit is described narrowly; page decommit remains
      unauthorized.
- [ ] Representation, reclaim, reuse, graft, display, and source-audit tests
      pass.
