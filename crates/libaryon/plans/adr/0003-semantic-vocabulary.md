# ADR 0003: the semantic vocabulary

Status: accepted 2026-07-30. This record **pre-defends** the four-variant
semantic algebra's names. It exists so that future rename proposals begin
by defeating this document rather than re-deriving it. The decision:
**retain `RelationalTable`, `RelationalRow`, `Token`, `None`.**

## The algebra

`SemanticKind` (`uast/kind.rs`, `#[repr(u8)]`, closed as of `98c3066`):

| variant | discriminant | operational definition |
|---|---|---|
| `None` | 0 (`#[default]`) | The identity element of semantic interpretation: interpreting this node is equivalent to interpreting its children directly. Fan-out buckets, untouched slots, reclaimed corpses. |
| `RelationalTable` | 1 | Establishes a semantic context containing addressable members or nested contexts. |
| `RelationalRow` | 2 | One addressable member of such a context; may itself contain nested structure, including tables. |
| `Token` | 3 | **Residual syntax** not promoted to table or row. *Not* guaranteed to be a topology leaf — Rust assigns it to unpromoted interior nodes. Consumers must key leaf-ness on topology (`first_child`), never on `Token`. |

These operational definitions are the contract. The names carry intuition;
the definitions carry meaning; both must appear in `kind.rs` doc comments
(the paired implementation copies them there).

## Evidence, producer side

Seven providers lower into the algebra without any language owning the
vocabulary:

| provider | Table | Row |
|---|---|---|
| Rust | file, fn, impl, trait, struct, enum | let, expr-stmt, use, const, macro |
| Rune | file, fn, struct, enum, impl, module, block | use, const, macro call, local, expression, statement |
| Python | module, fn, class, control block | assignment, import, return, expr |
| SQL | query, create table/view | insert/update/delete, CTE, statement |
| Markdown | document, heading section | paragraph, list item, fence, quote |
| TOML | document, section | key/value entry |
| CSV | document/table | record |

Nesting facts that destroy shape-based alternatives: tables contain
tables; rows contain tables (a CTE holds a full subquery); tokens may
have children. The relational reading survives all three.

## Evidence, consumer side (the stronger witness)

The operations validated the names before anyone audited them:

- The hybrid diff partitions both documents into **Table** and **Row**
  populations, then performs join-shaped structural matching across the
  corresponding populations — an algorithm written by authors who were not
  thinking about the enum's name. This is not a claim that the algorithm is a
  formal Codd relational join.
- Folding treats structured **Tables** and **Rows** as semantic boundaries
  while excluding **None** and **Token** scaffolding. Table-specific
  structural motions (`]m`, `vaf`) walk **Tables** as scopes.
- Projection and the inspector interpret **None** transparently — the
  identity law, in production, tested.

A vocabulary that predicts algorithms written later by different hands is
an abstraction that is real, not merely tasteful.

## The roadmap bridge

The rune-data-products direction (deferred relations; the polars-adjacent
engine) is designed to bind query machinery to document structure. A CSV's
`RelationalTable` node is a natural candidate handle for such a relation,
not merely a metaphor for one. The prefix preserves the seam between the
editor's semantic algebra and its data future; shape-based or code-centric
renames would discard that useful alignment.

## The honest caveat (and its resolution)

To a Codd-literate reader, "relational" gestures at unordered,
duplicate-free set semantics — which UAST children deliberately violate
(ordered, duplicate-preserving, byte-partitioning). The prefix here means
*pertaining to the relationship between semantic levels*, not Codd
compliance. This is exactly why the operational definitions above are
normative and must live in the doc comments: the names deflect one
category error (literal tabular shape) and the definitions prevent the
other (set semantics).

## Rejected alternatives

| pair | why it loses |
|---|---|
| Table / Row | Closest competitor. Sounds concrete: a Rust `fn` as a bare "Table" reads as a category error; the prefix announces an algebraic role, not SQL syntax or rectangular data. |
| Tensor / Scalar | False shape claims: tables are neither homogeneous nor fixed-rank; rows are emphatically not scalar (they nest). No operation for which None is naturally the tensor identity. |
| Matrix / Vector | More rigidly rectangular; fails on nested functions, sections, subqueries. |
| Relation / Tuple | Relations are unordered sets; tuples imply fixed arity. Children are ordered, duplicated, nested. |
| Set / Element | Loses ordering and multiplicity — both load-bearing. |
| Scope / Statement | Excellent for code; absurd for CSV records, TOML entries, paragraphs. SQL blurs both. |
| Block / Statement | Same code-centrism; tables need not be blocks; rows may contain blocks. |
| Container / Item | Topology, not meaning — None buckets are containers too; collapses the axis the algebra just separated. |
| Branch / Leaf | Directly false: rows branch, tokens may have children, None nodes are branches. |
| Composite / Atom | The same structural lie abstracted; rows and tokens may be composite. |
| Collection / Member | Nearly content-free; cannot say why a None bucket is not a semantic collection. |
| Aggregate / Unit | Vague and overloaded in both directions. |
| Dataset / Record | Great for CSV; wrong for functions, control flow, prose. |
| Context / Clause | Clause is SQL-local; Context so broad it absorbs None. |
| Domain / Fact | Overclaims: statements are actions, declarations, errors, incomplete syntax — not facts. |
| Frame / Entry | Frame collides with stack/UI frames; Entry underspecifies. |
| Baryon / Quark (physics) | Beautiful mythology, poor API: requires lore to predict behavior. The physics stays in the explanatory model (three quarks and a flavorless identity build everything); the discriminants stay operational. |

## Consequences

- Rename proposals must engage this table first.
- `kind.rs` doc comments carry the operational definitions.
- The Token-is-not-a-leaf fact is documented before, not after, someone
  codes the assumption.
- The physics naming (baryon = 3 + 1) remains the crate's origin story and
  teaching aid, deliberately absent from the API surface.
