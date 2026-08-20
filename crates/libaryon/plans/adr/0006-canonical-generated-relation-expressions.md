# ADR 0006: canonical generated-relation expressions

Status: accepted 2026-08-20, converged by the KET council (user, Claude, Codex).
This record creates the shared canonical-expression contract required by
PRODUCT-0137. It decides a narrow, versioned v1 value and encoding for
generated relations only. It does not authorize table creation, catalog
certificates, a materialize Rune verb, or a Polariton receipt.

## Context

PRODUCT-0137 requires each durable generated table to retain a neutral,
versioned relation definition. No public value exists. Toron's LogicalPlan and
BoundExprProgram are private engine representations; Rune and SQL source freeze
an ingress rather than preserving a neutral semantic definition.

KET-0005 assigns jointly consumed contracts to the interchange boundary, and
PRODUCT-0001 permits a root shared package for that exact case. The first
payload is deliberately narrow: TORON-0569's eager, capped generated range.
It is not a shared executor, planner, dataframe residence, or general relation
algebra.

## Decision

### 1. Boson owns the shared value

Create shared/boson as an explicit root-workspace member named boson. Its
manifest declares all package metadata locally, has zero dependencies, and uses
neither workspace-inherited package metadata nor workspace dependencies. The
KET council owns amendments to its public formats.

Toron, Baryon, and Polariton may depend on boson. Boson depends on none of
them, libaryon, any engine, connector, Arrow, Polars, Rune, SQL, or
serialization crate. Its birth implementation adds reciprocal architecture
guards: ADR-0005's libaryon denylist continues to reject boson, while boson
rejects editor heads, libaryon, Toron, Wendelon, connectors, Arrow, Polars,
Rune, SQL, and serialization crates.

Nilket's Toron crate consumes this exact package only through a direct
../../../shared/boson path dependency. It does not inherit the root workspace.
Root metadata must list the package; Nilket metadata must resolve that same
manifest.

### 2. One closed v1 semantic value

Boson exposes CanonicalRelationExpression, carrying format version one:

    GeneratedRange {
      count: i64,                    // 0 <= count <= 1 << 28
      projection: None | ordered Vec<Projection>
    }

    Projection {
      output_name: exact UTF-8 string,
      expression: ScalarExpression
    }

The source is one non-null Int64 ordinal with values [0, count). None is bare
ctx.range(n), never a rewritten project. A present projection is the exact
bound output, including an empty schema if Toron admits one. The value is
structural, not algebraically normalized: ordinal + 1 and 1 + ordinal remain
different definitions.

The v1 scalar tree admits only non-null ordinal; non-null Int32, Int64, and
finite Float64 literals; explicit numeric Cast; typed Add, Sub, Mul, Div;
Floor; and SeededUnitInterval { seed, key }. Every scalar records its bound
numeric type. SeededUnitInterval has two non-null Int64 children and a
non-null Float64 result under TORON-0569. Floor preserves its admitted numeric
child's type. Arithmetic children and result share the recorded numeric type,
so coercion is an explicit child Cast.

The tree fixes structure and bound types, not execution behavior. Overflow,
division, and all other evaluation semantics ride PRODUCT-0003's
language-surface coordinate stored beside the definition in PRODUCT-0137's
certificate; boson neither duplicates nor redefines them.

TryCast, Float32, decimal coercions, logical-type metadata, nulls, non-finite
floats, RANDOM, generic scalar functions, and every unlisted node refuse with
a public unsupported-expression error. Existing entropy RANDOM remains PerCall
and is not reseeded.

### 3. V1 has exactly one owned byte encoding

encode_v1 emits:

    magic "BOSONREL" | u16-be format version (1) | Relation

All tags are fixed one-byte discriminants. 0x00 and every unlisted byte are
invalid. Counts and UTF-8 byte lengths are u32 big-endian; i64 values are
fixed-width two's-complement big-endian; Float64 literals are to_bits() as u64
big-endian.

| Domain | Tag | Meaning |
| --- | --- | --- |
| relation | 0x01 | GeneratedRange |
| projection form | 0x02 | no projection |
| projection form | 0x03 | ordered projection list |
| projection entry | 0x04 | one entry |
| numeric type | 0x10 | Int32 |
| numeric type | 0x11 | Int64 |
| numeric type | 0x12 | Float64 |
| scalar | 0x20 | ordinal |
| scalar | 0x21 | Int32 literal |
| scalar | 0x22 | Int64 literal |
| scalar | 0x23 | Float64 literal |
| scalar | 0x24 | Cast |
| scalar | 0x25 | Add |
| scalar | 0x26 | Sub |
| scalar | 0x27 | Mul |
| scalar | 0x28 | Div |
| scalar | 0x29 | Floor |
| scalar | 0x2a | SeededUnitInterval |

Relation is 0x01, i64 count, then a projection form. 0x02 has no payload.
0x03 has a u32 entry count followed by that many 0x04, u32 UTF-8-name-length,
name-byte, Scalar entries. A Scalar is its tag, result-type tag, then payload:
ordinal has none; literals have their fixed-width value; Cast has exactly one
child and its already-serialized result-type tag is the target; arithmetic has
left then right child; Floor has one child; SeededUnitInterval has seed then
key child.

An identity Cast is well-formed and preserved when child and result types are
equal. Toron's explicit-cast binder deliberately emits that author-authored
shape, while its implicit cast-if-needed helper elides one. The value is
structural provenance, not a normalizer. A Cast has no second type field.

The decoder validates magic, version, tags, lengths and allocation bounds,
UTF-8, range bound, literal finiteness, scalar type and arity, and end of
input. It rejects unknown versions, malformed or duplicate Cast target data,
and trailing bytes. There are no maps, implicit defaults, serialization-library
choices, or alternative equivalent forms.

V1 bytes are the certificate definition and equality identity; there is no
separate digest. Future work retains a V1 decoder or performs explicit,
lossless certificate migration before removing one. Changed semantic nodes or
bytes require a new format version.

### 4. Toron lowers at the definition boundary

Toron owns a private one-way lowerer from the directly bound materialize source
to CanonicalRelationExpression. The later materialization companion invokes it
in Binder::bind_create_table_as_select immediately after
bind_plan_with_trace(source) and before the outer binder performs projection
optimization. It accepts only ExecutionPlan::Range or a direct
ExecutionPlan::Project over Range. The project's BoundExprProgram and final
output_schema provide explicit types, coercions, and names.

The immutable value travels beside the execution plan. Later physical planning
may alter only the execution copy, never the definition. The lowerer does not
traverse generic plans or fall back to Rune source, SQL text, debug output,
serde, or a private-plan blob.

## Rejected

- Full relation algebra, scans, joins, filters, aggregates, windows, external
  sources, and generic functions: a replacement planner is not this cut.
- Rune or SQL source, LogicalPlan, or BoundExprProgram serialization: none is
  the neutral public definition PRODUCT-0002 §6 requires.
- Encoding before binding or after physical optimization: the former loses
  explicit coercions/names; the latter makes provenance execution-dependent.
- Generic scalar IDs, algebraic normalization, or a separate digest.
- Assignment of tags at implementation seal: an independent decoder must be
  able to implement this record.
- Any boson dependency or materialization/catalog/Rune surface in this cut.

## Consequences

KET-0006 supplies the implemented prerequisite that PRODUCT-0137 requires.
The later Toron companion consumes this value and still owns catalog
certificate admission and the Rune materialize verb. No companion code lands
before this record is implemented. Streaming ranges, non-generated relations,
richer scalar families, content addressing, certificate viewers, and CSV ingest
each require their own record.
