# Implement ADR 0006: canonical generated-relation expressions

Scope: create the zero-dependency boson package, its public v1 AST and strict
owned encoding, and Toron's private post-binding lowerer. This implementation
does not add PRODUCT-0137 materialization, a catalog certificate, CTAS changes,
a Rune materialize verb, or a Polariton receipt.

## Halt

Any need for boson to depend on an editor, engine, connector, Arrow, Polars,
Rune, SQL, or serialization crate stops this cut. Unsupported relations and
scalars refuse; they do not widen the grammar opportunistically.

## Stage 0: birth and guard the package

1. Add shared/boson as an explicit root member with fully local package metadata
   and zero dependencies.
2. Add Toron's direct ../../../shared/boson path dependency; add none to
   libaryon.
3. Add reciprocal ADR-0005 architecture guards in the birth implementation.

Acceptance gates:

- Root cargo metadata --no-deps --format-version 1 lists boson; root cargo
  check -p boson succeeds.
- Nilket metadata resolves Toron's direct path dependency to the same manifest.
- Boson has no dependencies; positive-control guards reject forbidden direct
  dependencies and libaryon still rejects boson.

## Stage 1: AST and strict bytes

1. Define only the public types, validation errors, tag table, and grammar in
   ADR-0006.
2. Hand-write the encoder and decoder; add no serde or serialization dependency.
3. Add goldens for bare Range, absent versus present empty projection, named
   projection, every literal, Int32-to-Float64 Cast, Int64 identity Cast, each
   arithmetic node, Floor, and SeededUnitInterval.

Acceptance gates:

- decode(encode(value)) equals value, and re-encoding is byte-identical.
- Changing count, name, literal, target type, seed, key, child order, or tag
  changes or rejects bytes as applicable.
- Tests reject altered magic/version/tag/length/UTF-8, negative or over-cap
  counts, non-finite Float64, malformed type/arity, a duplicate Cast target
  type, and trailing bytes.

## Stage 2: private Toron lowering

1. Lower only directly bound Range or Project-to-Range plans to boson.
2. Exercise bare ctx.range, project/project_named, seeded_random, floor, and
   lit(4)'s Int32-to-Float64 coercion through Rune binding.
3. Guard against generic-plan traversal and private-plan, source, SQL, or serde
   serialization.

Acceptance gates:

- Repeated binding yields the same bytes; changed count, final alias, seed, or
  scalar tree changes them.
- Random, filter, join, scan, null, non-finite literal, TryCast, Float32,
  Decimal128, and logical types refuse.
- Lowered output names exactly equal the bound schema.
- Physical projection optimization over an execution copy leaves the already
  created definition unchanged.

## Stage 3: seal and handoff

After formatting, run root cargo test -p boson, the ADR-0005 audit family, and
Nilket's full council seal. The implementation evidence records format version,
tag table, golden bytes, refusal census, and two-workspace resolution evidence.

The future PRODUCT-0137 companion calls the lowerer before physical
optimization and stores encode_v1 bytes in its atomic catalog admission. This
implementation proves no catalog behavior.
