# ADR 066: FSST Predicate Evaluator

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/066_fsst_evaluator.md`

## Status

Council-converged 2026-05-09 after four iterations across
Claude / Codex / Gemini. V1 implementation is staged
default-off; local Phase 7 evidence is recorded and default-on
promotion remains blocked pending council review and cleared gates.

V1 contract:

- **Streaming token decode (Strategy A)** is the V1 byte-
  comparison strategy. Pre-encoded literal compare
  (Strategy B) is structurally unsafe under the current
  WFS1 encoder and becomes V2 work gated on a writer
  amendment that enforces row-safe FSST framing.
- **Multi-buffer variable-width predicate leaf** is the
  V1 planner shape (validity + offsets + values). FSST
  is the first variable-width native predicate path in
  ADR 058's catalog; the row-group predicate planner
  extends in place to fetch the three buffers together
  for FSST leaves.
- **Arrow offsets are decoded byte positions**, not
  encoded byte ranges. The kernel streams the encoded
  WFS1 payload from byte 0, advances decoded position as
  tokens emit, and uses offsets only as decoded row
  boundaries. Deriving per-row compressed slices from
  Arrow offsets is a correctness violation; an
  architectural guard pins this.
- **Decoded-length pre-filter** via
  `offsets[i+1] − offsets[i]` is the primary economic
  lever for `InList` and length-mismatch rows; saves
  byte-comparison and scratch-copy work, not stream-walk
  work.
- **No plan-time fetch-byte economics gate in V1.**
  Phase 0 audit confirmed dense Case B fetch already
  uses encoded ranges (per
  `src/storage/row_group.rs`
  `encoding_fetch_range_for_buffer_page`), so a
  fetch-byte comparison is a no-op. The kernel's win
  is CPU economics (avoiding dense plaintext
  materialization), not fetch bytes; Phase 7 bench
  measures wall-clock across short-string and weak-
  compression adversarial fixtures, and a CPU-cost
  gate is V2 if those fixtures show regression.
- **WFS1 header is authoritative** for the FSST symbol
  table; the kernel does not consult
  `EncodingCascade::Fsst { symbol_table_byte_len }`. The
  cascade field's disposition (currently populated as
  zero) is out of scope per ADR 066's evaluator-only
  scope discipline; cleanup belongs in a separate
  trivial directory-layout follow-up.
- **Closed atom set**: the FSST kernel engages on
  `Eq`, `Neq`, `InList` over `Utf8` / `LargeUtf8` /
  `Binary` / `LargeBinary`. `IsNull` / `IsNotNull`
  remain on the existing ADR 059 validity-only path
  (no FSST kernel engagement; no offsets / values
  fetch needed for null evaluation). All other atoms
  decline to dense via the existing pre-kernel
  eligibility gate.
- **Promotion gates** align with ADR 060 / 064 / 065
  cadence: ≥1.5× predicate wall-clock at clustered 5%
  on representative FSST-compressible UTF-8 fixtures,
  ≥1.2× end-to-end through ADR 065 → ADR 060 / 064
  Case A, ≤5% dense-fallback overhead on truly declined
  paths. Headline diagnostics (recorded, not gating):
  ADR 059's 2× CPU speedup, and kernel-vs-dense CPU
  overhead recorded on short-string and weak-compression
  adversarial fixtures (kernel engages; no V1 numeric
  threshold; informs V2 CPU-cost gate decision).

Phase 0 audit pins the FSST cascade encoding shape, the
offsets-buffer encoding shape, the dense-fallback site,
and constructs the boundary-crossing-symbol and
no-compressed-row-slice regression fixtures. Bench
diagnostic evidence, including full local encoding-compute and
row-group matrices, is recorded at
`plans/impl/066_fsst_evaluator_bench_results.md`; default-on
promotion remains pending cleared gates.

## Context

ADR 058 ships a closed 6-variant predicate evaluator
catalog (`BitPacked`, `RlePlain`, `RleBitPacked`,
`DictionaryPlain`, `DictionaryBitPacked`, `Delta`);
`Plain`, `Lz4Frame`, `Fsst`, `Alp` cascades fall back to
materialize-then-evaluate. ADR 059 ships native kernels
for the catalog members and flips
`EncodingComputeConfig::default()` to enabled with the
four-cascade default predicate allow-list (`BitPacked`,
`DictionaryPlain`, `DictionaryBitPacked`, `Delta`). ADR
065 ships the projection-side bridge that survives FSST
dense fallback by zipping decoded predicate columns with
sparse-projected remaining columns (Case B) or skipping
the zip when there is no output overlap (Case C).

FSST (Fast Static Symbol Table) is the dominant
compressed-string representation in warehouse OLAP. It
targets high-cardinality string columns — URLs, log
lines, identifiers, free-form text, JSON keys — where
Dictionary loses on cardinality and Plain loses on
bytes. ADR 057's `Fsst` cascade ships decode-only:
FSST-cascade buffers materialize as dense Arrow `Utf8` /
`Binary` arrays before predicate evaluation, populating
`predicate_projection_by_group` and routing to ADR 065
Case B/C dense fallback today.

The motivating V1 workload is the high-cardinality
string predicate where the FSST column is in the
filter but **not** in the output (Sub-case A1; see
Decision §12):

```sql
-- Sub-case A1 — kernel engages, ADR 065 Case B/C →
-- ADR 060 / 064 Case A migration:
SELECT status, latency_ms, response_bytes
FROM access_log
WHERE url_path = '/checkout/confirm'
```

Today this fully decodes the FSST `url_path` buffer,
scans Arrow strings for equality, populates
`predicate_projection_by_group`, and routes to ADR
065 Case B/C. With ADR 066, the kernel evaluates at
encoded grain and emits mask-only `PhaseBOutcome`;
the mask flows directly into ADR 060 / 064 Case A for
the fixed-width remaining columns (`status`,
`latency_ms`, `response_bytes`).

The adjacent query shape — where the FSST predicate
column is **also** in the output (predicate-output
overlap on the FSST column) — stays on the existing
ADR 065 Case B path in V1 because variable-width
sparse projection is V2-deferred per ADR 060 / 064:

```sql
-- Sub-case A2 — kernel does not engage in V1; stays
-- on existing ADR 065 Case B path:
SELECT url_path, status, latency_ms
FROM access_log
WHERE url_path = '/checkout/confirm'
```

ADR 066 narrows ADR 065's V1 engagement surface for
Sub-case A1 queries; Sub-case A2 unblocks when
variable-width sparse projection ships (V2).

### Five forces

**1. FSST is variable-width and requires multi-buffer
evaluation.** The FSST values buffer contains no row
delimiters. Row boundaries live in the companion
offsets buffer (Arrow's standard variable-width
layout). To evaluate row-by-row, the kernel cannot scan
values in isolation; it must fetch and walk validity +
offsets + values together. ADR 066 is therefore both a
new evaluator kind and the first multi-buffer variable-
width native predicate path in ADR 058's catalog; the
row-group predicate planner extends in place to fetch
the three buffers together for FSST leaves.

**2. Arrow offsets are decoded byte positions; the
kernel must stream the encoded payload from the
start.** Arrow's variable-width layout uses offsets
that index into the *logical decoded* byte stream:
`offsets[i+1] − offsets[i]` is the decoded length of
row i. The encoded WFS1 values buffer has its own
internal byte positions which Arrow offsets do not
index. The kernel therefore cannot slice the encoded
buffer by `[offsets[i], offsets[i+1])`; it must walk
the encoded payload from byte 0, advancing decoded
position as tokens emit, and use offsets only as
decoded row boundaries. Deriving per-row compressed
slices from Arrow offsets is a correctness violation;
an architectural guard pins this.

**3. Cross-row-boundary symbols mandate streaming
token decode.** The current `WFS1` encoder compresses
the entire values buffer as one stream; it does not
reset symbol boundaries at Arrow value boundaries. A
single two-byte FSST symbol may span the boundary
between two adjacent Arrow values, contributing
decoded bytes to row N and row N+1 from one encoded
token. Pre-encoding a literal and comparing its
encoded bytes against any "row-encoded byte range" is
doubly unsafe (the range itself is undefined per
force #2, and even if defined would carry partial
symbols). Streaming token decode — walking the
encoded payload from the start, emitting decoded
bytes into a per-row scratch buffer
(≤ longest-literal bytes) when the decoded position
is inside a length-matching row, comparing scratch to
literal — preserves row-boundary correctness, handles
escapes through one parser, avoids whole-column
allocation, and keeps the dense decoder as the single
oracle. Strategy B (pre-encoded literal compare) is V2
work gated on a writer amendment that enforces row-
safe FSST framing boundaries.

**4. The decoded-length pre-filter is the primary
economic lever for `InList` and length-mismatch
rows.** Bucket literals by decoded byte length once
per buffer. For each row, the decoded length is
`offsets[i+1] − offsets[i]` (cheap; computed before
any decode). If no literal matches the row's decoded
length, the kernel can determine
`row_matches_any_literal = false` without byte
comparison (the row's mask bit is then op-specific:
non-survivor under `Eq` / `InList`, survivor under
`Neq`); the streaming decoder still advances its
decoded position past the row's bytes (skipping the
scratch copy), so stream-walk cost remains O(decoded
buffer length) overall. On `InList(N)` predicates
with literals of varying lengths, this turns per-row
byte-comparison work from O(N) into O(1)–O(K) where
K is the count of literals of the row's matching
length. The pre-filter saves byte-comparison and
scratch-copy work, not stream-walk work.

**5. ADR 065 already pays for the dense bridge in
Sub-case A2; ADR 066 closes the Sub-case A1 win.**
Without ADR 066, FSST equality / membership
predicates pay full FSST decode + dense Arrow string
scan + ADR 065 Case B/C zip in *all* cases. With ADR
066, **Sub-case A1** queries (FSST predicate column
not in output) evaluate at encoded grain and emit
mask-only `PhaseBOutcome` — engaging ADR 060 / 064
mask-only sparse projection (Case A) for the remaining
fixed-width projection columns. **Sub-case A2**
queries (FSST predicate column in output) stay on the
existing ADR 065 Case B path in V1 because variable-
width sparse projection is V2-deferred — the FSST
column has to be dense-decoded for output regardless,
and ADR 065 Case B already optimizes that case. The
ADR 066 V1 win is therefore scoped to Sub-case A1
queries; Sub-case A2 unblocks when V2 variable-width
sparse projection lands.

### What this ADR does not propose

- **No new `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` / `PredicateAtomOp` /
  `EncodedRowMask` variant.** Reuses the existing
  taxonomies.
- **No format change.** ADR 057 `Fsst` cascade byte
  layout, symbol-table storage, buffer-relative
  offsets, and `BufferEncoding` shape all unchanged.
  The kernel does not modify, depend on, or remove
  `EncodingCascade::Fsst { symbol_table_byte_len }`;
  cascade-field cleanup is a separate (trivial)
  directory-layout follow-up out of scope here per
  ADR 066's evaluator-only V1 boundary.
- **No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.**
- **No page-grain FSST.** ADR 057 ships FSST as full-
  buffer-grain decode with a length-1
  `page_end_offsets` prefix array spanning the full
  buffer; ADR 066 evaluates at buffer grain. Page-
  grain FSST is a separate ADR (it would unblock ADR
  063 page stats and ADR 060 sparse projection on
  FSST).
- **No range, LIKE, regex, or substring predicates.**
  FSST does not preserve byte-lexicographic order in
  encoded form; `<` / `>` / `<=` / `>=` decline to
  dense fallback. The current `PredicateAtomOp`
  catalog has no `LIKE` / regex / substring variants,
  so prefix matching is not in scope. V1 atoms
  engaged by the FSST kernel: `Eq`, `Neq`, `InList`.
  `IsNull` / `IsNotNull` route via the existing ADR
  059 validity-only path and do not engage the FSST
  kernel.
- **No FSST aggregate folds.** String `min` / `max` /
  `count_distinct` on encoded form is a separate
  kernel proof and requires collation policy.
- **No sparse projection eligibility lift for FSST.**
  FSST is variable-width; ADR 060 / 064 deferral
  inherits unchanged. ADR 066 produces masks, not
  projection columns.
- **No symbol-table caching across queries.** Per-
  buffer parse is V1.
- **No compressed-literal byte equality (Strategy B).**
  Structurally unsafe under the current `WFS1` encoder
  per §"Five forces" #3 (boundary-crossing symbols).
  V2 work gated on a writer amendment.
- **No per-row compressed slicing.** Arrow offsets
  are decoded byte positions per §"Five forces" #2;
  the kernel must stream the encoded payload from the
  start and use offsets only as decoded row
  boundaries.
- **No constant-grain Disjoint short-circuit via
  literal unencodability.** FSST's escape byte can
  encode arbitrary bytes; literal-encoding-fails-
  therefore-Disjoint is not a structural truth.
  Disjoint claims in V1 come from the length pre-
  filter only.
- **No new public PBC opcode, wire `MsgType`,
  `NodeCapability` bit, IPC `custom_metadata` key,
  descriptor field, `ObjectStore` trait method, or
  Cargo feature.**
- **No retirement of the ADR 057 dense FSST
  fallback.** It remains the decline path for non-V1
  atoms and for buffers that fail eligibility (atom
  / type / coercion / offsets-fetchable / Sub-case
  A1).
- **No expansion to `Alp`, `Lz4Frame`, or `Plain`
  evaluator kernels.** Each is its own follow-up ADR.

### Constraints carry forward unchanged

ADR 003, 016, 019, 020, 022, 029, 030, 035, 036, 037,
039, 040, 042, 043, 044, 052, 053, 054, 055, 056, 057,
058, 059, 060, 061, 062, 063, 064, 065. ADR 057 cascade
catalog and `BufferEncoding` shape unchanged. ADR 058
evaluator catalog *extends by one* (`Fsst`); the
existing six remain shipped. ADR 059 default predicate
allow-list extends by `Fsst` only if Phase 7 bench
evidence clears the council's promotion gates. ADR 060
/ 063 / 064 / 065 surfaces unchanged.

## Decision

ADR 066 ships a native FSST predicate evaluator
extending ADR 058's catalog by one variant (`Fsst`).
The kernel evaluates equality and set-membership atoms
(`Eq` / `Neq` / `InList`) over multi-buffer FSST-encoded
variable-width predicate leaves (validity + offsets +
values) using streaming token decode, and produces mask-
only `EncodedRowMask` output integrated through ADR
058's existing predicate plan modes (Exact / Prefilter /
DenseFallback).

1. **Multi-buffer variable-width predicate leaf is the
   V1 shape.** ADR 066 is the first native predicate
   path that consumes a column's three Arrow buffers
   together:
   - **validity buffer** (optional; consumed for 3VL
     composition via `kernels/validity.rs`);
   - **offsets buffer** (required; 32-bit `Utf8` /
     `Binary`, 64-bit `LargeUtf8` / `LargeBinary`;
     itself potentially encoded under a fixed-width
     cascade — if the offsets buffer carries an
     `EncodingDirectory` entry, decode through the
     existing cascade decoder before kernel entry);
   - **values buffer** (required;
     `EncodingCascade::Fsst`).

   The row-group fetch planner extends to fetch
   validity + offsets + values together for FSST
   predicate leaves; ADR 058's existing single-buffer
   dispatch shape does not change for non-FSST
   cascades.

2. **Atom set is closed and small.** The FSST kernel
   engages on `Eq`, `Neq`, `InList`. `IsNull` /
   `IsNotNull` continue to route through the existing
   ADR 059 validity-only path (no FSST kernel
   engagement; no offsets / values fetch needed for
   null evaluation). All other `PredicateAtomOp`
   variants on FSST cascades route to dense fallback
   via the existing pre-kernel eligibility gate. Type
   catalog: `Utf8`, `LargeUtf8`, `Binary`,
   `LargeBinary`. Literal coercion follows the
   existing `LiteralCoercion` discipline; coercion
   mismatch declines to dense.

3. **Streaming token decode is the V1 byte-comparison
   strategy.** The kernel walks the encoded WFS1
   payload **once** from byte 0; rows are delineated
   via the offsets buffer's decoded byte positions.
   The kernel computes an *equality / membership match*
   per row (`row_matches_any_literal`), then applies
   op-specific inversion before validity composition:
   - initialize a streaming token reader at the start
     of the encoded WFS1 payload;
   - for each row i (in order):
     - compute decoded length from
       `offsets[i+1] − offsets[i]`;
     - look up decoded length in the literal-length
       bucket (length pre-filter);
     - if no literal of that length: set
       `row_matches_any_literal = false`; advance the
       streaming reader past the row's decoded bytes
       (no scratch copy, no byte comparison);
     - if one or more literals of that length: copy
       decoded bytes into per-row scratch
       (≤ longest-literal bytes) as the streaming
       reader emits them; on row completion, byte-
       compare scratch to the length-matching
       literals to determine
       `row_matches_any_literal`;
     - apply op-specific inversion to compute the row
       mask bit:
       - `Eq`: bit = `row_matches_any_literal`;
       - `InList`: bit = `row_matches_any_literal`;
       - `Neq`: bit = `!row_matches_any_literal`
         (so a length-mismatch row, which cannot
         equal any literal, satisfies `Neq`);
     - validity composition via
       `kernels/validity.rs` folds the bit with the
       row's validity to produce the final mask
       contribution.

4. **The kernel does not slice the encoded buffer by
   Arrow offsets.** Offsets are decoded byte
   positions; slicing the encoded buffer by
   `[offsets[i], offsets[i+1])` would index into a
   different stream than where the row's encoded bytes
   live. The kernel reads the entire encoded payload
   in one streaming pass.

5. **Length pre-filter is always-on.** Literal
   bucketing by decoded byte length runs once per
   buffer; per-row pre-filter via offsets runs before
   any scratch copy or byte comparison. Encoded-
   length-bound pre-filter (skip stream advance for
   structurally impossible rows) is V2 optimization.

6. **Symbol-table parse is bounded and per-buffer.**
   FSST symbol tables are ≤ 255 entries × ≤ 8 bytes
   ≤ ~2 KiB. The kernel parses the table once per
   buffer per query, holds it in bounded scratch
   (stack or arena; no global cache in V1). The parse
   is header-driven: the `WFS1` header is the single
   source of truth for symbol-table length. The
   kernel does not consult
   `EncodingCascade::Fsst { symbol_table_byte_len }`;
   that cascade field's disposition is out of scope
   for ADR 066 per §"Status" and is a separate
   directory-layout follow-up.

7. **Validity composition is central.** Reuses
   `kernels/validity.rs` (introduced in ADR 059); FSST
   kernel does not implement local 3VL. `IsNull` /
   `IsNotNull` reuse the existing ADR 059 validity-
   only path; the FSST kernel never engages on those
   atoms.

8. **No probabilistic shortcuts.** No bloom filters,
   no symbol-table hash prefilters, no unencodability-
   based Disjoint claims (FSST's escape byte makes
   them invalid) in V1.

9. **Pre-kernel eligibility gate at ADR 058's
   existing layer.** Engagement requires:
   - cascade is `Fsst` (with any leaf);
   - atom is `Eq` / `Neq` / `InList` (`IsNull` /
     `IsNotNull` route via the existing ADR 059
     validity-only path);
   - column type is `Utf8` / `LargeUtf8` / `Binary` /
     `LargeBinary`;
   - literal coerces cleanly to bytes for the column
     type;
   - offsets buffer is fetchable (decoded inline or
     via cascade decoder);
   - **the FSST predicate column is NOT in
     `requirement.output_columns`** (Sub-case A1; see
     Decision §12 — Sub-case A2 routes through ADR
     065 Case B as today, so the kernel does not
     engage when the FSST predicate column would also
     need dense materialization for output).

   Anything else routes to dense via the existing
   `EncodingComputeFallbackReason::UnsupportedProgram`
   path (atom mismatch) or via the planner declining
   to `EncodingComputeFallbackReason::UnsupportedCascade`
   on FSST + predicate-output overlap, which falls
   through to today's ADR 065 Case B path.

10. **No plan-time fetch-byte economics gate in V1.**
    Earlier drafts proposed comparing
    `fsst_predicate_fetch_bytes` against
    `dense_predicate_fetch_bytes` at plan time.
    Phase 0 audit revealed this comparison is a
    no-op: the existing dense fallback fetch path
    (`src/storage/row_group.rs`
    `encoding_fetch_range_for_buffer_page`)
    substitutes encoded byte ranges when an
    `EncodingDirectory` entry exists for the
    buffer, so the dense Case B path *already*
    fetches `values_encoded + offsets + validity`
    — the same bytes the FSST kernel path would
    fetch. The kernel's win is **CPU economics**
    (avoiding dense values-plaintext
    materialization and the subsequent Arrow
    string scan), not fetch bytes. V1 therefore
    does not gate engagement on a fetch-byte
    comparison; the kernel always engages on
    eligible (Sub-case A1, V1 atom, V1 type, valid
    coercion, offsets fetchable) FSST predicates.
    Phase 7 bench measures the CPU win across
    representative fixtures including short-
    string and weak-compression adversarial
    cases; if those rows show wall-clock
    regression vs. dense Case B, the council
    adds a CPU-cost gate as V2 work. Runtime
    invariant failures continue to fail-closed
    via the existing `EncodingComputeRejectCode`
    taxonomy regardless.

11. **Default enablement is Phase-7-evidence-gated.**
    Like ADR 059's RLE outcome, ADR 066 ships the
    kernel implementation even if Phase 7 evidence
    misses the council's promotion gates; default
    routing for `Fsst` predicates is controlled by
    the existing `enabled_predicate_evaluators`
    allow-list. Phase 7 records bench evidence;
    council decides default inclusion at promotion.

12. **Mask output is byte-identical to dense oracle;
    downstream dispatch splits by predicate-output
    overlap on the FSST column.** ADR 066 produces
    mask-only `PhaseBOutcome` when engaged; the mask
    is byte-identical to the dense decode-then-scan
    oracle on every (atom × type × selectivity ×
    boundary-crossing-symbol) combination. Downstream
    of the kernel, two sub-cases distinguish how the
    output materializes:

    - **Sub-case A1 — FSST predicate column NOT in
      `requirement.output_columns`** (no predicate-
      output overlap on the FSST cascade column): the
      mask flows directly into ADR 060 / 064 Case A
      mask-only sparse projection for the remaining
      (fixed-width-sparse-eligible) projection
      columns. This is the clean migration from ADR
      065 Case B/C to Case A — the byte savings stack
      (kernel encoded-grain predicate evaluation +
      sparse projection narrowing on remaining
      columns).

    - **Sub-case A2 — FSST predicate column IS in
      `requirement.output_columns`** (predicate-
      output overlap on the FSST cascade column):
      ADR 060 / 064 cannot materialize a variable-
      width column via sparse projection (variable-
      width sparse projection is V2-deferred per ADR
      060 / 064). The encoding-compute planner
      detects this overlap and declines to
      `EncodingComputeFallbackReason::UnsupportedCascade`,
      which falls through to today's ADR 065 Case B
      path: the predicate path uses dense decode-
      then-scan for the FSST column (today's ADR 065
      Case B behavior). The FSST kernel does not
      engage in this sub-case in V1; engagement is
      gated by the planner's overlap check before
      kernel dispatch. Variable-width sparse
      projection (V2) would later unblock kernel
      engagement in Sub-case A2 by allowing the FSST
      column itself to materialize at survivor grain.

    Sub-case A2 means ADR 066's V1 win is scoped to
    queries where the FSST predicate column is *not*
    in the SELECT list (e.g., `SELECT status,
    latency_ms FROM access_log WHERE url_path =
    '/foo'` engages the kernel; `SELECT url_path,
    status FROM access_log WHERE url_path = '/foo'`
    stays on ADR 065 Case B). The motivating-query
    section above describes a Sub-case A2 example
    that ADR 066 V1 does not narrow on its own; ADR
    065 Case B already covers that query shape.

    FSST predicates that decline the kernel for other
    reasons (non-V1 atom such as `Lt`; unsupported
    type; bad literal coercion) continue to populate
    `predicate_projection_by_group` and engage ADR
    065 Case B/C unchanged. Corruption / invariant
    failures during kernel execution (frame parse
    error, offsets non-monotonic, decoded-length
    mismatch) fail closed via
    `EncodingComputeRejectCode` per ADR 058
    discipline; they do **not** route to ADR 065
    Case B/C dense fallback.

The slab-format byte layout, AAD shape, nonce
derivation, key hierarchy, footer HMAC scope, ADR 022
footer layout, ADR 056 directory shape, ADR 057
encoding directory shape, ADR 058 / 059 evaluator
catalogs *(by addition only)*, ADR 060 sparse
projection planner module location, ADR 061 closed
chunk-size set, ADR 062 sizing profile catalog, ADR
063 page-stats shape, ADR 064 nullable validity
helpers, and ADR 065 manager three-case dispatch are
all unchanged.

## Architectural inextricability

Six design choices are physically inseparable in V1.

**1. Multi-buffer leaf is the new planner shape.** ADR
058's existing native evaluators all consume a single
values buffer (plus optional validity). FSST is the
first variable-width native predicate leaf, and it
requires the row-group planner to fetch validity +
offsets + values together before kernel invocation.
Treating FSST as a single-buffer kernel either misses
row boundaries (offsets buffer is the source of truth
for variable-width row spans) or forces a dense
decoded values buffer (defeats byte economy). The
multi-buffer planner shape is load-bearing.

**2. Arrow offsets are decoded byte positions; the
kernel must stream the encoded payload from the
start.** `offsets[i+1] − offsets[i]` is the decoded
length of row i. The encoded WFS1 values buffer has
its own internal byte positions which Arrow offsets
do not index. Slicing the encoded buffer by
`[offsets[i], offsets[i+1])` would index into a
different stream than where the row's encoded bytes
live. The kernel must walk the encoded payload from
byte 0, advancing decoded position as tokens emit,
and use offsets only as decoded row boundaries. An
architectural guard pins this.

**3. Streaming token decode is required by the WFS1
encoder's stream-grain framing.** A two-byte FSST
symbol may span an Arrow value boundary, contributing
decoded bytes to row N and row N+1 from one encoded
token. Pre-encoded literal compare against any "row-
encoded byte range" is doubly unsafe (the range
itself is undefined per inextricability #2, and even
if defined would carry partial symbols). Strategy B
(pre-encoded compare) is V2 work gated on a writer
amendment that enforces row-safe FSST framing. V1
mandates streaming token decode; the dense FSST
decoder remains the single correctness oracle; the
kernel reuses the writer's decoder via the streaming
token reader.

**4. Length pre-filter must run before scratch
copy.** The decoded-length-mismatch survivor pruning
is the kernel's primary economic claim on `InList`
workloads with literals of varying lengths.
`offsets[i+1] − offsets[i]` is cheap; comparing it to
the literal-length bucket before copying decoded
bytes into per-row scratch is the discipline that
makes streaming-walk-then-discard cheaper than
streaming-walk-then-scratch-then-compare.

**5. Symbol-table parse is once per buffer per
query.** The table is small (≤ 2 KiB) but parse cost
is non-trivial and must be amortized across all rows
in the buffer. Per-row parse defeats the kernel.

**6. The dense-fallback path remains the single
correctness oracle.** ADR 066 mask output must be
byte-identical to dense decode-then-scan on every
correctness fixture, including boundary-crossing-
symbol fixtures and the no-compressed-row-slice
regression fixture (where a token crosses a row
boundary and the matching row begins inside that
token). The dense path is the kill-switch target if
a kernel invariant fails post-promotion; removing it
is out of scope.

## Identity-vs-Locality framing

ADR 066 is identity-neutral and locality-neutral. It
changes how a predicate is evaluated against an
FSST-encoded buffer; row identity, record location,
and locality decisions are made upstream. Predicate
evaluation produces a row-aligned survival mask
exactly as the other ADR 058 / 059 native kernels do.
There is no separate "FSST plane" — Unified Ring rule
applies.

Cross-replica determinism is preserved because (a)
FSST encoding under a fixed symbol table is greedy
and deterministic in the writer's encoder, (b) the
streaming token decoder is deterministic given the
symbol table, (c) length pre-filter is deterministic,
(d) byte-compare is deterministic, and (e) the
kernel introduces no runtime branches that vary
across replicas. Mask byte-equality across replicas
holds.

## V1 scope

### 0. Phase 0 audit (as-built)

Phase 0 audit ran read-only against the staged
substrate; the section below records the pinned
contract:

**FSST cascade and frame:**

- `EncodingCascadeKind::Fsst` definition site;
- the actual `WFS1` frame layout (per
  `src/storage/encoding/cascade/codec.rs`
  `encode_fsst_bytes` / `decode_fsst_bytes`):
  4-byte magic `b"WFS1"`, 1-byte symbol count
  (≤ 127 — the writer caps at this; bit 7 of a
  payload byte is the symbol-vs-literal flag and
  `0xff` is reserved as the escape sentinel),
  `2 × symbol_count` bytes of symbol table (each
  symbol is exactly a 2-byte pair), then the
  encoded payload. **Decoded length is not stored
  in the frame** — it comes from
  `BufferEncoding.decoded_byte_len` for the FSST
  values buffer (the existing decoder accepts
  `expected_byte_len: RecordBodyByteLen` from the
  caller).
- `EncodingCascade::Fsst { symbol_table_byte_len }`
  current state — verify the kernel can parse the
  symbol table from the WFS1 header without
  consulting this cascade field (cascade-field
  disposition is out of scope for ADR 066 per
  §"Status"); the kernel reads
  `BufferEncoding.decoded_byte_len` for the
  expected decoded byte length, which is what the
  existing decoder already does.
- the FSST decoder entry point (Strategy A reuses
  this via the streaming token reader);
- whether the existing decoder separates symbol-table
  parse from value decode (preferred: separable; the
  kernel reuses the parser);
- **boundary-crossing-symbol fixture construction**:
  build a fixture where a frequent two-byte symbol
  spans adjacent Arrow values; verify the streaming
  token decoder produces correct per-row decoded
  bytes when the symbol's decoded bytes land in two
  adjacent rows;
- **no-compressed-row-slice regression fixture**:
  build a fixture where a token crosses a row
  boundary AND the matching row begins inside that
  token; this is the load-bearing fixture against
  the compressed-row-slice mistake.

**Offsets buffer for variable-width Arrow types:**

- the typical writer cascade for `Utf8` / `LargeUtf8`
  / `Binary` / `LargeBinary` offsets buffers (likely
  `BitPacked` delta-of-Int32 or similar);
- whether `BufferEncoding` carries an
  `EncodingDirectory` entry for the offsets buffer;
- the existing fetch path that decodes encoded
  offsets;
- **confirm Arrow offsets for FSST-cascaded
  variable-width columns are decoded byte
  positions** (not encoded byte positions);
  `offsets[i+1] − offsets[i]` is the decoded length
  of row i; the encoded WFS1 values buffer has its
  own internal byte positions that Arrow offsets do
  not index. If Phase 0 finds the writer emits
  encoded-position offsets instead, ADR 066
  escalates to an ADR 057 amendment first.
- the row-group predicate planner's current single-
  buffer assumption (validity + values today; needs
  validity + offsets + values for FSST leaves).

**Evaluator catalog and dispatch:**

- `EncodingComputeEvaluatorKind` — count of variants;
  whether `#[non_exhaustive]` or otherwise admits
  one-variant addition cleanly;
- `evaluator_kind_for_cascade` — current
  `Fsst → None` mapping;
- `encoding_compute_predicate_data_type_supported` — current type
  catalog (UTF-8 / Binary likely excluded);
- the dense-fallback site for FSST predicates: which
  pre-kernel eligibility gate fires; which
  `EncodingComputeFallbackReason` is observed
  (expected: `UnsupportedCascade`);
- `EncodingComputeConfig.enabled_predicate_evaluators`
  shape (set / vec / bitmap).

**Planner and fetch:**

- the row-group predicate planner's encoded-leaf
  fetch construction site (expand to multi-buffer
  for FSST);
- the encoded-page cap
  (`max_encoded_pages_per_record` per ADR 058) —
  confirm one FSST leaf counts as one chunk, not
  three;
- the row-group fetch plan structure
  (`RowGroupFetchPlan` per ADR 056) — whether multi-
  buffer leaves are expressible without a new type.

**Validity composition:**

- the `kernels/validity.rs` composition entry point
  ADR 066 needs;
- existing 3VL helpers reused.

**Bench harness:**

- the COCD bench harness
  (`benches/encoding_compute.rs` or equivalent) —
  whether it already generates FSST data; what
  shape; row counts; cardinalities;
- whether ADR 065's bench rows already include FSST
  predicate fixtures (those become Case A migration
  rows post-066);
- the row-group bench (`benches/row_group_fetch.rs`)
  — whether it can host the end-to-end FSST → Case A
  composition row.

Phase 0 also verifies the symbol-table parse cost is
bounded and worth amortizing once per buffer. If
Phase 0 finds the FSST cascade is implemented in a
way that prevents in-place multi-buffer evaluation
(e.g., symbol table is per-row, or per-value framing
requires decode beyond the streaming reader's
surface), implementation escalates to a separate ADR
057 amendment first. ADR 066 V1 does not silently
expand scope.

### 1. FSST kernel module

A new module `src/storage/encoding/compute/kernels/fsst.rs`
(sibling to ADR 059's per-cascade kernel modules)
hosts:

- `parse_fsst_frame(buffer_bytes: &[u8]) -> Result<FsstFrame, EncodingComputeRejectCode>`
  — parses the WFS1 frame once per buffer; returns
  an opaque `FsstFrame` carrying the symbol table,
  escape byte, and decoded-length recovery state.
- `decode_offsets(offsets: &[u8], data_type: &DataType, encoding: Option<&BufferEncoding>) -> Result<DecodedOffsets, EncodingComputeRejectCode>`
  — decodes 32-bit / 64-bit Arrow offsets,
  dispatching through the existing cascade decoder
  if the offsets buffer is encoded.
- `evaluate_fsst_equality(frame: &FsstFrame, offsets: &DecodedOffsets, validity: Option<&ValidityBitmap>, literals: &[LiteralBytes], op: PredicateAtomOp) -> Result<EncodedRowMask, EncodingComputeRejectCode>`
  — buffer-grain walker. Streams the encoded WFS1
  payload once from byte 0, advancing decoded byte
  position and current row index from the offsets
  buffer. For each row: computes decoded length from
  `offsets[i+1] − offsets[i]`; if no literal of that
  length, advances stream position past the row's
  decoded bytes and emits non-survivor (no scratch
  copy); otherwise copies decoded bytes into per-row
  scratch (≤ longest-literal bytes) and byte-compares
  to length-matching literals. Emits mask via existing
  shape selector. **The kernel does not derive per-
  row compressed slices from Arrow offsets** —
  offsets are decoded byte positions only.

Symbol identifiers are semantic (no `Phase` / `Step`
/ version suffixes; no project-name prefix).

### 2. Pre-kernel eligibility extension

Extend ADR 058's pre-kernel eligibility predicate to
admit FSST buffers when:

- atom is `Eq` / `Neq` / `InList` (`IsNull` /
  `IsNotNull` are validity-only and never reach the
  FSST kernel — they route through the existing ADR
  059 validity-only path);
- type is `Utf8` / `LargeUtf8` / `Binary` /
  `LargeBinary`;
- literal coerces cleanly to bytes for the column
  type;
- offsets buffer is fetchable (decoded inline or via
  cascade decoder).

All other FSST-cascade predicates fall through to
dense via the existing `UnsupportedCascade` /
`UnsupportedProgram` fallback paths.

### 3. Evaluator catalog extension

Add `Fsst` (label `"fsst"`) to ADR 058's
`EncodingComputeEvaluatorKind`. Update `ALL`,
`as_str`, `from_label`, config validation, and
architectural guards. Update
`evaluator_kind_for_cascade` so
`EncodingCascade::Fsst → EncodingComputeEvaluatorKind::Fsst`
(it currently maps to `None`). Update
`encoding_compute_predicate_data_type_supported` to admit `Utf8` /
`LargeUtf8` / `Binary` / `LargeBinary` only when the
evaluator kind is `Fsst` (evaluator-aware type
eligibility — non-FSST evaluator kinds do not inherit
UTF-8 / Binary support).

`enabled_predicate_evaluators` default extends per
Phase 7 evidence.

### 4. Row-group planner shape extension

The row-group predicate planner currently builds a
single fetch range per encoded predicate leaf
(validity + values). FSST extends this to three
ranges (validity + offsets + values). The change is
local to the planner's encoded-leaf fetch
construction and does not introduce a new public
planner type or trait. Each FSST row-group leaf
counts as **one** encoded chunk for the existing
encoded-page cap (the multi-buffer fetch is a single
logical leaf, not three).

### 5. Validity composition

Reuse `kernels/validity.rs` for 3VL. FSST kernel
produces a two-valued mask plus a validity bitmap;
central composition folds them. `IsNull` /
`IsNotNull` reuse the existing ADR 059 validity-only
path entirely.

### 6. Mask-shape selection

Reuse ADR 058's `EncodedRowMask` shape selector
(`Bitmap` / `Runs` / `All` / `None`). FSST equality
at high cardinality is unlikely to produce long
runs; default `Bitmap` shape with the existing
`max_mask_bytes_per_record` cap.

### 7. No plan-time fetch-byte economics gate

V1 ships **without** a plan-time fetch-byte
economics gate. The dense FSST fallback already
substitutes encoded byte ranges via
`encoding_fetch_range_for_buffer_page`
(`src/storage/row_group.rs:1018-1044`) when an
encoding directory exists, so the kernel and the
fallback fetch the same encoded payload. The
kernel's win is **CPU economics** (avoiding a dense
plaintext materialization pass before the
predicate), not fetch-byte savings. CPU-cost
adversarial cases are policed by the Phase 7 bench;
a CPU-cost gate is V2 if Phase 7 surfaces a
dense-faster regime.

### 8. WFS1 header authority

The kernel reads the FSST symbol-table length and
contents from the `WFS1` header authoritatively. ADR
066 does not modify, depend on, or remove
`EncodingCascade::Fsst { symbol_table_byte_len }`;
that cascade field's disposition is out of scope per
ADR 066's evaluator-only V1 boundary (§"Status") and
is a separate (trivial) directory-layout follow-up.
An architectural guard asserts the kernel does not
consult the cascade field.

### 9. Two-step dispatch discipline

Pre-kernel eligibility may decline FSST predicates
to dense fallback (atom mismatch, type mismatch,
literal coercion failure). Once the kernel begins parsing the
WFS1 frame, invariant failures (frame parse error,
offsets non-monotonic, decoded-length mismatch)
return through ADR 058's existing
`EncodingComputeRejectCode` taxonomy
(`KernelDecodedLengthMismatch` /
`EncodedValueOverflow` / `MaskLengthMismatch`). No
new reject code.

## Composability

**ADR 058 / 059 evaluator catalog.** ADR 066 extends
the catalog by one. Existing six evaluators
unchanged; existing default allow-list extends by
`Fsst` if Phase 7 evidence clears the council's
gates. Two-step dispatch discipline applies
recursively.

**ADR 058 row-group predicate planner.** ADR 066 is
the first multi-buffer native predicate leaf. The
planner extends in place to fetch validity + offsets
+ values together for FSST leaves; non-FSST cascades
remain on the existing single-buffer fetch shape.

**ADR 060 / 064 sparse projection.** ADR 066
produces mask-only `PhaseBOutcome`. FSST predicates
that engage the kernel route to ADR 060 / 064 Case A
mask-only sparse projection for the remaining
projection columns. FSST predicates that decline the
kernel (range / LIKE / unsupported atom / etc.)
continue to populate `predicate_projection_by_group`
and route to ADR 065 Case B/C as today.

**ADR 063 page stats.** FSST is not page-emitting in
ADR 057 V1; `emits_pages_in_v1()` is unchanged for
FSST. Page-stat emission on FSST buffers is a
follow-up ADR coupled with page-grain FSST.

**ADR 065 dense predicate-zip.** ADR 066 is the
first reduction in ADR 065's V1 engagement surface:
FSST `Eq` / `Neq` / `InList` Sub-case A1 predicates
(FSST predicate column not in output) migrate from
ADR 065 Case B/C to ADR 060 / 064 Case A. Sub-case
A2 predicates (FSST predicate column in output)
remain on the existing ADR 065 Case B path in V1
because variable-width sparse projection is V2-
deferred. Null predicates remain on the existing
ADR 059 validity-only path (no FSST kernel
engagement). This is the design intent — ADR 065 is
the bridge that survives evaluator-catalog gaps;
closing each gap migrates the eligible row groups
from Case B/C to Case A without redesigning sparse
projection. ADR 065 Case B/C remains reachable for
Sub-case A2, FSST range / LIKE atoms (V1 deferred),
and buffers that fail kernel eligibility.

**ADR 052 secondary-index point reads.** ADR 052's
confirm-step COCD path inherits FSST kernel coverage
through ADR 058's existing dispatch. Point reads on
FSST-keyed secondary indexes become byte-economical
for equality. No ADR 052 surface extension; if the
existing test harness can force the FSST cascade on
a secondary-index column without widening ADR 052,
path coverage lands in Phase 4.

**ADR 061 / 062.** No interaction. ADR 066 is
reader-side predicate evaluation; chunk layout and
sizing profile are upstream.

## Architectural lockdown

Incremental guards in `tests/architecture_test.rs`:

- `EncodingComputeEvaluatorKind` extends by exactly
  one variant (`Fsst`, label `"fsst"`); existing six
  unchanged.
- `evaluator_kind_for_cascade` maps `Fsst →
  EncodingComputeEvaluatorKind::Fsst`.
- `encoding_compute_predicate_data_type_supported` admits `Utf8` /
  `LargeUtf8` / `Binary` / `LargeBinary` only through
  the FSST evaluator path; architectural guard
  asserts non-FSST evaluator kinds do not inherit
  UTF-8 / Binary support.
- No new `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` / `PredicateAtomOp`
  / `EncodedRowMask` / `EncodingCascadeKind` /
  `RowGroupRejectCode` / `RowGroupFallbackReason` /
  `EncodingRejectCode` / `EncodingFallbackReason` /
  `L2FetchStrategy` / `NodeCapability` variant.
- No new IPC `custom_metadata` key.
- No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.
- No new wire `MsgType` / `FRAME_FLAG_*` / digest
  domain / `ObjectStore` trait method.
- No new descriptor field / fingerprint surface
  change.
- No new public PBC opcode.
- No new operator config field outside
  `enabled_predicate_evaluators`.
- No Cargo feature gating FSST evaluator.
- No nightly rustc dependency.
- ADR 057 `Fsst` cascade byte layout unchanged.
  `EncodingCascade::Fsst { symbol_table_byte_len }` is
  unmodified; an architectural guard asserts the
  kernel reads the symbol table from the `WFS1`
  header and does not consult the cascade field.
- **Streaming token decode is the only V1 survivor-
  row comparison path; literal pre-encoding (Strategy
  B) is absent from the kernel module.**
- **The kernel does not derive per-row compressed
  slices from Arrow offsets.** Offsets are decoded
  byte positions; the kernel streams the encoded
  WFS1 payload from byte 0 and uses offsets only as
  decoded row boundaries. An architectural guard
  pins this; the no-compressed-row-slice regression
  fixture (token crosses row boundary, matching row
  begins inside that token) is in the correctness
  suite.
- `kernels/fsst.rs` may import the writer's FSST
  decoder for the streaming token reader; it must
  not import the dense Arrow string materializer or
  use full-buffer FSST decode as its inner loop.
- The row-group planner's FSST predicate leaf
  fetches validity + offsets + values together
  (multi-buffer fetch shape); architectural guard
  asserts the fetch shape on FSST fixtures.
- Encoded-page cap counts one FSST leaf as one
  chunk (architectural guard asserts via fixture).
- The boundary-crossing-symbol fixture and no-
  compressed-row-slice regression fixture are in
  the correctness suite; their masks are byte-
  identical to dense oracle.
- ADR 020 §2 AAD layout, ADR 022 §3 / §5 byte
  layouts, ADR 056 / 057 / 063 directory shapes,
  ADR 058 / 059 evaluator catalogs *(by addition
  only)*, ADR 060 / 064 sparse projection
  eligibility, ADR 061 closed chunk-size set, ADR
  062 `RowGroupSizingProfile` catalog, ADR 065
  manager three-case dispatch: all unchanged.
- Cross-replica byte-equal mask output across two
  writers with the same FSST fixture and same
  predicate.
- ADR 060 / 064 mask-only sparse projection byte-
  identical pre / post on shared fixtures (mask
  shape unchanged).
- ADR 065 Case B/C still reachable for FSST
  predicates that decline the kernel (regression
  coverage).
- No version suffix or planning-phase identifier on
  new symbols. (`FsstFrame`, `DecodedOffsets`,
  `LiteralBytes`, `parse_fsst_frame`,
  `decode_offsets`, `evaluate_fsst_equality` are
  semantic.)
- No project-name prefix.

## Bench fixture mandate

Bench artifact records (matching ADR 060 / 063 / 064
/ 065 cadence):

- representative warehouse string fixtures:
  - URL-shaped (high cardinality, low-entropy
    prefixes; typically high FSST compression);
  - log-line-shaped (medium cardinality, structured
    tokens);
  - free-form text (high cardinality, high entropy);
  - identifier-shaped (UUID, ULID — high
    cardinality, fixed width);
  - JSON-key-shaped (very low cardinality across
    keys, high cardinality across values; tests
    cascade choice boundary with Dictionary);
  - **boundary-crossing-symbol fixture** (a frequent
    two-byte symbol spans adjacent Arrow values;
    correctness regression for streaming token
    decoder);
  - **no-compressed-row-slice regression fixture**
    (a frequent two-byte symbol crosses a row
    boundary AND the matching row begins inside that
    token; load-bearing fixture against the
    compressed-row-slice mistake — kernel must
    stream from start, not slice the encoded buffer
    by Arrow offsets);
  - **short-string adversarial** (≤ 16 bytes per
    row; pathological case for streaming token
    decode amortization; kernel engages and the
    bench records kernel-vs-dense CPU overhead);
  - **weak-compression adversarial** (high entropy;
    near-pass-through symbol table; kernel engages
    and the bench records kernel-vs-dense CPU
    overhead).
- predicate atoms (kernel-engaged):
  - `Eq` clustered selectivity 0% / 1% / 5% / 25% /
    50% / 95%;
  - `Neq` (inverse coverage; verifies length-mismatch
    rows correctly emit survivors under op-specific
    inversion);
  - `InList` with 1 / 4 / 16 constants;
- predicate atoms (validity-only path; kernel not
  invoked but regression coverage required):
  - `IsNull` / `IsNotNull`;
- decline fixtures (route to dense): `Lt`, `Gt`,
  `Lte`, `Gte` (each routes to dense; record
  overhead).
- column shape:
  - null-free fixtures;
  - nullable fixtures;
  - `Utf8` / `LargeUtf8` / `Binary` / `LargeBinary`
    coverage;
  - empty-string fixtures.
- length-pre-filter Disjoint fixture (literal length
  matches no row length; expect zero byte-comparison
  work via length pre-filter; streaming walk still
  happens).
- Prefilter mode fixture (FSST equality leaf + non-
  FSST residual leaf in conjunction).
- ADR 060 / 064 composability rows (FSST predicate
  engages kernel → mask-only `PhaseBOutcome` →
  sparse projection on remaining columns; expect
  Case A engagement).
- ADR 065 baseline rows (FSST predicate declines
  kernel → Case B/C zip; same fixture as ADR 066
  engagement to record byte savings via Case A
  migration).
- ADR 052 confirm-step row (FSST-keyed secondary-
  index point read; conditional on existing test
  harness ability per §"Composability").
- Corruption fixtures (malformed WFS1 frame, non-
  monotonic offsets, truncated escape; expect fail-
  closed via `EncodingComputeRejectCode`).
- Cross-replica determinism on representative
  fixtures.

**Promotion gates (matching ADR 060 / 064 / 065
cadence)**:

1. **Predicate-evaluation wall-clock ≥ 1.5×.** At
   clustered 5% selectivity for `Eq` and `InList`,
   predicate evaluation wall-clock is at least 1.5×
   faster than dense materialize-then-PBC on
   representative FSST-compressible UTF-8 fixtures.
2. **End-to-end query wall-clock ≥ 1.2×** when an
   exact FSST mask feeds fixed-width sparse
   projection through the ADR 065 → ADR 060 / 064
   Case A path.
3. **Dense-fallback overhead ≤ 5%** on truly
   declined paths (range, future LIKE, ineligible
   atoms / types / layouts) — measures the cost of
   threading FSST eligibility through dispatch when
   the kernel never engages.

Headline diagnostics (recorded, not gating):

- ≥ 2× CPU speedup on representative `Eq` fixtures
  (matches ADR 059's promote-to-default cadence).
- **Kernel-vs-dense CPU overhead recorded on
  short-string and weak-compression adversarial
  fixtures.** Kernel engages on these fixtures
  (V1 has no plan-time decline gate); the bench
  records kernel wall-clock vs dense decode + PBC
  per fixture to surface dense-faster regimes that
  would motivate a V2 CPU-cost gate. No V1 numeric
  threshold; observed overhead is reported per
  fixture and informs the V2 decision.
- Encoded-payload-vs-decoded-plaintext byte ratio
  per fixture (kernel walks the encoded WFS1
  payload; dense fallback materializes plaintext
  before evaluation — recorded as a workload
  characterization, not a fetch-byte promotion
  gate).
- Total query plaintext byte reduction across the
  dense baseline (FSST decode + Arrow scan + ADR
  065 Case B zip) vs. the kernel path (encoded walk
  + Case A sparse projection).
- Length-pre-filter elimination rate per fixture
  (fraction of rows skipped without scratch copy or
  byte comparison).

Correctness gates:

- Mask byte-identical to dense oracle on every
  correctness fixture across `Eq` / `Neq` /
  `InList` × `Utf8` / `LargeUtf8` / `Binary` /
  `LargeBinary` × null-free / nullable.
- **Boundary-crossing-symbol fixture mask byte-
  identical to dense oracle.**
- **No-compressed-row-slice regression fixture mask
  byte-identical to dense oracle.**
- Length pre-filter zero byte-comparison work on
  Disjoint-length fixtures (verified via bench
  instrumentation).
- Corrupt WFS1 frames and corrupt offsets fail
  closed through `EncodingComputeRejectCode`; no
  partial masks.
- ADR 060 / 064 mask-only composition byte-
  identical on engagement fixtures.
- ADR 065 Case B/C reachable for FSST predicates
  that decline the kernel (regression).
- Cross-replica determinism passes on
  representative fixtures.

## V1 deferred (with explicit V2 promotion paths)

ADR 066 does not ship:

- **Page-grain FSST.** Requires ADR 057 amendment
  to emit per-page boundaries on FSST buffers.
  **V2:** dedicated ADR; unblocks ADR 063 page-stat
  emission for FSST and ADR 060 sparse projection
  eligibility for FSST.
- **Strategy B (pre-encoded literal compare).**
  Requires a writer amendment that enforces row-
  safe FSST framing boundaries (currently the WFS1
  encoder compresses the entire values buffer as
  one stream, so symbols may cross Arrow value
  boundaries). **V2:** dedicated ADR (writer codec
  amendment + kernel optimization); promotes only
  if Phase 7 evidence on Strategy A shows per-row
  decode dominates kernel time and the writer
  amendment is operationally desirable.
- **Range / `Lt` / `Gt` / `Lte` / `Gte` on FSST.**
  Order-preserving FSST or speculative decode is a
  separate correctness story. **V2:** dedicated
  ADR.
- **`LIKE` / regex / substring on FSST.** Not in
  the current `PredicateAtomOp` catalog; deferred
  to a follow-up that adds those atoms first.
- **FSST aggregate folds.** `string_min` /
  `string_max` / `count_distinct` on encoded form.
  **V2:** dedicated ADR; may compose with ADR 040
  string-aggregate expansion.
- **Sparse projection eligibility for FSST.**
  Inherited variable-width deferral from ADR 060 /
  064.
- **Symbol-table caching across queries.** Per-
  buffer parse is V1; process-wide cache has its
  own determinism + eviction story.
- **Encoded-length-bound pre-filter (skip stream
  advance for structurally impossible rows).**
  Optional V2 optimization on top of V1's decoded-
  length pre-filter.
- **Bloom-filter prefilter on encoded equality.**
  Could short-circuit per-row work for very low
  selectivity; deferred pending bench evidence
  that the byte-walk dominates after the length
  pre-filter.
- **`Alp` / `Lz4Frame` / `Plain` predicate
  evaluators.** Each its own follow-up ADR.
- **Removal of
  `EncodingCascade::Fsst { symbol_table_byte_len }`
  cascade field.** Unused but its removal is a
  directory-layout change; ADR 066 is evaluator-
  only and does not bundle directory cleanup.
  **V2:** trivial follow-up directory-cleanup ADR.
- **Dictionary-of-FSST or FSST-of-Dictionary
  cascade composition.** ADR 057's leaf-cascade
  catalog and one-level composition rule apply
  unchanged.

## Rejected alternatives

- **Single-buffer FSST kernel.** Misses row
  boundaries (offsets buffer is the source of
  truth) or forces a dense decoded values buffer
  (defeats byte economy).
- **Strategy B (pre-encoded literal compare) in
  V1.** Structurally unsafe under the current
  WFS1 encoder per §"Five forces" #3; symbols can
  cross Arrow value boundaries. V2 work gated on a
  writer amendment.
- **Per-row compressed slicing using Arrow
  offsets.** Arrow offsets are decoded byte
  positions per §"Five forces" #2, not encoded
  byte ranges; slicing the encoded buffer by
  `[offsets[i], offsets[i+1])` would index into a
  different stream than where the row's encoded
  bytes live.
- **Constant-grain Disjoint short-circuit via
  literal unencodability.** FSST's escape byte
  encodes arbitrary bytes; literal-encoding-fails-
  therefore-Disjoint is not a structural truth.
- **Decode FSST inside the kernel and reuse the
  dense BitPacked / Dictionary kernel walk.** No
  improvement over ADR 057's existing dense-
  fallback path.
- **Add a new `EncodingCascadeKind` for "order-
  preserving FSST" to enable range predicates.**
  That is a different encoding; belongs in its own
  ADR.
- **Promote-default `Fsst` evaluator before Phase
  7 evidence.** Violates ADR 059's discipline.
- **Cache symbol tables process-wide.** Cross-
  replica determinism + eviction-policy story is
  its own ADR.
- **Bundle FSST aggregate folds.** Two correctness
  stories per ADR violates closed-catalog
  discipline.
- **Bundle page-grain FSST.** ADR 057 amendment
  expands scope; evaluator-only ADR ships
  independently.
- **Add a new `PredicateAtomOp` for "FSST
  equality".** Atom taxonomy is shape-grain, not
  encoding-grain. FSST-specific dispatch happens
  at the evaluator-kind layer.
- **Add a new `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` variant for
  FSST-specific declines.** Existing taxonomy
  covers it.
- **Promote-as-public-fallback-variant.** Decline
  reasons stay private to ADR 058's existing
  taxonomy.
- **Forking the FSST decoder.** Strategy A reuses
  the writer's decoder via the streaming token
  reader (single-source guard).
- **Range pushdown via speculative decode.**
  Reintroduces the cost ADR 066 narrows.
- **Bundle removal of
  `EncodingCascade::Fsst { symbol_table_byte_len }`
  with ADR 066.** ADR 066 is evaluator-only; even
  though the field is unused, removing it is a
  directory-layout change. The kernel reads the
  WFS1 header authoritatively and the cascade
  field's disposition is out of scope here.

## What not to do

- **Don't add a new `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` /
  `PredicateAtomOp` / `EncodedRowMask` variant.**
- **Don't change ADR 057 `Fsst` cascade byte
  layout** or `BufferEncoding` /
  `EncodingDirectory` shape.
- **Don't bump
  `ENCODING_DIRECTORY_FORMAT_VERSION`.**
- **Don't add an FSST-specific descriptor field,
  IPC `custom_metadata` key, wire `MsgType`,
  `NodeCapability` bit, digest domain, or
  `ObjectStore` trait method.**
- **Don't introduce planning-phase identifiers,
  version suffixes, or project-name prefixes** on
  new symbols.
- **Don't broaden the V1 atom set** without an ADR
  amendment.
- **Don't promote-default before Phase 7 evidence
  clears the council's gates.**
- **Don't bundle ADR 066 with page-grain FSST,
  FSST aggregate folds, sparse predicate-column
  fetch, ALP evaluator, or RLE value-run.**
- **Don't introduce a process-wide symbol-table
  cache.**
- **Don't assume FSST preserves byte-lexicographic
  order.**
- **Don't treat FSST as a single-buffer kernel.**
- **Don't pre-encode literals and compare against
  encoded byte ranges in V1** (Strategy B is
  unsafe under the current WFS1 encoder per
  §"Five forces" #3).
- **Don't slice the encoded values buffer by Arrow
  offsets.** Offsets are decoded byte positions
  (§"Five forces" #2); the kernel must stream the
  encoded payload from the start.
- **Don't rely on literal unencodability as a
  Disjoint proof.** FSST's escape byte makes it
  invalid.
- **Don't engage the kernel before validity +
  offsets + values are all fetchable for the FSST
  leaf.**
- **Don't use full-buffer FSST decode as the
  kernel's inner loop.** Streaming token decode
  through bounded per-row scratch is V1.

## Convergence summary

```text
ADR 057 per-buffer pages + cascades (Fsst cascade
  decode-only; multi-buffer values+offsets+validity
  layout for variable-width)
  → ADR 058 substrate COCD (Fsst not in evaluator
    catalog; single-buffer fixed-width planner shape)
  → ADR 059 native COCD kernels (Fsst not native)
  → ADR 060 sparse projection (Fsst not eligible;
    variable-width)
  → ADR 063 page-level predicate stats (Fsst not
    page-emitting)
  → ADR 064 nullable sparse projection (Fsst not
    eligible)
  → ADR 065 dense predicate-zip (Fsst predicates
    route Case B/C via dense fallback)
  → ADR 066 FSST predicate evaluator (this ADR;
    native `Eq` / `Neq` / `InList` on multi-
    buffer variable-width predicate leaf; Arrow
    offsets are decoded byte positions, not
    encoded byte ranges; streaming token decode
    walks the encoded payload from byte 0 and
    uses offsets only as decoded row boundaries;
    decoded-length pre-filter; mask-only output;
    no plan-time fetch-byte economics gate (kernel
    and dense fallback fetch the same encoded
    payload); first variable-width native predicate
    path; ADR 058 row-
    group planner extends in place; Sub-case A1
    predicates (FSST predicate column not in
    output) migrate from ADR 065 Case B/C to ADR
    060 / 064 Case A, Sub-case A2 (overlap)
    stays on Case B in V1, null predicates stay
    on the ADR 059 validity-only path; WFS1
    header authoritative for symbol table;
    cascade-field disposition out of scope; ADR
    057 cascade shape unchanged; no public
    protocol surface)
    ← here
  → next Toron-adjacent follow-ups (cascade-field
    removal directory cleanup, Strategy B for
    FSST gated on writer amendment, ALP
    evaluator, RLE value-run format, page-grain
    FSST, sparse predicate-column fetch,
    variable-width sparse projection, encoded-
    validity page decoding, all-null page-skip,
    range FSST, FSST aggregate folds)
  → capacity-tier compression / serverless / EC
    bundle
```

## Promotion criteria

ADR 066 promotes from council-converged → shipped
when:

1. Phase 0 audit pinned the FSST cascade encoding
   shape, the offsets-buffer encoding shape, the
   dense-fallback path, and confirmed the kernel
   can read the FSST symbol table from the `WFS1`
   header authoritatively (without consulting
   `EncodingCascade::Fsst { symbol_table_byte_len }`).
2. `EncodingComputeEvaluatorKind` extended by
   exactly one variant (`Fsst`, label `"fsst"`);
   existing six unchanged.
   `evaluator_kind_for_cascade` and
   `encoding_compute_predicate_data_type_supported` updated.
3. Pre-kernel eligibility gate admits FSST `Eq` /
   `Neq` / `InList` over `Utf8` / `LargeUtf8` /
   `Binary` / `LargeBinary` (predicate-output
   overlap on the FSST predicate column declines
   the kernel and routes through the existing ADR
   065 Case B path; see Decision §12); declines all
   other atoms / types / coercions to dense.
   `IsNull` / `IsNotNull` continue on the existing
   ADR 059 validity-only path with no FSST kernel
   engagement.
4. Length pre-filter verified to skip byte-
   comparison work on length-mismatch rows (bench
   instrumentation).
5. Streaming token decoder produces byte-identical
   output to the dense oracle on the boundary-
   crossing-symbol fixture and the no-compressed-
   row-slice regression fixture (correctness
   regressions for cross-row WFS1 framing and the
   offsets-are-decoded-positions invariant).
6. Mask output byte-identical to dense oracle on
   every correctness fixture across the closed
   atom × type × nullability × selectivity matrix.
7. Cross-replica byte-equal mask output on
   representative fixtures.
8. Multi-buffer planner shape verified by
   architectural guard (validity + offsets +
   values fetched together for FSST leaves).
9. WFS1 header is the kernel's authoritative
   source for the FSST symbol table; no consumer
   in the ADR 066 evaluator path reads
   `EncodingCascade::Fsst { symbol_table_byte_len }`
   (architectural guard).
10. Phase 7 bench evidence:
    - **Promotion gates:**
      - predicate-evaluation wall-clock ≥ 1.5×
        at clustered 5% for `Eq` / `InList` on
        representative FSST-compressible UTF-8
        fixtures;
      - end-to-end query wall-clock ≥ 1.2×
        through the ADR 065 → ADR 060 / 064
        Case A path;
      - dense-fallback overhead ≤ 5% on truly
        declined paths (range, future LIKE,
        ineligible atoms / types / layouts);
      - ADR 060 / 064 mask-only composition
        byte-identical;
      - ADR 065 Case B/C → Case A migration
        recorded.
    - **Recorded diagnostics (not gating):**
      - kernel-vs-dense CPU overhead recorded
        on short-string and weak-compression
        adversarial fixtures (kernel engages;
        no V1 numeric threshold; informs V2
        CPU-cost gate decision).
11. Default-enablement decision: if Phase 7 clears
    the council's gates, `Fsst` joins ADR 059's
    default predicate allow-list; if not, kernel
    ships default-off with a separate Phase
    evidence row recording the gap.
12. Architectural lockdown guards land per
    §"Architectural lockdown".
13. ADR 058 §"Where we are" amended: `Fsst` is in
    the evaluator catalog; row-group planner
    extends to multi-buffer for FSST leaves;
    default-on per Phase 7 outcome.
14. ADR 059 §"Where we are" amended (if default
    allow-list extends to include `Fsst`).
15. ADR 063 §"Where we are" amended: FSST native
    evaluation is row-group grain; FSST does not
    emit page stats in V1.
16. ADR 065 §"Where we are" amended: FSST `Eq` /
    `Neq` / `InList` Sub-case A1 predicates migrate
    to Case A; Sub-case A2 remains Case B; null
    predicates remain validity-only; ADR 065 V1
    engagement surface narrows accordingly.
17. ADR 020 / 022 / 056 / 057 / 058 / 059 / 060 /
    061 / 062 / 063 / 064 / 065 focused tests
    stay green at both `PointRead` and `Scan`
    profiles.
