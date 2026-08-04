# ADR 262 — Strict Q6 First Red Row

## Status

Proposed.

## Context

ADR 261 built the TPC-H reachability gauge. Its matrix currently reports *inventory* state — dev Q6
`pass`, strict Q6 `not_attempted(strict_inputs_required)`, the rest `not_attempted`. The gauge has not
yet *read a real strict blocker*. The highest-leverage next step is **not** implementing Decimal — it
is making the gauge produce the first real query × stage row from strict mode, so the next engine ADR
is selected from **measured** blockage rather than prediction.

For this cut, **"strict" means official physical *types* + Q6 *shape*, not official provenance or
answer-set correctness.** A Toron-authored `Decimal128(15,2)` Q6 proxy with **literal** date bounds is
enough to force the Decimal blocker honestly, without claiming TPC compliance, vendoring QGEN text, or
dragging in the `Date32 ± Interval` gap.

## Grounding — the probe was run; the first red row is measured

Both candidate strict paths were probed empirically (`crates/toron-tpch/tests/decimal_probe.rs`), not
predicted:

| probe | path | result |
|---|---|---|
| DDL | `plan("CREATE TABLE lineitem (… DECIMAL(15,2) …)")` | `Err Unsupported "unsupported CREATE TABLE column type: DECIMAL"` → **phase=plan** |
| **Decimal128 compute** | `Decimal128(15,2)` descriptor + strict Q6 → plan → bind → execute | descriptor **Ok**, register **Ok**, plan **Ok**, **bind `Err`** → **phase=bind** |

The Decimal128 path gets far past "probably Decimal at load/plan":

```
bind -> Err(TypeMismatch { message: "comparison operands have different types: Decimal128(15, 2) and Float64" })
```

**Root cause (grounded):** Toron's `ScalarValue` is a closed set of 6 with no Decimal, so a decimal
**literal** (`0.05`) lowers to `Float64`. The strict Q6's `l_discount BETWEEN 0.05 AND 0.07` then
compares a `Decimal128(15,2)` *column* against a `Float64` *literal* — a type-unification failure at
**bind**, *before* the `SUM(l_extendedprice * l_discount)` decimal arithmetic is reached. The
descriptor, catalog, and planner already tolerate `Decimal128`; the **bind-time type system** is the
first wall.

**This names ADR 263 precisely:** not "accept DECIMAL DDL," but a **Decimal `ScalarValue`** so that
(a) decimal literals carry a decimal type and (b) Decimal-vs-Decimal comparison/arithmetic unify.

## Decision

1. **Strict Q6 stops returning `not_attempted`** and attempts the Toron-authored `Decimal128(15,2)`
   strict proxy (literal date bounds) single-node through `plan → bind → execute`, recording the first
   phase it blocks. Grounded result: `phase=bind status=blocked reason=unsupported_decimal_scalar`.
2. **The DDL gap is a separate retained probe/evidence row** —
   `phase=plan reason=unsupported_decimal_ddl` — so the SQL-surface Decimal gap and the compute Decimal
   gap are two honest rows at two phases without making the default strict Q6 run emit multiple
   attempts.
3. **Matrix artifact**: a `matrix` command **regenerates** a readable query × {dev,strict} ×
   {phase,status,reason} table from `target/tpch/reachability.jsonl` — no hand-maintained state.
4. **Reason precision**: add `unsupported_decimal_scalar` to the allowlist and classify the bind
   `TypeMismatch` (`Decimal128` column vs `Float64` literal) to that reason. Keep
   `unsupported_decimal_arrow` for future Decimal carrier/load gaps; this cut's measured compute row is
   the scalar/literal gap. The honesty invariant is unchanged: an *unrecognized* engine error is
   `fail`, not `blocked`.

No Decimal implementation. No answer comparison. No official QGEN/DBGEN data or query files. No `Date32
± Interval` work (literal date bounds avoid it deliberately, so the row is *Decimal*, not Interval).

## Scope

- **In**: a `Decimal128(15,2)` strict Q6 fixture in `toron-tpch` (descriptor + harness-built batch +
  Toron-authored strict proxy); strict-mode attempt recording the grounded first block phase/reason;
  both probes retained/hardened as verify-first regression tests; a `matrix` artifact regenerated from the
  JSONL; the `unsupported_decimal_*` reason(s) finalized in the allowlist.
- **Out / deferred**: Decimal `ScalarValue`/arithmetic (ADR 263 — what this *measures*); answer-set
  comparison; the embedded official-Q6 text + SF=1 answers path (`tpchgen::q_and_a`, a later
  reachability+correctness cut); `Date32 ± Interval`; all 22 queries; an orchestrated `run --all` (a
  *static* matrix summarizer is in; an orchestrated all-query runner is not).

## Discriminators

- `run --query q06 --mode strict --allow-blocked` (no user inputs) emits **`status=blocked phase=bind
  reason=unsupported_decimal_scalar`** — not `not_attempted`, and not a shallow plan-phase guess.
- A separate DDL probe/evidence row records `phase=plan reason=unsupported_decimal_ddl`.
- Both reasons are allowlisted (so `--allow-blocked` exits 0); an *unrecognized* bind/exec error on the
  strict path still classifies `fail` — pinned by a test.
- The `matrix` artifact (regenerated from `reachability.jsonl`) shows `q06 dev=pass@execute`,
  `q06 strict=blocked(unsupported_decimal_scalar@bind)`.
- `status` reflects q06 strict as `implemented` inventory, not `input_gated`; it does **not**
  encode the last measured failure. Only the JSONL-backed `matrix` reports red rows.

## Architecture guards

- `adr262_strict_q6_attempts_decimal128_compute` — strict mode builds a `Decimal128(15,2)` fixture and
  runs `plan→bind→execute` (the probe lives as a retained test).
- `adr262_first_red_row_is_grounded_not_predeclared` — the strict reason is derived from the actual
  engine error, not a hardcoded string.
- `adr262_matrix_regenerated_from_jsonl` — the matrix command reads `target/tpch/reachability.jsonl`;
  no hand-maintained matrix state.
- `adr262_status_is_inventory_not_last_result` — `status` marks q06 strict as `implemented` but does not
  hardcode `blocked`; measured failure state comes from JSONL.
- (carry) the ADR 261 honesty + frontend/library-boundary + no-vendoring guards remain.

## Reconcile

ADR 261's `strict_without_inputs_is_not_attempted` test asserts `not_attempted`; strict now *attempts*
and blocks. Repoint it to `strict_without_inputs_blocks_on_decimal` (asserts `Blocked phase=Bind
reason=unsupported_decimal_scalar`). The `strict_inputs_required` reason now belongs only to a future
user-supplied official path.

## Forward arc

262 strict Q6 first red row (bind: Decimal-literal type unification) → **263 Decimal `ScalarValue`**
(literals + comparison + arithmetic — responding to the *measured* bind-time gap) → strict Q6 reaches
`execute` → `Date32 ± Interval` for the official Q6 text → official-text + SF=1 answer comparison via
`tpchgen::q_and_a` → Q1/Q3 reachability → distributed. Each red row names the cut that turns it green;
the gauge now moves itself.
