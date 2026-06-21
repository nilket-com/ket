# ADR 305 — SUBSTRING expression support

## Status
Proposed.

## Context

ADR 304's q22 measured `blocked(unsupported_substring@plan)` — frontend lowering rejects `SUBSTRING` at the
`lower_expr` catch-all. ADR 305 builds `SUBSTRING(x FROM start FOR len)` — the engine's
first string-slice scalar, broadly useful beyond q22. A 3-layer engine cut (lowering + IR/bind + exec), mirroring
`Like`/`Extract`.

## Decision

### D1 — frontend lowering (`sql_planner.rs`)
- Add a `SqlExpr::Substring { expr, substring_from, substring_for, special, shorthand }` arm in `lower_expr` (next to
  `Extract`, before the catch-all) → a new logical `Expr::Substring { input, from, length }`.
- **Literal `from`/`for` only** — this cut calls Arrow's scalar-start/scalar-length substring kernel, so per-row
  (expression) `from`/`for` is **rejected** (deferred). Handle both sqlparser encodings: `special == false`
  (`FROM … FOR …`) and `special == true` (comma form, `SUBSTRING(x, s, n)`). Reject the `SUBSTR` shorthand for now
  (`shorthand == true`); defer aliases and the `SIMILAR`/regex form.
- **Update the expr-walkers** for the new `SqlExpr::Substring` + `Expr::Substring` — `expr_contains_aggregate`,
  `expr_contains_subquery`, and every `Cast`/`Extract`-style visitor must recurse into `expr` plus the SQL
  `from`/`for` children before literal validation. A missed walker is a silent bug.

### D2 — IR + binder
- Logical `Expr::Substring` (`ir.rs`) → `BoundExprNode::Substring { input, from, length }` (`ir/execution.rs`), a
  new bound-expression variant mirroring `Like`/`Extract`. Bind: `input` must be `Utf8`; `from`/`length` validated
  integer literals; output `Utf8`.
- A new `BoundExprNode` variant trips the IR-variant architecture guards (variant lists/counts) — reconcile them;
  `cargo check -p toron --all-targets`.

### D3 — exec (`exec.rs`) — conservative start semantics (defined before coding)
`evaluate_substring` via Arrow's character-counted Utf8 substring kernel
(`arrow::compute::kernels::substring::substring_by_char` through the Arrow re-export). Do **not** use the
byte-counted `substring` kernel for Utf8 SQL semantics. **SQL is 1-based, Arrow is 0-based:**
- `FROM 1` → Arrow offset `0`.
- `FROM n` (n > 1) → Arrow offset `n − 1`.
- **`FROM ≤ 0` (zero/negative start) → REJECT cleanly at bind** — *do not clamp*. q22 only needs `FROM 1`, and
  rejecting an invalid/edge start is safer than baking a possibly-wrong SQL-dialect clamp. The full SQL start-<1
  clamp semantics are **deferred** to a later ADR (when a query/compatibility target nominates them).
- `FOR len` → Arrow `length = Some(len)`; `FOR 0` is allowed and returns the empty string; negative `FOR` length
  rejects cleanly at bind; no `FOR` → `None` (rest of string).
- Pin the mapping: `SUBSTRING('abcde' FROM 2 FOR 3)` → `'bcd'`.

## Scope / boundaries
- `SUBSTRING(string FROM <int literal> [FOR <int literal>])` (+ comma form), `from ≥ 1`, `for ≥ 0`.
  **Defer:** non-literal (per-row) `from`/`for`; `from ≤ 0` (reject for now); negative `for`; `SUBSTR`;
  the `SIMILAR`/regex form; non-`Utf8` input. No public surface beyond `SUBSTRING`.

## Acceptance
- A focused engine test: `SUBSTRING('abcde' FROM 2 FOR 3)` → `'bcd'`; `SUBSTRING(col FROM 1 FOR 2)` (q22's shape)
  binds + executes the right slice; the no-`FOR` (rest) case; `FOR 0` → empty; **`from ≤ 0` and negative `for`
  reject cleanly**; a non-literal `from`/`for` rejects precisely. Pin character semantics with a non-ASCII case
  such as `SUBSTRING('Γabc' FROM 2 FOR 2)` → `'ab'`. Discriminating: rejects pre-cut at the catch-all.
- **q22 moves OFF `unsupported_substring@plan`** — verify-first re-measure. Success is q22 reaching its **next**
  wall (the correlated `NOT EXISTS`, the `AVG`-threshold decorrelation, or another), measured + classified
  honestly — **not necessarily `pass@execute`**. q22 stays red until those land.
- Full `-p toron --no-fail-fast` + `--all-targets` + `architecture_test` green (new `Expr`/`BoundExprNode` variant
  + walkers + variant guards); 14 green reach cells + 14 correctness cells unchanged.

## Risks
- **A new `Expr` + `BoundExprNode` variant is wide** — the expr-walkers and the IR-variant architecture guards all
  need the new arm. `--all-targets` + the full suite + `architecture_test` are the net (a missed walker = a silent
  bug; a missed guard = a red seal). The JoinKind-variant-checklist discipline.
- **The 1-based → 0-based mapping** — the single easiest place to get SUBSTRING wrong; exact-slice tests pin it,
  and `FROM ≤ 0` rejects rather than guesses.
- **Scope creep** — literal `from`/`for`, `from ≥ 1`, `for ≥ 0` only; defer per-row, the `<1` clamp, `SUBSTR`,
  and SIMILAR.

## Forward arc
q22 re-measures its next wall (correlated `NOT EXISTS` / `AVG`-threshold → ADR 306+). SUBSTRING is then available
across the surface (q22 + literal-bounds string slicing). A later ADR can widen to full SQL start-<1 clamp,
`SUBSTR`, and per-row `from`/`for` if nominated. q13 (non-equi outer-join `ON`) and q19 (disjunctive) remain
reach machetes.

## Why toron-home
A `crates/toron` cut across lowering + IR + binder + exec (a new scalar expression). q22's row advances toward its
next wall in `toron-tpch` with no harness logic change. No Wendelon / cross-ring surface.

## Council provenance
3/3 (Claude/Gemini on the 3-layer shape; the operator's framing). Scope kept exactly as framed (literal `FROM`/
`FOR`, q22 re-measures). Operator's two refinements folded: **conservative start semantics** (`FROM 1`→0,
`n>1`→`n−1`, **`≤0` rejects cleanly — no clamp**; full SQL clamp deferred); **acceptance = q22 moves off
`unsupported_substring`, not necessarily green** (the next honestly-measured wall is success). Mirrors `Like`/
`Extract`; Arrow character-counted `substring_by_char` kernel; the walkers + IR-variant guards are the
load-bearing breadth.
