# ADR 440 impl - factor common OR conjuncts into the probe child

Binder change (correctness-bearing) + reuse of the existing cross-child push seam and `Filter<Scan>`
probe decomposition. **No executor change (locked).**

## The owner type

Replace the `extract_common_or_bridging_keys` tuple (`Option<(Vec<(Expr,Expr)>, Option<Expr>)>`,
`binder/outer_join_demotion.rs`) with:
```rust
struct CommonOrExtraction {
    keys: Vec<(Expr, Expr)>,        // ADR 319 bridging equalities (unchanged)
    left_common: Vec<Expr>,         // common conjuncts routed to the left child
    right_common: Vec<Expr>,        // common conjuncts routed to the right child
    residual_or: Option<Expr>,      // reduced (A'∨B'∨C'); None if any arm emptied
}
```
Keys, side-routed predicates, and residual stay aligned in one value threaded through `lift_cross_equi`
(`:860`).

## The extraction algorithm

1. **Fence:** run common-conjunct extraction **only when `keys` is non-empty** (a bridging key exists
   across every arm — ADR 319's admitted shape). Otherwise return the OR byte-identical.
2. Split each arm with `split_logical_and_conjuncts`. Build the **global structural intersection** across
   all arms via the new matcher (below).
3. For each common conjunct, keep it only if `expr.is_deterministic()` **and**
   `guard_filter_pushdown_safe(expr)` **and** it is side-local to exactly one cross child.
4. **Extract one structural occurrence** into `left_common`/`right_common`; **remove one matching
   occurrence from every arm**.
5. If any reduced arm is empty → `residual_or = None`; else `residual_or = Some(A'∨B'∨C')`.
6. Route `left_common`/`right_common` via `push_into_cross_child` (`:1223`) → `Filter<Scan>` (or
   `Exchange<Filter<Scan>>`) probe child. `residual_or` becomes the `join_filter_predicate`.

## The structural matcher (no general Expr equality)

Add a typed, exhaustive recursive equality over `Expr`:
- **float bit** equality (canonical bits, not `==`), **timestamp units**, **Decimal precision/scale**,
  **cast target types**, and **every variant field**;
- exhaustive `match` — a new `Expr` variant is a **compile error**, not a silent non-match;
- **not** `Debug`, `serde`, or naïve equality;
- hashing may **accelerate** candidate grouping but the **recursive equality is the gate**.

## Correctness tests (`-p toron`, binder + lib)

- **3VL equivalence:** NULL/TRUE/FALSE combinations of `x` and arm predicates; factored vs. original row
  sets identical.
- **Only-if-in-every-arm:** conjunct in 2/3 arms is not factored.
- **Duplicate / reordered:** one occurrence removed per arm; reordered arms still match.
- **Empty-arm → `residual_or=None`:** an arm that is entirely common conjuncts collapses the residual to
  None; output unchanged.
- **Side-local + pushdown-safe only:** cross-child or non-`guard_filter_pushdown_safe` or
  non-deterministic conjuncts stay in residual.
- **No-common-key OR:** an OR without a bridging key is byte-identical (plan + output).
- **Structural-key discrimination:** two conjuncts differing only in float bits / timestamp unit / Decimal
  scale / cast type are **not** equal.

## Physical-shape gate (`-p toron` + tpch)

Assert the bound q19 probe child is `Filter<Scan>` **or** `Exchange<Filter<Scan>>` (accept
`FilterDisposition::FullyPushed`), the ship predicates are **at/below the scan boundary / below the probe
exchange**, and `residual_or` holds the per-arm parts. Exact **M = 2,141,904** at the join proves early
application.

## Speed rail (seal gate) — ADR 356 dynamic band

- Baseline **439's 71,987.808 ms, spread 0.011899**; band = `max(0.10, 2 × max(0.011899, current_spread))`
  — floor **0.10**.
- Unprofiled ADR 356-shaped strict sf=10 q19: 1 warmup + 7 measured, exact 356 envelope. **≤0.20
  stability; one retained retry; never average.**
- Seal iff median execute wall clears the band vs. 71,987.808 ms. Sub-band → honest-red → revise/remove.

## M → S, invariance, survival, corroboration

- **M = 2,141,904 → S = 1,134** (SUM-arg = numeric-fold = 1,134); oracle `171+382+581 = 1,134`.
- **Plan fingerprint** changes from `65c3c2c0b0a99063`, then identical across all 7; Arrow digest / schema
  / one row / revenue value / **51,471,552** spill bytes invariant across all 7.
- **Survival:** strict sf=10 q19 persisted-Parquet `pass@compare` under `MemoryHigh=4G`/`MemoryMax=4G`/
  `MemorySwapMax=0`, `TasksMax=512`, `RuntimeMaxSec=600`, scratch fresh/empty before and after.
- **438-ledger reread (corroborative only):** five fresh children; D1 and `probe_lookup_select` fall in
  absolute micros and share vs. the 439 receipt.

## Seal gate (council.md, via scripts/cargo-cgroup, after fmt)

```
scripts/cargo-cgroup fmt  -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
```

- Binder rewrite touching a widely-exercised pass ⇒ the full `--no-fail-fast` suite is the should-reject /
  behavioral net. `exec.rs` 7,499; join root 3,713. Architecture guard: extraction is structural +
  `is_deterministic`/`guard_filter_pushdown_safe`-gated, **no query literal**.

## Implementation contingency (single, hard-stop)

- If the net wall is sub-band despite M→2,141,904, preserve the honest-red receipt and decide
  revise/remove — do not seal below the band (437 precedent).
- **No executor change is locked.** If the bound plan does not reach `decompose_streaming_scan_probe_input`
  (`streaming.rs:1941`) in a `Filter<Scan>` / `Exchange<Filter<Scan>>` / `FullyPushed` shape, **stop and
  revise the ADR** — never add executor work to force the seam.
