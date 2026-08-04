# Implementation Plan: M11 — Advanced Kernels

Author: AI Swarm Council (Claude / Codex / Gemini)
Primary draft: Gemini

## 1. Goal

Expand Toron's compute capabilities with advanced analytical kernels: Linear Regression, Approximate Quantiles (KLL), and Time Bucketing.

## 2. Implementation Contract

- **Aggregation Kernels:**
    - `LinearRegress`: Accumulates $n, \sum x, \sum y, \sum x^2, \sum xy$ to compute slope ($a$) and intercept ($b$).
    - `ApproxQuantile(q)`: Uses Wendelon's `KllSketch` to estimate the $q$-th quantile.
- **Expression Kernels:**
    - `TimeBucket(interval)`: Truncates timestamps to the nearest interval boundary.
- **Surface Integration:**
    - SQL: `SELECT linear_regress(y, x), approx_quantile(val, 0.95), time_bucket(ts, '1h') ...`
    - Rune: `df.aggregate([], [linear_regress(col("y"), col("x")), approx_quantile(col("val"), 0.95)])`.

## 3. Phase Plan

### 3.1. IR Expansion (Completed)
- [x] Add `LinearRegress` and `ApproxQuantile(f64)` to `AggregateFunction`.
- [x] Add `TimeBucket(Duration)` to `Expr`.
- [x] Refactor `LogicalPlan::Aggregate` to support multi-argument aggregates.

### 3.2. Aggregation Kernels (Completed)
- [x] Implement `LinearRegressAccumulator` in `src/exec.rs`.
- [x] Implement `QuantileAccumulator` in `src/exec.rs` (reusing `wendelon::storage::sketch::KllSketch`).

### 3.3. Expression Kernels (Completed)
- [x] Implement `time_bucket` kernel in `src/exec.rs`.

### 3.4. Surface Integration (Completed)
- [x] Update `SqlPlanner` to recognize new functions.
- [x] Update `RuneEngine` to expose new functions to scripts.

### 3.5. Verification & Testing (Completed)
- [x] **Linear Regression Test:** Verify slope and intercept against known small datasets.
- [x] **Quantile Test:** Verify that `approx_quantile` returns values within expected error bounds for a large random distribution.
- [x] **Time Bucket Test:** Verify truncation logic for various intervals (1m, 1h, 1d).


## 4. Design Details: Linear Regression State

```rust
struct LinRegState {
    n: u64,
    sum_x: f64,
    sum_y: f64,
    sum_x2: f64,
    sum_xy: f64,
}
```

## 5. Risks and Mitigations

- **Risk:** Floating point precision issues in linear regression.
  - **Mitigation:** Use `f64` for accumulation; document as a standard OLS implementation.
- **Risk:** `KllSketch` memory usage.
  - **Mitigation:** Rely on Wendelon's proven `KllSketch` bounds ($k=200$ by default).

## 6. Definition of Done

M11 is complete when all three advanced kernels are implemented, integrated into the SQL/Rune surfaces, and verified with unit tests.
