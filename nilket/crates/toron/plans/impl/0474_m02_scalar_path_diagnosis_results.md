# ADR 474 implementation receipt

The m02 20× has a named owner: `numeric_value` (`aggregate.rs:2602`) ran a chain of
up to eleven `as_any().downcast_ref::<T>()` type tests **per row**, called from
plain `for row in 0..batch.num_rows()` loops at seven sites. Subtractions and
profile agree; ADR 475 acts on it.

## Sub-primitives (sf=8, ADR 473 conditions, references single-threaded)

| primitive | Toron | DuckDB 1t | Polars 1t | Toron/best |
|---|---:|---:|---:|---:|
| m07 decimal decode + null-check | 380.9 ms | 12.0 | 206.1 | 31.7× |
| m08 Int64 decode + null-check | 422.4 ms | 12.0 | 134.2 | 35.2× |
| m09 Int64 decode + integer sum | 942.9 ms | 97.0 | 149.8 | 9.7× |
| m02 decimal decode + sum | 1,588.6 ms | 80.0 | 243.7 | 19.9× |
| m10 two-col decode + multiply + sum | 1,816.9 ms | 195.0 | 1,283.3 | 9.3× |

Layer subtractions (differences of medians, stated as arithmetic):

- decimal decode = 43.8 ms (0.9 ns/row); Int64 decode = 85.3 ms (1.8 ns/row)
- decimal accumulate = 1,207.7 ms (**25.2 ns/row**); Int64 accumulate = 520.5 ms
  (**10.8 ns/row**)
- decimal penalty = 687.2 ms (14.3 ns/row)
- m10's added column + multiply = +228 ms over m02 (~3.8 ns/row for the multiply)

**Decode is not the problem.** The cost is the accumulate step, and the multiply
kernel on the same rows in the same pipeline costs 6× less than the sum
accumulator — which rules out blanket per-row dispatch and points at the aggregate
path specifically.

## Profile (symbolized, `[profile.profiling]`)

`toron::exec::aggregate::numeric_value` 14.5% self, `fold_global_aggregate_state`
5.9%, with `<T as Any>::type_id` 2.5% and `PrimitiveArray::as_any` 2.1% as their own
line items — dispatch machinery only surfaces that way when it runs per value.
Profile and subtractions agree; no unresolved disagreement to report.

The decimal penalty is explained by **chain position**, not decimal arithmetic:
`Decimal128Array` is the last of eleven tests, so decimal values pay the most failed
type tests before matching.

## Measurement notes (durable)

- `toron_tpch::stable_file_hash` is 58% of the profiled **process** — harness
  manifest verification at child startup, excluded from `execute_ms`. It is startup
  cost and is explicitly not part of the accumulator diagnosis; any future
  whole-process profile must account for it.
- `[profile.profiling]` (release codegen + symbols) was added because `release`
  carries `strip = true, debug = 0`. Note also that `scripts/cargo-cgroup` runs
  builds under `systemd-run`, which does **not** inherit caller environment — env
  overrides like `CARGO_PROFILE_RELEASE_DEBUG` silently do nothing there.

## Outcome

Owner named, fix scoped to ADR 475 (hoist the downcast; measured 3.69× on m02).
m06's 18.6× remains the second target.
