# ADR 428 - Implementation plan: frugal q21 oracle

Pairs with `plans/adr/428_frugal_q21_oracle.md`. One toron-tpch-only instrument cut: replace the
q21 oracle's two co-resident giant maps (15M orders + 59.98M lineitems-by-order) with a
stream-merge of orders x lineitem-groups feeding a shared per-order semi/anti counter, so the
canonical in-process comparison passes under the standard 4 GiB cgroup and retires
`q21_sf10_oracle_residency_gap` (board 18/22 -> 19/22). No `crates/toron` engine surface.

## Phase 0 - reproduce the gap and record parity targets (before code)

1. **Reproduce the 4 GiB canonical OOM** on the current binary:
   ```bash
   scripts/cargo-cgroup build -p toron-tpch --release
   mkdir -p target/tpch/adr428
   unit="toron-adr428-q21-dense"
   systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=600s \
     /usr/bin/time -v target/release/toron-tpch correctness --query q21 --sf 10 --mode strict \
       --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr428/q21_dense.json 2> target/tpch/adr428/q21_dense.err
   { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
       --property=ExecMainStatus --property=MemoryPeak; } > target/tpch/adr428/q21_dense.meta
   systemctl --user reset-failed "$unit" 2>/dev/null || true
   ```
   Expect `oom-kill` at ~4 GiB during dense oracle construction, no terminal compare row.
2. **Record dense parity targets** (sorted `rows` + `contributing_rows`) at sf=0.01 and sf=1.
3. **Assert-order probe:** confirm `OrderGenerator` emits strictly increasing unique
   `o_orderkey` values and `LineItemGenerator` emits nondecreasing raw `l_orderkey` values with
   strictly increasing finalized group keys at sf=1/sf=10 (record; the assertions make these
   hard invariants).
4. Record everything before code.

## Phase 1 - the cut (toron-tpch only)

1. **Shared per-order counter:** extract the dense semi/anti block (`lib.rs:10073-10107`) into
   one helper `q21_count_order(order_status, items, saudi_supplier_names, ...) -> (rows_delta,
   per_name_deltas)` (shape illustrative). Both the frugal path and the `#[cfg(test)]` dense
   reference call it, so the existence arithmetic is identical by construction. All increments
   (`contributing_rows`, each `counts[s_name]`) are **checked**.
2. **Frugal `q21_oracle_from_rows`** (public seam, unchanged signature): consume `suppliers` and
   `nations` into the small resident maps; then **stream-merge** the `orders` and `lineitems`
   iterators:
   - group `lineitems` by contiguous `l_orderkey` with one-row lookahead; **assert nondecreasing
     raw keys** and strictly increasing finalized group keys (loud on decrease/reappearance);
   - advance the `orders` cursor (asserted strictly increasing `o_orderkey`) to each group's key
     for `o_orderstatus`; a group with no matching order, or a non-`F` order, contributes nothing;
   - call the shared counter per matched order; accumulate `counts`;
   - **drain both unmatched tails to EOF** so a late tail violation is still asserted;
   - build/sort rows by `numwait DESC, s_name`; return the `Q21Oracle`.
3. **Dense reference -> `#[cfg(test)]`:** move the former dense body to
   `q21_oracle_dense_from_rows` / `strict_q21_oracle_dense` in an internal test module (calling
   the same shared counter). Public `strict_q21_oracle` stays and calls the frugal seam.
4. **Unchanged:** `compare_strict_q21`, the non-vacuity guard, LIMIT 100, schema, and every other
   query's path.

## Tests (discriminating)

- **Dense-vs-frugal parity:** at sf=0.01 and sf=1, the frugal `strict_q21_oracle` structurally
  equals `strict_q21_oracle_dense` (full sorted `rows` + `contributing_rows`; `Q21Oracle` derives
  `PartialEq/Eq`).
- **Counter-fidelity edge locks** (synthetic inputs through both paths): order status != F; a
  sole-supplier order (EXISTS fails); a second late supplier (NOT-EXISTS fails); a sole-late
  Saudi supplier with multiple late lineitems (each counts); `receiptdate == commitdate` (not
  late); a non-Saudi late supplier (skipped); a lineitem group with no order and an order with no
  lineitems both contribute nothing; unknown supplier/nation references skip.
- **Order-assertion locks:** a decreasing / reappearing lineitem group key fails loudly; a
  non-strictly-increasing `o_orderkey` fails loudly; contiguous same-key lineitems succeed; a
  **tail-only** violation (in the longer stream's remainder) is still caught (both tails drained).
- **Checked-overflow locks:** seed `contributing_rows` at `usize::MAX` and a supplier count at
  `i64::MAX`; the shared counter fails loudly at each independent increment rather than wrapping.
- **Non-vacuity lock:** the unchanged guard still fires on a vacuous synthetic oracle.
- Carry the `toron-tpch` correctness/harness suites green.

## Phase 2 - re-measure

```bash
scripts/cargo-cgroup build -p toron-tpch --release
mkdir -p target/tpch/adr428
# board target: canonical in-process strict q21 sf=10 under the STANDARD 4 GiB cgroup
unit="toron-adr428-q21-frugal"
systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch correctness --query q21 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr428/q21_frugal.json 2> target/tpch/adr428/q21_frugal.err
{ echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
    --property=ExecMainStatus --property=MemoryPeak; } > target/tpch/adr428/q21_frugal.meta
systemctl --user reset-failed "$unit" 2>/dev/null || true
rm -rf /tmp/toron_join_spill
# small-scale frugal correctness (parity path also covered by unit tests)
for sf in 0.01 1; do
  target/release/toron-tpch correctness --query q21 --sf "$sf" --mode strict \
    --data-source memory >> target/tpch/adr428/q21_small.out 2>&1
done
```
- q21 sf=10: `phase=compare status=pass` under 4 GiB; record cgroup `MemoryPeak` when the
  completed transient unit retains it and `/usr/bin/time` maximum RSS when this systemd version
  reports `MemoryPeak` unset. Gap retired -> board 19/22.
- Controls: unchanged by a toron-tpch-only q21-oracle cut; a light re-run of a couple of
  spill-bearing greens is optional confirmation only (no engine surface touched).

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
scripts/cargo-cgroup test -p toron --test architecture_test        # source-scan guards
scripts/cargo-cgroup test -p toron --no-fail-fast                   # engine unaffected; cheap safety
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)

`crates/toron/plans/impl/428_frugal_q21_oracle_results.md`:
- Phase 0: the reproduced 4 GiB dense OOM, the sf=0.01/sf=1 parity targets, the assert-order
  probe - before code;
- the cut: shared per-order counter, frugal stream-merge (nondecreasing assertion, both tails
  drained, checked increments), the `#[cfg(test)]` dense reference, unchanged
  comparator/guard/LIMIT;
- q21 sf=10 canonical `pass@compare` under 4 GiB with recorded cgroup `MemoryPeak` or the
  `/usr/bin/time` maximum-RSS fallback; the retired gap; board 19/22;
- locks: why each is discriminating (the shared-counter structural fidelity, tail-drain, and
  order-assertion especially);
- confirmation: no engine surface, thirteen controls and q17 untouched, full `toron-tpch` gate,
  no `.perf.data`; scope honesty (q17/q02/q05 remain named).

## Risk / blast radius

- **Frugal-delivery bug** -> dense parity locks at sf=0.01/1; the shared counter removes the
  arithmetic-divergence class.
- **Out-of-order emission** -> loud assertion on either stream; both tails drained.
- **Missed non-test caller of the dense body** -> `--all-targets` compile catches it when the
  dense fns move under `#[cfg(test)]`.
- **Supplier/nation residency** -> a few MB at sf=10; Saudi-key pre-filter is the unused
  fallback.

## Seal

`plan: adr 428 frugal q21 oracle` (the dual). Implementation:
`impl: adr 428 frugal q21 oracle`. Do not commit `.perf.data`.
