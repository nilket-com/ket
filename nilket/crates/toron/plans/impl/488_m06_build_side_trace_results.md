# ADR 488 - m06 build-side trace (read-only); corrects the ADR 486/487 premise

The trace asked: does m06's global-aggregate join call the smaller-build comparator,
is it disabled, and if enabled what favors lineitem. The explain probe answers all
four **and falsifies the shared premise that m06 builds the large side**. No code
shipped; the deliverable is this record and the redirect it forces.

## The four questions, answered from the bound decision

`bind_with_explain_summary` on m06 (`COUNT(l_orderkey) FROM lineitem JOIN orders`),
both comparator arms:

- `hash_smaller_build_placement = false`: `#0 hash_partitioned **natural**
  hash_by_default` → builds the left = **lineitem** (48M).
- `hash_smaller_build_placement = true`: `#0 hash_partitioned **swapped_right_build**
  hash_smaller_build` → swaps to build the right = **orders** (12M).

1. **Does m06 call the comparator?** Yes — the plan and placement change with the knob.
2. **Is it disabled there?** No. It is **default-on** (ADR 472) and active for m06.
3. (n/a — it is called.)
4. **What favors lineitem?** Nothing, under the default. With the comparator on, m06
   **already builds orders, the smaller side.** The premise that m06 builds lineitem —
   carried from ADR 485 through 487 — is **false**.

## The correction, and what it retires

ADR 487 redirected the m06 lever to build-side/threshold levers, chief among them
**(a) build the smaller side**. This trace shows **(a) is already in effect**: the
smaller-build comparator (default-on) builds orders. So (a) is moot for m06.

And m06 **still spills** building the 12M orders side (~86.7 MB at a 2 GiB budget,
where orders' padded estimate ~1.44 GB is well under budget). Since the plain-path
engage threshold (`estimated_build_hash_bytes ≤ task_budget`) would decline to
in-memory at 2 GiB, m06's spill is **not** coming from that threshold. m06 is
`hash_partitioned` **with exchanges** (partition_count > 1), so it executes on the
**partitioned exchange path, which materializes/spills per partition regardless of
build side or budget** — the behavior ADR 486 probe 1 already showed (partition_count=1
still spilled) and probe 2 (skip-exchange → `JoinKeyExchangeMismatch`).

## Where this leaves the m06 lever

Build-side is already optimal; the plain-path threshold levers (487 (b)/(c)) do not
apply because m06 is on the **partitioned** path, not the plain in-memory-or-spill
one. So the only remaining m06 lever is **avoiding the partitioned exchange** — a
non-partitioned local inner join — which is exactly ADR 486's local shape, and which
`validate_strategy_shape` currently **rejects** for a `HashPartitioned` join without
exchanges (probe 2's bind-time mismatch).

The honest conclusion across 485-488: m06's ~3x gap to SQLite is the
**partitioned-exchange strategy itself**, chosen for an 88 MB build that fits memory.
Neither budget (485), nor the partition mask (484, a real but bounded 10.9%), nor
build-side selection (488: already optimal) closes it. Closing it requires admitting
a **non-partitioned local inner join** whose only correctness stake is single-node
peak residency (ADR 487: no distributed-match risk, shape/exchange semantics
unchanged in spirit) — but it must clear the `validate_strategy_shape` contract that
today ties `HashPartitioned` to its exchanges.

## Disposition

Probe reverted; tree clean; m06 at baseline. The next cut is scoped precisely: teach
`validate_strategy_shape` (and the binder) to admit a **single-partition local inner
join** — no exchange, orders build (already selected), in-memory — gated on
peak-residency and digest invariance, measured by matched A/B. That is the real m06
lever, and 488 confirms build-side is not the blocker; the exchange contract is.
