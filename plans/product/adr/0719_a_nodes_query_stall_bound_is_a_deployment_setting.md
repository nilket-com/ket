# ADR 0719: a node's query stall bound is a deployment setting

Status: proposed, 2026-11-12 (Claude, Codex). Design accepted by Codex 01a1d0cc (`reviews/dc223-20261112/CODEX-DESIGN-A-GO.txt`) after the DC-223 diagnostic and characterization (`reviews/dc223-20261112/RESULT.md`, rulings 01a1d0bb, 01a1d0c1, 01a1d0c7). Implementation lands with this plan.

## Why
- A query is refused with `QUERY_STALLED` when one unit of its work makes no progress for 250 ms (ADR 541). The unit is a location's whole read, a batch or an encode quantum, not one store round trip.
- For a tiered, encrypted record that read is several object-store requests in sequence: a size lookup, the tail window, sometimes additional metadata, then the record's ciphertext. Reopen verification, an inline rehydration, one re-resolution after a missing object and the adapter's retries can add to the same unit.
- **So a store whose every request is well under 250 ms can still fail the query.** Observed on the three-node workflow against RustFS through two ssh forwards (two executions of two, both at the whole-record read, `network/data_server.rs` `clocks.bound(slab.read(location))`), and reproduced without any remote service: four requests of 100 ms each are refused at 250 ms with the third outstanding (DC-223).
- The 250 ms was a constant with six constructions in the query path. A deployment whose object store is slower than a local disk had no way to state a different bound.

## Decision
**The no-progress bound of the queries a node executes is that node's setting, `[tuning] query_stall_bound_millis`. Its meaning, its default and every other part of ADR 541 are unchanged.**

```toml
[tuning]
query_stall_bound_millis = 250   # optional; absent means 250; 1 to 600000
```

1. **Meaning unchanged.** The bound still covers one unit of progress. Completing a location or a batch still marks progress and resets it. It is not a query lifetime and not a per-request timeout.
2. **Range.** An integer number of milliseconds from 1 to 600,000. Zero is refused: it is not "off", and there is no way to switch the bound off. The upper limit keeps the value a bound that a stuck operation reaches. **The range is an administrative supported range, not a derived safety threshold and not a store latency promise.** Absent means 250 ms.
3. **Errors.** A value outside the range is refused at load with `tuning.query_stall_bound_millis must be between 1 and 600000`. A mistyped key, a non-integer or a negative value is a parse failure and keeps ADR 0718's contract: position only, no key, no value.
4. **Whose setting.** The node that executes the query applies its own value. Nothing about it is on the wire; no format changes. **A caller cannot raise it.** Nodes of one cluster may hold different values; a query then meets the bound of whichever node executes it.
5. **Caller deadlines unchanged.** A caller's deadline, when sent, is still raced concurrently and still wins whenever it is nearer, with its own status (`QUERY_DEADLINE_EXCEEDED`). An aggregate's caller wall budget is still its deadline and never replaces the stall bound.
6. **One source.** `QueryStallBound` holds a validated value privately; a node's data server holds one. `QueryExecutionBudget` no longer has a `Default`: its only defaulting constructor takes the bound, so no entry path can keep a number of its own. Constructions, all in `network/data_server.rs`:
   - the table query handler (range and secondary-index plans, and the ParentDecisions reader): the server's value;
   - the key-range query handler: the server's value;
   - the aggregate handler: the server's value;
   - `execute_query_plan`, a helper with no production caller that drives the plan in tests: the default, stated at the call;
   - `query_plan_error`, the placeholder clock of an already-refused plan under which nothing executes: the default, stated at the call.
7. **Programmatic construction.** `NodeBootstrapConfig::query_stall_bound`, default 250 ms; `NodeBootstrapConfig::dev` keeps the default. The production bootstrap passes it to the data server.
8. **Unchanged and out of scope:** progress semantics, the default, write clocks, CDC, the index-publication gate waits, counters, the object-store adapter, the number of requests a tiered read makes, rehydration, reopen verification.

## Algebra
- Identity of the setting: the executing node. Key used: the node's own configuration, read once at start. Not narrower than its identity: there is no per-tenant, per-table or per-request bound, and none is implied.
- For a unit of `n` sequential requests each taking `t`, the unit completes under bound `B` only if `n·t` plus local work is under `B`. `n` is not fixed: it depends on encryption, object size against the tail window, and the branches above. **No value of `B` is derived here for any store.**

## Acceptance
- Config: absent and the rendered example give 250 ms; 1, 1,500 and 600,000 load and reach the node configuration; 0 and 600,001 are refused by field name; a mistyped key, a string and a negative number fail by position only, echoing neither key nor value.
- A real encrypted tiered slab behind a test store that delays each request, under the real range plan:
  - default bound, 40 ms per request: served, both rows, although the query as a whole runs longer than the bound;
  - default bound, 100 ms per request: `QUERY_STALLED` at the bound, three requests issued, one outstanding, none slower than 101 ms;
  - 1,000 ms bound, 100 ms per request: served, both rows;
  - 1,000 ms bound, a request that never answers: `QUERY_STALLED` at 1,000 ms;
  - 1,000 ms bound, caller deadline 150 ms ahead: `QUERY_DEADLINE_EXCEEDED`.
  These run the store's delay on tokio's paused clock. They demonstrate the bounded read; they do not exercise the `std::time::Instant` poll between units.
- A node bootstrapped through the production path with a non-default value: its table query handler and its key-range query handler return plans whose clocks carry that value. The aggregate handler's response exposes no clock; its budget builder is checked directly to carry the bound it is given, and the handler has no other bound to give it.
- No `QueryExecutionBudget::default` and no 250 ms literal remain in the query path.

## Delivery
One plan commit and one implementation commit. Focused tests during review; one full checkpoint on the final pair before delivery. No external RustFS run is part of this ADR.

## Limits
- **This is a policy control, not a correction of remote-read cost.** A tiered record read is still several sequential requests. The default is still 250 ms, and **the tunnelled RustFS workflow that failed still fails unchanged**; this ADR does not set a larger value for it.
- **DC-223 stays open**, re-scoped: what remains is a real-network workflow run with a declared bound, and whether the request count per read should fall. Counting each request as progress would change ADR 541 and is not proposed.
- Not addressed: DC-224 (a client sent to a stopped node), a daemon selecting ParentDecisions, allocation ownership and section 8 (FAILED, unwaived), fault and real-process testing.
