# W8 — Query Surface

**Effort band:** M (4–8 eng-weeks)
**Predecessors:** W6 (write path), W7 (L2 tiering)
**Folds:** `plans/015_server_side_wpbc_execution.md`

End of W8, a client can submit a WPBC (whole-program bytecode) query and have it executed server-side against the slab index and footer, including time-travel queries against MVCC snapshots.

## 1. ADRs consumed

- **ADR 005 (Compute Pushdown via WPBC).** Server-side WPBC execution against slab-local data; opcode surface, resource budget, and error boundary.
- **ADR 013 (Time-Travel Queries with MVCC).** Multi-version concurrency control for historical reads; snapshot retention budget; read-your-writes vs. time-travel semantics.

## 2. Deliverables

1. **WPBC interpreter or JIT** (per ADR 005's decision) loaded into the server and exercised against slab-local data through a bounded opcode surface.
2. **Resource budget** per-query (CPU, memory, wall time) enforced at the interpreter boundary; over-budget queries are terminated with a structured error.
3. **MVCC snapshot storage** integrated with W1's WAL and W7's slab format. Snapshot retention follows ADR 013's budget.
4. **Time-travel query surface** exposed through the SDK (W6) and routed to a specific snapshot.
5. **Fuzz harness** for the WPBC opcode surface — every opcode has fuzz inputs; crashes are build-breakers.

## 3. Acceptance criteria (ADR-referential)

- (ADR 005) A WPBC program cannot escape its resource budget. Verified by an adversarial-program test suite that attempts to consume unbounded memory, fork, or exceed wall-time.
- (ADR 005) WPBC can pushdown to L2 slab footer (W7) without pulling the full slab — the range-read contract holds under pushdown.
- (ADR 013) A time-travel read at a retained snapshot returns the data as-of-snapshot, not as-of-now. Property test confirms no read-your-writes leakage into time-travel.
- (ADR 013) Snapshot pruning respects retention budget; pruned snapshots are audit-logged (ADR 025 emission).
- Fuzz surface runs to the W10 coverage ladder (10k / 1M / 100M) without crash.

## 4. Explicit non-goals

- Client-side WPBC compilation / user-authored opcodes. Ships as post-pilot.
- Cross-tenant query. **W5's dispatcher pairing forbids; no W8-specific work.**
- Time-travel writes (un-redo). Not in scope for pilot.

## 5. Open items

- **ADR 005 Open Question — interpreter vs. JIT choice.** Close at W8 kickoff.
- **ADR 013 Open Question — default snapshot retention.** Land a default and tune against pilot workload.

## 6. Test harness tier

- **Unit:** WPBC opcode semantics; MVCC snapshot accessor.
- **Property:** budget enforcement; time-travel ≠ read-your-writes.
- **Integration:** SDK → cluster query with slab pushdown.
- **Fuzz:** WPBC opcode surface, wired into W10's coverage ladder.

## 7. Phasing (internal)

- **W8.a (M):** WPBC interpreter + resource budget.
- **W8.b (M):** MVCC + time-travel query surface.
- **W8.c (S):** Fuzz harness + W10 ladder wiring.
