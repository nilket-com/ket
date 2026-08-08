# ADR 540 - lossless Wendelon query-status boundary

The ADR 0539 confirmation run failed its reads with `storage unavailable` while the audit
stream recorded the truth: `query_budget_exceeded`, twenty-one times. Two unrelated failure
classes — a storage-integrity wedge and a scan-budget expiry — were indistinguishable at the
SQL surface because they shared one collapsed error, and the soak's own findings misattributed
one to the other for a day. The collapse site is concrete: Wendelon's client maps every failed
query status through `query_failure_error` into a plain `io::Error`, and Toron's
`ClientError::Io(_)` arm is a catch-all to `StorageError::Unavailable` (error.rs). ADR 0539
carved exactly one status out of that catch-all — reference-integrity — which proved the shape
works and left every other status still nameless.

The principle is ADR 0539's Decision 1, one level up: an integrity failure may not be reduced
to a generic error, and neither may any other *named* failure. A status the server took the
trouble to distinguish must survive the seam.

## Decision

1. **Every named `ReadStatus` crosses the boundary typed.** The wendelon-client carries failed
   query statuses as a dedicated variant (`ClientError::QueryFailed { status, .. }` or
   equivalent), never as a stringly `Io` error — the collapse becomes unrepresentable, the same
   doctrine as ADR 0539's required-context amendment. Toron maps that variant to a typed
   `StorageError` carrying the status name, and the SQL surface renders it: a budget expiry
   reads `query_budget_exceeded`, not `storage unavailable`.

2. **`StorageError::Unavailable` is reserved** for genuine transport failures and storage
   outages that carry no server-named status. An architecture guard enforces the reservation:
   no path may construct `Unavailable` from a response that contained a `ReadStatus`.

3. **The mapping is total and guarded.** `ReadStatus` is a newtype over named constants, not a
   closed enum, so totality cannot come from `match` exhaustiveness; instead the canonical
   status list is declared once, the mapping consumes it, and a guard test fails when a new
   status constant appears without a mapping entry.

4. **Audit and SQL agree.** The audit event's status label and the SQL error's status name come
   from the same rendering, so the operator never again needs the audit stream to learn what
   the client was actually told.

5. **The integrity path is unchanged.** ADR 0539's typed reference-integrity surfacing remains
   as shipped; this ADR generalizes the seam around it, not through it.

## Grounding

Phase 0 enumerates every `ReadStatus` constant and every construction site of
`StorageError::Unavailable` in Toron, and every `query_failure_error`-shaped mapping in the
Wendelon client — the enumeration is the deliverable, not a sample; a preselected grep is how
the collapse survived this long. The observed confusion pair (budget expiry versus integrity
wedge) becomes the discriminating fixture: one test drives each through the full
Toron-SQL-to-Wendelon path and asserts the two surface distinctly.

## Acceptance

1. A scan that exceeds the server budget surfaces `query_budget_exceeded` at the SQL client
   and in the audit event, from the same rendering.
2. A fenced-table read still surfaces the typed reference-integrity diagnosis exactly as ADR
   0539 shipped it.
3. The status-list guard fails when a `ReadStatus` constant exists without a typed mapping.
4. No construction of `StorageError::Unavailable` from a status-bearing response survives the
   architecture guard.
5. Standard Wendelon, Toron, architecture, format and full `--no-fail-fast` gates pass.

## Guardrails

- Do not rename or renumber existing statuses; this ADR moves names across a seam, it does not
  redesign the taxonomy.
- Do not let the typed variant grow a free-text field that becomes the next collapse point.

## Forward

With statuses lossless, the ADR 0539 confirmation rerun (after ADR 541) can assert its read
failures by name, and the soak's findings stop being attributable only via the audit stream.
