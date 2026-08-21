# ADR 577 - QUIC liveness and statement timeouts

**Status:** Proposed 2026-08-21. Closes the transport finding raised by the
PRODUCT-0137 scatter-threshold evidence; it does not decide scan performance
or result streaming.

## Context

The live scatter exercise found that a silent SQL or Rune query loses its QUIC
connection at 30.01 seconds. This is not a chart threshold or a
materialization-specific failure. Toron's daemon executes both frontends by
collecting `execute_query_with_context` before it writes a response, so a long
read can produce no application packets. None of Toron's server or client
QUIC constructors installs a `TransportConfig`.

Quinn's default `max_idle_timeout` is 30 seconds and its
`keep_alive_interval` is disabled. That unchosen transport deadline therefore
preempts both the configured SQL statement default (60 seconds) and the
Toron-client default query timeout (120 seconds). The daemon already has a
typed `StatementTimeout` branch, but it cannot reach the client when QUIC
closes first.

The apparent 14,000-row boundary in the evidence is consequently a wall-clock
artifact. The measured approximately 460 rows/second is real but its cause is
not yet known: no comparable non-materialized-table control could be created
or read before the transport closed.

`ExecuteRequest` and `ExecuteRuneRequest` may supply a
`statement_timeout_seconds` value, and the daemon adopts it directly. It has
no server-side maximum today. A static transport timeout merely set above the
60-second configuration default would still preempt a longer request override;
it is not an authority boundary for statement execution.

## Decision

### 1. The daemon's statement deadline is the server execution authority

For SQL and Rune alike, the daemon continues to select the request timeout
when supplied and otherwise the configured statement timeout. It cancels the
query and returns the existing typed `StatementTimeout` outcome when that
deadline expires.

QUIC liveness must not create a shorter, unconfigured execution deadline.
The client-side query timeout remains a caller-side deadline. A request whose
chosen statement timeout exceeds the client's configured query timeout may
still receive the client's explicit local timeout; it must not instead receive
a transport-loss error.

### 2. Every Toron SQL QUIC endpoint receives an explicit liveness policy

Toron's loopback and authenticated server and client constructors, including
the public `toron-client` constructors, install a `quinn::TransportConfig`.
The policy has both of these explicit values:

- a finite `max_idle_timeout`, retained for genuinely abandoned connections;
- a non-`None` `keep_alive_interval` strictly shorter than that timeout.

The interval is the mechanism that preserves an otherwise healthy connection
while execution is silent. Quinn documents that one peer's keep-alives suffice
only when the interval is lower than both peers' idle timeouts, so Toron's
owned peers configure the same policy rather than relying on an accidental
default or on a particular client implementation.

The constants live beside Toron's QUIC configuration, not in the SQL planner
or in a query handler. Their names state the relationship and a unit control
asserts `keep_alive_interval < max_idle_timeout`. The idle value is selected
with the configured statement default in mind, but the correctness guarantee
for per-request overrides comes from keep-alive rather than from an assumed
numeric ordering.

### 3. Result streaming is a separate execution-path decision

Streaming batches as `execute_query_stream_with_context` yields them would
also generate traffic and improve time-to-first-row. It changes response
framing, audit timing, cancellation, and partial-result semantics. This cut
does not smuggle that change into a transport repair: it preserves the current
collect-then-write behavior and makes the connection live underneath it.

## Acceptance

- No Toron SQL server or client QUIC constructor relies on Quinn's default
  liveness configuration. A source-level guard enumerates the owned
  constructors and requires the shared explicit policy at each.
- The configured interval is nonzero and strictly shorter than the configured
  finite idle timeout. Removing either setting, disabling either one, or
  bypassing the shared policy fails the guard.
- An end-to-end SQL request which is deliberately silent for longer than the
  previous 30-second transport boundary and shorter than its statement deadline
  succeeds. The control uses a deterministic test hook or controllable clock,
  not a timing race.
- A deliberately silent request which exceeds its statement deadline reaches
  the client as `SqlErrorCode::StatementTimeout` / the Rune execution timeout,
  never as a QUIC transport error.
- A request-level timeout longer than the configured default survives past that
  default without a transport disconnect. This proves the liveness guarantee
  does not depend on a static idle timeout being greater than every request.
- The existing client-side query deadline remains explicit and separately
  tested; a client timeout is reported as the client's timeout, not as a
  statement timeout or a transport disconnect.

## Guardrails

The liveness policy applies to Toron SQL transport only. It does not silently
alter Wendelon's independently owned QUIC policy. A later worker-transport
record may decide whether its traffic has the same silent-execution exposure.

The timeout tests must observe the typed result at the public client boundary.
A daemon-only test of its timeout branch is insufficient: the defect is that
the transport previously prevented that result from crossing the wire.

## Risks and deferrals

Keep-alives create small periodic traffic for open connections. That is an
intentional trade for allowing an admitted statement to reach its declared
outcome; the finite idle timeout still cleans up genuinely abandoned peers.

This record does not diagnose the flat per-row scan cost, tune scatter
thresholds, cap request-provided statement timeouts, or add a client API for a
query timeout larger than its current default. Those are separate policy or
performance decisions once long control reads can complete.

## Forward

After this cut, rerun the PRODUCT-0137 materialized-versus-plain control at
equal cardinality. Only then decide whether the approximately 460 rows/second
rate belongs to materialized layout, general scan execution, or the viz
consumer. Result streaming remains a separate candidate if time-to-first-row
or memory behavior earns a decision.
