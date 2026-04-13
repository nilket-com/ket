# Connection Pooling & Quinn Lifecycle

Wendelon's custom transport layer requires careful management of QUIC connections to maintain zero-copy, low-latency routing without connection exhaustion.

## 1. Endpoint Geometry
Each Wendelon Data Node listens on two distinct `quinn::Endpoint`s:
- **Data Ring Endpoint (e.g., `:4001`):** High-throughput, large `max_frame_size` (5 MiB). Handles `HRFT` reads and writes.
- **Control Ring Endpoint (e.g., `:4002`):** Low-throughput, highly secure (strict mTLS), small `max_frame_size` (64 KiB). Handles Raft consensus (`APPEND_ENTRIES`, `VOTE`) and lease management.

## 2. Server-Side Connection Limits
- `max_concurrent_connections`: Tuned based on `ulimit` (e.g., 100,000 for Data Ring, 1,000 for Control Ring).
- `max_concurrent_bidi_streams`: 256 per connection for the Data Ring (allowing high multiplexing from a single client) and 16 for the Control Ring.
- **Idle Timeout:** Connections idle for 30 seconds are forcefully closed to reclaim resources.
- **Keep-Alive:** Configured at 10 seconds to prevent stateful firewalls/NATs from aggressively dropping the UDP session.

## 3. Client SDK Connection Pooling
The `WendelonClient` must maintain an intelligent pool of `quinn::Connection` objects:
- **Topology-Aware Pooling:** The client maintains exactly one persistent `quinn::Connection` per active Data Node in `ring(E)`.
- **Stream Multiplexing:** When the client executes `.write()` or `.read()`, it DOES NOT open a new connection. It opens a new `quinn::SendStream` / `quinn::RecvStream` pair (a bi-directional stream) over the existing connection.
- **Reconnection Strategy:** If a connection drops (e.g., `ConnectionLost` error), the SDK enters an exponential backoff retry loop (capped at 5 seconds). During this time, queries to that node either block or immediately fail based on the `ReadOptions` timeout.
- **Epoch Refresh Trigger:** A dropped connection often correlates with a node failure. The SDK should proactively query the Control Ring for a new `ring(E)` snapshot on a connection loss.