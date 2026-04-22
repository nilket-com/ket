# Brainstorm 004: Real-Time Eventing, CDC, and Pushdown Triggers

## The Premise
As Wendelon handles massive high-throughput ingest, the natural next requirement is routing that data to downstream systems (CDC) or executing logic based on data changes (Triggers/Webhooks). Bolting on external polling systems (like Debezium) destroys performance and introduces latency. 

Fortunately, Wendelon's core architecture—QUIC multiplexing, Raft WALs, and WebAssembly (WPBC)—provides exactly the primitives needed to build a native, zero-polling real-time eventing system.

## 1. Zero-Polling CDC via the Raft WAL
The Raft Write-Ahead Log (WAL) in the L1 tier is the absolute source of truth for all mutations.
*   **The Mechanism:** Instead of clients polling for updates (`WHERE updated_at > X`), a client opens a QUIC stream and subscribes directly to a specific `Locality`'s Raft log.
*   **The Advantage:** As the consensus leader commits a new entry to the state machine, it simultaneously writes the bytes directly to the subscriber's network socket. This provides zero-polling CDC where the latency from "Write Committed" to "Event Received" is strictly bounded by the network RTT.

## 2. "Pushdown Triggers" via WPBC (Rune-Targeted Bytecode)
Traditional synchronous database triggers are notorious for eating CPU on the primary node and bottlenecking the ingest path.
*   **The Mechanism:** We leverage the existing **WPBC (Wendelon Pushdown Bytecode)** engine. WPBC is its own distinct, vectorized bytecode, but its primary high-level language target is **Rune (`rune-rs`)**. Users can write complex, expressive trigger logic in Rune, which Toron compiles into WPBC and binds to a `tenant_id` or specific `Locality`.
*   **The Advantage:** When a write arrives, the storage node executes the WPBC module locally. Because WPBC is a strictly constrained execution environment, it can safely enforce validation rules, enrich data, or normalize records inline. We completely avoid the 4GB memory limits of Wasm, while giving data engineers the full expressive power of Rune (a dynamic Rust-like language) to script the database's internal behavior natively.

## 3. Continuous / Streaming Queries over QUIC
Because Wendelon uses QUIC (via `quinn`), maintaining thousands of cheap, multiplexed streams is trivial.
*   **The Mechanism:** A client issues an OLAP query (e.g., `SELECT * FROM tenant_5 WHERE status = 'error'`) but attaches a `SUBSCRIBE` flag.
*   **The Advantage:** The Wendelon node executes the query against historical L2/L1 data and streams the results back. *Crucially, it keeps the QUIC stream open.* As new data arrives in L1, it is evaluated against the query's WPBC filter. If it matches, the new record is instantly pushed down the open stream. The client receives a seamless, unified blend of historical data and a real-time tail.

## 4. Async Webhook Queues via Native Storage
Synchronously triggering external HTTP webhooks (e.g., hitting a Stripe API on insert) from the database core will immediately destroy ingest throughput.
*   **The Mechanism:** We introduce a native `Webhook` schema. A lightweight, inline WPBC trigger evaluates a rule and writes a simple "Event" record into a dedicated, hidden `webhook_events` Locality on the Ring.
*   **The Advantage:** A background worker (similar to the L2 Sweeper) tails this specific Locality. It manages the fragile external HTTP calls, retries, exponential backoff, and dead-letter queues asynchronously. This completely decouples unreliable external networks from Wendelon's blistering fast internal ingest path.

## Conclusion
By surfacing Wendelon's internal replication and compute primitives to the client, we can provide world-class, low-latency streaming and CDC capabilities without introducing external dependencies or compromising the core ingest loop.