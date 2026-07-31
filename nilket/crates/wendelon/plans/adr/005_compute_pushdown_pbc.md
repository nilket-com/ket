# ADR 005: Decoupling Compute via Pushdown Bytecode (PBC)

## Status
Accepted

## Context
Wendelon is designed to back Polariton, an analytical platform utilizing Apache DataFusion. Users will expect to run complex SQL queries against the database. If we pull DataFusion into Wendelon, the database becomes a monolithic compute-and-storage engine, complicating resource isolation and violating our specialized storage mandate. However, sending terabytes of unfiltered data over the network to an external DataFusion cluster is prohibitively slow.

## Decision
Wendelon will remain a **Dumb, Fast Storage Layer** and will **NOT** parse or execute SQL natively. 
To support performant queries, Wendelon will implement **Pushdown Bytecode (PBC)**—a minimal, stack-based bytecode interpreter. External compute layers (DataFusion) will compile predicates (`WHERE` clauses) and projections into PBC and send them alongside the read request.

## Rationale
- **Strict Decoupling:** Compute scales differently than storage. This boundary allows users to scale DataFusion/Ballista executors independently of Wendelon Data Nodes.
- **Network Efficiency:** PBC allows the Data Node to filter rows and drop columns *before* placing the Arrow batch on the network, achieving the performance of a monolithic database without the architectural coupling.

## Consequences (The "Negatives" / Rejected Ideas)
- **Rejected Monolithic SQL:** We explicitly reject building a SQL parser or embedding a full SQL execution engine into the Wendelon binary.
- **Rejected Wasm (For V1):** We considered using WebAssembly for UDFs and pushdown, but rejected it for V1 due to the overhead of Wasm sandboxing on the ultra-fast read path. We accept the burden of maintaining a custom, limited PBC stack machine instead.
