# Brainstorm 002: Vector Embeddings as a First-Class Citizen

## The Premise
The current landscape of Vector Databases (like Pinecone, Milvus, or Qdrant) often treats the database as a standalone appliance dedicated purely to vector similarity search. However, the core operational requirements of a Vector DB—massive sequential scans, intense per-row compute, and the struggle with distributed indexing/metadata filtering—align perfectly with Wendelon's architectural strengths: the **Unified Ring**, **Context-Aware Locality**, and **WPBC (Wendelon Pushdown Bytecode)**.

By integrating vector capabilities at the core, Wendelon could offer a "Holy Grail" hybrid search engine without bolting on an external system.

## 1. Locality Sensitive Hashing (LSH) on the Unified Ring
Currently, Wendelon's `Locality` projection determines where data lives physically. For OLAP, we discussed Z-ordering by `(tenant, time)`. For vector workloads, we can introduce **Locality Sensitive Hashing (LSH)** as a native projection function.

*   **Mechanism:** LSH hashes similar vectors to the same (or adjacent) values. By mapping the LSH output to a coordinate on the Unified Ring, **semantically similar vectors will physically reside on the same or neighboring nodes.**
*   **The Advantage:** When executing a Nearest Neighbor (k-NN) query, we hash the target vector, locate its coordinate on the ring, and restrict the query to that specific arc. This eliminates the need for a centralized, massive vector index and prunes the majority of the cluster from the search space instantly.

## 2. Compute Pushdown (WPBC) for Distance Metrics
Moving high-dimensional vector data (e.g., arrays of 1536 floats) across the network to a central compute layer is a severe bottleneck.

*   **Mechanism:** Wendelon's WPBC allows us to push the query vector and the distance function (Cosine, L2 Euclidean, Dot Product) directly down to the storage nodes.
*   **The Advantage:** Storage nodes execute the similarity calculations locally (ideally leveraging Rust SIMD instructions). They only return the Top-K closest matches over the network. This transforms a network-IO bound problem into a purely CPU-bound one, maximizing throughput.

## 3. Native Product Quantization (PQ) in the L2 Slab Format
Memory pressure is the enemy of vector search. Fitting billions of raw float vectors in RAM is cost-prohibitive.

*   **Mechanism:** Wendelon's L2 tier ("Slab Format") can natively incorporate Product Quantization. The massive, raw float vectors remain on cold storage (NVMe/S3), but the Slab's footer/index retains highly compressed, quantized versions of the vectors.
*   **The Advantage:** Storage nodes can load billions of compressed vectors into RAM. WPBC performs a lightning-fast approximate search against the compressed vectors in memory, identifies the top candidates (e.g., Top 100), and only then fetches the raw float vectors from NVMe for exact distance re-ranking.

## 4. Solving the "Hybrid Search" Problem (Vector + OLAP)
The biggest pain point in modern AI architectures (RAG) is "metadata filtering" prior to vector search (e.g., "Find nearest neighbors, but only where `tenant_id = 5` and `timestamp > yesterday`").

*   **Mechanism:** By combining our OLAP Z-ordering with vector storage, we solve this elegantly. Vectors are stored alongside the relational metadata. 
*   **The Advantage:** The query planner routes the request based on the metadata (e.g., routing to the nodes holding `Tenant 5`'s recent data). Those nodes perform a massive sequential scan, apply the exact time/tenant filters first, and *then* run the vector distance calculation strictly on the surviving subset. This provides perfect, precise hybrid search without the complex synchronization required between a relational DB and a separate Vector DB.

## Conclusion
Wendelon's architecture is accidentally a perfect blueprint for a next-generation Vector DB. By viewing vector similarity search as just another form of "pushed-down compute over localized data," we can achieve linear scalability and perfect hybrid search out of the box.