# Security & Key Management

Wendelon must be secure by default, operating under zero-trust principles for both in-flight and at-rest data.

## 1. Network Security (mTLS)
- **QUIC / TCP:** All inter-node and client-to-node communication is strictly encrypted via TLS 1.3 using `quinn`.
- **Identity:** Nodes authenticate via mTLS. Certificates are issued by an internal or external PKI. A node's `NodeId` must map to its X.509 Subject Alternative Name (SAN).

## 2. Data-at-Rest (Envelope Encryption)
- **Slab Encryption:** Data Ring slabs (L1 NVMe and L2 Object Storage) must be encrypted before writing to disk.
- **Data Encryption Keys (DEK):** Generated dynamically per-slab using AES-256-GCM.
- **Key Encryption Keys (KEK):** The DEK is encrypted using a Master KEK managed by an external KMS (AWS KMS, HashiCorp Vault).

## 3. Key Rotation
- **Raft WAL:** KEK rotation is coordinated via a Raft `CONFIG` entry. 
- **Lazy Re-encryption:** Old slabs are not immediately re-encrypted on KEK rotation. The metadata index tracks which KEK version encrypted which slab's DEK. Re-encryption happens naturally during background `E_safe` compaction.