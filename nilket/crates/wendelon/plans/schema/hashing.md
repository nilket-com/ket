# Moved from plans/hashing.md

(This file was copied into plans/schema/hashing.md during a planned refactor.)

# Hashing & Control Ring Parameters

- The cluster records `hash_algo`, `hash_key_hex`, and `ring_bits` in the initial epoch metadata (epoch_entry_var_schema meta_keys/meta_values).
- Default: keyed HighwayHash, ring_bits = 64, per-cluster key generated at init and stored in the initial RaftEpochEntry.
- Alternatives: Rapidhash, XXH3 (non-crypto), BLAKE3 (crypto) with tradeoffs documented in the original spec.

Migration: changing the algorithm or ring_bits requires a full cluster rebootstrap.
