# Moved from plans/hashing.md

(This file was copied into plans/schema/hashing.md during a planned refactor.)

# Hashing & Control Ring Parameters

- The cluster records `hash_algo`, `ring_bits`, and any future locality-hash parameters in the initial epoch metadata (epoch_entry_var_schema meta_keys/meta_values).
- Default: BLAKE3-derived locality projection, `ring_bits = 64`. Record `Identity` remains full BLAKE3-256; ring placement consumes only the 64-bit `Locality` surface.
- Alternatives: Rapidhash, XXH3 (non-crypto), or other BLAKE3 parameterizations with tradeoffs documented in the original spec.

Migration: changing the algorithm or ring_bits requires a full cluster rebootstrap.
