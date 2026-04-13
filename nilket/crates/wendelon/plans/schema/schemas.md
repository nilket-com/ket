# Moved from plans/schemas.md

(This file was copied into plans/schema/schemas.md during a planned refactor.)

# Schemas — Bytemuck PODs & Arrow Schemas

This file centralizes all bytemuck Pod/Zeroable Rust struct layouts and Arrow IPC schema definitions. It serves as the canonical source-of-truth for wire formats used across the codebase.

Contents include:
- HybridTimestamp, NodeId, ClientWriteId, TransferToken, FinalizationMarker, LeaseDescriptor
- WriteRequestMeta, WriteResponseMeta, ReadRequestMeta, ReadResponseMeta
- RaftEpochEntryFixed (bytemuck) + epoch_entry_var_schema() (Arrow)
- Arrow schemas: write_payload_schema(), read_key_schema(), read_value_schema()

Refer to plans/schema/hashing.md for cluster hash metadata and ring parameters.
