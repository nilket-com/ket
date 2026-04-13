# Schema Evolution & Arrow Compatibility

Wendelon routes raw bytes, but higher-level consumers (DataFusion/Polariton) require structured schemas.

## 1. The Arrow IPC Contract
Wendelon relies on Apache Arrow's IPC format, which embeds schema information directly in the message header.
- **Flexibility:** Wendelon itself does not enforce schema validation on the `value` payload. It treats the Arrow batch as an opaque byte slice for replication.
- **Consumer Enforcement:** The DataFusion `TableProvider` is responsible for parsing the Arrow IPC schema upon reading.

## 2. Schema Registry (Optional Control Ring feature)
While Wendelon is a dumb storage layer, the Control Ring may host a lightweight Schema Registry to enforce write-time constraints.
- **Definition:** `CREATE TABLE` commands map to a Raft-committed `NamespaceDescriptor`.
- **Validation:** If enabled, the primary Data Node verifies that the incoming Arrow IPC schema matches the Raft-committed schema before persisting.

## 3. Migration & Casting
- **Backward Compatibility:** Adding nullable columns is safe. Dropping columns or changing types is a breaking change.
- **Read-time Casting:** The WPBC (Pushdown Bytecode) can include cast instructions to map older physical schemas to newer logical schemas during the read path, hiding the schema evolution from the end user.