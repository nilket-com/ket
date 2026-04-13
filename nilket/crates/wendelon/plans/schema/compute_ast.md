# Wendelon Pushdown Bytecode (WPBC) Spec

To keep Wendelon strictly as a high-performance storage layer while supporting complex relational queries from DataFusion and Ballista, Wendelon implements a native, stack-based bytecode AST for Predicate Pushdown.

## 1. The Compute Boundary

Wendelon does not parse SQL. It is a dumb, zero-copy byte server. 
When a DataFusion `TableProvider` executes a query, it compiles the physical plan's `WHERE` clauses and `SELECT` projections into WPBC. 

This bytecode is attached as the "trailing section" of a `ReadRequestMeta` frame. The Wendelon Data Node executes this bytecode against the Arrow `RecordBatch` *before* returning it over the network, drastically reducing QUIC bandwidth and client-side memory pressure.

## 2. The Stack Machine

The WPBC interpreter is a zero-allocation, hyper-optimized stack machine running directly on the Data Node.
- It evaluates the bytecode against each row in the Arrow batch (or utilizing Arrow's vectorized compute kernels where supported).
- The execution produces a boolean filter mask.
- Rows evaluating to `false` are discarded.
- Only the projected columns of the surviving rows are serialized back to the client.

## 3. Instruction Set Architecture (ISA)

WPBC uses 1-byte opcodes followed by optional immediate arguments. 

### 3.1 Data Push (Loads data onto the stack)
- `0x01 PUSH_COL_u32 [4 bytes col_index]`: Pushes the value of column `N` for the current row.
- `0x02 PUSH_INT_i64 [8 bytes literal]`: Pushes a literal 64-bit integer.
- `0x03 PUSH_STR [2 bytes len] [UTF-8 bytes]`: Pushes a literal string.
- `0x04 PUSH_BOOL [1 byte (0|1)]`: Pushes a literal boolean.
- `0x05 PUSH_FLOAT_f64 [8 bytes literal]`: Pushes a literal 64-bit float.
- `0x06 PUSH_TS_ms [8 bytes literal]`: Pushes a literal timestamp (milliseconds).

### 3.2 Comparisons (Pops 2, Pushes Boolean)
- `0x10 EQ`: Equals
- `0x11 NEQ`: Not Equals
- `0x12 GT`: Greater Than
- `0x13 LT`: Less Than
- `0x14 GTE`: Greater Than or Equal To
- `0x15 LTE`: Less Than or Equal To

### 3.3 Logical Operations
- `0x20 AND`: Pops 2 Booleans, Pushes Boolean
- `0x21 OR`: Pops 2 Booleans, Pushes Boolean
- `0x22 NOT`: Pops 1 Boolean, Pushes Boolean (Inverts)

### 3.4 Specialized Operations
- `0x30 IN_LIST [2 bytes count]`: Pops `count` items from the stack, then pops 1 target item. Pushes `true` if the target matches any of the `count` items.
- `0x31 IS_NULL`: Pops 1 item, pushes `true` if it is an Arrow Null.
- `0x32 IS_NOT_NULL`: Pops 1 item, pushes `true` if it is not an Arrow Null.

### 3.5 Terminal Operations
- `0xFF PROJECT [2 bytes count] [col_index 1..N]`: The final instruction. Discards all unrequested columns from the Arrow batch before returning it over QUIC. If omitted, all columns are returned.

## 4. Type System & Safety Bounds

To ensure Wendelon Data Nodes cannot be crashed or OOM'd by malicious or buggy WPBC payloads from DataFusion, strict safety bounds are enforced.

### 4.1 Type System & Mismatches
WPBC is dynamically typed during interpretation (based on the Arrow schema). 
- **Type Mismatch:** If an operation is invalid (e.g., comparing an `Int64` column to a `String` literal with `GT`), the interpreter immediately traps. The row evaluates to `false` (discarded).
- **Extended Types:** The ISA includes push opcodes for advanced Arrow types commonly used in analytics: `PUSH_FLOAT_f64`, `PUSH_TS_ms` (Timestamp milliseconds).

### 4.2 Null Propagation (3VL)
Wendelon adheres strictly to SQL's Three-Valued Logic (3VL).
- If any operand to a comparison (`EQ`, `GT`, etc.) is an Arrow Null, the result of the comparison is Null.
- In WPBC, a Null evaluation at the top of the stack is ultimately treated as `false` for the purpose of row filtering (i.e., the row is discarded), matching standard `WHERE` clause semantics.

### 4.3 Memory and Execution Bounds
- **Stack Depth Limit:** The stack is a fixed-size pre-allocated array of 64 slots. Pushing beyond 64 items traps the interpreter.
- **Instruction Limit:** A single WPBC payload cannot exceed 4096 bytes (4 KB).
- **Execution Fuel:** To prevent infinite loops (if jump instructions are added in V2), the interpreter traps if it executes more than 1024 instructions per row.

## 5. Execution Example

**SQL Query:** `SELECT name, age FROM users WHERE age >= 18 AND status IN ('active', 'pending')`

**WPBC Compilation (sent to Wendelon):**
```text
PUSH_COL_u32 1      // Push 'age' column (index 1)
PUSH_INT_i64 18     // Push literal 18
GTE                 // Pop 18, Pop 'age', Push (age >= 18)

PUSH_COL_u32 2      // Push 'status' column (index 2)
PUSH_STR 6 "active" // Push literal 'active'
PUSH_STR 7 "pending"// Push literal 'pending'
IN_LIST 2           // Pop 'pending', Pop 'active', Pop 'status', Push (status IN ('active', 'pending'))

AND                 // Pop the two booleans, Push final result

PROJECT 2 0 1       // Keep only 'name' (index 0) and 'age' (index 1)
```

By executing this locally, Wendelon ensures DataFusion only receives the exact bytes necessary to satisfy the query, adhering to the strict Storage/Compute decoupling.
