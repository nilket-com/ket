//! Wendelon Pushdown Bytecode (WPBC) interpreter.
//!
//! The stack machine defined in `plans/schema/compute_ast.md`.
//! DataFusion `TableProvider`s compile `WHERE` / `SELECT` fragments
//! into WPBC and attach the bytes to a `ReadRequestMeta` frame's
//! trailing section. A Wendelon Data Node runs the program locally
//! against the Arrow `RecordBatch` it was about to return, yielding
//! a filtered + projected batch so only the rows the client asked
//! for cross the network.
//!
//! ## Safety model
//!
//! Every bound from the spec §4.3 is enforced:
//!
//! * programs longer than [`MAX_BYTECODE_BYTES`] are rejected at
//!   compile time,
//! * the evaluation stack is capped at [`STACK_DEPTH_LIMIT`] slots,
//! * each row is given [`FUEL_PER_ROW`] opcode ticks before a trap.
//!
//! A trap — whether from stack over/underflow, fuel exhaustion,
//! type mismatch, or NaN — evaluates the current row to `false`
//! (i.e. the row is discarded). Null operands propagate per 3VL
//! and ultimately drop the row; `IS_NULL` / `IS_NOT_NULL` are the
//! only ways to produce a concrete boolean from a Null.
//!
//! ## Compile-then-run
//!
//! The raw byte stream is parsed once into a [`Program`] (a `Vec`
//! of decoded [`Op`]s plus an optional projection list); per-row
//! evaluation then walks that compiled form so we pay string /
//! slice decoding exactly once per query rather than per row.

use arrow::array::{
    Array, BooleanArray, BooleanBuilder, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, TimestampMillisecondArray,
};
use arrow::compute;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::io::{Error, ErrorKind};

// ============================================================
//  OPCODES — mirrors `plans/schema/compute_ast.md` §3.
// ============================================================

pub const OP_PUSH_COL: u8 = 0x01;
pub const OP_PUSH_INT: u8 = 0x02;
pub const OP_PUSH_STR: u8 = 0x03;
pub const OP_PUSH_BOOL: u8 = 0x04;
pub const OP_PUSH_FLOAT: u8 = 0x05;
pub const OP_PUSH_TS: u8 = 0x06;

pub const OP_EQ: u8 = 0x10;
pub const OP_NEQ: u8 = 0x11;
pub const OP_GT: u8 = 0x12;
pub const OP_LT: u8 = 0x13;
pub const OP_GTE: u8 = 0x14;
pub const OP_LTE: u8 = 0x15;

pub const OP_AND: u8 = 0x20;
pub const OP_OR: u8 = 0x21;
pub const OP_NOT: u8 = 0x22;

pub const OP_IN_LIST: u8 = 0x30;
pub const OP_IS_NULL: u8 = 0x31;
pub const OP_IS_NOT_NULL: u8 = 0x32;

pub const OP_PROJECT: u8 = 0xFF;

// ============================================================
//  SAFETY BOUNDS — spec §4.3.
// ============================================================

pub const MAX_BYTECODE_BYTES: usize = 4096;
pub const STACK_DEPTH_LIMIT: usize = 64;
pub const FUEL_PER_ROW: usize = 1024;

// ============================================================
//  COMPILED FORM
// ============================================================

#[derive(Clone, Debug)]
enum Op {
    PushCol(u32),
    PushInt(i64),
    PushStr(Vec<u8>),
    PushBool(bool),
    PushFloat(f64),
    PushTs(i64),
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
    Not,
    InList(u16),
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug, Default)]
pub struct Program {
    ops: Vec<Op>,
    /// Column indices requested by a terminating `PROJECT`. `None`
    /// means the program did not emit a PROJECT and every column
    /// should be returned.
    project: Option<Vec<usize>>,
}

impl Program {
    pub fn compile(bytecode: &[u8]) -> std::io::Result<Self> {
        if bytecode.len() > MAX_BYTECODE_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "WPBC bytecode is {} bytes; {MAX_BYTECODE_BYTES}-byte cap exceeded",
                    bytecode.len()
                ),
            ));
        }
        compile(bytecode)
    }
}

fn compile(bytecode: &[u8]) -> std::io::Result<Program> {
    let mut ops = Vec::new();
    let mut project = None;
    let mut i = 0;
    while i < bytecode.len() {
        let opcode = bytecode[i];
        i += 1;
        match opcode {
            OP_PUSH_COL => {
                let col = read_u32(bytecode, &mut i)?;
                ops.push(Op::PushCol(col));
            }
            OP_PUSH_INT => {
                let v = read_i64(bytecode, &mut i)?;
                ops.push(Op::PushInt(v));
            }
            OP_PUSH_STR => {
                let len = read_u16(bytecode, &mut i)? as usize;
                let bytes = read_slice(bytecode, &mut i, len)?.to_vec();
                ops.push(Op::PushStr(bytes));
            }
            OP_PUSH_BOOL => {
                let b = read_slice(bytecode, &mut i, 1)?[0] != 0;
                ops.push(Op::PushBool(b));
            }
            OP_PUSH_FLOAT => {
                let v = f64::from_le_bytes(
                    read_slice(bytecode, &mut i, 8)?.try_into().unwrap(),
                );
                ops.push(Op::PushFloat(v));
            }
            OP_PUSH_TS => {
                let v = read_i64(bytecode, &mut i)?;
                ops.push(Op::PushTs(v));
            }
            OP_EQ => ops.push(Op::Eq),
            OP_NEQ => ops.push(Op::Neq),
            OP_GT => ops.push(Op::Gt),
            OP_LT => ops.push(Op::Lt),
            OP_GTE => ops.push(Op::Gte),
            OP_LTE => ops.push(Op::Lte),
            OP_AND => ops.push(Op::And),
            OP_OR => ops.push(Op::Or),
            OP_NOT => ops.push(Op::Not),
            OP_IN_LIST => {
                let count = read_u16(bytecode, &mut i)?;
                ops.push(Op::InList(count));
            }
            OP_IS_NULL => ops.push(Op::IsNull),
            OP_IS_NOT_NULL => ops.push(Op::IsNotNull),
            OP_PROJECT => {
                let count = read_u16(bytecode, &mut i)? as usize;
                let mut indices = Vec::with_capacity(count);
                for _ in 0..count {
                    indices.push(read_u32(bytecode, &mut i)? as usize);
                }
                project = Some(indices);
                if i != bytecode.len() {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "WPBC: PROJECT must be the terminal instruction",
                    ));
                }
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("WPBC: unknown opcode 0x{other:02x}"),
                ));
            }
        }
    }
    Ok(Program { ops, project })
}

// ============================================================
//  PUBLIC ENTRY POINT
// ============================================================

/// Execute `bytecode` against `batch`, returning a new batch that
/// contains only the rows whose program top-of-stack was a concrete
/// `true`, projected to the columns named by the program's final
/// PROJECT instruction (or all columns if PROJECT was omitted).
///
/// Empty bytecode is treated as "no filter, no projection" — the
/// input batch is returned untouched.
pub fn execute(bytecode: &[u8], batch: &RecordBatch) -> std::io::Result<RecordBatch> {
    let program = Program::compile(bytecode)?;
    execute_program(&program, batch)
}

pub fn execute_program(program: &Program, batch: &RecordBatch) -> std::io::Result<RecordBatch> {
    // Filter stage. An empty op list means the caller sent only a
    // PROJECT (or nothing at all); either way there's no predicate
    // to apply and every row passes.
    let filtered = if program.ops.is_empty() {
        batch.clone()
    } else {
        let n_rows = batch.num_rows();
        let mut mask = BooleanBuilder::with_capacity(n_rows);
        for row in 0..n_rows {
            mask.append_value(eval_row(&program.ops, batch, row));
        }
        let filter: BooleanArray = mask.finish();
        compute::filter_record_batch(batch, &filter)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("WPBC filter: {e}")))?
    };

    match &program.project {
        Some(indices) => filtered
            .project(indices)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("WPBC project: {e}"))),
        None => Ok(filtered),
    }
}

// ============================================================
//  PER-ROW INTERPRETER
// ============================================================

#[derive(Clone, Debug)]
enum Value<'a> {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    /// Stored as raw bytes; `PUSH_STR` and string/utf8 columns both
    /// land here so comparison is a pure byte compare with no UTF-8
    /// re-validation on the hot path.
    Str(&'a [u8]),
    Ts(i64),
}

fn eval_row(ops: &[Op], batch: &RecordBatch, row: usize) -> bool {
    let mut stack: Vec<Value<'_>> = Vec::with_capacity(STACK_DEPTH_LIMIT);
    let mut fuel = FUEL_PER_ROW;
    for op in ops {
        if fuel == 0 {
            return false;
        }
        fuel -= 1;
        if !exec_op(op, &mut stack, batch, row) {
            return false;
        }
        if stack.len() > STACK_DEPTH_LIMIT {
            return false;
        }
    }
    matches!(stack.last(), Some(Value::Bool(true)))
}

fn exec_op<'a>(
    op: &'a Op,
    stack: &mut Vec<Value<'a>>,
    batch: &'a RecordBatch,
    row: usize,
) -> bool {
    match op {
        Op::PushCol(c) => match read_column(batch, *c as usize, row) {
            Some(v) => stack.push(v),
            None => return false,
        },
        Op::PushInt(v) => stack.push(Value::Int(*v)),
        Op::PushStr(b) => stack.push(Value::Str(b.as_slice())),
        Op::PushBool(b) => stack.push(Value::Bool(*b)),
        Op::PushFloat(v) => stack.push(Value::Float(*v)),
        Op::PushTs(v) => stack.push(Value::Ts(*v)),
        Op::Eq | Op::Neq | Op::Gt | Op::Lt | Op::Gte | Op::Lte => {
            let Some(b) = stack.pop() else { return false };
            let Some(a) = stack.pop() else { return false };
            match compare(&a, &b, op) {
                Some(v) => stack.push(v),
                None => return false,
            }
        }
        Op::And => {
            let Some(b) = stack.pop() else { return false };
            let Some(a) = stack.pop() else { return false };
            match (&a, &b) {
                (Value::Bool(x), Value::Bool(y)) => stack.push(Value::Bool(*x && *y)),
                (Value::Null, Value::Bool(false)) | (Value::Bool(false), Value::Null) => {
                    // 3VL: false AND null is false.
                    stack.push(Value::Bool(false));
                }
                (Value::Null, _) | (_, Value::Null) => stack.push(Value::Null),
                _ => return false,
            }
        }
        Op::Or => {
            let Some(b) = stack.pop() else { return false };
            let Some(a) = stack.pop() else { return false };
            match (&a, &b) {
                (Value::Bool(x), Value::Bool(y)) => stack.push(Value::Bool(*x || *y)),
                (Value::Null, Value::Bool(true)) | (Value::Bool(true), Value::Null) => {
                    // 3VL: true OR null is true.
                    stack.push(Value::Bool(true));
                }
                (Value::Null, _) | (_, Value::Null) => stack.push(Value::Null),
                _ => return false,
            }
        }
        Op::Not => {
            let Some(a) = stack.pop() else { return false };
            match a {
                Value::Bool(x) => stack.push(Value::Bool(!x)),
                Value::Null => stack.push(Value::Null),
                _ => return false,
            }
        }
        Op::InList(n) => {
            let n = *n as usize;
            if stack.len() < n + 1 {
                return false;
            }
            let split = stack.len() - n;
            let items: Vec<Value<'a>> = stack.drain(split..).collect();
            let Some(target) = stack.pop() else { return false };
            if matches!(target, Value::Null) {
                stack.push(Value::Null);
            } else {
                let mut found = false;
                for it in &items {
                    if matches!(compare(&target, it, &Op::Eq), Some(Value::Bool(true))) {
                        found = true;
                        break;
                    }
                }
                stack.push(Value::Bool(found));
            }
        }
        Op::IsNull => {
            let Some(a) = stack.pop() else { return false };
            stack.push(Value::Bool(matches!(a, Value::Null)));
        }
        Op::IsNotNull => {
            let Some(a) = stack.pop() else { return false };
            stack.push(Value::Bool(!matches!(a, Value::Null)));
        }
    }
    true
}

/// Apply a comparison opcode. `None` signals a trap (type mismatch
/// or NaN); `Some(Value::Null)` carries 3VL null propagation; the
/// normal result is `Some(Value::Bool(_))`.
fn compare<'a>(a: &Value<'a>, b: &Value<'a>, op: &Op) -> Option<Value<'a>> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Some(Value::Null);
    }
    use std::cmp::Ordering;
    let ord: Ordering = match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Int(x), Value::Float(y)) => (*x as f64).partial_cmp(y)?,
        (Value::Float(x), Value::Int(y)) => x.partial_cmp(&(*y as f64))?,
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y)?,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Str(x), Value::Str(y)) => x.cmp(y),
        (Value::Ts(x), Value::Ts(y)) => x.cmp(y),
        (Value::Ts(x), Value::Int(y)) => x.cmp(y),
        (Value::Int(x), Value::Ts(y)) => x.cmp(y),
        _ => return None,
    };
    let result = match op {
        Op::Eq => ord == Ordering::Equal,
        Op::Neq => ord != Ordering::Equal,
        Op::Gt => ord == Ordering::Greater,
        Op::Lt => ord == Ordering::Less,
        Op::Gte => ord != Ordering::Less,
        Op::Lte => ord != Ordering::Greater,
        _ => return None,
    };
    Some(Value::Bool(result))
}

/// Extract the value at `(col, row)` as a [`Value`], dispatching on
/// the Arrow `DataType`. Returns `Some(Value::Null)` for real nulls
/// and `None` for types the interpreter doesn't support — the
/// latter traps the current row to `false`.
fn read_column(batch: &RecordBatch, col: usize, row: usize) -> Option<Value<'_>> {
    if col >= batch.num_columns() {
        return None;
    }
    let array = batch.column(col);
    if array.is_null(row) {
        return Some(Value::Null);
    }
    match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| Value::Int(a.value(row))),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| Value::Int(a.value(row) as i64)),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| Value::Float(a.value(row))),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| Value::Float(a.value(row) as f64)),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::Bool(a.value(row))),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| Value::Str(a.value(row).as_bytes())),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| Value::Str(a.value(row).as_bytes())),
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|a| Value::Ts(a.value(row))),
        _ => None,
    }
}

// ============================================================
//  BYTECODE READER HELPERS
// ============================================================

fn read_slice<'a>(buf: &'a [u8], i: &mut usize, n: usize) -> std::io::Result<&'a [u8]> {
    if i.checked_add(n).map(|end| end > buf.len()).unwrap_or(true) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "WPBC: truncated instruction operand",
        ));
    }
    let out = &buf[*i..*i + n];
    *i += n;
    Ok(out)
}

fn read_u16(buf: &[u8], i: &mut usize) -> std::io::Result<u16> {
    Ok(u16::from_le_bytes(read_slice(buf, i, 2)?.try_into().unwrap()))
}

fn read_u32(buf: &[u8], i: &mut usize) -> std::io::Result<u32> {
    Ok(u32::from_le_bytes(read_slice(buf, i, 4)?.try_into().unwrap()))
}

fn read_i64(buf: &[u8], i: &mut usize) -> std::io::Result<i64> {
    Ok(i64::from_le_bytes(read_slice(buf, i, 8)?.try_into().unwrap()))
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// Assemble a bytecode program from a high-level list of op-and-
    /// immediate tuples. Keeps the test cases readable.
    fn assemble(parts: &[Instr]) -> Vec<u8> {
        let mut out = Vec::new();
        for p in parts {
            match p {
                Instr::PushCol(c) => {
                    out.push(OP_PUSH_COL);
                    out.extend_from_slice(&c.to_le_bytes());
                }
                Instr::PushInt(v) => {
                    out.push(OP_PUSH_INT);
                    out.extend_from_slice(&v.to_le_bytes());
                }
                Instr::PushStr(s) => {
                    out.push(OP_PUSH_STR);
                    out.extend_from_slice(&(s.len() as u16).to_le_bytes());
                    out.extend_from_slice(s.as_bytes());
                }
                Instr::Raw(b) => out.push(*b),
                Instr::InList(n) => {
                    out.push(OP_IN_LIST);
                    out.extend_from_slice(&n.to_le_bytes());
                }
                Instr::Project(indices) => {
                    let indices: &[u32] = *indices;
                    out.push(OP_PROJECT);
                    out.extend_from_slice(&(indices.len() as u16).to_le_bytes());
                    for idx in indices {
                        out.extend_from_slice(&idx.to_le_bytes());
                    }
                }
            }
        }
        out
    }

    enum Instr<'a> {
        PushCol(u32),
        PushInt(i64),
        PushStr(&'a str),
        Raw(u8),
        InList(u16),
        Project(&'a [u32]),
    }

    fn users_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true),
            Field::new("status", DataType::Utf8, true),
        ]));
        let names = StringArray::from(vec!["alice", "bob", "carol", "dave", "eve"]);
        let ages = Int64Array::from(vec![Some(30), Some(17), Some(22), None, Some(18)]);
        let statuses = StringArray::from(vec![
            Some("active"),
            Some("active"),
            Some("pending"),
            Some("active"),
            Some("inactive"),
        ]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(ages), Arc::new(statuses)],
        )
        .unwrap()
    }

    #[test]
    fn basic_where_age_ge_18_and_status_eq_active() {
        // SELECT * FROM users WHERE age >= 18 AND status = 'active'
        let bytecode = assemble(&[
            Instr::PushCol(1),
            Instr::PushInt(18),
            Instr::Raw(OP_GTE),
            Instr::PushCol(2),
            Instr::PushStr("active"),
            Instr::Raw(OP_EQ),
            Instr::Raw(OP_AND),
        ]);
        let batch = users_batch();
        let out = execute(&bytecode, &batch).unwrap();
        // Only alice(30, active) satisfies both clauses.
        // bob(17) fails age; carol(pending), dave(null age),
        // eve(inactive) fail the status check.
        assert_eq!(out.num_rows(), 1);
        let names = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
    }

    #[test]
    fn spec_example_age_and_status() {
        // age >= 18 AND status IN ('active', 'pending')  PROJECT name, age
        let bytecode = assemble(&[
            Instr::PushCol(1),
            Instr::PushInt(18),
            Instr::Raw(OP_GTE),
            Instr::PushCol(2),
            Instr::PushStr("active"),
            Instr::PushStr("pending"),
            Instr::InList(2),
            Instr::Raw(OP_AND),
            Instr::Project(&[0, 1]),
        ]);
        let batch = users_batch();
        let out = execute(&bytecode, &batch).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(0).name(), "name");
        assert_eq!(out.schema().field(1).name(), "age");
        let names = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let kept: Vec<_> = (0..names.len()).map(|i| names.value(i)).collect();
        // alice(30,active), carol(22,pending), eve(18,inactive→excluded),
        // bob(17→excluded), dave(null→excluded).
        assert_eq!(kept, vec!["alice", "carol"]);
    }

    #[test]
    fn null_age_row_is_discarded() {
        // age >= 0 → dave's NULL age traps to false.
        let bytecode = assemble(&[
            Instr::PushCol(1),
            Instr::PushInt(0),
            Instr::Raw(OP_GTE),
        ]);
        let batch = users_batch();
        let out = execute(&bytecode, &batch).unwrap();
        assert_eq!(out.num_rows(), 4); // dave dropped.
    }

    #[test]
    fn is_null_surfaces_real_nulls() {
        let bytecode = assemble(&[Instr::PushCol(1), Instr::Raw(OP_IS_NULL)]);
        let out = execute(&bytecode, &users_batch()).unwrap();
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn type_mismatch_traps_row_to_false() {
        // Compare age (Int64) to a string literal — trap → row dropped.
        let bytecode = assemble(&[
            Instr::PushCol(1),
            Instr::PushStr("oops"),
            Instr::Raw(OP_GT),
        ]);
        let out = execute(&bytecode, &users_batch()).unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn empty_program_passes_every_row_untouched() {
        let batch = users_batch();
        let out = execute(&[], &batch).unwrap();
        assert_eq!(out.num_rows(), batch.num_rows());
        assert_eq!(out.num_columns(), batch.num_columns());
    }

    #[test]
    fn project_only_keeps_all_rows() {
        let bytecode = assemble(&[Instr::Project(&[0])]);
        let out = execute(&bytecode, &users_batch()).unwrap();
        assert_eq!(out.num_rows(), 5);
        assert_eq!(out.num_columns(), 1);
    }

    #[test]
    fn oversize_bytecode_rejected() {
        let oversized = vec![OP_PUSH_BOOL; MAX_BYTECODE_BYTES + 1];
        assert!(execute(&oversized, &users_batch()).is_err());
    }

    #[test]
    fn unknown_opcode_is_a_compile_error() {
        let bad = vec![0x7F];
        assert!(execute(&bad, &users_batch()).is_err());
    }

    #[test]
    fn stack_depth_limit_traps() {
        // Push 65 ints with no operators → row should trap on push
        // beyond STACK_DEPTH_LIMIT.
        let mut parts: Vec<Instr> = Vec::new();
        for _ in 0..=STACK_DEPTH_LIMIT {
            parts.push(Instr::PushInt(1));
        }
        let bytecode = assemble(&parts);
        let out = execute(&bytecode, &users_batch()).unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn not_on_null_stays_null_and_drops_row() {
        // NOT(age IS NULL) on row 3 (dave) → NOT(true) = false → dropped.
        // On other rows → NOT(false) = true → kept.
        let bytecode = assemble(&[
            Instr::PushCol(1),
            Instr::Raw(OP_IS_NULL),
            Instr::Raw(OP_NOT),
        ]);
        let out = execute(&bytecode, &users_batch()).unwrap();
        assert_eq!(out.num_rows(), 4);
    }
}
