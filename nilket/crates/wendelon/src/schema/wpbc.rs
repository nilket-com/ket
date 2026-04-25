//! Wendelon Pushdown Bytecode (WPBC) interpreter.
//!
//! The stack machine defined in `plans/schema/compute_ast.md`.
//! Toron lowers pushdown-capable typed expression fragments into
//! WPBC and attaches the bytes to a `ReadRequestMeta` frame's
//! trailing section. A Wendelon Data Node runs the program locally
//! against the Arrow `RecordBatch` it was about to return, yielding
//! a filtered + projected batch so only the requested rows and
//! columns cross the network.
//!
//! ## Safety model
//!
//! The production vectorized path is bounded by the validated bytecode
//! length, stack depth, opcode families, query deadlines, and upstream
//! materialization limits:
//!
//! * programs longer than [`MAX_BYTECODE_BYTES`] are rejected at
//!   compile time,
//! * the evaluation stack is capped at [`STACK_DEPTH_LIMIT`] slots,
//! * unsupported runtime type combinations fail closed by producing
//!   a false/null mask entry or an empty projected batch.
//!
//! Null operands propagate per 3VL and ultimately drop the row;
//! `IS_NULL` / `IS_NOT_NULL` are the only ways to produce a concrete
//! boolean from a Null.
//!
//! ## Compile-then-run
//!
//! The raw byte stream is parsed once into a [`Program`] (a `Vec`
//! of decoded [`Op`]s plus an optional projection list); vector
//! evaluation then walks that compiled form once per batch.

use crate::metrics::{WpbcRuntime, global as metrics};
use crate::schema::{Identity, Locality};
use arrow::array::{
	Array, ArrayRef, BooleanArray, BooleanBuilder, Datum, Float32Array, Float64Array, Int32Array,
	Int64Array, LargeStringArray, StringArray, TimestampMillisecondArray,
};
use arrow::compute;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::io::{Error, ErrorKind};
use std::time::{Duration, Instant};

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

pub const OP_LOAD_IDENTITY: u8 = 0x40;
pub const OP_PUSH_IDENTITY: u8 = 0x41;
pub const OP_EQ_IDENTITY: u8 = 0x42;
pub const OP_HASH_RANGE_IDENTITY: u8 = 0x43;

pub const OP_ROUTE_BY_LOCALITY: u8 = 0x50;
pub const OP_PUSH_LOCALITY: u8 = 0x51;
pub const OP_RANGE_LOCALITY: u8 = 0x52;

pub const OP_PROJECT: u8 = 0xFF;

// ============================================================
//  SAFETY BOUNDS — spec §4.3.
// ============================================================

pub const MAX_BYTECODE_BYTES: usize = 4096;
pub const STACK_DEPTH_LIMIT: usize = 64;
#[cfg(test)]
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
	LoadIdentity,
	PushIdentity(Identity),
	EqIdentity,
	HashRangeIdentity,
	RouteByLocality,
	PushLocality(Locality),
	RangeLocality,
}

#[derive(Clone, Debug, Default)]
pub struct Program {
	ops: Vec<Op>,
	/// Column indices requested by a terminating `PROJECT`. `None`
	/// means the program did not emit a PROJECT and every column
	/// should be returned.
	project: Option<Vec<usize>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnPredicateV1 {
	pub column_index: usize,
	pub comparison: ColumnPredicateComparisonV1,
	pub literal: ColumnPredicateLiteralV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnPredicateComparisonV1 {
	Eq,
	Gt,
	Gte,
	Lt,
	Lte,
	InList,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnPredicateLiteralV1 {
	Int(i64),
	Str(Vec<u8>),
	Float(f64),
	Ts(i64),
	List(Vec<ColumnPredicateLiteralV1>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionContext {
	pub identity: Identity,
	pub locality: Locality,
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

	pub fn requires_scalar_context(&self) -> bool {
		self.ops
			.iter()
			.any(|op| matches!(op, Op::LoadIdentity | Op::RouteByLocality))
	}

	pub fn conjunctive_column_predicates(&self) -> Option<Vec<ColumnPredicateV1>> {
		extract_conjunctive_column_predicates(&self.ops)
	}
}

#[derive(Clone, Debug)]
enum PredicateExpr {
	Column(usize),
	Literal(ColumnPredicateLiteralV1),
	Predicates(Vec<ColumnPredicateV1>),
	True,
	Unsupported,
}

fn extract_conjunctive_column_predicates(ops: &[Op]) -> Option<Vec<ColumnPredicateV1>> {
	if ops.is_empty() {
		return Some(Vec::new());
	}
	let mut stack = Vec::with_capacity(STACK_DEPTH_LIMIT);
	for op in ops {
		match op {
			Op::PushCol(column) => stack.push(PredicateExpr::Column(*column as usize)),
			Op::PushInt(value) => stack.push(PredicateExpr::Literal(
				ColumnPredicateLiteralV1::Int(*value),
			)),
			Op::PushStr(bytes) => stack.push(PredicateExpr::Literal(
				ColumnPredicateLiteralV1::Str(bytes.clone()),
			)),
			Op::PushBool(value) => {
				if *value {
					stack.push(PredicateExpr::True);
				} else {
					stack.push(PredicateExpr::Unsupported);
				}
			}
			Op::PushFloat(value) => stack.push(PredicateExpr::Literal(
				ColumnPredicateLiteralV1::Float(*value),
			)),
			Op::PushTs(value) => {
				stack.push(PredicateExpr::Literal(ColumnPredicateLiteralV1::Ts(*value)))
			}
			Op::Eq | Op::Gt | Op::Lt | Op::Gte | Op::Lte => {
				let rhs = stack.pop()?;
				let lhs = stack.pop()?;
				stack.push(predicate_from_comparison(lhs, rhs, op)?);
			}
			Op::And => {
				let rhs = stack.pop()?;
				let lhs = stack.pop()?;
				stack.push(predicate_and(lhs, rhs)?);
			}
			Op::InList(count) => {
				let count = *count as usize;
				if stack.len() < count + 1 {
					return None;
				}
				let split = stack.len() - count;
				let items = stack.drain(split..).collect::<Vec<_>>();
				let target = stack.pop()?;
				stack.push(predicate_from_in_list(target, items)?);
			}
			Op::Neq
			| Op::Or
			| Op::Not
			| Op::IsNull
			| Op::IsNotNull
			| Op::LoadIdentity
			| Op::PushIdentity(_)
			| Op::EqIdentity
			| Op::HashRangeIdentity
			| Op::RouteByLocality
			| Op::PushLocality(_)
			| Op::RangeLocality => stack.push(PredicateExpr::Unsupported),
		}
		if stack.len() > STACK_DEPTH_LIMIT {
			return None;
		}
	}
	match stack.as_slice() {
		[PredicateExpr::Predicates(predicates)] => Some(predicates.clone()),
		[PredicateExpr::True] => Some(Vec::new()),
		_ => None,
	}
}

fn predicate_from_in_list(
	target: PredicateExpr,
	items: Vec<PredicateExpr>,
) -> Option<PredicateExpr> {
	let PredicateExpr::Column(column_index) = target else {
		return None;
	};
	let mut literals = Vec::with_capacity(items.len());
	for item in items {
		match item {
			PredicateExpr::Literal(literal) => literals.push(literal),
			_ => return None,
		}
	}
	Some(PredicateExpr::Predicates(vec![ColumnPredicateV1 {
		column_index,
		comparison: ColumnPredicateComparisonV1::InList,
		literal: ColumnPredicateLiteralV1::List(literals),
	}]))
}

fn predicate_from_comparison(
	lhs: PredicateExpr,
	rhs: PredicateExpr,
	op: &Op,
) -> Option<PredicateExpr> {
	match (lhs, rhs) {
		(PredicateExpr::Column(column_index), PredicateExpr::Literal(literal)) => {
			Some(PredicateExpr::Predicates(vec![ColumnPredicateV1 {
				column_index,
				comparison: predicate_comparison(op)?,
				literal,
			}]))
		}
		(PredicateExpr::Literal(literal), PredicateExpr::Column(column_index)) => {
			Some(PredicateExpr::Predicates(vec![ColumnPredicateV1 {
				column_index,
				comparison: predicate_comparison_reversed(op)?,
				literal,
			}]))
		}
		_ => None,
	}
}

fn predicate_and(lhs: PredicateExpr, rhs: PredicateExpr) -> Option<PredicateExpr> {
	match (lhs, rhs) {
		(PredicateExpr::Predicates(mut left), PredicateExpr::Predicates(right)) => {
			left.extend(right);
			Some(PredicateExpr::Predicates(left))
		}
		(PredicateExpr::Predicates(predicates), PredicateExpr::True)
		| (PredicateExpr::True, PredicateExpr::Predicates(predicates)) => {
			Some(PredicateExpr::Predicates(predicates))
		}
		(PredicateExpr::True, PredicateExpr::True) => Some(PredicateExpr::True),
		_ => None,
	}
}

fn predicate_comparison(op: &Op) -> Option<ColumnPredicateComparisonV1> {
	match op {
		Op::Eq => Some(ColumnPredicateComparisonV1::Eq),
		Op::Gt => Some(ColumnPredicateComparisonV1::Gt),
		Op::Gte => Some(ColumnPredicateComparisonV1::Gte),
		Op::Lt => Some(ColumnPredicateComparisonV1::Lt),
		Op::Lte => Some(ColumnPredicateComparisonV1::Lte),
		_ => None,
	}
}

fn predicate_comparison_reversed(op: &Op) -> Option<ColumnPredicateComparisonV1> {
	match op {
		Op::Eq => Some(ColumnPredicateComparisonV1::Eq),
		Op::Gt => Some(ColumnPredicateComparisonV1::Lt),
		Op::Gte => Some(ColumnPredicateComparisonV1::Lte),
		Op::Lt => Some(ColumnPredicateComparisonV1::Gt),
		Op::Lte => Some(ColumnPredicateComparisonV1::Gte),
		_ => None,
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
				let v = f64::from_le_bytes(read_slice(bytecode, &mut i, 8)?.try_into().unwrap());
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
			OP_LOAD_IDENTITY => ops.push(Op::LoadIdentity),
			OP_PUSH_IDENTITY => {
				let identity = read_identity(bytecode, &mut i)?;
				ops.push(Op::PushIdentity(identity));
			}
			OP_EQ_IDENTITY => ops.push(Op::EqIdentity),
			OP_HASH_RANGE_IDENTITY => ops.push(Op::HashRangeIdentity),
			OP_ROUTE_BY_LOCALITY => ops.push(Op::RouteByLocality),
			OP_PUSH_LOCALITY => {
				let locality = read_u64(bytecode, &mut i)?;
				ops.push(Op::PushLocality(Locality::new(locality)));
			}
			OP_RANGE_LOCALITY => ops.push(Op::RangeLocality),
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
	let program = Program { ops, project };
	validate_program(&program)?;
	Ok(program)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StackKind {
	Unknown,
	Bool,
	Identity,
	Locality,
}

fn validate_program(program: &Program) -> std::io::Result<()> {
	let mut stack = Vec::with_capacity(STACK_DEPTH_LIMIT);
	for op in &program.ops {
		match op {
			Op::PushCol(_) | Op::PushInt(_) | Op::PushStr(_) | Op::PushFloat(_) | Op::PushTs(_) => {
				stack.push(StackKind::Unknown);
			}
			Op::PushBool(_) => stack.push(StackKind::Bool),
			Op::Eq | Op::Neq | Op::Gt | Op::Lt | Op::Gte | Op::Lte => {
				let rhs = pop_kind(&mut stack, "generic comparison rhs")?;
				let lhs = pop_kind(&mut stack, "generic comparison lhs")?;
				reject_typed_family(lhs, "generic comparison")?;
				reject_typed_family(rhs, "generic comparison")?;
				stack.push(StackKind::Bool);
			}
			Op::And | Op::Or => {
				let rhs = pop_kind(&mut stack, "logical rhs")?;
				let lhs = pop_kind(&mut stack, "logical lhs")?;
				expect_boolish(lhs, "logical operation")?;
				expect_boolish(rhs, "logical operation")?;
				stack.push(StackKind::Bool);
			}
			Op::Not => {
				let value = pop_kind(&mut stack, "logical operand")?;
				expect_boolish(value, "logical operation")?;
				stack.push(StackKind::Bool);
			}
			Op::InList(n) => {
				let mut items = Vec::with_capacity(*n as usize);
				for _ in 0..*n as usize {
					items.push(pop_kind(&mut stack, "IN_LIST item")?);
				}
				let target = pop_kind(&mut stack, "IN_LIST target")?;
				reject_typed_family(target, "IN_LIST")?;
				for item in items {
					reject_typed_family(item, "IN_LIST")?;
				}
				stack.push(StackKind::Bool);
			}
			Op::IsNull | Op::IsNotNull => {
				let value = pop_kind(&mut stack, "null-check operand")?;
				reject_typed_family(value, "null-check")?;
				stack.push(StackKind::Bool);
			}
			Op::LoadIdentity | Op::PushIdentity(_) => stack.push(StackKind::Identity),
			Op::EqIdentity => {
				expect_identity(pop_kind(&mut stack, "identity rhs")?, "EQ_IDENTITY")?;
				expect_identity(pop_kind(&mut stack, "identity lhs")?, "EQ_IDENTITY")?;
				stack.push(StackKind::Bool);
			}
			Op::HashRangeIdentity => {
				expect_identity(
					pop_kind(&mut stack, "identity upper bound")?,
					"HASH_RANGE_IDENTITY",
				)?;
				expect_identity(
					pop_kind(&mut stack, "identity lower bound")?,
					"HASH_RANGE_IDENTITY",
				)?;
				expect_identity(
					pop_kind(&mut stack, "identity target")?,
					"HASH_RANGE_IDENTITY",
				)?;
				stack.push(StackKind::Bool);
			}
			Op::RouteByLocality | Op::PushLocality(_) => stack.push(StackKind::Locality),
			Op::RangeLocality => {
				expect_locality(
					pop_kind(&mut stack, "locality upper bound")?,
					"RANGE_LOCALITY",
				)?;
				expect_locality(
					pop_kind(&mut stack, "locality lower bound")?,
					"RANGE_LOCALITY",
				)?;
				expect_locality(pop_kind(&mut stack, "locality target")?, "RANGE_LOCALITY")?;
				stack.push(StackKind::Bool);
			}
		}
		if stack.len() > STACK_DEPTH_LIMIT {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"WPBC: stack depth limit exceeded at program load",
			));
		}
	}
	Ok(())
}

fn pop_kind(stack: &mut Vec<StackKind>, label: &str) -> std::io::Result<StackKind> {
	stack.pop().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			format!("WPBC: stack underflow while validating {label}"),
		)
	})
}

fn expect_boolish(kind: StackKind, op: &str) -> std::io::Result<()> {
	match kind {
		StackKind::Bool | StackKind::Unknown => Ok(()),
		StackKind::Identity | StackKind::Locality => Err(Error::new(
			ErrorKind::InvalidData,
			format!("WPBC: {op} cannot consume typed identity/locality values"),
		)),
	}
}

fn expect_identity(kind: StackKind, op: &str) -> std::io::Result<()> {
	match kind {
		StackKind::Identity => Ok(()),
		StackKind::Locality => Err(Error::new(
			ErrorKind::InvalidData,
			format!("WPBC: {op} expected Identity, got Locality"),
		)),
		_ => Err(Error::new(
			ErrorKind::InvalidData,
			format!("WPBC: {op} expected Identity operands"),
		)),
	}
}

fn expect_locality(kind: StackKind, op: &str) -> std::io::Result<()> {
	match kind {
		StackKind::Locality => Ok(()),
		StackKind::Identity => Err(Error::new(
			ErrorKind::InvalidData,
			format!("WPBC: {op} expected Locality, got Identity"),
		)),
		_ => Err(Error::new(
			ErrorKind::InvalidData,
			format!("WPBC: {op} expected Locality operands"),
		)),
	}
}

fn reject_typed_family(kind: StackKind, op: &str) -> std::io::Result<()> {
	match kind {
		StackKind::Identity | StackKind::Locality => Err(Error::new(
			ErrorKind::InvalidData,
			format!("WPBC: {op} must use typed identity/locality opcodes"),
		)),
		_ => Ok(()),
	}
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
	execute_program_with_context(program, batch, None)
}

pub fn execute_with_context(
	bytecode: &[u8],
	batch: &RecordBatch,
	context: ExecutionContext,
) -> std::io::Result<RecordBatch> {
	let program = Program::compile(bytecode)?;
	execute_program_with_context(&program, batch, Some(context))
}

pub fn execute_program_with_context(
	program: &Program,
	batch: &RecordBatch,
	context: Option<ExecutionContext>,
) -> std::io::Result<RecordBatch> {
	validate_program_against_batch(program, batch)?;
	execute_program_vectorized(program, batch, context)
}

fn execute_program_vectorized(
	program: &Program,
	batch: &RecordBatch,
	context: Option<ExecutionContext>,
) -> std::io::Result<RecordBatch> {
	let started = Instant::now();
	let mut stats = VectorExecStats::default();
	let result = match evaluate_program_vectorized(program, batch, context, &mut stats) {
		Ok(None) => apply_projection(program, batch),
		Ok(Some(mask)) => {
			let filtered = compute::filter_record_batch(batch, &mask).map_err(|e| {
				stats.kernel_errors += 1;
				Error::new(ErrorKind::InvalidData, format!("WPBC filter: {e}"))
			})?;
			apply_projection(program, &filtered)
		}
		Err(err) => {
			stats.kernel_errors += 1;
			tracing::debug!(error = %err, "WPBC vector runtime trap normalized to empty batch");
			empty_filtered_batch(program, batch)
		}
	};
	if let Ok(out) = &result {
		observe_wpbc_execution(
			WpbcRuntime::Vectorized,
			batch.num_rows(),
			out.num_rows(),
			started.elapsed(),
			stats,
		);
	}
	result
}

fn apply_projection(program: &Program, batch: &RecordBatch) -> std::io::Result<RecordBatch> {
	match &program.project {
		Some(indices) => batch
			.project(indices)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("WPBC project: {e}"))),
		None => Ok(batch.clone()),
	}
}

fn empty_filtered_batch(program: &Program, batch: &RecordBatch) -> std::io::Result<RecordBatch> {
	let empty = batch.slice(0, 0);
	apply_projection(program, &empty)
}

fn observe_wpbc_execution(
	runtime: WpbcRuntime,
	rows_scanned: usize,
	rows_returned: usize,
	duration: Duration,
	stats: VectorExecStats,
) {
	metrics().observe_wpbc_execution(
		runtime,
		rows_scanned as u64,
		rows_returned as u64,
		duration,
		stats.kernel_errors,
		stats.intermediate_arrays,
	);
}

fn validate_program_against_batch(program: &Program, batch: &RecordBatch) -> std::io::Result<()> {
	let schema = batch.schema();
	for op in &program.ops {
		let Op::PushCol(col) = op else {
			continue;
		};
		let field = schema.fields().get(*col as usize).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!("WPBC: column index {} is out of bounds", col),
			)
		})?;
		if is_supported_pushcol_type(field.data_type()) {
			continue;
		}
		let msg = match field.data_type() {
			DataType::FixedSizeBinary(32) | DataType::UInt64 => {
				"typed identity/locality values must use LOAD_IDENTITY / ROUTE_BY_LOCALITY"
			}
			other => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("WPBC: PUSH_COL does not support Arrow type {other:?}"),
				));
			}
		};
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("WPBC: column '{}' {msg}", field.name()),
		));
	}
	if let Some(indices) = &program.project {
		for index in indices {
			if *index >= batch.num_columns() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("WPBC: PROJECT column index {} is out of bounds", index),
				));
			}
		}
	}
	Ok(())
}

fn is_supported_pushcol_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Int64
			| DataType::Int32
			| DataType::Float64
			| DataType::Float32
			| DataType::Boolean
			| DataType::Utf8
			| DataType::LargeUtf8
			| DataType::Timestamp(TimeUnit::Millisecond, _)
	)
}

// ============================================================
//  VECTORIZED INTERPRETER
// ============================================================

#[derive(Clone, Debug)]
enum VectorValue {
	Array(ArrayRef),
	Scalar(ScalarLit),
	Bool(BooleanArray),
	Identity(Identity),
	Locality(Locality),
}

#[derive(Clone, Debug)]
enum ScalarLit {
	Int(i64),
	Float(f64),
	Str(Vec<u8>),
	Bool(bool),
	Ts(i64),
}

#[derive(Clone, Copy, Debug, Default)]
struct VectorExecStats {
	kernel_errors: u64,
	intermediate_arrays: u64,
}

struct ColumnCache<'a> {
	batch: &'a RecordBatch,
	columns: Vec<Option<ArrayRef>>,
}

impl<'a> ColumnCache<'a> {
	fn new(batch: &'a RecordBatch) -> Self {
		Self {
			batch,
			columns: vec![None; batch.num_columns()],
		}
	}

	fn column(&mut self, index: usize) -> Result<ArrayRef, String> {
		if index >= self.batch.num_columns() {
			return Err(format!("column index {index} is out of bounds"));
		}
		if self.columns[index].is_none() {
			self.columns[index] = Some(self.batch.column(index).clone());
		}
		Ok(self.columns[index]
			.as_ref()
			.expect("column cache populated")
			.clone())
	}
}

fn evaluate_program_vectorized(
	program: &Program,
	batch: &RecordBatch,
	context: Option<ExecutionContext>,
	stats: &mut VectorExecStats,
) -> Result<Option<BooleanArray>, String> {
	if program.ops.is_empty() {
		return Ok(None);
	}

	let len = batch.num_rows();
	let mut cache = ColumnCache::new(batch);
	let mut stack: Vec<VectorValue> = Vec::with_capacity(STACK_DEPTH_LIMIT);
	for op in &program.ops {
		match op {
			Op::PushCol(col) => stack.push(VectorValue::Array(cache.column(*col as usize)?)),
			Op::PushInt(value) => stack.push(VectorValue::Scalar(ScalarLit::Int(*value))),
			Op::PushStr(bytes) => stack.push(VectorValue::Scalar(ScalarLit::Str(bytes.clone()))),
			Op::PushBool(value) => stack.push(VectorValue::Scalar(ScalarLit::Bool(*value))),
			Op::PushFloat(value) => stack.push(VectorValue::Scalar(ScalarLit::Float(*value))),
			Op::PushTs(value) => stack.push(VectorValue::Scalar(ScalarLit::Ts(*value))),
			Op::LoadIdentity => {
				let ctx = context.ok_or("LOAD_IDENTITY requires execution context")?;
				stack.push(VectorValue::Identity(ctx.identity));
			}
			Op::PushIdentity(value) => stack.push(VectorValue::Identity(*value)),
			Op::EqIdentity => {
				let rhs = pop_vector(&mut stack, "identity rhs")?;
				let lhs = pop_vector(&mut stack, "identity lhs")?;
				let mask = identity_eq_mask(lhs, rhs, len)?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
			Op::HashRangeIdentity => {
				let upper = pop_vector(&mut stack, "identity upper bound")?;
				let lower = pop_vector(&mut stack, "identity lower bound")?;
				let target = pop_vector(&mut stack, "identity target")?;
				let mask = identity_range_mask(target, lower, upper, len)?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
			Op::RouteByLocality => {
				let ctx = context.ok_or("ROUTE_BY_LOCALITY requires execution context")?;
				stack.push(VectorValue::Locality(ctx.locality));
			}
			Op::PushLocality(value) => stack.push(VectorValue::Locality(*value)),
			Op::RangeLocality => {
				let upper = pop_vector(&mut stack, "locality upper bound")?;
				let lower = pop_vector(&mut stack, "locality lower bound")?;
				let target = pop_vector(&mut stack, "locality target")?;
				let mask = locality_range_mask(target, lower, upper, len)?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
			Op::Eq | Op::Neq | Op::Gt | Op::Lt | Op::Gte | Op::Lte => {
				let rhs = pop_vector(&mut stack, "comparison rhs")?;
				let lhs = pop_vector(&mut stack, "comparison lhs")?;
				let mask = compare_vector_values(&lhs, &rhs, op, len)?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
			Op::And => {
				let rhs = pop_vector(&mut stack, "logical rhs")?;
				let lhs = pop_vector(&mut stack, "logical lhs")?;
				let lhs = bool_array_from_value(&lhs, len)?;
				let rhs = bool_array_from_value(&rhs, len)?;
				let mask = compute::and_kleene(&lhs, &rhs)
					.map_err(|e| format!("AND boolean kernel: {e}"))?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
			Op::Or => {
				let rhs = pop_vector(&mut stack, "logical rhs")?;
				let lhs = pop_vector(&mut stack, "logical lhs")?;
				let lhs = bool_array_from_value(&lhs, len)?;
				let rhs = bool_array_from_value(&rhs, len)?;
				let mask = compute::or_kleene(&lhs, &rhs)
					.map_err(|e| format!("OR boolean kernel: {e}"))?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
			Op::Not => {
				let value = pop_vector(&mut stack, "logical operand")?;
				let value = bool_array_from_value(&value, len)?;
				let mask = compute::not(&value).map_err(|e| format!("NOT boolean kernel: {e}"))?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
			Op::InList(count) => {
				let count = *count as usize;
				if stack.len() < count + 1 {
					return Err("IN_LIST stack underflow".to_owned());
				}
				let split = stack.len() - count;
				let items = stack.drain(split..).collect::<Vec<_>>();
				let target = pop_vector(&mut stack, "IN_LIST target")?;
				let mask = in_list_vector(&target, &items, len)?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
			Op::IsNull => {
				let value = pop_vector(&mut stack, "null-check operand")?;
				let mask = is_null_vector(&value, len)?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
			Op::IsNotNull => {
				let value = pop_vector(&mut stack, "null-check operand")?;
				let mask = is_not_null_vector(&value, len)?;
				stats.intermediate_arrays += 1;
				stack.push(VectorValue::Bool(mask));
			}
		}
		if stack.len() > STACK_DEPTH_LIMIT {
			return Err("stack depth limit exceeded during vector execution".to_owned());
		}
	}

	let top = stack
		.last()
		.ok_or_else(|| "program ended with an empty stack".to_owned())?;
	Ok(Some(finish_mask(top, len)?))
}

fn pop_vector(stack: &mut Vec<VectorValue>, label: &str) -> Result<VectorValue, String> {
	stack
		.pop()
		.ok_or_else(|| format!("stack underflow while executing {label}"))
}

fn finish_mask(value: &VectorValue, len: usize) -> Result<BooleanArray, String> {
	bool_array_from_value(value, len)
}

fn bool_array_from_value(value: &VectorValue, len: usize) -> Result<BooleanArray, String> {
	match value {
		VectorValue::Bool(mask) => Ok(mask.clone()),
		VectorValue::Array(array) if matches!(array.data_type(), DataType::Boolean) => Ok(array
			.as_any()
			.downcast_ref::<BooleanArray>()
			.ok_or_else(|| "Boolean array downcast failed".to_owned())?
			.clone()),
		VectorValue::Scalar(ScalarLit::Bool(value)) => Ok(const_bool_array(*value, len)),
		other => Err(format!(
			"expected Boolean vector value, got {}",
			vector_value_type(other)
		)),
	}
}

fn const_bool_array(value: bool, len: usize) -> BooleanArray {
	BooleanArray::from(vec![value; len])
}

fn identity_eq_mask(
	lhs: VectorValue,
	rhs: VectorValue,
	len: usize,
) -> Result<BooleanArray, String> {
	let VectorValue::Identity(lhs) = lhs else {
		return Err("EQ_IDENTITY lhs was not Identity".to_owned());
	};
	let VectorValue::Identity(rhs) = rhs else {
		return Err("EQ_IDENTITY rhs was not Identity".to_owned());
	};
	Ok(const_bool_array(lhs == rhs, len))
}

fn identity_range_mask(
	target: VectorValue,
	lower: VectorValue,
	upper: VectorValue,
	len: usize,
) -> Result<BooleanArray, String> {
	let VectorValue::Identity(target) = target else {
		return Err("HASH_RANGE_IDENTITY target was not Identity".to_owned());
	};
	let VectorValue::Identity(lower) = lower else {
		return Err("HASH_RANGE_IDENTITY lower bound was not Identity".to_owned());
	};
	let VectorValue::Identity(upper) = upper else {
		return Err("HASH_RANGE_IDENTITY upper bound was not Identity".to_owned());
	};
	Ok(const_bool_array(lower <= target && target <= upper, len))
}

fn locality_range_mask(
	target: VectorValue,
	lower: VectorValue,
	upper: VectorValue,
	len: usize,
) -> Result<BooleanArray, String> {
	let VectorValue::Locality(target) = target else {
		return Err("RANGE_LOCALITY target was not Locality".to_owned());
	};
	let VectorValue::Locality(lower) = lower else {
		return Err("RANGE_LOCALITY lower bound was not Locality".to_owned());
	};
	let VectorValue::Locality(upper) = upper else {
		return Err("RANGE_LOCALITY upper bound was not Locality".to_owned());
	};
	Ok(const_bool_array(lower <= target && target <= upper, len))
}

fn is_null_vector(value: &VectorValue, len: usize) -> Result<BooleanArray, String> {
	match value {
		VectorValue::Array(array) => {
			compute::is_null(array.as_ref()).map_err(|e| format!("IS_NULL null kernel: {e}"))
		}
		VectorValue::Bool(mask) => {
			compute::is_null(mask).map_err(|e| format!("IS_NULL boolean null kernel: {e}"))
		}
		VectorValue::Scalar(_) => Ok(const_bool_array(false, len)),
		VectorValue::Identity(_) | VectorValue::Locality(_) => {
			Err("IS_NULL cannot consume typed identity/locality values".to_owned())
		}
	}
}

fn is_not_null_vector(value: &VectorValue, len: usize) -> Result<BooleanArray, String> {
	match value {
		VectorValue::Array(array) => compute::is_not_null(array.as_ref())
			.map_err(|e| format!("IS_NOT_NULL null kernel: {e}")),
		VectorValue::Bool(mask) => {
			compute::is_not_null(mask).map_err(|e| format!("IS_NOT_NULL boolean null kernel: {e}"))
		}
		VectorValue::Scalar(_) => Ok(const_bool_array(true, len)),
		VectorValue::Identity(_) | VectorValue::Locality(_) => {
			Err("IS_NOT_NULL cannot consume typed identity/locality values".to_owned())
		}
	}
}

#[derive(Clone, Copy, Debug)]
enum Cell<'a> {
	Null,
	Int(i64),
	Float(f64),
	Bool(bool),
	Str(&'a [u8]),
	Ts(i64),
}

#[derive(Clone, Copy, Debug)]
enum CompareResult {
	Null,
	Bool(bool),
	Trap,
}

fn compare_vector_values(
	lhs: &VectorValue,
	rhs: &VectorValue,
	op: &Op,
	len: usize,
) -> Result<BooleanArray, String> {
	if let Some(mask) = compare_vector_values_fast(lhs, rhs, op)? {
		return Ok(mask);
	}

	let mut mask = BooleanBuilder::with_capacity(len);
	for row in 0..len {
		let lhs = cell_at(lhs, row).ok_or_else(|| {
			format!(
				"comparison lhs unsupported vector value {}",
				vector_value_type(lhs)
			)
		})?;
		let rhs = cell_at(rhs, row).ok_or_else(|| {
			format!(
				"comparison rhs unsupported vector value {}",
				vector_value_type(rhs)
			)
		})?;
		match compare_cells(lhs, rhs, op) {
			CompareResult::Bool(value) => mask.append_value(value),
			CompareResult::Null => mask.append_null(),
			CompareResult::Trap => mask.append_value(false),
		}
	}
	Ok(mask.finish())
}

fn compare_vector_values_fast(
	lhs: &VectorValue,
	rhs: &VectorValue,
	op: &Op,
) -> Result<Option<BooleanArray>, String> {
	match (lhs, rhs) {
		(VectorValue::Array(array), VectorValue::Scalar(scalar)) => {
			compare_array_scalar_fast(array, scalar, op)
		}
		(VectorValue::Scalar(scalar), VectorValue::Array(array)) => {
			compare_scalar_array_fast(scalar, array, op)
		}
		(VectorValue::Array(lhs), VectorValue::Array(rhs)) => {
			compare_array_array_fast(lhs, rhs, op)
		}
		(VectorValue::Array(array), VectorValue::Bool(mask)) => {
			compare_array_bool_fast(array, mask, op)
		}
		(VectorValue::Bool(mask), VectorValue::Array(array)) => {
			compare_bool_array_fast(mask, array, op)
		}
		(VectorValue::Bool(lhs), VectorValue::Bool(rhs)) => {
			compare_arrow_datums(lhs, rhs, op).map(Some)
		}
		(VectorValue::Bool(mask), VectorValue::Scalar(ScalarLit::Bool(value))) => {
			let scalar = BooleanArray::new_scalar(*value);
			compare_arrow_datums(mask, &scalar, op).map(Some)
		}
		(VectorValue::Scalar(ScalarLit::Bool(value)), VectorValue::Bool(mask)) => {
			let scalar = BooleanArray::new_scalar(*value);
			compare_arrow_datums(&scalar, mask, op).map(Some)
		}
		_ => Ok(None),
	}
}

fn compare_array_scalar_fast(
	array: &ArrayRef,
	scalar: &ScalarLit,
	op: &Op,
) -> Result<Option<BooleanArray>, String> {
	match (array.data_type(), scalar) {
		(DataType::Int64, ScalarLit::Int(value)) | (DataType::Int64, ScalarLit::Ts(value)) => {
			let lhs = array
				.as_any()
				.downcast_ref::<Int64Array>()
				.ok_or_else(|| "Int64 array downcast failed".to_owned())?;
			let rhs = Int64Array::new_scalar(*value);
			compare_arrow_datums(lhs, &rhs, op).map(Some)
		}
		(DataType::Int32, ScalarLit::Int(value)) | (DataType::Int32, ScalarLit::Ts(value))
			if i32::try_from(*value).is_ok() =>
		{
			let lhs = array
				.as_any()
				.downcast_ref::<Int32Array>()
				.ok_or_else(|| "Int32 array downcast failed".to_owned())?;
			let rhs = Int32Array::new_scalar(*value as i32);
			compare_arrow_datums(lhs, &rhs, op).map(Some)
		}
		(DataType::Boolean, ScalarLit::Bool(value)) => {
			let lhs = array
				.as_any()
				.downcast_ref::<BooleanArray>()
				.ok_or_else(|| "Boolean array downcast failed".to_owned())?;
			let rhs = BooleanArray::new_scalar(*value);
			compare_arrow_datums(lhs, &rhs, op).map(Some)
		}
		(DataType::Utf8, ScalarLit::Str(bytes)) => {
			let Ok(value) = std::str::from_utf8(bytes) else {
				return Ok(None);
			};
			let lhs = array
				.as_any()
				.downcast_ref::<StringArray>()
				.ok_or_else(|| "Utf8 array downcast failed".to_owned())?;
			let rhs = StringArray::new_scalar(value);
			compare_arrow_datums(lhs, &rhs, op).map(Some)
		}
		(DataType::LargeUtf8, ScalarLit::Str(bytes)) => {
			let Ok(value) = std::str::from_utf8(bytes) else {
				return Ok(None);
			};
			let lhs = array
				.as_any()
				.downcast_ref::<LargeStringArray>()
				.ok_or_else(|| "LargeUtf8 array downcast failed".to_owned())?;
			let rhs = LargeStringArray::new_scalar(value);
			compare_arrow_datums(lhs, &rhs, op).map(Some)
		}
		(DataType::Timestamp(TimeUnit::Millisecond, _), ScalarLit::Ts(value))
		| (DataType::Timestamp(TimeUnit::Millisecond, _), ScalarLit::Int(value)) => {
			let lhs = array
				.as_any()
				.downcast_ref::<TimestampMillisecondArray>()
				.ok_or_else(|| "TimestampMillisecond array downcast failed".to_owned())?;
			let rhs = TimestampMillisecondArray::new_scalar(*value);
			compare_arrow_datums(lhs, &rhs, op).map(Some)
		}
		_ => Ok(None),
	}
}

fn compare_scalar_array_fast(
	scalar: &ScalarLit,
	array: &ArrayRef,
	op: &Op,
) -> Result<Option<BooleanArray>, String> {
	match (scalar, array.data_type()) {
		(ScalarLit::Int(value), DataType::Int64) | (ScalarLit::Ts(value), DataType::Int64) => {
			let lhs = Int64Array::new_scalar(*value);
			let rhs = array
				.as_any()
				.downcast_ref::<Int64Array>()
				.ok_or_else(|| "Int64 array downcast failed".to_owned())?;
			compare_arrow_datums(&lhs, rhs, op).map(Some)
		}
		(ScalarLit::Int(value), DataType::Int32) | (ScalarLit::Ts(value), DataType::Int32)
			if i32::try_from(*value).is_ok() =>
		{
			let lhs = Int32Array::new_scalar(*value as i32);
			let rhs = array
				.as_any()
				.downcast_ref::<Int32Array>()
				.ok_or_else(|| "Int32 array downcast failed".to_owned())?;
			compare_arrow_datums(&lhs, rhs, op).map(Some)
		}
		(ScalarLit::Bool(value), DataType::Boolean) => {
			let lhs = BooleanArray::new_scalar(*value);
			let rhs = array
				.as_any()
				.downcast_ref::<BooleanArray>()
				.ok_or_else(|| "Boolean array downcast failed".to_owned())?;
			compare_arrow_datums(&lhs, rhs, op).map(Some)
		}
		(ScalarLit::Str(bytes), DataType::Utf8) => {
			let Ok(value) = std::str::from_utf8(bytes) else {
				return Ok(None);
			};
			let lhs = StringArray::new_scalar(value);
			let rhs = array
				.as_any()
				.downcast_ref::<StringArray>()
				.ok_or_else(|| "Utf8 array downcast failed".to_owned())?;
			compare_arrow_datums(&lhs, rhs, op).map(Some)
		}
		(ScalarLit::Str(bytes), DataType::LargeUtf8) => {
			let Ok(value) = std::str::from_utf8(bytes) else {
				return Ok(None);
			};
			let lhs = LargeStringArray::new_scalar(value);
			let rhs = array
				.as_any()
				.downcast_ref::<LargeStringArray>()
				.ok_or_else(|| "LargeUtf8 array downcast failed".to_owned())?;
			compare_arrow_datums(&lhs, rhs, op).map(Some)
		}
		(ScalarLit::Ts(value), DataType::Timestamp(TimeUnit::Millisecond, _))
		| (ScalarLit::Int(value), DataType::Timestamp(TimeUnit::Millisecond, _)) => {
			let lhs = TimestampMillisecondArray::new_scalar(*value);
			let rhs = array
				.as_any()
				.downcast_ref::<TimestampMillisecondArray>()
				.ok_or_else(|| "TimestampMillisecond array downcast failed".to_owned())?;
			compare_arrow_datums(&lhs, rhs, op).map(Some)
		}
		_ => Ok(None),
	}
}

fn compare_array_array_fast(
	lhs: &ArrayRef,
	rhs: &ArrayRef,
	op: &Op,
) -> Result<Option<BooleanArray>, String> {
	if matches!(
		(lhs.data_type(), rhs.data_type()),
		(
			DataType::Timestamp(TimeUnit::Millisecond, _),
			DataType::Timestamp(TimeUnit::Millisecond, _)
		)
	) && lhs.data_type() != rhs.data_type()
	{
		return Ok(None);
	}

	match (lhs.data_type(), rhs.data_type()) {
		(DataType::Int64, DataType::Int64) => compare_arrow_arrays::<Int64Array>(lhs, rhs, op),
		(DataType::Int32, DataType::Int32) => compare_arrow_arrays::<Int32Array>(lhs, rhs, op),
		(DataType::Boolean, DataType::Boolean) => {
			compare_arrow_arrays::<BooleanArray>(lhs, rhs, op)
		}
		(DataType::Utf8, DataType::Utf8) => compare_arrow_arrays::<StringArray>(lhs, rhs, op),
		(DataType::LargeUtf8, DataType::LargeUtf8) => {
			compare_arrow_arrays::<LargeStringArray>(lhs, rhs, op)
		}
		(
			DataType::Timestamp(TimeUnit::Millisecond, _),
			DataType::Timestamp(TimeUnit::Millisecond, _),
		) => compare_arrow_arrays::<TimestampMillisecondArray>(lhs, rhs, op),
		_ => Ok(None),
	}
}

fn compare_arrow_arrays<T: Array + 'static>(
	lhs: &ArrayRef,
	rhs: &ArrayRef,
	op: &Op,
) -> Result<Option<BooleanArray>, String> {
	let lhs = lhs
		.as_any()
		.downcast_ref::<T>()
		.ok_or_else(|| "lhs array downcast failed".to_owned())?;
	let rhs = rhs
		.as_any()
		.downcast_ref::<T>()
		.ok_or_else(|| "rhs array downcast failed".to_owned())?;
	compare_arrow_datums(lhs, rhs, op).map(Some)
}

fn compare_array_bool_fast(
	array: &ArrayRef,
	mask: &BooleanArray,
	op: &Op,
) -> Result<Option<BooleanArray>, String> {
	if !matches!(array.data_type(), DataType::Boolean) {
		return Ok(None);
	}
	let array = array
		.as_any()
		.downcast_ref::<BooleanArray>()
		.ok_or_else(|| "Boolean array downcast failed".to_owned())?;
	compare_arrow_datums(array, mask, op).map(Some)
}

fn compare_bool_array_fast(
	mask: &BooleanArray,
	array: &ArrayRef,
	op: &Op,
) -> Result<Option<BooleanArray>, String> {
	if !matches!(array.data_type(), DataType::Boolean) {
		return Ok(None);
	}
	let array = array
		.as_any()
		.downcast_ref::<BooleanArray>()
		.ok_or_else(|| "Boolean array downcast failed".to_owned())?;
	compare_arrow_datums(mask, array, op).map(Some)
}

fn compare_arrow_datums(lhs: &dyn Datum, rhs: &dyn Datum, op: &Op) -> Result<BooleanArray, String> {
	match op {
		Op::Eq => compute::kernels::cmp::eq(lhs, rhs),
		Op::Neq => compute::kernels::cmp::neq(lhs, rhs),
		Op::Gt => compute::kernels::cmp::gt(lhs, rhs),
		Op::Lt => compute::kernels::cmp::lt(lhs, rhs),
		Op::Gte => compute::kernels::cmp::gt_eq(lhs, rhs),
		Op::Lte => compute::kernels::cmp::lt_eq(lhs, rhs),
		_ => return Err("unsupported comparison opcode".to_owned()),
	}
	.map_err(|e| format!("Arrow comparison kernel: {e}"))
}

fn in_list_vector(
	target: &VectorValue,
	items: &[VectorValue],
	len: usize,
) -> Result<BooleanArray, String> {
	if let Some(mask) = in_list_vector_fast(target, items)? {
		return Ok(mask);
	}

	let mut mask = BooleanBuilder::with_capacity(len);
	for row in 0..len {
		let target_cell = cell_at(target, row)
			.ok_or_else(|| format!("IN_LIST target unsupported {}", vector_value_type(target)))?;
		if matches!(target_cell, Cell::Null) {
			mask.append_null();
			continue;
		}

		let mut found = false;
		for item in items {
			let Some(item_cell) = cell_at(item, row) else {
				continue;
			};
			if matches!(
				compare_cells(target_cell, item_cell, &Op::Eq),
				CompareResult::Bool(true)
			) {
				found = true;
				break;
			}
		}
		mask.append_value(found);
	}
	Ok(mask.finish())
}

fn in_list_vector_fast(
	target: &VectorValue,
	items: &[VectorValue],
) -> Result<Option<BooleanArray>, String> {
	if items.is_empty() {
		return Ok(None);
	}

	let mut combined: Option<BooleanArray> = None;
	for item in items {
		let Some(eq) = compare_vector_values_fast(target, item, &Op::Eq)? else {
			return Ok(None);
		};
		combined = Some(match combined {
			Some(current) => compute::or_kleene(&current, &eq)
				.map_err(|e| format!("IN_LIST OR boolean kernel: {e}"))?,
			None => eq,
		});
	}
	Ok(combined)
}

fn cell_at(value: &VectorValue, row: usize) -> Option<Cell<'_>> {
	match value {
		VectorValue::Array(array) => array_cell_at(array.as_ref(), row),
		VectorValue::Bool(mask) => bool_cell_at(mask, row),
		VectorValue::Scalar(scalar) => Some(scalar_cell(scalar)),
		VectorValue::Identity(_) | VectorValue::Locality(_) => None,
	}
}

fn scalar_cell(scalar: &ScalarLit) -> Cell<'_> {
	match scalar {
		ScalarLit::Int(value) => Cell::Int(*value),
		ScalarLit::Float(value) => Cell::Float(*value),
		ScalarLit::Str(bytes) => Cell::Str(bytes.as_slice()),
		ScalarLit::Bool(value) => Cell::Bool(*value),
		ScalarLit::Ts(value) => Cell::Ts(*value),
	}
}

fn bool_cell_at(array: &BooleanArray, row: usize) -> Option<Cell<'_>> {
	if array.is_null(row) {
		Some(Cell::Null)
	} else {
		Some(Cell::Bool(array.value(row)))
	}
}

fn array_cell_at(array: &dyn Array, row: usize) -> Option<Cell<'_>> {
	if array.is_null(row) {
		return Some(Cell::Null);
	}
	match array.data_type() {
		DataType::Int64 => array
			.as_any()
			.downcast_ref::<Int64Array>()
			.map(|a| Cell::Int(a.value(row))),
		DataType::Int32 => array
			.as_any()
			.downcast_ref::<Int32Array>()
			.map(|a| Cell::Int(a.value(row) as i64)),
		DataType::Float64 => array
			.as_any()
			.downcast_ref::<Float64Array>()
			.map(|a| Cell::Float(a.value(row))),
		DataType::Float32 => array
			.as_any()
			.downcast_ref::<Float32Array>()
			.map(|a| Cell::Float(a.value(row) as f64)),
		DataType::Boolean => array
			.as_any()
			.downcast_ref::<BooleanArray>()
			.map(|a| Cell::Bool(a.value(row))),
		DataType::Utf8 => array
			.as_any()
			.downcast_ref::<StringArray>()
			.map(|a| Cell::Str(a.value(row).as_bytes())),
		DataType::LargeUtf8 => array
			.as_any()
			.downcast_ref::<LargeStringArray>()
			.map(|a| Cell::Str(a.value(row).as_bytes())),
		DataType::Timestamp(TimeUnit::Millisecond, _) => array
			.as_any()
			.downcast_ref::<TimestampMillisecondArray>()
			.map(|a| Cell::Ts(a.value(row))),
		_ => None,
	}
}

fn compare_cells(lhs: Cell<'_>, rhs: Cell<'_>, op: &Op) -> CompareResult {
	if matches!(lhs, Cell::Null) || matches!(rhs, Cell::Null) {
		return CompareResult::Null;
	}

	use std::cmp::Ordering;
	let Some(ord) = (match (lhs, rhs) {
		(Cell::Int(x), Cell::Int(y)) => Some(x.cmp(&y)),
		(Cell::Int(x), Cell::Float(y)) => (x as f64).partial_cmp(&y),
		(Cell::Float(x), Cell::Int(y)) => x.partial_cmp(&(y as f64)),
		(Cell::Float(x), Cell::Float(y)) => x.partial_cmp(&y),
		(Cell::Bool(x), Cell::Bool(y)) => Some(x.cmp(&y)),
		(Cell::Str(x), Cell::Str(y)) => Some(x.cmp(y)),
		(Cell::Ts(x), Cell::Ts(y)) => Some(x.cmp(&y)),
		(Cell::Ts(x), Cell::Int(y)) => Some(x.cmp(&y)),
		(Cell::Int(x), Cell::Ts(y)) => Some(x.cmp(&y)),
		_ => None,
	}) else {
		return CompareResult::Trap;
	};

	let result = match op {
		Op::Eq => ord == Ordering::Equal,
		Op::Neq => ord != Ordering::Equal,
		Op::Gt => ord == Ordering::Greater,
		Op::Lt => ord == Ordering::Less,
		Op::Gte => ord != Ordering::Less,
		Op::Lte => ord != Ordering::Greater,
		_ => return CompareResult::Trap,
	};
	CompareResult::Bool(result)
}

fn vector_value_type(value: &VectorValue) -> String {
	match value {
		VectorValue::Array(array) => format!("array({:?})", array.data_type()),
		VectorValue::Scalar(ScalarLit::Int(_)) => "int-scalar".to_owned(),
		VectorValue::Scalar(ScalarLit::Float(_)) => "float-scalar".to_owned(),
		VectorValue::Scalar(ScalarLit::Str(_)) => "string-scalar".to_owned(),
		VectorValue::Scalar(ScalarLit::Bool(_)) => "bool-scalar".to_owned(),
		VectorValue::Scalar(ScalarLit::Ts(_)) => "timestamp-scalar".to_owned(),
		VectorValue::Bool(mask) => format!("bool-array(nulls={})", mask.null_count()),
		VectorValue::Identity(_) => "identity".to_owned(),
		VectorValue::Locality(_) => "locality".to_owned(),
	}
}

// ============================================================
//  PER-ROW INTERPRETER
// ============================================================

#[cfg(test)]
fn execute_program_scalar_reference(
	program: &Program,
	batch: &RecordBatch,
	context: Option<ExecutionContext>,
) -> std::io::Result<RecordBatch> {
	validate_program_against_batch(program, batch)?;
	let started = Instant::now();
	let filtered = if program.ops.is_empty() {
		batch.clone()
	} else {
		let n_rows = batch.num_rows();
		let mut mask = BooleanBuilder::with_capacity(n_rows);
		for row in 0..n_rows {
			mask.append_value(eval_row(&program.ops, batch, row, context));
		}
		let filter: BooleanArray = mask.finish();
		compute::filter_record_batch(batch, &filter)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("WPBC filter: {e}")))?
	};
	let out = apply_projection(program, &filtered)?;
	observe_wpbc_execution(
		WpbcRuntime::ScalarReference,
		batch.num_rows(),
		out.num_rows(),
		started.elapsed(),
		VectorExecStats::default(),
	);
	Ok(out)
}

#[cfg(test)]
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
	Identity(Identity),
	Locality(Locality),
}

#[cfg(test)]
fn eval_row(
	ops: &[Op],
	batch: &RecordBatch,
	row: usize,
	context: Option<ExecutionContext>,
) -> bool {
	let mut stack: Vec<Value<'_>> = Vec::with_capacity(STACK_DEPTH_LIMIT);
	let mut fuel = FUEL_PER_ROW;
	for op in ops {
		if fuel == 0 {
			return false;
		}
		fuel -= 1;
		if !exec_op(op, &mut stack, batch, row, context) {
			return false;
		}
		if stack.len() > STACK_DEPTH_LIMIT {
			return false;
		}
	}
	matches!(stack.last(), Some(Value::Bool(true)))
}

#[cfg(test)]
fn exec_op<'a>(
	op: &'a Op,
	stack: &mut Vec<Value<'a>>,
	batch: &'a RecordBatch,
	row: usize,
	context: Option<ExecutionContext>,
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
		Op::LoadIdentity => match context {
			Some(ctx) => stack.push(Value::Identity(ctx.identity)),
			None => return false,
		},
		Op::PushIdentity(v) => stack.push(Value::Identity(*v)),
		Op::EqIdentity => {
			let Some(Value::Identity(rhs)) = stack.pop() else {
				return false;
			};
			let Some(Value::Identity(lhs)) = stack.pop() else {
				return false;
			};
			stack.push(Value::Bool(lhs == rhs));
		}
		Op::HashRangeIdentity => {
			let Some(Value::Identity(upper)) = stack.pop() else {
				return false;
			};
			let Some(Value::Identity(lower)) = stack.pop() else {
				return false;
			};
			let Some(Value::Identity(target)) = stack.pop() else {
				return false;
			};
			stack.push(Value::Bool(lower <= target && target <= upper));
		}
		Op::RouteByLocality => match context {
			Some(ctx) => stack.push(Value::Locality(ctx.locality)),
			None => return false,
		},
		Op::PushLocality(v) => stack.push(Value::Locality(*v)),
		Op::RangeLocality => {
			let Some(Value::Locality(upper)) = stack.pop() else {
				return false;
			};
			let Some(Value::Locality(lower)) = stack.pop() else {
				return false;
			};
			let Some(Value::Locality(target)) = stack.pop() else {
				return false;
			};
			stack.push(Value::Bool(lower <= target && target <= upper));
		}
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
			let Some(target) = stack.pop() else {
				return false;
			};
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
#[cfg(test)]
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
#[cfg(test)]
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
	Ok(u16::from_le_bytes(
		read_slice(buf, i, 2)?.try_into().unwrap(),
	))
}

fn read_u32(buf: &[u8], i: &mut usize) -> std::io::Result<u32> {
	Ok(u32::from_le_bytes(
		read_slice(buf, i, 4)?.try_into().unwrap(),
	))
}

fn read_u64(buf: &[u8], i: &mut usize) -> std::io::Result<u64> {
	Ok(u64::from_le_bytes(
		read_slice(buf, i, 8)?.try_into().unwrap(),
	))
}

fn read_i64(buf: &[u8], i: &mut usize) -> std::io::Result<i64> {
	Ok(i64::from_le_bytes(
		read_slice(buf, i, 8)?.try_into().unwrap(),
	))
}

fn read_identity(buf: &[u8], i: &mut usize) -> std::io::Result<Identity> {
	let bytes: [u8; 32] = read_slice(buf, i, 32)?.try_into().unwrap();
	Ok(Identity::new(bytes))
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::array::{
		FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float32Array, Float64Array, Int32Array,
		Int64Array, LargeStringArray, StringArray, TimestampMillisecondArray, UInt64Array,
	};
	use arrow::datatypes::{Field, Schema};
	use rand::{Rng, SeedableRng};
	use std::fmt::Write as _;
	use std::sync::Arc;

	const SMOKE_FUZZ_SAMPLE_COUNT: u64 = 10_000;
	const WEEKLY_FUZZ_SAMPLE_COUNT: u64 = 1_000_000;

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
				Instr::PushBool(value) => {
					out.push(OP_PUSH_BOOL);
					out.push(u8::from(*value));
				}
				Instr::PushFloat(value) => {
					out.push(OP_PUSH_FLOAT);
					out.extend_from_slice(&value.to_le_bytes());
				}
				Instr::PushTs(value) => {
					out.push(OP_PUSH_TS);
					out.extend_from_slice(&value.to_le_bytes());
				}
				Instr::PushIdentity(bytes) => {
					out.push(OP_PUSH_IDENTITY);
					out.extend_from_slice(bytes);
				}
				Instr::PushLocality(locality) => {
					out.push(OP_PUSH_LOCALITY);
					out.extend_from_slice(&locality.to_le_bytes());
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
		PushBool(bool),
		PushFloat(f64),
		PushTs(i64),
		PushIdentity([u8; 32]),
		PushLocality(u64),
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

	fn identity_locality_batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![
			Field::new("identity", DataType::FixedSizeBinary(32), false),
			Field::new("locality", DataType::UInt64, false),
			Field::new("label", DataType::Utf8, true),
		]));
		let mut identities = FixedSizeBinaryBuilder::with_capacity(4, 32);
		for fill in [0x11_u8, 0x22, 0x33, 0x44] {
			identities.append_value(&[fill; 32]).unwrap();
		}
		let localities = UInt64Array::from(vec![1_u64, 2, 3, 4]);
		let labels = StringArray::from(vec![Some("a"), None, Some("b"), Some("c")]);
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(identities.finish()),
				Arc::new(localities),
				Arc::new(labels),
			],
		)
		.unwrap()
	}

	fn analytics_batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![
			Field::new("small", DataType::Int32, true),
			Field::new("score", DataType::Float64, true),
			Field::new("flag", DataType::Boolean, true),
			Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
			Field::new("tag", DataType::LargeUtf8, true),
			Field::new("score32", DataType::Float32, true),
		]));
		let small = Int32Array::from(vec![Some(1_i32), Some(7), None, Some(-3), Some(i32::MAX)]);
		let score = Float64Array::from(vec![
			Some(1.5_f64),
			Some(f64::NAN),
			None,
			Some(-0.0),
			Some(10.0),
		]);
		let flag = BooleanArray::from(vec![Some(true), Some(false), None, Some(true), Some(false)]);
		let ts = TimestampMillisecondArray::from(vec![
			Some(1_000_i64),
			Some(2_000),
			None,
			Some(-100),
			Some(10_000),
		]);
		let tag = LargeStringArray::from(vec![
			Some("alpha"),
			Some("beta"),
			None,
			Some("gamma"),
			Some("delta"),
		]);
		let score32 = Float32Array::from(vec![
			Some(1.25_f32),
			Some(f32::NAN),
			None,
			Some(-0.0),
			Some(9.5),
		]);
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(small),
				Arc::new(score),
				Arc::new(flag),
				Arc::new(ts),
				Arc::new(tag),
				Arc::new(score32),
			],
		)
		.unwrap()
	}

	fn one_row_batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, false)]));
		let ages = Int64Array::from(vec![42_i64]);
		RecordBatch::try_new(schema, vec![Arc::new(ages)]).unwrap()
	}

	fn identity(fill: u8) -> Identity {
		Identity::new([fill; 32])
	}

	#[derive(Clone, Copy)]
	enum FuzzBatch {
		Users,
		IdentityLocality,
	}

	#[derive(Clone, Copy)]
	enum StructuredReference {
		PassThrough,
		UsersProjectName,
		UsersAgeGte18,
		UsersStatusActive,
		UsersAgeAndStatusProject,
		PassAllRows,
		DropAllRows,
	}

	struct StructuredFuzzCase {
		batch: FuzzBatch,
		bytecode: Vec<u8>,
		context: Option<ExecutionContext>,
		reference: Option<StructuredReference>,
	}

	fn batch_for_fuzz_fixture(fixture: FuzzBatch) -> RecordBatch {
		match fixture {
			FuzzBatch::Users => users_batch(),
			FuzzBatch::IdentityLocality => identity_locality_batch(),
		}
	}

	fn structured_fuzz_cases() -> Vec<StructuredFuzzCase> {
		vec![
			StructuredFuzzCase {
				batch: FuzzBatch::Users,
				bytecode: Vec::new(),
				context: None,
				reference: Some(StructuredReference::PassThrough),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::Users,
				bytecode: assemble(&[Instr::Project(&[0])]),
				context: None,
				reference: Some(StructuredReference::UsersProjectName),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::Users,
				bytecode: assemble(&[Instr::PushCol(1), Instr::PushInt(18), Instr::Raw(OP_GTE)]),
				context: None,
				reference: Some(StructuredReference::UsersAgeGte18),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::Users,
				bytecode: assemble(&[
					Instr::PushCol(2),
					Instr::PushStr("active"),
					Instr::Raw(OP_EQ),
				]),
				context: None,
				reference: Some(StructuredReference::UsersStatusActive),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::Users,
				bytecode: assemble(&[
					Instr::PushCol(1),
					Instr::PushInt(18),
					Instr::Raw(OP_GTE),
					Instr::PushCol(2),
					Instr::PushStr("active"),
					Instr::PushStr("pending"),
					Instr::InList(2),
					Instr::Raw(OP_AND),
					Instr::Project(&[0, 1]),
				]),
				context: None,
				reference: Some(StructuredReference::UsersAgeAndStatusProject),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::IdentityLocality,
				bytecode: assemble(&[
					Instr::Raw(OP_LOAD_IDENTITY),
					Instr::PushIdentity(identity(0x22).into_bytes()),
					Instr::Raw(OP_EQ_IDENTITY),
				]),
				context: Some(ExecutionContext {
					identity: identity(0x22),
					locality: Locality::new(9),
				}),
				reference: Some(StructuredReference::PassAllRows),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::IdentityLocality,
				bytecode: assemble(&[
					Instr::Raw(OP_LOAD_IDENTITY),
					Instr::PushIdentity(identity(0x22).into_bytes()),
					Instr::Raw(OP_EQ_IDENTITY),
				]),
				context: Some(ExecutionContext {
					identity: identity(0x77),
					locality: Locality::new(9),
				}),
				reference: Some(StructuredReference::DropAllRows),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::IdentityLocality,
				bytecode: assemble(&[
					Instr::Raw(OP_ROUTE_BY_LOCALITY),
					Instr::PushLocality(1),
					Instr::PushLocality(3),
					Instr::Raw(OP_RANGE_LOCALITY),
				]),
				context: Some(ExecutionContext {
					identity: identity(0x55),
					locality: Locality::new(2),
				}),
				reference: Some(StructuredReference::PassAllRows),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::IdentityLocality,
				bytecode: assemble(&[
					Instr::Raw(OP_ROUTE_BY_LOCALITY),
					Instr::PushLocality(1),
					Instr::PushLocality(3),
					Instr::Raw(OP_RANGE_LOCALITY),
				]),
				context: Some(ExecutionContext {
					identity: identity(0x55),
					locality: Locality::new(9),
				}),
				reference: Some(StructuredReference::DropAllRows),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::IdentityLocality,
				bytecode: assemble(&[
					Instr::Raw(OP_LOAD_IDENTITY),
					Instr::PushIdentity(identity(0x11).into_bytes()),
					Instr::PushIdentity(identity(0x44).into_bytes()),
					Instr::Raw(OP_HASH_RANGE_IDENTITY),
				]),
				context: Some(ExecutionContext {
					identity: identity(0x33),
					locality: Locality::new(1),
				}),
				reference: Some(StructuredReference::PassAllRows),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::IdentityLocality,
				bytecode: assemble(&[
					Instr::Raw(OP_LOAD_IDENTITY),
					Instr::PushIdentity(identity(0x11).into_bytes()),
					Instr::PushIdentity(identity(0x44).into_bytes()),
					Instr::Raw(OP_HASH_RANGE_IDENTITY),
				]),
				context: Some(ExecutionContext {
					identity: identity(0x77),
					locality: Locality::new(1),
				}),
				reference: Some(StructuredReference::DropAllRows),
			},
			StructuredFuzzCase {
				batch: FuzzBatch::IdentityLocality,
				bytecode: assemble(&[
					Instr::Raw(OP_LOAD_IDENTITY),
					Instr::PushLocality(1),
					Instr::Raw(OP_EQ_IDENTITY),
				]),
				context: None,
				reference: None,
			},
			StructuredFuzzCase {
				batch: FuzzBatch::IdentityLocality,
				bytecode: assemble(&[
					Instr::Raw(OP_ROUTE_BY_LOCALITY),
					Instr::PushIdentity(identity(0x55).into_bytes()),
					Instr::Raw(OP_RANGE_LOCALITY),
				]),
				context: None,
				reference: None,
			},
			StructuredFuzzCase {
				batch: FuzzBatch::IdentityLocality,
				bytecode: assemble(&[
					Instr::Raw(OP_LOAD_IDENTITY),
					Instr::PushLocality(1),
					Instr::PushLocality(2),
					Instr::Raw(OP_RANGE_LOCALITY),
				]),
				context: None,
				reference: None,
			},
		]
	}

	fn filter_batch(batch: &RecordBatch, mask: &[bool]) -> RecordBatch {
		let filter = BooleanArray::from(mask.to_vec());
		compute::filter_record_batch(batch, &filter).unwrap()
	}

	fn reference_output(batch: &RecordBatch, reference: StructuredReference) -> RecordBatch {
		match reference {
			StructuredReference::PassThrough => batch.clone(),
			StructuredReference::UsersProjectName => batch.project(&[0]).unwrap(),
			StructuredReference::UsersAgeGte18 => {
				filter_batch(batch, &[true, false, true, false, true])
			}
			StructuredReference::UsersStatusActive => {
				filter_batch(batch, &[true, true, false, true, false])
			}
			StructuredReference::UsersAgeAndStatusProject => {
				filter_batch(batch, &[true, false, true, false, false])
					.project(&[0, 1])
					.unwrap()
			}
			StructuredReference::PassAllRows => batch.clone(),
			StructuredReference::DropAllRows => batch.slice(0, 0),
		}
	}

	fn bytes_to_hex(bytes: &[u8]) -> String {
		let mut out = String::with_capacity(bytes.len() * 2);
		for byte in bytes {
			let _ = write!(&mut out, "{byte:02x}");
		}
		out
	}

	fn scalar_fingerprint(array: &dyn Array, row: usize) -> Option<String> {
		if array.is_null(row) {
			return None;
		}
		Some(match array.data_type() {
			DataType::Utf8 => array
				.as_any()
				.downcast_ref::<StringArray>()
				.unwrap()
				.value(row)
				.to_owned(),
			DataType::LargeUtf8 => array
				.as_any()
				.downcast_ref::<LargeStringArray>()
				.unwrap()
				.value(row)
				.to_owned(),
			DataType::Int64 => array
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap()
				.value(row)
				.to_string(),
			DataType::Int32 => array
				.as_any()
				.downcast_ref::<Int32Array>()
				.unwrap()
				.value(row)
				.to_string(),
			DataType::Float64 => format!(
				"{:016x}",
				array
					.as_any()
					.downcast_ref::<Float64Array>()
					.unwrap()
					.value(row)
					.to_bits()
			),
			DataType::Float32 => format!(
				"{:08x}",
				array
					.as_any()
					.downcast_ref::<Float32Array>()
					.unwrap()
					.value(row)
					.to_bits()
			),
			DataType::Boolean => array
				.as_any()
				.downcast_ref::<BooleanArray>()
				.unwrap()
				.value(row)
				.to_string(),
			DataType::Timestamp(TimeUnit::Millisecond, _) => array
				.as_any()
				.downcast_ref::<TimestampMillisecondArray>()
				.unwrap()
				.value(row)
				.to_string(),
			DataType::UInt64 => array
				.as_any()
				.downcast_ref::<UInt64Array>()
				.unwrap()
				.value(row)
				.to_string(),
			DataType::FixedSizeBinary(32) => bytes_to_hex(
				array
					.as_any()
					.downcast_ref::<FixedSizeBinaryArray>()
					.unwrap()
					.value(row),
			),
			other => panic!("unsupported test fingerprint type: {other:?}"),
		})
	}

	fn assert_batches_equivalent(expected: &RecordBatch, actual: &RecordBatch) {
		assert_eq!(expected.num_columns(), actual.num_columns());
		assert_eq!(expected.num_rows(), actual.num_rows());
		for (expected_field, actual_field) in expected
			.schema()
			.fields()
			.iter()
			.zip(actual.schema().fields().iter())
		{
			assert_eq!(expected_field.name(), actual_field.name());
			assert_eq!(expected_field.data_type(), actual_field.data_type());
			assert_eq!(expected_field.is_nullable(), actual_field.is_nullable());
		}
		for col in 0..expected.num_columns() {
			for row in 0..expected.num_rows() {
				assert_eq!(
					scalar_fingerprint(expected.column(col).as_ref(), row),
					scalar_fingerprint(actual.column(col).as_ref(), row),
					"mismatch at row {row}, column {}",
					expected.schema().field(col).name(),
				);
			}
		}
	}

	fn assert_scalar_vector_parity(
		bytecode: &[u8],
		batch: &RecordBatch,
		context: Option<ExecutionContext>,
	) {
		let program = Program::compile(bytecode).expect("parity bytecode must compile");
		assert_program_scalar_vector_parity(&program, batch, context);
	}

	fn assert_program_scalar_vector_parity(
		program: &Program,
		batch: &RecordBatch,
		context: Option<ExecutionContext>,
	) {
		let scalar = execute_program_scalar_reference(program, batch, context);
		let vector = execute_program_with_context(program, batch, context);
		assert_eq!(
			scalar.is_ok(),
			vector.is_ok(),
			"scalar/vector accepted behavior diverged: scalar={scalar:?} vector={vector:?}"
		);
		if let (Ok(expected), Ok(actual)) = (scalar, vector) {
			assert_batches_equivalent(&expected, &actual);
		}
	}

	fn mutate_bytecode(
		seed: &[u8],
		rng: &mut rand::rngs::StdRng,
		max_bytecode_bytes: usize,
	) -> Vec<u8> {
		let mut bytecode = seed.to_vec();
		let mutation_ops = 1 + (rng.next_u32() as usize % 3);
		for _ in 0..mutation_ops {
			match rng.next_u32() % 4 {
				0 if !bytecode.is_empty() => {
					let idx = (rng.next_u32() as usize) % bytecode.len();
					bytecode[idx] ^= rng.next_u32() as u8;
				}
				1 if bytecode.len() < max_bytecode_bytes => {
					let idx = if bytecode.is_empty() {
						0
					} else {
						(rng.next_u32() as usize) % (bytecode.len() + 1)
					};
					let extra_len = 1
						+ ((rng.next_u32() as usize)
							% (max_bytecode_bytes.saturating_sub(bytecode.len()).max(1)));
					let mut extra = vec![0_u8; extra_len];
					rng.fill_bytes(&mut extra);
					bytecode.splice(idx..idx, extra);
				}
				2 if !bytecode.is_empty() => {
					let start = (rng.next_u32() as usize) % bytecode.len();
					let len = 1 + ((rng.next_u32() as usize) % (bytecode.len() - start));
					bytecode.drain(start..start + len);
				}
				_ if bytecode.len() < max_bytecode_bytes => {
					bytecode.push(rng.next_u32() as u8);
				}
				_ => {}
			}
		}
		if bytecode.len() > max_bytecode_bytes {
			bytecode.truncate(max_bytecode_bytes);
		}
		bytecode
	}

	fn random_execution_context(rng: &mut rand::rngs::StdRng) -> ExecutionContext {
		let mut bytes = [0_u8; 32];
		rng.fill_bytes(&mut bytes);
		ExecutionContext {
			identity: Identity::new(bytes),
			locality: Locality::new(rng.next_u64()),
		}
	}

	fn assert_wpbc_fuzz_surface(sample_count: u64, max_bytecode_bytes: usize) {
		let structured = structured_fuzz_cases();
		let mutation_seeds = structured
			.iter()
			.filter(|case| case.reference.is_some())
			.map(|case| case.bytecode.as_slice())
			.collect::<Vec<_>>();
		let mut rng = rand::rngs::StdRng::seed_from_u64(0xD1CE_F00D_005EED);

		for sample in 0..sample_count {
			if sample % 8 == 0 {
				let case = &structured[(sample as usize / 8) % structured.len()];
				let batch = batch_for_fuzz_fixture(case.batch);
				match case.reference {
					Some(reference) => {
						let program = Program::compile(&case.bytecode)
							.expect("structured semantic corpus entry must compile");
						let actual =
							execute_program_with_context(&program, &batch, case.context).unwrap();
						let expected = reference_output(&batch, reference);
						assert_batches_equivalent(&expected, &actual);
						assert_program_scalar_vector_parity(&program, &batch, case.context);
					}
					None => {
						assert!(
							Program::compile(&case.bytecode).is_err(),
							"cross-family corpus entry must fail validation"
						);
					}
				}
				continue;
			}

			let batch = batch_for_fuzz_fixture(if rng.next_u32() % 2 == 0 {
				FuzzBatch::Users
			} else {
				FuzzBatch::IdentityLocality
			});
			let context = random_execution_context(&mut rng);
			let bytecode = if sample % 2 == 0 {
				let seed = mutation_seeds[(rng.next_u32() as usize) % mutation_seeds.len()];
				mutate_bytecode(seed, &mut rng, max_bytecode_bytes)
			} else {
				let len = (rng.next_u32() as usize) % (max_bytecode_bytes + 1);
				let mut bytecode = vec![0_u8; len];
				rng.fill_bytes(&mut bytecode);
				bytecode
			};
			if let Ok(program) = Program::compile(&bytecode) {
				assert_program_scalar_vector_parity(&program, &batch, Some(context));
			} else {
				let _ = execute_with_context(&bytecode, &batch, context);
			}
		}
	}

	fn assert_generated_valid_program_fuzz(sample_count: u64) {
		let mut rng = rand::rngs::StdRng::seed_from_u64(0x005E_D1FF_C0DE_2026);
		for _ in 0..sample_count {
			let (batch, bytecode, context) = generated_valid_case(&mut rng);
			assert_scalar_vector_parity(&bytecode, &batch, context);
		}
	}

	fn generated_valid_case(
		rng: &mut rand::rngs::StdRng,
	) -> (RecordBatch, Vec<u8>, Option<ExecutionContext>) {
		match rng.next_u32() % 12 {
			0 => (
				users_batch(),
				assemble(&[
					Instr::PushCol(1),
					Instr::PushInt((rng.next_u32() % 40) as i64),
					Instr::Raw(OP_GTE),
				]),
				None,
			),
			1 => (
				users_batch(),
				assemble(&[
					Instr::PushCol(2),
					Instr::PushStr("active"),
					Instr::PushStr("pending"),
					Instr::PushStr("missing"),
					Instr::InList(3),
				]),
				None,
			),
			2 => (
				users_batch(),
				assemble(&[
					Instr::PushCol(1),
					Instr::PushInt(18),
					Instr::Raw(OP_GTE),
					Instr::PushCol(2),
					Instr::PushStr("active"),
					Instr::Raw(OP_EQ),
					Instr::Raw(OP_OR),
					Instr::Project(&[0, 1]),
				]),
				None,
			),
			3 => (
				analytics_batch(),
				assemble(&[
					Instr::PushCol(0),
					Instr::PushInt(rng.next_u32() as i64 - i32::MAX as i64),
					Instr::Raw(OP_LT),
				]),
				None,
			),
			4 => (
				analytics_batch(),
				assemble(&[
					Instr::PushCol(1),
					Instr::PushFloat((rng.next_u32() % 12) as f64 - 2.0),
					Instr::Raw(OP_GTE),
				]),
				None,
			),
			5 => (
				analytics_batch(),
				assemble(&[
					Instr::PushCol(5),
					Instr::PushFloat((rng.next_u32() % 8) as f64),
					Instr::Raw(OP_LTE),
				]),
				None,
			),
			6 => (
				analytics_batch(),
				assemble(&[
					Instr::PushCol(3),
					Instr::PushTs((rng.next_u32() % 4_000) as i64),
					Instr::Raw(OP_GTE),
				]),
				None,
			),
			7 => (
				analytics_batch(),
				assemble(&[
					Instr::PushCol(4),
					Instr::PushStr(if rng.next_u32() % 2 == 0 {
						"alpha"
					} else {
						"delta"
					}),
					Instr::Raw(OP_EQ),
				]),
				None,
			),
			8 => (
				analytics_batch(),
				assemble(&[
					Instr::PushCol(2),
					Instr::PushBool(true),
					Instr::Raw(OP_EQ),
					Instr::Raw(OP_NOT),
				]),
				None,
			),
			9 => {
				let ctx = ExecutionContext {
					identity: identity(0x22),
					locality: Locality::new(2),
				};
				(
					identity_locality_batch(),
					assemble(&[
						Instr::Raw(OP_LOAD_IDENTITY),
						Instr::PushIdentity(
							identity(if rng.next_u32() % 2 == 0 { 0x22 } else { 0x33 })
								.into_bytes(),
						),
						Instr::Raw(OP_EQ_IDENTITY),
					]),
					Some(ctx),
				)
			}
			10 => (
				identity_locality_batch(),
				assemble(&[
					Instr::Raw(OP_ROUTE_BY_LOCALITY),
					Instr::PushLocality(1),
					Instr::PushLocality(3),
					Instr::Raw(OP_RANGE_LOCALITY),
				]),
				Some(ExecutionContext {
					identity: identity(0x11),
					locality: Locality::new(rng.next_u64() % 5),
				}),
			),
			_ => (
				analytics_batch(),
				assemble(&[
					Instr::PushCol(1),
					Instr::PushFloat(f64::NAN),
					Instr::Raw(OP_EQ),
				]),
				None,
			),
		}
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
		let bytecode = assemble(&[Instr::PushCol(1), Instr::PushInt(0), Instr::Raw(OP_GTE)]);
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
		let bytecode = assemble(&[Instr::PushCol(1), Instr::PushStr("oops"), Instr::Raw(OP_GT)]);
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
	fn stack_depth_limit_is_a_compile_error() {
		// Push 65 ints with no operators → program load should fail
		// before any row is evaluated.
		let mut parts: Vec<Instr> = Vec::new();
		for _ in 0..=STACK_DEPTH_LIMIT {
			parts.push(Instr::PushInt(1));
		}
		let bytecode = assemble(&parts);
		let err = execute(&bytecode, &users_batch()).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("stack depth limit exceeded"));
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

	#[test]
	fn typed_identity_equality_matches_execution_context() {
		let bytecode = assemble(&[
			Instr::Raw(OP_LOAD_IDENTITY),
			Instr::PushIdentity(identity(0x22).into_bytes()),
			Instr::Raw(OP_EQ_IDENTITY),
		]);
		let out = execute_with_context(
			&bytecode,
			&one_row_batch(),
			ExecutionContext {
				identity: identity(0x22),
				locality: Locality::new(9),
			},
		)
		.unwrap();
		assert_eq!(out.num_rows(), 1);
	}

	#[test]
	fn typed_locality_range_matches_execution_context() {
		let bytecode = assemble(&[
			Instr::Raw(OP_ROUTE_BY_LOCALITY),
			Instr::PushLocality(10),
			Instr::PushLocality(20),
			Instr::Raw(OP_RANGE_LOCALITY),
		]);
		let out = execute_with_context(
			&bytecode,
			&one_row_batch(),
			ExecutionContext {
				identity: identity(0x55),
				locality: Locality::new(15),
			},
		)
		.unwrap();
		assert_eq!(out.num_rows(), 1);
	}

	#[test]
	fn typed_identity_range_is_lexicographic_and_inclusive() {
		let target = identity(0x33);
		let bytecode = assemble(&[
			Instr::Raw(OP_LOAD_IDENTITY),
			Instr::PushIdentity(identity(0x11).into_bytes()),
			Instr::PushIdentity(identity(0x44).into_bytes()),
			Instr::Raw(OP_HASH_RANGE_IDENTITY),
		]);
		let out = execute_with_context(
			&bytecode,
			&one_row_batch(),
			ExecutionContext {
				identity: target,
				locality: Locality::from(target),
			},
		)
		.unwrap();
		assert_eq!(out.num_rows(), 1);
	}

	#[test]
	fn mixing_identity_and_locality_families_is_a_compile_error() {
		let invalid = assemble(&[
			Instr::Raw(OP_LOAD_IDENTITY),
			Instr::PushLocality(1),
			Instr::PushLocality(2),
			Instr::Raw(OP_RANGE_LOCALITY),
		]);
		assert!(Program::compile(&invalid).is_err());

		let invalid_generic = assemble(&[
			Instr::Raw(OP_ROUTE_BY_LOCALITY),
			Instr::Raw(OP_LOAD_IDENTITY),
			Instr::Raw(OP_EQ),
		]);
		assert!(Program::compile(&invalid_generic).is_err());
	}

	#[test]
	fn null_checks_reject_typed_identity_and_locality_families() {
		let invalid_identity = assemble(&[Instr::Raw(OP_LOAD_IDENTITY), Instr::Raw(OP_IS_NULL)]);
		assert!(Program::compile(&invalid_identity).is_err());

		let invalid_locality =
			assemble(&[Instr::Raw(OP_ROUTE_BY_LOCALITY), Instr::Raw(OP_IS_NOT_NULL)]);
		assert!(Program::compile(&invalid_locality).is_err());
	}

	#[test]
	fn push_col_rejects_identity_and_locality_typed_columns() {
		let identity_program = assemble(&[Instr::PushCol(0), Instr::Raw(OP_IS_NOT_NULL)]);
		let err = execute(&identity_program, &identity_locality_batch()).unwrap_err();
		assert!(
			err.to_string().contains("LOAD_IDENTITY"),
			"identity columns should direct callers to typed context opcodes"
		);

		let locality_program = assemble(&[Instr::PushCol(1), Instr::Raw(OP_IS_NOT_NULL)]);
		let err = execute(&locality_program, &identity_locality_batch()).unwrap_err();
		assert!(
			err.to_string().contains("ROUTE_BY_LOCALITY"),
			"locality columns should direct callers to typed context opcodes"
		);
	}

	#[test]
	fn cross_family_bad_corpus_entries_are_rejected() {
		let invalid_identity_compare = assemble(&[
			Instr::Raw(OP_LOAD_IDENTITY),
			Instr::PushLocality(1),
			Instr::Raw(OP_EQ_IDENTITY),
		]);
		assert!(Program::compile(&invalid_identity_compare).is_err());

		let invalid_locality_range = assemble(&[
			Instr::Raw(OP_ROUTE_BY_LOCALITY),
			Instr::PushIdentity(identity(0x55).into_bytes()),
			Instr::Raw(OP_RANGE_LOCALITY),
		]);
		assert!(Program::compile(&invalid_locality_range).is_err());
	}

	#[test]
	fn scalar_reference_matches_vectorized_structured_corpus() {
		for case in structured_fuzz_cases() {
			if case.reference.is_none() {
				continue;
			}
			let batch = batch_for_fuzz_fixture(case.batch);
			assert_scalar_vector_parity(&case.bytecode, &batch, case.context);
		}
	}

	#[test]
	fn generated_valid_programs_match_scalar_reference() {
		assert_generated_valid_program_fuzz(512);
	}

	#[test]
	fn float_nan_comparison_drops_nan_row_without_total_ordering() {
		let bytecode = assemble(&[Instr::PushCol(1), Instr::PushFloat(0.0), Instr::Raw(OP_GT)]);
		let batch = analytics_batch();
		assert_scalar_vector_parity(&bytecode, &batch, None);
		let out = execute(&bytecode, &batch).unwrap();
		assert_eq!(out.num_rows(), 2);
	}

	#[test]
	fn timestamp_int_cross_type_comparison_matches_scalar_semantics() {
		let bytecode = assemble(&[Instr::PushCol(3), Instr::PushInt(1_000), Instr::Raw(OP_GTE)]);
		let batch = analytics_batch();
		assert_scalar_vector_parity(&bytecode, &batch, None);
		let out = execute(&bytecode, &batch).unwrap();
		assert_eq!(out.num_rows(), 3);
	}

	#[test]
	fn int_float_cross_type_comparison_matches_scalar_semantics() {
		let bytecode = assemble(&[Instr::PushCol(0), Instr::PushFloat(0.5), Instr::Raw(OP_GT)]);
		let batch = analytics_batch();
		assert_scalar_vector_parity(&bytecode, &batch, None);
		let out = execute(&bytecode, &batch).unwrap();
		assert_eq!(out.num_rows(), 3);
	}

	#[test]
	fn in_list_null_target_remains_null_for_is_null() {
		let bytecode = assemble(&[
			Instr::PushCol(1),
			Instr::PushInt(18),
			Instr::InList(1),
			Instr::Raw(OP_IS_NULL),
		]);
		let batch = users_batch();
		assert_scalar_vector_parity(&bytecode, &batch, None);
		let out = execute(&bytecode, &batch).unwrap();
		assert_eq!(out.num_rows(), 1);
	}

	#[test]
	fn runtime_trap_returns_empty_projected_batch() {
		let bytecode = assemble(&[
			Instr::PushCol(1),
			Instr::PushBool(true),
			Instr::Raw(OP_AND),
			Instr::Project(&[0]),
		]);
		let out = execute(&bytecode, &users_batch()).unwrap();
		assert_eq!(out.num_rows(), 0);
		assert_eq!(out.num_columns(), 1);
		assert_eq!(out.schema().field(0).name(), "name");
	}

	#[test]
	fn empty_batch_with_projection_stays_empty_and_projected() {
		let bytecode = assemble(&[
			Instr::PushCol(1),
			Instr::PushInt(18),
			Instr::Raw(OP_GTE),
			Instr::Project(&[0]),
		]);
		let batch = users_batch().slice(0, 0);
		assert_scalar_vector_parity(&bytecode, &batch, None);
		let out = execute(&bytecode, &batch).unwrap();
		assert_eq!(out.num_rows(), 0);
		assert_eq!(out.num_columns(), 1);
	}

	#[test]
	fn wpbc_surface_survives_smoke_fuzz_samples() {
		assert_wpbc_fuzz_surface(SMOKE_FUZZ_SAMPLE_COUNT, 512);
	}

	#[test]
	#[ignore = "weekly ladder: run via ci/check_wpbc_fuzz_ladder.sh"]
	fn wpbc_surface_survives_weekly_fuzz_ladder() {
		let sample_count = std::env::var("WPBC_FUZZ_SAMPLES")
			.ok()
			.and_then(|raw| raw.parse::<u64>().ok())
			.unwrap_or(WEEKLY_FUZZ_SAMPLE_COUNT);
		eprintln!("running WPBC fuzz ladder with {sample_count} samples");
		assert_wpbc_fuzz_surface(sample_count, MAX_BYTECODE_BYTES);
	}
}
