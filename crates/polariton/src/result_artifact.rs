//! Producer-neutral query results.
//!
//! Arrow IPC is authoritative where a producer already supplies it. Polars is
//! retained as a lazy compatibility projection for existing consumers and as
//! the backing for producers that currently return a `DataFrame` directly.

#![allow(dead_code)] // Schema/nullness availability precedes its first admission consumer.

use arrow_array::{Array, RecordBatch};
use arrow_buffer::NullBuffer;
use arrow_csv::Writer as ArrowCsvWriter;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType as ArrowDataType, SchemaRef};
use polars::frame::DataFrame;
use polars::io::ipc::{IpcStreamReader, IpcStreamWriter};
use polars::io::{SerReader, SerWriter};
use polars::prelude::AnyValue;
use std::fmt;
use std::io::{Cursor, Write};
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// Product-owned identity for one materialized publication.
///
/// This is deliberately not an engine revision, query id, connection id, or
/// relation-expression hash. Every result-artifact construction path mints
/// one, so adapter-neutral consumers can compare publication identity without
/// branching on the producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ArtifactToken(NonZeroU64);

static NEXT_ARTIFACT_TOKEN: AtomicU64 = AtomicU64::new(1);

impl ArtifactToken {
	pub(crate) fn mint() -> Self {
		let raw = NEXT_ARTIFACT_TOKEN.fetch_add(1, Ordering::Relaxed);
		Self(NonZeroU64::new(raw).expect("artifact token counter exhausted"))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldNullability {
	Known(bool),
	Unknown,
}

/// PRODUCT-0151 §4: the stable logical input an artifact is bound to — a
/// result name, a buffer, a Rune lineage, or a notebook cell — minted once per
/// binding and never recycled within a process.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InputIdentity(NonZeroU64);

static NEXT_INPUT_IDENTITY: AtomicU64 = AtomicU64::new(1);

impl InputIdentity {
	pub fn mint() -> Self {
		let raw = NEXT_INPUT_IDENTITY.fetch_add(1, Ordering::Relaxed);
		Self(NonZeroU64::new(raw).expect("input identity counter exhausted"))
	}

	pub(crate) fn raw(self) -> u64 {
		self.0.get()
	}

	pub(crate) fn from_raw(raw: u64) -> Self {
		Self(NonZeroU64::new(raw).expect("stored input identity is nonzero"))
	}
}

/// An input identity paired with the token of the artifact currently
/// published for it: what a selection was computed against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Publication {
	pub input: InputIdentity,
	pub token: ArtifactToken,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultField {
	name: String,
	nullability: FieldNullability,
}

impl ResultField {
	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn nullability(&self) -> FieldNullability {
		self.nullability
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResultSchema {
	fields: Vec<ResultField>,
}

impl ResultSchema {
	pub fn fields(&self) -> &[ResultField] {
		&self.fields
	}

	pub fn field(&self, name: &str) -> Option<&ResultField> {
		self.fields.iter().find(|field| field.name == name)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArrowNullabilityPolicy {
	Preserve,
	Unknown,
}

#[derive(Debug)]
struct ArrowBacking {
	schema: SchemaRef,
	ipc_streams: Vec<Vec<u8>>,
	batches: Vec<RecordBatch>,
}

#[derive(Debug)]
enum ResultBacking {
	Arrow(ArrowBacking),
	Polars(Arc<DataFrame>),
}

#[derive(Debug)]
pub struct ResultArtifact {
	artifact_token: ArtifactToken,
	/// Bound before publication becomes observable. The atomic lets a producer
	/// materialize first and the product binding (result name, buffer, Rune
	/// lineage, or notebook cell) install its stable identity without copying
	/// Arrow batches. Derived artifacts snapshot it through
	/// `from_polars_derived`.
	input_identity: AtomicU64,
	input_identity_bound: AtomicBool,
	/// PRODUCT-0154 §4: the adapter kind that produced these rows, bound once at
	/// the dispatch seam where the adapter is known and inherited by derived
	/// artifacts, so provenance travels with the data instead of being
	/// inferred at publish time.
	producer: OnceLock<String>,
	schema: ResultSchema,
	backing: ResultBacking,
	shape: (usize, usize),
	projection: OnceLock<Result<Arc<DataFrame>, String>>,
}

#[derive(Debug)]
pub enum ArrowNulls<'a> {
	AllValid,
	Physical(&'a NullBuffer),
	Logical(NullBuffer),
}

#[derive(Debug)]
pub struct ValiditySegment<'a> {
	pub rows: Range<usize>,
	pub nulls: ArrowNulls<'a>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultArtifactError {
	EmptyIpcInput,
	Ipc(String),
	SchemaMismatch { stream: usize },
	RowCountOverflow,
	FieldNotFound(String),
	RowOutOfBounds { row: usize, height: usize },
	NotArrowBacked,
	Polars(String),
	Csv(String),
}

impl fmt::Display for ResultArtifactError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::EmptyIpcInput => {
				formatter.write_str("an Arrow result requires at least one complete IPC stream")
			}
			Self::Ipc(message) => write!(formatter, "Arrow IPC decode failed: {message}"),
			Self::SchemaMismatch { stream } => {
				write!(
					formatter,
					"Arrow IPC stream {stream} has a different schema"
				)
			}
			Self::RowCountOverflow => {
				formatter.write_str("Arrow result row count overflowed usize")
			}
			Self::FieldNotFound(field) => write!(formatter, "result field not found: {field}"),
			Self::RowOutOfBounds { row, height } => {
				write!(formatter, "result row {row} is outside 0..{height}")
			}
			Self::NotArrowBacked => {
				formatter.write_str("raw validity segments require an Arrow backing")
			}
			Self::Polars(message) => write!(formatter, "Polars result access failed: {message}"),
			Self::Csv(message) => write!(formatter, "CSV export failed: {message}"),
		}
	}
}

impl std::error::Error for ResultArtifactError {}

impl ResultArtifact {
	/// Construct the sole authoritative Arrow representation from complete IPC
	/// streams. Batches and schema are derived here and cannot be injected.
	pub fn from_arrow_ipc(
		ipc_streams: Vec<Vec<u8>>,
		nullability_policy: ArrowNullabilityPolicy,
	) -> Result<Arc<Self>, ResultArtifactError> {
		if ipc_streams.is_empty() {
			return Err(ResultArtifactError::EmptyIpcInput);
		}
		let mut schema: Option<SchemaRef> = None;
		let mut batches = Vec::new();
		let mut height = 0_usize;
		for (stream_index, bytes) in ipc_streams.iter().enumerate() {
			let mut reader = StreamReader::try_new(Cursor::new(bytes.as_slice()), None)
				.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
			let stream_schema = reader.schema();
			if let Some(expected) = &schema {
				if expected.as_ref() != stream_schema.as_ref() {
					return Err(ResultArtifactError::SchemaMismatch {
						stream: stream_index,
					});
				}
			} else {
				schema = Some(Arc::clone(&stream_schema));
			}
			for batch in &mut reader {
				let batch = batch.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
				if batch.schema().as_ref() != stream_schema.as_ref() {
					return Err(ResultArtifactError::SchemaMismatch {
						stream: stream_index,
					});
				}
				height = height
					.checked_add(batch.num_rows())
					.ok_or(ResultArtifactError::RowCountOverflow)?;
				batches.push(batch);
			}
		}
		let schema = schema.expect("nonempty IPC input always yielded a schema");
		let result_schema = ResultSchema {
			fields: schema
				.fields()
				.iter()
				.map(|field| ResultField {
					name: field.name().clone(),
					nullability: match nullability_policy {
						ArrowNullabilityPolicy::Preserve => {
							FieldNullability::Known(field.is_nullable())
						}
						ArrowNullabilityPolicy::Unknown => FieldNullability::Unknown,
					},
				})
				.collect(),
		};
		let width = schema.fields().len();
		Ok(Arc::new(Self {
			artifact_token: ArtifactToken::mint(),
			input_identity: AtomicU64::new(InputIdentity::mint().raw()),
			input_identity_bound: AtomicBool::new(false),
			producer: OnceLock::new(),
			schema: result_schema,
			backing: ResultBacking::Arrow(ArrowBacking {
				schema,
				ipc_streams,
				batches,
			}),
			shape: (height, width),
			projection: OnceLock::new(),
		}))
	}

	/// Row/file producers have no admitted nullability metadata in this cut.
	pub fn from_polars(frame: Arc<DataFrame>) -> Arc<Self> {
		Self::from_polars_with_token(frame, ArtifactToken::mint())
	}

	/// Re-wrap a projection or derived frame without changing its publication
	/// identity. A view may change the frame allocation while remaining bound
	/// to the same published relation for selection and staleness checks.
	pub fn from_polars_derived(frame: Arc<DataFrame>, source: &Self) -> Arc<Self> {
		let derived = Self::from_polars_with_publication(
			frame,
			source.publication(),
			source.input_identity_is_bound(),
		);
		if let Some(producer) = source.producer() {
			derived.bind_producer(producer);
		}
		derived
	}

	fn from_polars_with_token(frame: Arc<DataFrame>, artifact_token: ArtifactToken) -> Arc<Self> {
		Self::from_polars_with_publication(
			frame,
			Publication {
				input: InputIdentity::mint(),
				token: artifact_token,
			},
			false,
		)
	}

	fn from_polars_with_publication(
		frame: Arc<DataFrame>,
		publication: Publication,
		input_identity_bound: bool,
	) -> Arc<Self> {
		let schema = ResultSchema {
			fields: frame
				.get_column_names()
				.into_iter()
				.map(|name| ResultField {
					name: name.to_string(),
					nullability: FieldNullability::Unknown,
				})
				.collect(),
		};
		let shape = frame.shape();
		Arc::new(Self {
			artifact_token: publication.token,
			input_identity: AtomicU64::new(publication.input.raw()),
			input_identity_bound: AtomicBool::new(input_identity_bound),
			producer: OnceLock::new(),
			schema,
			backing: ResultBacking::Polars(frame),
			shape,
			projection: OnceLock::new(),
		})
	}

	#[cfg(test)]
	pub fn test_frame(frame: DataFrame) -> Arc<Self> {
		Self::from_polars(Arc::new(frame))
	}

	pub fn schema(&self) -> &ResultSchema {
		&self.schema
	}

	/// The adapter-neutral identity of this materialized publication.
	pub fn artifact_token(&self) -> ArtifactToken {
		self.artifact_token
	}

	pub fn publication(&self) -> Publication {
		Publication {
			input: InputIdentity::from_raw(self.input_identity.load(Ordering::Acquire)),
			token: self.artifact_token,
		}
	}

	pub(crate) fn input_identity_is_bound(&self) -> bool {
		self.input_identity_bound.load(Ordering::Acquire)
	}

	/// Bind a freshly materialized artifact to the stable logical input before
	/// the artifact is exposed to consumers. Rebinding changes no publication
	/// token and therefore cannot make stale data appear current.
	pub(crate) fn bind_input_identity(&self, input: InputIdentity) {
		self.input_identity.store(input.raw(), Ordering::Release);
		self.input_identity_bound.store(true, Ordering::Release);
	}

	/// PRODUCT-0154 §4: bind the producing adapter kind. First binding wins;
	/// a producer is a birth fact, not a mutable label.
	pub fn bind_producer(&self, producer: &str) {
		let _ = self.producer.set(producer.to_owned());
	}

	pub fn producer(&self) -> Option<&str> {
		self.producer.get().map(String::as_str)
	}

	pub fn shape(&self) -> (usize, usize) {
		self.shape
	}

	pub fn height(&self) -> usize {
		self.shape.0
	}

	pub fn width(&self) -> usize {
		self.shape.1
	}

	pub fn projection(&self) -> Result<Arc<DataFrame>, ResultArtifactError> {
		match &self.backing {
			ResultBacking::Polars(frame) => Ok(Arc::clone(frame)),
			ResultBacking::Arrow(backing) => self
				.projection
				.get_or_init(|| decode_polars_projection(&backing.ipc_streams).map(Arc::new))
				.clone()
				.map_err(ResultArtifactError::Polars),
		}
	}

	/// Serialize this artifact through the product's canonical Arrow CSV
	/// writer. The Polars backing crosses its existing IPC compatibility seam
	/// before reaching that writer; no Polars CSV surface is admitted.
	pub fn write_csv<W: Write>(&self, output: W) -> Result<usize, ResultArtifactError> {
		match &self.backing {
			ResultBacking::Arrow(backing) => write_arrow_csv(
				output,
				Arc::clone(&backing.schema),
				backing.batches.iter().cloned(),
			),
			ResultBacking::Polars(frame) => write_polars_csv(output, frame),
		}
	}

	pub fn row_is_null(&self, field: &str, global_row: usize) -> Result<bool, ResultArtifactError> {
		if global_row >= self.height() {
			return Err(ResultArtifactError::RowOutOfBounds {
				row: global_row,
				height: self.height(),
			});
		}
		match &self.backing {
			ResultBacking::Polars(frame) => frame
				.column(field)
				.map_err(|_| ResultArtifactError::FieldNotFound(field.to_owned()))?
				.get(global_row)
				.map(|value| matches!(value, AnyValue::Null))
				.map_err(|error| ResultArtifactError::Polars(error.to_string())),
			ResultBacking::Arrow(_) => {
				let segments = self.arrow_validity_segments(field)?;
				let segment = segments
					.into_iter()
					.find(|segment| segment.rows.contains(&global_row))
					.ok_or(ResultArtifactError::RowOutOfBounds {
						row: global_row,
						height: self.height(),
					})?;
				let local = global_row - segment.rows.start;
				Ok(match segment.nulls {
					ArrowNulls::AllValid => false,
					ArrowNulls::Physical(nulls) => nulls.is_null(local),
					ArrowNulls::Logical(nulls) => nulls.is_null(local),
				})
			}
		}
	}

	pub fn arrow_validity_segments(
		&self,
		field: &str,
	) -> Result<Vec<ValiditySegment<'_>>, ResultArtifactError> {
		let ResultBacking::Arrow(backing) = &self.backing else {
			return Err(ResultArtifactError::NotArrowBacked);
		};
		let field_index = backing
			.schema
			.fields()
			.iter()
			.position(|candidate| candidate.name() == field)
			.ok_or_else(|| ResultArtifactError::FieldNotFound(field.to_owned()))?;
		let mut start = 0_usize;
		let mut segments = Vec::with_capacity(backing.batches.len());
		for batch in &backing.batches {
			let array = batch.column(field_index);
			let end = start + batch.num_rows();
			let nulls = if needs_logical_nulls(array.data_type()) {
				match array.logical_nulls() {
					Some(nulls) => ArrowNulls::Logical(nulls),
					None => ArrowNulls::AllValid,
				}
			} else {
				match array.nulls() {
					Some(nulls) => ArrowNulls::Physical(nulls),
					None => ArrowNulls::AllValid,
				}
			};
			segments.push(ValiditySegment {
				rows: start..end,
				nulls,
			});
			start = end;
		}
		Ok(segments)
	}

	#[cfg(test)]
	pub fn projection_is_built(&self) -> bool {
		match &self.backing {
			ResultBacking::Arrow(_) => self.projection.get().is_some(),
			ResultBacking::Polars(_) => true,
		}
	}

	/// PRODUCT-0154 §3: the package's canonical data encoding. One uncompressed
	/// Arrow IPC stream, schema and field metadata stripped, batches in producer
	/// order, written by one writer with fixed options — so equal batches yield
	/// equal bytes. Polars-backed artifacts cross this seam through Polars' IPC
	/// writer and back through the Arrow reader before the canonical writer.
	pub fn canonical_ipc(&self) -> Result<Vec<u8>, ResultArtifactError> {
		let batches: Vec<RecordBatch> = match &self.backing {
			ResultBacking::Arrow(backing) => backing.batches.clone(),
			ResultBacking::Polars(frame) => {
				let mut polars_bytes = Vec::new();
				let mut frame = frame.as_ref().clone();
				IpcStreamWriter::new(&mut polars_bytes)
					.finish(&mut frame)
					.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
				let reader = StreamReader::try_new(Cursor::new(polars_bytes.as_slice()), None)
					.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
				reader
					.collect::<Result<Vec<_>, _>>()
					.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?
			}
		};
		let source_schema = batches.first().map_or_else(
			|| match &self.backing {
				ResultBacking::Arrow(backing) => Arc::clone(&backing.schema),
				ResultBacking::Polars(_) => Arc::new(arrow_schema::Schema::empty()),
			},
			|batch| batch.schema(),
		);
		let fields: Vec<arrow_schema::Field> = source_schema
			.fields()
			.iter()
			.map(|field| canonical_field(field))
			.collect();
		let fields = fields
			.into_iter()
			.map(|field| {
				canonical_type_supported(field.data_type())
					.map(|()| field)
					.map_err(ResultArtifactError::Ipc)
			})
			.collect::<Result<Vec<_>, _>>()?;
		let schema: SchemaRef = Arc::new(arrow_schema::Schema::new(fields));
		let mut out = Vec::new();
		{
			let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut out, schema.as_ref())
				.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
			// Producer batching is not data: every publication is one batch.
			let batches = if batches.len() > 1 {
				vec![
					arrow_select::concat::concat_batches(&batches[0].schema(), &batches)
						.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?,
				]
			} else {
				batches
			};
			for batch in batches {
				let columns =
					batch
						.columns()
						.iter()
						.zip(schema.fields())
						.map(|(column, field)| {
							let cast = arrow_cast::cast(column, field.data_type())
								.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
							// An all-valid validity bitmap is not data: drop it so a producer
							// that attached one and one that did not encode identically.
							Ok(if cast.null_count() == 0 && cast.nulls().is_some() {
								arrow_array::make_array(
									cast.to_data().into_builder().nulls(None).build().map_err(
										|error| ResultArtifactError::Ipc(error.to_string()),
									)?,
								)
							} else {
								cast
							})
						})
						.collect::<Result<Vec<_>, _>>()?;
				let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
					.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
				writer
					.write(&batch)
					.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
			}
			writer
				.finish()
				.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
		}
		Ok(out)
	}

	#[cfg(test)]
	pub fn is_arrow_backed(&self) -> bool {
		matches!(&self.backing, ResultBacking::Arrow(_))
	}
}

fn write_arrow_csv<W, B>(
	output: W,
	schema: SchemaRef,
	batches: B,
) -> Result<usize, ResultArtifactError>
where
	W: Write,
	B: IntoIterator<Item = RecordBatch>,
{
	let mut writer = ArrowCsvWriter::new(output);
	let mut rows = 0_usize;
	let mut wrote_batch = false;
	for batch in batches {
		rows = rows
			.checked_add(batch.num_rows())
			.ok_or(ResultArtifactError::RowCountOverflow)?;
		writer
			.write(&batch)
			.map_err(|error| ResultArtifactError::Csv(error.to_string()))?;
		wrote_batch = true;
	}
	if !wrote_batch {
		writer
			.write(&RecordBatch::new_empty(schema))
			.map_err(|error| ResultArtifactError::Csv(error.to_string()))?;
	}
	Ok(rows)
}

fn write_polars_csv<W: Write>(output: W, frame: &DataFrame) -> Result<usize, ResultArtifactError> {
	let mut ipc_stream = Vec::new();
	let mut frame = frame.clone();
	IpcStreamWriter::new(&mut ipc_stream)
		.finish(&mut frame)
		.map_err(|error| ResultArtifactError::Polars(error.to_string()))?;
	let mut reader = StreamReader::try_new(Cursor::new(ipc_stream), None)
		.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
	let schema = reader.schema();
	let batches = reader
		.by_ref()
		.collect::<Result<Vec<_>, _>>()
		.map_err(|error| ResultArtifactError::Ipc(error.to_string()))?;
	write_arrow_csv(output, schema, batches)
}

fn needs_logical_nulls(data_type: &ArrowDataType) -> bool {
	matches!(
		data_type,
		ArrowDataType::Null
			| ArrowDataType::Dictionary(_, _)
			| ArrowDataType::RunEndEncoded(_, _)
			| ArrowDataType::Union(_, _)
	)
}

fn decode_polars_projection(ipc_streams: &[Vec<u8>]) -> Result<DataFrame, String> {
	let mut frames = ipc_streams.iter().map(|bytes| {
		IpcStreamReader::new(Cursor::new(bytes.as_slice()))
			.finish()
			.map_err(|error| error.to_string())
	});
	let Some(first) = frames.next() else {
		return Ok(DataFrame::default());
	};
	let mut result = first?;
	for frame in frames {
		result
			.vstack_mut(&frame?)
			.map_err(|error| error.to_string())?;
	}
	Ok(result)
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow_array::{
		ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, NullArray, StringArray,
		TimestampMicrosecondArray,
	};
	use arrow_ipc::writer::StreamWriter;
	use arrow_schema::{DataType, Field, Schema, TimeUnit};
	use libaryon::core::BufferId;

	fn schema(fields: Vec<Field>) -> SchemaRef {
		Arc::new(Schema::new(fields))
	}

	fn batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
		RecordBatch::try_new(schema, columns).expect("valid Arrow fixture")
	}

	fn stream(schema: &SchemaRef, batches: &[RecordBatch]) -> Vec<u8> {
		let mut bytes = Vec::new();
		{
			let mut writer =
				StreamWriter::try_new(&mut bytes, schema.as_ref()).expect("Arrow stream writer");
			for batch in batches {
				writer.write(batch).expect("write Arrow batch");
			}
			writer.finish().expect("finish Arrow stream");
		}
		bytes
	}

	fn asymmetric_stream() -> Vec<u8> {
		let schema = schema(vec![
			Field::new("required_value", DataType::Int32, false),
			Field::new("optional_value", DataType::Int32, true),
		]);
		let batch = batch(
			Arc::clone(&schema),
			vec![
				Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
				Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
			],
		);
		stream(&schema, &[batch])
	}

	fn csv_fixture_stream() -> Vec<u8> {
		let schema = schema(vec![
			Field::new("big", DataType::Int64, false),
			Field::new("ratio", DataType::Float64, false),
			Field::new(
				"when",
				DataType::Timestamp(TimeUnit::Microsecond, None),
				false,
			),
			Field::new("day", DataType::Date32, false),
			Field::new("text", DataType::Utf8, true),
			Field::new("nullable", DataType::Utf8, true),
		]);
		let batch = batch(
			Arc::clone(&schema),
			vec![
				Arc::new(Int64Array::from(vec![9_007_199_254_740_991_i64, -9])) as ArrayRef,
				Arc::new(Float64Array::from(vec![0.1_f64 + 0.2, -1.25])) as ArrayRef,
				Arc::new(TimestampMicrosecondArray::from(vec![
					1_704_067_200_123_456_i64,
					1_704_067_200_000_001_i64,
				])) as ArrayRef,
				Arc::new(Date32Array::from(vec![19_723_i32, 19_724])) as ArrayRef,
				Arc::new(StringArray::from(vec![
					Some("comma, quote\" and\nnewline"),
					Some("plain"),
				])) as ArrayRef,
				Arc::new(StringArray::from(vec![None::<&str>, None])) as ArrayRef,
			],
		);
		stream(&schema, &[batch])
	}

	#[test]
	fn product_0084_arrow_policy_preserves_or_withholds_schema_knowledge() {
		let bytes = asymmetric_stream();
		let toron =
			ResultArtifact::from_arrow_ipc(vec![bytes.clone()], ArrowNullabilityPolicy::Preserve)
				.expect("Toron artifact");
		assert_eq!(
			toron
				.schema()
				.field("required_value")
				.unwrap()
				.nullability(),
			FieldNullability::Known(false)
		);
		assert_eq!(
			toron
				.schema()
				.field("optional_value")
				.unwrap()
				.nullability(),
			FieldNullability::Known(true)
		);
		assert_eq!(toron.schema().fields().len(), 2);
		assert!(!toron.projection_is_built());

		let bigquery = ResultArtifact::from_arrow_ipc(vec![bytes], ArrowNullabilityPolicy::Unknown)
			.expect("BigQuery artifact");
		assert!(bigquery.is_arrow_backed());
		assert!(
			bigquery
				.schema()
				.fields()
				.iter()
				.all(|field| field.nullability() == FieldNullability::Unknown)
		);
	}

	#[test]
	fn product_0084_polars_backing_is_unknown_and_projection_is_the_same_allocation() {
		let frame = Arc::new(polars::df!("x" => [1_i32, 2], "y" => [3_i32, 4]).unwrap());
		let artifact = ResultArtifact::from_polars(Arc::clone(&frame));
		assert!(!artifact.is_arrow_backed());
		assert!(
			artifact
				.schema()
				.fields()
				.iter()
				.all(|field| field.nullability() == FieldNullability::Unknown)
		);
		assert!(Arc::ptr_eq(&frame, &artifact.projection().unwrap()));
	}

	#[test]
	fn artifact_tokens_are_unique_and_stable_across_arc_sharing() {
		let first = ResultArtifact::from_polars(Arc::new(polars::df!("x" => [1_i32]).unwrap()));
		let second = ResultArtifact::from_polars(Arc::new(polars::df!("x" => [2_i32]).unwrap()));
		let shared = Arc::clone(&first);

		assert_ne!(first.artifact_token(), second.artifact_token());
		assert_eq!(first.artifact_token(), shared.artifact_token());
	}

	#[test]
	fn product_0151_derived_artifacts_inherit_the_complete_publication() {
		let source =
			ResultArtifact::from_polars(Arc::new(polars::df!("x" => [1_i32, 2]).expect("source")));
		let derived = ResultArtifact::from_polars_derived(
			Arc::new(polars::df!("x" => [2_i32]).expect("derived")),
			&source,
		);
		assert_eq!(derived.publication(), source.publication());
	}

	#[test]
	fn derived_polars_artifacts_inherit_publication_identity() {
		let source = ResultArtifact::from_polars(Arc::new(
			polars::df!("x" => [1_i32], "y" => [2_i32]).unwrap(),
		));
		let derived = ResultArtifact::from_polars_derived(
			Arc::new(source.projection().unwrap().select(["x"]).unwrap()),
			&source,
		);
		let fresh = ResultArtifact::from_polars(Arc::new(polars::df!("x" => [1_i32]).unwrap()));

		assert_eq!(derived.artifact_token(), source.artifact_token());
		assert_ne!(fresh.artifact_token(), source.artifact_token());
	}

	#[test]
	fn product_0084_arrow_projection_is_lazy_cached_and_value_equivalent() {
		let artifact = ResultArtifact::from_arrow_ipc(
			vec![asymmetric_stream()],
			ArrowNullabilityPolicy::Preserve,
		)
		.unwrap();
		assert_eq!(artifact.shape(), (2, 2));
		assert!(!artifact.projection_is_built());
		let first = artifact.projection().unwrap();
		let second = artifact.projection().unwrap();
		assert!(Arc::ptr_eq(&first, &second));
		assert_eq!(first.shape(), (2, 2));
		assert_eq!(
			first
				.column("required_value")
				.unwrap()
				.i32()
				.unwrap()
				.into_no_null_iter()
				.collect::<Vec<_>>(),
			vec![1, 2]
		);
	}

	#[test]
	fn product_0084_empty_arrow_result_keeps_width_and_publishes_without_projection() {
		let schema = schema(vec![
			Field::new("x", DataType::Int32, false),
			Field::new("optional", DataType::Int32, true),
		]);
		let artifact = ResultArtifact::from_arrow_ipc(
			vec![stream(&schema, &[])],
			ArrowNullabilityPolicy::Preserve,
		)
		.unwrap();
		assert_eq!(artifact.shape(), (0, 2));
		assert_eq!(
			artifact.schema().field("optional").unwrap().nullability(),
			FieldNullability::Known(true)
		);

		let mut registry = crate::result_registry::ResultRegistry::default();
		let claim = registry.begin("empty", BufferId::new(84), "toron".to_owned());
		let transport = crate::adapters::common::ExecutionResult::Rows {
			artifact: Arc::clone(&artifact),
			statements: 1,
		};
		let crate::adapters::common::ExecutionResult::Rows {
			artifact: transported,
			..
		} = transport
		else {
			unreachable!("constructed the rows transport")
		};
		assert!(Arc::ptr_eq(&artifact, &transported));
		assert!(registry.publish(&claim, transported));
		let provenance = registry.provenance("empty").unwrap();
		assert_eq!((provenance.rows, provenance.cols), (0, 2));
		assert!(Arc::ptr_eq(registry.artifact("empty").unwrap(), &artifact));
		assert!(!artifact.projection_is_built());
	}

	#[test]
	fn product_0084_arrow_stream_schemas_must_agree_in_dtype() {
		let int_schema = schema(vec![Field::new("x", DataType::Int32, false)]);
		let float_schema = schema(vec![Field::new("x", DataType::Float64, false)]);
		let int_batch = batch(
			Arc::clone(&int_schema),
			vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
		);
		let float_batch = batch(
			Arc::clone(&float_schema),
			vec![Arc::new(arrow_array::Float64Array::from(vec![1.0])) as ArrayRef],
		);
		let error = ResultArtifact::from_arrow_ipc(
			vec![
				stream(&int_schema, &[int_batch]),
				stream(&float_schema, &[float_batch]),
			],
			ArrowNullabilityPolicy::Preserve,
		)
		.unwrap_err();
		assert_eq!(error, ResultArtifactError::SchemaMismatch { stream: 1 });
	}

	#[test]
	fn product_0084_validity_segments_cover_all_valid_physical_sliced_and_batch_boundaries() {
		let schema = schema(vec![Field::new("value", DataType::Int32, true)]);
		let all_valid = batch(
			Arc::clone(&schema),
			vec![Arc::new(Int32Array::from(vec![Some(10), Some(11)])) as ArrayRef],
		);
		let parent = Int32Array::from(vec![None, Some(20), None, Some(22)]);
		let sliced = parent.slice(1, 3);
		let sliced = batch(Arc::clone(&schema), vec![Arc::new(sliced) as ArrayRef]);
		let artifact = ResultArtifact::from_arrow_ipc(
			vec![stream(&schema, &[all_valid]), stream(&schema, &[sliced])],
			ArrowNullabilityPolicy::Preserve,
		)
		.unwrap();
		let segments = artifact.arrow_validity_segments("value").unwrap();
		assert_eq!(segments.len(), 2);
		assert_eq!(segments[0].rows, 0..2);
		assert!(matches!(segments[0].nulls, ArrowNulls::AllValid));
		assert_eq!(segments[1].rows, 2..5);
		let ArrowNulls::Physical(nulls) = &segments[1].nulls else {
			panic!("flat nullable array must expose its physical validity")
		};
		assert_eq!(
			(0..artifact.height())
				.map(|row| artifact.row_is_null("value", row).unwrap())
				.collect::<Vec<_>>(),
			vec![false, false, false, true, false]
		);
		assert_eq!(nulls.len(), 3);
		assert!(!artifact.projection_is_built());
	}

	#[test]
	fn product_0084_logical_nullness_is_owned_without_materializing_polars() {
		let schema = schema(vec![Field::new("nothing", DataType::Null, true)]);
		let nulls = batch(
			Arc::clone(&schema),
			vec![Arc::new(NullArray::new(3)) as ArrayRef],
		);
		let artifact = ResultArtifact::from_arrow_ipc(
			vec![stream(&schema, &[nulls])],
			ArrowNullabilityPolicy::Preserve,
		)
		.unwrap();
		let segments = artifact.arrow_validity_segments("nothing").unwrap();
		assert!(matches!(segments[0].nulls, ArrowNulls::Logical(_)));
		assert!((0..3).all(|row| artifact.row_is_null("nothing", row).unwrap()));
		assert!(!artifact.projection_is_built());
	}

	#[test]
	fn product_0136_arrow_csv_is_canonical_and_keeps_the_projection_lazy() {
		let artifact = ResultArtifact::from_arrow_ipc(
			vec![csv_fixture_stream()],
			ArrowNullabilityPolicy::Preserve,
		)
		.expect("Arrow CSV fixture");
		let mut csv = Vec::new();
		assert_eq!(artifact.write_csv(&mut csv).expect("Arrow CSV writes"), 2);
		assert!(!artifact.projection_is_built());
		assert_eq!(
			String::from_utf8(csv).expect("CSV UTF-8"),
			concat!(
				"big,ratio,when,day,text,nullable\n",
				"9007199254740991,0.30000000000000004,2024-01-01T00:00:00.123456,2024-01-01,\"comma, quote\"\" and\nnewline\",\n",
				"-9,-1.25,2024-01-01T00:00:00.000001,2024-01-02,plain,\n"
			)
		);
	}

	#[test]
	fn product_0136_polars_compatibility_projection_matches_arrow_csv_bytes() {
		let arrow = ResultArtifact::from_arrow_ipc(
			vec![csv_fixture_stream()],
			ArrowNullabilityPolicy::Preserve,
		)
		.expect("Arrow fixture");
		let polars = ResultArtifact::from_polars(arrow.projection().expect("Polars projection"));
		let mut canonical = Vec::new();
		let mut compatibility = Vec::new();
		assert_eq!(arrow.write_csv(&mut canonical).expect("Arrow writes"), 2);
		assert_eq!(
			polars.write_csv(&mut compatibility).expect("Polars writes"),
			2
		);
		assert_eq!(compatibility, canonical);
	}
}

#[cfg(test)]
mod product_0154_tests {
	use super::*;
	use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray};
	use arrow_ipc::writer::StreamWriter;
	use arrow_schema::{DataType, Field, Schema};

	fn arrow_backed() -> Arc<ResultArtifact> {
		let mut metadata = std::collections::HashMap::new();
		metadata.insert(
			"producer".to_owned(),
			"noise that must not reach the package".to_owned(),
		);
		let schema = Arc::new(
			Schema::new(vec![
				Field::new("x", DataType::Int64, false),
				Field::new("y", DataType::Float64, false),
				Field::new("s", DataType::Utf8, false),
			])
			.with_metadata(metadata),
		);
		let batch = RecordBatch::try_new(
			Arc::clone(&schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
				Arc::new(Float64Array::from(vec![2.0, 3.0, 5.0])) as ArrayRef,
				Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
			],
		)
		.expect("batch");
		let mut bytes = Vec::new();
		{
			let mut writer = StreamWriter::try_new(&mut bytes, schema.as_ref()).expect("writer");
			writer.write(&batch).expect("write");
			writer.finish().expect("finish");
		}
		ResultArtifact::from_arrow_ipc(vec![bytes], ArrowNullabilityPolicy::Preserve)
			.expect("artifact")
	}

	fn polars_backed() -> Arc<ResultArtifact> {
		ResultArtifact::from_polars(Arc::new(
			polars::df!("x" => [1_i64, 2, 3], "y" => [2.0_f64, 3.0, 5.0], "s" => ["a", "b", "c"])
				.expect("frame"),
		))
	}

	#[test]
	fn product_0154_canonical_ipc_is_equal_across_backings_and_strips_metadata() {
		let arrow = arrow_backed().canonical_ipc().expect("arrow canonical");
		let polars = polars_backed().canonical_ipc().expect("polars canonical");
		assert_eq!(
			arrow, polars,
			"equal content must yield equal canonical bytes"
		);
		let reader = StreamReader::try_new(Cursor::new(arrow.as_slice()), None).expect("reader");
		assert!(
			reader.schema().metadata().is_empty(),
			"schema metadata is not data"
		);
		assert!(
			reader
				.schema()
				.fields()
				.iter()
				.all(|field| field.metadata().is_empty()),
			"field metadata is not data"
		);
		let again = arrow_backed().canonical_ipc().expect("stable");
		assert_eq!(arrow, again);
	}
	#[test]
	fn product_0154_canonical_ipc_ignores_producer_batching_and_refuses_maps() {
		let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
		let one = RecordBatch::try_new(
			Arc::clone(&schema),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef],
		)
		.expect("one batch");
		let first = RecordBatch::try_new(
			Arc::clone(&schema),
			vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
		)
		.expect("first");
		let second = RecordBatch::try_new(
			Arc::clone(&schema),
			vec![Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef],
		)
		.expect("second");
		let encode = |batches: &[RecordBatch]| {
			let mut bytes = Vec::new();
			{
				let mut writer =
					StreamWriter::try_new(&mut bytes, schema.as_ref()).expect("writer");
				for batch in batches {
					writer.write(batch).expect("write");
				}
				writer.finish().expect("finish");
			}
			ResultArtifact::from_arrow_ipc(vec![bytes], ArrowNullabilityPolicy::Preserve)
				.expect("artifact")
				.canonical_ipc()
				.expect("canonical")
		};
		let whole = encode(&[one]);
		let split = encode(&[first, second]);
		if whole != split {
			let first_diff = whole.iter().zip(&split).position(|(a, b)| a != b);
			let describe = |bytes: &[u8]| {
				StreamReader::try_new(Cursor::new(bytes), None)
					.expect("reader")
					.map(|batch| {
						let batch = batch.expect("batch");
						batch
							.columns()
							.iter()
							.map(|column| {
								let data = column.to_data();
								format!(
									"rows={} offset={} nulls={:?} buffers={:?}",
									data.len(),
									data.offset(),
									data.nulls().map(|n| n.len()),
									data.buffers().iter().map(|b| b.len()).collect::<Vec<_>>()
								)
							})
							.collect::<Vec<_>>()
					})
					.collect::<Vec<_>>()
			};
			panic!(
				"batching leaked at byte {first_diff:?}: {} vs {} bytes\nwhole: {:?}\nsplit: {:?}",
				whole.len(),
				split.len(),
				describe(&whole),
				describe(&split)
			);
		}

		let keys = Field::new("keys", DataType::Utf8, false);
		let values = Field::new("values", DataType::Int64, true);
		let entries = Field::new(
			"entries",
			DataType::Struct(vec![keys, values].into()),
			false,
		);
		let map_schema = Arc::new(Schema::new(vec![Field::new(
			"m",
			DataType::Map(Arc::new(entries), false),
			true,
		)]));
		let mut bytes = Vec::new();
		{
			let mut writer =
				StreamWriter::try_new(&mut bytes, map_schema.as_ref()).expect("writer");
			writer.finish().expect("finish");
		}
		let refused = ResultArtifact::from_arrow_ipc(vec![bytes], ArrowNullabilityPolicy::Preserve)
			.expect("artifact")
			.canonical_ipc();
		assert!(
			matches!(refused, Err(ResultArtifactError::Ipc(message)) if message.contains("does not support"))
		);
	}
}

/// PRODUCT-0154 §3: one canonical field shape. Every field is nullable, so a
/// producer that knows a column has no nulls and one that does not yield the
/// same schema; wide offset widths collapse to their ordinary forms, since a
/// publication is never wider than 2 GiB per column; metadata is not data.
fn canonical_field(field: &arrow_schema::Field) -> arrow_schema::Field {
	arrow_schema::Field::new(field.name(), canonical_type(field.data_type()), true)
}

fn canonical_type(data_type: &ArrowDataType) -> ArrowDataType {
	match data_type {
		ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => ArrowDataType::Utf8,
		ArrowDataType::LargeBinary | ArrowDataType::BinaryView => ArrowDataType::Binary,
		ArrowDataType::Dictionary(_, value) => canonical_type(value),
		ArrowDataType::LargeList(inner) | ArrowDataType::List(inner) => {
			ArrowDataType::List(Arc::new(canonical_field(inner)))
		}
		ArrowDataType::Struct(fields) => {
			ArrowDataType::Struct(fields.iter().map(|f| canonical_field(f)).collect())
		}
		other => other.clone(),
	}
}

/// PRODUCT-0154 §3: representations the canonical writer cannot make
/// deterministic are refused with a diagnostic, never cloned through.
fn canonical_type_supported(data_type: &ArrowDataType) -> Result<(), String> {
	match data_type {
		ArrowDataType::Map(..)
		| ArrowDataType::Union(..)
		| ArrowDataType::RunEndEncoded(..)
		| ArrowDataType::ListView(_)
		| ArrowDataType::LargeListView(_) => Err(format!(
			"canonical Arrow encoding does not support {data_type}; the package refuses it"
		)),
		ArrowDataType::List(inner) | ArrowDataType::FixedSizeList(inner, _) => {
			canonical_type_supported(inner.data_type())
		}
		ArrowDataType::Struct(fields) => fields
			.iter()
			.try_for_each(|field| canonical_type_supported(field.data_type())),
		_ => Ok(()),
	}
}
