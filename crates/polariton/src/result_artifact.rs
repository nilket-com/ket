//! Producer-neutral query results.
//!
//! Arrow IPC is authoritative where a producer already supplies it. Polars is
//! retained as a lazy compatibility projection for existing consumers and as
//! the backing for producers that currently return a `DataFrame` directly.

#![allow(dead_code)] // Schema/nullness availability precedes its first admission consumer.

use arrow_array::{Array, RecordBatch};
use arrow_buffer::NullBuffer;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType as ArrowDataType, SchemaRef};
use polars::frame::DataFrame;
use polars::io::SerReader;
use polars::io::ipc::IpcStreamReader;
use polars::prelude::AnyValue;
use std::fmt;
use std::io::Cursor;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
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
	fn mint() -> Self {
		let raw = NEXT_ARTIFACT_TOKEN.fetch_add(1, Ordering::Relaxed);
		Self(NonZeroU64::new(raw).expect("artifact token counter exhausted"))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldNullability {
	Known(bool),
	Unknown,
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
		Self::from_polars_with_token(frame, source.artifact_token)
	}

	fn from_polars_with_token(frame: Arc<DataFrame>, artifact_token: ArtifactToken) -> Arc<Self> {
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
			artifact_token,
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

	#[cfg(test)]
	pub fn is_arrow_backed(&self) -> bool {
		matches!(&self.backing, ResultBacking::Arrow(_))
	}
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
	use arrow_array::{ArrayRef, Int32Array, NullArray};
	use arrow_ipc::writer::StreamWriter;
	use arrow_schema::{DataType, Field, Schema};
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
}
