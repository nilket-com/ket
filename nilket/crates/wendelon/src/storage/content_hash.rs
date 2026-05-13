use crate::schema::table::{
	LocalityPolicy, PbcRejectCode, TableObjectId, TableSchemaDescriptor, TableWriteAdmission,
	TableWriteRequestMeta, TableWriteValidationError, table_spread_locality,
};
use crate::schema::{Identity, Locality, TenantId};
use arrow::array::{
	Array, ArrayAccessor, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
	Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray, DurationMicrosecondArray,
	DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, Float32Array,
	Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
	IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeStringArray,
	StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
	Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
	TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
	UInt64Array,
};
use arrow::datatypes::{ArrowDictionaryKeyType, DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeSet;
use std::fmt;
use unicode_normalization::UnicodeNormalization;

/// ADR 049 row-identity BLAKE3 domain tag.
pub const DOMAIN_CONTENT_HASH_ROW: &[u8] = b"table.append-content-row.v1";

const COLUMN_TAG_DOMAIN: &[u8] = b"table.append-content-column-tag.v1";

const ENTRY_COLUMN_TAG_BYTES: usize = 8;
const ENTRY_TYPE_TAG_BYTES: usize = 2;
const ENTRY_VALUE_LENGTH_BYTES: usize = 4;

const TYPE_BOOL: u16 = 0x0001;
const TYPE_INT8: u16 = 0x0101;
const TYPE_INT16: u16 = 0x0102;
const TYPE_INT32: u16 = 0x0103;
const TYPE_INT64: u16 = 0x0104;
const TYPE_UINT8: u16 = 0x0111;
const TYPE_UINT16: u16 = 0x0112;
const TYPE_UINT32: u16 = 0x0113;
const TYPE_UINT64: u16 = 0x0114;
const TYPE_FLOAT32: u16 = 0x0201;
const TYPE_FLOAT64: u16 = 0x0202;
const TYPE_DECIMAL32: u16 = 0x0301;
const TYPE_DECIMAL64: u16 = 0x0302;
const TYPE_DECIMAL128: u16 = 0x0303;
const TYPE_DECIMAL256: u16 = 0x0304;
const TYPE_TIMESTAMP_SECOND: u16 = 0x0401;
const TYPE_TIMESTAMP_MILLISECOND: u16 = 0x0402;
const TYPE_TIMESTAMP_MICROSECOND: u16 = 0x0403;
const TYPE_TIMESTAMP_NANOSECOND: u16 = 0x0404;
const TYPE_DATE32: u16 = 0x0411;
const TYPE_DATE64: u16 = 0x0412;
const TYPE_TIME32_SECOND: u16 = 0x0421;
const TYPE_TIME32_MILLISECOND: u16 = 0x0422;
const TYPE_TIME64_MICROSECOND: u16 = 0x0423;
const TYPE_TIME64_NANOSECOND: u16 = 0x0424;
const TYPE_DURATION_SECOND: u16 = 0x0431;
const TYPE_DURATION_MILLISECOND: u16 = 0x0432;
const TYPE_DURATION_MICROSECOND: u16 = 0x0433;
const TYPE_DURATION_NANOSECOND: u16 = 0x0434;
const TYPE_INTERVAL_YEAR_MONTH: u16 = 0x0441;
const TYPE_INTERVAL_DAY_TIME: u16 = 0x0442;
const TYPE_INTERVAL_MONTH_DAY_NANO: u16 = 0x0443;
const TYPE_UTF8: u16 = 0x0501;
const TYPE_LARGE_UTF8: u16 = 0x0502;
const TYPE_BINARY: u16 = 0x0601;
const TYPE_LARGE_BINARY: u16 = 0x0602;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalRowBytes(Vec<u8>);

impl CanonicalRowBytes {
	pub fn new(bytes: Vec<u8>) -> Self {
		Self(bytes)
	}

	pub fn as_slice(&self) -> &[u8] {
		&self.0
	}

	pub fn into_vec(self) -> Vec<u8> {
		self.0
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanonicalEncodeError {
	SchemaDecode(String),
	ColumnMismatch(String),
	ColumnTagCollision(String),
	UnsupportedType { column: String, data_type: String },
	InvalidUtf8 { column: String },
	ValueTooLarge { column: String, len: usize },
	DowncastFailed { column: String, data_type: String },
}

impl CanonicalEncodeError {
	pub fn reject_code(&self) -> PbcRejectCode {
		match self {
			Self::InvalidUtf8 { .. } => PbcRejectCode::CONTENT_HASH_REJECTED_INVALID_UTF8,
			_ => PbcRejectCode::PBC_REJECT_COLUMN_TYPE_MISMATCH,
		}
	}
}

impl fmt::Display for CanonicalEncodeError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::SchemaDecode(err) => {
				write!(f, "descriptor Arrow schema cannot be decoded: {err}")
			}
			Self::ColumnMismatch(err) => f.write_str(err),
			Self::ColumnTagCollision(column) => {
				write!(f, "content-hash column_tag collision at column {column:?}")
			}
			Self::UnsupportedType { column, data_type } => {
				write!(
					f,
					"content-hash canonical encoder does not support column {column:?} type {data_type}"
				)
			}
			Self::InvalidUtf8 { column } => {
				write!(f, "CONTENT_HASH_REJECTED_INVALID_UTF8: column {column:?}")
			}
			Self::ValueTooLarge { column, len } => {
				write!(
					f,
					"content-hash value for column {column:?} exceeds u32 length: {len}"
				)
			}
			Self::DowncastFailed { column, data_type } => {
				write!(
					f,
					"content-hash canonical encoder failed to downcast column {column:?} type {data_type}"
				)
			}
		}
	}
}

impl std::error::Error for CanonicalEncodeError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentHashAdmissionOutcome {
	Absorbed,
	Stored { added_digest_element: bool },
}

/// ADR 049 content-hash identity helper.
pub fn content_hash_row_key_id(
	tenant: TenantId,
	table: TableObjectId,
	canonical_row_bytes: &[u8],
) -> Identity {
	let mut hasher = blake3::Hasher::new();
	hasher.update(DOMAIN_CONTENT_HASH_ROW);
	hasher.update(&tenant_id_slot(tenant));
	hasher.update(table.as_bytes());
	hasher.update(canonical_row_bytes);
	Identity::new(*hasher.finalize().as_bytes())
}

/// Stable fallback column tag for ADR 049 V1.
///
/// The descriptor does not yet expose assigned per-column ids, so
/// the V1 fallback is a domain-separated logical-name hash. This is
/// stable across additive evolution and descriptor field reordering.
pub fn content_hash_column_tag(column_name: &str) -> u64 {
	let mut hasher = blake3::Hasher::new();
	hasher.update(COLUMN_TAG_DOMAIN);
	hasher.update(&(column_name.len() as u32).to_le_bytes());
	hasher.update(column_name.as_bytes());
	let digest = hasher.finalize();
	u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("slice width"))
}

pub fn encode_row_canonical(
	descriptor: &TableSchemaDescriptor,
	batch: &RecordBatch,
	row: usize,
) -> Result<CanonicalRowBytes, CanonicalEncodeError> {
	if row >= batch.num_rows() {
		return Err(CanonicalEncodeError::ColumnMismatch(format!(
			"row index {row} out of bounds for {} rows",
			batch.num_rows()
		)));
	}
	let schema = descriptor
		.arrow_schema()
		.map_err(|err| CanonicalEncodeError::SchemaDecode(err.to_string()))?;
	if batch.schema().as_ref() != &schema {
		return Err(CanonicalEncodeError::ColumnMismatch(
			"table write Arrow schema does not match descriptor schema".to_string(),
		));
	}
	if schema.fields().len() != batch.num_columns() {
		return Err(CanonicalEncodeError::ColumnMismatch(format!(
			"schema column count {} does not match batch column count {}",
			schema.fields().len(),
			batch.num_columns()
		)));
	}

	let mut seen_tags = BTreeSet::new();
	let mut entries: Vec<(u64, u16, Vec<u8>)> = Vec::new();
	for (field, array) in schema.fields().iter().zip(batch.columns()) {
		let column = field.name();
		if array.is_null(row) {
			continue;
		}
		let tag = content_hash_column_tag(column);
		if !seen_tags.insert(tag) {
			return Err(CanonicalEncodeError::ColumnTagCollision(column.clone()));
		}
		let Some((type_tag, value_bytes)) = encode_value(field.as_ref(), array.as_ref(), row)?
		else {
			continue;
		};
		entries.push((tag, type_tag, value_bytes));
	}
	entries.sort_by_key(|entry| entry.0);

	let mut bytes = Vec::new();
	for (column_tag, type_tag, value_bytes) in entries {
		let len =
			u32::try_from(value_bytes.len()).map_err(|_| CanonicalEncodeError::ValueTooLarge {
				column: format!("tag:{column_tag}"),
				len: value_bytes.len(),
			})?;
		bytes.reserve(
			ENTRY_COLUMN_TAG_BYTES
				+ ENTRY_TYPE_TAG_BYTES
				+ ENTRY_VALUE_LENGTH_BYTES
				+ value_bytes.len(),
		);
		bytes.extend_from_slice(&column_tag.to_le_bytes());
		bytes.extend_from_slice(&type_tag.to_le_bytes());
		bytes.extend_from_slice(&len.to_le_bytes());
		bytes.extend_from_slice(&value_bytes);
	}
	Ok(CanonicalRowBytes::new(bytes))
}

pub fn admit_content_hash_write(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
) -> Result<TableWriteAdmission, TableWriteValidationError> {
	let mut key_identities = Vec::with_capacity(batch.num_rows());
	let mut localities = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		let canonical = encode_row_canonical(descriptor, batch, row).map_err(|err| {
			TableWriteValidationError::new(
				err.reject_code(),
				format!("content-hash canonical admission failed: {err}"),
			)
		})?;
		let key_identity =
			content_hash_row_key_id(meta.tenant_id, meta.table_object_id, canonical.as_slice());
		let locality = content_hash_locality(meta, descriptor, key_identity)?;
		key_identities.push(key_identity);
		localities.push(locality);
	}
	Ok(TableWriteAdmission {
		canonical_primary_keys: Vec::new(),
		key_identities,
		localities,
	})
}

fn content_hash_locality(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptor,
	key_identity: Identity,
) -> Result<Locality, TableWriteValidationError> {
	match descriptor.locality_policy {
		LocalityPolicy::IdentityProjection => Ok(Locality::from(key_identity)),
		LocalityPolicy::TableSpreadProjection(spread) => {
			table_spread_locality(meta.tenant_id, meta.table_object_id, key_identity, spread)
				.map_err(|err| {
					TableWriteValidationError::new(
						PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED,
						format!("content-hash table spread locality failed: {err}"),
					)
				})
		}
		LocalityPolicy::ColumnProjection(_) => Err(TableWriteValidationError::new(
			PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED,
			"AppendOnlyContentHash rejects LocalityPolicy::ColumnProjection",
		)),
	}
}

fn tenant_id_slot(tenant_id: TenantId) -> [u8; 16] {
	let mut bytes = [0u8; 16];
	bytes[..8].copy_from_slice(&tenant_id.get().to_le_bytes());
	bytes
}

fn encode_value(
	field: &Field,
	array: &dyn Array,
	row: usize,
) -> Result<Option<(u16, Vec<u8>)>, CanonicalEncodeError> {
	if array.is_null(row) {
		return Ok(None);
	}
	let column = field.name();
	match field.data_type() {
		DataType::Boolean => {
			let value = downcast::<BooleanArray>(array, column)?;
			Ok(Some((TYPE_BOOL, vec![u8::from(value.value(row))])))
		}
		DataType::Int8 => {
			encode_fixed::<Int8Array, _>(array, column, TYPE_INT8, row, |v| vec![v as u8])
		}
		DataType::Int16 => encode_fixed::<Int16Array, _>(array, column, TYPE_INT16, row, |v| {
			v.to_le_bytes().to_vec()
		}),
		DataType::Int32 => encode_fixed::<Int32Array, _>(array, column, TYPE_INT32, row, |v| {
			v.to_le_bytes().to_vec()
		}),
		DataType::Int64 => encode_fixed::<Int64Array, _>(array, column, TYPE_INT64, row, |v| {
			v.to_le_bytes().to_vec()
		}),
		DataType::UInt8 => {
			encode_fixed::<UInt8Array, _>(array, column, TYPE_UINT8, row, |v| vec![v])
		}
		DataType::UInt16 => encode_fixed::<UInt16Array, _>(array, column, TYPE_UINT16, row, |v| {
			v.to_le_bytes().to_vec()
		}),
		DataType::UInt32 => encode_fixed::<UInt32Array, _>(array, column, TYPE_UINT32, row, |v| {
			v.to_le_bytes().to_vec()
		}),
		DataType::UInt64 => encode_fixed::<UInt64Array, _>(array, column, TYPE_UINT64, row, |v| {
			v.to_le_bytes().to_vec()
		}),
		DataType::Float32 => {
			encode_fixed::<Float32Array, _>(array, column, TYPE_FLOAT32, row, |v| {
				canonical_f32_bits(v).to_le_bytes().to_vec()
			})
		}
		DataType::Float64 => {
			encode_fixed::<Float64Array, _>(array, column, TYPE_FLOAT64, row, |v| {
				canonical_f64_bits(v).to_le_bytes().to_vec()
			})
		}
		DataType::Decimal32(precision, scale) => {
			let array = downcast::<Decimal32Array>(array, column)?;
			let mut bytes = decimal_prefix(*precision, *scale);
			bytes.extend_from_slice(&array.value(row).to_le_bytes());
			Ok(Some((TYPE_DECIMAL32, bytes)))
		}
		DataType::Decimal64(precision, scale) => {
			let array = downcast::<Decimal64Array>(array, column)?;
			let mut bytes = decimal_prefix(*precision, *scale);
			bytes.extend_from_slice(&array.value(row).to_le_bytes());
			Ok(Some((TYPE_DECIMAL64, bytes)))
		}
		DataType::Decimal128(precision, scale) => {
			let array = downcast::<Decimal128Array>(array, column)?;
			let mut bytes = decimal_prefix(*precision, *scale);
			bytes.extend_from_slice(&array.value(row).to_le_bytes());
			Ok(Some((TYPE_DECIMAL128, bytes)))
		}
		DataType::Decimal256(precision, scale) => {
			let array = downcast::<Decimal256Array>(array, column)?;
			let mut bytes = decimal_prefix(*precision, *scale);
			bytes.extend_from_slice(&array.value(row).to_le_bytes());
			Ok(Some((TYPE_DECIMAL256, bytes)))
		}
		DataType::Timestamp(TimeUnit::Second, _) => encode_fixed::<TimestampSecondArray, _>(
			array,
			column,
			TYPE_TIMESTAMP_SECOND,
			row,
			|v| v.to_le_bytes().to_vec(),
		),
		DataType::Timestamp(TimeUnit::Millisecond, _) => {
			encode_fixed::<TimestampMillisecondArray, _>(
				array,
				column,
				TYPE_TIMESTAMP_MILLISECOND,
				row,
				|v| v.to_le_bytes().to_vec(),
			)
		}
		DataType::Timestamp(TimeUnit::Microsecond, _) => {
			encode_fixed::<TimestampMicrosecondArray, _>(
				array,
				column,
				TYPE_TIMESTAMP_MICROSECOND,
				row,
				|v| v.to_le_bytes().to_vec(),
			)
		}
		DataType::Timestamp(TimeUnit::Nanosecond, _) => {
			encode_fixed::<TimestampNanosecondArray, _>(
				array,
				column,
				TYPE_TIMESTAMP_NANOSECOND,
				row,
				|v| v.to_le_bytes().to_vec(),
			)
		}
		DataType::Date32 => encode_fixed::<Date32Array, _>(array, column, TYPE_DATE32, row, |v| {
			v.to_le_bytes().to_vec()
		}),
		DataType::Date64 => encode_fixed::<Date64Array, _>(array, column, TYPE_DATE64, row, |v| {
			v.to_le_bytes().to_vec()
		}),
		DataType::Time32(TimeUnit::Second) => {
			encode_fixed::<Time32SecondArray, _>(array, column, TYPE_TIME32_SECOND, row, |v| {
				v.to_le_bytes().to_vec()
			})
		}
		DataType::Time32(TimeUnit::Millisecond) => encode_fixed::<Time32MillisecondArray, _>(
			array,
			column,
			TYPE_TIME32_MILLISECOND,
			row,
			|v| v.to_le_bytes().to_vec(),
		),
		DataType::Time64(TimeUnit::Microsecond) => encode_fixed::<Time64MicrosecondArray, _>(
			array,
			column,
			TYPE_TIME64_MICROSECOND,
			row,
			|v| v.to_le_bytes().to_vec(),
		),
		DataType::Time64(TimeUnit::Nanosecond) => encode_fixed::<Time64NanosecondArray, _>(
			array,
			column,
			TYPE_TIME64_NANOSECOND,
			row,
			|v| v.to_le_bytes().to_vec(),
		),
		DataType::Duration(TimeUnit::Second) => {
			encode_fixed::<DurationSecondArray, _>(array, column, TYPE_DURATION_SECOND, row, |v| {
				v.to_le_bytes().to_vec()
			})
		}
		DataType::Duration(TimeUnit::Millisecond) => encode_fixed::<DurationMillisecondArray, _>(
			array,
			column,
			TYPE_DURATION_MILLISECOND,
			row,
			|v| v.to_le_bytes().to_vec(),
		),
		DataType::Duration(TimeUnit::Microsecond) => encode_fixed::<DurationMicrosecondArray, _>(
			array,
			column,
			TYPE_DURATION_MICROSECOND,
			row,
			|v| v.to_le_bytes().to_vec(),
		),
		DataType::Duration(TimeUnit::Nanosecond) => encode_fixed::<DurationNanosecondArray, _>(
			array,
			column,
			TYPE_DURATION_NANOSECOND,
			row,
			|v| v.to_le_bytes().to_vec(),
		),
		DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => {
			encode_fixed::<IntervalYearMonthArray, _>(
				array,
				column,
				TYPE_INTERVAL_YEAR_MONTH,
				row,
				|v| v.to_le_bytes().to_vec(),
			)
		}
		DataType::Interval(arrow::datatypes::IntervalUnit::DayTime) => {
			let array = downcast::<IntervalDayTimeArray>(array, column)?;
			let value = array.value(row);
			let mut bytes = Vec::with_capacity(8);
			bytes.extend_from_slice(&value.days.to_le_bytes());
			bytes.extend_from_slice(&value.milliseconds.to_le_bytes());
			Ok(Some((TYPE_INTERVAL_DAY_TIME, bytes)))
		}
		DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => {
			let array = downcast::<IntervalMonthDayNanoArray>(array, column)?;
			let value = array.value(row);
			let mut bytes = Vec::with_capacity(16);
			bytes.extend_from_slice(&value.months.to_le_bytes());
			bytes.extend_from_slice(&value.days.to_le_bytes());
			bytes.extend_from_slice(&value.nanoseconds.to_le_bytes());
			Ok(Some((TYPE_INTERVAL_MONTH_DAY_NANO, bytes)))
		}
		DataType::Utf8 => {
			let array = downcast::<StringArray>(array, column)?;
			Ok(Some((
				TYPE_UTF8,
				encode_utf8_value(column, array.value(row))?,
			)))
		}
		DataType::LargeUtf8 => {
			let array = downcast::<LargeStringArray>(array, column)?;
			Ok(Some((
				TYPE_LARGE_UTF8,
				encode_utf8_value(column, array.value(row))?,
			)))
		}
		DataType::Binary => {
			let array = downcast::<BinaryArray>(array, column)?;
			Ok(Some((
				TYPE_BINARY,
				encode_bytes_value(column, array.value(row))?,
			)))
		}
		DataType::LargeBinary => {
			let array = downcast::<LargeBinaryArray>(array, column)?;
			Ok(Some((
				TYPE_LARGE_BINARY,
				encode_bytes_value(column, array.value(row))?,
			)))
		}
		DataType::Dictionary(key_type, value_type) => {
			encode_dictionary_value(column, key_type, value_type, array, row)
		}
		other => Err(CanonicalEncodeError::UnsupportedType {
			column: column.clone(),
			data_type: format!("{other:?}"),
		}),
	}
}

fn encode_fixed<A, F>(
	array: &dyn Array,
	column: &str,
	type_tag: u16,
	row: usize,
	encode: F,
) -> Result<Option<(u16, Vec<u8>)>, CanonicalEncodeError>
where
	A: Array + 'static,
	for<'a> &'a A: arrow::array::ArrayAccessor,
	F: for<'a> FnOnce(<&'a A as arrow::array::ArrayAccessor>::Item) -> Vec<u8>,
{
	let array = downcast::<A>(array, column)?;
	Ok(Some((type_tag, encode(array.value(row)))))
}

fn downcast<'a, A: Array + 'static>(
	array: &'a dyn Array,
	column: &str,
) -> Result<&'a A, CanonicalEncodeError> {
	array
		.as_any()
		.downcast_ref::<A>()
		.ok_or_else(|| CanonicalEncodeError::DowncastFailed {
			column: column.to_string(),
			data_type: format!("{:?}", array.data_type()),
		})
}

fn decimal_prefix(precision: u8, scale: i8) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(2);
	bytes.push(precision);
	bytes.extend_from_slice(&scale.to_le_bytes());
	bytes
}

fn canonical_f32_bits(value: f32) -> u32 {
	if value.is_nan() {
		f32::NAN.to_bits()
	} else if value == 0.0 {
		0.0f32.to_bits()
	} else {
		value.to_bits()
	}
}

fn canonical_f64_bits(value: f64) -> u64 {
	if value.is_nan() {
		f64::NAN.to_bits()
	} else if value == 0.0 {
		0.0f64.to_bits()
	} else {
		value.to_bits()
	}
}

fn encode_utf8_value(column: &str, value: &str) -> Result<Vec<u8>, CanonicalEncodeError> {
	if !value.is_char_boundary(value.len()) {
		return Err(CanonicalEncodeError::InvalidUtf8 {
			column: column.to_string(),
		});
	}
	let normalized = value.nfc().collect::<String>();
	encode_bytes_value(column, normalized.as_bytes())
}

fn encode_bytes_value(column: &str, value: &[u8]) -> Result<Vec<u8>, CanonicalEncodeError> {
	let len = u32::try_from(value.len()).map_err(|_| CanonicalEncodeError::ValueTooLarge {
		column: column.to_string(),
		len: value.len(),
	})?;
	let mut bytes = Vec::with_capacity(4 + value.len());
	bytes.extend_from_slice(&len.to_le_bytes());
	bytes.extend_from_slice(value);
	Ok(bytes)
}

fn encode_dictionary_value(
	column: &str,
	key_type: &DataType,
	value_type: &DataType,
	array: &dyn Array,
	row: usize,
) -> Result<Option<(u16, Vec<u8>)>, CanonicalEncodeError> {
	match key_type {
		DataType::Int8 => {
			encode_dictionary_key::<arrow::datatypes::Int8Type>(column, value_type, array, row)
		}
		DataType::Int16 => {
			encode_dictionary_key::<arrow::datatypes::Int16Type>(column, value_type, array, row)
		}
		DataType::Int32 => {
			encode_dictionary_key::<arrow::datatypes::Int32Type>(column, value_type, array, row)
		}
		DataType::Int64 => {
			encode_dictionary_key::<arrow::datatypes::Int64Type>(column, value_type, array, row)
		}
		DataType::UInt8 => {
			encode_dictionary_key::<arrow::datatypes::UInt8Type>(column, value_type, array, row)
		}
		DataType::UInt16 => {
			encode_dictionary_key::<arrow::datatypes::UInt16Type>(column, value_type, array, row)
		}
		DataType::UInt32 => {
			encode_dictionary_key::<arrow::datatypes::UInt32Type>(column, value_type, array, row)
		}
		DataType::UInt64 => {
			encode_dictionary_key::<arrow::datatypes::UInt64Type>(column, value_type, array, row)
		}
		other => Err(CanonicalEncodeError::UnsupportedType {
			column: column.to_string(),
			data_type: format!("Dictionary({other:?}, {value_type:?})"),
		}),
	}
}

fn encode_dictionary_key<K>(
	column: &str,
	value_type: &DataType,
	array: &dyn Array,
	row: usize,
) -> Result<Option<(u16, Vec<u8>)>, CanonicalEncodeError>
where
	K: ArrowDictionaryKeyType,
	DictionaryArray<K>: 'static,
{
	let dictionary = array
		.as_any()
		.downcast_ref::<DictionaryArray<K>>()
		.ok_or_else(|| CanonicalEncodeError::DowncastFailed {
			column: column.to_string(),
			data_type: format!("{:?}", array.data_type()),
		})?;
	let Some(value_index) = dictionary.key(row) else {
		return Ok(None);
	};
	let value_field = Field::new(column, value_type.clone(), true);
	encode_value(&value_field, dictionary.values().as_ref(), value_index)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{
		ColumnBloomSpecV1, ColumnStatsPolicyV1, DurabilityLevel, HybridTimestamp, LocalityPolicy,
		MultiRowLayoutDescriptor, NodeId, RecordLayoutPolicy, RowIdentityPolicy,
		SchemaEvolutionPolicyV1, encode_table_write_row_count,
	};
	use arrow::array::{
		ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int64Array, LargeBinaryArray,
		ListArray, StringArray, TimestampMillisecondArray,
	};
	use arrow::datatypes::{DataType, Field, Int8Type, Int32Type, Schema};
	use std::sync::Arc;

	fn descriptor(schema: &Schema) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new_with_layout(
			TableObjectId::new([0x49; 16]),
			1,
			"analytics",
			"content_events",
			schema,
			RowIdentityPolicy::AppendOnlyContentHash,
			LocalityPolicy::IdentityProjection,
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn meta(descriptor: &TableSchemaDescriptor, rows: usize) -> TableWriteRequestMeta {
		TableWriteRequestMeta {
			parent_write_id: crate::schema::ClientWriteId {
				client_uuid: [0x49; 16],
				sequence_no: 1,
				idempotency_ttl_ms: 30_000,
				_pad: 0,
			},
			client_hlc: HybridTimestamp {
				physical_ms: 10,
				logical: 0,
				node_id: 1,
			},
			expected_epoch: 1,
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			child_ordinal_base: 0,
			proxy_source: NodeId {
				id: 0,
				_reserved: 0,
			},
			row_count: encode_table_write_row_count(rows).unwrap(),
			proxy_hop: 0,
			durability: DurabilityLevel::QUORUM,
			await_finalization: 0,
			_pad: [0; 3],
		}
	}

	#[test]
	fn sparse_nullable_addition_preserves_canonical_bytes_and_identity() {
		let old_schema = Schema::new(vec![
			Field::new("event_id", DataType::Int64, false),
			Field::new("payload", DataType::LargeBinary, true),
		]);
		let new_schema = Schema::new(vec![
			Field::new("event_id", DataType::Int64, false),
			Field::new("payload", DataType::LargeBinary, true),
			Field::new("optional_region", DataType::Utf8, true),
		]);
		let old_descriptor = descriptor(&old_schema);
		let mut new_descriptor = descriptor(&new_schema);
		new_descriptor.schema_generation = 2;
		new_descriptor.refresh_descriptor_fingerprints().unwrap();
		new_descriptor.validate().unwrap();

		let old_batch = RecordBatch::try_new(
			Arc::new(old_schema),
			vec![
				Arc::new(Int64Array::from(vec![42_i64])) as ArrayRef,
				Arc::new(LargeBinaryArray::from(vec![Some(b"payload".as_slice())])) as ArrayRef,
			],
		)
		.unwrap();
		let new_batch = RecordBatch::try_new(
			Arc::new(new_schema),
			vec![
				Arc::new(Int64Array::from(vec![42_i64])) as ArrayRef,
				Arc::new(LargeBinaryArray::from(vec![Some(b"payload".as_slice())])) as ArrayRef,
				Arc::new(StringArray::from(vec![Option::<&str>::None])) as ArrayRef,
			],
		)
		.unwrap();

		let old_bytes = encode_row_canonical(&old_descriptor, &old_batch, 0).unwrap();
		let new_bytes = encode_row_canonical(&new_descriptor, &new_batch, 0).unwrap();
		assert_eq!(old_bytes, new_bytes);

		let tenant = TenantId::new(7);
		assert_eq!(
			content_hash_row_key_id(tenant, old_descriptor.table_object_id, old_bytes.as_slice()),
			content_hash_row_key_id(tenant, new_descriptor.table_object_id, new_bytes.as_slice())
		);
	}

	#[test]
	fn content_hash_identity_ignores_descriptor_fingerprint_bytes() {
		let schema = Schema::new(vec![
			Field::new("event_id", DataType::Int64, false),
			Field::new("payload", DataType::LargeBinary, true),
		]);
		let descriptor = descriptor(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![42_i64])) as ArrayRef,
				Arc::new(LargeBinaryArray::from(vec![Some(b"payload".as_slice())])) as ArrayRef,
			],
		)
		.unwrap();
		let canonical = encode_row_canonical(&descriptor, &batch, 0).unwrap();
		let tenant = TenantId::new(7);
		let expected =
			content_hash_row_key_id(tenant, descriptor.table_object_id, canonical.as_slice());

		let mut tampered = descriptor.clone();
		tampered.logical_schema_fingerprint =
			crate::schema::LogicalSchemaFingerprint::from_bytes([0x51; 32]);
		tampered.physical_descriptor_fingerprint =
			crate::schema::PhysicalDescriptorFingerprint::from_bytes([0x52; 32]);
		let tampered_canonical = encode_row_canonical(&tampered, &batch, 0).unwrap();

		assert_eq!(tampered_canonical, canonical);
		assert_eq!(
			content_hash_row_key_id(
				tenant,
				tampered.table_object_id,
				tampered_canonical.as_slice()
			),
			expected
		);
	}

	#[test]
	fn content_hash_identity_survives_physical_retune() {
		let schema = Schema::new(vec![
			Field::new("event_id", DataType::Int64, false),
			Field::new("payload", DataType::LargeBinary, true),
		]);
		let descriptor = descriptor(&schema);
		let mut retuned = descriptor.clone();
		retuned.schema_generation = 2;
		retuned.stats_policy = ColumnStatsPolicyV1 {
			bloom_filters: vec![ColumnBloomSpecV1::new("event_id")],
			..ColumnStatsPolicyV1::default()
		};
		retuned.refresh_descriptor_fingerprints().unwrap();
		retuned.validate().unwrap();
		assert_eq!(
			retuned.logical_schema_fingerprint,
			descriptor.logical_schema_fingerprint
		);
		assert_ne!(
			retuned.physical_descriptor_fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![42_i64])) as ArrayRef,
				Arc::new(LargeBinaryArray::from(vec![Some(b"payload".as_slice())])) as ArrayRef,
			],
		)
		.unwrap();
		let tenant = TenantId::new(7);
		let before = encode_row_canonical(&descriptor, &batch, 0).unwrap();
		let after = encode_row_canonical(&retuned, &batch, 0).unwrap();

		assert_eq!(after, before);
		assert_eq!(
			content_hash_row_key_id(tenant, descriptor.table_object_id, before.as_slice()),
			content_hash_row_key_id(tenant, retuned.table_object_id, after.as_slice())
		);
	}

	#[test]
	fn canonical_encoder_normalizes_strings_zeroes_and_nans() {
		let schema = Schema::new(vec![
			Field::new("label", DataType::Utf8, false),
			Field::new("f32", DataType::Float32, false),
			Field::new("f64", DataType::Float64, false),
		]);
		let descriptor = descriptor(&schema);
		let left = RecordBatch::try_new(
			Arc::new(schema.clone()),
			vec![
				Arc::new(StringArray::from(vec!["e\u{301}"])) as ArrayRef,
				Arc::new(Float32Array::from(vec![-0.0_f32])) as ArrayRef,
				Arc::new(Float64Array::from(vec![f64::from_bits(
					0x7ff0_0000_0000_0001,
				)])) as ArrayRef,
			],
		)
		.unwrap();
		let right = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(StringArray::from(vec!["\u{00e9}"])) as ArrayRef,
				Arc::new(Float32Array::from(vec![0.0_f32])) as ArrayRef,
				Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef,
			],
		)
		.unwrap();

		assert_eq!(
			encode_row_canonical(&descriptor, &left, 0).unwrap(),
			encode_row_canonical(&descriptor, &right, 0).unwrap()
		);
	}

	#[test]
	fn dictionary_encoding_uses_logical_values_not_dictionary_slots() {
		let schema = Schema::new(vec![Field::new(
			"label",
			DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
			false,
		)]);
		let descriptor = descriptor(&schema);
		let left_dictionary = DictionaryArray::<Int8Type>::new(
			Int8Array::from(vec![Some(0_i8)]),
			Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
		);
		let right_dictionary = DictionaryArray::<Int8Type>::new(
			Int8Array::from(vec![Some(1_i8)]),
			Arc::new(StringArray::from(vec!["beta", "alpha"])) as ArrayRef,
		);
		let left = RecordBatch::try_new(
			Arc::new(schema.clone()),
			vec![Arc::new(left_dictionary) as ArrayRef],
		)
		.unwrap();
		let right = RecordBatch::try_new(
			Arc::new(schema),
			vec![Arc::new(right_dictionary) as ArrayRef],
		)
		.unwrap();

		assert_eq!(
			encode_row_canonical(&descriptor, &left, 0).unwrap(),
			encode_row_canonical(&descriptor, &right, 0).unwrap()
		);
	}

	#[test]
	fn direct_value_encodings_are_stable() {
		assert_eq!(
			encode_value(
				&Field::new("flag", DataType::Boolean, false),
				&BooleanArray::from(vec![true]),
				0,
			)
			.unwrap(),
			Some((TYPE_BOOL, vec![1]))
		);
		assert_eq!(
			encode_value(
				&Field::new("small", DataType::Int16, false),
				&Int16Array::from(vec![-2_i16]),
				0,
			)
			.unwrap(),
			Some((TYPE_INT16, (-2_i16).to_le_bytes().to_vec()))
		);
		assert_eq!(
			encode_value(
				&Field::new(
					"ts",
					DataType::Timestamp(TimeUnit::Millisecond, None),
					false
				),
				&TimestampMillisecondArray::from(vec![123_i64]),
				0,
			)
			.unwrap(),
			Some((TYPE_TIMESTAMP_MILLISECOND, 123_i64.to_le_bytes().to_vec()))
		);
		let decimal = Decimal128Array::from_iter_values([12_345_i128])
			.with_precision_and_scale(10, 2)
			.unwrap();
		let mut expected_decimal = vec![10_u8, 2_u8];
		expected_decimal.extend_from_slice(&12_345_i128.to_le_bytes());
		assert_eq!(
			encode_value(
				&Field::new("amount", DataType::Decimal128(10, 2), false),
				&decimal,
				0,
			)
			.unwrap(),
			Some((TYPE_DECIMAL128, expected_decimal))
		);
		assert_eq!(
			encode_value(
				&Field::new("blob", DataType::LargeBinary, false),
				&LargeBinaryArray::from(vec![Some(b"abc".as_slice())]),
				0,
			)
			.unwrap(),
			Some((
				TYPE_LARGE_BINARY,
				[3_u32.to_le_bytes().as_slice(), b"abc"].concat()
			))
		);
	}

	#[test]
	fn nested_types_are_rejected() {
		let values = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![
			Some(1_i32),
			Some(2_i32),
		])]);
		let field = Field::new(
			"items",
			DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
			true,
		);
		let err = encode_value(&field, &values, 0).unwrap_err();
		assert!(matches!(err, CanonicalEncodeError::UnsupportedType { .. }));
	}

	#[test]
	fn content_hash_admission_derives_identity_projection_locality() {
		let schema = Schema::new(vec![
			Field::new("event_id", DataType::Int64, false),
			Field::new("payload", DataType::LargeBinary, true),
		]);
		let descriptor = descriptor(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1_i64, 1_i64])) as ArrayRef,
				Arc::new(LargeBinaryArray::from(vec![
					Some(b"x".as_slice()),
					Some(b"x".as_slice()),
				])) as ArrayRef,
			],
		)
		.unwrap();
		let meta = meta(&descriptor, batch.num_rows());
		let admission = admit_content_hash_write(&meta, &batch, &descriptor).unwrap();
		assert_eq!(admission.key_identities.len(), 2);
		assert_eq!(admission.key_identities[0], admission.key_identities[1]);
		assert_eq!(
			admission.localities[0],
			Locality::from(admission.key_identities[0])
		);
		assert_eq!(
			admission.localities[1],
			Locality::from(admission.key_identities[1])
		);
	}
}
