use super::*;
use crate::storage::mutation_visibility::{commitment::TestProofKey, *};
use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use std::sync::Arc;
fn golden(name: &str) -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("../tests/fixtures/b1_goldens.json")).unwrap();
	let s = v["cases"]
		.as_array()
		.unwrap()
		.iter()
		.find(|c| c["name"] == name)
		.unwrap()["expected_hex"]
		.as_str()
		.unwrap();
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn batch() -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("email", DataType::Utf8, true),
		])),
		vec![
			Arc::new(Int64Array::from(vec![42])),
			Arc::new(StringArray::from(vec![None::<&str>])),
		],
	)
	.unwrap()
}
fn schema(b: &RecordBatch) -> ProofSchema {
	ProofSchema::new([2; 16], 3, b.schema(), 0).unwrap()
}
fn id() -> MutationIdentity {
	MutationIdentity {
		generation: [1; 16],
		tenant: 7,
		scope: MutationScope::Table,
		table: [2; 16],
		schema_generation: 3,
		caller: [4; 16],
		sequence: 9,
	}
}
fn binding() -> ProofKeyBinding {
	ProofKeyBinding {
		generation: [1; 16],
		tenant: 7,
		key_id: [5; 16],
		key_version: 1,
	}
}
fn child() -> ProofChildInput {
	ProofChildInput {
		range_start: 0,
		range_end: u64::MAX,
		partition: None,
		input_rows: vec![0],
	}
}
#[test]
fn independent_key_row_and_append_goldens() {
	let b = batch();
	let s = schema(&b);
	let key = s.encode_key(&b, 0).unwrap();
	assert_eq!(key, golden("key_int64_42"));
	s.validate_key(&key).unwrap();
	let row = s.encode_row(&b, 0).unwrap();
	assert_eq!(row, golden("row_id42_email_null"));
	s.validate_row(&row).unwrap();
	let i = MutationIdentity {
		generation: [0x11; 16],
		tenant: 1,
		scope: MutationScope::Table,
		table: [0x22; 16],
		schema_generation: 1,
		caller: [0x33; 16],
		sequence: 7,
	};
	assert_eq!(
		append_only_key_input(&i, MutationOperation::Insert, 3)
			.unwrap()
			.as_slice(),
		golden("append_ordinal3")
	);
	let timestamp_batch = RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new(
			"id",
			DataType::Timestamp(TimeUnit::Millisecond, None),
			false,
		)])),
		vec![Arc::new(TimestampMillisecondArray::from(vec![-1]))],
	)
	.unwrap();
	let timestamp_schema = schema(&timestamp_batch);
	let timestamp_key = timestamp_schema.encode_key(&timestamp_batch, 0).unwrap();
	assert_eq!(timestamp_key, golden("key_timestamp_ms_minus1"));
	timestamp_schema.validate_key(&timestamp_key).unwrap();
	let mut moved = i;
	moved.schema_generation = 2;
	assert_eq!(
		append_only_key_input(&moved, MutationOperation::Insert, 3).unwrap(),
		append_only_key_input(&i, MutationOperation::Insert, 3).unwrap()
	);
	assert!(append_only_key_input(&i, MutationOperation::Update, 3).is_err());
	assert!(append_only_key_input(&i, MutationOperation::Insert, 512).is_err());
}
#[test]
fn every_scalar_sample_has_independent_payload_bytes() {
	let samples: Vec<(&str, ArrayRef)> = vec![
		("bool_true", Arc::new(BooleanArray::from(vec![true]))),
		("int8_minus1", Arc::new(Int8Array::from(vec![-1]))),
		("int16_minus2", Arc::new(Int16Array::from(vec![-2]))),
		("int32_minus3", Arc::new(Int32Array::from(vec![-3]))),
		("int64_42", Arc::new(Int64Array::from(vec![42]))),
		("uint8_255", Arc::new(UInt8Array::from(vec![255]))),
		("uint16_256", Arc::new(UInt16Array::from(vec![256]))),
		("uint32_65536", Arc::new(UInt32Array::from(vec![65536]))),
		(
			"uint64_4294967296",
			Arc::new(UInt64Array::from(vec![4294967296])),
		),
		(
			"float32_negative_zero",
			Arc::new(Float32Array::from(vec![-0.0])),
		),
		(
			"float64_nan_payload",
			Arc::new(Float64Array::from(vec![f64::from_bits(0x7ff8000000000001)])),
		),
		("utf8_e_acute", Arc::new(StringArray::from(vec!["é"]))),
		(
			"binary_00ff",
			Arc::new(BinaryArray::from(vec![&[0u8, 255][..]])),
		),
		("date32_minus1", Arc::new(Date32Array::from(vec![-1]))),
		(
			"timestamp_ms_minus1",
			Arc::new(TimestampMillisecondArray::from(vec![-1])),
		),
		(
			"decimal128_123_p5_s2",
			Arc::new(
				Decimal128Array::from(vec![123])
					.with_precision_and_scale(5, 2)
					.unwrap(),
			),
		),
	];
	for (name, a) in samples {
		let t = ProofScalarType::from_arrow(a.data_type()).unwrap();
		assert_eq!(
			scalar::cell(a.as_ref(), 0, t).unwrap().unwrap().bytes(),
			golden(name)
		);
	}
}
#[test]
fn utc_units_have_distinct_descriptors_rows_and_pk_policy() {
	let samples: Vec<(&str, ArrayRef, bool)> = vec![
		(
			"ms",
			Arc::new(TimestampMillisecondArray::from(vec![-1]).with_timezone("UTC")),
			true,
		),
		(
			"us",
			Arc::new(TimestampMicrosecondArray::from(vec![1000]).with_timezone("UTC")),
			false,
		),
		(
			"ns",
			Arc::new(TimestampNanosecondArray::from(vec![1000000]).with_timezone("UTC")),
			false,
		),
	];
	for (unit, a, pk) in samples {
		let t = ProofScalarType::from_arrow(a.data_type()).unwrap();
		assert_eq!(t.descriptor(), golden(&format!("utc_{unit}_descriptor")));
		assert_eq!(
			scalar::cell(a.as_ref(), 0, t).unwrap().unwrap().bytes(),
			golden(&format!("utc_{unit}_payload"))
		);
		assert_eq!(t.pk_supported(), pk);
		// Content golden has one timestamp field. Private test schema permits no PK action.
		let b = RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new(
				"t",
				a.data_type().clone(),
				false,
			)])),
			vec![a],
		)
		.unwrap();
		let mut bytes = b"VTROW001".to_vec();
		bytes.extend_from_slice(&1u16.to_le_bytes());
		table::write_field(
			&mut bytes,
			0,
			b.schema().field(0),
			t,
			b.column(0).as_ref(),
			0,
		)
		.unwrap();
		assert_eq!(bytes, golden(&format!("row_utc_{unit}")));
		if pk {
			let s = ProofSchema::new([2; 16], 3, b.schema(), 0).unwrap();
			assert_eq!(s.encode_key(&b, 0).unwrap(), golden("key_utc_ms_minus1"))
		} else {
			assert!(ProofSchema::new([2; 16], 3, b.schema(), 0).is_err())
		}
	}
	for zone in ["utc", "Etc/UTC", "GMT", "+00:00", "America/Chicago"] {
		assert!(
			ProofScalarType::from_arrow(&DataType::Timestamp(
				TimeUnit::Millisecond,
				Some(zone.into())
			))
			.is_err()
		)
	}
	for tag in 17..=19 {
		let mut bytes = vec![tag, tag - 16, b'U', b'T', b'C'];
		let mut r = super::super::encoding::Reader::new(&bytes);
		ProofScalarType::read(&mut r).unwrap();
		bytes[1] = 0;
		assert!(ProofScalarType::read(&mut super::super::encoding::Reader::new(&bytes)).is_err());
	}
}
#[test]
fn scalar_bounds_and_bit_identity_refusals() {
	for (t, width) in [
		(ProofScalarType::Int8, 1),
		(ProofScalarType::Int16, 2),
		(ProofScalarType::Int32, 4),
		(ProofScalarType::Int64, 8),
		(ProofScalarType::UInt8, 1),
		(ProofScalarType::UInt16, 2),
		(ProofScalarType::UInt32, 4),
		(ProofScalarType::UInt64, 8),
		(ProofScalarType::Float32, 4),
		(ProofScalarType::Float64, 8),
		(ProofScalarType::Date32, 4),
		(ProofScalarType::TimestampMillis, 8),
		(ProofScalarType::UtcMillis, 8),
		(ProofScalarType::UtcMicros, 8),
		(ProofScalarType::UtcNanos, 8),
	] {
		for n in [width - 1, width + 1] {
			assert!(t.validate(&vec![0; n]).is_err())
		}
		t.validate(&vec![0xff; width]).unwrap();
	}
	assert!(ProofScalarType::Boolean.validate(&[2]).is_err());
	assert!(ProofScalarType::Utf8.validate(&[0xff]).is_err());
	let decimal = ProofScalarType::Decimal128(2, 1);
	decimal.validate(&99i128.to_le_bytes()).unwrap();
	assert!(decimal.validate(&100i128.to_le_bytes()).is_err());
	assert!(decimal.validate(&i128::MIN.to_le_bytes()).is_err());
	for t in [
		DataType::Decimal128(0, 0),
		DataType::Decimal128(39, 0),
		DataType::Decimal128(2, -1),
		DataType::Decimal128(2, 3),
		DataType::Date64,
		DataType::Null,
		DataType::FixedSizeBinary(2),
		DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
	] {
		assert!(ProofScalarType::from_arrow(&t).is_err())
	}
	let decimal38 = ProofScalarType::Decimal128(38, 38);
	decimal38
		.validate(&((10u128.pow(38) - 1) as i128).to_le_bytes())
		.unwrap();
	assert!(
		decimal38
			.validate(&(10u128.pow(38) as i128).to_le_bytes())
			.is_err()
	);
	for bits in [
		0x7ff8000000000001u64,
		0x7ff8000000000002,
		0x7ff0000000000000,
		0xfff0000000000000,
	] {
		let a = Float64Array::from(vec![f64::from_bits(bits)]);
		assert_eq!(
			scalar::cell(&a, 0, ProofScalarType::Float64)
				.unwrap()
				.unwrap()
				.bytes(),
			bits.to_le_bytes()
		);
	}
	let composed = StringArray::from(vec!["é"]);
	let decomposed = StringArray::from(vec!["e\u{0301}"]);
	assert_ne!(
		scalar::cell(&composed, 0, ProofScalarType::Utf8)
			.unwrap()
			.unwrap()
			.bytes(),
		scalar::cell(&decomposed, 0, ProofScalarType::Utf8)
			.unwrap()
			.unwrap()
			.bytes()
	);
	let neg = Float32Array::from(vec![-0.0]);
	let pos = Float32Array::from(vec![0.0]);
	assert_ne!(
		scalar::cell(&neg, 0, ProofScalarType::Float32)
			.unwrap()
			.unwrap()
			.bytes(),
		scalar::cell(&pos, 0, ProofScalarType::Float32)
			.unwrap()
			.unwrap()
			.bytes()
	);
}
#[test]
fn supported_integer_min_max_arrays() {
	macro_rules! signed {
		($a:ty,$t:ty,$variant:ident) => {{
			let a = <$a>::from(vec![<$t>::MIN, <$t>::MAX]);
			for i in 0..2 {
				assert_eq!(
					scalar::cell(&a, i, ProofScalarType::$variant)
						.unwrap()
						.unwrap()
						.bytes(),
					a.value(i).to_le_bytes()
				)
			}
		}};
	}
	macro_rules! unsigned {
		($a:ty,$t:ty,$variant:ident) => {{
			let a = <$a>::from(vec![0, <$t>::MAX]);
			for i in 0..2 {
				assert_eq!(
					scalar::cell(&a, i, ProofScalarType::$variant)
						.unwrap()
						.unwrap()
						.bytes(),
					a.value(i).to_le_bytes()
				)
			}
		}};
	}
	signed!(Int8Array, i8, Int8);
	signed!(Int16Array, i16, Int16);
	signed!(Int32Array, i32, Int32);
	signed!(Int64Array, i64, Int64);
	unsigned!(UInt8Array, u8, UInt8);
	unsigned!(UInt16Array, u16, UInt16);
	unsigned!(UInt32Array, u32, UInt32);
	unsigned!(UInt64Array, u64, UInt64);
}
#[test]
fn schema_metadata_names_pk_and_canonical_parser_refuse() {
	let b = batch();
	let s = schema(&b);
	for name in ["nilket.toron.logical_type_id", "extension"] {
		let f = b
			.schema()
			.field(0)
			.clone()
			.with_metadata(std::collections::HashMap::from([(
				name.to_string(),
				"7".to_string(),
			)]));
		let schema = Arc::new(Schema::new(vec![f, b.schema().field(1).clone()]));
		assert!(ProofSchema::new([2; 16], 3, schema, 0).is_err())
	}
	assert!(ProofSchema::new([2; 16], 3, b.schema(), 1).is_err());
	assert!(ProofSchema::new([0; 16], 3, b.schema(), 0).is_err());
	let row = s.encode_row(&b, 0).unwrap();
	for i in [0, 8, 10, 12, 14, 16, 17, 18, 19, 31, 33, 35, 40, 41, 42, 43] {
		let mut bad = row.clone();
		bad[i] ^= 0xff;
		assert!(s.validate_row(&bad).is_err(), "offset {i}")
	}
	let mut bad = row;
	bad.push(0);
	assert!(s.validate_row(&bad).is_err());
	let mut key = s.encode_key(&b, 0).unwrap();
	key[8] = 12;
	assert!(s.validate_key(&key).is_err());
}
#[test]
fn large_offsets_slices_null_and_empty_are_logical_cells() {
	let b = batch();
	let s = schema(&b);
	let large = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			b.schema().field(0).clone(),
			Field::new("email", DataType::LargeUtf8, true),
		])),
		vec![
			Arc::new(Int64Array::from(vec![999, 42])),
			Arc::new(LargeStringArray::from(vec![Some("prefix"), None])),
		],
	)
	.unwrap()
	.slice(1, 1);
	assert_eq!(
		s.encode_row(&b, 0).unwrap(),
		s.encode_row(&large, 0).unwrap()
	);
	let null_batch = |junk: i64| {
		RecordBatch::try_new(
			Arc::new(Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("v", DataType::Int64, true),
			])),
			vec![
				Arc::new(Int64Array::from(vec![42])),
				Arc::new(Int64Array::new(
					vec![junk].into(),
					Some(arrow::buffer::NullBuffer::from(vec![false])),
				)),
			],
		)
		.unwrap()
	};
	let first = null_batch(999);
	let second = null_batch(-999);
	let ns = schema(&first);
	assert_eq!(
		ns.encode_row(&first, 0).unwrap(),
		ns.encode_row(&second, 0).unwrap()
	);
	let empty = RecordBatch::try_new(
		b.schema(),
		vec![
			b.column(0).clone(),
			Arc::new(StringArray::from(vec![Some("")])),
		],
	)
	.unwrap();
	assert_ne!(
		s.encode_row(&b, 0).unwrap(),
		s.encode_row(&empty, 0).unwrap()
	);
	let f = Field::new("id", DataType::Utf8, false);
	let b = RecordBatch::try_new(
		Arc::new(Schema::new(vec![f])),
		vec![Arc::new(StringArray::from(vec![""]))],
	)
	.unwrap();
	let s = schema(&b);
	assert_eq!(s.encode_key(&b, 0).unwrap(), golden("key_utf8_empty"));
	let b = RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new("id", DataType::Binary, false)])),
		vec![Arc::new(BinaryArray::from(vec![&[0, 255][..]]))],
	)
	.unwrap();
	assert_eq!(
		schema(&b).encode_key(&b, 0).unwrap(),
		golden("key_binary_00ff")
	);
}
#[test]
fn parent_builders_update_delete_raw_and_duplicate_refusal() {
	let b = batch();
	let s = schema(&b);
	let key = TestProofKey::new(binding(), [99; 32]);
	let keys = vec![s.encode_key(&b, 0).unwrap()];
	let insert = build_table_parent(
		&key,
		id(),
		Admission {
			epoch: 1,
			holder: 1,
		},
		binding(),
		MutationOperation::Insert,
		&s,
		&b,
		None,
		&[child()],
	)
	.unwrap();
	assert_eq!(insert.children[0].versions.len(), 1);
	build_table_parent(
		&key,
		id(),
		Admission {
			epoch: 1,
			holder: 1,
		},
		binding(),
		MutationOperation::Update,
		&s,
		&b,
		Some(&keys),
		&[child()],
	)
	.unwrap();
	let mut wrong = keys.clone();
	wrong[0][13] ^= 1;
	assert_eq!(
		build_table_parent(
			&key,
			id(),
			Admission {
				epoch: 1,
				holder: 1
			},
			binding(),
			MutationOperation::Update,
			&s,
			&b,
			Some(&wrong),
			&[child()]
		),
		Err(AdapterError::KeyChanged)
	);
	let delete = build_table_delete_parent(
		&key,
		id(),
		Admission {
			epoch: 1,
			holder: 1,
		},
		binding(),
		&s,
		&keys,
		&[child()],
	)
	.unwrap();
	assert!(delete.children[0].versions[0].tombstone);
	assert!(
		build_table_delete_parent(
			&key,
			id(),
			Admission {
				epoch: 1,
				holder: 1
			},
			binding(),
			&s,
			&[],
			&[]
		)
		.is_err()
	);
	assert!(
		build_table_parent(
			&key,
			id(),
			Admission {
				epoch: 1,
				holder: 1
			},
			binding(),
			MutationOperation::Insert,
			&s,
			&b.slice(0, 0),
			None,
			&[]
		)
		.is_err()
	);
	let duplicate = RecordBatch::try_new(
		b.schema(),
		vec![
			Arc::new(Int64Array::from(vec![42, 42])),
			Arc::new(StringArray::from(vec![None::<&str>, None])),
		],
	)
	.unwrap();
	let mut c = child();
	c.input_rows = vec![0, 1];
	assert!(
		build_table_parent(
			&key,
			id(),
			Admission {
				epoch: 1,
				holder: 1
			},
			binding(),
			MutationOperation::Insert,
			&s,
			&duplicate,
			None,
			&[c]
		)
		.is_err()
	);
	let mut raw = id();
	raw.scope = MutationScope::Raw;
	raw.table = [0; 16];
	raw.schema_generation = 0;
	for v in [None, Some(&b""[..]), Some(&b"abc"[..])] {
		build_raw_parent(
			&key,
			raw,
			Admission {
				epoch: 1,
				holder: 1,
			},
			binding(),
			b"key",
			v,
			&child(),
		)
		.unwrap();
	}
	assert!(
		build_raw_parent(
			&key,
			raw,
			Admission {
				epoch: 1,
				holder: 1
			},
			binding(),
			b"",
			None,
			&child()
		)
		.is_err()
	);
}
#[test]
fn canonical_key_content_limit_is_total_encoded_size() {
	for n in [4083, 4084] {
		let b = RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
			vec![Arc::new(StringArray::from(vec!["x".repeat(n)]))],
		)
		.unwrap();
		assert_eq!(schema(&b).encode_key(&b, 0).is_ok(), n == 4083)
	}
	// id entry 21 + email entry 16 + header 10 = 47 bytes.
	for n in [MAX_CONTENT_INPUT - 47, MAX_CONTENT_INPUT - 46] {
		let b = RecordBatch::try_new(
			batch().schema(),
			vec![
				Arc::new(Int64Array::from(vec![42])),
				Arc::new(StringArray::from(vec!["x".repeat(n)])),
			],
		)
		.unwrap();
		assert_eq!(
			schema(&b).encode_row(&b, 0).is_ok(),
			n == MAX_CONTENT_INPUT - 47
		)
	}
}
#[test]
fn canonical_adapter_bytes_reach_unchanged_a_domain() {
	struct Capture(std::cell::RefCell<Vec<Vec<u8>>>);
	#[cfg(test)]
	impl ProofKeyProvider for Capture {
		fn keyed_digest(
			&self,
			_: &ProofKeyBinding,
			b: &[u8],
		) -> Result<[u8; 32], ProofKeyUnavailable> {
			self.0.borrow_mut().push(b.to_vec());
			Ok([9; 32])
		}
	}
	let p = Capture(Default::default());
	let b = batch();
	build_table_parent(
		&p,
		id(),
		Admission {
			epoch: 1,
			holder: 1,
		},
		binding(),
		MutationOperation::Insert,
		&schema(&b),
		&b,
		None,
		&[child()],
	)
	.unwrap();
	let captured = p.0.borrow();
	assert_eq!(captured.len(), 2);
	assert!(captured[0].starts_with(b"wendelon/adr0100/key-token/v1\0"));
	assert!(captured[0].ends_with(&golden("key_int64_42")));
	assert!(captured[1].starts_with(b"wendelon/adr0100/row-content/v1\0"));
	assert!(captured[1].ends_with(&golden("row_id42_email_null")));
}
