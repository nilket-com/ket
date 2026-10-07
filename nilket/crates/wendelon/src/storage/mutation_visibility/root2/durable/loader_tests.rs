use super::*;
use crate::storage::mutation_visibility::{
	adapters::*,
	commitment::TestProofKey,
	provenance::{ProvenanceParent, ProvenanceReference},
	*,
};
use arrow::{
	array::{ArrayRef, BinaryArray, Int64Array, StringArray, TimestampMicrosecondArray},
	datatypes::{DataType, Field, Schema, TimeUnit},
	ipc::writer::{IpcWriteOptions, StreamWriter},
};
use std::sync::Arc;
fn frame(batch: &RecordBatch, compressed: bool) -> Vec<u8> {
	let mut b = Vec::new();
	let opt = IpcWriteOptions::default()
		.try_with_compression(if compressed {
			Some(arrow::ipc::CompressionType::LZ4_FRAME)
		} else {
			None
		})
		.unwrap();
	{
		let mut w =
			StreamWriter::try_new_with_options(&mut b, batch.schema().as_ref(), opt).unwrap();
		w.write(batch).unwrap();
		w.finish().unwrap();
	}
	b
}
fn child(c: &ClaimProjection2) -> ProofChildInput {
	let x = &c.manifest.children[0];
	ProofChildInput {
		range_start: x.range_start,
		range_end: x.range_end,
		partition: x.partition,
		input_rows: vec![0],
	}
}
fn template() -> ClaimProjection2 {
	UnverifiedBundle2::decode(&super::super::tests::fixture("bundle_one_parent"))
		.unwrap()
		.claims
		.remove(0)
		.0
}
fn wrap_data(
	batch: &RecordBatch,
	mut claim: ClaimProjection2,
	manifest: ParentManifest,
	key: &TestProofKey,
) -> UnverifiedData2 {
	let bytes = frame(batch, false);
	let commit = parent_manifest_commitment(key, &manifest).unwrap();
	claim.manifest = manifest.clone();
	claim.manifest_commitment = commit;
	claim.claim_commitment = claim_mac(&claim, [0x12; 16], key).unwrap();
	let id = manifest.identity;
	let prov = UnverifiedFrameProvenance::new(
		&id,
		&bytes,
		vec![ProvenanceParent {
			manifest,
			commitment: commit,
		}],
		vec![ProvenanceReference {
			physical_row: 0,
			parent: 0,
			child: 0,
			row: 0,
		}],
	)
	.unwrap();
	UnverifiedData2 {
		frame: bytes,
		bundle: UnverifiedBundle2 {
			provenance: prov,
			claims: vec![(
				claim,
				Log2 {
					term: 1,
					node: 1,
					index: 5,
				},
			)],
		},
	}
}
pub(in crate::storage::mutation_visibility::root2::durable) fn raw(
	delete: bool,
) -> (UnverifiedData2, LoaderCapability2, TestProofKey) {
	raw_variant(delete, None)
}
pub(in crate::storage::mutation_visibility::root2::durable) fn raw_variant(
	delete: bool,
	sequence: Option<u64>,
) -> (UnverifiedData2, LoaderCapability2, TestProofKey) {
	let mut c = template();
	if let Some(sequence) = sequence {
		c.manifest.identity.sequence = sequence;
	}
	c.manifest.identity.scope = MutationScope::Raw;
	c.manifest.identity.table = [0; 16];
	c.manifest.identity.schema_generation = 0;
	let binding = c.manifest.binding();
	let key = TestProofKey::new(binding, [0x73; 32]);
	// Independent logical expectation: key "customer-7", exact value "desktop-license" or tombstone.
	let value = if delete {
		b"".as_slice()
	} else {
		b"desktop-license".as_slice()
	};
	let batch = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("key", DataType::Binary, false),
			Field::new("value", DataType::Binary, false),
		])),
		vec![
			Arc::new(BinaryArray::from(vec![b"customer-7".as_slice()])) as ArrayRef,
			Arc::new(BinaryArray::from(vec![value])),
		],
	)
	.unwrap();
	let m = build_raw_parent(
		&key,
		c.manifest.identity,
		c.manifest.admission,
		binding,
		b"customer-7",
		if delete { None } else { Some(value) },
		&child(&c),
	)
	.unwrap();
	let d = wrap_data(&batch, c, m, &key);
	let cap = LoaderCapability2::synthetic(&d, [0x12; 16], None, false);
	(d, cap, key)
}
#[test]
fn real_uncompressed_raw_put_and_delete_match_declared_logical_expectations() {
	for delete in [false, true] {
		let (d, cap, key) = raw(delete);
		let decoded = decode_batch(&d.frame).unwrap();
		assert_eq!(
			decoded
				.column(0)
				.as_any()
				.downcast_ref::<BinaryArray>()
				.unwrap()
				.value(0),
			b"customer-7"
		);
		assert_eq!(
			decoded
				.column(1)
				.as_any()
				.downcast_ref::<BinaryArray>()
				.unwrap()
				.value(0),
			if delete {
				b"".as_slice()
			} else {
				b"desktop-license".as_slice()
			}
		);
		let content = load_content2(
			UnverifiedData2::decode(&d.encode().unwrap()).unwrap(),
			&cap,
			&key,
		)
		.unwrap();
		let m = &content.provenance.provenance().parents()[0].manifest;
		assert_eq!(
			m.operation,
			if delete {
				MutationOperation::RawDelete
			} else {
				MutationOperation::RawPut
			}
		);
		assert_eq!(m.children[0].versions[0].tombstone, delete);
	}
}
#[test]
fn real_table_ipc_preserves_user_schema_and_utc_tag() {
	let mut c = template();
	c.manifest.identity.scope = MutationScope::Table;
	c.manifest.identity.table = [2; 16];
	c.manifest.identity.schema_generation = 3;
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("email", DataType::Utf8, false),
		Field::new(
			"created",
			DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
			false,
		),
	]));
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![7])) as ArrayRef,
			Arc::new(StringArray::from(vec!["synthetic@example.invalid"])),
			Arc::new(TimestampMicrosecondArray::from(vec![1234567]).with_timezone("UTC")),
		],
	)
	.unwrap();
	let ps = ProofSchema::new(
		c.manifest.identity.table,
		c.manifest.identity.schema_generation,
		schema,
		0,
	)
	.unwrap();
	let key = TestProofKey::new(c.manifest.binding(), [0x74; 32]);
	// Independently declared expectations: id=7, email synthetic@example.invalid, UTC1234567us.
	let m = build_table_parent(
		&key,
		c.manifest.identity,
		c.manifest.admission,
		c.manifest.binding(),
		MutationOperation::Insert,
		&ps,
		&batch,
		None,
		&[child(&c)],
	)
	.unwrap();
	let d = wrap_data(&batch, c, m, &key);
	let cap = LoaderCapability2::synthetic(&d, [0x12; 16], Some(ps.clone()), true);
	let decoded = decode_batch(&d.frame).unwrap();
	assert_eq!(
		decoded
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap()
			.value(0),
		7
	);
	assert_eq!(
		decoded
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.unwrap()
			.value(0),
		"synthetic@example.invalid"
	);
	assert_eq!(
		decoded
			.column(2)
			.as_any()
			.downcast_ref::<TimestampMicrosecondArray>()
			.unwrap()
			.value(0),
		1234567
	);
	let loaded = load_content2(d.clone(), &cap, &key).unwrap();
	assert_eq!(
		loaded.provenance.provenance().parents()[0]
			.manifest
			.operation,
		MutationOperation::Insert
	);
	let bad_schema = ProofSchema::new(
		cap.scope.table,
		cap.scope.schema_generation,
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("email", DataType::Utf8, false),
			Field::new(
				"created",
				DataType::Timestamp(TimeUnit::Millisecond, None),
				false,
			),
		])),
		0,
	)
	.unwrap();
	let bad = LoaderCapability2::synthetic(&d, cap.cluster, Some(bad_schema), true);
	assert!(load_content2(d, &bad, &key).is_err());
}
#[test]
fn compressed_dictionary_extension_opaque_and_wrong_scope_refuse() {
	let (d, cap, key) = raw(false);
	assert!(ipc_preflight(b"NOT-IPC").is_err());
	let batch = decode_batch(&d.frame).unwrap();
	assert!(ipc_preflight(&frame(&batch, true)).is_err());
	let mut foreign = LoaderCapability2::synthetic(&d, [0x19; 16], None, false);
	assert!(load_content2(d.clone(), &foreign, &key).is_err());
	foreign.cluster = [0x12; 16];
	let mut bad = cap;
	bad.scope.tenant += 1;
	assert!(load_content2(d, &bad, &key).is_err());
	let dictionary = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
	let schema = Schema::new(vec![Field::new("key", dictionary, false)]);
	let mut bytes = Vec::new();
	{
		let mut w =
			StreamWriter::try_new_with_options(&mut bytes, &schema, IpcWriteOptions::default())
				.unwrap();
		w.finish().unwrap();
	}
	assert!(ipc_preflight(&bytes).is_err());
}
#[test]
fn selected_row_content_changed_repaired_outer_frame_still_refuses_proof() {
	let (mut d, cap, key) = raw(false);
	let batch = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("key", DataType::Binary, false),
			Field::new("value", DataType::Binary, false),
		])),
		vec![
			Arc::new(BinaryArray::from(vec![b"customer-7".as_slice()])) as ArrayRef,
			Arc::new(BinaryArray::from(vec![b"changed".as_slice()])),
		],
	)
	.unwrap();
	d.frame = frame(&batch, false);
	let p = d.bundle.provenance.parents().to_vec();
	let refs = d.bundle.provenance.references().to_vec();
	d.bundle.provenance = UnverifiedFrameProvenance::new(&cap.scope, &d.frame, p, refs).unwrap();
	assert!(load_content2(d, &cap, &key).is_err());
}
#[test]
fn nonempty_delete_content_and_missing_key_cannot_verify() {
	let (d, cap, _) = raw(true);
	let wrong = TestProofKey::new(d.bundle.claims[0].0.manifest.binding(), [0x99; 32]);
	assert!(load_content2(d, &cap, &wrong).is_err());
}
#[test]
fn original_hlc_reservation_or_scoped_claim_changes_refuse_even_if_structurally_valid() {
	let (mut d, cap, key) = raw(false);
	d.bundle.claims[0].0.stamps[0].hlc.physical_ms += 1;
	assert!(d.bundle.validate().is_ok());
	assert!(load_content2(d, &cap, &key).is_err());
	let (mut d, cap, key) = raw(false);
	d.bundle.claims[0].1.index += 1;
	assert!(load_content2(d, &cap, &key).is_err());
	let (mut d, cap, key) = raw(false);
	d.bundle.claims[0].0.claim_commitment[0] ^= 1;
	let mut altered = LoaderCapability2::synthetic(&d, cap.cluster, None, false);
	altered.scope = cap.scope;
	assert!(load_content2(d, &altered, &key).is_err());
}
#[test]
fn index_original_clock_tombstone_and_row_are_checked_against_bundle() {
	let (d, _, _) = raw(false);
	let c = &d.bundle.claims[0].0;
	let mut i = UnverifiedIndex2 {
		identity: c.manifest.identity,
		child: 0,
		row: 0,
		hlc: c.stamps[0].hlc,
		claim: c.digest().unwrap(),
		reservation: d.bundle.claims[0].1,
		binding: [0; 32],
		object: [1; 16],
		object_digest: [2; 32],
		offset: 0,
		length: d.encode().unwrap().len() as u32,
		physical_row: 0,
		tombstone: false,
		partition: c.manifest.children[0].partition,
	};
	i.check_original_binding(&d.bundle).unwrap();
	i.hlc.physical_ms += 1;
	assert!(i.check_original_binding(&d.bundle).is_err());
	i.hlc = c.stamps[0].hlc;
	i.tombstone = true;
	assert!(i.check_original_binding(&d.bundle).is_err());
	i.tombstone = false;
	i.physical_row = 1;
	assert!(i.check_original_binding(&d.bundle).is_err());
}
#[test]
fn keyed_update_and_explicit_target_delete_have_real_ipc_content_fixtures() {
	let mut c = template();
	c.manifest.identity.scope = MutationScope::Table;
	c.manifest.identity.table = [2; 16];
	c.manifest.identity.schema_generation = 3;
	let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
	let ps = ProofSchema::new(c.manifest.identity.table, 3, schema.clone(), 0).unwrap();
	let batch = RecordBatch::try_new(
		schema,
		vec![Arc::new(Int64Array::from(vec![7])) as ArrayRef],
	)
	.unwrap();
	let keys = vec![ps.encode_key(&batch, 0).unwrap()];
	let key = TestProofKey::new(c.manifest.binding(), [0x75; 32]);
	let m = build_table_parent(
		&key,
		c.manifest.identity,
		c.manifest.admission,
		c.manifest.binding(),
		MutationOperation::Update,
		&ps,
		&batch,
		Some(&keys),
		&[child(&c)],
	)
	.unwrap();
	let d = wrap_data(&batch, c.clone(), m, &key);
	let cap = LoaderCapability2::synthetic(&d, [0x12; 16], Some(ps.clone()), true);
	assert!(load_content2(d, &cap, &key).is_ok());
	let m = build_table_delete_parent(
		&key,
		c.manifest.identity,
		c.manifest.admission,
		c.manifest.binding(),
		&ps,
		&keys,
		&[child(&c)],
	)
	.unwrap();
	let deleted = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("key", DataType::Binary, false),
			Field::new("value", DataType::Binary, false),
		])),
		vec![
			Arc::new(BinaryArray::from(vec![keys[0].as_slice()])) as ArrayRef,
			Arc::new(BinaryArray::from(vec![b"".as_slice()])),
		],
	)
	.unwrap();
	let d = wrap_data(&deleted, c, m, &key);
	let cap = LoaderCapability2::synthetic(&d, [0x12; 16], Some(ps), false);
	assert!(load_content2(d, &cap, &key).is_ok());
}
#[test]
fn metadata_extra_columns_nullability_and_nonempty_delete_value_refuse() {
	let (mut d, cap, key) = raw(true);
	let schema = Arc::new(Schema::new(vec![
		Field::new("key", DataType::Binary, false),
		Field::new("value", DataType::Binary, false),
	]));
	let b = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(BinaryArray::from(vec![b"customer-7".as_slice()])) as ArrayRef,
			Arc::new(BinaryArray::from(vec![b"nonempty".as_slice()])),
		],
	)
	.unwrap();
	d.frame = frame(&b, false);
	d.bundle.provenance = UnverifiedFrameProvenance::new(
		&cap.scope,
		&d.frame,
		d.bundle.provenance.parents().to_vec(),
		d.bundle.provenance.references().to_vec(),
	)
	.unwrap();
	assert!(load_content2(d, &cap, &key).is_err());
	let schema = Schema::new_with_metadata(
		schema.fields().clone(),
		[("ARROW:extension:name".to_owned(), "unknown".to_owned())]
			.into_iter()
			.collect(),
	);
	let b = RecordBatch::try_new(Arc::new(schema), b.columns().to_vec()).unwrap();
	assert!(ipc_preflight(&frame(&b, false)).is_err());
}

#[test]
fn raw_extra_column_and_nullable_key_refuse_after_valid_ipc_preflight() {
	for nullable in [false, true] {
		let (mut d, cap, key) = raw(false);
		let mut fields = vec![
			Field::new("key", DataType::Binary, nullable),
			Field::new("value", DataType::Binary, false),
		];
		let mut arrays: Vec<ArrayRef> = vec![
			Arc::new(BinaryArray::from(vec![b"customer-7".as_slice()])),
			Arc::new(BinaryArray::from(vec![b"desktop-license".as_slice()])),
		];
		if !nullable {
			fields.push(Field::new("extra", DataType::Int64, false));
			arrays.push(Arc::new(Int64Array::from(vec![42])));
		}
		let b = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
		d.frame = frame(&b, false);
		ipc_preflight(&d.frame).unwrap();
		d.bundle.provenance = UnverifiedFrameProvenance::new(
			&cap.scope,
			&d.frame,
			d.bundle.provenance.parents().to_vec(),
			d.bundle.provenance.references().to_vec(),
		)
		.unwrap();
		assert!(load_content2(d, &cap, &key).is_err());
	}
}

fn wire_messages(bytes: &[u8]) -> Vec<(usize, usize, usize, usize)> {
	let mut out = Vec::new();
	let mut pos = 0;
	loop {
		let start = pos;
		let marker = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
		pos += 4;
		let len = if marker == u32::MAX {
			let n = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
			pos += 4;
			n
		} else {
			marker
		} as usize;
		if len == 0 {
			break;
		}
		let meta = pos;
		let msg = root_as_message(&bytes[meta..meta + len]).unwrap();
		let body_len = msg.bodyLength() as usize;
		pos += len + body_len;
		out.push((start, meta, len, body_len));
	}
	out
}
#[test]
fn fake_body_padding_huge_metadata_prefix_refuses_before_arrow_construction() {
	let (mut d, cap, key) = raw(false);
	let parts = wire_messages(&d.frame);
	let (_, meta, len, body_len) = parts[1];
	assert_eq!(body_len % 8, 0);
	assert!(body_len >= 8);
	let body_start = meta + len;
	let msg = root_as_message(&d.frame[meta..body_start]).unwrap();
	let batch = msg.header_as_record_batch().unwrap();
	assert!(
		batch
			.buffers()
			.unwrap()
			.iter()
			.all(|b| (b.offset() + b.length()) as usize <= body_len - 4),
		"fixture's removed bytes are genuine unused body padding"
	);
	let table = meta + u32::from_le_bytes(d.frame[meta..meta + 4].try_into().unwrap()) as usize;
	let vt = (table as i64
		- i32::from_le_bytes(d.frame[table..table + 4].try_into().unwrap()) as i64) as usize;
	let field = table + u16::from_le_bytes(d.frame[vt + 10..vt + 12].try_into().unwrap()) as usize;
	d.frame[field..field + 8].copy_from_slice(&((body_len - 4) as i64).to_le_bytes());
	d.frame.truncate(body_start + body_len - 4);
	d.frame.extend(0x7fffffffu32.to_le_bytes()); // old preflight skipped these; Arrow allocated from them
	d.frame.extend(0u32.to_le_bytes());
	let before = READER_CONSTRUCTIONS.with(|n| n.get());
	assert!(decode_batch(&d.frame).is_err());
	assert_eq!(READER_CONSTRUCTIONS.with(|n| n.get()), before);
	d.bundle.provenance = UnverifiedFrameProvenance::new(
		&d.bundle.claims[0].0.manifest.identity,
		&d.frame,
		d.bundle.provenance.parents().to_vec(),
		d.bundle.provenance.references().to_vec(),
	)
	.unwrap();
	assert!(load_content2(d, &cap, &key).is_err());
	assert_eq!(READER_CONSTRUCTIONS.with(|n| n.get()), before);
}
#[test]
fn accepted_seeded_frames_have_the_exact_arrow_message_boundaries() {
	let schema = Arc::new(Schema::new(vec![
		Field::new("key", DataType::Binary, false),
		Field::new("value", DataType::Binary, false),
	]));
	let mut seed = 0x9e3779b97f4a7c15u64;
	for size in 0..128 {
		let key = vec![b'k'; size + 1];
		let value = vec![b'v'; size];
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(BinaryArray::from(vec![key.as_slice()])),
				Arc::new(BinaryArray::from(vec![value.as_slice()])),
			],
		)
		.unwrap();
		let original = frame(&batch, false);
		for mutate in [false, true] {
			let mut bytes = original.clone();
			if mutate {
				let (_, meta, len, body) = wire_messages(&bytes)[1];
				seed ^= seed << 13;
				seed ^= seed >> 7;
				seed ^= seed << 17;
				bytes[meta + len + (seed as usize % body)] ^= 1;
			}
			let mut ends = Vec::new();
			if ipc_preflight_with(&bytes, |_, end| ends.push(end)).is_err() {
				continue;
			}
			assert_eq!(ends.len(), 3);
			let mut reader =
				arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes.as_slice()), None)
					.unwrap();
			assert_eq!(reader.get_ref().position() as usize, ends[0]);
			assert!(reader.next().is_some()); // may be a bounded content error, boundaries must still agree
			assert_eq!(reader.get_ref().position() as usize, ends[1]);
			assert!(reader.next().is_none());
			assert_eq!(reader.get_ref().position() as usize, ends[2]);
		}
	}
}
#[test]
fn unaligned_metadata_length_refuses_before_arrow_construction() {
	let (d, _, _) = raw(false);
	let mut bytes = d.frame;
	let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
	bytes[4..8].copy_from_slice(&(len - 4).to_le_bytes());
	let before = READER_CONSTRUCTIONS.with(|n| n.get());
	assert!(decode_batch(&bytes).is_err());
	assert_eq!(READER_CONSTRUCTIONS.with(|n| n.get()), before);
}

#[test]
fn auth_preimage_matches_existing_loader_provider_capture() {
	use std::cell::RefCell;
	struct Capture(RefCell<Vec<(ProofKeyBinding, Vec<u8>)>>);
	impl ProofKeyProvider for Capture {
		fn keyed_digest(
			&self,
			binding: &ProofKeyBinding,
			preimage: &[u8],
		) -> Result<[u8; 32], ProofKeyUnavailable> {
			self.0.borrow_mut().push((*binding, preimage.to_vec()));
			Ok([0x31; 32])
		}
	}
	let values: serde_json::Value =
		serde_json::from_str(include_str!("../auth/tests/vectors.json")).unwrap();
	let literal = |name: &str| {
		let s = values[name].as_str().unwrap();
		(0..s.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
			.collect::<Vec<_>>()
	};
	let mut c = ClaimProjection2::decode(&literal("claim_c2")).unwrap();
	for round in 0..3 {
		if round == 1 {
			c.signature = [0xf1; 64];
			c.claim_commitment = [0xf2; 32];
		}
		if round == 2 {
			c.original.lease.signature = [0xf3; 64];
		}
		let capture = Capture(RefCell::new(Vec::new()));
		assert_eq!(claim_mac(&c, [0x12; 16], &capture), Ok([0x31; 32]));
		let expected =
			super::super::super::auth::preimage::claim_mac_preimage2(&c, [0x12; 16]).unwrap();
		let calls = capture.0.borrow();
		assert_eq!(calls.len(), 1);
		assert!(calls[0].0 == c.manifest.binding());
		assert_eq!(calls[0].1, expected);
		if round < 2 {
			assert_eq!(expected, literal("claim_mac_preimage"));
		}
	}
}
