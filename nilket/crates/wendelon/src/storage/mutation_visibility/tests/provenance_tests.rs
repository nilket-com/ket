use super::*;
use crate::storage::mutation_visibility::{adapters::*, provenance::*};
use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use std::sync::Arc;
pub(super) fn batch(ids: Vec<i64>) -> RecordBatch {
	let values = ids.iter().map(|i| format!("value-{i}")).collect::<Vec<_>>();
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, false),
		])),
		vec![
			Arc::new(Int64Array::from(ids)),
			Arc::new(StringArray::from(values)),
		],
	)
	.unwrap()
}
pub(super) fn schema(b: &RecordBatch) -> ProofSchema {
	ProofSchema::new([2; 16], 3, b.schema(), 0).unwrap()
}
pub(super) fn key() -> TestProofKey {
	TestProofKey::new(manifest(1).binding(), [99; 32])
}
pub(super) fn parent(sequence: u64, b: &RecordBatch, children: usize) -> ProvenanceParent {
	let assignments = (0..children)
		.map(|c| ProofChildInput {
			range_start: 0,
			range_end: u64::MAX,
			partition: None,
			input_rows: (0..b.num_rows()).filter(|r| r % children == c).collect(),
		})
		.collect::<Vec<_>>();
	let m = build_table_parent(
		&key(),
		identity(sequence),
		Admission {
			epoch: 1,
			holder: 1,
		},
		manifest(1).binding(),
		MutationOperation::Insert,
		&schema(b),
		b,
		None,
		&assignments,
	)
	.unwrap();
	let commitment = parent_manifest_commitment(&key(), &m).unwrap();
	ProvenanceParent {
		manifest: m,
		commitment,
	}
}
fn reference(physical_row: u32, parent: u16, child: u32, row: u32) -> ProvenanceReference {
	ProvenanceReference {
		physical_row,
		parent,
		child,
		row,
	}
}
pub(super) fn framed(
	b: &RecordBatch,
	policy: crate::schema::RecordBodyCompressionPolicy,
) -> Vec<u8> {
	crate::storage::multirow::encode_batch_with_policy(b, policy)
		.unwrap()
		.bytes
}
#[test]
fn packed_two_parents_reorder_large_offsets_compression_and_survivor() {
	use crate::schema::RecordBodyCompressionPolicy::*;
	let parents = vec![parent(1, &batch(vec![1]), 1), parent(2, &batch(vec![2]), 1)];
	for (ids, refs) in [
		(
			vec![1, 2],
			vec![reference(0, 0, 0, 0), reference(1, 1, 0, 0)],
		),
		(
			vec![2, 1],
			vec![reference(0, 1, 0, 0), reference(1, 0, 0, 0)],
		),
	] {
		for policy in [Uncompressed, Lz4Frame] {
			let b = batch(ids.clone());
			let frame = framed(&b, policy);
			let p =
				UnverifiedFrameProvenance::new(&identity(1), &frame, parents.clone(), refs.clone())
					.unwrap();
			let encoded = p.encode().unwrap();
			let decoded = UnverifiedFrameProvenance::decode(&encoded).unwrap();
			assert_eq!(decoded, p);
			let trusted = TrustedFrameRows::from_table_frame(frame, schema(&b)).unwrap();
			let verified = decoded.verify(&key(), &trusted).unwrap();
			assert_eq!(verified.provenance().parents(), parents);
		}
	}
	let b = batch(vec![2]);
	let large = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::LargeUtf8, false),
		])),
		vec![
			b.column(0).clone(),
			Arc::new(LargeStringArray::from(vec!["junk", "value-2"]).slice(1, 1)),
		],
	)
	.unwrap();
	let frame = framed(&large, Uncompressed);
	let p = UnverifiedFrameProvenance::new(
		&identity(1),
		&frame,
		vec![parents[1].clone()],
		vec![reference(0, 0, 0, 0)],
	)
	.unwrap();
	p.verify(
		&key(),
		&TrustedFrameRows::from_table_frame(frame, schema(&b)).unwrap(),
	)
	.unwrap();
}
#[test]
fn real_multirow_packer_and_synthetic_strip_preserve_logical_proofs() {
	use crate::schema::*;
	let b = batch(vec![1, 2]);
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([2; 16]),
		3,
		"test",
		"b1",
		b.schema().as_ref(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
			spread_factor: 1,
			prefix_bits: 1,
		}),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = TenantId::new(7);
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(2).unwrap();
	meta.client_hlc = HybridTimestamp {
		physical_ms: 10,
		logical: 0,
		node_id: 1,
	};
	let packed = crate::storage::multirow::pack_record_batch_by_bucket(
		&meta,
		&descriptor,
		&b,
		WriterNodeId::new([1; 16]),
		meta.client_hlc,
	)
	.unwrap();
	let parent = parent(1, &b, 1);
	for record in packed {
		let refs = record
			.source_row_indices
			.iter()
			.enumerate()
			.map(|(physical, source)| reference(physical as u32, 0, 0, *source as u32))
			.collect();
		let p = UnverifiedFrameProvenance::new(
			&identity(1),
			&record.frame_bytes,
			vec![parent.clone()],
			refs,
		)
		.unwrap();
		let trusted = TrustedFrameRows::from_table_frame(record.frame_bytes, schema(&b)).unwrap();
		p.verify(&key(), &trusted).unwrap();
	}
}
#[test]
fn tampered_content_mapping_parent_frame_and_unavailable_keys_refuse() {
	let b = batch(vec![1, 2]);
	let frame = framed(&b, crate::schema::RecordBodyCompressionPolicy::Uncompressed);
	let parents = vec![parent(1, &batch(vec![1]), 1), parent(2, &batch(vec![2]), 1)];
	let refs = vec![reference(0, 0, 0, 0), reference(1, 1, 0, 0)];
	let good = UnverifiedFrameProvenance::new(&identity(1), &frame, parents.clone(), refs.clone())
		.unwrap();
	let trusted = TrustedFrameRows::from_table_frame(frame.clone(), schema(&b)).unwrap();
	let wrong = TestProofKey::new(manifest(1).binding(), [100; 32]);
	assert!(good.clone().verify(&wrong, &trusted).is_err());
	let foreign = TestProofKey::new(
		ProofKeyBinding {
			tenant: 999,
			..manifest(1).binding()
		},
		[99; 32],
	);
	assert_eq!(
		good.clone().verify(&foreign, &trusted).err(),
		Some(AdapterError::KeyUnavailable)
	);
	let mut swapped = refs.clone();
	swapped[0].parent = 1;
	swapped[1].parent = 0;
	assert!(
		UnverifiedFrameProvenance::new(&identity(1), &frame, parents.clone(), swapped)
			.unwrap()
			.verify(&key(), &trusted)
			.is_err()
	);
	let tampered = framed(
		&batch(vec![2, 1]),
		crate::schema::RecordBodyCompressionPolicy::Uncompressed,
	);
	assert!(
		good.clone()
			.verify(
				&key(),
				&TrustedFrameRows::from_table_frame(tampered.clone(), schema(&b)).unwrap()
			)
			.is_err()
	);
	// Repaired frame digest still cannot authenticate swapped logical contents.
	assert!(
		UnverifiedFrameProvenance::new(&identity(1), &tampered, parents.clone(), refs.clone())
			.unwrap()
			.verify(
				&key(),
				&TrustedFrameRows::from_table_frame(tampered, schema(&b)).unwrap()
			)
			.is_err()
	);
	let mut corrupt = good.encode().unwrap();
	corrupt[20] ^= 1;
	assert!(UnverifiedFrameProvenance::decode(&corrupt).is_err());
	overwrite_envelope(&mut corrupt, 20, &[0xff]);
	assert!(UnverifiedFrameProvenance::decode(&corrupt).is_err());
	let mut p = parents.clone();
	p[0].commitment[0] ^= 1;
	assert!(
		UnverifiedFrameProvenance::new(&identity(1), &frame, p, refs.clone())
			.unwrap()
			.verify(&key(), &trusted)
			.is_err()
	);
	let mut p = parents.clone();
	p[1].manifest.identity.tenant = 8;
	assert!(UnverifiedFrameProvenance::new(&identity(1), &frame, p, refs.clone()).is_err());
	let duplicate = vec![reference(0, 0, 0, 0), reference(1, 0, 0, 0)];
	assert!(
		UnverifiedFrameProvenance::new(&identity(1), &frame, parents.clone(), duplicate).is_err()
	);
	assert!(
		UnverifiedFrameProvenance::new(
			&identity(1),
			&frame,
			parents.clone(),
			vec![reference(0, 0, 0, 0)]
		)
		.is_err()
	);
	let mut reversed = parents;
	reversed.reverse();
	assert!(UnverifiedFrameProvenance::new(&identity(1), &frame, reversed, refs).is_err());
}
fn key_value_frame(key: &[u8], value: &[u8]) -> Vec<u8> {
	let b = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("key", DataType::Binary, false),
			Field::new("value", DataType::Binary, false),
		])),
		vec![
			Arc::new(BinaryArray::from(vec![key])),
			Arc::new(BinaryArray::from(vec![value])),
		],
	)
	.unwrap();
	framed(&b, crate::schema::RecordBodyCompressionPolicy::Uncompressed)
}
#[test]
fn raw_and_explicit_table_delete_reconstruct_with_empty_tombstone() {
	let c = ProofChildInput {
		range_start: 0,
		range_end: u64::MAX,
		partition: None,
		input_rows: vec![0],
	};
	let mut raw = identity(1);
	raw.scope = MutationScope::Raw;
	raw.table = [0; 16];
	raw.schema_generation = 0;
	for value in [None, Some(&b""[..]), Some(&b"value"[..])] {
		let m = build_raw_parent(
			&key(),
			raw,
			Admission {
				epoch: 1,
				holder: 1,
			},
			manifest(1).binding(),
			b"key",
			value,
			&c,
		)
		.unwrap();
		let commitment = parent_manifest_commitment(&key(), &m).unwrap();
		let frame = key_value_frame(b"key", value.unwrap_or(&[]));
		let p = UnverifiedFrameProvenance::new(
			&raw,
			&frame,
			vec![ProvenanceParent {
				manifest: m,
				commitment,
			}],
			vec![reference(0, 0, 0, 0)],
		)
		.unwrap();
		p.verify(
			&key(),
			&TrustedFrameRows::from_key_value_frame(frame, None).unwrap(),
		)
		.unwrap();
	}
	let b = batch(vec![1]);
	let schema = schema(&b);
	let input = schema.encode_key(&b, 0).unwrap();
	let m = build_table_delete_parent(
		&key(),
		identity(1),
		Admission {
			epoch: 1,
			holder: 1,
		},
		manifest(1).binding(),
		&schema,
		&[input.clone()],
		&[c],
	)
	.unwrap();
	let commitment = parent_manifest_commitment(&key(), &m).unwrap();
	let frame = key_value_frame(&input, &[]);
	let p = UnverifiedFrameProvenance::new(
		&identity(1),
		&frame,
		vec![ProvenanceParent {
			manifest: m,
			commitment,
		}],
		vec![reference(0, 0, 0, 0)],
	)
	.unwrap();
	p.clone()
		.verify(
			&key(),
			&TrustedFrameRows::from_key_value_frame(frame.clone(), Some(schema)).unwrap(),
		)
		.unwrap();
	assert!(
		p.verify(
			&key(),
			&TrustedFrameRows::from_key_value_frame(frame, None).unwrap()
		)
		.is_err()
	);
}
#[test]
fn provenance_structural_golden_and_counts_lengths() {
	let g: serde_json::Value =
		serde_json::from_str(include_str!("fixtures/b1_goldens.json")).unwrap();
	let bytes = |name: &str| {
		let s = g["cases"]
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
			.collect::<Vec<_>>()
	};
	let payload = bytes("structural_provenance_payload");
	assert_eq!(payload.len(), 357);
	let encoded = crate::storage::mutation_visibility::encoding::envelope(
		b"VISPROV1",
		&payload,
		MAX_PROVENANCE_BYTES,
	)
	.unwrap();
	assert_eq!(&encoded[..14], bytes("structural_envelope_prefix"));
	assert_eq!(encoded.len(), 403);
	let decoded = UnverifiedFrameProvenance::decode(&encoded).unwrap();
	assert_eq!(
		decoded.parents()[0].manifest.encode().unwrap(),
		bytes("structural_manifest")
	);
	assert_eq!(decoded.encode().unwrap(), encoded);
	for (off, value) in [
		(14 + 85, 0u32.to_le_bytes().to_vec()),
		(14 + 85, 513u32.to_le_bytes().to_vec()),
		(14 + 89, 513u16.to_le_bytes().to_vec()),
		(14 + 91, 2u32.to_le_bytes().to_vec()),
		(14 + 49, 0u32.to_le_bytes().to_vec()),
		(
			14 + 49,
			(MAX_PROOF_FRAME_BYTES as u32 + 1).to_le_bytes().to_vec(),
		),
		(14 + 95, 65537u32.to_le_bytes().to_vec()),
	] {
		let mut bad = encoded.clone();
		overwrite_envelope(&mut bad, off, &value);
		assert!(UnverifiedFrameProvenance::decode(&bad).is_err())
	}
	assert!(UnverifiedFrameProvenance::decode(&vec![0; MAX_PROVENANCE_BYTES + 1]).is_err());
	let b = batch(vec![1]);
	let p = parent(1, &b, 1);
	assert!(
		UnverifiedFrameProvenance::new(
			&identity(1),
			&vec![0; MAX_PROOF_FRAME_BYTES + 1],
			vec![p.clone()],
			vec![reference(0, 0, 0, 0)]
		)
		.is_err()
	);
	let huge = vec![0; MAX_PROOF_FRAME_BYTES];
	UnverifiedFrameProvenance::new(&identity(1), &huge, vec![p], vec![reference(0, 0, 0, 0)])
		.unwrap();
}
#[test]
fn maximum_rows_children_and_dictionary_do_not_change_original_ordinals() {
	let b = batch((0..512).collect());
	let p = parent(1, &b, 256);
	let frame = framed(&b, crate::schema::RecordBodyCompressionPolicy::Uncompressed);
	let refs = (0..512)
		.map(|i| reference(i, 0, i % 256, i / 256))
		.collect();
	let provenance = UnverifiedFrameProvenance::new(&identity(1), &frame, vec![p], refs).unwrap();
	provenance
		.verify(
			&key(),
			&TrustedFrameRows::from_table_frame(frame, schema(&b)).unwrap(),
		)
		.unwrap();
	let b = batch((0..513).collect());
	let c = ProofChildInput {
		range_start: 0,
		range_end: u64::MAX,
		partition: None,
		input_rows: (0..513).collect(),
	};
	assert!(
		build_table_parent(
			&key(),
			identity(1),
			Admission {
				epoch: 1,
				holder: 1
			},
			manifest(1).binding(),
			MutationOperation::Insert,
			&schema(&b),
			&b,
			None,
			&[c]
		)
		.is_err()
	);
	let b = batch(vec![1]);
	let frame = framed(&b, crate::schema::RecordBodyCompressionPolicy::Uncompressed);
	let p = parent(1, &b, 1);
	assert!(
		UnverifiedFrameProvenance::new(
			&identity(1),
			&frame,
			vec![p; 513],
			vec![reference(0, 0, 0, 0)]
		)
		.is_err()
	);
}
