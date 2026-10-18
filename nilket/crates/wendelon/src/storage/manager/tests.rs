use super::compaction::*;
use super::encoding_compute::*;
use super::retention_reclaim::*;
use super::secondary_index_segments::*;
use super::stats_facts::*;
use super::tiered_io::*;
use super::*;
use crate::crypto::CryptoManager;
use crate::schema::table::{
	ColumnHeavyHitterSpec, ColumnRangeDistributionSpec, ColumnSmallSetSpec, ColumnStatsPolicy,
	LocalityPolicy, MultiRowLayoutDescriptor, PrimaryKeyDescriptor, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, RowIdentityPolicy, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN, SchemaEvolutionPolicy, TableObjectId, TableSchemaCatalog,
	TableSpreadLocalityDescriptor, default_kv_table_descriptor_v1, encode_table_write_row_count,
};
use crate::schema::{
	BufferValueIndex, EncodingPageIndex, IndexArrowTypeTag, IndexKeyEncoding, IndexObjectId,
	NodeId, RetentionWindowMillis, RowGroupRowCount, SecondaryIndexCatalog,
	SecondaryIndexKeyDescriptor, TableLifecycleCatalog, TableRetentionPolicy,
};
use crate::storage::EncodingPageDescriptor;
use crate::storage::io_uring::spawn_io_thread;
use crate::storage::l2::{LocalFsObjectStore, ObjectStore};
use crate::storage::multirow::pack_record_batch_by_bucket;
use crate::storage::secondary_index_backfill::SECONDARY_INDEX_BACKFILL_ATTEMPT_BUDGET;
use crate::storage::stats::{
	ARROW_STATS_TAG_FLOAT64, ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_LARGE_UTF8,
	ARROW_STATS_TAG_UTF8, COLUMN_STAT_HAS_GROUP_NDV, COLUMN_STAT_HAS_GROUP_TOP_K,
	COLUMN_STAT_HAS_HEAVY_HITTERS, COLUMN_STAT_HAS_HLL, COLUMN_STAT_HAS_MAX, COLUMN_STAT_HAS_MIN,
	COLUMN_STAT_HAS_NON_NULL_COUNT, COLUMN_STAT_HAS_NULL_COUNT, COLUMN_STAT_HAS_RANGE_DISTRIBUTION,
	COLUMN_STAT_HAS_SMALL_SET, COLUMN_STAT_MAX_TRUNCATED, COLUMN_STAT_MIN_TRUNCATED,
	ColumnStatsBlockEntry, ColumnStatsBlockHeader, build_always_on_column_stats_block,
	decode_column_stats_payload, encode_float64_stat_value, encode_length_prefixed_bytes,
	encode_signed_stat_value, parse_column_stats_block,
};
use crate::storage::{
	FaultAction, FaultOp, FaultyFileOps, IndexQuery, InjectedFault, LsmConfig, SecondaryIndexEntry,
	SecondaryIndexIdentity,
};
use arrow::array::{
	Array, BooleanArray, FixedSizeBinaryBuilder, Float64Array, Int64Array, LargeBinaryBuilder,
	LargeStringArray, StringArray, UInt8Array, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

fn fresh_dir(tag: &str) -> crate::test_support::TestDir {
	crate::test_support::TestDir::new(&format!("wendelon-manager-{tag}"))
}

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 1,
	}
}

fn id(n: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&n.to_le_bytes());
	Identity::new(bytes)
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

#[tokio::test]
async fn maintenance_staged_publication_recovery_distinguishes_s0_from_s1() {
	let dir = fresh_dir("maintenance-staged-recovery");
	let file_ops = BlockingFileOps;
	let old = SlabId::new(41);
	let replacement = SlabId::new(42);
	let staged = maintenance_staged_slab_path(&dir, old, replacement);
	std::fs::write(&staged, b"staged-replacement").unwrap();

	let old_run = LsmRunSummary::new(
		old,
		CompactionLevelIndex::L0,
		hlc(1),
		hlc(1),
		ByteCount::new(1),
		RowCount::new(1),
		ManifestEpoch::ZERO,
	)
	.with_state(SlabState::Retired {
		successors: vec![replacement],
	});
	let replacement_run = LsmRunSummary::new(
		replacement,
		CompactionLevelIndex::L0,
		hlc(1),
		hlc(1),
		ByteCount::new(1),
		RowCount::new(1),
		ManifestEpoch::ZERO,
	);
	reconcile_staged_maintenance_publications(
		&file_ops,
		&dir,
		Some(&[old_run, replacement_run]),
		None,
	)
	.await
	.unwrap();
	assert!(
		!staged.exists() && slab_path(&dir, replacement).exists(),
		"S1 must install the committed staged replacement before discovery"
	);

	let s0_replacement = SlabId::new(43);
	let s0 = maintenance_staged_slab_path(&dir, old, s0_replacement);
	std::fs::write(&s0, b"uncommitted-replacement").unwrap();
	reconcile_staged_maintenance_publications(&file_ops, &dir, None, None)
		.await
		.unwrap();
	assert!(
		!s0.exists() && !slab_path(&dir, s0_replacement).exists(),
		"S0 reaps only the invisible staged file; it must never create a final slab"
	);
	std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn maintenance_staged_install_retries_and_preserves_s1_for_recovery() {
	let dir = fresh_dir("maintenance-staged-install-retry");
	let old = SlabId::new(51);
	let replacement = SlabId::new(52);
	let staged = maintenance_staged_slab_path(&dir, old, replacement);
	let final_path = slab_path(&dir, replacement);
	let old_run = LsmRunSummary::new(
		old,
		CompactionLevelIndex::L0,
		hlc(1),
		hlc(1),
		ByteCount::new(1),
		RowCount::new(1),
		ManifestEpoch::ZERO,
	)
	.with_state(SlabState::Retired {
		successors: vec![replacement],
	});
	let replacement_run = LsmRunSummary::new(
		replacement,
		CompactionLevelIndex::L0,
		hlc(1),
		hlc(1),
		ByteCount::new(1),
		RowCount::new(1),
		ManifestEpoch::ZERO,
	);

	// One transient D1′ failure is retried by the shared S1 installer.
	std::fs::write(&staged, b"staged-replacement").unwrap();
	let transient = FaultyFileOps::new(&*dir).unwrap();
	transient.inject(
		InjectedFault::on(
			FaultOp::Rename,
			FaultAction::Error {
				kind: ErrorKind::Interrupted,
				message: "injected transient staged install failure".into(),
			},
		)
		.for_path(&final_path),
	);
	reconcile_staged_maintenance_publications(
		&transient,
		&dir,
		Some(&[old_run.clone(), replacement_run.clone()]),
		Some((old, replacement)),
	)
	.await
	.unwrap();
	assert!(final_path.exists() && !staged.exists());

	// Persistent D1′ failure never reclassifies S1 as an ordinary S0 cleanup:
	// the staged bytes remain for the identical startup reconciler to finish.
	std::fs::remove_file(&final_path).unwrap();
	std::fs::write(&staged, b"staged-replacement").unwrap();
	let persistent = FaultyFileOps::new(&*dir).unwrap();
	for _ in 0..3 {
		persistent.inject(
			InjectedFault::on(
				FaultOp::Rename,
				FaultAction::Error {
					kind: ErrorKind::WouldBlock,
					message: "injected persistent staged install failure".into(),
				},
			)
			.for_path(&final_path),
		);
	}
	let err = reconcile_staged_maintenance_publications(
		&persistent,
		&dir,
		Some(&[old_run.clone(), replacement_run.clone()]),
		Some((old, replacement)),
	)
	.await
	.expect_err("persistent D1′ failure must not be reported as a successful pass");
	assert!(err.to_string().contains("restart-required"));
	assert!(staged.exists() && !final_path.exists());

	reconcile_staged_maintenance_publications(
		&BlockingFileOps,
		&dir,
		Some(&[old_run, replacement_run]),
		Some((old, replacement)),
	)
	.await
	.unwrap();
	assert!(final_path.exists() && !staged.exists());
	std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn maintenance_publication_fail_stop_blocks_reads_until_restart() {
	let (_manager_dir, manager) = fresh_manager().await;
	let key = id(0x5A);
	manager
		.append_durable(tenant(), key, hlc(1), b"must-not-serve-half-published")
		.await
		.unwrap();
	let location = manager.lookup(tenant(), key).unwrap();
	let publication_gate = manager
		.maintenance_publication_gate
		.clone()
		.write_owned()
		.await;
	manager.enter_maintenance_publication_fail_stop(publication_gate);
	let error = manager
		.read(location)
		.await
		.expect_err("D1′ exhaustion must fail-stop local reads until restart reconciliation");
	assert!(error.to_string().contains("restart reconciliation"));
}

fn test_crypto() -> Arc<CryptoManager> {
	let crypto = Arc::new(CryptoManager::new_dev());
	crypto.install_local_only_for_tests(tenant()).unwrap();
	crypto
}

fn optimizer_fact_stats_header(column_block_count: u32) -> ColumnStatsBlockHeader {
	ColumnStatsBlockHeader {
		table_object_id: TableObjectId::new([0x42; 16]),
		schema_generation: 7,
		logical_schema_fingerprint: LogicalSchemaFingerprint::from_bytes([0x51; 32]),
		row_count: 1,
		column_block_count,
	}
}

fn optimizer_fact_block(columns: Vec<ColumnStatsBlockEntry>) -> ParsedColumnStatsBlock {
	ParsedColumnStatsBlock {
		header: optimizer_fact_stats_header(columns.len() as u32),
		columns,
	}
}

fn optimizer_fact_block_with_row_count(row_count: u64) -> ParsedColumnStatsBlock {
	let mut block = optimizer_fact_block(Vec::new());
	block.header.row_count = row_count;
	block
}

fn optimizer_fact_small_set_entry(values: &[&[u8]]) -> ColumnStatsBlockEntry {
	let mut payload = Vec::new();
	payload.extend_from_slice(&0_u64.to_be_bytes());
	payload.extend_from_slice(&(values.len() as u64).to_be_bytes());
	payload.extend_from_slice(&(values.len() as u32).to_be_bytes());
	for value in values {
		encode_length_prefixed_bytes(&mut payload, *value).expect("encode small-set value");
	}
	ColumnStatsBlockEntry {
		column_ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_INT64,
		stat_flags: COLUMN_STAT_HAS_NULL_COUNT
			| COLUMN_STAT_HAS_NON_NULL_COUNT
			| COLUMN_STAT_HAS_SMALL_SET,
		payload,
	}
}

fn optimizer_fact_hll_entry(precision: u8, values: &[&[u8]]) -> ColumnStatsBlockEntry {
	let precision = HllPrecision::new(precision).expect("test HLL precision");
	let mut hll = HllRegisters::new(precision);
	for value in values {
		hll.insert(*value);
	}
	let mut payload = Vec::new();
	payload.extend_from_slice(&0_u64.to_be_bytes());
	payload.extend_from_slice(&(values.len() as u64).to_be_bytes());
	payload.push(precision.get());
	payload.extend_from_slice(hll.registers());
	ColumnStatsBlockEntry {
		column_ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_INT64,
		stat_flags: COLUMN_STAT_HAS_NULL_COUNT
			| COLUMN_STAT_HAS_NON_NULL_COUNT
			| COLUMN_STAT_HAS_HLL,
		payload,
	}
}

fn optimizer_fact_heavy_hitter_entry(
	complete: bool,
	entries: &[(&[u8], u64)],
	non_null_count: u64,
) -> ColumnStatsBlockEntry {
	let mut payload = Vec::new();
	payload.extend_from_slice(&0_u64.to_be_bytes());
	payload.extend_from_slice(&non_null_count.to_be_bytes());
	payload.push(u8::from(complete));
	payload.extend_from_slice(&(entries.len() as u32).to_be_bytes());
	for (value, frequency) in entries {
		encode_length_prefixed_bytes(&mut payload, value).expect("encode heavy-hitter value");
		payload.extend_from_slice(&frequency.to_be_bytes());
	}
	ColumnStatsBlockEntry {
		column_ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_UTF8,
		stat_flags: COLUMN_STAT_HAS_NULL_COUNT
			| COLUMN_STAT_HAS_NON_NULL_COUNT
			| COLUMN_STAT_HAS_HEAVY_HITTERS,
		payload,
	}
}

fn optimizer_fact_group_top_k_entry(
	column_ordinals: &[u16],
	arrow_type_tags: &[u8],
	complete: bool,
	entries: &[(Vec<Vec<u8>>, u64)],
) -> ColumnStatsBlockEntry {
	let anchor = column_ordinals
		.iter()
		.min()
		.copied()
		.expect("group top-k anchor");
	let anchor_tag = column_ordinals
		.iter()
		.position(|ordinal| *ordinal == anchor)
		.and_then(|index| arrow_type_tags.get(index))
		.copied()
		.expect("group top-k anchor tag");
	let non_null_count = entries.iter().map(|(_, frequency)| *frequency).sum::<u64>();
	let mut sorted_entries = entries.to_vec();
	sorted_entries.sort_by(|(left, _), (right, _)| left.cmp(right));
	let mut payload = Vec::new();
	payload.extend_from_slice(&0_u64.to_be_bytes());
	payload.extend_from_slice(&non_null_count.to_be_bytes());
	payload.push(u8::from(complete));
	payload.extend_from_slice(&(column_ordinals.len() as u32).to_be_bytes());
	for (ordinal, tag) in column_ordinals.iter().zip(arrow_type_tags) {
		payload.extend_from_slice(&ordinal.to_be_bytes());
		payload.push(*tag);
	}
	payload.extend_from_slice(&(entries.len() as u32).to_be_bytes());
	for (value_keys, frequency) in &sorted_entries {
		for value_key in value_keys {
			encode_length_prefixed_bytes(&mut payload, value_key)
				.expect("encode group top-k value");
		}
		payload.extend_from_slice(&frequency.to_be_bytes());
	}
	ColumnStatsBlockEntry {
		column_ordinal: anchor,
		arrow_type_tag: anchor_tag,
		stat_flags: COLUMN_STAT_HAS_NULL_COUNT
			| COLUMN_STAT_HAS_NON_NULL_COUNT
			| COLUMN_STAT_HAS_GROUP_TOP_K,
		payload,
	}
}

fn optimizer_fact_group_ndv_entry(
	column_ordinals: &[u16],
	arrow_type_tags: &[u8],
	precision: u8,
	exact_tuples: Option<&[Vec<Vec<u8>>]>,
	hll_tuples: &[Vec<Vec<u8>>],
) -> ColumnStatsBlockEntry {
	let anchor = column_ordinals
		.iter()
		.min()
		.copied()
		.expect("group NDV anchor");
	let anchor_tag = column_ordinals
		.iter()
		.position(|ordinal| *ordinal == anchor)
		.and_then(|index| arrow_type_tags.get(index))
		.copied()
		.expect("group NDV anchor tag");
	let precision = HllPrecision::new(precision).expect("test HLL precision");
	let mut hll = HllRegisters::new(precision);
	let hll_tuple_keys = hll_tuples
		.iter()
		.map(|tuple| group_ndv_tuple_key_for_test(tuple))
		.collect::<Vec<_>>();
	for tuple_key in &hll_tuple_keys {
		hll.insert(tuple_key);
	}
	let mut payload = Vec::new();
	payload.extend_from_slice(&0_u64.to_be_bytes());
	payload.extend_from_slice(&(hll_tuples.len() as u64).to_be_bytes());
	payload.extend_from_slice(&(column_ordinals.len() as u32).to_be_bytes());
	for (ordinal, tag) in column_ordinals.iter().zip(arrow_type_tags) {
		payload.extend_from_slice(&ordinal.to_be_bytes());
		payload.push(*tag);
	}
	match exact_tuples {
		Some(exact_tuples) => {
			payload.push(1);
			let mut exact_values = exact_tuples
				.iter()
				.map(|tuple| group_ndv_tuple_key_for_test(tuple))
				.collect::<Vec<_>>();
			exact_values.sort();
			exact_values.dedup();
			payload.extend_from_slice(&(exact_values.len() as u32).to_be_bytes());
			for value in exact_values {
				encode_length_prefixed_bytes(&mut payload, &value)
					.expect("encode group NDV exact value");
			}
		}
		None => payload.push(0),
	}
	payload.push(precision.get());
	payload.extend_from_slice(hll.registers());
	ColumnStatsBlockEntry {
		column_ordinal: anchor,
		arrow_type_tag: anchor_tag,
		stat_flags: COLUMN_STAT_HAS_NULL_COUNT
			| COLUMN_STAT_HAS_NON_NULL_COUNT
			| COLUMN_STAT_HAS_GROUP_NDV,
		payload,
	}
}

fn group_ndv_tuple_key_for_test(value_keys: &[Vec<u8>]) -> Vec<u8> {
	let mut encoded = Vec::new();
	encoded.extend_from_slice(&(value_keys.len() as u32).to_be_bytes());
	for value_key in value_keys {
		encode_length_prefixed_bytes(&mut encoded, value_key)
			.expect("encode group NDV tuple component");
	}
	encoded
}

fn optimizer_fact_range_entry(
	column_ordinal: u16,
	arrow_type_tag: u8,
	null_count: u64,
	values: &[i64],
) -> ColumnStatsBlockEntry {
	let mut payload = Vec::new();
	if let Some(min) = values.iter().min() {
		encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(*min, 8).unwrap())
			.unwrap();
	}
	if let Some(max) = values.iter().max() {
		encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(*max, 8).unwrap())
			.unwrap();
	}
	payload.extend_from_slice(&null_count.to_be_bytes());
	payload.extend_from_slice(&(values.len() as u64).to_be_bytes());
	let mut stat_flags = COLUMN_STAT_HAS_NULL_COUNT | COLUMN_STAT_HAS_NON_NULL_COUNT;
	if !values.is_empty() {
		stat_flags |= COLUMN_STAT_HAS_MIN | COLUMN_STAT_HAS_MAX;
	}
	ColumnStatsBlockEntry {
		column_ordinal,
		arrow_type_tag,
		stat_flags,
		payload,
	}
}

fn optimizer_fact_truncated_utf8_range_entry() -> ColumnStatsBlockEntry {
	let mut payload = Vec::new();
	encode_length_prefixed_bytes(&mut payload, b"abc").unwrap();
	encode_length_prefixed_bytes(&mut payload, b"zzz").unwrap();
	payload.extend_from_slice(&0_u64.to_be_bytes());
	payload.extend_from_slice(&1_u64.to_be_bytes());
	ColumnStatsBlockEntry {
		column_ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
		stat_flags: COLUMN_STAT_HAS_MIN
			| COLUMN_STAT_HAS_MAX
			| COLUMN_STAT_HAS_NULL_COUNT
			| COLUMN_STAT_HAS_NON_NULL_COUNT
			| COLUMN_STAT_MIN_TRUNCATED
			| COLUMN_STAT_MAX_TRUNCATED,
		payload,
	}
}

#[test]
fn complete_small_set_produces_exact_ndv_fact() {
	let candidate = NdvColumnCandidate {
		ordinal: 1,
		small_set_max_values: Some(8),
		small_set_max_bytes: Some(128),
		hll_enabled: false,
	};
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_small_set_entry(&[
			b"east".as_slice(),
			b"west".as_slice(),
		])]),
		optimizer_fact_block(vec![optimizer_fact_small_set_entry(&[
			b"east".as_slice(),
			b"north".as_slice(),
		])]),
	];

	assert_eq!(
		ndv_fact_for_column(&candidate, &blocks).expect("small-set NDV"),
		Some(NdvFact::Exact { count: 3 })
	);
}

#[test]
fn compatible_hll_contributors_produce_approx_ndv_fact() {
	let candidate = NdvColumnCandidate {
		ordinal: 1,
		small_set_max_values: None,
		small_set_max_bytes: None,
		hll_enabled: true,
	};
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_hll_entry(
			10,
			&[b"east".as_slice(), b"west".as_slice()],
		)]),
		optimizer_fact_block(vec![optimizer_fact_hll_entry(
			10,
			&[b"north".as_slice(), b"south".as_slice()],
		)]),
	];

	match ndv_fact_for_column(&candidate, &blocks).expect("HLL NDV") {
		Some(NdvFact::Approx {
			estimate,
			rel_err_ppm,
		}) => {
			assert!(estimate > 0);
			assert!(rel_err_ppm > 0);
		}
		other => panic!("expected approximate HLL NDV fact, got {other:?}"),
	}
}

#[test]
fn partial_contributor_coverage_emits_no_ndv_fact() {
	let candidate = NdvColumnCandidate {
		ordinal: 1,
		small_set_max_values: Some(8),
		small_set_max_bytes: Some(128),
		hll_enabled: false,
	};
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_small_set_entry(&[b"east".as_slice()])]),
		optimizer_fact_block(Vec::new()),
	];

	assert_eq!(
		ndv_fact_for_column(&candidate, &blocks).expect("partial NDV"),
		None
	);
}

#[test]
fn incompatible_hll_precision_emits_no_ndv_fact() {
	let candidate = NdvColumnCandidate {
		ordinal: 1,
		small_set_max_values: None,
		small_set_max_bytes: None,
		hll_enabled: true,
	};
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_hll_entry(10, &[b"east".as_slice()])]),
		optimizer_fact_block(vec![optimizer_fact_hll_entry(11, &[b"west".as_slice()])]),
	];

	assert_eq!(
		ndv_fact_for_column(&candidate, &blocks).expect("incompatible HLL NDV"),
		None
	);
}

#[test]
fn range_fact_merges_counts_min_and_max() {
	let candidate = RangeColumnCandidate {
		ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_INT64,
	};
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_range_entry(
			1,
			ARROW_STATS_TAG_INT64,
			1,
			&[10, 20],
		)]),
		optimizer_fact_block(vec![optimizer_fact_range_entry(
			1,
			ARROW_STATS_TAG_INT64,
			2,
			&[-5, 40],
		)]),
	];

	let fact = range_fact_for_column(&candidate, &blocks)
		.expect("range fact")
		.expect("complete range fact");
	assert_eq!(fact.column_ordinal, 1);
	assert_eq!(fact.arrow_type_tag, ARROW_STATS_TAG_INT64);
	assert_eq!(fact.null_count, 3);
	assert_eq!(fact.non_null_count, 4);
	assert_eq!(fact.min, Some(encode_signed_stat_value(-5, 8).unwrap()));
	assert_eq!(fact.max, Some(encode_signed_stat_value(40, 8).unwrap()));
}

#[test]
fn range_fact_all_null_has_no_min_max() {
	let candidate = RangeColumnCandidate {
		ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_INT64,
	};
	let blocks = vec![optimizer_fact_block(vec![optimizer_fact_range_entry(
		1,
		ARROW_STATS_TAG_INT64,
		3,
		&[],
	)])];

	let fact = range_fact_for_column(&candidate, &blocks)
		.expect("range fact")
		.expect("all-null range fact");
	assert_eq!(fact.null_count, 3);
	assert_eq!(fact.non_null_count, 0);
	assert_eq!(fact.min, None);
	assert_eq!(fact.max, None);
}

#[test]
fn range_truncated_min_max_omits_fact() {
	let candidate = RangeColumnCandidate {
		ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
	};
	let blocks = vec![optimizer_fact_block(vec![
		optimizer_fact_truncated_utf8_range_entry(),
	])];

	assert_eq!(
		range_fact_for_column(&candidate, &blocks).expect("truncated range fact"),
		None
	);
}

#[test]
fn heavy_hitter_fact_merges_exact_frequencies_per_key() {
	let candidate = HeavyHitterColumnCandidate {
		ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_UTF8,
		max_entries: 8,
		max_bytes: 256,
	};
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_heavy_hitter_entry(
			true,
			&[(b"cold".as_slice(), 1), (b"hot".as_slice(), 3)],
			4,
		)]),
		optimizer_fact_block(vec![optimizer_fact_heavy_hitter_entry(
			true,
			&[(b"hot".as_slice(), 2), (b"warm".as_slice(), 1)],
			3,
		)]),
	];

	let fact = heavy_hitter_fact_for_column(&candidate, &blocks)
		.expect("heavy-hitter fact")
		.expect("complete heavy-hitter fact");
	assert_eq!(fact.column_ordinal, 1);
	assert_eq!(fact.arrow_type_tag, ARROW_STATS_TAG_UTF8);
	assert_eq!(
		fact.entries,
		vec![
			ColumnHeavyHitterEntry {
				value_key: b"cold".to_vec(),
				frequency: 1,
			},
			ColumnHeavyHitterEntry {
				value_key: b"hot".to_vec(),
				frequency: 5,
			},
			ColumnHeavyHitterEntry {
				value_key: b"warm".to_vec(),
				frequency: 1,
			},
		]
	);
}

#[test]
fn column_group_top_k_merges_across_two_slabs() {
	let candidate = ColumnGroupTopKCandidate {
		column_ordinals: vec![1, 2],
		arrow_type_tags: vec![ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
		max_entries: 8,
		max_bytes: 512,
	};
	let hot = vec![
		encode_signed_stat_value(7, 8).unwrap(),
		encode_signed_stat_value(2, 8).unwrap(),
	];
	let cold = vec![
		encode_signed_stat_value(8, 8).unwrap(),
		encode_signed_stat_value(2, 8).unwrap(),
	];
	let warm = vec![
		encode_signed_stat_value(7, 8).unwrap(),
		encode_signed_stat_value(3, 8).unwrap(),
	];
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_group_top_k_entry(
			&[1, 2],
			&[ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
			true,
			&[(cold.clone(), 1), (hot.clone(), 3)],
		)]),
		optimizer_fact_block(vec![optimizer_fact_group_top_k_entry(
			&[1, 2],
			&[ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
			true,
			&[(hot.clone(), 2), (warm.clone(), 1)],
		)]),
	];

	let fact = column_group_top_k_fact_for_group(&candidate, &blocks)
		.expect("group top-k fact")
		.expect("complete group top-k fact");
	assert_eq!(fact.column_ordinals, vec![1, 2]);
	assert_eq!(
		fact.arrow_type_tags,
		vec![ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64]
	);
	assert_eq!(
		fact.entries,
		vec![
			ColumnGroupTopKEntry {
				value_keys: hot,
				frequency: 5,
			},
			ColumnGroupTopKEntry {
				value_keys: warm,
				frequency: 1,
			},
			ColumnGroupTopKEntry {
				value_keys: cold,
				frequency: 1,
			},
		]
	);
}

#[test]
fn column_group_top_k_declines_on_partial_coverage() {
	let candidate = ColumnGroupTopKCandidate {
		column_ordinals: vec![1, 2],
		arrow_type_tags: vec![ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
		max_entries: 8,
		max_bytes: 512,
	};
	let hot = vec![
		encode_signed_stat_value(7, 8).unwrap(),
		encode_signed_stat_value(2, 8).unwrap(),
	];
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_group_top_k_entry(
			&[1, 2],
			&[ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
			true,
			&[(hot, 3)],
		)]),
		optimizer_fact_block(Vec::new()),
	];

	assert_eq!(
		column_group_top_k_fact_for_group(&candidate, &blocks)
			.expect("group top-k partial coverage"),
		None
	);
}

#[test]
fn column_group_ndv_merges_exact_tuple_sets_across_two_slabs() {
	let candidate = ColumnGroupNdvCandidate {
		column_ordinals: vec![1, 2],
		arrow_type_tags: vec![ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
		precision: HllPrecision::new(10).unwrap(),
		exact_max_distinct_values: 8,
		exact_max_bytes: 1024,
	};
	let hot = vec![
		encode_signed_stat_value(7, 8).unwrap(),
		encode_signed_stat_value(2, 8).unwrap(),
	];
	let cold = vec![
		encode_signed_stat_value(8, 8).unwrap(),
		encode_signed_stat_value(2, 8).unwrap(),
	];
	let warm = vec![
		encode_signed_stat_value(7, 8).unwrap(),
		encode_signed_stat_value(3, 8).unwrap(),
	];
	let first = vec![hot.clone(), cold.clone()];
	let second = vec![hot, warm];
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_group_ndv_entry(
			&[1, 2],
			&[ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
			10,
			Some(&first),
			&first,
		)]),
		optimizer_fact_block(vec![optimizer_fact_group_ndv_entry(
			&[1, 2],
			&[ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
			10,
			Some(&second),
			&second,
		)]),
	];

	let fact = column_group_ndv_fact_for_group(&candidate, &blocks)
		.expect("group NDV fact")
		.expect("complete group NDV fact");
	assert_eq!(fact.column_ordinals, vec![1, 2]);
	assert_eq!(fact.distinct, NdvFact::Exact { count: 3 });
}

#[test]
fn column_group_ndv_declines_on_partial_coverage() {
	let candidate = ColumnGroupNdvCandidate {
		column_ordinals: vec![1, 2],
		arrow_type_tags: vec![ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
		precision: HllPrecision::new(10).unwrap(),
		exact_max_distinct_values: 8,
		exact_max_bytes: 1024,
	};
	let hot = vec![
		encode_signed_stat_value(7, 8).unwrap(),
		encode_signed_stat_value(2, 8).unwrap(),
	];
	let tuples = vec![hot];
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_group_ndv_entry(
			&[1, 2],
			&[ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
			10,
			Some(&tuples),
			&tuples,
		)]),
		optimizer_fact_block(Vec::new()),
	];

	assert_eq!(
		column_group_ndv_fact_for_group(&candidate, &blocks).expect("group NDV partial coverage"),
		None
	);
}

#[test]
fn column_group_ndv_merges_hll_when_exact_sets_are_absent() {
	let candidate = ColumnGroupNdvCandidate {
		column_ordinals: vec![1, 2],
		arrow_type_tags: vec![ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
		precision: HllPrecision::new(10).unwrap(),
		exact_max_distinct_values: 1,
		exact_max_bytes: 1,
	};
	let first = vec![
		vec![
			encode_signed_stat_value(1, 8).unwrap(),
			encode_signed_stat_value(1, 8).unwrap(),
		],
		vec![
			encode_signed_stat_value(2, 8).unwrap(),
			encode_signed_stat_value(1, 8).unwrap(),
		],
	];
	let second = vec![vec![
		encode_signed_stat_value(3, 8).unwrap(),
		encode_signed_stat_value(1, 8).unwrap(),
	]];
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_group_ndv_entry(
			&[1, 2],
			&[ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
			10,
			None,
			&first,
		)]),
		optimizer_fact_block(vec![optimizer_fact_group_ndv_entry(
			&[1, 2],
			&[ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_INT64],
			10,
			None,
			&second,
		)]),
	];

	match column_group_ndv_fact_for_group(&candidate, &blocks).expect("group NDV HLL") {
		Some(ColumnGroupNdvFact {
			column_ordinals,
			distinct: NdvFact::Approx {
				estimate,
				rel_err_ppm,
			},
		}) => {
			assert_eq!(column_ordinals, vec![1, 2]);
			assert!(estimate > 0);
			assert!(rel_err_ppm > 0);
		}
		other => panic!("expected approximate group NDV fact, got {other:?}"),
	}
}

#[test]
fn incomplete_contributor_topk_absence_is_unknown_not_zero() {
	let candidate = HeavyHitterColumnCandidate {
		ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_UTF8,
		max_entries: 8,
		max_bytes: 256,
	};
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_heavy_hitter_entry(
			false,
			&[(b"hot".as_slice(), 3)],
			10,
		)]),
		optimizer_fact_block(vec![optimizer_fact_heavy_hitter_entry(
			true,
			&[(b"cold".as_slice(), 2), (b"hot".as_slice(), 1)],
			3,
		)]),
	];

	let fact = heavy_hitter_fact_for_column(&candidate, &blocks)
		.expect("heavy-hitter fact")
		.expect("hot key remains exact");
	assert_eq!(
		fact.entries,
		vec![ColumnHeavyHitterEntry {
			value_key: b"hot".to_vec(),
			frequency: 4,
		}]
	);
}

#[test]
fn table_heavy_hitter_fact_respects_max_entries_and_max_bytes() {
	let candidate = HeavyHitterColumnCandidate {
		ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_UTF8,
		max_entries: 1,
		max_bytes: 32,
	};
	let blocks = vec![optimizer_fact_block(vec![
		optimizer_fact_heavy_hitter_entry(
			true,
			&[
				(b"cold".as_slice(), 1),
				(b"hot".as_slice(), 9),
				(b"warm".as_slice(), 3),
			],
			13,
		),
	])];

	let fact = heavy_hitter_fact_for_column(&candidate, &blocks)
		.expect("heavy-hitter fact")
		.expect("capped heavy-hitter fact");
	assert_eq!(
		fact.entries,
		vec![ColumnHeavyHitterEntry {
			value_key: b"hot".to_vec(),
			frequency: 9,
		}]
	);
}

#[test]
fn partial_contributor_coverage_suppresses_heavy_hitter_fact() {
	let candidate = HeavyHitterColumnCandidate {
		ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_UTF8,
		max_entries: 8,
		max_bytes: 256,
	};
	let blocks = vec![
		optimizer_fact_block(vec![optimizer_fact_heavy_hitter_entry(
			true,
			&[(b"hot".as_slice(), 3)],
			3,
		)]),
		optimizer_fact_block(Vec::new()),
	];

	assert_eq!(
		heavy_hitter_fact_for_column(&candidate, &blocks).expect("heavy-hitter fact"),
		None
	);
}

#[test]
fn partial_contributor_coverage_suppresses_range_distribution_fact() {
	let descriptor = optimizer_fact_amount_range_distribution_descriptor(100);
	let candidate = RangeDistributionColumnCandidate {
		ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_INT64,
	};
	let blocks = vec![
		optimizer_fact_range_distribution_block(
			&descriptor,
			&optimizer_fact_amount_batch(&[(1, Some(10)), (2, Some(20))]),
		),
		optimizer_fact_block(Vec::new()),
	];

	assert_eq!(
		range_distribution_fact_for_column(&candidate, &blocks).expect("range-distribution fact"),
		None
	);
}

#[test]
fn adr083_v1_range_distribution_payload_does_not_produce_optimizer_fact() {
	let descriptor = optimizer_fact_amount_range_distribution_descriptor(100);
	let candidate = RangeDistributionColumnCandidate {
		ordinal: 1,
		arrow_type_tag: ARROW_STATS_TAG_INT64,
	};
	let blocks = vec![optimizer_fact_range_distribution_block(
		&descriptor,
		&optimizer_fact_amount_batch(&[
			(1, Some(0)),
			(2, Some(10)),
			(3, Some(20)),
			(4, Some(30)),
			(5, Some(40)),
		]),
	)];

	assert_eq!(
		range_distribution_fact_for_column(&candidate, &blocks)
			.expect("range-distribution fact decision"),
		None,
	);
}

#[test]
fn row_upper_bound_saturates_on_overflow() {
	let blocks = vec![
		optimizer_fact_block_with_row_count(u64::MAX - 1),
		optimizer_fact_block_with_row_count(7),
	];

	assert_eq!(row_upper_bound_for_blocks(&blocks), u64::MAX);
}

#[tokio::test]
async fn table_optimizer_facts_merges_tiered_small_sets_into_exact_ndv() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-exact").await;
	let descriptor = optimizer_fact_region_descriptor();
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_region_batch(&[(1, Some("east")), (2, Some("west"))]),
		"optimizer-facts-exact-a",
		10,
	)
	.await;
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_region_batch(&[(3, Some("east")), (4, Some("north"))]),
		"optimizer-facts-exact-b",
		20,
	)
	.await;

	let facts = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("covered tiered facts");

	assert_eq!(facts.table_object_id, descriptor.table_object_id);
	assert_eq!(facts.schema_generation, descriptor.schema_generation);
	assert_eq!(
		facts.physical_descriptor_fingerprint,
		descriptor.physical_descriptor_fingerprint
	);
	assert_eq!(facts.column_ndv.len(), 1);
	assert_eq!(facts.column_ndv[0].column_ordinal, 1);
	assert_eq!(facts.column_ndv[0].distinct, NdvFact::Exact { count: 3 });
	assert_eq!(facts.row_upper_bound, Some(4));
}

#[tokio::test]
async fn table_optimizer_facts_merges_tiered_heavy_hitters_into_exact_facts() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-heavy-hitter").await;
	let descriptor = optimizer_fact_utf8_region_heavy_hitter_descriptor();
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_utf8_region_batch(&[(1, Some("hot")), (2, Some("cold")), (3, Some("hot"))]),
		"optimizer-facts-heavy-hitter-a",
		10,
	)
	.await;
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_utf8_region_batch(&[(4, Some("warm")), (5, Some("hot"))]),
		"optimizer-facts-heavy-hitter-b",
		20,
	)
	.await;

	let facts = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("covered heavy-hitter facts");

	assert_eq!(facts.row_upper_bound, Some(5));
	assert_eq!(facts.column_heavy_hitters.len(), 1);
	let fact = &facts.column_heavy_hitters[0];
	assert_eq!(fact.column_ordinal, 1);
	assert_eq!(fact.arrow_type_tag, ARROW_STATS_TAG_UTF8);
	assert_eq!(
		fact.entries,
		vec![
			ColumnHeavyHitterEntry {
				value_key: b"cold".to_vec(),
				frequency: 1,
			},
			ColumnHeavyHitterEntry {
				value_key: b"hot".to_vec(),
				frequency: 3,
			},
			ColumnHeavyHitterEntry {
				value_key: b"warm".to_vec(),
				frequency: 1,
			},
		]
	);
}

#[tokio::test]
async fn adr083_table_optimizer_facts_suppresses_v1_range_distributions() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-range-distribution").await;
	let descriptor = optimizer_fact_amount_range_distribution_descriptor(100);
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_amount_batch(&[(1, Some(0)), (2, Some(0)), (3, Some(50)), (4, Some(100))]),
		"optimizer-facts-range-distribution-a",
		10,
	)
	.await;
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_amount_batch(&[(5, Some(0)), (6, Some(75)), (7, Some(10_000)), (8, None)]),
		"optimizer-facts-range-distribution-b",
		20,
	)
	.await;

	let facts = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("covered range-distribution facts");

	assert_eq!(facts.row_upper_bound, Some(8));
	assert!(facts.column_range_distributions.is_empty());
}

#[tokio::test]
async fn table_optimizer_facts_emits_row_bound_for_candidate_less_table() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-stat-less-row").await;
	let descriptor = optimizer_fact_candidate_less_descriptor();
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_candidate_less_batch(&["new", "paid"]),
		"optimizer-facts-stat-less-row-a",
		10,
	)
	.await;
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_candidate_less_batch(&["shipped", "settled", "closed"]),
		"optimizer-facts-stat-less-row-b",
		20,
	)
	.await;

	let facts = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("covered row facts");

	assert_eq!(facts.row_upper_bound, Some(5));
	assert!(facts.column_ndv.is_empty());
	assert!(facts.column_range.is_empty());
}

#[tokio::test]
async fn table_optimizer_facts_known_empty_emits_zero_row_bound() {
	let (_mgr_dirs, mgr, _l2) = fresh_manager_with_l2("optimizer-facts-known-empty").await;
	let descriptor = optimizer_fact_candidate_less_descriptor();

	let facts = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("known-empty row fact");

	assert_eq!(facts.row_upper_bound, Some(0));
	assert!(facts.column_ndv.is_empty());
	assert!(facts.column_range.is_empty());
}

#[tokio::test]
async fn table_optimizer_facts_missing_tiered_stats_suppresses_row_bound() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-missing-row").await;
	let descriptor = optimizer_fact_candidate_less_descriptor();
	let slab_id = tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_candidate_less_batch(&["new", "paid"]),
		"optimizer-facts-missing-row",
		10,
	)
	.await;
	let mut pointer = mgr.tier_pointer(slab_id).await.expect("tier pointer");
	pointer.stats_format_version = None;
	pointer.stats_block_len = None;
	pointer.stats_block_hash128 = None;
	mgr.apply_tier_slab(pointer).await.unwrap();

	assert_eq!(
		mgr.table_optimizer_facts(tenant(), &descriptor)
			.await
			.unwrap(),
		None
	);
}

#[tokio::test]
async fn range_fact_with_empty_stats_policy_is_produced_from_min_max() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-range-empty-policy").await;
	let descriptor = optimizer_fact_amount_descriptor();
	assert_eq!(descriptor.stats_policy, ColumnStatsPolicy::default());
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_amount_batch(&[(1, Some(10)), (2, Some(20))]),
		"optimizer-facts-range-empty-policy-a",
		10,
	)
	.await;
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_amount_batch(&[(3, Some(-5)), (4, None), (5, Some(40))]),
		"optimizer-facts-range-empty-policy-b",
		20,
	)
	.await;

	let facts = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("covered range facts");

	assert!(facts.column_ndv.is_empty());
	let amount = facts
		.column_range
		.iter()
		.find(|fact| fact.column_ordinal == 1)
		.expect("amount range fact");
	assert_eq!(facts.row_upper_bound, Some(5));
	assert_eq!(amount.arrow_type_tag, ARROW_STATS_TAG_INT64);
	assert_eq!(amount.null_count, 1);
	assert_eq!(amount.non_null_count, 4);
	assert_eq!(amount.min, Some(encode_signed_stat_value(-5, 8).unwrap()));
	assert_eq!(amount.max, Some(encode_signed_stat_value(40, 8).unwrap()));
}

#[tokio::test]
async fn table_optimizer_facts_emits_float_range_for_finite_values() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-float-range").await;
	let descriptor = optimizer_fact_score_descriptor();
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_score_batch(&[(1, Some(0.25)), (2, Some(0.75))]),
		"optimizer-facts-float-range-a",
		10,
	)
	.await;
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_score_batch(&[(3, Some(-0.5)), (4, None), (5, Some(1.0))]),
		"optimizer-facts-float-range-b",
		20,
	)
	.await;

	let facts = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("covered float range facts");

	let score = facts
		.column_range
		.iter()
		.find(|fact| fact.column_ordinal == 1)
		.expect("score range fact");
	assert_eq!(facts.row_upper_bound, Some(5));
	assert_eq!(score.arrow_type_tag, ARROW_STATS_TAG_FLOAT64);
	assert_eq!(score.null_count, 1);
	assert_eq!(score.non_null_count, 4);
	assert_eq!(score.min, Some(encode_float64_stat_value(-0.5).unwrap()));
	assert_eq!(score.max, Some(encode_float64_stat_value(1.0).unwrap()));
}

#[tokio::test]
async fn table_optimizer_facts_suppresses_float_range_with_nan_or_infinity() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-float-nonfinite").await;
	let descriptor = optimizer_fact_score_descriptor();
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_score_batch(&[(1, Some(0.25)), (2, Some(f64::NAN))]),
		"optimizer-facts-float-nonfinite-a",
		10,
	)
	.await;

	let facts = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("covered float range facts");

	assert!(
		facts
			.column_range
			.iter()
			.all(|fact| fact.column_ordinal != 1),
		"non-finite score column must not produce a range fact"
	);
	assert_eq!(facts.row_upper_bound, Some(2));
}

#[tokio::test]
async fn active_l1_without_stats_makes_ndv_unknown() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-active-l1").await;
	let descriptor = optimizer_fact_region_descriptor();
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_region_batch(&[(1, Some("east")), (2, Some("west"))]),
		"optimizer-facts-active-l1",
		10,
	)
	.await;

	let covered = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("covered tiered facts before active append");
	assert_eq!(covered.column_ndv[0].distinct, NdvFact::Exact { count: 2 });
	assert_eq!(covered.row_upper_bound, Some(2));

	append_optimizer_fact_rows(
		&mgr,
		&descriptor,
		&optimizer_fact_region_batch(&[(3, Some("south"))]),
		30,
	)
	.await;

	assert_eq!(
		mgr.table_optimizer_facts(tenant(), &descriptor)
			.await
			.unwrap(),
		None
	);
}

#[tokio::test]
async fn range_fact_requires_full_contributor_coverage() {
	let (_mgr_dirs, mgr, l2) = fresh_manager_with_l2("optimizer-facts-range-active-l1").await;
	let descriptor = optimizer_fact_amount_descriptor();
	tier_optimizer_fact_batch(
		&mgr,
		&l2,
		&descriptor,
		&optimizer_fact_amount_batch(&[(1, Some(10)), (2, Some(20))]),
		"optimizer-facts-range-active-l1",
		10,
	)
	.await;

	let covered = mgr
		.table_optimizer_facts(tenant(), &descriptor)
		.await
		.unwrap()
		.expect("covered range facts before active append");
	assert!(!covered.column_range.is_empty());
	assert_eq!(covered.row_upper_bound, Some(2));

	append_optimizer_fact_rows(
		&mgr,
		&descriptor,
		&optimizer_fact_amount_batch(&[(3, Some(30))]),
		30,
	)
	.await;

	assert_eq!(
		mgr.table_optimizer_facts(tenant(), &descriptor)
			.await
			.unwrap(),
		None
	);
}

#[test]
fn all_null_sparse_projection_disposition_materializes_null_page() {
	let schema = Schema::new(vec![Field::new("value", DataType::Int64, true)]);
	let record_id = TieredRecordId {
		slab_id: SlabId::new(70),
		record_offset: RecordBodyByteOffset::ZERO,
	};
	let record_directory = RecordBufferDirectory {
		record_id,
		record_plaintext_range: PlaintextByteRange { start: 0, len: 0 },
		row_count: 4,
		record_body_compression: crate::schema::RecordBodyCompressionPolicy::Uncompressed,
		ipc_schema: schema,
		buffers: vec![
			crate::storage::columnar::IpcBufferRange {
				offset_in_record: crate::schema::IpcBufferOffset::ZERO,
				length: crate::schema::IpcBufferLength::ZERO,
			},
			crate::storage::columnar::IpcBufferRange {
				offset_in_record: crate::schema::IpcBufferOffset::ZERO,
				length: crate::schema::IpcBufferLength::ZERO,
			},
		],
		column_to_buffers: vec![crate::storage::columnar::ColumnBufferRange {
			first_buffer_ordinal: IpcBufferIndex::ZERO,
			buffer_count: 2,
		}],
		column_null_counts: vec![4],
		storage_column_ordinals: BTreeMap::new(),
		logical_schema_fingerprint: LogicalSchemaFingerprint::from_bytes([0x70; 32]),
		page_directory: None,
		encoding_directory: None,
	};
	let requirement = ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([0]),
		..ColumnFetchRequirement::default()
	};
	let page = EncodingPageDescriptor {
		page_index: EncodingPageIndex::ZERO,
		first_value_index: BufferValueIndex::ZERO,
		value_count: BufferValueCount::new(4),
		byte_range: EncodedBufferByteRange {
			start: EncodingPageByteOffset::ZERO,
			len: RecordBodyByteLen::ZERO,
		},
	};
	let value_key = PageMaterializeBufferKey {
		row_group_index: RowGroupIndex::ZERO,
		ipc_column_ordinal: IpcColumnOrdinal::ZERO,
		ipc_buffer_ordinal: IpcBufferIndex::new(1),
		page_index: 0,
	};
	let plan = SparseProjectionFetchPlan {
		selected_groups: vec![RowGroupIndex::ZERO],
		group_pages: vec![
			crate::storage::encoding::sparse_projection_fetch::SparseProjectionFetchGroup {
				row_group_index: RowGroupIndex::ZERO,
				selected_pages: vec![page],
			},
		],
		columns: vec![PageMaterializeColumn {
			physical_ordinal: 0,
			null_buffer_ordinal: IpcBufferIndex::ZERO,
			value_buffer_ordinal: IpcBufferIndex::new(1),
			bytes_per_value: 8,
			data_type: DataType::Int64,
			nullable: true,
		}],
		fetches: Vec::new(),
		validity_fetches: Vec::new(),
		value_page_dispositions: BTreeMap::from([(
			value_key,
			SelectedPageDisposition::AllNullElided {
				page_row_count: 4,
				bytes_per_value: 8,
			},
		)]),
		all_valid_nullable_pages: BTreeSet::new(),
		selected_body_ranges: Vec::new(),
	};
	let predicate_outcome = crate::storage::row_group::PhaseBOutcome {
		survival_masks: HashMap::from([(
			RowGroupIndex::ZERO,
			BooleanArray::from(vec![true, false, true, false]),
		)]),
		..crate::storage::row_group::PhaseBOutcome::default()
	};
	let batches = materialize_sparse_projection_page_batches::<Vec<u8>>(
		&record_directory,
		&requirement,
		&plan,
		&[],
		&[],
		&predicate_outcome,
	)
	.unwrap();

	assert_eq!(batches.len(), 1);
	let values = batches[0]
		.batch
		.batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(values.len(), 2);
	assert_eq!(values.null_count(), 2);
}

struct CountingRangeStore {
	bytes: Bytes,
	in_flight: std::sync::atomic::AtomicUsize,
	max_seen: std::sync::atomic::AtomicUsize,
}

impl CountingRangeStore {
	fn new(bytes: Bytes) -> Self {
		Self {
			bytes,
			in_flight: std::sync::atomic::AtomicUsize::new(0),
			max_seen: std::sync::atomic::AtomicUsize::new(0),
		}
	}

	fn max_seen(&self) -> usize {
		self.max_seen.load(std::sync::atomic::Ordering::SeqCst)
	}
}

#[async_trait::async_trait]
impl ObjectStore for CountingRangeStore {
	async fn put(&self, _key: &str, _bytes: &[u8]) -> std::io::Result<String> {
		Err(Error::other("put not implemented for CountingRangeStore"))
	}

	async fn head(&self, uri: &str) -> std::io::Result<crate::storage::l2::ObjectMetadata> {
		Ok(crate::storage::l2::ObjectMetadata {
			key: "counting".to_string(),
			uri: uri.to_string(),
			size_bytes: self.bytes.len() as u64,
		})
	}

	async fn get_range(&self, _uri: &str, range: std::ops::Range<u64>) -> std::io::Result<Bytes> {
		let current = self
			.in_flight
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
			.saturating_add(1);
		let mut observed = self.max_seen.load(std::sync::atomic::Ordering::SeqCst);
		while current > observed {
			match self.max_seen.compare_exchange(
				observed,
				current,
				std::sync::atomic::Ordering::SeqCst,
				std::sync::atomic::Ordering::SeqCst,
			) {
				Ok(_) => break,
				Err(next) => observed = next,
			}
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
		self.in_flight
			.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
		let start = usize::try_from(range.start)
			.map_err(|_| Error::new(ErrorKind::InvalidInput, "range start exceeds usize"))?;
		let end = usize::try_from(range.end)
			.map_err(|_| Error::new(ErrorKind::InvalidInput, "range end exceeds usize"))?;
		if start > end || end > self.bytes.len() {
			return Err(Error::new(ErrorKind::InvalidInput, "range out of bounds"));
		}
		Ok(self.bytes.slice(start..end))
	}

	async fn get_bytes(&self, _uri: &str) -> std::io::Result<Bytes> {
		Ok(self.bytes.clone())
	}

	async fn list(
		&self,
		_prefix: &str,
	) -> std::io::Result<Vec<crate::storage::l2::ObjectMetadata>> {
		Ok(Vec::new())
	}

	async fn delete(&self, _uri: &str) -> std::io::Result<()> {
		Ok(())
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ObjectStoreCallSnapshot {
	get_range_calls: usize,
	get_range_bytes: u64,
	get_bytes_calls: usize,
	get_bytes_bytes: u64,
}

struct CountingObjectStore {
	inner: Arc<dyn ObjectStore>,
	get_range_calls: std::sync::atomic::AtomicUsize,
	get_range_bytes: std::sync::atomic::AtomicU64,
	get_bytes_calls: std::sync::atomic::AtomicUsize,
	get_bytes_bytes: std::sync::atomic::AtomicU64,
}

impl CountingObjectStore {
	fn new(inner: Arc<dyn ObjectStore>) -> Self {
		Self {
			inner,
			get_range_calls: std::sync::atomic::AtomicUsize::new(0),
			get_range_bytes: std::sync::atomic::AtomicU64::new(0),
			get_bytes_calls: std::sync::atomic::AtomicUsize::new(0),
			get_bytes_bytes: std::sync::atomic::AtomicU64::new(0),
		}
	}

	fn snapshot(&self) -> ObjectStoreCallSnapshot {
		ObjectStoreCallSnapshot {
			get_range_calls: self
				.get_range_calls
				.load(std::sync::atomic::Ordering::SeqCst),
			get_range_bytes: self
				.get_range_bytes
				.load(std::sync::atomic::Ordering::SeqCst),
			get_bytes_calls: self
				.get_bytes_calls
				.load(std::sync::atomic::Ordering::SeqCst),
			get_bytes_bytes: self
				.get_bytes_bytes
				.load(std::sync::atomic::Ordering::SeqCst),
		}
	}
}

#[async_trait::async_trait]
impl ObjectStore for CountingObjectStore {
	async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
		self.inner.put(key, bytes).await
	}

	async fn head(&self, uri: &str) -> std::io::Result<crate::storage::l2::ObjectMetadata> {
		self.inner.head(uri).await
	}

	async fn get_range(&self, uri: &str, range: std::ops::Range<u64>) -> std::io::Result<Bytes> {
		self.get_range_calls
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		self.get_range_bytes.fetch_add(
			range.end.saturating_sub(range.start),
			std::sync::atomic::Ordering::SeqCst,
		);
		self.inner.get_range(uri, range).await
	}

	async fn get_bytes(&self, uri: &str) -> std::io::Result<Bytes> {
		let bytes = self.inner.get_bytes(uri).await?;
		self.get_bytes_calls
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		self.get_bytes_bytes
			.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::SeqCst);
		Ok(bytes)
	}

	async fn list(&self, prefix: &str) -> std::io::Result<Vec<crate::storage::l2::ObjectMetadata>> {
		self.inner.list(prefix).await
	}

	async fn delete(&self, uri: &str) -> std::io::Result<()> {
		self.inner.delete(uri).await
	}
}

#[tokio::test]
async fn bounded_l2_range_fetch_preserves_order_and_parallel_cap() {
	let store = Arc::new(CountingRangeStore::new(Bytes::from_static(
		b"abcdefghijklmnopqrstuvwxyz",
	)));
	let ranges = vec![0..2, 2..5, 5..9, 9..14, 14..20, 20..26];

	let fetched =
		fetch_l2_ranges_bounded(store.clone(), "counting://object".to_string(), ranges, 2)
			.await
			.unwrap();

	assert_eq!(
		fetched.iter().map(Bytes::as_ref).collect::<Vec<_>>(),
		vec![
			b"ab".as_slice(),
			b"cde".as_slice(),
			b"fghi".as_slice(),
			b"jklmn".as_slice(),
			b"opqrst".as_slice(),
			b"uvwxyz".as_slice(),
		]
	);
	assert_eq!(store.max_seen(), 2);
}

fn encode_test_batch(batch: &RecordBatch) -> Vec<u8> {
	crate::storage::ipc::encode_record_batch_bytes(batch, RecordBodyCompressionPolicy::Uncompressed)
		.unwrap()
}

fn test_hlc_storage_bytes(hlc: HybridTimestamp) -> [u8; 16] {
	let mut bytes = [0_u8; 16];
	bytes[..8].copy_from_slice(&hlc.physical_ms.to_le_bytes());
	bytes[8..12].copy_from_slice(&hlc.logical.to_le_bytes());
	bytes[12..16].copy_from_slice(&hlc.node_id.to_le_bytes());
	bytes
}

fn encode_test_multirow_storage_frame(
	rows: &[(Identity, Locality, HybridTimestamp, u64)],
) -> Vec<u8> {
	let rows = rows
		.iter()
		.map(|(identity, locality, hlc, value)| (*identity, *locality, *hlc, *value, false))
		.collect::<Vec<_>>();
	encode_test_multirow_storage_frame_with_tombstones(&rows)
}

fn encode_test_multirow_storage_frame_with_tombstones(
	rows: &[(Identity, Locality, HybridTimestamp, u64, bool)],
) -> Vec<u8> {
	let mut row_hlc = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
	let mut row_identity = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
	let mut row_locality = UInt64Builder::with_capacity(rows.len());
	for (identity, locality, hlc, _, _) in rows {
		row_hlc.append_value(test_hlc_storage_bytes(*hlc)).unwrap();
		row_identity.append_value(identity.as_bytes()).unwrap();
		row_locality.append_value(locality.get());
	}
	let row_tombstone = BooleanArray::from(
		rows.iter()
			.map(|(_, _, _, _, value)| *value)
			.collect::<Vec<_>>(),
	);
	let schema = Arc::new(Schema::new(vec![
		Field::new("value", DataType::UInt64, false),
		Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
		Field::new(
			STORAGE_ROW_KEY_ID_COLUMN,
			DataType::FixedSizeBinary(32),
			false,
		),
		Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
		Field::new(STORAGE_ROW_TOMBSTONE_COLUMN, DataType::Boolean, false),
	]));
	let batch = RecordBatch::try_new(
		schema,
		vec![
			Arc::new(UInt64Array::from(
				rows.iter()
					.map(|(_, _, _, value, _)| *value)
					.collect::<Vec<_>>(),
			)) as Arc<dyn Array>,
			Arc::new(row_hlc.finish()) as Arc<dyn Array>,
			Arc::new(row_identity.finish()) as Arc<dyn Array>,
			Arc::new(row_locality.finish()) as Arc<dyn Array>,
			Arc::new(row_tombstone) as Arc<dyn Array>,
		],
	)
	.unwrap();
	encode_test_batch(&batch)
}

fn encode_test_append_only_storage_frame(
	table_object_id: TableObjectId,
	rows: &[(Identity, Locality, HybridTimestamp, u64, bool)],
) -> Vec<u8> {
	let mut row_hlc = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
	let mut row_identity = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
	let mut row_locality = UInt64Builder::with_capacity(rows.len());
	let mut row_table_object_id = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
	for (identity, locality, hlc, _, _) in rows {
		row_hlc.append_value(test_hlc_storage_bytes(*hlc)).unwrap();
		row_identity.append_value(identity.as_bytes()).unwrap();
		row_locality.append_value(locality.get());
		row_table_object_id
			.append_value(table_object_id.as_bytes())
			.unwrap();
	}
	let row_tombstone = BooleanArray::from(
		rows.iter()
			.map(|(_, _, _, _, value)| *value)
			.collect::<Vec<_>>(),
	);
	let schema = Arc::new(Schema::new(vec![
		Field::new("value", DataType::UInt64, false),
		Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
		Field::new(
			STORAGE_ROW_KEY_ID_COLUMN,
			DataType::FixedSizeBinary(32),
			false,
		),
		Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
		Field::new(
			STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
			DataType::FixedSizeBinary(16),
			false,
		),
		Field::new(STORAGE_ROW_TOMBSTONE_COLUMN, DataType::Boolean, false),
	]));
	let batch = RecordBatch::try_new(
		schema,
		vec![
			Arc::new(UInt64Array::from(
				rows.iter()
					.map(|(_, _, _, value, _)| *value)
					.collect::<Vec<_>>(),
			)) as Arc<dyn Array>,
			Arc::new(row_hlc.finish()) as Arc<dyn Array>,
			Arc::new(row_identity.finish()) as Arc<dyn Array>,
			Arc::new(row_locality.finish()) as Arc<dyn Array>,
			Arc::new(row_table_object_id.finish()) as Arc<dyn Array>,
			Arc::new(row_tombstone) as Arc<dyn Array>,
		],
	)
	.unwrap();
	encode_test_batch(&batch)
}

#[tokio::test]
async fn durable_multirow_table_attribution_survives_manager_reopen() {
	let dir = fresh_dir("adr0539-table-attribution");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let table_object_id = TableObjectId::new([0x53; 16]);
	let key_identity = id(5_039);
	let row_hlc = hlc(5_039);
	let frame = encode_test_append_only_storage_frame(
		table_object_id,
		&[(key_identity, Locality::new(53), row_hlc, 53, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("typed storage frame must expose multi-row entries");
	manager
		.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let location = manager
		.lookup(tenant(), key_identity)
		.expect("published typed row");
	assert_eq!(
		manager.published_table_object_id(location),
		Some(table_object_id)
	);
	assert_eq!(
		manager.tables_referencing_slab_by_kind(
			location.slab_id,
			PublishedReferenceKind::RecordLocation,
		),
		BTreeSet::from([(tenant(), table_object_id)])
	);

	drop(manager);
	let reopened = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	assert_eq!(
		reopened.published_table_object_id(location),
		Some(table_object_id)
	);
	assert_eq!(
		reopened.tables_referencing_slab_by_kind(
			location.slab_id,
			PublishedReferenceKind::RecordLocation,
		),
		BTreeSet::from([(tenant(), table_object_id)])
	);
}

#[tokio::test]
async fn missing_published_reference_fences_the_referencing_typed_table_before_write_ack() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table_object_id = TableObjectId::new([0x54; 16]);
	let key_identity = id(5_040);
	let row_hlc = hlc(5_040);
	let frame = encode_test_append_only_storage_frame(
		table_object_id,
		&[(key_identity, Locality::new(54), row_hlc, 54, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("typed storage frame must expose multi-row entries");
	manager
		.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let location = manager.lookup(tenant(), key_identity).unwrap();
	let slab_id = manager.force_seal_active_for_test().await;
	assert_eq!(location.slab_id, slab_id);
	let unrelated_table_object_id = TableObjectId::new([0x56; 16]);
	let unrelated_key_identity = id(5_042);
	let unrelated_frame = encode_test_append_only_storage_frame(
		unrelated_table_object_id,
		&[(
			unrelated_key_identity,
			Locality::new(56),
			hlc(5_042),
			56,
			false,
		)],
	);
	let unrelated_entries = storage_multirow_index_entries_from_frame(&unrelated_frame)
		.unwrap()
		.expect("typed storage frame must expose multi-row entries");
	manager
		.append_durable_multirow_with_locality(tenant(), &unrelated_frame, &unrelated_entries)
		.await
		.unwrap();
	let unrelated_location = manager.lookup(tenant(), unrelated_key_identity).unwrap();
	assert_ne!(unrelated_location.slab_id, slab_id);
	manager.state.lock().await.sealed.remove(&slab_id);

	let read_error = manager.read(location).await.unwrap_err();
	assert_eq!(read_error.kind(), std::io::ErrorKind::NotFound);
	let failure = manager
		.table_integrity_failure(tenant(), table_object_id)
		.expect("missing backing must fence the known referencing table");
	assert_eq!(failure.missing_slab_id, slab_id);
	assert_eq!(
		failure.reference_kind,
		PublishedReferenceKind::RecordLocation
	);
	assert_eq!(failure.origin, PublishedReferenceOrigin::Read);
	assert!(manager.is_compaction_slab_quarantined(slab_id));

	let write_error = manager
		.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap_err();
	assert_eq!(write_error.kind(), std::io::ErrorKind::InvalidData);
	assert!(
		write_error
			.to_string()
			.contains("published reference integrity failure")
	);
	assert_eq!(
		manager.table_integrity_failure(tenant(), unrelated_table_object_id),
		None,
		"a table without a reference into the missing slab stays outside the fence"
	);
	assert!(manager.read(unrelated_location).await.is_ok());
}

#[tokio::test]
async fn recovery_fences_durable_reference_to_missing_slab() {
	let dir = fresh_dir("adr0539-recovery-fence");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let table_object_id = TableObjectId::new([0x55; 16]);
	let key_identity = id(5_041);
	let row_hlc = hlc(5_041);
	let frame = encode_test_append_only_storage_frame(
		table_object_id,
		&[(key_identity, Locality::new(55), row_hlc, 55, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("typed storage frame must expose multi-row entries");
	manager
		.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let location = manager.lookup(tenant(), key_identity).unwrap();
	let slab_id = manager.force_seal_active_for_test().await;
	assert_eq!(location.slab_id, slab_id);
	drop(manager);
	std::fs::remove_file(slab_path(&dir, slab_id)).unwrap();

	let reopened = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let failure = reopened
		.table_integrity_failure(tenant(), table_object_id)
		.expect("recovery must detect the durable reference to the removed slab");
	assert_eq!(failure.missing_slab_id, slab_id);
	assert_eq!(failure.origin, PublishedReferenceOrigin::Recovery);
	drop(reopened);
	let reopened_again = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	assert_eq!(
		reopened_again
			.table_integrity_failure(tenant(), table_object_id)
			.expect("recovery must not erase the durable dangling-reference evidence")
			.origin,
		PublishedReferenceOrigin::Recovery
	);
}

#[tokio::test]
async fn durable_index_reference_blocks_local_slab_retirement() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table_object_id = TableObjectId::new([0x57; 16]);
	let key_identity = id(5_043);
	let row_hlc = hlc(5_043);
	let frame = encode_test_append_only_storage_frame(
		table_object_id,
		&[(key_identity, Locality::new(57), row_hlc, 57, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("typed storage frame must expose multi-row entries");
	manager
		.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let location = manager.lookup(tenant(), key_identity).unwrap();
	let slab_id = manager.force_seal_active_for_test().await;
	assert_eq!(location.slab_id, slab_id);

	// Fault injection: the runtime index has advanced but the durable index
	// still names the old slab. Retiring the file must stop here.
	manager.rollback_indexed_location(tenant(), key_identity, location);
	assert!(
		!manager.can_retire_local_slab(slab_id).await.unwrap(),
		"stale durable locations must prevent unlinking their backing slab"
	);
	assert!(manager.is_compaction_slab_quarantined(slab_id));
	assert!(manager.state.lock().await.sealed.contains_key(&slab_id));
}

fn encode_test_pk_tombstone_marker_frame(
	descriptor: &TableSchemaDescriptor,
	key_identity: Identity,
	routing_locality: Locality,
	row_hlc: HybridTimestamp,
) -> (Vec<u8>, Vec<MultiRowIndexEntry>) {
	let RecordLayoutPolicy::MultiRow(layout) = descriptor.record_layout_policy else {
		panic!("pk tombstone marker test requires multi-row layout")
	};
	let rows = vec![MultiRowSourceRow {
		source_row_index: 0,
		key_identity,
		routing_locality,
		row_hlc,
		is_tombstone: true,
		partition: None,
		table_object_id: Some(descriptor.table_object_id),
	}];
	let mut packed = crate::storage::multirow::pack_tombstone_multirow_source_rows_with_policy(
		layout,
		&rows,
		descriptor.record_body_compression_policy,
	)
	.unwrap();
	assert_eq!(packed.len(), 1);
	let record = packed.remove(0);
	let entries = record
		.encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	(record.encoded.frame, entries)
}

fn encode_test_row_tombstone_marker_frame(
	table_object_id: TableObjectId,
	key_identity: Identity,
	routing_locality: Locality,
	row_hlc: HybridTimestamp,
) -> (Vec<u8>, Vec<MultiRowIndexEntry>) {
	let layout = MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap();
	let rows = vec![MultiRowSourceRow {
		source_row_index: 0,
		key_identity,
		routing_locality,
		row_hlc,
		is_tombstone: true,
		partition: None,
		table_object_id: Some(table_object_id),
	}];
	let mut packed = crate::storage::multirow::pack_tombstone_multirow_source_rows_with_policy(
		layout,
		&rows,
		RecordBodyCompressionPolicy::Uncompressed,
	)
	.unwrap();
	assert_eq!(packed.len(), 1);
	let record = packed.remove(0);
	let entries = record
		.encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	(record.encoded.frame, entries)
}

fn append_only_retention_descriptor(table_object_id: TableObjectId) -> TableSchemaDescriptor {
	append_only_retention_descriptor_named(table_object_id, "append_events")
}

fn append_only_retention_descriptor_named(
	table_object_id: TableObjectId,
	table_name: &str,
) -> TableSchemaDescriptor {
	let schema = Schema::new(vec![Field::new("value", DataType::UInt64, false)]);
	TableSchemaDescriptor::new_with_layout(
		table_object_id,
		1,
		"analytics",
		table_name,
		&schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn pk_retention_descriptor(table_object_id: TableObjectId) -> TableSchemaDescriptor {
	let schema = Schema::new(vec![Field::new("value", DataType::UInt64, false)]);
	TableSchemaDescriptor::new_with_layout(
		table_object_id,
		1,
		"analytics",
		"pk_events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("value")),
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn fixed_table_retention_catalog(
	descriptor: &TableSchemaDescriptor,
	window_ms: u64,
) -> (TableSchemaCatalog, TableLifecycleCatalog) {
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), descriptor.clone()).unwrap();
	let mut lifecycle = TableLifecycleCatalog::default();
	lifecycle
		.set_next_generation(
			tenant(),
			descriptor.table_object_id,
			crate::schema::PolicyGeneration::ZERO,
			TableRetentionPolicy::FixedWindow(RetentionWindowMillis::try_new(window_ms).unwrap()),
			hlc(1),
		)
		.unwrap();
	(schemas, lifecycle)
}

fn layout_recluster_descriptor(
	object_byte: u8,
	generation: u64,
	layout: RecordLayoutPolicy,
) -> TableSchemaDescriptor {
	let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([object_byte; 16]),
		generation,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
			spread_factor: 1,
			prefix_bits: 1,
		}),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.record_layout_policy = layout;
	descriptor.record_body_compression_policy =
		RecordBodyCompressionPolicy::default_for_layout(&descriptor.record_layout_policy);
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor
}

fn layout_recluster_descriptor_with_codec(
	object_byte: u8,
	generation: u64,
	layout: RecordLayoutPolicy,
	compression_policy: RecordBodyCompressionPolicy,
) -> TableSchemaDescriptor {
	let mut descriptor = layout_recluster_descriptor(object_byte, generation, layout);
	descriptor.record_body_compression_policy = compression_policy;
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor
}

fn layout_recluster_batch(values: &[i64]) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
		vec![Arc::new(Int64Array::from(
			values.iter().copied().map(Some).collect::<Vec<_>>(),
		)) as Arc<dyn Array>],
	)
	.unwrap()
}

fn layout_recluster_meta(
	descriptor: &TableSchemaDescriptor,
	row_count: usize,
) -> TableWriteRequestMeta {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant();
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(row_count).unwrap();
	meta
}

fn first_i64(batch: &RecordBatch) -> i64 {
	batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap()
		.value(0)
}

fn storage_compression_l2_fetch_metric_value(codec: RecordBodyCompressionPolicy) -> u64 {
	let segment = "storage_compression_l2_record_body_bytes_fetched_total";
	let metric = format!("{}{}", "wendelon_", segment);
	let label = format!("{{codec=\"{}\"}}", codec.codec_label());
	metrics()
		.render_prometheus()
		.lines()
		.find_map(|line| {
			if line.starts_with(&metric) && line.contains(&label) {
				line.rsplit_once(' ')
					.and_then(|(_, value)| value.parse::<u64>().ok())
			} else {
				None
			}
		})
		.unwrap_or(0)
}

fn empty_compaction_catalogs() -> (TableSchemaCatalog, SecondaryIndexCatalog) {
	(
		TableSchemaCatalog::default(),
		SecondaryIndexCatalog::default(),
	)
}

async fn fresh_manager() -> (crate::test_support::TestDir, Arc<SlabManager>) {
	fresh_manager_with_group_commit(GroupCommitConfig::default()).await
}

async fn fresh_manager_with_l2(
	tag: &str,
) -> (
	[crate::test_support::TestDir; 2],
	Arc<SlabManager>,
	Arc<dyn ObjectStore>,
) {
	let l1_dir = fresh_dir(&format!("{tag}-l1"));
	let l2_root = fresh_dir(&format!("{tag}-l2"));
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
	let io = spawn_io_thread(64).unwrap();
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	([l1_dir, l2_root], manager, l2)
}

fn optimizer_fact_region_descriptor() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
	]);
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0xA2; 16]),
		3,
		"analytics",
		"customers",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.stats_policy = ColumnStatsPolicy {
		small_sets: vec![ColumnSmallSetSpec::new("region")],
		..ColumnStatsPolicy::default()
	};
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor.validate().unwrap();
	descriptor
}

fn optimizer_fact_utf8_region_heavy_hitter_descriptor() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::Utf8, true),
	]);
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0xA6; 16]),
		3,
		"analytics",
		"customers",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.stats_policy = ColumnStatsPolicy {
		heavy_hitters: vec![ColumnHeavyHitterSpec::with_caps("region", 4, 256).unwrap()],
		..ColumnStatsPolicy::default()
	};
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor.validate().unwrap();
	descriptor
}

fn optimizer_fact_amount_descriptor() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int64, true),
	]);
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0xA3; 16]),
		3,
		"analytics",
		"amounts",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor.validate().unwrap();
	descriptor
}

fn optimizer_fact_amount_range_distribution_descriptor(k: u32) -> TableSchemaDescriptor {
	let mut descriptor = optimizer_fact_amount_descriptor();
	descriptor.stats_policy = ColumnStatsPolicy {
		range_distributions: vec![ColumnRangeDistributionSpec::with_k("amount", k).unwrap()],
		..ColumnStatsPolicy::default()
	};
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor.validate().unwrap();
	descriptor
}

fn optimizer_fact_range_distribution_block(
	descriptor: &TableSchemaDescriptor,
	batch: &RecordBatch,
) -> ParsedColumnStatsBlock {
	let encoded = build_always_on_column_stats_block(descriptor, batch).unwrap();
	let parsed = parse_column_stats_block(&encoded).unwrap();
	assert_eq!(
		parsed.columns[1].stat_flags & COLUMN_STAT_HAS_RANGE_DISTRIBUTION,
		0
	);
	parsed
}

fn optimizer_fact_score_descriptor() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("score", DataType::Float64, true),
	]);
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0xA5; 16]),
		3,
		"analytics",
		"scores",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor.validate().unwrap();
	descriptor
}

fn optimizer_fact_candidate_less_descriptor() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![Field::new("status", DataType::LargeUtf8, false)]);
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0xA4; 16]),
		3,
		"analytics",
		"statuses",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("status")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.validate().unwrap();
	descriptor
}

fn optimizer_fact_region_batch(rows: &[(i64, Option<&str>)]) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		])),
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|(id, _)| Some(*id)).collect::<Vec<_>>(),
			)) as Arc<dyn Array>,
			Arc::new(LargeStringArray::from(
				rows.iter()
					.map(|(_, region)| *region)
					.collect::<Vec<Option<&str>>>(),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap()
}

fn optimizer_fact_utf8_region_batch(rows: &[(i64, Option<&str>)]) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		])),
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|(id, _)| Some(*id)).collect::<Vec<_>>(),
			)) as Arc<dyn Array>,
			Arc::new(StringArray::from(
				rows.iter()
					.map(|(_, region)| *region)
					.collect::<Vec<Option<&str>>>(),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap()
}

fn optimizer_fact_amount_batch(rows: &[(i64, Option<i64>)]) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int64, true),
		])),
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|(id, _)| Some(*id)).collect::<Vec<_>>(),
			)) as Arc<dyn Array>,
			Arc::new(Int64Array::from(
				rows.iter()
					.map(|(_, amount)| *amount)
					.collect::<Vec<Option<i64>>>(),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap()
}

fn optimizer_fact_score_batch(rows: &[(i64, Option<f64>)]) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("score", DataType::Float64, true),
		])),
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|(id, _)| Some(*id)).collect::<Vec<_>>(),
			)) as Arc<dyn Array>,
			Arc::new(Float64Array::from(
				rows.iter()
					.map(|(_, score)| *score)
					.collect::<Vec<Option<f64>>>(),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap()
}

fn optimizer_fact_candidate_less_batch(values: &[&str]) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new(
			"status",
			DataType::LargeUtf8,
			false,
		)])),
		vec![Arc::new(LargeStringArray::from(
			values.iter().map(|value| Some(*value)).collect::<Vec<_>>(),
		)) as Arc<dyn Array>],
	)
	.unwrap()
}

async fn append_optimizer_fact_rows(
	manager: &Arc<SlabManager>,
	descriptor: &TableSchemaDescriptor,
	batch: &RecordBatch,
	hlc_base: u64,
) {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant();
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&meta,
		batch,
		descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	for row in 0..batch.num_rows() {
		let row_batch = batch.slice(row, 1);
		let frame = encode_test_batch(&row_batch);
		manager
			.append_durable_with_locality(
				tenant(),
				admission.key_identities[row],
				admission.localities[row],
				hlc(hlc_base + row as u64),
				&frame,
			)
			.await
			.unwrap();
	}
}

async fn tier_optimizer_fact_batch(
	manager: &Arc<SlabManager>,
	l2: &Arc<dyn ObjectStore>,
	descriptor: &TableSchemaDescriptor,
	batch: &RecordBatch,
	object_prefix: &str,
	hlc_base: u64,
) -> SlabId {
	append_optimizer_fact_rows(manager, descriptor, batch, hlc_base).await;
	let slab_id = force_seal_active(manager).await;
	let raw = manager.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = manager
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2
		.put(
			&format!("{object_prefix}/slab_{slab_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	manager
		.apply_tier_slab(TieredSlabPointer {
			slab_id,
			l2_uri,
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: Some(locator.stats_format_version),
			stats_block_len: Some(locator.stats_block_len),
			stats_block_hash128: Some(locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();
	slab_id
}

struct DirectLsmCommitter {
	manager: Arc<SlabManager>,
}

#[async_trait]
impl TierCommitter for DirectLsmCommitter {
	async fn commit_tier_slab(&self, _tier: crate::consensus::TieredSlab) -> std::io::Result<()> {
		Err(Error::new(
			ErrorKind::Unsupported,
			"tier commits are not used by this test committer",
		))
	}

	async fn retire_tiered_slab(&self, _slab_id: SlabId) -> std::io::Result<()> {
		Err(Error::new(
			ErrorKind::Unsupported,
			"tier retirements are not used by this test committer",
		))
	}

	async fn commit_lsm_compaction(&self, commit: CompactionCommit) -> std::io::Result<()> {
		self.manager
			.apply_lsm_manifest_compaction_commit(&commit)
			.map(|_| ())
			.map_err(Error::other)
	}
}

struct DirectTierCommitter {
	manager: Arc<SlabManager>,
}

#[async_trait]
impl TierCommitter for DirectTierCommitter {
	async fn commit_tier_slab(&self, tier: TieredSlab) -> std::io::Result<()> {
		self.manager
			.apply_tier_slab(TieredSlabPointer {
				slab_id: tier.slab_id,
				l2_uri: tier.l2_uri,
				payload_hash: tier.payload_hash,
				is_encrypted: tier.is_encrypted,
				tenant_id: tier.tenant_id,
				tenant_kek_id: tier.tenant_kek_id,
				stats_format_version: tier.stats_format_version,
				stats_block_len: tier.stats_block_len,
				stats_block_hash128: tier.stats_block_hash128,
				tombstone_metadata: tier.tombstone_metadata,
				supersession_metadata: tier.supersession_metadata,
			})
			.await
	}

	async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<()> {
		self.manager.retire_tiered_slab(slab_id).await.map(|_| ())
	}
}

struct FailingLsmCommitter;

#[async_trait]
impl TierCommitter for FailingLsmCommitter {
	async fn commit_tier_slab(&self, _tier: crate::consensus::TieredSlab) -> std::io::Result<()> {
		Err(Error::new(
			ErrorKind::Unsupported,
			"tier commits are not used by this test committer",
		))
	}

	async fn retire_tiered_slab(&self, _slab_id: SlabId) -> std::io::Result<()> {
		Err(Error::new(
			ErrorKind::Unsupported,
			"tier retirements are not used by this test committer",
		))
	}

	async fn commit_lsm_compaction(&self, _commit: CompactionCommit) -> std::io::Result<()> {
		Err(Error::other("injected LSM commit failure"))
	}
}

async fn fresh_manager_with_group_commit(
	group_commit: GroupCommitConfig,
) -> (crate::test_support::TestDir, Arc<SlabManager>) {
	let dir = fresh_dir("mv");
	let io = spawn_io_thread(64).unwrap();
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			// Large enough that no rotation fires during tests;
			// we want every version in the same active slab so
			// the test targets the multi-version index
			// semantics rather than cross-slab drain behaviour.
			max_slab_bytes: 1024 * 1024,
			io,
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit,
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	(dir, manager)
}
async fn open_manager_at_dir(
	dir: std::path::PathBuf,
	io: crate::storage::io_uring::IoRingHandle,
	crypto: Arc<CryptoManager>,
) -> Arc<SlabManager> {
	Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir,
			max_slab_bytes: 1024 * 1024,
			io,
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto,
		})
		.await
		.unwrap(),
	)
}

// ============================================================
//  ADR 075 cut-3a: SlabManager-backed counter store + recovery
//  (storage-only; COUNTER_ADD_REQ still dark)
// ============================================================

use crate::storage::counter::{CounterKey, SlabCounterStore, counter_identity};

fn ckey(b: &[u8]) -> CounterKey {
	CounterKey(b.to_vec())
}

// Acceptance: a manager-backed tentative append indexes live but does NOT
// survive restart without a commit marker (recovery skips the orphan).
#[tokio::test]
async fn counter_tentative_without_marker_does_not_survive_restart() {
	let dir = fresh_dir("adr075-counter-orphan");
	let crypto = test_crypto();
	let k = ckey(b"verdict");
	{
		let mgr = open_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			crypto.clone(),
		)
		.await;
		let store = SlabCounterStore::new(
			mgr.clone(),
			tenant(),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		);
		store.tentative_append(&k, 5, hlc(1)).await.unwrap();
		assert_eq!(
			store.read_committed(&k).await.unwrap(),
			Some(5),
			"tentative is live-indexed in the manager"
		);
		mgr.fsync_all_local_slabs().await.unwrap();
	}
	let mgr = open_manager_at_dir(dir.to_path_buf(), spawn_io_thread(64).unwrap(), crypto).await;
	let store = SlabCounterStore::new(
		mgr,
		tenant(),
		NodeId {
			id: 1,
			_reserved: 0,
		},
	);
	assert_eq!(
		store.read_committed(&k).await.unwrap(),
		None,
		"un-committed tentative was skipped by recovery"
	);
}

// Acceptance (the headline #6): a manager-backed commit marker makes the
// tentative survive restart, and the production recovery path through
// SlabManager::open lands the committed counter in the manager's
// AUTHORITATIVE key_index (proven by a direct lookup).
#[tokio::test]
async fn counter_commit_marker_survives_restart_into_manager_index() {
	let dir = fresh_dir("adr075-counter-commit");
	let crypto = test_crypto();
	let k = ckey(b"verdict");
	{
		let mgr = open_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			crypto.clone(),
		)
		.await;
		let store = SlabCounterStore::new(
			mgr.clone(),
			tenant(),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		);
		let t = store.tentative_append(&k, 5, hlc(1)).await.unwrap();
		store.commit_tentative(t).await.unwrap();
		mgr.fsync_all_local_slabs().await.unwrap();
	}
	let mgr = open_manager_at_dir(dir.to_path_buf(), spawn_io_thread(64).unwrap(), crypto).await;
	// Direct proof: the committed counter is in the manager's key_index.
	assert!(
		mgr.lookup(tenant(), counter_identity(&k)).is_some(),
		"committed counter landed in the manager's authoritative index after open"
	);
	let store = SlabCounterStore::new(
		mgr,
		tenant(),
		NodeId {
			id: 1,
			_reserved: 0,
		},
	);
	assert_eq!(store.read_committed(&k).await.unwrap(), Some(5));
}

// ADR 0102: V2 marker/tentative pairing is by persisted version identity across
// slabs, not by an offset that can be reused after rotation.
#[tokio::test]
async fn ctr6_v2_recovery_pairs_across_rotation_and_ignores_same_offset_decoy() {
	let (dir, manager) = fresh_manager().await;
	let key = id(6_102);
	let origin = NodeId {
		id: 43,
		_reserved: 0,
	};
	let decoy_origin = NodeId {
		id: 44,
		_reserved: 0,
	};
	let tentative = manager
		.counter_append_tentative(tenant(), key, hlc(41), 42, origin)
		.await
		.unwrap();
	manager.force_seal_active_for_test().await;
	let decoy = manager
		.counter_append_tentative(tenant(), key, hlc(99), 99, decoy_origin)
		.await
		.unwrap();
	assert_eq!(
		decoy.offset, tentative.offset,
		"fixture must reuse the slab offset"
	);
	manager
		.counter_append_commit_marker(tenant(), key, hlc(41), tentative.offset, origin)
		.await
		.unwrap();
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);

	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	assert_eq!(
		reopened
			.counter_eligible_cells()
			.read(&(tenant(), key))
			.unwrap()
			.unwrap()
			.value,
		42,
		"the marker must recover its exact version across rotation"
	);
	let recovered = reopened.key_index.get(&(tenant(), key)).unwrap().clone();
	assert_eq!(
		recovered.len(),
		1,
		"the unmarked decoy must remain unindexed"
	);
	assert_eq!(recovered[0], tentative);
	assert_ne!(recovered[0].slab_id, decoy.slab_id);
}

#[tokio::test]
async fn ctr6_v2_orphan_marker_fails_closed_and_duplicate_marker_is_idempotent() {
	for duplicate in [false, true] {
		let (dir, manager) = fresh_manager().await;
		let key = id(if duplicate { 6_104 } else { 6_103 });
		let origin = NodeId {
			id: 45,
			_reserved: 0,
		};
		if duplicate {
			let tentative = manager
				.counter_append_tentative(tenant(), key, hlc(50), 51, origin)
				.await
				.unwrap();
			for _ in 0..2 {
				manager
					.counter_append_commit_marker(tenant(), key, hlc(50), tentative.offset, origin)
					.await
					.unwrap();
			}
		} else {
			manager
				.counter_append_commit_marker(tenant(), key, hlc(50), 1234, origin)
				.await
				.unwrap();
		}
		manager.fsync_all_local_slabs().await.unwrap();
		drop(manager);
		let reopened = open_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			test_crypto(),
		)
		.await;
		let result = reopened.counter_eligible_cells().read(&(tenant(), key));
		if duplicate {
			assert_eq!(result.unwrap().unwrap().value, 51);
		} else {
			assert!(result.is_err(), "orphan marker must refuse its key");
		}
	}
}

#[tokio::test]
async fn ctr6_recovered_committed_identity_allows_redrive_and_rejects_value_conflict() {
	let (dir, manager) = fresh_manager().await;
	let key = id(6_105);
	let origin = NodeId {
		id: 46,
		_reserved: 0,
	};
	let commit_hlc = HybridTimestamp {
		physical_ms: 71,
		logical: 0,
		node_id: 0,
	};
	let service = Arc::new(crate::storage::counter::CounterReplicaService::new(
		manager.clone(),
	));
	service
		.replicate_tentative(origin, 71, tenant(), key, 72, commit_hlc)
		.await
		.unwrap();
	service.commit(origin, 71, tenant(), key).await.unwrap();
	service.commit(origin, 71, tenant(), key).await.unwrap();
	manager.fsync_all_local_slabs().await.unwrap();
	drop(service);
	drop(manager);

	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let service = crate::storage::counter::CounterReplicaService::new(reopened);
	service.commit(origin, 71, tenant(), key).await.unwrap();
	let absent = service.commit(origin, 72, tenant(), key).await.unwrap_err();
	assert_eq!(
		absent.get_ref().and_then(
			|source| source.downcast_ref::<crate::storage::counter::CounterVisibilityError>()
		),
		Some(&crate::storage::counter::CounterVisibilityError::TentativeMissing),
		"a commit without a pending or recovered identity is a typed refusal"
	);
	let mismatch = service
		.replicate_tentative(origin, 71, tenant(), key, 73, commit_hlc)
		.await
		.unwrap_err();
	assert_eq!(
		mismatch.get_ref().and_then(
			|source| source.downcast_ref::<crate::storage::counter::CounterVisibilityError>()
		),
		Some(&crate::storage::counter::CounterVisibilityError::Conflict),
		"the recovered identity is bound to its committed value"
	);
}

#[tokio::test]
async fn ctr6_recovery_keeps_older_committed_identity_after_supersession() {
	let (dir, manager) = fresh_manager().await;
	let key = id(6_106);
	let origin = NodeId {
		id: 47,
		_reserved: 0,
	};
	let service = crate::storage::counter::CounterReplicaService::new(manager.clone());
	for (op_id, value) in [(81, 82), (83, 84)] {
		let stamp = HybridTimestamp {
			physical_ms: op_id,
			logical: 0,
			node_id: 0,
		};
		service
			.replicate_tentative(origin, op_id, tenant(), key, value, stamp)
			.await
			.unwrap();
		service.commit(origin, op_id, tenant(), key).await.unwrap();
	}
	manager.fsync_all_local_slabs().await.unwrap();
	drop(service);
	drop(manager);

	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let service = crate::storage::counter::CounterReplicaService::new(reopened);
	assert_eq!(
		service.read_committed(tenant(), key).await.unwrap(),
		Some(84)
	);
	service.commit(origin, 81, tenant(), key).await.unwrap();
}

#[tokio::test]
async fn ctr6_same_identity_with_different_tentative_values_refuses_key() {
	let (dir, manager) = fresh_manager().await;
	let key = id(6_107);
	let origin = NodeId {
		id: 48,
		_reserved: 0,
	};
	let hlc = HybridTimestamp {
		physical_ms: 85,
		logical: 0,
		node_id: 0,
	};
	let first = manager
		.counter_append_tentative(tenant(), key, hlc, 86, origin)
		.await
		.unwrap();
	manager
		.counter_append_tentative(tenant(), key, hlc, 87, origin)
		.await
		.unwrap();
	for _ in 0..2 {
		manager
			.counter_append_commit_marker(tenant(), key, hlc, first.offset, origin)
			.await
			.unwrap();
	}
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);
	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	assert_eq!(
		reopened.counter_eligible_cells().read(&(tenant(), key)),
		Err(crate::storage::counter::CounterVisibilityError::Conflict)
	);
}

#[tokio::test]
async fn ctr6_same_slab_offset_mismatch_refuses_key() {
	let (dir, manager) = fresh_manager().await;
	let key = id(6_108);
	let origin = NodeId {
		id: 49,
		_reserved: 0,
	};
	let hlc = HybridTimestamp {
		physical_ms: 88,
		logical: 0,
		node_id: 0,
	};
	let tentative = manager
		.counter_append_tentative(tenant(), key, hlc, 89, origin)
		.await
		.unwrap();
	manager
		.counter_append_commit_marker(tenant(), key, hlc, tentative.offset + 4096, origin)
		.await
		.unwrap();
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);
	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	assert_eq!(
		reopened.counter_eligible_cells().read(&(tenant(), key)),
		Err(crate::storage::counter::CounterVisibilityError::Conflict)
	);
}

#[tokio::test]
async fn ctr6_orphan_marker_recovery_does_not_rewrite_slab_bytes() {
	let (dir, manager) = fresh_manager().await;
	let key = id(6_109);
	let origin = NodeId {
		id: 50,
		_reserved: 0,
	};
	manager
		.counter_append_commit_marker(
			tenant(),
			key,
			HybridTimestamp {
				physical_ms: 90,
				logical: 0,
				node_id: 0,
			},
			1234,
			origin,
		)
		.await
		.unwrap();
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);
	let mut before = std::fs::read_dir(&*dir)
		.unwrap()
		.map(|entry| {
			let path = entry.unwrap().path();
			(
				path.file_name().unwrap().to_owned(),
				std::fs::read(path).unwrap(),
			)
		})
		.filter(|(name, _)| name.to_string_lossy().ends_with(".wnd"))
		.collect::<Vec<_>>();
	before.sort_by(|a, b| a.0.cmp(&b.0));
	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	assert!(
		reopened
			.counter_eligible_cells()
			.read(&(tenant(), key))
			.is_err(),
		"an orphan marker must fail closed"
	);
	drop(reopened);
	let mut after = std::fs::read_dir(&*dir)
		.unwrap()
		.map(|entry| {
			let path = entry.unwrap().path();
			(
				path.file_name().unwrap().to_owned(),
				std::fs::read(path).unwrap(),
			)
		})
		.filter(|(name, _)| name.to_string_lossy().ends_with(".wnd"))
		.collect::<Vec<_>>();
	after.sort_by(|a, b| a.0.cmp(&b.0));
	assert_eq!(
		before, after,
		"recovery must leave orphan marker bytes unchanged"
	);
}

#[tokio::test]
async fn ctr6_leader_local_marker_after_rotation_reopens_at_42() {
	let (dir, manager) = fresh_manager().await;
	let key = id(6_110);
	let origin = NodeId {
		id: 61,
		_reserved: 0,
	};
	let commit_hlc = HybridTimestamp {
		physical_ms: 42,
		logical: 0,
		node_id: 0,
	};
	let tentative = manager
		.counter_append_tentative(tenant(), key, commit_hlc, 42, origin)
		.await
		.unwrap();
	manager.force_seal_active_for_test().await;
	manager
		.counter_append_commit_marker(tenant(), key, commit_hlc, tentative.offset, origin)
		.await
		.unwrap();
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);

	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	assert_eq!(
		reopened
			.counter_eligible_cells()
			.read(&(tenant(), key))
			.unwrap()
			.unwrap()
			.value,
		42,
		"leader-local marked value must survive a marker written after slab rotation"
	);
}

#[tokio::test]
async fn ctr6_leader_add_after_straddled_reopen_uses_42_as_base_and_commits_43() {
	let (dir, manager) = fresh_manager().await;
	let counter_key = crate::storage::counter::CounterKey(b"ctr6-leader-base".to_vec());
	let key = counter_identity(&counter_key);
	let origin = NodeId {
		id: 63,
		_reserved: 0,
	};
	let commit_hlc = HybridTimestamp {
		physical_ms: 42,
		logical: 0,
		node_id: 0,
	};
	let tentative = manager
		.counter_append_tentative(tenant(), key, commit_hlc, 42, origin)
		.await
		.unwrap();
	manager.force_seal_active_for_test().await;
	manager
		.counter_append_commit_marker(tenant(), key, commit_hlc, tentative.offset, origin)
		.await
		.unwrap();
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);

	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let service = Arc::new(crate::storage::counter::CounterReplicaService::new(
		reopened.clone(),
	));
	let local = crate::storage::counter::LocalCounterParticipant::new(service, tenant(), origin);
	let leader = crate::storage::counter::CounterLeader::new(
		origin,
		local.clone(),
		true,
		crate::storage::counter::DurableCounterWitnessStore::open(dir.join("counter-witness"))
			.unwrap(),
	);
	// The version at HLC 42 represents the preceding committed op; align the
	// freshly reconstructed leader's allocator with the next canonical op id.
	leader.set_next_op_id_for_test(43);
	let mut write_key = [0; 24];
	write_key[..8].copy_from_slice(&43u64.to_le_bytes());
	let result = leader
		.fetch_add(
			crate::storage::counter::CounterParticipantPlan::new(vec![(origin, local)], 1),
			write_key,
			&counter_key,
			1,
		)
		.await
		.unwrap();
	assert_eq!(
		result,
		crate::storage::counter::CounterAddResult::Applied(
			crate::storage::counter::CounterAddOutcome {
				previous: 42,
				current: 43,
			}
		),
		"the first post-reopen leader add must base on the straddled value"
	);
	assert_eq!(
		reopened
			.counter_eligible_cells()
			.read(&(tenant(), key))
			.unwrap()
			.unwrap()
			.value,
		43
	);
}

#[tokio::test]
async fn ctr6_recovery_is_independent_of_slab_enumeration_order() {
	let (dir, manager) = fresh_manager().await;
	let cross_key = id(6_111);
	let local_key = id(6_112);
	let origin = NodeId {
		id: 62,
		_reserved: 0,
	};
	let cross_hlc = HybridTimestamp {
		physical_ms: 42,
		logical: 0,
		node_id: 0,
	};
	let cross_tentative = manager
		.counter_append_tentative(tenant(), cross_key, cross_hlc, 42, origin)
		.await
		.unwrap();
	manager.force_seal_active_for_test().await;
	manager
		.counter_append_commit_marker(
			tenant(),
			cross_key,
			cross_hlc,
			cross_tentative.offset,
			origin,
		)
		.await
		.unwrap();
	let local_hlc = HybridTimestamp {
		physical_ms: 43,
		logical: 0,
		node_id: 0,
	};
	let local_tentative = manager
		.counter_append_tentative(tenant(), local_key, local_hlc, 43, origin)
		.await
		.unwrap();
	manager
		.counter_append_commit_marker(
			tenant(),
			local_key,
			local_hlc,
			local_tentative.offset,
			origin,
		)
		.await
		.unwrap();
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);

	let slab_dir = dir.to_path_buf();
	let open_slab = |slab_id: SlabId| {
		let path = slab_dir.join(format!("slab_{:016x}.wnd", slab_id.get()));
		async move {
			Arc::new(
				crate::storage::LocalSlabStore::open(
					path,
					slab_id,
					Arc::new(BlockingFileOps),
					GroupCommitConfig::default(),
				)
				.await
				.unwrap(),
			)
		}
	};
	let slab0 = open_slab(SlabId::ZERO).await;
	let slab1 = open_slab(SlabId::new(1)).await;
	let first_order = [slab0.clone(), slab1.clone()];
	let reverse_order = [slab1, slab0];
	let recover = |slabs: &[Arc<crate::storage::LocalSlabStore>]| {
		SlabManager::recover_counter_eligible_from_slabs(slabs, &DashSet::new(), &DashMap::new())
	};
	let first = recover(&first_order);
	let reverse = recover(&reverse_order);
	for key in [cross_key, local_key] {
		assert_eq!(
			first.read(&(tenant(), key)).unwrap(),
			reverse.read(&(tenant(), key)).unwrap(),
			"eligible dump changed with slab enumeration order for {key:?}"
		);
	}
}

async fn compact_sealed_counter_fixture(
	manager: &Arc<SlabManager>,
	slab_id: SlabId,
	records: &[CompactionRecord],
) -> std::io::Result<Option<CompactionOutcome>> {
	let slab = manager
		.state
		.lock()
		.await
		.sealed
		.get(&slab_id)
		.map(|entry| entry.value().clone())
		.expect("counter compaction fixture source is sealed");
	let claims = manager.acquire_maintenance_claims(&[slab_id]).await;
	let schemas = TableSchemaCatalog::default();
	let indexes = SecondaryIndexCatalog::default();
	manager
		.compact_sealed_slab_records(
			slab_id,
			slab,
			records,
			SecondaryIndexCompactionContext {
				table_schemas: &schemas,
				secondary_indexes: &indexes,
			},
			&claims,
		)
		.await
}

#[tokio::test]
async fn fctr5a_counter_only_rewrite_preserves_pair_and_remaps_same_slab_marker() {
	use crate::storage::core::ScannedCounterFrame;
	use crate::storage::counter::CounterReplicaService;

	let (dir, manager) = fresh_manager().await;
	let origin = NodeId {
		id: 0x5a01,
		_reserved: 0,
	};
	let key = id(0x5a02);
	let ordinary_key = id(0x5a03);
	let ordinary = manager
		.append_durable(tenant(), ordinary_key, hlc(9), b"ordinary frame elided")
		.await
		.unwrap();
	let service = CounterReplicaService::new(manager.clone());
	service
		.replicate_tentative(
			origin,
			10,
			tenant(),
			key,
			42,
			HybridTimestamp {
				physical_ms: 10,
				logical: 0,
				node_id: 0,
			},
		)
		.await
		.unwrap();
	service.commit(origin, 10, tenant(), key).await.unwrap();
	let source_id = manager.force_seal_active_for_test().await;
	let source = manager
		.state
		.lock()
		.await
		.sealed
		.get(&source_id)
		.unwrap()
		.value()
		.clone();
	let source_frames = source.scan_counter_frames().await.unwrap();
	assert_eq!(
		source_frames
			.iter()
			.map(|frame| frame.record_class)
			.collect::<Vec<_>>(),
		vec![
			crate::schema::record_class::COUNTER_TENTATIVE_V2,
			crate::schema::record_class::COUNTER_COMMIT_V2,
		]
	);
	let source_tentative = source_frames
		.iter()
		.find(|frame| frame.record_class == crate::schema::record_class::COUNTER_TENTATIVE_V2)
		.unwrap()
		.clone();
	let source_marker = source_frames
		.iter()
		.find(|frame| frame.record_class == crate::schema::record_class::COUNTER_COMMIT_V2)
		.unwrap()
		.clone();
	assert!(source_tentative.location.offset > 0);
	assert_eq!(
		u64::from_le_bytes(source_marker.payload[..8].try_into().unwrap()),
		source_tentative.location.offset
	);

	let dead = [CompactionRecord {
		tenant_id: tenant(),
		key_identity: ordinary_key,
		routing_locality: Locality::from(ordinary_key),
		old_location: ordinary,
		reclaim: true,
		retention_ambiguous: false,
		retention_lag_seconds: 0,
	}];
	let outcome = compact_sealed_counter_fixture(&manager, source_id, &dead)
		.await
		.unwrap()
		.expect(
			"counter-bearing source must publish a replacement even when all ordinary rows elide",
		);
	assert_eq!(outcome.reclaimed_records, 1);
	assert_eq!(
		service.read_committed(tenant(), key).await.unwrap(),
		Some(42)
	);
	assert!(manager.lookup(tenant(), ordinary_key).is_none());

	let moved = manager
		.lookup(tenant(), key)
		.expect("counter tentative remains indexed");
	assert_ne!(moved.slab_id, source_id);
	let replacement = manager
		.state
		.lock()
		.await
		.sealed
		.get(&moved.slab_id)
		.unwrap()
		.value()
		.clone();
	let frames: Vec<ScannedCounterFrame> = replacement.scan_counter_frames().await.unwrap();
	let tentative = frames
		.iter()
		.find(|frame| frame.record_class == crate::schema::record_class::COUNTER_TENTATIVE_V2)
		.unwrap();
	let marker = frames
		.iter()
		.find(|frame| frame.record_class == crate::schema::record_class::COUNTER_COMMIT_V2)
		.unwrap();
	assert_eq!(tentative.payload, source_tentative.payload);
	assert_eq!(tentative.location.offset, 0);
	assert_eq!(
		u64::from_le_bytes(marker.payload[..8].try_into().unwrap()),
		tentative.location.offset,
		"same-slab marker must be remapped to the replacement tentative"
	);
	assert_ne!(marker.payload, source_marker.payload);
	assert_eq!(
		marker.record_class,
		crate::schema::record_class::COUNTER_COMMIT_V2
	);
	let manifest = manager.compaction_manifest_snapshot();
	let replacement_run = manifest
		.run(moved.slab_id)
		.expect("replacement run published");
	assert_eq!(replacement_run.row_count.get(), 1);
	assert!(replacement_run.size_bytes.get() > 0);
	assert!(
		manifest
			.run(source_id)
			.is_some_and(|run| !run.state.is_live())
	);

	manager.fsync_all_local_slabs().await.unwrap();
	drop(service);
	drop(manager);
	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let recovered = crate::storage::counter::CounterReplicaService::new(reopened);
	assert_eq!(
		recovered.read_committed(tenant(), key).await.unwrap(),
		Some(42)
	);
}

#[tokio::test]
async fn fctr5a_cross_slab_marker_is_copied_verbatim_by_identity() {
	use crate::storage::counter::CounterReplicaService;

	let (dir, manager) = fresh_manager().await;
	let origin = NodeId {
		id: 0x5a11,
		_reserved: 0,
	};
	let key = id(0x5a12);
	let service = CounterReplicaService::new(manager.clone());
	service
		.replicate_tentative(
			origin,
			20,
			tenant(),
			key,
			43,
			HybridTimestamp {
				physical_ms: 20,
				logical: 0,
				node_id: 0,
			},
		)
		.await
		.unwrap();
	let tentative_slab = manager.force_seal_active_for_test().await;
	service.commit(origin, 20, tenant(), key).await.unwrap();
	let marker_slab = manager.force_seal_active_for_test().await;
	assert_ne!(tentative_slab, marker_slab);
	let marker_source = manager
		.state
		.lock()
		.await
		.sealed
		.get(&marker_slab)
		.unwrap()
		.value()
		.clone();
	let source_marker = marker_source
		.scan_counter_frames()
		.await
		.unwrap()
		.into_iter()
		.find(|frame| frame.record_class == crate::schema::record_class::COUNTER_COMMIT_V2)
		.unwrap();
	let original_payload = source_marker.payload.clone();

	compact_sealed_counter_fixture(&manager, marker_slab, &[])
		.await
		.unwrap()
		.expect("marker-only slab must be retained via identity-bound committed state");
	let tentative = manager.lookup(tenant(), key).unwrap();
	assert_eq!(tentative.slab_id, tentative_slab);
	let replacement = {
		let state = manager.state.lock().await;
		let mut replacements = state
			.sealed
			.iter()
			.filter(|entry| entry.key() != &tentative_slab && entry.key() != &marker_slab)
			.map(|entry| entry.value().clone());
		let replacement = replacements
			.next()
			.expect("replacement marker slab installed");
		assert!(
			replacements.next().is_none(),
			"only one replacement slab expected"
		);
		replacement
	};
	let moved_frame = replacement
		.scan_counter_frames()
		.await
		.unwrap()
		.into_iter()
		.find(|frame| frame.record_class == crate::schema::record_class::COUNTER_COMMIT_V2)
		.unwrap();
	assert_eq!(moved_frame.payload, original_payload);
	let manifest = manager.compaction_manifest_snapshot();
	let marker_run = manifest
		.run(replacement.slab_id())
		.expect("counter-only replacement has a manifest run");
	assert_eq!(marker_run.row_count.get(), 0);
	assert!(marker_run.size_bytes.get() > 0);
	assert_eq!(
		service.read_committed(tenant(), key).await.unwrap(),
		Some(43)
	);
	manager.fsync_all_local_slabs().await.unwrap();
	drop(service);
	drop(manager);
	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let recovered = crate::storage::counter::CounterReplicaService::new(reopened);
	assert_eq!(
		recovered.read_committed(tenant(), key).await.unwrap(),
		Some(43)
	);
}

#[tokio::test]
async fn fctr5a_legacy_and_orphan_marker_rewrites_refuse_without_mutation() {
	for legacy in [true, false] {
		let (dir, manager) = fresh_manager().await;
		let key = id(if legacy { 0x5a21 } else { 0x5a22 });
		let origin = NodeId {
			id: 0x5a23,
			_reserved: 0,
		};
		let slab = manager.state.lock().await.active.clone();
		let payload = crate::storage::counter::encode_origin_frame(7, origin).unwrap();
		if legacy {
			slab.write_framed(
				tenant(),
				key,
				hlc(70),
				crate::schema::record_class::COUNTER_TENTATIVE,
				&payload,
			)
			.await
			.unwrap();
		} else {
			manager
				.counter_append_commit_marker(tenant(), key, hlc(70), 1234, origin)
				.await
				.unwrap();
		}
		let source_id = manager.force_seal_active_for_test().await;
		let path = slab_path(&dir, source_id);
		let before = std::fs::read(&path).unwrap();
		let manifest_epoch = manager.compaction_manifest_snapshot().manifest_epoch;
		let schemas = TableSchemaCatalog::default();
		let indexes = SecondaryIndexCatalog::default();
		let result = manager
			.compact_one_sealed_slab(
				source_id,
				&[],
				SecondaryIndexCompactionContext {
					table_schemas: &schemas,
					secondary_indexes: &indexes,
				},
			)
			.await;
		assert!(
			result.is_err(),
			"invalid counter maintenance source must fail closed"
		);
		assert_eq!(
			std::fs::read(&path).unwrap(),
			before,
			"source bytes changed"
		);
		assert_eq!(
			manager.compaction_manifest_snapshot().manifest_epoch,
			manifest_epoch,
			"refusal must not publish a replacement manifest"
		);
		assert_eq!(
			std::fs::read_dir(&*dir)
				.unwrap()
				.filter_map(Result::ok)
				.filter(|entry| entry.file_name().to_string_lossy().ends_with(".wnd"))
				.count(),
			2,
			"only source and fresh active slab may exist after refusal"
		);
	}
}

#[tokio::test]
async fn fctr5a_same_slab_offset_conflict_refuses_before_publication() {
	use crate::storage::counter::CounterReplicaService;

	let (dir, manager) = fresh_manager().await;
	let origin = NodeId {
		id: 0x5a24,
		_reserved: 0,
	};
	let key = id(0x5a25);
	let commit_hlc = HybridTimestamp {
		physical_ms: 75,
		logical: 0,
		node_id: 0,
	};
	let service = CounterReplicaService::new(manager.clone());
	service
		.replicate_tentative(origin, 75, tenant(), key, 75, commit_hlc)
		.await
		.unwrap();
	let tentative = manager.lookup(tenant(), key).unwrap();
	service.commit(origin, 75, tenant(), key).await.unwrap();
	manager
		.counter_append_commit_marker(tenant(), key, commit_hlc, tentative.offset + 4096, origin)
		.await
		.unwrap();
	let source_id = manager.force_seal_active_for_test().await;
	let path = slab_path(&dir, source_id);
	let source_before = std::fs::read(&path).unwrap();
	let manifest_before = manager.compaction_manifest_snapshot();
	let schemas = TableSchemaCatalog::default();
	let indexes = SecondaryIndexCatalog::default();
	let result = manager
		.compact_one_sealed_slab(
			source_id,
			&[],
			SecondaryIndexCompactionContext {
				table_schemas: &schemas,
				secondary_indexes: &indexes,
			},
		)
		.await;
	let error = match result {
		Err(error) => error,
		Ok(_) => panic!("same-slab marker offset mismatch must refuse maintenance"),
	};
	assert_eq!(
		error.get_ref().and_then(
			|inner| inner.downcast_ref::<crate::storage::counter::CounterVisibilityError>()
		),
		Some(&crate::storage::counter::CounterVisibilityError::Conflict)
	);
	assert_eq!(std::fs::read(&path).unwrap(), source_before);
	let manifest_after = manager.compaction_manifest_snapshot();
	assert_eq!(
		manifest_after.manifest_epoch,
		manifest_before.manifest_epoch
	);
	assert_eq!(
		manifest_after.run(source_id),
		manifest_before.run(source_id)
	);
	assert_eq!(
		std::fs::read_dir(&*dir)
			.unwrap()
			.filter_map(Result::ok)
			.filter(|entry| entry.file_name().to_string_lossy().ends_with(".wnd"))
			.count(),
		2,
		"conflict admission must not leave a staged replacement"
	);
}

#[tokio::test]
async fn fctr5a_pending_tentative_defers_without_mutation() {
	let (dir, manager) = fresh_manager().await;
	let origin = NodeId {
		id: 0x5a26,
		_reserved: 0,
	};
	let key = id(0x5a27);
	manager
		.counter_append_tentative(
			tenant(),
			key,
			HybridTimestamp {
				physical_ms: 76,
				logical: 0,
				node_id: 0,
			},
			76,
			origin,
		)
		.await
		.unwrap();
	assert_eq!(manager.counter_pending_frame_count(), 1);
	let source_id = manager.force_seal_active_for_test().await;
	let path = slab_path(&dir, source_id);
	let source_before = std::fs::read(&path).unwrap();
	let manifest_before = manager.compaction_manifest_snapshot();
	let schemas = TableSchemaCatalog::default();
	let indexes = SecondaryIndexCatalog::default();
	let outcome = manager
		.compact_one_sealed_slab(
			source_id,
			&[],
			SecondaryIndexCompactionContext {
				table_schemas: &schemas,
				secondary_indexes: &indexes,
			},
		)
		.await
		.unwrap();
	assert_eq!(outcome.scan, SealedSlabScan::Deferred);
	assert!(outcome.compaction.is_none());
	assert_eq!(std::fs::read(&path).unwrap(), source_before);
	let manifest_after = manager.compaction_manifest_snapshot();
	assert_eq!(
		manifest_after.manifest_epoch,
		manifest_before.manifest_epoch
	);
	assert_eq!(
		manifest_after.run(source_id),
		manifest_before.run(source_id)
	);
	assert_eq!(
		std::fs::read_dir(&*dir)
			.unwrap()
			.filter_map(Result::ok)
			.filter(|entry| entry.file_name().to_string_lossy().ends_with(".wnd"))
			.count(),
		2,
		"pending counter admission must not create a replacement"
	);
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);

	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	assert_eq!(reopened.counter_pending_frame_count(), 0);
	let outcome = reopened
		.compact_one_sealed_slab(
			source_id,
			&[],
			SecondaryIndexCompactionContext {
				table_schemas: &schemas,
				secondary_indexes: &indexes,
			},
		)
		.await
		.expect("restart discards volatile pending state, leaving the orphan compactable");
	assert!(outcome.compaction.is_some());
}

#[tokio::test]
async fn fctr5a_duplicate_marker_rewrite_remains_idempotent() {
	use crate::storage::counter::CounterReplicaService;

	let (dir, manager) = fresh_manager().await;
	let origin = NodeId {
		id: 0x5a28,
		_reserved: 0,
	};
	let key = id(0x5a29);
	let commit_hlc = HybridTimestamp {
		physical_ms: 77,
		logical: 0,
		node_id: 0,
	};
	let service = CounterReplicaService::new(manager.clone());
	service
		.replicate_tentative(origin, 77, tenant(), key, 77, commit_hlc)
		.await
		.unwrap();
	let tentative = manager.lookup(tenant(), key).unwrap();
	service.commit(origin, 77, tenant(), key).await.unwrap();
	manager
		.counter_append_commit_marker(tenant(), key, commit_hlc, tentative.offset, origin)
		.await
		.unwrap();
	let source_id = manager.force_seal_active_for_test().await;
	let source = manager
		.state
		.lock()
		.await
		.sealed
		.get(&source_id)
		.unwrap()
		.value()
		.clone();
	let source_markers = source
		.scan_counter_frames()
		.await
		.unwrap()
		.into_iter()
		.filter(|frame| frame.record_class == crate::schema::record_class::COUNTER_COMMIT_V2)
		.collect::<Vec<_>>();
	assert_eq!(source_markers.len(), 2);
	assert_eq!(source_markers[0].payload, source_markers[1].payload);

	compact_sealed_counter_fixture(&manager, source_id, &[])
		.await
		.unwrap()
		.expect("duplicate committed markers are preserved by one-source rewrite");
	assert_eq!(
		service.read_committed(tenant(), key).await.unwrap(),
		Some(77)
	);
	let replacement = {
		let state = manager.state.lock().await;
		state
			.sealed
			.iter()
			.find(|entry| *entry.key() != source_id)
			.expect("replacement slab installed")
			.value()
			.clone()
	};
	let replacement_markers = replacement
		.scan_counter_frames()
		.await
		.unwrap()
		.into_iter()
		.filter(|frame| frame.record_class == crate::schema::record_class::COUNTER_COMMIT_V2)
		.collect::<Vec<_>>();
	assert_eq!(replacement_markers.len(), 2);
	assert_eq!(
		replacement_markers[0].payload,
		replacement_markers[1].payload
	);
	manager.fsync_all_local_slabs().await.unwrap();
	drop(service);
	drop(manager);
	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let recovered = crate::storage::counter::CounterReplicaService::new(reopened);
	assert_eq!(
		recovered.read_committed(tenant(), key).await.unwrap(),
		Some(77)
	);
}

#[tokio::test]
async fn fctr5a_malformed_v2_payload_refuses_without_mutation() {
	let (dir, manager) = fresh_manager().await;
	let key = id(0x5a2a);
	let slab = manager.state.lock().await.active.clone();
	slab.write_framed(
		tenant(),
		key,
		hlc(78),
		crate::schema::record_class::COUNTER_COMMIT_V2,
		b"truncated origin frame",
	)
	.await
	.unwrap();
	let source_id = manager.force_seal_active_for_test().await;
	let path = slab_path(&dir, source_id);
	let source_before = std::fs::read(&path).unwrap();
	let manifest_before = manager.compaction_manifest_snapshot();
	let schemas = TableSchemaCatalog::default();
	let indexes = SecondaryIndexCatalog::default();
	let result = manager
		.compact_one_sealed_slab(
			source_id,
			&[],
			SecondaryIndexCompactionContext {
				table_schemas: &schemas,
				secondary_indexes: &indexes,
			},
		)
		.await;
	assert!(result.is_err(), "malformed V2 input must fail closed");
	assert_eq!(std::fs::read(&path).unwrap(), source_before);
	let manifest_after = manager.compaction_manifest_snapshot();
	assert_eq!(
		manifest_after.manifest_epoch,
		manifest_before.manifest_epoch
	);
	assert_eq!(
		manifest_after.run(source_id),
		manifest_before.run(source_id)
	);
	assert_eq!(
		std::fs::read_dir(&*dir)
			.unwrap()
			.filter_map(Result::ok)
			.filter(|entry| entry.file_name().to_string_lossy().ends_with(".wnd"))
			.count(),
		2,
		"malformed admission must not create a replacement"
	);
}

#[tokio::test]
async fn fctr7_lsm_rewrite_refuses_counter_input_before_output_creation() {
	use crate::storage::counter::CounterReplicaService;
	use crate::storage::lsm::CompactionFailure;
	use crate::storage::maintenance_policy::{
		MaintenanceFailureClass, classify_compaction_failure,
	};

	let (dir, manager) = fresh_manager().await;
	let key = id(0x5a31);
	let origin = NodeId {
		id: 0x5a32,
		_reserved: 0,
	};
	let service = CounterReplicaService::new(manager.clone());
	service
		.replicate_tentative(
			origin,
			42,
			tenant(),
			key,
			42,
			HybridTimestamp {
				physical_ms: 42,
				logical: 0,
				node_id: 0,
			},
		)
		.await
		.unwrap();
	service.commit(origin, 42, tenant(), key).await.unwrap();
	let counter_slab = manager.force_seal_active_for_test().await;
	let ordinary_key = id(0x5a33);
	manager
		.append_durable(tenant(), ordinary_key, hlc(43), b"ordinary LSM row")
		.await
		.unwrap();
	manager.force_seal_active_for_test().await;
	for n in 0..3 {
		manager
			.append_durable(
				tenant(),
				id(0x5a40 + n),
				hlc(44 + n),
				b"another ordinary LSM row",
			)
			.await
			.unwrap();
		manager.force_seal_active_for_test().await;
	}
	let before_manifest = manager.compaction_manifest_snapshot();
	let mut before_files = std::fs::read_dir(&*dir)
		.unwrap()
		.filter_map(Result::ok)
		.map(|entry| entry.path())
		.filter(|path| path.extension().is_some_and(|extension| extension == "wnd"))
		.map(|path| {
			(
				path.file_name().unwrap().to_owned(),
				std::fs::read(path).unwrap(),
			)
		})
		.collect::<Vec<_>>();
	before_files.sort_by(|a, b| a.0.cmp(&b.0));
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let error = manager
		.run_lsm_compaction_tick(&worker, hlc(20_000))
		.await
		.expect_err("multi-source LSM must defer counter-bearing input before output preparation");
	assert!(matches!(
		error,
		CompactionFailure::CounterMaintenanceSkipped
	));
	assert_eq!(
		classify_compaction_failure(&error),
		MaintenanceFailureClass::Retryable,
		"the refusal must return through the existing retryable policy"
	);
	assert_eq!(
		manager.compaction_manifest_snapshot().manifest_epoch,
		before_manifest.manifest_epoch
	);
	let mut after_files = std::fs::read_dir(&*dir)
		.unwrap()
		.filter_map(Result::ok)
		.map(|entry| entry.path())
		.filter(|path| path.extension().is_some_and(|extension| extension == "wnd"))
		.map(|path| {
			(
				path.file_name().unwrap().to_owned(),
				std::fs::read(path).unwrap(),
			)
		})
		.collect::<Vec<_>>();
	after_files.sort_by(|a, b| a.0.cmp(&b.0));
	assert_eq!(after_files, before_files, "LSM refusal changed slab files");
	let retryable = manager.maintenance_policy().report().retryable;
	assert_eq!(
		retryable.len(),
		1,
		"the counter-bearing plan is counted once"
	);
	assert_eq!(retryable[0].consecutive_failures, 1);
	assert!(manager.counter_deferred_l0_slabs.contains(&counter_slab));
	let next_plan = manager
		.pick_lsm_compaction_plan(&worker, hlc(20_001))
		.expect("ordinary L0 inputs remain eligible after the counter deferral");
	assert_eq!(next_plan.input_slabs.len(), 4);
	assert!(!next_plan.input_slabs.contains(&counter_slab));
	let outcome = manager
		.run_lsm_compaction_tick(&worker, hlc(20_001))
		.await
		.unwrap()
		.expect("ordinary L0 runs compact on the next picker tick");
	assert!(outcome.rows_out > 0);
	assert_eq!(
		service.read_committed(tenant(), key).await.unwrap(),
		Some(42)
	);
	manager.fsync_all_local_slabs().await.unwrap();
	drop(service);
	drop(manager);
	let reopened = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let recovered = CounterReplicaService::new(reopened);
	assert_eq!(
		recovered.read_committed(tenant(), key).await.unwrap(),
		Some(42)
	);
}

#[tokio::test]
async fn fctr5a_lsm_compacts_tiered_ordinary_l0_with_local_runs() {
	let l1_dir = fresh_dir("fctr5a-lsm-l2-l1");
	let l2_dir = fresh_dir("fctr5a-lsm-l2-l2");
	let io = spawn_io_thread(64).unwrap();
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1,
			io: io.clone(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	manager
		.append_durable(tenant(), id(0x5a50), hlc(10), b"tiered ordinary row")
		.await
		.unwrap();
	let tiered_slab = manager.force_seal_active_for_test().await;
	let raw = manager.read_sealed_raw(tiered_slab).await.unwrap();
	let encrypted = manager
		.encrypt_tiered_object_blob(tiered_slab, &raw)
		.unwrap();
	let payload_hash = crate::hash::content::digest(&encrypted.bytes);
	let uri = l2
		.put(
			&format!("sealed/slab_{tiered_slab:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	manager
		.apply_tier_slab(TieredSlabPointer {
			slab_id: tiered_slab,
			l2_uri: uri,
			payload_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();
	assert!(manager.is_tiered(tiered_slab).await);

	for n in 0..3 {
		manager
			.append_durable(tenant(), id(0x5a51 + n), hlc(11 + n), b"local ordinary row")
			.await
			.unwrap();
		manager.force_seal_active_for_test().await;
	}
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let plan = manager
		.pick_lsm_compaction_plan(&worker, hlc(20_000))
		.expect("four L0 runs including the tiered ordinary run are eligible");
	assert!(plan.input_slabs.contains(&tiered_slab));
	let outcome = manager
		.run_lsm_compaction_tick(&worker, hlc(20_000))
		.await
		.unwrap()
		.expect("counter-free L2 source must not stall ordinary LSM work");
	assert_eq!(outcome.rows_out, 4);
}

// Acceptance: manager rollback removes the live tentative index entry only
// (no durable abort marker, no effect on prior committed state).
#[tokio::test]
async fn counter_manager_rollback_removes_live_tentative_only() {
	let dir = fresh_dir("adr075-counter-rollback");
	let mgr = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let store = SlabCounterStore::new(
		mgr,
		tenant(),
		NodeId {
			id: 1,
			_reserved: 0,
		},
	);
	let k = ckey(b"verdict");
	let t = store.tentative_append(&k, 5, hlc(1)).await.unwrap();
	assert_eq!(store.read_committed(&k).await.unwrap(), Some(5));
	store.rollback_tentative(t);
	assert_eq!(
		store.read_committed(&k).await.unwrap(),
		None,
		"rollback removed the live tentative from the manager index"
	);
}

// Acceptance: a marker-write failure rolls back the live tentative (caller
// decides) and recovery skips the orphan; a prior committed value survives.
#[tokio::test]
async fn counter_manager_marker_failure_rolls_back_and_recovery_skips_orphan() {
	let dir = fresh_dir("adr075-counter-markerfail");
	let crypto = test_crypto();
	let k = ckey(b"verdict");
	{
		let mgr = open_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			crypto.clone(),
		)
		.await;
		let store = SlabCounterStore::new(
			mgr.clone(),
			tenant(),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		);
		let t1 = store.tentative_append(&k, 3, hlc(1)).await.unwrap();
		store.commit_tentative(t1).await.unwrap();
		let t2 = store.tentative_append(&k, 50, hlc(2)).await.unwrap();
		assert_eq!(store.read_committed(&k).await.unwrap(), Some(50));
		store.arm_commit_marker_failure();
		assert!(store.commit_tentative(t2).await.is_err());
		store.rollback_tentative(t2); // terminal caller aborts
		assert_eq!(store.read_committed(&k).await.unwrap(), Some(3));
		mgr.fsync_all_local_slabs().await.unwrap();
	}
	let mgr = open_manager_at_dir(dir.to_path_buf(), spawn_io_thread(64).unwrap(), crypto).await;
	let store = SlabCounterStore::new(
		mgr,
		tenant(),
		NodeId {
			id: 1,
			_reserved: 0,
		},
	);
	assert_eq!(
		store.read_committed(&k).await.unwrap(),
		Some(3),
		"the marker-failed orphan (50) did not resurrect; committed (3) survived"
	);
}

// Acceptance: ordinary blob writes/recovery are unchanged when counters share
// the manager — the blob round-trips and the counter recovers, independently.
#[tokio::test]
async fn counter_and_blob_coexist_blob_recovery_unchanged() {
	let dir = fresh_dir("adr075-counter-blob");
	let crypto = test_crypto();
	let blob_id = Identity::new([0xBB; 32]);
	let blob_value = b"ordinary-blob-value".to_vec();
	let k = ckey(b"verdict");
	{
		let mgr = open_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			crypto.clone(),
		)
		.await;
		mgr.append_durable(tenant(), blob_id, hlc(1), &blob_value)
			.await
			.unwrap();
		let store = SlabCounterStore::new(
			mgr.clone(),
			tenant(),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		);
		let t = store.tentative_append(&k, 7, hlc(2)).await.unwrap();
		store.commit_tentative(t).await.unwrap();
		mgr.fsync_all_local_slabs().await.unwrap();
	}
	let mgr = open_manager_at_dir(dir.to_path_buf(), spawn_io_thread(64).unwrap(), crypto).await;
	// Ordinary blob recovers and reads back byte-identical.
	let loc = mgr
		.lookup(tenant(), blob_id)
		.expect("blob must be re-indexed");
	assert_eq!(mgr.read(loc).await.unwrap(), blob_value);
	// The counter recovers independently.
	let store = SlabCounterStore::new(
		mgr,
		tenant(),
		NodeId {
			id: 1,
			_reserved: 0,
		},
	);
	assert_eq!(store.read_committed(&k).await.unwrap(), Some(7));
}

// ----- cut-3b: the identity-based server replica (CounterReplicaService) -----

use crate::storage::counter::CounterReplicaService;

// 3b acceptance: handler-level replicate creates a live tentative; restart
// skips it without a commit marker.
#[tokio::test]
async fn counter_replica_service_replicate_live_then_restart_skips_without_marker() {
	use crate::storage::counter::CounterSlab;
	let dir = fresh_dir("adr075-replica-svc-orphan");
	let crypto = test_crypto();
	let id = counter_identity(&ckey(b"verdict"));
	{
		let mgr = open_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			crypto.clone(),
		)
		.await;
		let svc = CounterReplicaService::new(mgr.clone());
		svc.replicate_tentative(
			NodeId {
				id: 1,
				_reserved: 0,
			},
			1,
			tenant(),
			id,
			5,
			HybridTimestamp {
				physical_ms: 1,
				logical: 0,
				node_id: 0,
			},
		)
		.await
		.unwrap();
		assert_eq!(
			svc.read_committed(tenant(), id).await.unwrap(),
			None,
			"unmarked tentative is not committed"
		);
		assert_eq!(
			mgr.counter_read_value(mgr.counter_lookup(tenant(), id).unwrap())
				.await
				.unwrap(),
			5
		);
		mgr.fsync_all_local_slabs().await.unwrap();
	}
	let mgr = open_manager_at_dir(dir.to_path_buf(), spawn_io_thread(64).unwrap(), crypto).await;
	let svc = CounterReplicaService::new(mgr);
	assert_eq!(
		svc.read_committed(tenant(), id).await.unwrap(),
		None,
		"un-committed tentative skipped on restart"
	);
}

// 3b acceptance: handler-level rollback removes only the matching tentative
// op and is idempotent.
#[tokio::test]
async fn counter_replica_service_rollback_removes_matching_op_idempotent() {
	use crate::storage::counter::CounterSlab;
	let dir = fresh_dir("adr075-replica-svc-rollback");
	let mgr = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let svc = CounterReplicaService::new(mgr.clone());
	let id = counter_identity(&ckey(b"verdict"));
	svc.replicate_tentative(
		NodeId {
			id: 1,
			_reserved: 0,
		},
		1,
		tenant(),
		id,
		5,
		HybridTimestamp {
			physical_ms: 1,
			logical: 0,
			node_id: 0,
		},
	)
	.await
	.unwrap();
	svc.replicate_tentative(
		NodeId {
			id: 1,
			_reserved: 0,
		},
		2,
		tenant(),
		id,
		9,
		HybridTimestamp {
			physical_ms: 2,
			logical: 0,
			node_id: 0,
		},
	)
	.await
	.unwrap();
	assert_eq!(svc.pending_count(), 2);
	svc.rollback(
		NodeId {
			id: 1,
			_reserved: 0,
		},
		1,
		tenant(),
		id,
	)
	.await
	.unwrap();
	assert_eq!(svc.pending_count(), 1, "only op 1 removed");
	// op 2's tentative (value 9, higher HLC) remains the live latest.
	assert_eq!(svc.read_committed(tenant(), id).await.unwrap(), None);
	assert_eq!(
		mgr.counter_read_value(mgr.counter_lookup(tenant(), id).unwrap())
			.await
			.unwrap(),
		9
	);
	// Idempotent: rolling back op 1 again is a harmless no-op.
	svc.rollback(
		NodeId {
			id: 1,
			_reserved: 0,
		},
		1,
		tenant(),
		id,
	)
	.await
	.unwrap();
	assert_eq!(svc.pending_count(), 1);
}

// 3b: the COMMIT phase writes the marker so the replicated value survives
// restart (the positive twin).
#[tokio::test]
async fn counter_replica_service_commit_makes_value_survive_restart() {
	let dir = fresh_dir("adr075-replica-svc-commit");
	let crypto = test_crypto();
	let id = counter_identity(&ckey(b"verdict"));
	{
		let mgr = open_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			crypto.clone(),
		)
		.await;
		let svc = CounterReplicaService::new(mgr.clone());
		svc.replicate_tentative(
			NodeId {
				id: 1,
				_reserved: 0,
			},
			7,
			tenant(),
			id,
			42,
			HybridTimestamp {
				physical_ms: 7,
				logical: 0,
				node_id: 0,
			},
		)
		.await
		.unwrap();
		svc.commit(
			NodeId {
				id: 1,
				_reserved: 0,
			},
			7,
			tenant(),
			id,
		)
		.await
		.unwrap();
		assert_eq!(svc.pending_count(), 0, "committed op left the pending set");
		mgr.fsync_all_local_slabs().await.unwrap();
	}
	let mgr = open_manager_at_dir(dir.to_path_buf(), spawn_io_thread(64).unwrap(), crypto).await;
	let svc = CounterReplicaService::new(mgr);
	assert_eq!(svc.read_committed(tenant(), id).await.unwrap(), Some(42));
}

// ----- cut-3: the leader drives the production participant set
// (CounterParticipantKind) over the REAL CounterReplicaService -----

use crate::storage::counter::{
	CounterAddOutcome, CounterAddResult, CounterLeader, CounterParticipantKind,
	CounterParticipantPlan, CounterWriteKey, DurableCounterWitnessStore, LocalCounterParticipant,
};

/// Canonical write-key fixture: the test's small id, LE-embedded in 24 bytes.
fn wkey(n: u64) -> CounterWriteKey {
	let mut key = [0u8; 24];
	key[..8].copy_from_slice(&n.to_le_bytes());
	key
}

// cut-3 acceptance: a `CounterLeader` over the production `CounterParticipantKind`
// — whose sole wired arm is the leader's own `LocalCounterParticipant` over a REAL
// `CounterReplicaService` — applies an add, and the committed value is visible
// through the service's read path at the EXACT (tenant, counter_identity) the
// adapter is required to derive. The assertion reads the service INDEPENDENTLY of
// the adapter (the test derives the identity + names the tenant itself), so a
// broken `counter_identity(key)` derivation or a tenant mismatch in
// `LocalCounterParticipant` would land the value under different coordinates and
// this read would see `None`.
#[tokio::test]
async fn leader_over_real_replica_service_commits_value_visible_through_service() {
	let dir = fresh_dir("adr075-leader-real-svc");
	let wdir = fresh_dir("adr075-leader-real-svc-witness");
	let mgr = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let service = Arc::new(CounterReplicaService::new(mgr));

	// The production participant set: the leader's own local arm over the real
	// service, pinned to `tenant()`. Quorum 1 = the lone local arm.
	let local_id = NodeId {
		id: 1,
		_reserved: 0,
	};
	let local = CounterParticipantKind::Local(LocalCounterParticipant::new(
		service.clone(),
		tenant(),
		local_id,
	));
	let leader = CounterLeader::new(
		local_id,
		local.clone(),
		true,
		DurableCounterWitnessStore::open(&*wdir).unwrap(),
	);
	let plan = || CounterParticipantPlan::new(vec![(local_id, local.clone())], 1);

	let k = ckey(b"verdict");
	let out = leader.fetch_add(plan(), wkey(1), &k, 7).await.unwrap();
	assert_eq!(
		out,
		CounterAddResult::Applied(CounterAddOutcome {
			previous: 0,
			current: 7
		})
	);
	// Independent of the adapter's own derivation: read the real service at the
	// coordinates the adapter was REQUIRED to write to.
	assert_eq!(
		service
			.read_committed(tenant(), counter_identity(&k))
			.await
			.unwrap(),
		Some(7),
		"the committed add is visible through the real service at the derived identity+tenant"
	);

	// A second add accumulates on the same key through the same path (proves the
	// leader's committed base read also routes through the adapter's derivation).
	let out2 = leader.fetch_add(plan(), wkey(2), &k, 4).await.unwrap();
	assert_eq!(
		out2,
		CounterAddResult::Applied(CounterAddOutcome {
			previous: 7,
			current: 11
		})
	);
	assert_eq!(
		service
			.read_committed(tenant(), counter_identity(&k))
			.await
			.unwrap(),
		Some(11),
	);
}

async fn force_seal_active(manager: &Arc<SlabManager>) -> SlabId {
	let mut state = manager.state.lock().await;
	let sealed = state.active.clone();
	sealed.fsync().await.unwrap();
	let sealed_id = sealed.slab_id();
	let next_id = state.next_slab_id;
	let next = Arc::new(
		LocalSlabStore::open(
			slab_path(&manager.config.dir, next_id),
			next_id,
			manager.file_ops.clone(),
			manager.config.group_commit.clone(),
		)
		.await
		.unwrap(),
	);
	state.active = next;
	state.sealed.insert(sealed_id, sealed);
	state.next_slab_id = next_id.saturating_add(1);
	sealed_id
}

fn secondary_index_table_descriptor_for_test() -> TableSchemaDescriptor {
	let table_object_id = TableObjectId::new([0x52; 16]);
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("age", DataType::Int64, true),
	]);
	TableSchemaDescriptor::new_with_layout(
		table_object_id,
		1,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn secondary_index_descriptor_for_test() -> SecondaryIndexDescriptor {
	let table = secondary_index_table_descriptor_for_test();
	SecondaryIndexDescriptor {
		index_object_id: IndexObjectId::new([0x9A; 16]),
		table_object_id: table.table_object_id,
		namespace: table.namespace.clone(),
		index_name: "events_age_idx".to_owned(),
		table_schema_generation: table.schema_generation,
		logical_schema_fingerprint: table.logical_schema_fingerprint,
		key: SecondaryIndexKeyDescriptor {
			column_name: "age".to_owned(),
			column_ordinal: 1,
			arrow_type_tag: IndexArrowTypeTag::Int64,
			encoding: IndexKeyEncoding::OrderedScalar,
		},
	}
}

fn secondary_index_encoded_i64_for_test(value: i64) -> Vec<u8> {
	((value as u64) ^ 0x8000_0000_0000_0000)
		.to_be_bytes()
		.to_vec()
}

fn secondary_index_storage_batch_for_test(
	ages: Vec<Option<i64>>,
	row_hlc: HybridTimestamp,
	identity_byte: u8,
) -> RecordBatch {
	let ids = Int64Array::from_iter_values(0..ages.len() as i64);
	let ages = Int64Array::from(ages);
	let batch = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("age", DataType::Int64, true),
		])),
		vec![Arc::new(ids), Arc::new(ages)],
	)
	.unwrap();
	crate::storage::append_storage_synthetic_columns(
		&batch,
		&(0..batch.num_rows()).collect::<Vec<_>>(),
		&crate::schema::TableWriteAdmission {
			canonical_primary_keys: vec![vec![identity_byte]; batch.num_rows()],
			key_identities: vec![Identity::new([identity_byte; 32]); batch.num_rows()],
			localities: vec![Locality::new(11); batch.num_rows()],
		},
		row_hlc,
		Some(TableObjectId::new([0x52; 16])),
	)
	.unwrap()
}

fn secondary_index_user_batch_for_test(id_value: i64, age: Option<i64>) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("age", DataType::Int64, true),
		])),
		vec![
			Arc::new(Int64Array::from(vec![id_value])) as Arc<dyn Array>,
			Arc::new(Int64Array::from(vec![age])) as Arc<dyn Array>,
		],
	)
	.unwrap()
}

async fn append_secondary_index_table_row_for_test(
	manager: &Arc<SlabManager>,
	table: &TableSchemaDescriptor,
	id_value: i64,
	age: Option<i64>,
	row_hlc: HybridTimestamp,
) -> Identity {
	let batch = secondary_index_user_batch_for_test(id_value, age);
	let meta = layout_recluster_meta(table, batch.num_rows());
	let packed =
		pack_record_batch_by_bucket(&meta, table, &batch, WriterNodeId::ZERO, row_hlc).unwrap();
	assert_eq!(packed.len(), 1);
	let record = &packed[0];
	let entries = storage_multirow_index_entries_from_frame(&record.frame_bytes)
		.unwrap()
		.unwrap();
	let row_identity = entries[0].key_identity;
	manager
		.append_durable_multirow_with_locality(tenant(), &record.frame_bytes, &entries)
		.await
		.unwrap();
	row_identity
}

fn secondary_index_segment_for_test(source_slab_id: SlabId) -> SecondaryIndexSegment {
	let descriptor = secondary_index_descriptor_for_test();
	SecondaryIndexSegment::build(
		SecondaryIndexIdentity {
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			index_object_id: descriptor.index_object_id,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			column_ordinal: 1,
		},
		source_slab_id,
		descriptor.table_schema_generation,
		IndexArrowTypeTag::Int64,
		vec![SecondaryIndexEntry {
			encoded_key: ((42_i64 as u64) ^ 0x8000_0000_0000_0000)
				.to_be_bytes()
				.to_vec(),
			row_key_id: id(42),
			row_hlc: hlc(42),
			record_location: RecordLocation {
				slab_id: source_slab_id,
				offset: 128,
				length: 256,
				hlc: hlc(42),
				record_row_index: RecordRowIndex::ZERO,
				is_tombstone: false,
				placement: PlacementSemantics::RawLocality,
				partition: None,
			},
		}],
	)
	.unwrap()
}

#[tokio::test]
async fn secondary_index_segment_catalog_persists_loads_and_deletes() {
	let dir = fresh_dir("secondary-index-sidecar");
	let io = spawn_io_thread(64).unwrap();
	let crypto = test_crypto();
	let manager = open_manager_at_dir(dir.to_path_buf(), io, crypto.clone()).await;
	let segment = secondary_index_segment_for_test(SlabId::new(41));
	let info = manager
		.persist_secondary_index_segment(&segment)
		.await
		.unwrap();

	let listed = manager.list_secondary_index_segments(
		info.tenant_id,
		info.table_object_id,
		info.index_object_id,
		info.logical_schema_fingerprint,
	);
	assert_eq!(listed, vec![info.clone()]);
	let path = dir.join(&info.segment_path);
	assert!(path.exists());
	assert_eq!(
		manager
			.load_secondary_index_segment(&info)
			.await
			.unwrap()
			.unwrap(),
		segment
	);
	let coverage = manager
		.secondary_index_coverage_snapshot(info.coverage_key())
		.unwrap();
	assert_eq!(coverage.covered_slabs, vec![info.source_slab_id]);
	assert!(coverage.uncovered_slabs.is_empty());

	drop(manager);
	let reopened =
		open_manager_at_dir(dir.to_path_buf(), spawn_io_thread(64).unwrap(), crypto).await;
	assert_eq!(
		reopened.list_secondary_index_segments(
			info.tenant_id,
			info.table_object_id,
			info.index_object_id,
			info.logical_schema_fingerprint,
		),
		vec![info.clone()]
	);
	assert_eq!(
		reopened
			.load_secondary_index_segment(&info)
			.await
			.unwrap()
			.unwrap(),
		segment
	);
	let coverage = reopened
		.secondary_index_coverage_snapshot(info.coverage_key())
		.unwrap();
	assert_eq!(coverage.covered_slabs, vec![info.source_slab_id]);
	assert!(
		reopened
			.delete_secondary_index_segment(&info)
			.await
			.unwrap()
	);
	assert!(!path.exists());
	assert!(
		reopened
			.list_secondary_index_segments(
				info.tenant_id,
				info.table_object_id,
				info.index_object_id,
				info.logical_schema_fingerprint,
			)
			.is_empty()
	);
	let coverage = reopened
		.secondary_index_coverage_snapshot(info.coverage_key())
		.unwrap();
	assert!(coverage.covered_slabs.is_empty());
	// Deletion is lifecycle, not failure: no failure record, no spent retry budget —
	// the slab simply stops being covered, which re-opens the completeness gap and
	// returns it to backfill candidacy.
	assert!(coverage.uncovered_slabs.is_empty());
}

#[tokio::test]
async fn recovery_fences_missing_secondary_index_source_slab() {
	let dir = fresh_dir("adr0539-secondary-source-recovery");
	let crypto = test_crypto();
	let manager = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		crypto.clone(),
	)
	.await;
	let segment = secondary_index_segment_for_test(SlabId::new(5_039));
	let info = manager
		.persist_secondary_index_segment(&segment)
		.await
		.unwrap();
	drop(manager);

	let reopened =
		open_manager_at_dir(dir.to_path_buf(), spawn_io_thread(64).unwrap(), crypto).await;
	assert_eq!(
		reopened.table_integrity_failure(info.tenant_id, info.table_object_id),
		Some(PublishedReferenceIntegrityFailure {
			tenant_id: info.tenant_id,
			table_object_id: info.table_object_id,
			reference_kind: PublishedReferenceKind::SecondaryIndexSourceSlab,
			missing_slab_id: info.source_slab_id,
			origin: PublishedReferenceOrigin::Recovery,
		})
	);
	assert!(reopened.is_compaction_slab_quarantined(info.source_slab_id));
}

#[tokio::test]
async fn secondary_index_corrupt_sidecar_marks_uncovered_and_fails_closed() {
	let dir = fresh_dir("secondary-index-corrupt-sidecar");
	let io = spawn_io_thread(64).unwrap();
	let crypto = test_crypto();
	let manager = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	let segment = secondary_index_segment_for_test(SlabId::new(41));
	let info = manager
		.persist_secondary_index_segment(&segment)
		.await
		.unwrap();
	std::fs::write(
		dir.join(&info.segment_path),
		b"not a secondary index segment",
	)
	.unwrap();

	let err = manager
		.load_secondary_index_segment(&info)
		.await
		.unwrap_err();
	assert!(err.to_string().contains("decode secondary index segment"));
	let coverage = manager
		.secondary_index_coverage_snapshot(info.coverage_key())
		.unwrap();
	assert!(coverage.covered_slabs.is_empty());
	assert_eq!(coverage.uncovered_slabs[0].0, info.source_slab_id);
	assert_eq!(
		coverage.uncovered_slabs[0].1.reason,
		SecondaryIndexBackfillErrorReason::CorruptSegment
	);
}

#[tokio::test]
async fn secondary_index_drop_gc_deletes_sidecars_and_clears_local_coverage() {
	let dir = fresh_dir("secondary-index-drop-gc");
	let io = spawn_io_thread(64).unwrap();
	let crypto = test_crypto();
	let manager = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	let descriptor = secondary_index_descriptor_for_test();
	let first = manager
		.persist_secondary_index_segment(&secondary_index_segment_for_test(SlabId::new(41)))
		.await
		.unwrap();
	let second = manager
		.persist_secondary_index_segment(&secondary_index_segment_for_test(SlabId::new(42)))
		.await
		.unwrap();

	let removed = manager
		.gc_secondary_index_segments_for_dropping_index(tenant(), &descriptor)
		.await
		.unwrap();

	assert_eq!(removed, 2);
	assert!(!dir.join(&first.segment_path).exists());
	assert!(!dir.join(&second.segment_path).exists());
	assert!(
		manager
			.list_secondary_index_segments(
				tenant(),
				descriptor.table_object_id,
				descriptor.index_object_id,
				descriptor.logical_schema_fingerprint,
			)
			.is_empty()
	);
	assert!(
		manager
			.secondary_index_coverage_snapshot(first.coverage_key())
			.is_none()
	);
}

#[tokio::test]
async fn adr0095_slab_rewrite_claim_serializes_both_rewrite_orders() {
	let (_manager_dir, manager) = fresh_manager().await;
	let source_slab_id = force_seal_active(&manager).await;
	let source_slab_lease = manager
		.acquire_slab_rewrite_read_claim(source_slab_id)
		.await
		.unwrap();

	let blocked = tokio::time::timeout(
		Duration::from_millis(20),
		manager.acquire_slab_rewrite_exclusive_claim(source_slab_id),
	)
	.await;
	assert!(
		blocked.is_err(),
		"compaction claim should wait while backfill holds the source_slab_lease"
	);

	drop(source_slab_lease);
	let compaction_lease = tokio::time::timeout(
		Duration::from_secs(1),
		manager.acquire_slab_rewrite_exclusive_claim(source_slab_id),
	)
	.await
	.expect("compaction lease should become available after backfill releases");
	let blocked_reader = tokio::time::timeout(
		Duration::from_millis(20),
		manager.acquire_slab_rewrite_read_claim(source_slab_id),
	)
	.await;
	assert!(
		blocked_reader.is_err(),
		"a backfill/read-side rewrite cannot enter while reclaim or compaction owns the slab"
	);
	drop(compaction_lease);
	let reader = tokio::time::timeout(
		Duration::from_secs(1),
		manager.acquire_slab_rewrite_read_claim(source_slab_id),
	)
	.await
	.expect("read-side rewrite proceeds after the exclusive owner leaves")
	.unwrap();
	drop(reader);
}

#[tokio::test]
async fn secondary_index_coverage_plan_recovers_covered_and_enqueues_missing_sealed_slabs() {
	let (_manager_dir, manager) = fresh_manager().await;
	let first_sealed = force_seal_active(&manager).await;
	let second_sealed = force_seal_active(&manager).await;
	let descriptor = secondary_index_descriptor_for_test();
	let info = manager
		.persist_secondary_index_segment(&secondary_index_segment_for_test(first_sealed))
		.await
		.unwrap();

	let plan = manager
		.ensure_secondary_index_coverage(tenant(), &descriptor, 4096)
		.await
		.unwrap();

	assert_eq!(plan.snapshot.covered_slabs, vec![info.source_slab_id]);
	assert_eq!(
		plan.snapshot.active_buffer.unwrap().source_slab_id,
		SlabId::new(2)
	);
	assert_eq!(plan.candidates.len(), 1);
	assert_eq!(plan.candidates[0].key(), info.coverage_key());
	assert_eq!(plan.candidates[0].source_slab_id(), second_sealed);
}

#[tokio::test]
async fn secondary_index_active_buffer_seal_persists_sidecar_and_marks_covered() {
	let (_manager_dir, manager) = fresh_manager().await;
	let descriptor = secondary_index_descriptor_for_test();
	let active_slab_id = manager.active_slab_id().await;
	let row_hlc = hlc(50);
	let base_location = RecordLocation {
		slab_id: active_slab_id,
		offset: 64,
		length: 512,
		hlc: row_hlc,
		record_row_index: RecordRowIndex::ZERO,
		is_tombstone: false,
		placement: PlacementSemantics::RawLocality,
		partition: None,
	};

	let appended = manager
		.append_secondary_index_active_buffer_batch(
			tenant(),
			&descriptor,
			&secondary_index_storage_batch_for_test(vec![Some(42)], row_hlc, 7),
			base_location,
			4096,
		)
		.unwrap();
	assert_eq!(appended, 1);

	let info = manager
		.seal_secondary_index_active_buffer(tenant(), &descriptor)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(info.source_slab_id, active_slab_id);
	assert_eq!(info.entry_count, 1);
	let loaded = manager
		.load_secondary_index_segment(&info)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(loaded.entries.len(), 1);
	assert_eq!(loaded.entries[0].row_hlc, row_hlc);
	let coverage = manager
		.secondary_index_coverage_snapshot(info.coverage_key())
		.unwrap();
	assert_eq!(coverage.covered_slabs, vec![active_slab_id]);
	assert!(coverage.active_buffer.is_none());
}

#[tokio::test]
async fn secondary_index_declaration_cut_backfills_the_complete_old_active_slab() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	let old_active = manager.active_slab_id().await;

	// This row predates publication and therefore has no active-index entry.
	append_secondary_index_table_row_for_test(&manager, &table, 1, Some(10), hlc(10)).await;
	// This row models a write admitted after Building became visible but before
	// the lifecycle driver acquired the declaration cut.
	append_secondary_index_table_row_for_test(&manager, &table, 2, Some(20), hlc(20)).await;
	manager
		.append_secondary_index_active_buffer_entries(
			tenant(),
			&descriptor,
			vec![SecondaryIndexEntry {
				encoded_key: secondary_index_encoded_i64_for_test(20),
				row_key_id: Identity::new([2; 32]),
				row_hlc: hlc(20),
				record_location: RecordLocation {
					slab_id: old_active,
					offset: 0,
					length: 1,
					hlc: hlc(20),
					record_row_index: RecordRowIndex::ZERO,
					is_tombstone: false,
					placement: PlacementSemantics::RawLocality,
					partition: None,
				},
			}],
			old_active,
			4096,
		)
		.unwrap();

	// An in-flight write owns a read admission; the cut cannot pass it.
	let admission = manager.acquire_secondary_index_admission_guard().await;
	let cut_manager = manager.clone();
	let cut_descriptor = descriptor.clone();
	let cut = tokio::spawn(async move {
		cut_manager
			.establish_secondary_index_declaration_cut(
				tenant(),
				&cut_descriptor,
				std::slice::from_ref(&cut_descriptor),
				4096,
			)
			.await
	});
	tokio::task::yield_now().await;
	assert!(
		!cut.is_finished(),
		"declaration cut passed an admitted write"
	);
	drop(admission);
	assert_eq!(cut.await.unwrap().unwrap(), old_active);
	assert_ne!(manager.active_slab_id().await, old_active);

	let report = manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.unwrap();
	assert!(report.historical_complete);
	let infos = manager.list_secondary_index_segments(
		tenant(),
		descriptor.table_object_id,
		descriptor.index_object_id,
		descriptor.logical_schema_fingerprint,
	);
	let old_segment = infos
		.iter()
		.find(|info| info.source_slab_id == old_active)
		.expect("declaration cut must backfill the sealed source slab");
	let segment = manager
		.load_secondary_index_segment(old_segment)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(
		segment.entries.len(),
		2,
		"sealing the suffix-only active buffer would omit the pre-declaration row"
	);

	// A second declaration must rotate without stranding the first index's
	// complete active buffer on the newly historical slab.
	let middle_active = manager.active_slab_id().await;
	append_secondary_index_table_row_for_test(&manager, &table, 3, Some(30), hlc(30)).await;
	manager
		.append_secondary_index_active_buffer_entries(
			tenant(),
			&descriptor,
			vec![SecondaryIndexEntry {
				encoded_key: secondary_index_encoded_i64_for_test(30),
				row_key_id: Identity::new([3; 32]),
				row_hlc: hlc(30),
				record_location: RecordLocation {
					slab_id: middle_active,
					offset: 0,
					length: 1,
					hlc: hlc(30),
					record_row_index: RecordRowIndex::ZERO,
					is_tombstone: false,
					placement: PlacementSemantics::RawLocality,
					partition: None,
				},
			}],
			middle_active,
			4096,
		)
		.unwrap();
	let mut second_descriptor = descriptor.clone();
	second_descriptor.index_object_id = IndexObjectId::new([0x9B; 16]);
	second_descriptor.index_name = "events_age_idx_second".to_owned();
	manager
		.establish_secondary_index_declaration_cut(
			tenant(),
			&second_descriptor,
			&[descriptor.clone(), second_descriptor.clone()],
			4096,
		)
		.await
		.unwrap();
	let first_index_middle = manager
		.list_secondary_index_segments(
			tenant(),
			descriptor.table_object_id,
			descriptor.index_object_id,
			descriptor.logical_schema_fingerprint,
		)
		.into_iter()
		.find(|info| info.source_slab_id == middle_active)
		.expect("the established sibling buffer must seal before another declaration rotates");
	assert_eq!(first_index_middle.entry_count, 1);
}

#[tokio::test]
async fn secondary_index_backfill_source_slab_emits_every_retained_data_version() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	let row_identity =
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(42), hlc(10)).await;
	assert_eq!(
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(43), hlc(20)).await,
		row_identity
	);
	assert_eq!(
		append_secondary_index_table_row_for_test(&manager, &table, 7, None, hlc(30)).await,
		row_identity
	);
	let source_slab_id = force_seal_active(&manager).await;
	let plan = manager
		.ensure_secondary_index_coverage(tenant(), &descriptor, 4096)
		.await
		.unwrap();
	// Inspected rather than compared against a literal: a candidate carries
	// unforgeable provenance, so only enumeration can build one.
	assert_eq!(plan.candidates.len(), 1);
	assert_eq!(
		plan.candidates[0].key(),
		SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
			tenant(),
			&descriptor,
		))
	);
	assert_eq!(plan.candidates[0].source_slab_id(), source_slab_id);

	let SecondaryIndexBackfillOutcome::Completed(info) = manager
		.backfill_secondary_index_candidate(tenant(), &table, &descriptor, &plan.candidates[0])
		.await
		.unwrap()
	else {
		panic!("nothing replaced the source, so the candidate cannot be stale");
	};
	assert_eq!(info.source_slab_id, source_slab_id);
	assert_eq!(info.entry_count, 2);
	let segment = manager
		.load_secondary_index_segment(&info)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(segment.entries.len(), 2);
	assert_eq!(segment.entries[0].row_key_id, row_identity);
	assert_eq!(segment.entries[1].row_key_id, row_identity);
	assert_eq!(segment.entries[0].row_hlc, hlc(10));
	assert_eq!(segment.entries[1].row_hlc, hlc(20));
	assert_eq!(
		segment.entries[0].encoded_key,
		((42_i64 as u64) ^ 0x8000_0000_0000_0000)
			.to_be_bytes()
			.to_vec()
	);
	assert_eq!(
		segment.entries[1].encoded_key,
		((43_i64 as u64) ^ 0x8000_0000_0000_0000)
			.to_be_bytes()
			.to_vec()
	);
	let coverage = manager
		.secondary_index_coverage_snapshot(info.coverage_key())
		.unwrap();
	assert_eq!(coverage.covered_slabs, vec![source_slab_id]);
}

/// ADR 0097 decision 4: the deterministic class must survive the REAL path, not
/// just its constructor.
///
/// A constructor-only test cannot detect a layer that re-wraps the error by its
/// string somewhere between the origin and the scheduler; this drives a genuine
/// production failure - a rewrite attempted without the ownership claim that makes
/// it safe - and classifies what actually comes back, after `?` and the
/// `CompactionFailure` conversion.
#[tokio::test]
async fn adr0097_deterministic_class_survives_the_real_failure_path() {
	use crate::storage::maintenance_policy::{
		DeterministicOrigin, MaintenanceFailureClass, classify_compaction_failure,
	};

	let (_manager_dir, manager) = fresh_manager().await;
	let table = adr0097_colocated_table_descriptor_for_test();
	append_attributed_multirow_frame_for_test(&manager, &table, &[1, 2], hlc(1_000)).await;
	let slab_id = manager.force_seal_active_for_test().await;
	let slab = manager
		.state
		.lock()
		.await
		.sealed
		.get(&slab_id)
		.map(|slab| slab.value().clone())
		.expect("the sealed slab is resident");

	// Ownership of a DIFFERENT slab: the rewrite must refuse, deterministically.
	let unrelated = SlabId::new(slab_id.get() + 1_000);
	let claims = manager.acquire_maintenance_claims(&[unrelated]).await;
	let records = [CompactionRecord {
		tenant_id: tenant(),
		key_identity: id(1),
		routing_locality: Locality::new(1),
		old_location: RecordLocation {
			slab_id,
			offset: 0,
			length: 1,
			hlc: hlc(1_000),
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement: crate::schema::table::PlacementSemantics::RawLocality,
			partition: None,
		},
		reclaim: true,
		retention_ambiguous: false,
		retention_lag_seconds: 0,
	}];
	let outcome = manager
		.compact_sealed_slab_records(
			slab_id,
			slab,
			&records,
			SecondaryIndexCompactionContext {
				table_schemas: &TableSchemaCatalog::default(),
				secondary_indexes: &SecondaryIndexCatalog::default(),
			},
			&claims,
		)
		.await;
	let error = match outcome {
		Err(error) => error,
		Ok(_) => panic!("a rewrite without ownership must be refused"),
	};

	// Through the `?` conversion the schedulers actually use.
	let failure = crate::storage::lsm::CompactionFailure::from(error);
	assert_eq!(
		classify_compaction_failure(&failure),
		MaintenanceFailureClass::Deterministic(DeterministicOrigin::MaintenanceRewriteWithoutClaim),
		"the class survives the production path, not merely its constructor"
	);
}

/// ADR 0097: a committer that stops the LSM job exactly at publication.
///
/// The pause discriminates the two designs that a read-side test cannot tell
/// apart: a job that releases its claims once its READS are done, and a job that
/// holds them for its whole life. Only the second keeps a competing rewrite out
/// while it publishes.
struct PausingLsmCommitter {
	manager: Arc<SlabManager>,
	reached_publication: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
	release_publication: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[async_trait]
impl TierCommitter for PausingLsmCommitter {
	async fn commit_tier_slab(&self, _tier: crate::consensus::TieredSlab) -> std::io::Result<()> {
		Err(Error::new(ErrorKind::Unsupported, "unused"))
	}

	async fn retire_tiered_slab(&self, _slab_id: SlabId) -> std::io::Result<()> {
		Err(Error::new(ErrorKind::Unsupported, "unused"))
	}

	async fn commit_lsm_compaction(&self, commit: CompactionCommit) -> std::io::Result<()> {
		if let Some(sender) = self.reached_publication.lock().await.take() {
			let _ = sender.send(());
		}
		if let Some(receiver) = self.release_publication.lock().await.take() {
			let _ = receiver.await;
		}
		self.manager
			.apply_lsm_manifest_compaction_commit(&commit)
			.map(|_| ())
			.map_err(Error::other)
	}
}

/// ADR 0097 decision 3, end to end: one maintenance job owns every slab it touches
/// for its WHOLE life, and a competing job blocks and then replans rather than
/// rewriting from a stale scan.
///
/// The two-slab latch makes the interleaving deterministic instead of timing-based.
/// `acquire_maintenance_claims` sorts internally, so an LSM plan over {lower,
/// higher} takes the lower first; holding a read claim on the HIGHER slab parks the
/// job with the lower one owned. That is a real ordering fact, not a sleep.
///
/// Three separate facts fall out, and each has its own assertion:
///   1. acquisition order - LSM owns the lower slab while parked on the higher;
///   2. exclusion during reads - retention on the lower slab cannot proceed;
///   3. whole-job ownership - retention is STILL excluded at publication, after
///      every read is done, which is what a claim released after reads would not do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adr0097_lsm_owns_its_inputs_through_publication_and_retention_replans() {
	let dir = fresh_dir("adr0097-ownership-latch");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	let table = adr0097_colocated_table_descriptor_for_test();

	append_attributed_multirow_frame_for_test(&manager, &table, &[1, 2], hlc(1_000)).await;
	let lower = manager.force_seal_active_for_test().await;
	append_attributed_multirow_frame_for_test(&manager, &table, &[3, 4], hlc(2_000)).await;
	let higher = manager.force_seal_active_for_test().await;
	assert!(lower < higher, "the latch needs a known acquisition order");

	// 1. Hold a read claim on the HIGHER slab: the LSM job will park there.
	let higher_lease = manager
		.acquire_slab_rewrite_read_claim(higher)
		.await
		.unwrap();

	let snapshot = manager.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x0097_0003),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![lower, higher],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(9_000),
		compaction_start_hlc: hlc(9_001),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
	let (release_tx, release_rx) = tokio::sync::oneshot::channel();
	let committer = Arc::new(PausingLsmCommitter {
		manager: manager.clone(),
		reached_publication: tokio::sync::Mutex::new(Some(reached_tx)),
		release_publication: tokio::sync::Mutex::new(Some(release_rx)),
	});

	// 2. Start the LSM job. It takes `lower` and parks on `higher`.
	let lsm = tokio::spawn({
		let manager = manager.clone();
		let committer = committer.clone();
		async move {
			let worker = LsmCompactionWorker::new(
				crate::storage::LsmConfig::default(),
				manager.slab_pin_table(),
			)
			.unwrap();
			manager
				.run_lsm_compaction_once_via_control_ring_with_context(
					&worker,
					plan,
					committer.as_ref(),
					None,
				)
				.await
		}
	});

	// Wait for the LSM job to actually take the lower slab, rather than assuming it
	// has been scheduled. If it never does, the job failed early and its error is
	// far more informative than a bare ownership assertion.
	let mut owned = false;
	for _ in 0..40 {
		if tokio::time::timeout(
			Duration::from_millis(50),
			manager.acquire_slab_rewrite_read_claim(lower),
		)
		.await
		.is_err()
		{
			owned = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
	}
	assert!(
		owned,
		"LSM never took the lower slab; job finished={} ",
		lsm.is_finished()
	);

	// 3. Retention on that same slab must block, not scan.
	let mut catalog_tables = TableSchemaCatalog::default();
	catalog_tables.upsert(tenant(), table.clone()).unwrap();
	let catalog_indexes = SecondaryIndexCatalog::default();
	let mut retention = tokio::spawn({
		let manager = manager.clone();
		let catalog_tables = catalog_tables.clone();
		let catalog_indexes = catalog_indexes.clone();
		async move {
			manager
				.prune_one_sealed_slab(
					lower,
					hlc(9_000),
					SecondaryIndexCompactionContext {
						table_schemas: &catalog_tables,
						secondary_indexes: &catalog_indexes,
					},
				)
				.await
		}
	});
	assert!(
		tokio::time::timeout(Duration::from_millis(300), &mut retention)
			.await
			.is_err(),
		"retention blocks behind the LSM claim"
	);

	// 4. Release the higher claim; the LSM job proceeds to publication.
	drop(higher_lease);
	reached_rx.await.expect("the LSM job reaches publication");

	// 5. THE DISCRIMINATING ASSERTION. Every read is done and the job is inside
	// publication. A claim released after reads would let retention in here.
	assert!(
		tokio::time::timeout(Duration::from_millis(300), &mut retention)
			.await
			.is_err(),
		"ownership is held THROUGH publication, not released after the reads"
	);

	// 6. Release publication and let both finish.
	let _ = release_tx.send(());
	lsm.await
		.expect("the lsm task joins")
		.expect("the lsm compaction succeeds");
	let pruned = retention
		.await
		.expect("the retention task joins")
		.expect("retention returns cleanly rather than erroring");
	assert!(
		pruned.is_none(),
		"retention discards the retired candidate BEFORE scanning it, rather than \
		 rewriting from a scan of bytes the LSM job replaced"
	);

	let live = manager
		.compaction_manifest_snapshot()
		.live_runs()
		.map(|run| run.slab_id)
		.collect::<Vec<_>>();
	assert!(
		!live.contains(&lower) && !live.contains(&higher),
		"both inputs are retired: {live:?}"
	);
	assert!(
		!live.is_empty(),
		"the LSM successor is the only thing left live: {live:?}"
	);
}

/// ADR 0097 gap B: a backfill candidate enumerated while its source was live, then
/// replaced by retention before the backfill takes its read claim, is STALE WORK -
/// discarded silently, re-enumerated next tick. It is not a coverage gap and not an
/// integrity failure.
///
/// This is the loop the live node ran for hours: every pass logged
/// `secondary index backfill source slab N not found` with an advancing N, because
/// retention kept replacing the slab each pass had just enumerated.
#[tokio::test]
async fn adr0097_backfill_candidate_replaced_before_its_claim_is_stale_not_uncovered() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	for id_value in 0..3_i64 {
		append_secondary_index_table_row_for_test(
			&manager,
			&table,
			id_value,
			Some(id_value),
			hlc(6_000 + id_value as u64),
		)
		.await;
	}
	// A second version of key 0 gives retention something to prune, so the rewrite
	// actually replaces the slab rather than leaving it alone.
	append_secondary_index_table_row_for_test(&manager, &table, 0, Some(99), hlc(6_010)).await;
	let source_slab_id = manager.force_seal_active_for_test().await;

	// 1. Enumerate while the source is live.
	let plan = manager
		.ensure_secondary_index_coverage(tenant(), &descriptor, 4096)
		.await
		.unwrap();
	let candidate = plan
		.candidates
		.iter()
		.find(|candidate| candidate.source_slab_id() == source_slab_id)
		.expect("the live source is enumerated")
		.clone();

	// 2. Retention replaces that source before the backfill claims it.
	let mut catalog_tables = TableSchemaCatalog::default();
	catalog_tables.upsert(tenant(), table.clone()).unwrap();
	let mut catalog_indexes = SecondaryIndexCatalog::default();
	catalog_indexes
		.create(tenant(), descriptor.clone(), &catalog_tables)
		.unwrap();
	manager
		.prune_one_sealed_slab(
			source_slab_id,
			hlc(7_000),
			SecondaryIndexCompactionContext {
				table_schemas: &catalog_tables,
				secondary_indexes: &catalog_indexes,
			},
		)
		.await
		.expect("retention rewrite succeeds")
		.expect("the fixture must actually replace the slab, not skip it");
	// The replacement alone does not remove the source: `finish_compaction_with_
	// replacement` deliberately keeps the old backing sealed for stale readers and
	// leaves disposal to the delete-pending sweep. The live loop's `not found`
	// therefore appears only once that sweep has run, so the fixture runs it too.
	assert!(
		manager
			.ensure_secondary_index_source_slab_available(source_slab_id)
			.await
			.is_ok(),
		"the retired source is still readable until the sweep disposes it"
	);
	let gc = LsmGcWorker::new(
		crate::storage::lsm::LsmConfig::default(),
		manager.slab_pin_table(),
	)
	.unwrap();
	manager
		.sweep_lsm_delete_pending_once(&gc)
		.await
		.expect("disposal sweep runs");
	assert!(
		manager
			.ensure_secondary_index_source_slab_available(source_slab_id)
			.await
			.is_err(),
		"the enumerated source is genuinely gone now"
	);

	// 3. The stale candidate is discarded, not recorded as damage.
	let outcome = manager
		.backfill_secondary_index_candidate(tenant(), &table, &descriptor, &candidate)
		.await
		.expect("a replaced candidate is not an error");
	assert!(
		matches!(
			outcome,
			SecondaryIndexBackfillOutcome::StaleCandidate { source_slab_id: stale }
				if stale == source_slab_id
		),
		"the replaced candidate reports staleness"
	);
	assert!(
		manager
			.secondary_index_coverage_snapshot(candidate.key())
			.is_none_or(|snapshot| !snapshot
				.uncovered_slabs
				.iter()
				.any(|(uncovered, _)| *uncovered == source_slab_id)),
		"a stale candidate must not be recorded as uncovered: that is what spent \
		 retry budget and reported a healthy store as damaged"
	);

	// 4. The next tick re-enumerates and covers the REPLACEMENT. Asserted
	// positively: "no failures" is also true of a tick that did nothing at all.
	let report = manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.expect("the next tick runs cleanly");
	assert!(
		report.failures.is_empty(),
		"re-enumeration produces no failures: {:?}",
		report.failures
	);
	// Positively: coverage is COMPLETE and it covers the successor, not the retired
	// source. There is deliberately no completed candidate to count - the retention
	// rewrite reconciles the successor's segments as it publishes, so by the next
	// tick there is no backfill work left. Asserting "one completion" here would
	// assert behaviour the system correctly does not have; asserting only "no
	// failures" would pass for a tick that did nothing. What must hold is that the
	// successor ended up covered and the retired source did not linger as debt.
	assert!(
		report.historical_complete,
		"coverage is complete after the replacement is indexed: after={:?}",
		report.after.snapshot.uncovered_slabs
	);
	assert!(
		report.after.snapshot.uncovered_slabs.is_empty(),
		"nothing is left uncovered: {:?}",
		report.after.snapshot.uncovered_slabs
	);
	let covered = report.after.snapshot.covered_slabs.clone();
	assert!(
		!covered.contains(&source_slab_id),
		"the retired source is not carried as covered debt: {covered:?}"
	);
	assert!(
		covered.iter().any(|slab| *slab != source_slab_id),
		"the SUCCESSOR is covered: {covered:?}"
	);
}

/// The inverse fence: a slab missing behind a DURABLE PUBLISHED REFERENCE is still a
/// hard error. Nothing enumerated it, so nothing can re-enumerate it, and treating
/// it as stale would silently swallow real damage.
#[tokio::test]
async fn adr0097_missing_published_source_is_still_an_integrity_error() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	append_secondary_index_table_row_for_test(&manager, &table, 1, Some(1), hlc(10)).await;
	let source_slab_id = manager.force_seal_active_for_test().await;
	let absent = SlabId::new(source_slab_id.get() + 4_242);

	// The availability check that durable published references resolve through
	// keeps returning NotFound rather than reporting absence as ordinary staleness.
	let err = manager
		.ensure_secondary_index_source_slab_available(absent)
		.await
		.expect_err("a slab that was never enumerated cannot be stale work");
	assert_eq!(err.kind(), ErrorKind::NotFound);

	// And a caller naming ONE slab explicitly is not enumerating, so it gets the
	// fact rather than a silent skip.
	let err = manager
		.backfill_secondary_index_source_slab(tenant(), &table, &descriptor, absent)
		.await
		.expect_err("an explicitly named missing source is an error");
	assert_eq!(err.kind(), ErrorKind::NotFound);

	// A forged candidate is no longer expressible: `SecondaryIndexBackfillCandidate`
	// has private fields and a crate-internal constructor, so only enumeration can
	// produce one. That is checked by the compile-fail doctest on the type rather
	// than here - a runtime test written inside the crate could still build one and
	// would prove nothing about external callers.
}

#[tokio::test]
async fn secondary_index_backfill_tick_respects_limit_and_reports_completion() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();

	append_secondary_index_table_row_for_test(&manager, &table, 1, Some(10), hlc(10)).await;
	let first = force_seal_active(&manager).await;
	append_secondary_index_table_row_for_test(&manager, &table, 2, Some(20), hlc(20)).await;
	let second = force_seal_active(&manager).await;

	let first_tick = manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 1, 4096)
		.await
		.unwrap();
	assert_eq!(first_tick.before.candidates.len(), 2);
	assert_eq!(first_tick.attempted, 1);
	assert_eq!(first_tick.completed.len(), 1);
	assert!(first_tick.failures.is_empty());
	assert!(!first_tick.historical_complete);
	assert_eq!(first_tick.completed[0].candidate.source_slab_id(), first);

	let second_tick = manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.unwrap();
	assert_eq!(second_tick.before.candidates.len(), 1);
	assert_eq!(second_tick.attempted, 1);
	assert_eq!(second_tick.completed.len(), 1);
	assert!(second_tick.failures.is_empty());
	assert_eq!(second_tick.completed[0].candidate.source_slab_id(), second);
	assert!(second_tick.historical_complete);

	let listed = manager.list_secondary_index_segments(
		tenant(),
		descriptor.table_object_id,
		descriptor.index_object_id,
		descriptor.logical_schema_fingerprint,
	);
	assert_eq!(listed.len(), 2);
}

#[tokio::test]
async fn secondary_index_compaction_rebuilds_building_sidecar_and_heals_uncovered_slab() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	let row_identity =
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
	assert_eq!(
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(20), hlc(20)).await,
		row_identity
	);
	let old_slab_id = force_seal_active(&manager).await;

	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), table.clone()).unwrap();
	let lifecycle = TableLifecycleCatalog::default();
	let mut secondary_indexes = SecondaryIndexCatalog::default();
	secondary_indexes
		.create(tenant(), descriptor.clone(), &schemas)
		.unwrap();
	let coverage_key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
		tenant(),
		&descriptor,
	));
	manager.mark_secondary_index_backfill_uncovered(
		coverage_key,
		old_slab_id,
		SecondaryIndexBackfillError::new(
			SecondaryIndexBackfillErrorReason::CompactionRace,
			"source slab compacted before backfill completed",
		),
	);

	let report = manager
		.prune_versions_for_retention(
			MvccRetentionPolicy::FixedWindow(Duration::from_millis(60_000)),
			&lifecycle,
			&schemas,
			&secondary_indexes,
			60_025,
			1,
		)
		.await
		.unwrap();

	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.pruned_versions, 1);
	let listed = manager.list_secondary_index_segments(
		tenant(),
		descriptor.table_object_id,
		descriptor.index_object_id,
		descriptor.logical_schema_fingerprint,
	);
	assert_eq!(listed.len(), 1);
	let info = &listed[0];
	assert_ne!(info.source_slab_id, old_slab_id);
	assert_eq!(info.entry_count, 1);
	let segment = manager
		.load_secondary_index_segment(info)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(segment.entries.len(), 1);
	assert_eq!(segment.entries[0].row_key_id, row_identity);
	assert_eq!(segment.entries[0].row_hlc, hlc(20));
	assert_eq!(
		segment.entries[0].encoded_key,
		secondary_index_encoded_i64_for_test(20)
	);
	let coverage = manager
		.secondary_index_coverage_snapshot(coverage_key)
		.unwrap();
	assert_eq!(coverage.covered_slabs, vec![info.source_slab_id]);
	assert!(coverage.uncovered_slabs.is_empty());
}

#[tokio::test]
async fn lsm_compaction_rebuilds_secondary_index_sidecar_with_catalog_context() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	let row_identity =
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
	let source_slab_id = manager.force_seal_active_for_test().await;

	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), table.clone()).unwrap();
	let mut secondary_indexes = SecondaryIndexCatalog::default();
	secondary_indexes
		.create(tenant(), descriptor.clone(), &schemas)
		.unwrap();
	let tick = manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.unwrap();
	assert_eq!(tick.completed.len(), 1);
	let old_info = manager
		.list_secondary_index_segments(
			tenant(),
			descriptor.table_object_id,
			descriptor.index_object_id,
			descriptor.logical_schema_fingerprint,
		)
		.into_iter()
		.find(|info| info.source_slab_id == source_slab_id)
		.expect("old source sidecar should exist before LSM compaction");
	assert_eq!(old_info.source_slab_id, source_slab_id);

	let snapshot = manager.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x5151),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![source_slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let worker = LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		manager.slab_pin_table(),
	)
	.unwrap();
	let committer = DirectLsmCommitter {
		manager: manager.clone(),
	};

	let outcome = manager
		.run_lsm_compaction_once_via_control_ring_with_context(
			&worker,
			plan,
			&committer,
			Some(SecondaryIndexCompactionContext {
				table_schemas: &schemas,
				secondary_indexes: &secondary_indexes,
			}),
		)
		.await
		.unwrap();
	let output_slab_id = outcome.output_slabs[0].slab_id;

	assert!(
		manager
			.load_secondary_index_segment(&old_info)
			.await
			.unwrap()
			.is_none(),
		"old sidecar should be retired after LSM compaction"
	);
	let listed = manager.list_secondary_index_segments(
		tenant(),
		descriptor.table_object_id,
		descriptor.index_object_id,
		descriptor.logical_schema_fingerprint,
	);
	assert_eq!(listed.len(), 1);
	let info = &listed[0];
	assert_eq!(info.source_slab_id, output_slab_id);
	assert_eq!(info.entry_count, 1);
	let segment = manager
		.load_secondary_index_segment(info)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(segment.entries.len(), 1);
	assert_eq!(segment.entries[0].row_key_id, row_identity);
	assert_eq!(
		segment.entries[0].encoded_key,
		secondary_index_encoded_i64_for_test(10)
	);
	let coverage = manager
		.secondary_index_coverage_snapshot(info.coverage_key())
		.unwrap();
	assert_eq!(coverage.covered_slabs, vec![output_slab_id]);
	assert!(coverage.uncovered_slabs.is_empty());
	assert_eq!(
		manager
			.compaction_manifest_snapshot()
			.run(output_slab_id)
			.unwrap()
			.sidecar_coverage,
		crate::storage::SidecarCoverage::Complete
	);
}

#[tokio::test]
async fn lsm_compaction_commit_failure_does_not_publish_prepared_sidecar() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
	let source_slab_id = manager.force_seal_active_for_test().await;

	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), table.clone()).unwrap();
	let mut secondary_indexes = SecondaryIndexCatalog::default();
	secondary_indexes
		.create(tenant(), descriptor.clone(), &schemas)
		.unwrap();
	manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.unwrap();

	let snapshot = manager.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x5154),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![source_slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let worker = LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		manager.slab_pin_table(),
	)
	.unwrap();

	let err = manager
		.run_lsm_compaction_once_via_control_ring_with_context(
			&worker,
			plan,
			&FailingLsmCommitter,
			Some(SecondaryIndexCompactionContext {
				table_schemas: &schemas,
				secondary_indexes: &secondary_indexes,
			}),
		)
		.await
		.unwrap_err();

	assert!(matches!(err, CompactionFailure::IoError(_)));
	let listed = manager.list_secondary_index_segments(
		tenant(),
		descriptor.table_object_id,
		descriptor.index_object_id,
		descriptor.logical_schema_fingerprint,
	);
	assert_eq!(listed.len(), 1);
	assert_eq!(listed[0].source_slab_id, source_slab_id);
	assert!(
		manager
			.compaction_manifest_snapshot()
			.run(source_slab_id)
			.unwrap()
			.state
			.is_live()
	);
}

#[tokio::test]
async fn lsm_compaction_rejects_secondary_index_sidecar_without_catalog_context() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
	let source_slab_id = manager.force_seal_active_for_test().await;

	manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.unwrap();
	let old_info = manager
		.list_secondary_index_segments(
			tenant(),
			descriptor.table_object_id,
			descriptor.index_object_id,
			descriptor.logical_schema_fingerprint,
		)
		.into_iter()
		.find(|info| info.source_slab_id == source_slab_id)
		.expect("old source sidecar should exist before LSM compaction");

	let snapshot = manager.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x5152),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![source_slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let worker = LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		manager.slab_pin_table(),
	)
	.unwrap();
	let committer = DirectLsmCommitter {
		manager: manager.clone(),
	};

	let err = manager
		.run_lsm_compaction_once_via_control_ring(&worker, plan, &committer)
		.await
		.unwrap_err();

	assert!(matches!(err, CompactionFailure::SidecarDigestMismatch));
	assert!(
		manager
			.load_secondary_index_segment(&old_info)
			.await
			.unwrap()
			.is_some(),
		"sidecar should remain intact after rejected LSM compaction"
	);
	assert!(
		manager
			.compaction_manifest_snapshot()
			.run(source_slab_id)
			.unwrap()
			.state
			.is_live()
	);
}

#[tokio::test]
async fn lsm_compaction_elides_non_visible_primary_key_versions_with_catalog_context() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let row_identity =
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
	assert_eq!(
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(20), hlc(20)).await,
		row_identity
	);
	assert_eq!(
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(30), hlc(30)).await,
		row_identity
	);
	let source_slab_id = manager.force_seal_active_for_test().await;

	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), table).unwrap();
	let secondary_indexes = SecondaryIndexCatalog::default();
	let snapshot = manager.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x5153),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![source_slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(25),
		compaction_start_hlc: hlc(101),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let worker = LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		manager.slab_pin_table(),
	)
	.unwrap();
	let committer = DirectLsmCommitter {
		manager: manager.clone(),
	};

	let outcome = manager
		.run_lsm_compaction_once_via_control_ring_with_context(
			&worker,
			plan,
			&committer,
			Some(SecondaryIndexCompactionContext {
				table_schemas: &schemas,
				secondary_indexes: &secondary_indexes,
			}),
		)
		.await
		.unwrap();

	assert_eq!(outcome.rows_in, 3);
	assert_eq!(outcome.rows_out, 2);
	assert_eq!(outcome.elided_versions, 1);
	assert!(
		manager
			.lookup_as_of(tenant(), row_identity, hlc(15))
			.is_none()
	);
	assert_eq!(
		manager
			.lookup_as_of(tenant(), row_identity, hlc(25))
			.unwrap()
			.hlc,
		hlc(20)
	);
	assert_eq!(manager.lookup(tenant(), row_identity).unwrap().hlc, hlc(30));
}

#[tokio::test]
async fn secondary_index_lookup_point_equal_revalidates_sidecar_candidate() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	let first_identity =
		append_secondary_index_table_row_for_test(&manager, &table, 1, Some(10), hlc(10)).await;
	append_secondary_index_table_row_for_test(&manager, &table, 2, Some(20), hlc(20)).await;
	force_seal_active(&manager).await;
	manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.unwrap();

	let request = TableLookupByIndexRequest::from_descriptor(
		tenant(),
		&descriptor,
		IndexQuery::PointEqual {
			encoded_key: secondary_index_encoded_i64_for_test(10),
		},
		hlc(99),
	);
	let report = manager
		.table_lookup_by_index(&request, &descriptor, SecondaryIndexState::Ready)
		.await
		.unwrap();
	assert_eq!(report.rows.len(), 1);
	assert_eq!(report.rows[0].row_key_id, first_identity);
	assert_eq!(report.rows[0].row_hlc, hlc(10));
	assert_eq!(report.stats.scanned_segments, 1);
	assert_eq!(report.stats.candidate_count, 1);
	assert_eq!(report.stats.visible_count, 1);
	assert_eq!(report.stats.superseded_count, 0);

	let err = manager
		.table_lookup_by_index(&request, &descriptor, SecondaryIndexState::Building)
		.await
		.unwrap_err();
	assert!(
		err.to_string()
			.contains(INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH)
	);
}

#[tokio::test]
async fn secondary_index_lookup_rejects_superseded_candidate_but_honors_as_of() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	let row_identity =
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
	force_seal_active(&manager).await;
	manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.unwrap();
	assert_eq!(
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(20), hlc(20)).await,
		row_identity
	);

	let historical_request = TableLookupByIndexRequest::from_descriptor(
		tenant(),
		&descriptor,
		IndexQuery::PointEqual {
			encoded_key: secondary_index_encoded_i64_for_test(10),
		},
		hlc(15),
	);
	let historical = manager
		.table_lookup_by_index(&historical_request, &descriptor, SecondaryIndexState::Ready)
		.await
		.unwrap();
	assert_eq!(historical.rows.len(), 1);
	assert_eq!(historical.rows[0].row_key_id, row_identity);
	assert_eq!(historical.stats.visible_count, 1);

	let current_request = TableLookupByIndexRequest::from_descriptor(
		tenant(),
		&descriptor,
		IndexQuery::PointEqual {
			encoded_key: secondary_index_encoded_i64_for_test(10),
		},
		hlc(25),
	);
	let current = manager
		.table_lookup_by_index(&current_request, &descriptor, SecondaryIndexState::Ready)
		.await
		.unwrap();
	assert!(current.rows.is_empty());
	assert_eq!(current.stats.candidate_count, 1);
	assert_eq!(current.stats.visible_count, 0);
	assert_eq!(current.stats.superseded_count, 1);
}

#[tokio::test]
async fn locality_candidates_recover_after_index_lock_poison() {
	let (_manager_dir, manager) = fresh_manager().await;
	let key = id(0x4242);
	let locality = Locality::from(key);
	manager
		.append_durable(tenant(), key, hlc(10), b"v1")
		.await
		.unwrap();

	let locality_index = &manager.locality_index;
	let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		let _guard = locality_index.write().unwrap();
		panic!("poison locality index");
	}));

	assert_eq!(
		manager.locality_candidates_for_tenant(tenant(), locality, locality),
		vec![(locality, key)]
	);
}

#[tokio::test]
async fn lookup_returns_latest_version_after_multiple_appends() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let key = id(42);
	mgr.append(tenant(), key, hlc(10), b"v1").await.unwrap();
	mgr.append(tenant(), key, hlc(20), b"v2").await.unwrap();
	mgr.append(tenant(), key, hlc(30), b"v3").await.unwrap();

	let latest = mgr.lookup(tenant(), key).expect("key present");
	assert_eq!(latest.hlc.physical_ms, 30);
}

#[tokio::test]
async fn encrypt_tiered_object_blob_uses_adr022_layout() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	mgr.append(tenant(), id(0x1234), hlc(10), b"adr022-row")
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let plaintext = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &plaintext).unwrap();
	assert!(encrypted.bytes.starts_with(b"WENDSLAB"));
	let footer = mgr
		.encrypted_tiered_footer_summary(&encrypted.bytes)
		.unwrap()
		.expect("encrypted footer expected");
	assert_eq!(footer.slab_id, slab_id);
	assert_eq!(footer.tenant_id, tenant());
}

#[tokio::test]
async fn encrypt_tiered_object_blob_with_table_stats_builds_decryptable_csb() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
	]);
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x39; 16]),
		3,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.stats_policy = ColumnStatsPolicy {
		small_sets: vec![ColumnSmallSetSpec::new("region")],
		..ColumnStatsPolicy::default()
	};
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from(vec![Some(9), Some(1)])) as Arc<dyn Array>,
			Arc::new(LargeStringArray::from(vec![Some("us"), Some("eu")])) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant();
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	for row in 0..batch.num_rows() {
		let row_batch = batch.slice(row, 1);
		let frame = encode_test_batch(&row_batch);
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[row],
			admission.localities[row],
			hlc(10 + row as u64),
			&frame,
		)
		.await
		.unwrap();
	}
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();

	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	assert_eq!(locator.stats_column_count, 2);
	assert!(locator.has_small_set());
	let stats = mgr
		.config
		.crypto
		.decrypt_tiered_stats_block(slab_id, &encrypted.bytes, descriptor.schema_generation)
		.unwrap()
		.expect("stats block");
	let parsed = parse_column_stats_block(&stats.plaintext).unwrap();
	assert_eq!(parsed.header.table_object_id, descriptor.table_object_id);
	assert_eq!(
		parsed.header.schema_generation,
		descriptor.schema_generation
	);
	assert_eq!(parsed.header.row_count, 2);
	assert_eq!(parsed.columns[0].arrow_type_tag, ARROW_STATS_TAG_INT64);
	let id_stats = decode_column_stats_payload(&parsed.columns[0]).unwrap();
	assert_eq!(id_stats.min, Some(encode_signed_stat_value(1, 8).unwrap()));
	assert_eq!(id_stats.max, Some(encode_signed_stat_value(9, 8).unwrap()));
	let region_stats = decode_column_stats_payload(&parsed.columns[1]).unwrap();
	assert_eq!(
		region_stats.small_set,
		Some(vec![b"eu".to_vec(), b"us".to_vec()])
	);
}

#[tokio::test]
async fn table_stats_are_stable_across_record_body_compression() {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
	]));
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x43; 16]),
		7,
		"analytics",
		"events",
		schema.as_ref(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
	descriptor.stats_policy = ColumnStatsPolicy {
		small_sets: vec![ColumnSmallSetSpec::new("region")],
		..ColumnStatsPolicy::default()
	};
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor.validate().unwrap();
	let batch = RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![Some(9), Some(1), Some(6)])) as Arc<dyn Array>,
			Arc::new(LargeStringArray::from(vec![
				Some("us"),
				Some("eu"),
				Some("us"),
			])) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant();
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();

	let mut plaintexts = Vec::new();
	for policy in [
		RecordBodyCompressionPolicy::Uncompressed,
		RecordBodyCompressionPolicy::Lz4Frame,
	] {
		let (_mgr_dir, mgr) = fresh_manager().await;
		for row in 0..batch.num_rows() {
			let row_batch = batch.slice(row, 1);
			let frame = encode_batch_with_policy(&row_batch, policy).unwrap().bytes;
			mgr.append_durable_with_locality(
				tenant(),
				admission.key_identities[row],
				admission.localities[row],
				hlc(30 + row as u64),
				&frame,
			)
			.await
			.unwrap();
		}
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalog::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let stats = mgr
			.config
			.crypto
			.decrypt_tiered_stats_block(slab_id, &encrypted.bytes, descriptor.schema_generation)
			.unwrap()
			.expect("stats block");
		let parsed = parse_column_stats_block(&stats.plaintext).unwrap();
		assert_eq!(parsed.header.row_count, 3);
		assert_eq!(parsed.columns[0].arrow_type_tag, ARROW_STATS_TAG_INT64);
		let id_stats = decode_column_stats_payload(&parsed.columns[0]).unwrap();
		assert_eq!(id_stats.min, Some(encode_signed_stat_value(1, 8).unwrap()));
		assert_eq!(id_stats.max, Some(encode_signed_stat_value(9, 8).unwrap()));
		let region_stats = decode_column_stats_payload(&parsed.columns[1]).unwrap();
		assert_eq!(
			region_stats.small_set,
			Some(vec![b"eu".to_vec(), b"us".to_vec()])
		);
		plaintexts.push(stats.plaintext);
	}

	assert_eq!(
		plaintexts[0], plaintexts[1],
		"CSB plaintext should be a function of decoded rows and descriptor, not IPC body codec"
	);
}

#[tokio::test]
async fn table_stats_ignore_default_kv_descriptor_for_legacy_slabs() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let descriptor = default_kv_table_descriptor_v1().unwrap();
	let schema = descriptor.arrow_schema().unwrap();
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(b"legacy-key");
	let mut values = LargeBinaryBuilder::new();
	values.append_value(b"legacy-value");
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.unwrap();
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant();
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let frame = encode_test_batch(&batch);
	mgr.append(tenant(), admission.key_identities[0], hlc(12), &frame)
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor).unwrap();

	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();

	assert!(
		encrypted.stats_locator.is_none(),
		"default KV descriptors must not opt legacy KV slabs into ADR 039 stats"
	);
}

#[tokio::test]
async fn typed_table_stats_are_mandatory_when_any_row_matches_descriptor() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	mgr.append(tenant(), id(0x44), hlc(9), b"legacy-row")
		.await
		.unwrap();

	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
	]);
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x41; 16]),
		1,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from(vec![Some(7)])) as Arc<dyn Array>,
			Arc::new(LargeStringArray::from(vec![Some("us")])) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant();
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let frame = encode_test_batch(&batch);
	mgr.append_durable_with_locality(
		tenant(),
		admission.key_identities[0],
		admission.localities[0],
		hlc(10),
		&frame,
	)
	.await
	.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor).unwrap();

	let err = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap_err();

	assert_eq!(err.kind(), ErrorKind::InvalidData);
	assert!(
		err.to_string().contains("stats are mandatory"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn stats_advisory_absence_skips_stats_fetch() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x42; 16]),
		1,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let pointer = TieredSlabPointer {
		slab_id: SlabId::new(42),
		l2_uri: "memory://stats-should-not-be-read".to_owned(),
		payload_hash: [0xAB; 32],
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(TenantKekId::try_from(1).unwrap()),
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};

	let stats = mgr
		.load_tiered_column_stats_block(&pointer, &descriptor)
		.await
		.unwrap();

	assert!(stats.is_none());
}

#[tokio::test]
async fn stats_advisory_drift_fails_closed_to_unavailable() {
	let l1_dir = fresh_dir("stats-drift-l1");
	let l2_root = fresh_dir("stats-drift-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
	let io = spawn_io_thread(64).unwrap();
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
	]);
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x43; 16]),
		1,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from(vec![Some(1)])) as Arc<dyn Array>,
			Arc::new(LargeStringArray::from(vec![Some("eu")])) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant();
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let frame = encode_test_batch(&batch);
	mgr.append_durable_with_locality(
		tenant(),
		admission.key_identities[0],
		admission.localities[0],
		hlc(10),
		&frame,
	)
	.await
	.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2
		.put(
			&format!("stats-drift/slab_{slab_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	let mut drifted_hash = locator.stats_block_hash128;
	drifted_hash[0] ^= 0xFF;
	let pointer = TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: crate::hash::content::digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: Some(locator.stats_format_version),
		stats_block_len: Some(locator.stats_block_len),
		stats_block_hash128: Some(drifted_hash),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	let drift_before = metrics().pbc_slab_stats_pointer_drift_total.get();

	let stats = mgr
		.load_tiered_column_stats_block(&pointer, &descriptor)
		.await
		.unwrap();

	assert!(stats.is_none());
	assert!(
		metrics().pbc_slab_stats_pointer_drift_total.get() > drift_before,
		"pointer drift metric should increment when advisories disagree with footer locator"
	);
}

#[tokio::test]
async fn column_stats_plaintext_does_not_encode_slab_lineage() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
	]);
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x40; 16]),
		4,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.stats_policy = ColumnStatsPolicy {
		small_sets: vec![ColumnSmallSetSpec::new("region")],
		..ColumnStatsPolicy::default()
	};
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from(vec![Some(2), Some(8)])) as Arc<dyn Array>,
			Arc::new(LargeStringArray::from(vec![Some("eu"), Some("us")])) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant();
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	for row in 0..batch.num_rows() {
		let row_batch = batch.slice(row, 1);
		let frame = encode_test_batch(&row_batch);
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[row],
			admission.localities[row],
			hlc(20 + row as u64),
			&frame,
		)
		.await
		.unwrap();
	}
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();

	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let alternate_slab_id = slab_id.saturating_add(0x1000);
	let alternate_encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(alternate_slab_id, &raw, &catalog)
		.unwrap();
	assert_ne!(
		encrypted
			.stats_locator
			.expect("stats locator")
			.stats_block_hash128,
		alternate_encrypted
			.stats_locator
			.expect("alternate stats locator")
			.stats_block_hash128,
		"CSB ciphertext is still slab-bound through ADR 039 AEAD metadata"
	);

	let stats = mgr
		.config
		.crypto
		.decrypt_tiered_stats_block(slab_id, &encrypted.bytes, descriptor.schema_generation)
		.unwrap()
		.expect("stats block");
	let alternate_stats = mgr
		.config
		.crypto
		.decrypt_tiered_stats_block(
			alternate_slab_id,
			&alternate_encrypted.bytes,
			descriptor.schema_generation,
		)
		.unwrap()
		.expect("alternate stats block");
	assert_eq!(
		stats.plaintext, alternate_stats.plaintext,
		"CSB plaintext is a function of output rows and descriptor only"
	);
	let parsed = parse_column_stats_block(&stats.plaintext).unwrap();
	assert_eq!(parsed.header.table_object_id, descriptor.table_object_id);
	assert_eq!(
		parsed.header.schema_generation,
		descriptor.schema_generation
	);
	assert_eq!(parsed.header.row_count, 2);
	assert_eq!(parsed.header.column_block_count, 2);
}

#[tokio::test]
async fn tiered_retention_rewrite_recomputes_stats_from_survivors() {
	let l1_dir = fresh_dir("retention-stats-l1");
	let l2_root = fresh_dir("retention-stats-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
	let io = spawn_io_thread(64).unwrap();
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
	]);
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x39; 16]),
		3,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.stats_policy = ColumnStatsPolicy {
		small_sets: vec![ColumnSmallSetSpec::new("region")],
		..ColumnStatsPolicy::default()
	};
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();

	for (hlc_value, region) in [
		(hlc(10), "aa-old"),
		(hlc(20), "mm-anchor"),
		(hlc(30), "zz-latest"),
	] {
		let batch = RecordBatch::try_new(
			Arc::new(schema.clone()),
			vec![
				Arc::new(Int64Array::from(vec![Some(9)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![Some(region)])) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = tenant();
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let frame = encode_test_batch(&batch);
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[0],
			admission.localities[0],
			hlc_value,
			&frame,
		)
		.await
		.unwrap();
	}

	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let old_encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let old_stats = mgr
		.config
		.crypto
		.decrypt_tiered_stats_block(slab_id, &old_encrypted.bytes, descriptor.schema_generation)
		.unwrap()
		.expect("old stats block");
	let old_region = decode_column_stats_payload(
		&parse_column_stats_block(&old_stats.plaintext)
			.unwrap()
			.columns[1],
	)
	.unwrap();
	assert_eq!(old_region.min, Some(b"aa-old".to_vec()));
	let old_footer = mgr
		.encrypted_tiered_footer_summary(&old_encrypted.bytes)
		.unwrap()
		.expect("encrypted footer");
	let old_uri = l2
		.put(
			&format!("tier/slab_{slab_id:016x}.wnd"),
			&old_encrypted.bytes,
		)
		.await
		.unwrap();
	let old_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: old_uri,
		payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_footer.tenant_kek_id),
		stats_format_version: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

	let prepared = mgr
		.prepare_tiered_retention_rewrite(&old_pointer, hlc(25), &catalog)
		.await
		.unwrap()
		.expect("retention rewrite");
	let PreparedTieredRetentionRewrite::Rewrite {
		encrypted_bytes,
		pruned_versions,
		..
	} = prepared
	else {
		panic!("expected rewrite, not retire");
	};
	assert_eq!(pruned_versions, 1);
	let rewritten_stats = mgr
		.config
		.crypto
		.decrypt_tiered_stats_block(slab_id, &encrypted_bytes, descriptor.schema_generation)
		.unwrap()
		.expect("rewritten stats block");
	let parsed = parse_column_stats_block(&rewritten_stats.plaintext).unwrap();
	assert_eq!(parsed.header.row_count, 2);
	let region_stats = decode_column_stats_payload(&parsed.columns[1]).unwrap();
	assert_eq!(region_stats.min, Some(b"mm-anchor".to_vec()));
	assert_eq!(region_stats.max, Some(b"zz-latest".to_vec()));
	assert_eq!(
		region_stats.small_set,
		Some(vec![b"mm-anchor".to_vec(), b"zz-latest".to_vec()])
	);
}

#[tokio::test]
async fn tiered_retention_rewrite_filters_partial_multirow_storage_records() {
	let l1_dir = fresh_dir("retention-multirow-l1");
	let l2_root = fresh_dir("retention-multirow-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
	let io = spawn_io_thread(64).unwrap();
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);

	let key_a = id(0xA1);
	let key_b = id(0xB2);
	let key_c = id(0xC3);
	let loc_a = Locality::new(20);
	let loc_b = Locality::new(10);
	let loc_c = Locality::new(30);
	let frame = encode_test_multirow_storage_frame(&[
		(key_c, loc_c, hlc(30), 300),
		(key_a, loc_a, hlc(10), 100),
		(key_b, loc_b, hlc(20), 200),
	]);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("multi-row entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	mgr.append_durable_with_locality(tenant(), key_a, loc_a, hlc(40), b"new-a")
		.await
		.unwrap();

	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let old_encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let old_uri = l2
		.put(
			&format!("tier/slab_{slab_id:016x}.wnd"),
			&old_encrypted.bytes,
		)
		.await
		.unwrap();
	let old_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: old_uri,
		payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_encrypted.tenant_kek_id),
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

	let prepared = mgr
		.prepare_tiered_retention_rewrite(&old_pointer, hlc(50), &TableSchemaCatalog::default())
		.await
		.unwrap()
		.expect("retention rewrite");
	let PreparedTieredRetentionRewrite::Rewrite {
		encrypted_bytes,
		pruned_versions,
		..
	} = prepared
	else {
		panic!("expected rewrite, not retire");
	};
	assert_eq!(pruned_versions, 1);

	let rewritten_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: "rewritten".to_string(),
		payload_hash: crate::hash::content::digest(&encrypted_bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_encrypted.tenant_kek_id),
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	let rewritten_plaintext = decode_tiered_blob(
		mgr.config.crypto.as_ref(),
		&rewritten_pointer,
		&encrypted_bytes,
		&BTreeSet::new(),
	)
	.unwrap();
	let recovered =
		recover_slab_entries_from_blob(&rewritten_plaintext, slab_id, &BTreeSet::new()).unwrap();
	assert!(
		!recovered
			.iter()
			.any(|entry| entry.key_identity == key_a && entry.location.hlc == hlc(10))
	);
	assert!(
		recovered
			.iter()
			.any(|entry| entry.key_identity == key_a && entry.location.hlc == hlc(40))
	);
	assert!(
		recovered
			.iter()
			.any(|entry| entry.key_identity == key_b && entry.location.hlc == hlc(20))
	);
	assert!(
		recovered
			.iter()
			.any(|entry| entry.key_identity == key_c && entry.location.hlc == hlc(30))
	);

	let mut groups: BTreeMap<(u64, u32), Vec<RecoveredSlabEntry>> = BTreeMap::new();
	for entry in recovered {
		groups
			.entry((entry.location.offset, entry.location.length))
			.or_default()
			.push(entry);
	}
	assert_eq!(groups.len(), 2);
	let multirow = groups
		.values()
		.find(|entries| entries.len() == 2)
		.expect("rewritten multi-row record");
	assert_eq!(multirow[0].key_identity, key_b);
	assert_eq!(multirow[0].location.record_row_index.get(), 0);
	assert_eq!(multirow[1].key_identity, key_c);
	assert_eq!(multirow[1].location.record_row_index.get(), 1);
}

#[tokio::test]
async fn l1_retention_pruning_filters_partial_multirow_storage_records_once() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let key_a = id(0x1A);
	let key_b = id(0x1B);
	let key_c = id(0x1C);
	let loc_a = Locality::new(20);
	let loc_b = Locality::new(10);
	let loc_c = Locality::new(30);
	let frame = encode_test_multirow_storage_frame(&[
		(key_c, loc_c, hlc(30), 300),
		(key_a, loc_a, hlc(10), 100),
		(key_b, loc_b, hlc(20), 200),
	]);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("multi-row entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	mgr.append_durable_with_locality(tenant(), key_a, loc_a, hlc(40), b"new-a")
		.await
		.unwrap();

	let old_slab_id = force_seal_active(&mgr).await;
	let report = mgr
		.prune_versions_older_than(
			hlc(50),
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			1,
		)
		.await
		.unwrap();
	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.pruned_versions, 1);
	assert!(
		mgr.lookup_as_of(tenant(), key_a, hlc(10)).is_none(),
		"pruned old row should leave the historical slot absent"
	);

	let loc_b_new = mgr.lookup(tenant(), key_b).expect("key_b survivor");
	let loc_c_new = mgr.lookup(tenant(), key_c).expect("key_c survivor");
	assert_ne!(loc_b_new.slab_id, old_slab_id);
	assert_eq!(loc_b_new.slab_id, loc_c_new.slab_id);
	assert_eq!(loc_b_new.offset, loc_c_new.offset);
	assert_eq!(loc_b_new.record_row_index.get(), 0);
	assert_eq!(loc_c_new.record_row_index.get(), 1);

	let rewritten_frame = mgr.read(loc_b_new).await.unwrap();
	let rewritten_entries = storage_multirow_index_entries_from_frame(&rewritten_frame)
		.unwrap()
		.expect("rewritten multi-row entries");
	assert_eq!(rewritten_entries.len(), 2);
	assert_eq!(rewritten_entries[0].key_identity, key_b);
	assert_eq!(rewritten_entries[1].key_identity, key_c);
	assert_eq!(
		mgr.read(mgr.lookup(tenant(), key_a).expect("new key_a"))
			.await
			.unwrap(),
		b"new-a"
	);
}

#[tokio::test]
async fn table_drop_reclaim_removes_target_table_rows() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let target = append_only_retention_descriptor(TableObjectId::new([0x39; 16]));
	let other =
		append_only_retention_descriptor_named(TableObjectId::new([0x40; 16]), "other_events");
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), target.clone()).unwrap();
	schemas.upsert(tenant(), other.clone()).unwrap();

	let target_key_a = id(0x391);
	let target_key_b = id(0x392);
	let other_key = id(0x401);
	let target_frame = encode_test_append_only_storage_frame(
		target.table_object_id,
		&[
			(target_key_a, Locality::new(10), hlc(10), 100, false),
			(target_key_b, Locality::new(11), hlc(11), 200, false),
		],
	);
	let target_entries = storage_multirow_index_entries_from_frame(&target_frame)
		.unwrap()
		.expect("target table entries");
	mgr.append_durable_multirow_with_locality(tenant(), &target_frame, &target_entries)
		.await
		.unwrap();
	let other_frame = encode_test_append_only_storage_frame(
		other.table_object_id,
		&[(other_key, Locality::new(20), hlc(20), 300, false)],
	);
	let other_entries = storage_multirow_index_entries_from_frame(&other_frame)
		.unwrap()
		.expect("other table entries");
	mgr.append_durable_multirow_with_locality(tenant(), &other_frame, &other_entries)
		.await
		.unwrap();

	let report = mgr
		.reclaim_table_drop_storage(
			tenant(),
			&target,
			&schemas,
			&SecondaryIndexCatalog::default(),
			usize::MAX,
		)
		.await
		.unwrap();

	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.reclaimed_records, 2);
	assert!(mgr.lookup(tenant(), target_key_a).is_none());
	assert!(mgr.lookup(tenant(), target_key_b).is_none());
	assert!(mgr.lookup(tenant(), other_key).is_some());
}

#[tokio::test]
async fn adr0095_table_drop_slice_bounds_examined_work_and_resumes_after_cursor() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let target = append_only_retention_descriptor(TableObjectId::new([0x95; 16]));
	let other =
		append_only_retention_descriptor_named(TableObjectId::new([0x96; 16]), "other_0095");
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), target.clone()).unwrap();
	schemas.upsert(tenant(), other.clone()).unwrap();
	let target_key = id(0x9500);
	let mut nonmatching_keys = Vec::new();
	for slab_ordinal in 0..7_u64 {
		let (table_object_id, key) = if slab_ordinal == 5 {
			(target.table_object_id, target_key)
		} else {
			let key = id(0x9600 + slab_ordinal);
			nonmatching_keys.push(key);
			(other.table_object_id, key)
		};
		let frame = encode_test_append_only_storage_frame(
			table_object_id,
			&[(
				key,
				Locality::new(100 + slab_ordinal),
				hlc(100 + slab_ordinal),
				1,
				false,
			)],
		);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("one-row storage entry");
		mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();
		force_seal_active(&mgr).await;
	}

	let mut cursor = TableDropReclaimCursor::default();
	let mut examined_total = 0usize;
	let mut rewritten_total = 0usize;
	let mut prior_after = None;
	loop {
		let report = mgr
			.reclaim_table_drop_storage_slice(
				tenant(),
				&target,
				&schemas,
				&SecondaryIndexCatalog::default(),
				cursor,
				2,
			)
			.await
			.unwrap();
		assert!(report.examined <= 2, "examined work is the API budget");
		if let (Some(prior), Some(next)) = (prior_after, report.cursor_next.after_slab_id) {
			assert!(
				next > prior,
				"the durable cursor never re-examines prior slabs"
			);
		}
		examined_total += report.examined;
		rewritten_total += report.rewritten;
		prior_after = report.cursor_next.after_slab_id;
		cursor = report.cursor_next;
		if report.done {
			break;
		}
	}
	assert_eq!(
		examined_total, 7,
		"every captured slab was examined exactly once"
	);
	assert_eq!(
		rewritten_total, 1,
		"only the target-bearing slab was rewritten"
	);
	assert!(mgr.lookup(tenant(), target_key).is_none());
	for key in nonmatching_keys {
		assert!(mgr.lookup(tenant(), key).is_some());
	}
}

#[tokio::test]
async fn table_drop_reclaim_rewrites_tiered_target_rows() {
	let l1_dir = fresh_dir("drop-reclaim-l1");
	let l2_root = fresh_dir("drop-reclaim-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
	let io = spawn_io_thread(64).unwrap();
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let target = append_only_retention_descriptor(TableObjectId::new([0x41; 16]));
	let other =
		append_only_retention_descriptor_named(TableObjectId::new([0x42; 16]), "tier_other");
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), target.clone()).unwrap();
	schemas.upsert(tenant(), other.clone()).unwrap();

	let target_key_a = id(0x411);
	let target_key_b = id(0x412);
	let other_key = id(0x421);
	let target_frame = encode_test_append_only_storage_frame(
		target.table_object_id,
		&[
			(target_key_a, Locality::new(30), hlc(30), 100, false),
			(target_key_b, Locality::new(31), hlc(31), 200, false),
		],
	);
	let target_entries = storage_multirow_index_entries_from_frame(&target_frame)
		.unwrap()
		.expect("target table entries");
	mgr.append_durable_multirow_with_locality(tenant(), &target_frame, &target_entries)
		.await
		.unwrap();
	let other_frame = encode_test_append_only_storage_frame(
		other.table_object_id,
		&[(other_key, Locality::new(40), hlc(40), 300, false)],
	);
	let other_entries = storage_multirow_index_entries_from_frame(&other_frame)
		.unwrap()
		.expect("other table entries");
	mgr.append_durable_multirow_with_locality(tenant(), &other_frame, &other_entries)
		.await
		.unwrap();

	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let old_encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let old_uri = l2
		.put(
			&format!("tier/drop_reclaim_{slab_id:016x}.wnd"),
			&old_encrypted.bytes,
		)
		.await
		.unwrap();
	let old_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: old_uri.clone(),
		payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_encrypted.tenant_kek_id),
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(old_pointer).await.unwrap();

	let committer = DirectTierCommitter {
		manager: mgr.clone(),
	};
	let report = mgr
		.reclaim_table_drop_tiered_storage(tenant(), &target, &schemas, &committer, usize::MAX)
		.await
		.unwrap();

	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.reclaimed_records, 2);
	assert!(mgr.lookup(tenant(), target_key_a).is_none());
	assert!(mgr.lookup(tenant(), target_key_b).is_none());
	assert!(mgr.lookup(tenant(), other_key).is_some());
	let rewritten = mgr.tier_pointer(slab_id).await.expect("rewritten pointer");
	assert_ne!(rewritten.l2_uri, old_uri);
	assert!(l2.get(&old_uri).await.is_err());
}

#[tokio::test]
async fn l1_table_retention_prunes_append_only_rows_when_cluster_retention_disabled() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let descriptor = append_only_retention_descriptor(TableObjectId::new([0x48; 16]));
	let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
	let old_key = id(0x4801);
	let live_key = id(0x4802);
	let frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[
			(old_key, Locality::new(11), hlc(30_000), 100, false),
			(live_key, Locality::new(12), hlc(50_000), 200, false),
		],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("append-only storage entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();

	let old_slab_id = force_seal_active(&mgr).await;
	let report = mgr
		.prune_versions_for_retention(
			MvccRetentionPolicy::Disabled,
			&lifecycle,
			&schemas,
			&SecondaryIndexCatalog::default(),
			100_000,
			1,
		)
		.await
		.unwrap();

	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.pruned_versions, 1);
	assert!(mgr.lookup(tenant(), old_key).is_none());
	let live_location = mgr.lookup(tenant(), live_key).expect("live row survives");
	assert_ne!(live_location.slab_id, old_slab_id);
	let live_frame = mgr.read(live_location).await.unwrap();
	let live_entries = storage_multirow_index_entries_from_frame(&live_frame)
		.unwrap()
		.expect("rewritten live entry");
	assert_eq!(live_entries.len(), 1);
	assert_eq!(live_entries[0].key_identity, live_key);
}

#[tokio::test]
async fn l1_table_retention_reports_lag_for_retained_rows_below_floor() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let descriptor = append_only_retention_descriptor(TableObjectId::new([0x4A; 16]));
	let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
	let pruned_key = id(0x4A01);
	let retained_key = id(0x4A02);
	let frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[
			(pruned_key, Locality::new(31), hlc(30_000), 100, false),
			(retained_key, Locality::new(32), hlc(30_000), 200, false),
		],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("append-only storage entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
		descriptor.table_object_id,
		retained_key,
		Locality::new(32),
		hlc(50_000),
	);
	mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();

	force_seal_active(&mgr).await;
	let report = mgr
		.prune_versions_for_retention(
			MvccRetentionPolicy::Disabled,
			&lifecycle,
			&schemas,
			&SecondaryIndexCatalog::default(),
			100_000,
			1,
		)
		.await
		.unwrap();

	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.pruned_versions, 1);
	assert_eq!(report.max_lag_seconds, 70);
	assert!(mgr.lookup(tenant(), pruned_key).is_none());
	assert!(
		!mgr.lookup_as_of(tenant(), retained_key, hlc(30_000))
			.expect("retained data row below floor")
			.is_tombstone
	);
	assert!(
		mgr.lookup_as_of(tenant(), retained_key, hlc(50_000))
			.expect("retained tombstone above floor")
			.is_tombstone
	);
}

#[tokio::test]
async fn l1_table_retention_reports_lag_without_rewrite_for_pair_waiting_on_tombstone() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let descriptor = append_only_retention_descriptor(TableObjectId::new([0x4B; 16]));
	let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
	let retained_key = id(0x4B01);
	let frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(retained_key, Locality::new(33), hlc(30_000), 200, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("append-only storage entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
		descriptor.table_object_id,
		retained_key,
		Locality::new(33),
		hlc(50_000),
	);
	mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();

	force_seal_active(&mgr).await;
	let report = mgr
		.prune_versions_for_retention(
			MvccRetentionPolicy::Disabled,
			&lifecycle,
			&schemas,
			&SecondaryIndexCatalog::default(),
			100_000,
			1,
		)
		.await
		.unwrap();

	assert_eq!(report.compacted_slabs, 0);
	assert_eq!(report.pruned_versions, 0);
	assert_eq!(report.max_lag_seconds, 70);
	assert!(
		!mgr.lookup_as_of(tenant(), retained_key, hlc(30_000))
			.expect("retained data row below floor")
			.is_tombstone
	);
	assert!(
		mgr.lookup_as_of(tenant(), retained_key, hlc(50_000))
			.expect("retained tombstone above floor")
			.is_tombstone
	);
}

#[tokio::test]
async fn l1_table_retention_keeps_pk_boundary_tombstone_under_cluster_floor() {
	let dir = fresh_dir("pk-boundary-tombstone-reopen");
	let io = spawn_io_thread(64).unwrap();
	let crypto = test_crypto();
	let mgr = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let descriptor = pk_retention_descriptor(TableObjectId::new([0x50; 16]));
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), descriptor.clone()).unwrap();
	let lifecycle = TableLifecycleCatalog::default();
	let key = id(0x5001);
	let locality = Locality::new(50);
	let data_frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(key, locality, hlc(10_000), 100, false)],
	);
	let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
		.unwrap()
		.expect("pk data entry");
	mgr.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
		.await
		.unwrap();
	let (tombstone_frame, tombstone_entries) =
		encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20_000));
	mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();

	force_seal_active(&mgr).await;
	let report = mgr
		.prune_versions_for_retention(
			MvccRetentionPolicy::FixedWindow(Duration::from_millis(60_000)),
			&lifecycle,
			&schemas,
			&SecondaryIndexCatalog::default(),
			100_000,
			1,
		)
		.await
		.unwrap();

	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.pruned_versions, 1);
	assert_eq!(report.pair_drops, 0);
	assert!(mgr.lookup_as_of(tenant(), key, hlc(10_000)).is_none());
	assert!(
		mgr.lookup_as_of(tenant(), key, hlc(20_000))
			.expect("pk boundary tombstone survives")
			.is_tombstone
	);
	assert!(
		mgr.lookup(tenant(), key)
			.expect("latest pk boundary tombstone survives")
			.is_tombstone
	);
	mgr.fsync_all_local_slabs().await.unwrap();
	drop(mgr);

	let reopened = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	assert!(
		reopened.lookup_as_of(tenant(), key, hlc(10_000)).is_none(),
		"pruned data row should stay absent after reopen"
	);
	assert!(
		reopened
			.lookup_as_of(tenant(), key, hlc(20_000))
			.expect("compacted pk boundary tombstone survives reopen")
			.is_tombstone
	);
	assert!(
		reopened
			.lookup(tenant(), key)
			.expect("latest compacted pk tombstone survives reopen")
			.is_tombstone
	);
}

#[tokio::test]
async fn l1_retention_pruning_pair_gc_drops_data_and_row_tombstone() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let key = id(0x4701);
	let loc = Locality::new(47);
	let data_frame = encode_test_multirow_storage_frame(&[(key, loc, hlc(10), 100)]);
	let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
		.unwrap()
		.expect("data entry");
	mgr.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
		.await
		.unwrap();
	let (tombstone_frame, tombstone_entries) =
		encode_test_row_tombstone_marker_frame(TableObjectId::new([0x47; 16]), key, loc, hlc(20));
	mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();

	force_seal_active(&mgr).await;
	let report = mgr
		.prune_versions_older_than(
			hlc(30),
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			1,
		)
		.await
		.unwrap();

	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.pruned_versions, 2);
	assert!(mgr.lookup(tenant(), key).is_none());
	assert!(mgr.lookup_as_of(tenant(), key, hlc(10)).is_none());
}

#[tokio::test]
async fn l1_retention_pruning_keeps_pair_until_floor_passes_tombstone() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let key = id(0x4702);
	let loc = Locality::new(48);
	let data_frame = encode_test_multirow_storage_frame(&[(key, loc, hlc(10), 100)]);
	let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
		.unwrap()
		.expect("data entry");
	mgr.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
		.await
		.unwrap();
	let (tombstone_frame, tombstone_entries) =
		encode_test_row_tombstone_marker_frame(TableObjectId::new([0x48; 16]), key, loc, hlc(20));
	mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();

	force_seal_active(&mgr).await;
	let report = mgr
		.prune_versions_older_than(
			hlc(20),
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			1,
		)
		.await
		.unwrap();

	assert_eq!(report.compacted_slabs, 0);
	assert_eq!(report.pruned_versions, 0);
	assert!(
		!mgr.lookup_as_of(tenant(), key, hlc(10))
			.expect("data before delete")
			.is_tombstone
	);
	assert!(
		mgr.lookup_as_of(tenant(), key, hlc(20))
			.expect("tombstone at delete")
			.is_tombstone
	);
}

#[tokio::test]
async fn tiered_retention_rewrite_pair_gc_drops_data_and_row_tombstone() {
	let l1_dir = fresh_dir("retention-tombstone-pair-l1");
	let l2_root = fresh_dir("retention-tombstone-pair-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
	let io = spawn_io_thread(64).unwrap();
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);

	let key_a = id(0x4703);
	let key_b = id(0x4704);
	let loc_a = Locality::new(49);
	let loc_b = Locality::new(50);
	let data_a = encode_test_multirow_storage_frame(&[(key_a, loc_a, hlc(10), 100)]);
	let entries_a = storage_multirow_index_entries_from_frame(&data_a)
		.unwrap()
		.expect("data a entry");
	mgr.append_durable_multirow_with_locality(tenant(), &data_a, &entries_a)
		.await
		.unwrap();
	let (tombstone_a, tombstone_entries_a) = encode_test_row_tombstone_marker_frame(
		TableObjectId::new([0x49; 16]),
		key_a,
		loc_a,
		hlc(20),
	);
	mgr.append_durable_multirow_with_locality(tenant(), &tombstone_a, &tombstone_entries_a)
		.await
		.unwrap();
	let data_b = encode_test_multirow_storage_frame(&[(key_b, loc_b, hlc(12), 200)]);
	let entries_b = storage_multirow_index_entries_from_frame(&data_b)
		.unwrap()
		.expect("data b entry");
	mgr.append_durable_multirow_with_locality(tenant(), &data_b, &entries_b)
		.await
		.unwrap();

	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let old_encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let old_uri = l2
		.put(
			&format!("tier/slab_{slab_id:016x}-pair.wnd"),
			&old_encrypted.bytes,
		)
		.await
		.unwrap();
	let old_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: old_uri,
		payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_encrypted.tenant_kek_id),
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

	let prepared = mgr
		.prepare_tiered_retention_rewrite(&old_pointer, hlc(30), &TableSchemaCatalog::default())
		.await
		.unwrap()
		.expect("retention rewrite");
	let PreparedTieredRetentionRewrite::Rewrite {
		encrypted_bytes,
		pruned_versions,
		..
	} = prepared
	else {
		panic!("expected rewrite with key_b survivor");
	};
	assert_eq!(pruned_versions, 2);

	let rewritten_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: "rewritten-pair".to_string(),
		payload_hash: crate::hash::content::digest(&encrypted_bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_encrypted.tenant_kek_id),
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	let rewritten_plaintext = decode_tiered_blob(
		mgr.config.crypto.as_ref(),
		&rewritten_pointer,
		&encrypted_bytes,
		&BTreeSet::new(),
	)
	.unwrap();
	let recovered =
		recover_slab_entries_from_blob(&rewritten_plaintext, slab_id, &BTreeSet::new()).unwrap();
	assert_eq!(recovered.len(), 1);
	assert_eq!(recovered[0].key_identity, key_b);
	assert!(!recovered[0].location.is_tombstone);
}

#[tokio::test]
async fn tiered_table_retention_keeps_pk_boundary_tombstone_under_cluster_floor() {
	let l1_dir = fresh_dir("retention-pk-boundary-l1");
	let l2_root = fresh_dir("retention-pk-boundary-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
	let io = spawn_io_thread(64).unwrap();
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let descriptor = pk_retention_descriptor(TableObjectId::new([0x58; 16]));
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), descriptor.clone()).unwrap();
	let lifecycle = TableLifecycleCatalog::default();
	let key = id(0x5801);
	let locality = Locality::new(58);
	let data_frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(key, locality, hlc(10_000), 100, false)],
	);
	let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
		.unwrap()
		.expect("pk data entry");
	mgr.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
		.await
		.unwrap();
	let (tombstone_frame, tombstone_entries) =
		encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20_000));
	mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();

	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let old_encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &schemas)
		.unwrap();
	let old_footer = mgr
		.encrypted_tiered_footer_summary(&old_encrypted.bytes)
		.unwrap()
		.expect("encrypted footer");
	let old_uri = l2
		.put(
			&format!("tier/slab_{slab_id:016x}-pk-boundary.wnd"),
			&old_encrypted.bytes,
		)
		.await
		.unwrap();
	let old_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: old_uri,
		payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_footer.tenant_kek_id),
		stats_format_version: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

	let prepared = mgr
		.prepare_tiered_table_retention_rewrite(
			&old_pointer,
			MvccRetentionPolicy::FixedWindow(Duration::from_millis(60_000)),
			&lifecycle,
			&schemas,
			100_000,
		)
		.await
		.unwrap()
		.expect("pk boundary retention rewrite");
	let PreparedTieredRetentionRewrite::Rewrite {
		encrypted_bytes,
		pruned_versions,
		retention_stats,
		tombstone_metadata,
		..
	} = prepared
	else {
		panic!("expected rewrite retaining pk boundary tombstone");
	};
	assert_eq!(pruned_versions, 1);
	assert_eq!(retention_stats.pruned_data_rows, 1);
	assert_eq!(retention_stats.pruned_tombstone_rows, 0);
	assert_eq!(tombstone_metadata, TombstoneMetadata::Earliest(hlc(20_000)));

	let rewritten_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: "rewritten-pk-boundary".to_string(),
		payload_hash: crate::hash::content::digest(&encrypted_bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_footer.tenant_kek_id),
		stats_format_version: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	let rewritten_plaintext = decode_tiered_blob(
		mgr.config.crypto.as_ref(),
		&rewritten_pointer,
		&encrypted_bytes,
		&BTreeSet::new(),
	)
	.unwrap();
	let recovered =
		recover_slab_entries_from_blob(&rewritten_plaintext, slab_id, &BTreeSet::new()).unwrap();
	assert_eq!(recovered.len(), 1);
	assert_eq!(recovered[0].key_identity, key);
	assert_eq!(recovered[0].location.hlc, hlc(20_000));
	assert!(recovered[0].location.is_tombstone);
}

#[tokio::test]
async fn tiered_table_retention_rewrite_prunes_append_only_rows_when_cluster_retention_disabled() {
	let l1_dir = fresh_dir("retention-table-l1");
	let l2_root = fresh_dir("retention-table-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
	let io = spawn_io_thread(64).unwrap();
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let descriptor = append_only_retention_descriptor(TableObjectId::new([0x49; 16]));
	let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
	let old_key = id(0x4901);
	let live_key = id(0x4902);
	let frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[
			(old_key, Locality::new(21), hlc(30_000), 100, false),
			(live_key, Locality::new(22), hlc(50_000), 200, false),
		],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("append-only storage entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();

	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let old_encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &schemas)
		.unwrap();
	let old_footer = mgr
		.encrypted_tiered_footer_summary(&old_encrypted.bytes)
		.unwrap()
		.expect("encrypted footer");
	let old_uri = l2
		.put(
			&format!("tier/slab_{slab_id:016x}-table-retention.wnd"),
			&old_encrypted.bytes,
		)
		.await
		.unwrap();
	let old_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: old_uri,
		payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_footer.tenant_kek_id),
		stats_format_version: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

	let prepared = mgr
		.prepare_tiered_table_retention_rewrite(
			&old_pointer,
			MvccRetentionPolicy::Disabled,
			&lifecycle,
			&schemas,
			100_000,
		)
		.await
		.unwrap()
		.expect("table retention rewrite");
	let PreparedTieredRetentionRewrite::Rewrite {
		encrypted_bytes,
		pruned_versions,
		..
	} = prepared
	else {
		panic!("expected rewrite with live row survivor");
	};
	assert_eq!(pruned_versions, 1);

	let rewritten_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: "rewritten-table-retention".to_string(),
		payload_hash: crate::hash::content::digest(&encrypted_bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_footer.tenant_kek_id),
		stats_format_version: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: old_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	let rewritten_plaintext = decode_tiered_blob(
		mgr.config.crypto.as_ref(),
		&rewritten_pointer,
		&encrypted_bytes,
		&BTreeSet::new(),
	)
	.unwrap();
	let recovered =
		recover_slab_entries_from_blob(&rewritten_plaintext, slab_id, &BTreeSet::new()).unwrap();
	assert_eq!(recovered.len(), 1);
	assert_eq!(recovered[0].key_identity, live_key);
}

#[tokio::test]
async fn tiered_table_retention_reports_lag_without_rewrite_for_pair_waiting_on_tombstone() {
	let l1_dir = fresh_dir("retention-table-lag-l1");
	let l2_root = fresh_dir("retention-table-lag-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
	let io = spawn_io_thread(64).unwrap();
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let descriptor = append_only_retention_descriptor(TableObjectId::new([0x4C; 16]));
	let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
	let retained_key = id(0x4C01);
	let frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(retained_key, Locality::new(34), hlc(30_000), 200, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("append-only storage entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
		descriptor.table_object_id,
		retained_key,
		Locality::new(34),
		hlc(50_000),
	);
	mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();

	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let old_encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let old_uri = l2
		.put(
			&format!("tier/slab_{slab_id:016x}-table-retention-lag.wnd"),
			&old_encrypted.bytes,
		)
		.await
		.unwrap();
	let old_pointer = TieredSlabPointer {
		slab_id,
		l2_uri: old_uri,
		payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(old_encrypted.tenant_kek_id),
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

	let prepared = mgr
		.prepare_tiered_table_retention_rewrite(
			&old_pointer,
			MvccRetentionPolicy::Disabled,
			&lifecycle,
			&schemas,
			100_000,
		)
		.await
		.unwrap()
		.expect("retention observation");
	let PreparedTieredRetentionRewrite::Observe { retention_stats } = prepared else {
		panic!("expected lag-only observation");
	};
	assert_eq!(retention_stats.pruned_data_rows, 0);
	assert_eq!(retention_stats.pruned_tombstone_rows, 0);
	assert_eq!(retention_stats.max_lag_seconds, 70);
}

#[tokio::test]
async fn sealed_layout_recluster_single_to_multi_preserves_reads() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let single_descriptor = layout_recluster_descriptor(0x62, 1, RecordLayoutPolicy::SingleRow);
	let multi_descriptor = layout_recluster_descriptor(
		0x62,
		2,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap()),
	);
	let batch = layout_recluster_batch(&[1, 2, 3, 4]);
	let meta = layout_recluster_meta(&single_descriptor, batch.num_rows());
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&single_descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	for row in 0..batch.num_rows() {
		let row_batch = batch.slice(row, 1);
		let frame = encode_test_batch(&row_batch);
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[row],
			admission.localities[row],
			hlc(10 + row as u64),
			&frame,
		)
		.await
		.unwrap();
	}
	let old_slab_id = force_seal_active(&mgr).await;

	let report = mgr
		.recluster_sealed_table_layout(
			tenant(),
			&multi_descriptor,
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			10,
		)
		.await
		.unwrap();

	assert_eq!(report.rewritten_slabs, 1);
	assert_eq!(report.relocated_rows, batch.num_rows());
	let mut offsets = BTreeSet::new();
	for (row, expected) in [1, 2, 3, 4].into_iter().enumerate() {
		let location = mgr
			.lookup_as_of(tenant(), admission.key_identities[row], hlc(100))
			.expect("reclustered row");
		assert_ne!(location.slab_id, old_slab_id);
		offsets.insert(location.offset);
		let frame = mgr.read(location).await.unwrap();
		let storage_batch = decode_record_batch(&frame).unwrap();
		assert!(has_storage_synthetic_columns(&storage_batch));
		let user_batch =
			materialize_record_location_batch(&storage_batch, location.record_row_index).unwrap();
		assert_eq!(first_i64(&user_batch), expected);
	}
	assert_eq!(
		offsets.len(),
		batch.num_rows(),
		"the test descriptor maps each primary key into a distinct routing bucket"
	);

	let second = mgr
		.recluster_sealed_table_layout(
			tenant(),
			&multi_descriptor,
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			10,
		)
		.await
		.unwrap();
	assert_eq!(second, LayoutReclusterReport::default());
}

#[tokio::test]
async fn sealed_layout_recluster_multi_to_single_preserves_reads() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let multi_descriptor = layout_recluster_descriptor(
		0x63,
		1,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap()),
	);
	let single_descriptor = layout_recluster_descriptor(0x63, 2, RecordLayoutPolicy::SingleRow);
	let batch = layout_recluster_batch(&[1, 2, 3, 4]);
	let meta = layout_recluster_meta(&multi_descriptor, batch.num_rows());
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&multi_descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let packed = pack_record_batch_by_bucket(
		&meta,
		&multi_descriptor,
		&batch,
		WriterNodeId::ZERO,
		hlc(10),
	)
	.unwrap();
	for record in packed {
		let entries = storage_multirow_index_entries_from_frame(&record.frame_bytes)
			.unwrap()
			.expect("multi-row entries");
		mgr.append_durable_multirow_with_locality(tenant(), &record.frame_bytes, &entries)
			.await
			.unwrap();
	}
	let old_slab_id = force_seal_active(&mgr).await;

	let report = mgr
		.recluster_sealed_table_layout(
			tenant(),
			&single_descriptor,
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			10,
		)
		.await
		.unwrap();

	assert_eq!(report.rewritten_slabs, 1);
	assert_eq!(report.relocated_rows, batch.num_rows());
	let mut offsets = BTreeSet::new();
	for (row, expected) in [1, 2, 3, 4].into_iter().enumerate() {
		let location = mgr
			.lookup_as_of(tenant(), admission.key_identities[row], hlc(100))
			.expect("reclustered row");
		assert_ne!(location.slab_id, old_slab_id);
		assert_eq!(location.record_row_index, RecordRowIndex::ZERO);
		offsets.insert(location.offset);
		let frame = mgr.read(location).await.unwrap();
		let user_batch = decode_record_batch(&frame).unwrap();
		assert!(!has_storage_synthetic_columns(&user_batch));
		assert_eq!(first_i64(&user_batch), expected);
	}
	assert_eq!(
		offsets.len(),
		batch.num_rows(),
		"multi-row rollback should materialize one physical record per row"
	);
}

async fn assert_sealed_layout_recluster_updates_multirow_codec(
	object_byte: u8,
	from_policy: RecordBodyCompressionPolicy,
	to_policy: RecordBodyCompressionPolicy,
) {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let layout = MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap();
	let source_descriptor = layout_recluster_descriptor_with_codec(
		object_byte,
		1,
		RecordLayoutPolicy::MultiRow(layout),
		from_policy,
	);
	let target_descriptor = layout_recluster_descriptor_with_codec(
		object_byte,
		2,
		RecordLayoutPolicy::MultiRow(layout),
		to_policy,
	);
	let batch = layout_recluster_batch(&[1, 2, 3, 4]);
	let meta = layout_recluster_meta(&source_descriptor, batch.num_rows());
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&source_descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let source_rows = admission
		.key_identities
		.iter()
		.zip(admission.localities.iter())
		.enumerate()
		.map(
			|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
				source_row_index,
				key_identity: *key_identity,
				routing_locality: *routing_locality,
				row_hlc: hlc(10 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: None,
			},
		)
		.collect::<Vec<_>>();
	let encoded =
		encode_multirow_from_survivors_with_policy(&batch, &source_rows, from_policy).unwrap();
	assert_eq!(
		crate::storage::ipc::record_batch_compression_policy_from_ipc(&encoded.frame).unwrap(),
		from_policy
	);
	let encoded_entries = encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	let old_slab_id = force_seal_active(&mgr).await;

	let report = mgr
		.recluster_sealed_table_layout(
			tenant(),
			&target_descriptor,
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			10,
		)
		.await
		.unwrap();

	assert_eq!(report.rewritten_slabs, 1);
	assert_eq!(report.relocated_rows, batch.num_rows());
	for (row, expected) in [1, 2, 3, 4].into_iter().enumerate() {
		let location = mgr
			.lookup_as_of(tenant(), admission.key_identities[row], hlc(100))
			.expect("reclustered row");
		assert_ne!(location.slab_id, old_slab_id);
		let frame = mgr.read(location).await.unwrap();
		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&frame).unwrap(),
			to_policy
		);
		let storage_batch = decode_record_batch(&frame).unwrap();
		assert!(has_storage_synthetic_columns(&storage_batch));
		let user_batch =
			materialize_record_location_batch(&storage_batch, location.record_row_index).unwrap();
		assert_eq!(first_i64(&user_batch), expected);
	}

	let second = mgr
		.recluster_sealed_table_layout(
			tenant(),
			&target_descriptor,
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			10,
		)
		.await
		.unwrap();
	assert_eq!(second, LayoutReclusterReport::default());
}

#[tokio::test]
async fn sealed_layout_recluster_same_layout_updates_multirow_codec() {
	assert_sealed_layout_recluster_updates_multirow_codec(
		0x64,
		RecordBodyCompressionPolicy::Uncompressed,
		RecordBodyCompressionPolicy::Lz4Frame,
	)
	.await;
	assert_sealed_layout_recluster_updates_multirow_codec(
		0x65,
		RecordBodyCompressionPolicy::Lz4Frame,
		RecordBodyCompressionPolicy::Uncompressed,
	)
	.await;
}

#[tokio::test]
async fn active_slab_reads_mixed_record_body_compression() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let uncompressed_descriptor = layout_recluster_descriptor_with_codec(
		0x66,
		1,
		RecordLayoutPolicy::SingleRow,
		RecordBodyCompressionPolicy::Uncompressed,
	);
	let lz4_descriptor = layout_recluster_descriptor_with_codec(
		0x66,
		2,
		RecordLayoutPolicy::SingleRow,
		RecordBodyCompressionPolicy::Lz4Frame,
	);

	let mut inserted = Vec::new();
	for (descriptor, value, ts) in [
		(&uncompressed_descriptor, 7_i64, hlc(10)),
		(&lz4_descriptor, 8_i64, hlc(20)),
	] {
		let batch = layout_recluster_batch(&[value]);
		let meta = layout_recluster_meta(descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let frame = encode_batch_with_policy(&batch, descriptor.record_body_compression_policy)
			.unwrap()
			.bytes;
		let location = mgr
			.append_durable_with_locality(
				tenant(),
				admission.key_identities[0],
				admission.localities[0],
				ts,
				&frame,
			)
			.await
			.unwrap();
		inserted.push((
			admission.key_identities[0],
			location,
			value,
			descriptor.record_body_compression_policy,
		));
	}

	assert_eq!(inserted[0].1.slab_id, inserted[1].1.slab_id);
	for (key_identity, location, expected_value, expected_policy) in inserted {
		let found = mgr
			.lookup_as_of(tenant(), key_identity, hlc(100))
			.expect("mixed-codec row");
		assert_eq!(found, location);
		let frame = mgr.read(found).await.unwrap();
		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&frame).unwrap(),
			expected_policy
		);
		let batch = decode_record_batch(&frame).unwrap();
		assert!(!has_storage_synthetic_columns(&batch));
		assert_eq!(first_i64(&batch), expected_value);
	}
}

#[tokio::test]
async fn recovery_reopens_pk_delete_and_resurrection_chain() {
	let dir = fresh_dir("pk-delete-recovery");
	let io = spawn_io_thread(64).unwrap();
	let crypto = test_crypto();
	let descriptor = pk_retention_descriptor(TableObjectId::new([0x57; 16]));
	let key = id(0x5701);
	let locality = Locality::new(57);

	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let data_frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(key, locality, hlc(10), 100, false)],
	);
	let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
		.unwrap()
		.expect("pk data entry");
	manager
		.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
		.await
		.unwrap();
	let (tombstone_frame, tombstone_entries) =
		encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20));
	manager
		.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);

	let reopened = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	assert_eq!(
		reopened.routing_locality_for_existing_key(tenant(), key),
		Some(locality)
	);
	assert!(
		!reopened
			.lookup_as_of(tenant(), key, hlc(10))
			.expect("pre-delete data survives reopen")
			.is_tombstone
	);
	assert!(
		reopened
			.lookup_as_of(tenant(), key, hlc(20))
			.expect("delete tombstone survives reopen")
			.is_tombstone
	);
	assert!(
		reopened
			.lookup(tenant(), key)
			.expect("latest delete tombstone survives reopen")
			.is_tombstone
	);

	let resurrected_frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(key, locality, hlc(30), 200, false)],
	);
	let resurrected_entries = storage_multirow_index_entries_from_frame(&resurrected_frame)
		.unwrap()
		.expect("pk resurrected data entry");
	reopened
		.append_durable_multirow_with_locality(tenant(), &resurrected_frame, &resurrected_entries)
		.await
		.unwrap();
	reopened.fsync_all_local_slabs().await.unwrap();
	drop(reopened);

	let reopened = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	assert!(
		reopened
			.lookup_as_of(tenant(), key, hlc(20))
			.expect("historical delete tombstone survives resurrection reopen")
			.is_tombstone
	);
	let latest = reopened
		.lookup_as_of(tenant(), key, hlc(30))
		.expect("resurrected data survives reopen");
	assert!(!latest.is_tombstone);
	let frame = reopened.read(latest).await.unwrap();
	let batch = decode_record_batch(&frame).unwrap();
	let values = batch
		.column(0)
		.as_any()
		.downcast_ref::<UInt64Array>()
		.unwrap();
	assert_eq!(values.value(0), 200);
}

#[tokio::test]
async fn recovery_fails_closed_on_cross_slab_consecutive_pk_tombstones() {
	let dir = fresh_dir("pk-consecutive-tombstone-recovery");
	let io = spawn_io_thread(64).unwrap();
	let crypto = test_crypto();
	let descriptor = pk_retention_descriptor(TableObjectId::new([0x59; 16]));
	let key = id(0x5901);
	let locality = Locality::new(59);

	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let data_frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(key, locality, hlc(10), 100, false)],
	);
	let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
		.unwrap()
		.expect("pk data entry");
	manager
		.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
		.await
		.unwrap();
	let (first_tombstone_frame, first_tombstone_entries) =
		encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20));
	manager
		.append_durable_multirow_with_locality(
			tenant(),
			&first_tombstone_frame,
			&first_tombstone_entries,
		)
		.await
		.unwrap();
	force_seal_active(&manager).await;
	let (second_tombstone_frame, second_tombstone_entries) =
		encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(30));
	manager
		.append_durable_multirow_with_locality(
			tenant(),
			&second_tombstone_frame,
			&second_tombstone_entries,
		)
		.await
		.unwrap();
	manager.fsync_all_local_slabs().await.unwrap();
	drop(manager);

	let reopened = SlabManager::open(SlabManagerConfig {
		dir: dir.to_path_buf(),
		max_slab_bytes: 1024 * 1024,
		io,
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto,
	})
	.await;
	let err = match reopened {
		Ok(_) => panic!("consecutive tombstones across slabs must fail recovery"),
		Err(err) => err,
	};
	assert_eq!(err.kind(), ErrorKind::InvalidData);
	assert!(err.to_string().contains("duplicate row tombstone"));
}

#[tokio::test]
async fn recovery_reopens_mixed_record_body_compression_slab() {
	let dir = fresh_dir("mixed-compression-recovery");
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024,
			io: spawn_io_thread(64).unwrap(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let uncompressed_descriptor = layout_recluster_descriptor_with_codec(
		0x68,
		1,
		RecordLayoutPolicy::SingleRow,
		RecordBodyCompressionPolicy::Uncompressed,
	);
	let lz4_descriptor = layout_recluster_descriptor_with_codec(
		0x68,
		2,
		RecordLayoutPolicy::SingleRow,
		RecordBodyCompressionPolicy::Lz4Frame,
	);

	let mut inserted = Vec::new();
	for (descriptor, value, ts) in [
		(&uncompressed_descriptor, 17_i64, hlc(10)),
		(&lz4_descriptor, 18_i64, hlc(20)),
	] {
		let batch = layout_recluster_batch(&[value]);
		let meta = layout_recluster_meta(descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let frame = encode_batch_with_policy(&batch, descriptor.record_body_compression_policy)
			.unwrap()
			.bytes;
		let location = mgr
			.append_durable_with_locality(
				tenant(),
				admission.key_identities[0],
				admission.localities[0],
				ts,
				&frame,
			)
			.await
			.unwrap();
		inserted.push((
			admission.key_identities[0],
			location,
			value,
			descriptor.record_body_compression_policy,
		));
	}
	assert_eq!(inserted[0].1.slab_id, inserted[1].1.slab_id);
	mgr.fsync_all_local_slabs().await.unwrap();
	drop(mgr);

	let reopened = SlabManager::open(SlabManagerConfig {
		dir: dir.to_path_buf(),
		max_slab_bytes: 1024 * 1024,
		io: spawn_io_thread(64).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();
	for (key_identity, location, expected_value, expected_policy) in inserted {
		let found = reopened
			.lookup_as_of(tenant(), key_identity, hlc(100))
			.expect("recovered mixed-codec row");
		assert_eq!(found, location);
		let frame = reopened.read(found).await.unwrap();
		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&frame).unwrap(),
			expected_policy
		);
		let batch = decode_record_batch(&frame).unwrap();
		assert_eq!(first_i64(&batch), expected_value);
	}
}

#[tokio::test]
async fn l2_reads_mixed_record_body_compression_slab() {
	let l1_dir = fresh_dir("mixed-compression-l2-l1");
	let l2_dir = fresh_dir("mixed-compression-l2-objects");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let uncompressed_descriptor = layout_recluster_descriptor_with_codec(
		0x69,
		1,
		RecordLayoutPolicy::SingleRow,
		RecordBodyCompressionPolicy::Uncompressed,
	);
	let lz4_descriptor = layout_recluster_descriptor_with_codec(
		0x69,
		2,
		RecordLayoutPolicy::SingleRow,
		RecordBodyCompressionPolicy::Lz4Frame,
	);

	let mut inserted = Vec::new();
	for (descriptor, value, ts) in [
		(&uncompressed_descriptor, 27_i64, hlc(10)),
		(&lz4_descriptor, 28_i64, hlc(20)),
	] {
		let batch = layout_recluster_batch(&[value]);
		let meta = layout_recluster_meta(descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let frame = encode_batch_with_policy(&batch, descriptor.record_body_compression_policy)
			.unwrap()
			.bytes;
		let location = mgr
			.append_durable_with_locality(
				tenant(),
				admission.key_identities[0],
				admission.localities[0],
				ts,
				&frame,
			)
			.await
			.unwrap();
		inserted.push((
			admission.key_identities[0],
			location,
			value,
			descriptor.record_body_compression_policy,
			frame.len() as u64,
		));
	}
	let sealed_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(sealed_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
	let payload_hash = crate::hash::content::digest(&encrypted.bytes);
	let uri = l2
		.put(
			&format!("mixed-compression/slab_{sealed_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id: sealed_id,
		l2_uri: uri,
		payload_hash,
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();
	assert!(mgr.is_tiered(sealed_id).await);
	assert!(
		!slab_path(&l1_dir, sealed_id).exists(),
		"tiered slab should be unlinked from L1 before L2 reads"
	);

	let uncompressed_fetch_before =
		storage_compression_l2_fetch_metric_value(RecordBodyCompressionPolicy::Uncompressed);
	let lz4_fetch_before =
		storage_compression_l2_fetch_metric_value(RecordBodyCompressionPolicy::Lz4Frame);
	let mut expected_uncompressed_fetch_bytes = 0_u64;
	let mut expected_lz4_fetch_bytes = 0_u64;

	for (key_identity, location, expected_value, expected_policy, expected_frame_bytes) in inserted
	{
		let found = mgr
			.lookup_as_of(tenant(), key_identity, hlc(100))
			.expect("tiered mixed-codec row");
		assert_eq!(found, location);
		let frame = mgr.read(found).await.unwrap();
		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&frame).unwrap(),
			expected_policy
		);
		let batch = decode_record_batch(&frame).unwrap();
		assert_eq!(first_i64(&batch), expected_value);
		match expected_policy {
			RecordBodyCompressionPolicy::Uncompressed => {
				expected_uncompressed_fetch_bytes =
					expected_uncompressed_fetch_bytes.saturating_add(expected_frame_bytes);
			}
			RecordBodyCompressionPolicy::Lz4Frame => {
				expected_lz4_fetch_bytes =
					expected_lz4_fetch_bytes.saturating_add(expected_frame_bytes);
			}
		}
	}
	assert!(
		storage_compression_l2_fetch_metric_value(RecordBodyCompressionPolicy::Uncompressed)
			>= uncompressed_fetch_before.saturating_add(expected_uncompressed_fetch_bytes)
	);
	assert!(
		storage_compression_l2_fetch_metric_value(RecordBodyCompressionPolicy::Lz4Frame)
			>= lz4_fetch_before.saturating_add(expected_lz4_fetch_bytes)
	);
}

#[tokio::test]
async fn compression_resident_gauge_tracks_sealed_recluster_convergence() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let layout = MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap();
	let source_descriptor = layout_recluster_descriptor_with_codec(
		0x67,
		1,
		RecordLayoutPolicy::MultiRow(layout),
		RecordBodyCompressionPolicy::Uncompressed,
	);
	let target_descriptor = layout_recluster_descriptor_with_codec(
		0x67,
		2,
		RecordLayoutPolicy::MultiRow(layout),
		RecordBodyCompressionPolicy::Lz4Frame,
	);
	let batch = layout_recluster_batch(&[1, 2, 3, 4]);
	let meta = layout_recluster_meta(&source_descriptor, batch.num_rows());
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&source_descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let source_rows = admission
		.key_identities
		.iter()
		.zip(admission.localities.iter())
		.enumerate()
		.map(
			|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
				source_row_index,
				key_identity: *key_identity,
				routing_locality: *routing_locality,
				row_hlc: hlc(10 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: None,
			},
		)
		.collect::<Vec<_>>();
	let encoded = encode_multirow_from_survivors_with_policy(
		&batch,
		&source_rows,
		RecordBodyCompressionPolicy::Uncompressed,
	)
	.unwrap();
	let encoded_entries = encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	force_seal_active(&mgr).await;

	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), target_descriptor.clone()).unwrap();
	let resident = mgr
		.refresh_storage_compression_uncompressed_resident_bytes(&catalog)
		.await
		.unwrap();
	assert_eq!(resident, encoded.frame.len() as u64);
	assert_eq!(
		metrics()
			.storage_compression_uncompressed_resident_bytes
			.get(),
		resident
	);

	let report = mgr
		.recluster_sealed_table_layout(
			tenant(),
			&target_descriptor,
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			10,
		)
		.await
		.unwrap();
	assert_eq!(report.rewritten_slabs, 1);
	let resident = mgr
		.refresh_storage_compression_uncompressed_resident_bytes(&catalog)
		.await
		.unwrap();
	assert_eq!(resident, 0);
	assert_eq!(
		metrics()
			.storage_compression_uncompressed_resident_bytes
			.get(),
		0
	);
}

#[tokio::test]
async fn compression_resident_gauge_counts_tiered_uncompressed_frames() {
	let l1_dir = fresh_dir("compression-gauge-tiered-l1");
	let l2_dir = fresh_dir("compression-gauge-tiered-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let layout = MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap();
	let source_descriptor = layout_recluster_descriptor_with_codec(
		0x6a,
		1,
		RecordLayoutPolicy::MultiRow(layout),
		RecordBodyCompressionPolicy::Uncompressed,
	);
	let target_descriptor = layout_recluster_descriptor_with_codec(
		0x6a,
		2,
		RecordLayoutPolicy::MultiRow(layout),
		RecordBodyCompressionPolicy::Lz4Frame,
	);
	let batch = layout_recluster_batch(&[31, 32, 33, 34]);
	let meta = layout_recluster_meta(&source_descriptor, batch.num_rows());
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&source_descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let source_rows = admission
		.key_identities
		.iter()
		.zip(admission.localities.iter())
		.enumerate()
		.map(
			|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
				source_row_index,
				key_identity: *key_identity,
				routing_locality: *routing_locality,
				row_hlc: hlc(50 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: None,
			},
		)
		.collect::<Vec<_>>();
	let encoded = encode_multirow_from_survivors_with_policy(
		&batch,
		&source_rows,
		RecordBodyCompressionPolicy::Uncompressed,
	)
	.unwrap();
	let encoded_entries = encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let payload_hash = crate::hash::content::digest(&encrypted.bytes);
	let uri = l2
		.put(
			&format!("compression-gauge/slab_{slab_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri: uri,
		payload_hash,
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();
	assert!(
		!slab_path(&l1_dir, slab_id).exists(),
		"tiered gauge should be reading the L2 object, not the deleted L1 slab"
	);

	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), target_descriptor).unwrap();
	let resident = mgr
		.refresh_storage_compression_uncompressed_resident_bytes(&catalog)
		.await
		.unwrap();
	assert_eq!(resident, encoded.frame.len() as u64);
	assert_eq!(
		metrics()
			.storage_compression_uncompressed_resident_bytes
			.get(),
		resident
	);
}

#[tokio::test]
async fn projected_l2_read_materializes_sparse_multirow_record() {
	let l1_dir = fresh_dir("projected-l2-l1");
	let l2_dir = fresh_dir("projected-l2-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024 * 16,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy {
				promote_after_l2_reads: 0,
				..RehydrationPolicy::default()
			},
			l2_column_fetch: L2ColumnFetchConfig {
				enabled: true,
				min_record_bytes: RecordBodyByteLen::new(1),
				..L2ColumnFetchConfig::default()
			},
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let mut fields = vec![Field::new("id", DataType::Int64, false)];
	for column in 1..16 {
		fields.push(Field::new(format!("c{column}"), DataType::Int64, false));
	}
	let schema = Schema::new(fields);
	let mut descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x72; 16]),
		1,
		"analytics",
		"projected_l2",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(20_000, 20_000, 8 * 1024 * 1024).unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let row_count = 20_000_usize;
	let columns = (0..16)
		.map(|column| {
			Arc::new(Int64Array::from_iter_values((0..row_count).map(
				move |row| i64::try_from(column * 1_000_000 + row).unwrap(),
			))) as Arc<dyn Array>
		})
		.collect::<Vec<_>>();
	let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
	let meta = layout_recluster_meta(&descriptor, batch.num_rows());
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let source_rows = admission
		.key_identities
		.iter()
		.zip(admission.localities.iter())
		.enumerate()
		.map(
			|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
				source_row_index,
				key_identity: *key_identity,
				routing_locality: *routing_locality,
				row_hlc: hlc(100 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: None,
			},
		)
		.collect::<Vec<_>>();
	let encoded = encode_multirow_from_survivors_with_policy(
		&batch,
		&source_rows,
		RecordBodyCompressionPolicy::Lz4Frame,
	)
	.unwrap();
	let encoded_entries = encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let payload_hash = crate::hash::content::digest(&encrypted.bytes);
	let uri = l2
		.put(
			&format!("projected-l2/slab_{slab_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	let pointer = TieredSlabPointer {
		slab_id,
		l2_uri: uri,
		payload_hash,
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(pointer.clone()).await.unwrap();
	let location = mgr
		.lookup(tenant(), admission.key_identities[0])
		.expect("tiered row location");
	let requirement = ColumnFetchRequirement {
		output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([15]),
		..ColumnFetchRequirement::default()
	};

	let projected = mgr
		.read_projected_record_from_l2(
			l2.clone(),
			&pointer,
			location,
			&descriptor,
			&requirement,
			None,
			None,
		)
		.await
		.unwrap();
	let full_frame = mgr
		.read_encrypted_record_from_l2(l2.as_ref(), &pointer, location)
		.await
		.unwrap();
	let whole = decode_record_batch(&full_frame).unwrap();

	assert!(matches!(projected.strategy, L2FetchStrategy::Columnar(_)));
	assert_eq!(projected.projected.batch, whole.project(&[15]).unwrap());
	assert_eq!(projected.projected.batch.num_columns(), 1);
	assert_eq!(projected.projected.batch.num_rows(), row_count);
}

#[tokio::test]
async fn row_group_l2_point_read_materializes_projected_group() {
	let l1_dir = fresh_dir("row-group-l2-l1");
	let l2_dir = fresh_dir("row-group-l2-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024 * 16,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy {
				promote_after_l2_reads: 0,
				..RehydrationPolicy::default()
			},
			l2_column_fetch: L2ColumnFetchConfig {
				enabled: true,
				min_record_bytes: RecordBodyByteLen::new(1),
				..L2ColumnFetchConfig::default()
			},
			row_group_writer: Default::default(),
			l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig {
				enabled: true,
				max_selected_row_group_fraction_micros:
					crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
				..Default::default()
			},
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("payload", DataType::Int64, false),
	]);
	let mut descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x73; 16]),
		1,
		"analytics",
		"row_group_l2",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(20_000, 20_000, 8 * 1024 * 1024).unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let row_count = 20_000_usize;
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
			Arc::new(Int64Array::from_iter_values(
				(0..row_count).map(|row| 10_000_000_i64 + row as i64),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let meta = layout_recluster_meta(&descriptor, batch.num_rows());
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let source_rows = admission
		.key_identities
		.iter()
		.zip(admission.localities.iter())
		.enumerate()
		.map(
			|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
				source_row_index,
				key_identity: *key_identity,
				routing_locality: *routing_locality,
				row_hlc: hlc(200 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: None,
			},
		)
		.collect::<Vec<_>>();
	let mut writer_config =
		crate::storage::row_group::RowGroupWriterConfig::point_read().with_enabled(true);
	writer_config.stats_supported_types = std::collections::BTreeSet::new();
	let encoded = crate::storage::multirow::encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&source_rows,
		&descriptor,
		RecordBodyCompressionPolicy::Lz4Frame,
		&writer_config,
	)
	.unwrap();
	let encoded_entries = encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let payload_hash = crate::hash::content::digest(&encrypted.bytes);
	let uri = l2
		.put(
			&format!("row-group-l2/slab_{slab_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	let pointer = TieredSlabPointer {
		slab_id,
		l2_uri: uri,
		payload_hash,
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(pointer.clone()).await.unwrap();
	let target = encoded
		.rows
		.iter()
		.find(|row| row.record_row_index == RecordRowIndex::new(10_000))
		.expect("target storage row");
	let location = mgr
		.lookup(tenant(), target.key_identity)
		.expect("tiered row location");
	let requirement = ColumnFetchRequirement {
		output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([1]),
		..ColumnFetchRequirement::default()
	};
	let predicate_requirement = ColumnFetchRequirement {
		predicate_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([0]),
		output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([1]),
		..ColumnFetchRequirement::default()
	};
	let mut bytecode = Vec::new();
	bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_PUSH_INT);
	bytecode.extend_from_slice(&50_000_i64.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_GT);
	let disjoint_program = crate::schema::pbc::Program::compile(&bytecode).unwrap();

	let pruned = mgr
		.read_projected_record_from_l2(
			l2.clone(),
			&pointer,
			location,
			&descriptor,
			&predicate_requirement,
			Some(&disjoint_program),
			None,
		)
		.await
		.unwrap();
	assert_eq!(pruned.projected.batch.num_rows(), 0);
	let target_group = crate::schema::RowGroupIndex::new(2);
	match &pruned.strategy {
		L2FetchStrategy::RowGroup(plan) => {
			assert!(plan.surviving_groups.is_empty());
			assert_eq!(plan.predicate_fetched_groups, vec![target_group]);
			assert!(plan.phase_b_fetch.is_some());
			assert!(plan.phase_c_fetch.selected_plaintext_ranges.is_empty());
		}
		other => panic!("expected row-group no-survivors strategy, got {other:?}"),
	}

	let mut bytecode = Vec::new();
	bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_PUSH_INT);
	bytecode.extend_from_slice(&0_i64.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_GTE);
	let phase_b_pass_program = crate::schema::pbc::Program::compile(&bytecode).unwrap();
	let phase_b_pass = mgr
		.read_projected_record_from_l2(
			l2.clone(),
			&pointer,
			location,
			&descriptor,
			&predicate_requirement,
			Some(&phase_b_pass_program),
			None,
		)
		.await
		.unwrap();
	match &phase_b_pass.strategy {
		L2FetchStrategy::RowGroup(plan) => {
			assert_eq!(plan.surviving_groups, vec![target_group]);
			assert_eq!(plan.predicate_fetched_groups, vec![target_group]);
			let phase_b_fetch = plan.phase_b_fetch.as_ref().expect("Phase B fetch plan");
			for page in &plan.directory.buffer_pages {
				if page.row_group_index != target_group || page.range.len.get() == 0 {
					continue;
				}
				if page.ipc_column_ordinal == crate::schema::IpcColumnOrdinal::new(0) {
					assert!(body_ranges_cover(
						&phase_b_fetch.selected_plaintext_ranges,
						page.range
					));
					assert!(!body_ranges_cover(
						&plan.phase_c_fetch.selected_plaintext_ranges,
						page.range
					));
				}
				if page.ipc_column_ordinal == crate::schema::IpcColumnOrdinal::new(1) {
					assert!(body_ranges_cover(
						&plan.phase_c_fetch.selected_plaintext_ranges,
						page.range
					));
				}
			}
		}
		other => panic!("expected row-group Phase B strategy, got {other:?}"),
	}
	let predicate_only_requirement = ColumnFetchRequirement {
		predicate_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([0]),
		..ColumnFetchRequirement::default()
	};
	let predicate_only = mgr
		.read_projected_record_from_l2(
			l2.clone(),
			&pointer,
			location,
			&descriptor,
			&predicate_only_requirement,
			Some(&phase_b_pass_program),
			None,
		)
		.await
		.unwrap();
	match &predicate_only.strategy {
		L2FetchStrategy::RowGroup(plan) => {
			assert_eq!(plan.surviving_groups, vec![target_group]);
			assert_eq!(plan.predicate_fetched_groups, vec![target_group]);
			assert!(plan.phase_b_fetch.is_some());
			assert!(plan.phase_c_fetch.selected_plaintext_ranges.is_empty());
		}
		other => panic!("expected predicate-only row-group Phase B strategy, got {other:?}"),
	}
	assert_eq!(predicate_only.projected.batch.num_rows(), 4096);
	assert!(predicate_only.projected.physical_for(0).is_some());

	let full_frame = mgr
		.read_encrypted_record_from_l2(l2.as_ref(), &pointer, location)
		.await
		.unwrap();
	let whole = decode_record_batch(&full_frame).unwrap();
	let expected_id_payload = whole.project(&[0, 1]).unwrap().slice(8192, 4096);
	assert_eq!(phase_b_pass.projected.batch.num_rows(), 4096);
	let id_values = phase_b_pass
		.projected
		.batch
		.column(phase_b_pass.projected.physical_for(0).expect("id physical"))
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("id column");
	let payload_values = phase_b_pass
		.projected
		.batch
		.column(
			phase_b_pass
				.projected
				.physical_for(1)
				.expect("payload physical"),
		)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("payload column");
	let expected_ids = expected_id_payload.column(0);
	let expected_ids = expected_ids
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("expected id column");
	let expected_payloads = expected_id_payload.column(1);
	let expected_payloads = expected_payloads
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("expected payload column");
	assert_eq!(id_values.values(), expected_ids.values());
	assert_eq!(payload_values.values(), expected_payloads.values());

	let indexed_locations = [
		RecordRowIndex::new(10_000),
		RecordRowIndex::new(10_001),
		RecordRowIndex::new(13_000),
	]
	.into_iter()
	.map(|record_row_index| {
		let row = encoded
			.rows
			.iter()
			.find(|row| row.record_row_index == record_row_index)
			.expect("indexed source row");
		mgr.lookup(tenant(), row.key_identity)
			.expect("indexed row location")
	})
	.collect::<Vec<_>>();
	let indexed_projected = mgr
		.read_projected_indexed_rows(&indexed_locations, &descriptor, &requirement)
		.await
		.unwrap()
		.expect("projected indexed rows");
	match &indexed_projected.strategy {
		L2FetchStrategy::RowGroup(plan) => {
			assert_eq!(
				plan.surviving_groups,
				vec![
					crate::schema::RowGroupIndex::new(2),
					crate::schema::RowGroupIndex::new(3)
				]
			);
			assert!(plan.phase_b_fetch.is_none());
		}
		other => panic!("expected coalesced row-group indexed fetch, got {other:?}"),
	}
	let indexed_values = indexed_projected.projected.batch.column(0);
	let indexed_values = indexed_values
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("indexed payload column");
	let whole_payload = whole.column(1);
	let whole_payload = whole_payload
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("whole payload column");
	for (position, location) in indexed_locations.iter().enumerate() {
		assert_eq!(
			indexed_values.value(position),
			whole_payload.value(location.record_row_index.get() as usize)
		);
	}

	let mut bytecode = Vec::new();
	bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_PUSH_INT);
	bytecode.extend_from_slice(&12_000_i64.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_GT);
	let multi_group_program = crate::schema::pbc::Program::compile(&bytecode).unwrap();
	let indexed_record_rows = indexed_locations
		.iter()
		.map(|location| location.record_row_index)
		.collect::<Vec<_>>();
	let multi_group_masked = mgr
		.read_projected_record_from_l2(
			l2.clone(),
			&pointer,
			location,
			&descriptor,
			&predicate_requirement,
			Some(&multi_group_program),
			Some(&indexed_record_rows),
		)
		.await
		.unwrap();
	match &multi_group_masked.strategy {
		L2FetchStrategy::RowGroup(plan) => {
			assert_eq!(
				plan.surviving_groups,
				vec![
					crate::schema::RowGroupIndex::new(2),
					crate::schema::RowGroupIndex::new(3)
				]
			);
			assert_eq!(plan.predicate_fetched_groups, plan.surviving_groups);
			assert!(plan.phase_b_fetch.is_some());
		}
		other => panic!("expected multi-group Phase B row-group strategy, got {other:?}"),
	}
	let masked_ids = multi_group_masked
		.projected
		.batch
		.column(
			multi_group_masked
				.projected
				.physical_for(0)
				.expect("id physical"),
		)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("masked id column");
	let masked_payloads = multi_group_masked
		.projected
		.batch
		.column(
			multi_group_masked
				.projected
				.physical_for(1)
				.expect("payload physical"),
		)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("masked payload column");
	let selected_whole = arrow::compute::concat_batches(
		&whole.project(&[0, 1]).unwrap().schema(),
		&[
			whole.project(&[0, 1]).unwrap().slice(8192, 4096),
			whole.project(&[0, 1]).unwrap().slice(12288, 4096),
		],
	)
	.unwrap();
	let expected_masked =
		crate::schema::pbc::execute_program(&multi_group_program, &selected_whole).unwrap();
	let expected_ids = expected_masked
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("expected masked ids");
	let expected_payloads = expected_masked
		.column(1)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("expected masked payloads");
	assert_eq!(masked_ids.values(), expected_ids.values());
	assert_eq!(masked_payloads.values(), expected_payloads.values());

	let projected = mgr
		.read_projected_record_from_l2(
			l2.clone(),
			&pointer,
			location,
			&descriptor,
			&requirement,
			None,
			None,
		)
		.await
		.unwrap();
	let expected = whole.project(&[1]).unwrap().slice(8192, 4096);

	assert!(matches!(projected.strategy, L2FetchStrategy::RowGroup(_)));
	assert_eq!(
		projected.projected_first_record_row_index,
		RecordRowIndex::new(8192)
	);
	assert_eq!(projected.projected.batch.num_rows(), expected.num_rows());
	let actual_values = projected.projected.batch.column(0);
	let actual_values = actual_values
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("actual payload column");
	let expected_values = expected.column(0);
	let expected_values = expected_values
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("expected payload column");
	assert_eq!(actual_values.values(), expected_values.values());
}

#[test]
fn fsst_phase_b_plan_fetches_values_and_offsets_and_declines_overlap() {
	let row_count = 512_usize;
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("url", DataType::Utf8, false),
		Field::new("payload", DataType::Int64, false),
	]);
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x66; 16]),
		1,
		"analytics",
		"fsst_phase_b_plan",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(row_count as u32, row_count as u32, 8 * 1024 * 1024)
				.unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let urls = (0..row_count)
		.map(|row| format!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/path/{row:04}"))
		.collect::<Vec<_>>();
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
			Arc::new(StringArray::from_iter_values(
				urls.iter().map(String::as_str),
			)) as Arc<dyn Array>,
			Arc::new(Int64Array::from_iter_values(
				(0..row_count).map(|row| (row % 17) as i64),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let source_rows = (0..row_count)
		.map(|source_row_index| {
			let mut key = [0_u8; 32];
			key[..8].copy_from_slice(&(source_row_index as u64).to_be_bytes());
			MultiRowSourceRow {
				source_row_index,
				key_identity: Identity::new(key),
				routing_locality: Locality::ZERO,
				row_hlc: hlc(660 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: Some(descriptor.table_object_id),
			}
		})
		.collect::<Vec<_>>();
	let row_group_config =
		crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
			RowGroupRowCount::new(256),
			RowGroupRowCount::new(256),
			RowCount::new(1),
			RecordBodyByteLen::new(1),
		)
		.with_enabled(true);
	let encoding_config = crate::storage::encoding::EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: SimdPageValueCount::new(1024),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: [
			crate::storage::encoding::EncodingCascadeKind::Plain,
			crate::storage::encoding::EncodingCascadeKind::FsstWholeBuffer,
		]
		.into_iter()
		.collect(),
		..Default::default()
	};
	let encoded =
			crate::storage::multirow::encode_multirow_from_survivors_with_row_group_and_encoding_config(
				&batch,
				&source_rows,
				&descriptor,
				RecordBodyCompressionPolicy::Uncompressed,
				&row_group_config,
				&encoding_config,
			)
			.unwrap();
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(66),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&descriptor,
	)
	.unwrap();
	let page_directory = directory.page_directory.as_ref().unwrap();
	let encoding_directory = directory.encoding_directory.as_ref().unwrap();
	let (_, value_buffer_ordinal) = encoding_compute_value_buffer(&directory, 1).unwrap();
	let value_encoding = encoding_directory
		.buffers
		.iter()
		.find(|encoding| {
			encoding.row_group_index == RowGroupIndex::ZERO
				&& encoding.ipc_buffer_ordinal == value_buffer_ordinal
		})
		.expect("FSST url value encoding");
	let cascade = encoding_directory
		.cascades
		.get(value_encoding.cascade_index.get() as usize)
		.expect("FSST cascade");
	assert!(
		matches!(cascade, EncodingCascade::FsstWholeBuffer { .. }),
		"{cascade:?}"
	);

	let target = urls[7].as_bytes();
	let mut bytecode = Vec::new();
	bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_PUSH_STR);
	bytecode.extend_from_slice(&(target.len() as u16).to_le_bytes());
	bytecode.extend_from_slice(target);
	bytecode.push(crate::schema::pbc::OP_EQ);
	let program = crate::schema::pbc::Program::compile(&bytecode).unwrap();
	let encoded_predicate = encoded_predicate_node_from_tree(&program.predicate_tree()).unwrap();
	let selected_groups = page_directory
		.row_groups
		.iter()
		.map(|group| group.row_group_index)
		.collect::<Vec<_>>();
	let mut compute_config = EncodingComputeConfig::default();
	compute_config
		.enabled_predicate_evaluators
		.insert(crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstWholeBuffer);
	let no_overlap = ColumnOrdinalSet::from_columns([2]);
	let plan = build_encoding_compute_phase_b_plan(
		&directory,
		page_directory,
		encoding_directory,
		&encoded_predicate,
		&no_overlap,
		&selected_groups,
		&selected_groups,
		&compute_config,
		0,
	)
	.expect("FSST no-overlap predicate should build encoded Phase B plan");
	assert_eq!(
		plan.fetches
			.iter()
			.filter(|fetch| matches!(
				fetch.kind,
				EncodingComputePhaseBFetchKind::FsstValues { .. }
			))
			.count(),
		selected_groups.len()
	);
	assert_eq!(
		plan.fetches
			.iter()
			.filter(|fetch| matches!(
				fetch.kind,
				EncodingComputePhaseBFetchKind::FsstOffsets { .. }
			))
			.count(),
		selected_groups.len()
	);

	assert_eq!(
		build_encoding_compute_phase_b_plan(
			&directory,
			page_directory,
			encoding_directory,
			&encoded_predicate,
			&no_overlap,
			&selected_groups,
			&selected_groups,
			&EncodingComputeConfig::default(),
			0,
		)
		.unwrap_err(),
		EncodingComputeFallbackReason::UnsupportedCascade
	);
	let overlap = ColumnOrdinalSet::from_columns([1, 2]);
	assert_eq!(
		build_encoding_compute_phase_b_plan(
			&directory,
			page_directory,
			encoding_directory,
			&encoded_predicate,
			&overlap,
			&selected_groups,
			&selected_groups,
			&compute_config,
			0,
		)
		.unwrap_err(),
		EncodingComputeFallbackReason::UnsupportedCascade
	);
}

#[test]
fn fsst_row_safe_phase_b_plan_uses_strategy_b_and_declines_overlap() {
	let row_count = 512_usize;
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("url", DataType::Utf8, false),
		Field::new("payload", DataType::Int64, false),
	]);
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x67; 16]),
		1,
		"analytics",
		"fsst_row_safe_phase_b_plan",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(row_count as u32, row_count as u32, 8 * 1024 * 1024)
				.unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let prefix = "a".repeat(120);
	let urls = (0..row_count)
		.map(|row| format!("{prefix}/path/{row:04}"))
		.collect::<Vec<_>>();
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
			Arc::new(StringArray::from_iter_values(
				urls.iter().map(String::as_str),
			)) as Arc<dyn Array>,
			Arc::new(Int64Array::from_iter_values(
				(0..row_count).map(|row| (row % 17) as i64),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let source_rows = (0..row_count)
		.map(|source_row_index| {
			let mut key = [0_u8; 32];
			key[..8].copy_from_slice(&(source_row_index as u64).to_be_bytes());
			MultiRowSourceRow {
				source_row_index,
				key_identity: Identity::new(key),
				routing_locality: Locality::ZERO,
				row_hlc: hlc(670 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: Some(descriptor.table_object_id),
			}
		})
		.collect::<Vec<_>>();
	let row_group_config =
		crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
			RowGroupRowCount::new(256),
			RowGroupRowCount::new(256),
			RowCount::new(1),
			RecordBodyByteLen::new(1),
		)
		.with_enabled(true);
	let encoding_config = crate::storage::encoding::EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: SimdPageValueCount::new(1024),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: [
			crate::storage::encoding::EncodingCascadeKind::Plain,
			crate::storage::encoding::EncodingCascadeKind::FsstWholeBuffer,
			crate::storage::encoding::EncodingCascadeKind::FsstRowSafe,
		]
		.into_iter()
		.collect(),
		..Default::default()
	};
	let encoded =
			crate::storage::multirow::encode_multirow_from_survivors_with_row_group_and_encoding_config(
				&batch,
				&source_rows,
				&descriptor,
				RecordBodyCompressionPolicy::Uncompressed,
				&row_group_config,
				&encoding_config,
			)
			.unwrap();
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(67),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&descriptor,
	)
	.unwrap();
	let page_directory = directory.page_directory.as_ref().unwrap();
	let encoding_directory = directory.encoding_directory.as_ref().unwrap();
	let (_, value_buffer_ordinal) = encoding_compute_value_buffer(&directory, 1).unwrap();
	let value_encoding = encoding_directory
		.buffers
		.iter()
		.find(|encoding| {
			encoding.row_group_index == RowGroupIndex::ZERO
				&& encoding.ipc_buffer_ordinal == value_buffer_ordinal
		})
		.expect("row-safe FSST url value encoding");
	let cascade = encoding_directory
		.cascades
		.get(value_encoding.cascade_index.get() as usize)
		.expect("row-safe FSST cascade");
	assert!(
		matches!(cascade, EncodingCascade::FsstRowSafe { .. }),
		"{cascade:?}"
	);

	let target = urls[7].as_bytes();
	let mut bytecode = Vec::new();
	bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_PUSH_STR);
	bytecode.extend_from_slice(&(target.len() as u16).to_le_bytes());
	bytecode.extend_from_slice(target);
	bytecode.push(crate::schema::pbc::OP_EQ);
	let program = crate::schema::pbc::Program::compile(&bytecode).unwrap();
	let encoded_predicate = encoded_predicate_node_from_tree(&program.predicate_tree()).unwrap();
	let selected_groups = page_directory
		.row_groups
		.iter()
		.map(|group| group.row_group_index)
		.collect::<Vec<_>>();
	// ADR 067 Phase 8 promotion: FsstRowSafe is now in
	// EncodingComputeConfig::default().enabled_predicate_evaluators.
	// Construct an explicit compute_config to remain test-self-contained
	// against future default changes.
	let mut compute_config = EncodingComputeConfig::default();
	compute_config
		.enabled_predicate_evaluators
		.insert(crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstRowSafe);
	let no_overlap = ColumnOrdinalSet::from_columns([2]);
	let plan = build_encoding_compute_phase_b_plan(
		&directory,
		page_directory,
		encoding_directory,
		&encoded_predicate,
		&no_overlap,
		&selected_groups,
		&selected_groups,
		&compute_config,
		0,
	)
	.expect("row-safe FSST no-overlap predicate should build encoded Phase B plan");
	assert_eq!(
		plan.fetches
			.iter()
			.filter(|fetch| matches!(
				fetch.kind,
				EncodingComputePhaseBFetchKind::FsstValues { .. }
			))
			.count(),
		selected_groups.len()
	);
	assert_eq!(
		plan.fetches
			.iter()
			.filter(|fetch| matches!(
				fetch.kind,
				EncodingComputePhaseBFetchKind::FsstOffsets { .. }
			))
			.count(),
		selected_groups.len()
	);
	// Default-config no-overlap plan: post-ADR-067-promotion, this now
	// succeeds (Sub-case A1 / Strategy B) and mirrors the explicit-config
	// plan above.
	let default_plan = build_encoding_compute_phase_b_plan(
		&directory,
		page_directory,
		encoding_directory,
		&encoded_predicate,
		&no_overlap,
		&selected_groups,
		&selected_groups,
		&EncodingComputeConfig::default(),
		0,
	)
	.expect("row-safe FSST default-config no-overlap predicate should build encoded Phase B plan");
	assert_eq!(default_plan.fetches.len(), plan.fetches.len());
	let overlap = ColumnOrdinalSet::from_columns([1, 2]);
	assert_eq!(
		build_encoding_compute_phase_b_plan(
			&directory,
			page_directory,
			encoding_directory,
			&encoded_predicate,
			&overlap,
			&selected_groups,
			&selected_groups,
			&compute_config,
			0,
		)
		.unwrap_err(),
		EncodingComputeFallbackReason::UnsupportedCascade
	);
}

#[tokio::test]
async fn fsst_phase_b_l2_read_filters_a1_and_declines_a2_overlap() {
	let l1_dir = fresh_dir("fsst-phase-b-l2-l1");
	let l2_dir = fresh_dir("fsst-phase-b-l2-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let mut encoding_compute = EncodingComputeConfig::default();
	encoding_compute
		.enabled_predicate_evaluators
		.insert(crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstWholeBuffer);
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024 * 16,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy {
				promote_after_l2_reads: 0,
				..RehydrationPolicy::default()
			},
			l2_column_fetch: L2ColumnFetchConfig {
				enabled: true,
				min_record_bytes: RecordBodyByteLen::new(1),
				..L2ColumnFetchConfig::default()
			},
			row_group_writer: Default::default(),
			l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig {
				enabled: true,
				max_selected_row_group_fraction_micros:
					crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
				max_selected_byte_fraction_micros:
					crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
				..Default::default()
			},
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: crate::storage::encoding::L2EncodingFetchConfig {
				enabled: true,
				..Default::default()
			},
			encoding_compute,
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);

	let row_count = 4_096_usize;
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("url", DataType::Utf8, false),
		Field::new("payload", DataType::Int64, false),
	]);
	let mut descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x66; 16]),
		2,
		"analytics",
		"fsst_phase_b_l2",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(1024, 1024, 8 * 1024 * 1024).unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Uncompressed;
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let urls = (0..row_count)
		.map(|row| format!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/path/{row:04}"))
		.collect::<Vec<_>>();
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
			Arc::new(StringArray::from_iter_values(
				urls.iter().map(String::as_str),
			)) as Arc<dyn Array>,
			Arc::new(Int64Array::from_iter_values(
				(0..row_count).map(|row| 900_000_i64 + row as i64),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let source_rows = (0..row_count)
		.map(|source_row_index| {
			let mut key = [0_u8; 32];
			key[..8].copy_from_slice(&(source_row_index as u64).to_be_bytes());
			MultiRowSourceRow {
				source_row_index,
				key_identity: Identity::new(key),
				routing_locality: Locality::ZERO,
				row_hlc: hlc(700 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: Some(descriptor.table_object_id),
			}
		})
		.collect::<Vec<_>>();
	let mut row_group_config =
		crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
			RowGroupRowCount::new(1024),
			RowGroupRowCount::new(1024),
			RowCount::new(1),
			RecordBodyByteLen::new(1),
		)
		.with_enabled(true);
	row_group_config.stats_supported_types = std::collections::BTreeSet::new();
	let encoding_config = crate::storage::encoding::EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: SimdPageValueCount::new(1024),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: [
			crate::storage::encoding::EncodingCascadeKind::Plain,
			crate::storage::encoding::EncodingCascadeKind::FsstWholeBuffer,
		]
		.into_iter()
		.collect(),
		..Default::default()
	};
	let encoded =
			crate::storage::multirow::encode_multirow_from_survivors_with_row_group_and_encoding_config(
				&batch,
				&source_rows,
				&descriptor,
				RecordBodyCompressionPolicy::Uncompressed,
				&row_group_config,
				&encoding_config,
			)
			.unwrap();
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(66),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&descriptor,
	)
	.unwrap();
	let encoding_directory = directory.encoding_directory.as_ref().unwrap();
	let (_, url_value_buffer_ordinal) = encoding_compute_value_buffer(&directory, 1).unwrap();
	let value_encoding = encoding_directory
		.buffers
		.iter()
		.find(|encoding| encoding.ipc_buffer_ordinal == url_value_buffer_ordinal)
		.expect("FSST url value encoding");
	let cascade = encoding_directory
		.cascades
		.get(value_encoding.cascade_index.get() as usize)
		.expect("FSST url value cascade");
	assert!(
		matches!(cascade, EncodingCascade::FsstWholeBuffer { .. }),
		"{cascade:?}"
	);

	let encoded_entries = encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let payload_hash = crate::hash::content::digest(&encrypted.bytes);
	let uri = l2
		.put(
			&format!("fsst-phase-b-l2/slab_{slab_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	let pointer = TieredSlabPointer {
		slab_id,
		l2_uri: uri,
		payload_hash,
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(pointer.clone()).await.unwrap();

	let target_row = 333_usize;
	let target = encoded
		.rows
		.iter()
		.find(|row| row.record_row_index == RecordRowIndex::new(target_row as u32))
		.expect("target storage row");
	let location = mgr
		.lookup(tenant(), target.key_identity)
		.expect("tiered FSST row location");
	let selected_rows = [RecordRowIndex::new(target_row as u32)];
	let mut bytecode = Vec::new();
	bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_PUSH_STR);
	bytecode.extend_from_slice(&(urls[target_row].len() as u16).to_le_bytes());
	bytecode.extend_from_slice(urls[target_row].as_bytes());
	bytecode.push(crate::schema::pbc::OP_EQ);
	let predicate = crate::schema::pbc::Program::compile(&bytecode).unwrap();

	let a1_requirement = ColumnFetchRequirement {
		predicate_columns: ColumnOrdinalSet::from_columns([1]),
		output_columns: ColumnOrdinalSet::from_columns([2]),
		..ColumnFetchRequirement::default()
	};
	let a1 = mgr
		.read_projected_record_from_l2(
			l2.clone(),
			&pointer,
			location,
			&descriptor,
			&a1_requirement,
			Some(&predicate),
			Some(&selected_rows),
		)
		.await
		.unwrap();
	let L2FetchStrategy::RowGroup(a1_plan) = &a1.strategy else {
		panic!("expected FSST A1 row-group strategy, got {:?}", a1.strategy);
	};
	assert!(a1_plan.phase_b_fetch.is_some());
	assert!(
		a1.projected.physical_for(1).is_none(),
		"FSST A1 must not carry the predicate column into Phase C projection"
	);
	let a1_expected = crate::schema::pbc::execute_program(&predicate, &batch)
		.unwrap()
		.project(&[2])
		.unwrap();
	let a1_payload_physical = a1.projected.physical_for(2).expect("payload physical");
	assert_eq!(
		a1.projected.batch.project(&[a1_payload_physical]).unwrap(),
		a1_expected
	);

	let a2_requirement = ColumnFetchRequirement {
		predicate_columns: ColumnOrdinalSet::from_columns([1]),
		output_columns: ColumnOrdinalSet::from_columns([1, 2]),
		..ColumnFetchRequirement::default()
	};
	let a2 = mgr
		.read_projected_record_from_l2(
			l2,
			&pointer,
			location,
			&descriptor,
			&a2_requirement,
			Some(&predicate),
			Some(&selected_rows),
		)
		.await
		.unwrap();
	let L2FetchStrategy::RowGroup(a2_plan) = &a2.strategy else {
		panic!(
			"expected FSST A2 dense row-group strategy, got {:?}",
			a2.strategy
		);
	};
	assert!(a2_plan.phase_b_fetch.is_some());
	let a2_expected = crate::schema::pbc::execute_program(&predicate, &batch)
		.unwrap()
		.project(&[1, 2])
		.unwrap();
	let a2_url_physical = a2.projected.physical_for(1).expect("url physical");
	let a2_payload_physical = a2.projected.physical_for(2).expect("payload physical");
	assert_eq!(
		a2.projected
			.batch
			.project(&[a2_url_physical, a2_payload_physical])
			.unwrap(),
		a2_expected
	);
}

#[test]
fn encoding_predicate_page_plan_narrows_clustered_scan_pages() {
	let row_count = crate::storage::row_group::ROW_GROUP_SCAN_TARGET_ROWS as usize;
	let column_count = 17_usize;
	let schema = Schema::new(
		(0..column_count)
			.map(|column| Field::new(format!("c{column}"), DataType::UInt8, false))
			.collect::<Vec<_>>(),
	);
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x63; 16]),
		1,
		"analytics",
		"adr063_page_plan",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("c0")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(
				crate::storage::row_group::ROW_GROUP_SCAN_TARGET_ROWS,
				crate::storage::row_group::ROW_GROUP_SCAN_TARGET_ROWS,
				8 * 1024 * 1024,
			)
			.unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let predicate_values = (0..row_count)
		.map(|row| {
			let page = row / 1_024;
			if page == 0 {
				0
			} else if page == 1 {
				1
			} else if page == 2 {
				if row % 2 == 0 { 1 } else { 3 }
			} else {
				3
			}
		})
		.collect::<Vec<_>>();
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		(0..column_count)
			.map(|column| {
				if column == 0 {
					Arc::new(UInt8Array::from_iter_values(
						predicate_values.iter().copied(),
					)) as Arc<dyn Array>
				} else {
					Arc::new(UInt8Array::from_iter_values(
						(0..row_count).map(|row| ((row + column) % 4) as u8),
					)) as Arc<dyn Array>
				}
			})
			.collect::<Vec<_>>(),
	)
	.unwrap();
	let source_rows = (0..row_count)
		.map(|source_row_index| {
			let mut key = [0_u8; 32];
			key[..8].copy_from_slice(&(source_row_index as u64).to_be_bytes());
			MultiRowSourceRow {
				source_row_index,
				key_identity: Identity::new(key),
				routing_locality: Locality::ZERO,
				row_hlc: hlc(63),
				is_tombstone: false,
				partition: None,
				table_object_id: Some(descriptor.table_object_id),
			}
		})
		.collect::<Vec<_>>();
	let row_group_config =
		crate::storage::row_group::RowGroupWriterConfig::scan().with_enabled(true);
	let encoding_config = EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: crate::schema::SimdPageValueCount::new(1_024),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: [
			crate::storage::EncodingCascadeKind::Plain,
			crate::storage::EncodingCascadeKind::Dictionary,
		]
		.into_iter()
		.collect(),
		..EncodingWriterConfig::default()
	};
	let encoded =
			crate::storage::multirow::encode_multirow_from_survivors_with_row_group_and_encoding_config(
				&batch,
				&source_rows,
				&descriptor,
				RecordBodyCompressionPolicy::Uncompressed,
				&row_group_config,
				&encoding_config,
			)
			.unwrap();
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(63),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&descriptor,
	)
	.unwrap();
	let page_directory = directory.page_directory.as_ref().unwrap();
	let encoding_directory = directory.encoding_directory.as_ref().unwrap();
	let mut bytecode = Vec::new();
	bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_PUSH_INT);
	bytecode.extend_from_slice(&2_i64.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_LT);
	let program = crate::schema::pbc::Program::compile(&bytecode).unwrap();
	let encoded_predicate = encoded_predicate_node_from_tree(&program.predicate_tree())
		.expect("clustered predicate should be page-plannable");
	let predicate_atom = PredicateAtom {
		op: crate::storage::encoding::compute::PredicateAtomOp::Lt,
		literal: Some(crate::schema::pbc::ColumnPredicateLiteral::Int(2)),
	};
	let (physical_ordinal, value_buffer_ordinal) =
		encoding_compute_value_buffer(&directory, 0).unwrap();
	let value_page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == RowGroupIndex::ZERO
				&& page.ipc_column_ordinal == IpcColumnOrdinal::new(physical_ordinal as u32)
				&& page.ipc_buffer_ordinal == value_buffer_ordinal
				&& page.range.len.get() > 0
		})
		.expect("predicate value page");
	let value_encoding = find_buffer_encoding(
		encoding_directory,
		RowGroupIndex::ZERO,
		value_buffer_ordinal,
		value_page.range,
	)
	.expect("predicate value encoding");
	let cascade = encoding_directory
		.cascades
		.get(value_encoding.cascade_index.get() as usize)
		.expect("predicate cascade");
	assert_eq!(value_encoding.page_stats.len(), 64);
	assert!(page_stats_cascade_eligible(cascade), "{cascade:?}");
	let coercion = coerce_literal(predicate_atom.literal.as_ref(), &DataType::UInt8).unwrap();
	assert!(
		matches!(
			native_evaluation_eligibility(cascade, &predicate_atom, &coercion),
			NativeEvaluationEligibility::Eligible(_)
		),
		"{cascade:?}"
	);
	let selected_groups = page_directory
		.row_groups
		.iter()
		.map(|group| group.row_group_index)
		.collect::<Vec<_>>();
	let compute_config = EncodingComputeConfig::default();
	let output_columns = ColumnOrdinalSet::new();
	let page_plan = build_encoding_predicate_page_plan(
		&directory,
		page_directory,
		encoding_directory,
		&encoded_predicate,
		&output_columns,
		&selected_groups,
		&selected_groups,
		&compute_config,
		0,
	)
	.expect("clustered predicate should build a page-level plan");
	let dense_plan = build_encoding_compute_phase_b_plan(
		&directory,
		page_directory,
		encoding_directory,
		&encoded_predicate,
		&output_columns,
		&selected_groups,
		&selected_groups,
		&compute_config,
		0,
	)
	.expect("clustered predicate should also build a dense plan");
	let page_bytes = page_plan
		.record_body_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	let dense_bytes = dense_plan
		.record_body_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	let disjoint_pages = page_plan
		.decisions
		.iter()
		.filter(|decision| {
			matches!(
				decision.classification,
				EncodingPredicatePageDecisionKind::Disjoint
			)
		})
		.count();
	let exact_pages = page_plan
		.decisions
		.iter()
		.filter(|decision| {
			matches!(
				decision.classification,
				EncodingPredicatePageDecisionKind::ExactMatch
			)
		})
		.count();
	let maybe_pages = page_plan
		.decisions
		.iter()
		.filter(|decision| {
			matches!(
				decision.classification,
				EncodingPredicatePageDecisionKind::Maybe { .. }
			)
		})
		.count();

	assert_eq!(page_plan.fetches.len(), 1);
	assert_eq!((disjoint_pages, exact_pages, maybe_pages), (61, 2, 1));
	assert!(
		page_bytes * 10 < dense_bytes,
		"page plan bytes {page_bytes} should be materially smaller than dense bytes {dense_bytes}"
	);
}

#[tokio::test]
async fn sparse_projection_fetch_materializes_exact_cocd_clustered_pages() {
	let l1_dir = fresh_dir("sparse-projection-l1");
	let l2_dir = fresh_dir("sparse-projection-l2");
	let l2_counter = Arc::new(CountingObjectStore::new(Arc::new(
		LocalFsObjectStore::new(&*l2_dir).unwrap(),
	)));
	let l2: Arc<dyn ObjectStore> = l2_counter.clone();
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024 * 16,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy {
				promote_after_l2_reads: 0,
				..RehydrationPolicy::default()
			},
			l2_column_fetch: L2ColumnFetchConfig {
				enabled: true,
				min_record_bytes: RecordBodyByteLen::new(1),
				..L2ColumnFetchConfig::default()
			},
			row_group_writer: Default::default(),
			l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig {
				enabled: true,
				max_selected_row_group_fraction_micros:
					crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
				max_selected_byte_fraction_micros:
					crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
				..Default::default()
			},
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: crate::storage::encoding::L2EncodingFetchConfig {
				enabled: true,
				..Default::default()
			},
			encoding_compute: crate::storage::encoding::compute::EncodingComputeConfig {
				min_selectivity_for_encoded_eval_micros:
					crate::schema::SelectivityFractionMicros::new(1_000_000),
				..Default::default()
			},
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("selector", DataType::Int64, false),
		Field::new("payload", DataType::Int64, true),
		Field::new("tag", DataType::Utf8, false),
	]);
	let mut descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x84; 16]),
		1,
		"analytics",
		"sparse_projection",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(4096, 4096, 8 * 1024 * 1024).unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Uncompressed;
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let row_count = 4096_usize;
	let selector = (0..row_count).map(|row| row as i64).collect::<Vec<_>>();
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
			Arc::new(Int64Array::from_iter_values(selector.iter().copied())) as Arc<dyn Array>,
			Arc::new(Int64Array::from_iter(
				(0..row_count).map(|row| (row % 11 != 0).then_some((row % 128) as i64)),
			)) as Arc<dyn Array>,
			Arc::new(StringArray::from_iter_values(
				(0..row_count).map(|row| if row < 256 { "hit" } else { "miss" }),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let source_rows = (0..row_count)
		.map(|source_row_index| {
			let mut key = [0_u8; 32];
			key[..8].copy_from_slice(&(source_row_index as u64).to_be_bytes());
			MultiRowSourceRow {
				source_row_index,
				key_identity: Identity::new(key),
				routing_locality: Locality::ZERO,
				row_hlc: hlc(600 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: None,
			}
		})
		.collect::<Vec<_>>();
	let mut row_group_config =
		crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
			crate::schema::RowGroupRowCount::new(1024),
			crate::schema::RowGroupRowCount::new(1024),
			RowCount::new(1024),
			RecordBodyByteLen::new(1),
		)
		.with_enabled(true);
	row_group_config.stats_supported_types = std::collections::BTreeSet::new();
	let encoding_config = crate::storage::encoding::EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: crate::schema::SimdPageValueCount::new(256),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		..Default::default()
	};
	let encoded =
			crate::storage::multirow::encode_multirow_from_survivors_with_row_group_and_encoding_config(
				&batch,
				&source_rows,
				&descriptor,
				RecordBodyCompressionPolicy::Uncompressed,
				&row_group_config,
				&encoding_config,
			)
			.unwrap();
	let encoded_entries = encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let payload_hash = crate::hash::content::digest(&encrypted.bytes);
	let uri = l2
		.put(
			&format!("sparse-projection/slab_{slab_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	let pointer = TieredSlabPointer {
		slab_id,
		l2_uri: uri,
		payload_hash,
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(pointer.clone()).await.unwrap();
	let target = encoded
		.rows
		.iter()
		.find(|row| row.record_row_index == RecordRowIndex::new(300))
		.expect("target storage row");
	let location = mgr
		.lookup(tenant(), target.key_identity)
		.expect("tiered row location");
	// Sparse projection fetch consumes the exact COCD mask but
	// does not page-slice dense predicate columns.
	let requirement = ColumnFetchRequirement {
		output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([2]),
		..ColumnFetchRequirement::default()
	};
	let mut bytecode = Vec::new();
	bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_PUSH_INT);
	bytecode.extend_from_slice(&256_i64.to_le_bytes());
	bytecode.push(crate::schema::pbc::OP_LT);
	let predicate = crate::schema::pbc::Program::compile(&bytecode).unwrap();
	let selected_rows = [RecordRowIndex::new(300)];

	let counter_before_sparse = l2_counter.snapshot();
	let sparse = mgr
		.read_projected_record_from_l2(
			l2.clone(),
			&pointer,
			location,
			&descriptor,
			&requirement,
			Some(&predicate),
			Some(&selected_rows),
		)
		.await
		.unwrap();
	let counter_after_sparse = l2_counter.snapshot();
	assert_eq!(counter_after_sparse.get_bytes_calls, 0);
	assert_eq!(counter_after_sparse.get_bytes_bytes, 0);
	assert!(
		counter_after_sparse.get_range_calls > counter_before_sparse.get_range_calls,
		"sparse projection read must use L2 range GETs"
	);
	let sparse_get_range_bytes = counter_after_sparse
		.get_range_bytes
		.saturating_sub(counter_before_sparse.get_range_bytes);
	let L2FetchStrategy::RowGroup(plan) = &sparse.strategy else {
		panic!(
			"expected row-group sparse projection strategy, got {:?}",
			sparse.strategy
		);
	};
	assert_eq!(
		plan.surviving_groups,
		vec![crate::schema::RowGroupIndex::ZERO]
	);
	assert_eq!(plan.predicate_fetched_groups, plan.surviving_groups);
	assert!(plan.phase_b_fetch.is_some());
	assert!(
		sparse_get_range_bytes >= plan.phase_c_fetch.fetched_ciphertext_bytes,
		"runtime range GET counter should cover sparse projection ciphertext bytes"
	);

	let full_frame = mgr
		.read_encrypted_record_from_l2(l2.as_ref(), &pointer, location)
		.await
		.unwrap();
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id,
			record_offset: RecordBodyByteOffset::new(location.offset),
		},
		PlaintextByteRange {
			start: 0,
			len: full_frame.len() as u64,
		},
		&full_frame,
		&descriptor,
	)
	.unwrap();
	let page_directory = directory.page_directory.as_ref().unwrap();
	let encoding_directory = directory.encoding_directory.as_ref().unwrap();
	let payload_encoding = encoding_directory
		.buffers
		.iter()
		.find(|encoding| {
			encoding.row_group_index == crate::schema::RowGroupIndex::ZERO
				&& encoding.ipc_column_ordinal == crate::schema::IpcColumnOrdinal::new(2)
				&& encoding.page_end_offsets.len() > 1
				&& encoding.encoded_range.len < encoding.decoded_byte_len
		})
		.expect("payload value buffer should be page-encoded");
	let payload_null_ordinal = directory.column_to_buffers[2].first_buffer_ordinal;
	let payload_null_page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == payload_encoding.row_group_index
				&& page.ipc_column_ordinal == payload_encoding.ipc_column_ordinal
				&& page.ipc_buffer_ordinal == payload_null_ordinal
				&& page.range.len.get() > 0
		})
		.expect("payload validity buffer page");
	assert!(
		body_ranges_overlap(
			&plan.phase_c_fetch.selected_plaintext_ranges,
			payload_null_page.range
		),
		"sparse projection should fetch the projected nullable column validity bitmap"
	);
	let payload_page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == payload_encoding.row_group_index
				&& page.ipc_column_ordinal == payload_encoding.ipc_column_ordinal
				&& page.ipc_buffer_ordinal == payload_encoding.ipc_buffer_ordinal
		})
		.unwrap();
	let full_payload_range = crate::storage::row_group::RecordBodyByteRange {
		start: RecordBodyByteOffset::new(
			payload_page.range.start.get() + u64::from(payload_encoding.encoded_range.start.get()),
		),
		len: payload_encoding.encoded_range.len,
	};
	assert!(
		!body_ranges_cover(
			&plan.phase_c_fetch.selected_plaintext_ranges,
			full_payload_range
		),
		"sparse projection should not fetch the whole projected payload buffer"
	);
	let selected_projection_bytes = plan
		.phase_c_fetch
		.selected_plaintext_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	assert!(
		selected_projection_bytes < full_payload_range.len.get(),
		"sparse projection bytes {selected_projection_bytes} should stay below full payload range {}",
		full_payload_range.len.get()
	);

	let expected = crate::schema::pbc::execute_program(&predicate, &batch)
		.unwrap()
		.project(&[2])
		.unwrap();
	assert_eq!(sparse.projected.batch, expected);

	let mut all_bytecode = Vec::new();
	all_bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	all_bytecode.extend_from_slice(&1_u32.to_le_bytes());
	all_bytecode.push(crate::schema::pbc::OP_PUSH_INT);
	all_bytecode.extend_from_slice(&(row_count as i64).to_le_bytes());
	all_bytecode.push(crate::schema::pbc::OP_LT);
	let all_predicate = crate::schema::pbc::Program::compile(&all_bytecode).unwrap();
	let all = mgr
		.read_projected_record_from_l2(
			l2.clone(),
			&pointer,
			location,
			&descriptor,
			&requirement,
			Some(&all_predicate),
			Some(&selected_rows),
		)
		.await
		.unwrap();
	let L2FetchStrategy::RowGroup(all_plan) = &all.strategy else {
		panic!(
			"expected row-group dense projection strategy for all mask, got {:?}",
			all.strategy
		);
	};
	assert!(all_plan.phase_b_fetch.is_some());
	assert_eq!(
		all_plan.surviving_groups,
		vec![crate::schema::RowGroupIndex::ZERO]
	);
	assert!(
		body_ranges_cover(
			&all_plan.phase_c_fetch.selected_plaintext_ranges,
			full_payload_range
		),
		"all-mask sparse decline should route to the dense projection range"
	);
	let all_expected = batch.slice(0, row_group_config.target_row_group_rows().get() as usize);
	let all_expected = crate::schema::pbc::execute_program(&all_predicate, &all_expected)
		.unwrap()
		.project(&[2])
		.unwrap();
	assert_eq!(all.projected.batch, all_expected);

	let mut none_bytecode = Vec::new();
	none_bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	none_bytecode.extend_from_slice(&1_u32.to_le_bytes());
	none_bytecode.push(crate::schema::pbc::OP_PUSH_INT);
	none_bytecode.extend_from_slice(&0_i64.to_le_bytes());
	none_bytecode.push(crate::schema::pbc::OP_LT);
	let none_predicate = crate::schema::pbc::Program::compile(&none_bytecode).unwrap();
	let none = mgr
		.read_projected_record_from_l2(
			l2,
			&pointer,
			location,
			&descriptor,
			&requirement,
			Some(&none_predicate),
			Some(&selected_rows),
		)
		.await
		.unwrap();
	let L2FetchStrategy::RowGroup(none_plan) = &none.strategy else {
		panic!(
			"expected row-group sparse projection strategy for none mask, got {:?}",
			none.strategy
		);
	};
	assert!(none_plan.phase_b_fetch.is_some());
	assert!(none_plan.surviving_groups.is_empty());
	assert!(none_plan.phase_c_fetch.selected_plaintext_ranges.is_empty());
	let none_expected = crate::schema::pbc::execute_program(&none_predicate, &batch)
		.unwrap()
		.project(&[2])
		.unwrap();
	assert_eq!(none.projected.batch, none_expected);

	let scalar_l1_dir = fresh_dir("sparse-projection-scalar-l1");
	let scalar_l2_dir = fresh_dir("sparse-projection-scalar-l2");
	let scalar_l2_counter = Arc::new(CountingObjectStore::new(Arc::new(
		LocalFsObjectStore::new(&*scalar_l2_dir).unwrap(),
	)));
	let scalar_l2: Arc<dyn ObjectStore> = scalar_l2_counter.clone();
	let scalar_mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: scalar_l1_dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024 * 16,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(scalar_l2.clone()),
			rehydration: RehydrationPolicy {
				promote_after_l2_reads: 0,
				..RehydrationPolicy::default()
			},
			l2_column_fetch: L2ColumnFetchConfig {
				enabled: true,
				min_record_bytes: RecordBodyByteLen::new(1),
				..L2ColumnFetchConfig::default()
			},
			row_group_writer: Default::default(),
			l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig {
				enabled: true,
				max_selected_row_group_fraction_micros:
					crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
				max_selected_byte_fraction_micros:
					crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
				..Default::default()
			},
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: crate::storage::encoding::L2EncodingFetchConfig {
				enabled: true,
				..Default::default()
			},
			encoding_compute: crate::storage::encoding::compute::EncodingComputeConfig {
				enabled: false,
				..Default::default()
			},
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	scalar_mgr
		.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	let scalar_slab_id = force_seal_active(&scalar_mgr).await;
	let scalar_raw = scalar_mgr.read_sealed_raw(scalar_slab_id).await.unwrap();
	let scalar_encrypted = scalar_mgr
		.encrypt_tiered_object_blob(scalar_slab_id, &scalar_raw)
		.unwrap();
	let scalar_payload_hash = crate::hash::content::digest(&scalar_encrypted.bytes);
	let scalar_uri = scalar_l2
		.put(
			&format!("sparse-projection-scalar/slab_{scalar_slab_id:016x}.wnd"),
			&scalar_encrypted.bytes,
		)
		.await
		.unwrap();
	let scalar_pointer = TieredSlabPointer {
		slab_id: scalar_slab_id,
		l2_uri: scalar_uri,
		payload_hash: scalar_payload_hash,
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(scalar_encrypted.tenant_kek_id),
		stats_format_version: scalar_encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: scalar_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: scalar_encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	scalar_mgr
		.apply_tier_slab(scalar_pointer.clone())
		.await
		.unwrap();
	let scalar_target = encoded
		.rows
		.iter()
		.find(|row| row.record_row_index == RecordRowIndex::new(300))
		.expect("scalar target storage row");
	let scalar_location = scalar_mgr
		.lookup(tenant(), scalar_target.key_identity)
		.expect("scalar tiered row location");
	let no_overlap_requirement = ColumnFetchRequirement {
		predicate_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([1]),
		output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([2]),
		..ColumnFetchRequirement::default()
	};
	let no_overlap_before = scalar_l2_counter.snapshot();
	let no_overlap = scalar_mgr
		.read_projected_record_from_l2(
			scalar_l2.clone(),
			&scalar_pointer,
			scalar_location,
			&descriptor,
			&no_overlap_requirement,
			Some(&predicate),
			Some(&selected_rows),
		)
		.await
		.unwrap();
	let no_overlap_after = scalar_l2_counter.snapshot();
	let L2FetchStrategy::RowGroup(no_overlap_plan) = &no_overlap.strategy else {
		panic!(
			"expected scalar no-overlap row-group strategy, got {:?}",
			no_overlap.strategy
		);
	};
	assert!(no_overlap_plan.phase_b_fetch.is_some());
	assert!(
		!body_ranges_cover(
			&no_overlap_plan.phase_c_fetch.selected_plaintext_ranges,
			full_payload_range
		),
		"no-overlap scalar predicate decode should keep projection-side sparse ranges"
	);
	assert!(
		no_overlap_after.get_range_calls > no_overlap_before.get_range_calls,
		"no-overlap scalar predicate decode should fetch L2 ranges"
	);
	let no_overlap_expected = crate::schema::pbc::execute_program(&predicate, &batch)
		.unwrap()
		.project(&[2])
		.unwrap();
	assert_eq!(no_overlap.projected.batch, no_overlap_expected);

	let overlap_requirement = ColumnFetchRequirement {
		predicate_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([1]),
		output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([1, 2]),
		..ColumnFetchRequirement::default()
	};
	let overlap_zip = scalar_mgr
		.read_projected_record_from_l2(
			scalar_l2.clone(),
			&scalar_pointer,
			scalar_location,
			&descriptor,
			&overlap_requirement,
			Some(&predicate),
			Some(&selected_rows),
		)
		.await
		.unwrap();
	let L2FetchStrategy::RowGroup(overlap_plan) = &overlap_zip.strategy else {
		panic!(
			"expected scalar overlap row-group strategy, got {:?}",
			overlap_zip.strategy
		);
	};
	assert!(overlap_plan.phase_b_fetch.is_some());
	assert!(
		!body_ranges_cover(
			&overlap_plan.phase_c_fetch.selected_plaintext_ranges,
			full_payload_range
		),
		"overlap scalar predicate decode should zip dense predicate pages with sparse projection pages"
	);
	let overlap_expected = crate::schema::pbc::execute_program(&predicate, &batch)
		.unwrap()
		.project(&[1, 2])
		.unwrap();
	assert_eq!(overlap_zip.projected.batch, overlap_expected);

	let mut string_bytecode = Vec::new();
	string_bytecode.push(crate::schema::pbc::OP_PUSH_COL);
	string_bytecode.extend_from_slice(&3_u32.to_le_bytes());
	string_bytecode.push(crate::schema::pbc::OP_PUSH_STR);
	string_bytecode.extend_from_slice(&3_u16.to_le_bytes());
	string_bytecode.extend_from_slice(b"hit");
	string_bytecode.push(crate::schema::pbc::OP_EQ);
	let string_predicate = crate::schema::pbc::Program::compile(&string_bytecode).unwrap();
	let variable_width_overlap_requirement = ColumnFetchRequirement {
		predicate_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([3]),
		output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([2, 3]),
		..ColumnFetchRequirement::default()
	};
	let variable_width_overlap = scalar_mgr
		.read_projected_record_from_l2(
			scalar_l2,
			&scalar_pointer,
			scalar_location,
			&descriptor,
			&variable_width_overlap_requirement,
			Some(&string_predicate),
			Some(&selected_rows),
		)
		.await
		.unwrap();
	let L2FetchStrategy::RowGroup(variable_width_plan) = &variable_width_overlap.strategy else {
		panic!(
			"expected variable-width scalar overlap row-group strategy, got {:?}",
			variable_width_overlap.strategy
		);
	};
	assert!(variable_width_plan.phase_b_fetch.is_some());
	assert!(
		!body_ranges_cover(
			&variable_width_plan.phase_c_fetch.selected_plaintext_ranges,
			full_payload_range
		),
		"variable-width scalar predicate overlap should keep projection-side sparse ranges"
	);
	let variable_width_expected = crate::schema::pbc::execute_program(&string_predicate, &batch)
		.unwrap()
		.project(&[2, 3])
		.unwrap();
	assert_eq!(
		variable_width_overlap.projected.batch,
		variable_width_expected
	);
}

#[tokio::test]
async fn adr061_enabled_chunk_layout_writes_16k_encrypted_slab() {
	let dir = fresh_dir("adr061-chunk-layout");
	let chunk_layout_config = crate::storage::encoding::L2ChunkLayoutConfig {
		enabled: true,
		..Default::default()
	};
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024 * 16,
			io: spawn_io_thread(64).unwrap(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: L2ColumnFetchConfig::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: chunk_layout_config.clone(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("payload", DataType::UInt8, false),
	]);
	let mut descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x61; 16]),
		1,
		"analytics",
		"adr061_chunk_layout",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(4096, 4096, 8 * 1024 * 1024).unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Uncompressed;
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let row_count = 4096_usize;
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
			Arc::new(UInt8Array::from_iter_values(
				(0..row_count).map(|row| row as u8),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let source_rows = (0..row_count)
		.map(|source_row_index| {
			let mut key = [0_u8; 32];
			key[..8].copy_from_slice(&(source_row_index as u64).to_be_bytes());
			MultiRowSourceRow {
				source_row_index,
				key_identity: Identity::new(key),
				routing_locality: Locality::ZERO,
				row_hlc: hlc(610 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: None,
			}
		})
		.collect::<Vec<_>>();
	let mut row_group_config =
		crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
			crate::schema::RowGroupRowCount::new(1024),
			crate::schema::RowGroupRowCount::new(1024),
			RowCount::new(1024),
			RecordBodyByteLen::new(1),
		)
		.with_enabled(true);
	row_group_config.stats_supported_types = std::collections::BTreeSet::new();
	let encoding_config = crate::storage::encoding::EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: crate::schema::SimdPageValueCount::new(256),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		..Default::default()
	};
	let encoded = crate::storage::multirow::encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config(
			&batch,
			&source_rows,
			&descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&row_group_config,
			&encoding_config,
			&chunk_layout_config,
		)
		.unwrap();
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::ZERO,
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&descriptor,
	)
	.unwrap();
	let page_directory = parsed.page_directory.as_ref().unwrap();
	let encoding_directory = parsed.encoding_directory.as_ref().unwrap();
	for encoding in &encoding_directory.buffers {
		if !crate::storage::encoding::chunk_layout::default_sparse_engaged_key(
			encoding_directory,
			encoding.row_group_index,
			encoding.ipc_column_ordinal,
			encoding.ipc_buffer_ordinal,
		) {
			continue;
		}
		let page = page_directory
			.buffer_pages
			.iter()
			.find(|page| {
				page.row_group_index == encoding.row_group_index
					&& page.ipc_column_ordinal == encoding.ipc_column_ordinal
					&& page.ipc_buffer_ordinal == encoding.ipc_buffer_ordinal
			})
			.unwrap();
		let physical_start = page.range.start.get() + u64::from(encoding.encoded_range.start.get());
		assert_eq!(
			physical_start % u64::from(16 * 1024_u32),
			0,
			"default-engaged encoded buffer should start on the selected chunk boundary"
		);
	}
	let encoded_entries = encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	assert_eq!(
		encrypted.chunk_size_bytes,
		crate::schema::ChunkSizeBytes::new(16 * 1024)
	);
	let footer = mgr
		.encrypted_tiered_footer_summary(&encrypted.bytes)
		.unwrap()
		.expect("encrypted footer");
	assert_eq!(footer.chunk_size_bytes, encrypted.chunk_size_bytes);
	let decrypted = mgr
		.config
		.crypto
		.decrypt_tiered_slab(slab_id, &encrypted.bytes)
		.unwrap();
	assert_eq!(decrypted.plaintext, raw);
}

#[tokio::test]
async fn encoding_secondary_index_fetches_only_selected_simd_pages() {
	let l1_dir = fresh_dir("encoding-index-l1");
	let l2_dir = fresh_dir("encoding-index-l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let mgr = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024 * 16,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy {
				promote_after_l2_reads: 0,
				..RehydrationPolicy::default()
			},
			l2_column_fetch: L2ColumnFetchConfig {
				enabled: true,
				min_record_bytes: RecordBodyByteLen::new(1),
				..L2ColumnFetchConfig::default()
			},
			row_group_writer: Default::default(),
			l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig {
				enabled: true,
				max_selected_row_group_fraction_micros:
					crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
				..Default::default()
			},
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: crate::storage::encoding::L2EncodingFetchConfig {
				enabled: true,
				..Default::default()
			},
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("payload", DataType::UInt8, false),
	]);
	let mut descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x83; 16]),
		1,
		"analytics",
		"encoding_index",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(4096, 4096, 8 * 1024 * 1024).unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Uncompressed;
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let row_count = 4096_usize;
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
			Arc::new(UInt8Array::from_iter_values((0..row_count).map(|_| 7_u8))) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let meta = layout_recluster_meta(&descriptor, batch.num_rows());
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let source_rows = admission
		.key_identities
		.iter()
		.zip(admission.localities.iter())
		.enumerate()
		.map(
			|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
				source_row_index,
				key_identity: *key_identity,
				routing_locality: *routing_locality,
				row_hlc: hlc(500 + source_row_index as u64),
				is_tombstone: false,
				partition: None,
				table_object_id: None,
			},
		)
		.collect::<Vec<_>>();
	let mut row_group_config =
		crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
			crate::schema::RowGroupRowCount::new(1024),
			crate::schema::RowGroupRowCount::new(1024),
			RowCount::new(1024),
			RecordBodyByteLen::new(1),
		)
		.with_enabled(true);
	row_group_config.stats_supported_types = std::collections::BTreeSet::new();
	let encoding_config = crate::storage::encoding::EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: crate::schema::SimdPageValueCount::new(256),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		..Default::default()
	};
	let encoded =
				crate::storage::multirow::encode_multirow_from_survivors_with_row_group_and_encoding_config(
					&batch,
					&source_rows,
					&descriptor,
					RecordBodyCompressionPolicy::Uncompressed,
					&row_group_config,
					&encoding_config,
				)
				.unwrap();
	let encoded_entries = encoded
		.rows
		.iter()
		.copied()
		.map(Into::into)
		.collect::<Vec<_>>();
	mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let payload_hash = crate::hash::content::digest(&encrypted.bytes);
	let uri = l2
		.put(
			&format!("encoding-index/slab_{slab_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	let pointer = TieredSlabPointer {
		slab_id,
		l2_uri: uri,
		payload_hash,
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	mgr.apply_tier_slab(pointer.clone()).await.unwrap();
	let indexed_locations = [
		RecordRowIndex::new(300),
		RecordRowIndex::new(301),
		RecordRowIndex::new(900),
	]
	.into_iter()
	.map(|record_row_index| {
		let row = encoded
			.rows
			.iter()
			.find(|row| row.record_row_index == record_row_index)
			.expect("indexed source row");
		mgr.lookup(tenant(), row.key_identity)
			.expect("indexed row location")
	})
	.collect::<Vec<_>>();
	let requirement = ColumnFetchRequirement {
		output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([1]),
		..ColumnFetchRequirement::default()
	};

	let indexed = mgr
		.read_projected_indexed_rows(&indexed_locations, &descriptor, &requirement)
		.await
		.unwrap()
		.expect("projected indexed rows");
	assert!(indexed.indexed_rows_materialized);
	assert_eq!(indexed.projected.batch.num_rows(), indexed_locations.len());
	let L2FetchStrategy::RowGroup(plan) = &indexed.strategy else {
		panic!(
			"expected encoding indexed row-group strategy, got {:?}",
			indexed.strategy
		);
	};
	assert_eq!(
		plan.surviving_groups,
		vec![crate::schema::RowGroupIndex::ZERO]
	);
	assert!(plan.phase_b_fetch.is_none());

	let full_frame = mgr
		.read_encrypted_record_from_l2(l2.as_ref(), &pointer, indexed_locations[0])
		.await
		.unwrap();
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id,
			record_offset: RecordBodyByteOffset::new(indexed_locations[0].offset),
		},
		PlaintextByteRange {
			start: 0,
			len: full_frame.len() as u64,
		},
		&full_frame,
		&descriptor,
	)
	.unwrap();
	let page_directory = directory.page_directory.as_ref().unwrap();
	let encoding_directory = directory.encoding_directory.as_ref().unwrap();
	let payload_encoding = encoding_directory
		.buffers
		.iter()
		.find(|encoding| {
			encoding.row_group_index == crate::schema::RowGroupIndex::ZERO
				&& encoding.ipc_column_ordinal == crate::schema::IpcColumnOrdinal::new(1)
				&& encoding.page_end_offsets.len() > 1
				&& encoding.encoded_range.len < encoding.decoded_byte_len
		})
		.expect("payload value buffer should be page-encoded");
	let payload_page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == payload_encoding.row_group_index
				&& page.ipc_column_ordinal == payload_encoding.ipc_column_ordinal
				&& page.ipc_buffer_ordinal == payload_encoding.ipc_buffer_ordinal
		})
		.unwrap();
	let full_encoded_range = crate::storage::row_group::RecordBodyByteRange {
		start: RecordBodyByteOffset::new(
			payload_page.range.start.get() + u64::from(payload_encoding.encoded_range.start.get()),
		),
		len: payload_encoding.encoded_range.len,
	};
	assert!(
		!body_ranges_cover(
			&plan.phase_c_fetch.selected_plaintext_ranges,
			full_encoded_range
		),
		"SIMD-page fast path should not fetch the whole encoded buffer"
	);
	let selected_bytes = plan
		.phase_c_fetch
		.selected_plaintext_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	assert!(
		plan.phase_c_fetch.selected_plaintext_ranges.len() < indexed_locations.len(),
		"Encoding indexed L2 plan should deduplicate multiple selected rows on the same SIMD page"
	);
	assert!(selected_bytes < full_encoded_range.len.get());

	let values = indexed.projected.batch.column(0);
	let values = values
		.as_any()
		.downcast_ref::<UInt8Array>()
		.expect("indexed payload column");
	assert_eq!(values.values(), &[7, 7, 7]);
}

fn body_ranges_cover(
	ranges: &[crate::storage::row_group::RecordBodyByteRange],
	candidate: crate::storage::row_group::RecordBodyByteRange,
) -> bool {
	let Ok(candidate_end) = candidate.end() else {
		return false;
	};
	ranges.iter().any(|range| {
		range.start.get() <= candidate.start.get()
			&& range
				.end()
				.is_ok_and(|range_end| range_end >= candidate_end)
	})
}

fn body_ranges_overlap(
	ranges: &[crate::storage::row_group::RecordBodyByteRange],
	candidate: crate::storage::row_group::RecordBodyByteRange,
) -> bool {
	let Ok(candidate_end) = candidate.end() else {
		return false;
	};
	ranges.iter().any(|range| {
		let Ok(range_end) = range.end() else {
			return false;
		};
		range.start.get() < candidate_end && candidate.start.get() < range_end
	})
}

#[test]
fn compaction_rewrites_table_bound_multirow_frame_with_row_group_directory() {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("payload", DataType::Int64, false),
	]);
	let table_object_id = TableObjectId::new([0x88; 16]);
	let descriptor = TableSchemaDescriptor::new_with_layout(
		table_object_id,
		1,
		"analytics",
		"row_group_compaction",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(10, 10, 8 * 1024 * 1024).unwrap(),
		),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let row_count = 10_usize;
	let batch = RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
			Arc::new(Int64Array::from_iter_values(
				(0..row_count).map(|row| 1_000_i64 + row as i64),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let source_rows = (0..row_count)
		.map(|row| MultiRowSourceRow {
			source_row_index: row,
			key_identity: id(row as u64),
			routing_locality: Locality::new(7),
			row_hlc: hlc(300 + row as u64),
			is_tombstone: false,
			table_object_id: Some(table_object_id),
			partition: None,
		})
		.collect::<Vec<_>>();
	let old = encode_multirow_from_survivors_with_policy(
		&batch,
		&source_rows,
		RecordBodyCompressionPolicy::Uncompressed,
	)
	.unwrap();
	let records = old
		.rows
		.iter()
		.map(|row| CompactionRecord {
			tenant_id: tenant(),
			key_identity: row.key_identity,
			routing_locality: row.routing_locality,
			old_location: RecordLocation {
				slab_id: SlabId::new(77),
				offset: 0,
				length: old.frame.len() as u32,
				hlc: row.row_hlc,
				record_row_index: row.record_row_index,
				is_tombstone: row.is_tombstone,
				placement: PlacementSemantics::RawLocality,
				partition: None,
			},
			reclaim: false,
			retention_ambiguous: false,
			retention_lag_seconds: 0,
		})
		.collect::<Vec<_>>();
	let writer_config = crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(4),
		RowGroupRowCount::new(4),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true);
	let compacted = compact_physical_record_frame(
		&old.frame,
		&records,
		Some(&catalog),
		&writer_config,
		&EncodingWriterConfig::default(),
		&L2ChunkLayoutConfig::default(),
	)
	.unwrap()
	.expect("compacted frame");
	let compacted_again = compact_physical_record_frame(
		&old.frame,
		&records,
		Some(&catalog),
		&writer_config,
		&EncodingWriterConfig::default(),
		&L2ChunkLayoutConfig::default(),
	)
	.unwrap()
	.expect("compacted frame");
	let parsed = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(77),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: compacted.frame.len() as u64,
		},
		&compacted.frame,
		&descriptor,
	)
	.unwrap();
	let page_directory = parsed.page_directory.expect("row-group directory");

	assert_eq!(compacted.frame, compacted_again.frame);
	assert_eq!(page_directory.row_groups.len(), 3);
	assert_eq!(page_directory.row_count.get(), row_count as u64);
	assert_eq!(
		decode_record_batch(&compacted.frame).unwrap().num_rows(),
		row_count
	);
}

#[tokio::test]
async fn encrypt_tiered_object_blob_rejects_empty_plain_slab() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let err = mgr
		.encrypt_tiered_object_blob(SlabId::new(77), &[])
		.unwrap_err();
	assert_eq!(err.kind(), ErrorKind::InvalidInput);
	assert!(
		err.to_string()
			.contains("does not contain any committed records"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn decode_tiered_blob_rejects_footer_tenant_mismatch_with_payload_tenant() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let key = id(0xfeed);
	mgr.append(tenant(), key, hlc(10), b"tenant-bound payload")
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let plaintext = mgr.read_sealed_raw(slab_id).await.unwrap();

	let mismatched_tenant = TenantId::new(17);
	mgr.config
		.crypto
		.install_local_only_for_tests(mismatched_tenant)
		.unwrap();
	let encrypted = mgr
		.config
		.crypto
		.encrypt_tiered_slab_object(
			slab_id,
			&TieredSlabObjectMetadata {
				tenant_id: mismatched_tenant,
				created_hlc: 10,
				chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
				offset_index: vec![TieredOffsetIndexEntry {
					key_locator: IdentityPrefix::ZERO,
					byte_offset: 0,
					byte_length: plaintext.len().try_into().unwrap(),
					flags: 0,
				}],
				column_stats_block: None,
				has_row_tombstone_records: false,
			},
			&plaintext,
		)
		.unwrap();
	let pointer = TieredSlabPointer {
		slab_id,
		l2_uri: format!("memory://tiered/{slab_id:016x}"),
		payload_hash: crate::hash::content::digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};

	let err = decode_tiered_blob(
		mgr.config.crypto.as_ref(),
		&pointer,
		&encrypted.bytes,
		&BTreeSet::new(),
	)
	.unwrap_err();
	assert_eq!(err.kind(), ErrorKind::InvalidData);
	assert!(
		err.to_string().contains("footer tenant"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn lookup_as_of_returns_historical_version() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let key = id(7);
	mgr.append(tenant(), key, hlc(10), b"v1").await.unwrap();
	mgr.append(tenant(), key, hlc(20), b"v2").await.unwrap();
	mgr.append(tenant(), key, hlc(30), b"v3").await.unwrap();

	assert_eq!(
		mgr.lookup_as_of(tenant(), key, hlc(15))
			.unwrap()
			.hlc
			.physical_ms,
		10
	);
	assert_eq!(
		mgr.lookup_as_of(tenant(), key, hlc(20))
			.unwrap()
			.hlc
			.physical_ms,
		20
	);
	assert_eq!(
		mgr.lookup_as_of(tenant(), key, hlc(25))
			.unwrap()
			.hlc
			.physical_ms,
		20
	);
	// Bound before the first write -> nothing visible yet.
	assert!(mgr.lookup_as_of(tenant(), key, hlc(5)).is_none());
}

#[tokio::test]
async fn scan_all_versions_returns_every_retained_version_above_floor() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	// Two keys, two versions each — one above floor, one below.
	let key_a = id(1);
	let key_b = id(2);
	mgr.append(tenant(), key_a, hlc(5), b"a@5").await.unwrap();
	mgr.append(tenant(), key_a, hlc(50), b"a@50").await.unwrap();
	mgr.append(tenant(), key_b, hlc(20), b"b@20").await.unwrap();
	mgr.append(tenant(), key_b, hlc(200), b"b@200")
		.await
		.unwrap();

	let hits = mgr.scan_all_versions(Locality::ZERO, Locality::MAX, hlc(10));
	// Floor at 10 drops (1, hlc=5); the remaining three survive.
	assert_eq!(hits.len(), 3);
	// Sorted first by locality/identity, then by hlc ascending.
	assert_eq!(hits[0].0, tenant());
	assert_eq!(hits[0].1, key_a);
	assert_eq!(hits[0].2, Locality::from(key_a));
	assert_eq!(hits[0].3.hlc.physical_ms, 50);
	assert_eq!(hits[1].0, tenant());
	assert_eq!(hits[1].1, key_b);
	assert_eq!(hits[1].2, Locality::from(key_b));
	assert_eq!(hits[1].3.hlc.physical_ms, 20);
	assert_eq!(hits[2].0, tenant());
	assert_eq!(hits[2].1, key_b);
	assert_eq!(hits[2].2, Locality::from(key_b));
	assert_eq!(hits[2].3.hlc.physical_ms, 200);

	// Inverted window → empty rather than panic.
	assert!(
		mgr.scan_all_versions(Locality::new(10), Locality::new(5), hlc(0))
			.is_empty()
	);
}

#[tokio::test]
async fn append_migration_is_idempotent_on_identical_payload() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let key = id(42);
	let ts = hlc(100);
	let frame = b"payload-bytes";
	let hash = crate::hash::content::digest(frame);

	let first = mgr
		.append_migration(7, tenant(), key, ts, hash, frame)
		.await
		.unwrap();
	assert!(matches!(first, MigrationIngestOutcome::Inserted(_)));

	let second = mgr
		.append_migration(7, tenant(), key, ts, hash, frame)
		.await
		.unwrap();
	assert!(matches!(second, MigrationIngestOutcome::Duplicate(_)));

	// Only one version at this (key, hlc) should exist.
	let locality = Locality::from(key);
	let versions = mgr.scan_all_versions(locality, locality, hlc(0));
	assert_eq!(versions.len(), 1);
}

#[tokio::test]
async fn append_migration_with_locality_preserves_custom_range_membership() {
	let (_source_dir, source) = fresh_manager().await;
	let (_destination_dir, destination) = fresh_manager().await;
	let key = id(0xDADA);
	let identity_locality = Locality::from(key);
	let routing_locality = Locality::new(identity_locality.get().wrapping_add(1));
	assert_ne!(routing_locality, identity_locality);
	let ts = hlc(100);

	source
		.append_durable_with_locality(tenant(), key, routing_locality, ts, b"table-row")
		.await
		.unwrap();
	let source_versions = source.scan_all_versions(routing_locality, routing_locality, hlc(0));
	assert_eq!(source_versions.len(), 1);
	let (tenant_id, migrated_key, migrated_locality, source_loc) = source_versions[0];
	assert_eq!(migrated_key, key);
	assert_eq!(migrated_locality, routing_locality);

	let frame = source.read(source_loc).await.unwrap();
	let payload_hash = crate::hash::content::digest(&frame);
	destination
		.append_migration_with_locality(
			7,
			tenant_id,
			migrated_key,
			migrated_locality,
			source_loc.hlc,
			payload_hash,
			&frame,
		)
		.await
		.unwrap();

	let custom_hits = destination.scan_all_versions(routing_locality, routing_locality, hlc(0));
	assert_eq!(custom_hits.len(), 1);
	assert_eq!(custom_hits[0].1, key);
	assert_eq!(custom_hits[0].2, routing_locality);
	assert!(
		destination
			.scan_all_versions(identity_locality, identity_locality, hlc(0))
			.is_empty()
	);
}

#[tokio::test]
async fn append_migration_rejects_conflicting_payload_for_same_slot() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let key = id(99);
	let ts = hlc(200);
	let frame_a = b"original";
	let hash_a = crate::hash::content::digest(frame_a);
	mgr.append_migration(1, tenant(), key, ts, hash_a, frame_a)
		.await
		.unwrap();

	// A peer claims different bytes for the exact same MVCC
	// slot — the mismatch must fail loudly, not silently
	// accept either side.
	let frame_b = b"tampered";
	let hash_b = crate::hash::content::digest(frame_b);
	let err = mgr
		.append_migration(1, tenant(), key, ts, hash_b, frame_b)
		.await
		.unwrap_err();
	assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn scan_as_of_filters_versions_per_key() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	// Two keys in the same range, each with two versions at
	// different HLCs.
	let key_a = id(1);
	let key_b = id(2);
	mgr.append(tenant(), key_a, hlc(10), b"a@10").await.unwrap();
	mgr.append(tenant(), key_a, hlc(30), b"a@30").await.unwrap();
	mgr.append(tenant(), key_b, hlc(20), b"b@20").await.unwrap();
	mgr.append(tenant(), key_b, hlc(40), b"b@40").await.unwrap();

	// AS OF 25 — should see key=1 at 10 (20 hasn't landed),
	// key=2 at 20.
	let at_25 = mgr.scan_as_of(tenant(), Locality::ZERO, Locality::MAX, hlc(25));
	assert_eq!(at_25.len(), 2);
	assert_eq!(at_25[0].0, Locality::from(key_a).get());
	assert_eq!(at_25[0].1.hlc.physical_ms, 10);
	assert_eq!(at_25[1].0, Locality::from(key_b).get());
	assert_eq!(at_25[1].1.hlc.physical_ms, 20);

	// AS OF 5 — nothing visible yet.
	assert!(
		mgr.scan_as_of(tenant(), Locality::ZERO, Locality::MAX, hlc(5))
			.is_empty()
	);

	// Zero sentinel -> latest versions.
	let latest = mgr.scan(tenant(), Locality::ZERO, Locality::MAX);
	assert_eq!(latest[0].1.hlc.physical_ms, 30);
	assert_eq!(latest[1].1.hlc.physical_ms, 40);
}

#[tokio::test]
async fn scan_as_of_scopes_locality_membership_per_tenant() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let shared_identity = id(0x55);
	let locality = Locality::from(shared_identity);
	let other_tenant = TenantId::new(7);

	mgr.append(tenant(), shared_identity, hlc(10), b"tenant-a")
		.await
		.unwrap();
	mgr.append(other_tenant, shared_identity, hlc(20), b"tenant-b")
		.await
		.unwrap();

	let primary = mgr.scan_as_of(tenant(), locality, locality, hlc(30));
	assert_eq!(primary.len(), 1);
	assert_eq!(primary[0].0, locality.get());
	assert_eq!(primary[0].1.hlc.physical_ms, 10);

	let secondary = mgr.scan_as_of(other_tenant, locality, locality, hlc(30));
	assert_eq!(secondary.len(), 1);
	assert_eq!(secondary[0].0, locality.get());
	assert_eq!(secondary[0].1.hlc.physical_ms, 20);
}

#[tokio::test]
async fn custom_routing_locality_survives_reopen() {
	let dir = fresh_dir("custom-locality-reopen");
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024,
			io: spawn_io_thread(64).unwrap(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let key = id(0xCAFE);
	let routing_locality = Locality::new(42);
	assert_ne!(routing_locality, Locality::from(key));
	manager
		.append_durable_with_locality(tenant(), key, routing_locality, hlc(10), b"table-row")
		.await
		.unwrap();
	assert_eq!(
		manager
			.scan_as_of_with_identity_and_locality(
				tenant(),
				routing_locality,
				routing_locality,
				hlc(20),
			)
			.len(),
		1
	);
	drop(manager);

	let reopened = SlabManager::open(SlabManagerConfig {
		dir: dir.to_path_buf(),
		max_slab_bytes: 1024 * 1024,
		io: spawn_io_thread(64).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();
	let custom_hits = reopened.scan_as_of_with_identity_and_locality(
		tenant(),
		routing_locality,
		routing_locality,
		hlc(20),
	);
	assert_eq!(custom_hits.len(), 1);
	assert_eq!(custom_hits[0].0, key);
	assert_eq!(custom_hits[0].1, routing_locality);
	assert!(
		reopened
			.scan_as_of_with_identity_and_locality(
				tenant(),
				Locality::from(key),
				Locality::from(key),
				hlc(20),
			)
			.is_empty()
	);
}

#[tokio::test]
async fn append_durable_coalesces_concurrent_fdatasyncs() {
	let (_mgr_dir, mgr) = fresh_manager_with_group_commit(GroupCommitConfig {
		floor: Duration::from_millis(20),
		ceiling: Duration::from_millis(20),
		per_write_cost_estimate: Duration::from_millis(20),
	})
	.await;

	let first = {
		let mgr = mgr.clone();
		tokio::spawn(async move {
			mgr.append_durable(tenant(), id(0x11), hlc(10), b"first")
				.await
				.unwrap();
		})
	};
	tokio::time::sleep(Duration::from_millis(1)).await;
	let second = {
		let mgr = mgr.clone();
		tokio::spawn(async move {
			mgr.append_durable(tenant(), id(0x22), hlc(20), b"second")
				.await
				.unwrap();
		})
	};
	first.await.unwrap();
	second.await.unwrap();

	let fdatasync_calls = {
		let state = mgr.state.lock().await;
		state.active.fdatasync_call_count()
	};
	assert_eq!(fdatasync_calls, 1);
}

#[tokio::test]
async fn reopen_restores_tiered_rows_from_redb() {
	let l1_dir = fresh_dir("tiered-reopen-l1");
	let l2_dir = fresh_dir("tiered-reopen-l2");
	let io = spawn_io_thread(64).unwrap();
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let crypto = test_crypto();

	let manager = SlabManager::open(SlabManagerConfig {
		dir: l1_dir.to_path_buf(),
		max_slab_bytes: 1,
		io: io.clone(),
		l2: Some(l2.clone()),
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: crypto.clone(),
	})
	.await
	.unwrap();

	let tiered_key = id(0x44);
	let other_key = id(0x99);
	let tiered_bytes = b"tiered-row".to_vec();
	manager
		.append(tenant(), tiered_key, hlc(10), &tiered_bytes)
		.await
		.unwrap();
	manager
		.append(tenant(), other_key, hlc(20), b"active-row")
		.await
		.unwrap();

	let sealed = manager.list_sealed_slabs().await;
	assert_eq!(sealed.len(), 1, "expected one sealed slab after rotation");
	let sealed_id = sealed[0].slab_id;
	let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
	let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
	let payload_hash = crate::hash::content::digest(&encrypted.bytes);
	let uri = l2
		.put(
			&format!("sealed/slab_{sealed_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	manager
		.apply_tier_slab(TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: uri,
			payload_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();
	assert!(manager.is_tiered(sealed_id).await);
	assert!(
		!slab_path(&l1_dir, sealed_id).exists(),
		"tiered slab should be unlinked from L1"
	);

	drop(manager);

	let reopened = SlabManager::open(SlabManagerConfig {
		dir: l1_dir.to_path_buf(),
		max_slab_bytes: 1,
		io,
		l2: Some(l2),
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: crypto.clone(),
	})
	.await
	.unwrap();
	assert!(reopened.is_tiered(sealed_id).await);

	let location = reopened
		.lookup(tenant(), tiered_key)
		.expect("tiered key should survive reopen");
	assert_eq!(location.slab_id, sealed_id);
	assert_eq!(reopened.read(location).await.unwrap(), tiered_bytes);
}

#[tokio::test]
async fn cleanup_retention_rewrite_tmp_files_removes_stale_plaintext() {
	let dir = fresh_dir("retention-tmp-cleanup");
	let tmp_path = retention_rewrite_tmp_path(&dir, SlabId::new(0x77));
	let table_drop_tmp_path = table_drop_rewrite_tmp_path(&dir, SlabId::new(0x78));
	std::fs::write(&tmp_path, b"plaintext-leak").unwrap();
	std::fs::write(&table_drop_tmp_path, b"dropped-table-plaintext-leak").unwrap();
	assert!(
		tmp_path.exists() && table_drop_tmp_path.exists(),
		"both rewrite fixture files should exist before open"
	);

	let file_ops = BlockingFileOps;
	cleanup_retention_rewrite_tmp_files(&file_ops, &dir)
		.await
		.unwrap();

	assert!(
		!tmp_path.exists() && !table_drop_tmp_path.exists(),
		"startup cleanup removes stale retention and table-drop plaintext"
	);
}

#[tokio::test]
async fn adr0095_tiered_cleanup_intent_is_durable_and_budgeted_before_receipt() {
	let (_manager_dirs, manager, l2) = fresh_manager_with_l2("adr0095-l2-cleanup-intent").await;
	let slab_id = SlabId::new(0x95);
	let key = "adr0095/old-target-bearing-slab.wnd";
	let old_uri = l2.put(key, b"old-target-bytes").await.unwrap();
	manager
		.persist_table_drop_l2_cleanup(slab_id, &old_uri, None)
		.await
		.unwrap();
	let marker = super::retention_reclaim::table_drop_l2_cleanup_path(&manager.config.dir, slab_id);
	assert!(
		marker.exists(),
		"cleanup intent is durable before pointer mutation"
	);

	let (examined, done) = manager
		.reconcile_table_drop_l2_cleanup_markers(l2.as_ref(), 0)
		.await
		.unwrap();
	assert_eq!(examined, 0);
	assert!(
		!done,
		"a zero budget cannot erase or attest pending cleanup"
	);
	assert!(marker.exists());

	let (examined, done) = manager
		.reconcile_table_drop_l2_cleanup_markers(l2.as_ref(), 1)
		.await
		.unwrap();
	assert_eq!(examined, 1);
	assert!(done);
	assert!(
		!marker.exists(),
		"the marker clears only after old-object deletion"
	);
	assert!(
		l2.list("adr0095/old-target-bearing-slab")
			.await
			.unwrap()
			.is_empty(),
		"no target-bearing L2 object survives the cleanup receipt"
	);
}

#[tokio::test]
async fn reopen_after_tombstone_compaction_preserves_survivor_lookup() {
	let dir = fresh_dir("compaction-redb");
	let io = spawn_io_thread(64).unwrap();
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024,
			io: io.clone(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);

	let removed_key = id(0x1010);
	let survivor_key = id(0x2020);
	manager
		.append_durable(tenant(), removed_key, hlc(10), b"drop-me")
		.await
		.unwrap();
	manager
		.append_durable(tenant(), survivor_key, hlc(20), b"keep-me")
		.await
		.unwrap();
	let sealed_id = force_seal_active(&manager).await;
	manager
		.append_durable(tenant(), id(0x3030), hlc(30), b"active-row")
		.await
		.unwrap();
	let survivor_before = manager
		.lookup(tenant(), survivor_key)
		.expect("survivor location before maintenance");

	let target = Locality::from(removed_key).get();
	let report = manager
		.reclaim_tombstoned_ranges(
			&[MigratedRangeTombstone {
				migration_id: 1,
				range_start: target,
				range_end: target,
				to_epoch: 2,
				completed_by: NodeId {
					id: 7,
					_reserved: 0,
				},
				completed_at_hlc: hlc(1),
				migration_hlc_floor: hlc(0),
			}],
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			1,
		)
		.await
		.unwrap();
	assert_eq!(report.compacted_slabs, 1);
	assert!(
		manager.lookup(tenant(), removed_key).is_none(),
		"reclaimed key should disappear from the in-memory index"
	);

	let survivor = manager
		.lookup(tenant(), survivor_key)
		.expect("survivor should remain indexed");
	assert_ne!(
		survivor.slab_id, sealed_id,
		"compaction should relocate the survivor into a replacement slab"
	);
	assert_eq!(manager.read(survivor).await.unwrap(), b"keep-me");
	let manifest = manager.compaction_manifest_snapshot();
	let replacement_run = manifest
		.run(survivor.slab_id)
		.expect("replacement must be published Live");
	assert!(replacement_run.state.is_live());
	assert!(
		matches!(
			manifest.run(sealed_id).map(|run| &run.state),
			Some(SlabState::Retired { successors }) if successors.as_slice() == [survivor.slab_id]
		),
		"maintenance publication must retire the old run before GC, not leave a Live run naming a removed slab"
	);
	assert!(
		manager.read(survivor_before).await.is_ok(),
		"the old backing stays readable for a stale pre-D3 location until GC disposes it"
	);

	drop(manager);

	let reopened = SlabManager::open(SlabManagerConfig {
		dir: dir.to_path_buf(),
		max_slab_bytes: 1024 * 1024,
		io,
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();
	assert!(
		reopened.lookup(tenant(), removed_key).is_none(),
		"reclaimed key should stay absent after reopen"
	);
	let survivor = reopened
		.lookup(tenant(), survivor_key)
		.expect("survivor should still be indexed after reopen");
	assert_eq!(reopened.read(survivor).await.unwrap(), b"keep-me");
}

#[tokio::test]
async fn lsm_compaction_does_not_retain_empty_active_slabs() {
	let dir = fresh_dir("lsm-empty-active");
	let io = spawn_io_thread(16).unwrap();
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1,
			io: io.clone(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);

	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let slab_lengths = || {
		std::fs::read_dir(&dir)
			.unwrap()
			.filter_map(Result::ok)
			.filter_map(|entry| {
				let slab_id = parse_slab_filename(&entry.file_name().to_string_lossy())?;
				Some((slab_id, std::fs::metadata(entry.path()).unwrap().len()))
			})
			.collect::<Vec<_>>()
	};

	const ROUNDS: u64 = 6;
	for round in 0..ROUNDS {
		for n in 0..5 {
			manager
				.append(
					tenant(),
					id(0x7000 + round * 0x100 + n),
					hlc(round * 100 + n + 1),
					b"row",
				)
				.await
				.unwrap();
		}
		// Leave one empty active slab for the compaction path to cross. The
		// loop makes the old defect's empty-file count grow once per round.
		manager.force_seal_active_for_test().await;
		let empty_active_id = manager.active_slab_id().await;
		let empty_active_path = slab_path(&dir, empty_active_id);
		assert!(empty_active_path.exists());

		let before_files = slab_lengths();
		let outcome = manager
			.run_lsm_compaction_tick(&worker, hlc(20_000 + round * 100))
			.await
			.unwrap()
			.expect("five fresh L0 slabs should produce a compaction");
		assert!(outcome.rows_out > 0);

		assert!(
			!empty_active_path.exists(),
			"round {round}: an empty active slab must be unlinked when LSM output advancement crosses it"
		);
		let after_files = slab_lengths();
		let zero_length = after_files.iter().filter(|(_, len)| *len == 0).count();
		assert_eq!(
			zero_length, 1,
			"round {round}: only the current active slab may be empty; empty slabs must not accumulate"
		);
		assert!(
			after_files.len() <= before_files.len() + 1,
			"round {round}: compaction may add its output and successor after removing the empty slab: before={}, after={}",
			before_files.len(),
			after_files.len()
		);
	}

	drop(manager);
	let reopened = SlabManager::open(SlabManagerConfig {
		dir: dir.to_path_buf(),
		max_slab_bytes: 1,
		io,
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();
	assert!(
		reopened.lookup(tenant(), id(0x7000)).is_some(),
		"restart must retain rows after empty-slab retirement"
	);
}

#[tokio::test]
async fn recovery_skips_zero_length_sealed_slabs_but_preserves_records_and_active_tail() {
	let dir = fresh_dir("recovery-empty-sealed");
	let io = spawn_io_thread(16).unwrap();
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1,
			io: io.clone(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	let key = id(0x7A00);
	manager
		.append(tenant(), key, hlc(1), b"record-survives")
		.await
		.unwrap();
	manager.force_seal_active_for_test().await;
	drop(manager);

	// Slab 0 is non-empty, slab 1 was the empty active tail, and the rest are
	// historical empty files. Slab 32 must remain the active tail after reopen.
	for slab_id in 2..=32 {
		std::fs::File::create(slab_path(&dir, SlabId::new(slab_id))).unwrap();
	}

	let reopened = SlabManager::open(SlabManagerConfig {
		dir: dir.to_path_buf(),
		max_slab_bytes: 1,
		io,
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	assert_eq!(reopened.active_slab_id().await, SlabId::new(32));
	assert_eq!(reopened.sealed_count().await, 1);
	let location = reopened
		.lookup(tenant(), key)
		.expect("non-empty sealed slab must remain discoverable");
	assert_eq!(reopened.read(location).await.unwrap(), b"record-survives");
}

#[tokio::test]
async fn recovery_refuses_before_opening_more_than_the_slab_bound() {
	let dir = fresh_dir("recovery-open-bound");
	let io = spawn_io_thread(16).unwrap();
	for slab_id in 0..=(MAX_RECOVERY_OPEN_SLABS as u64) {
		// The preflight must refuse on metadata counts before it attempts to
		// parse these deliberately invalid non-empty slab bodies.
		std::fs::write(slab_path(&dir, SlabId::new(slab_id)), [0xA5]).unwrap();
	}

	let error = match SlabManager::open(SlabManagerConfig {
		dir: dir.to_path_buf(),
		max_slab_bytes: 1,
		io,
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	{
		Ok(_) => panic!("recovery must refuse an over-bound slab directory"),
		Err(error) => error,
	};

	assert_eq!(error.kind(), std::io::ErrorKind::Other);
	let message = error.to_string();
	assert!(message.contains("total slab files=4097"), "{message}");
	assert!(message.contains("non-empty slab files=4097"), "{message}");
	assert!(message.contains("slabs to open=4097"), "{message}");
	assert!(message.contains("bound=4096"), "{message}");
}

#[tokio::test]
async fn lsm_compaction_rejects_sidecar_covered_source_runs_without_catalog_context() {
	let (_manager_dir, manager) = fresh_manager().await;
	manager
		.append_durable(tenant(), id(0x55CA), hlc(10), b"sidecar-covered")
		.await
		.unwrap();
	let source_slab_id = manager.force_seal_active_for_test().await;

	{
		let mut manifest =
			recover_rwlock_write(&manager.compaction_manifest, "compaction_manifest");
		let mut run = manifest
			.get_run(source_slab_id)
			.expect("sealed slab should be present in the LSM manifest")
			.clone();
		run.sidecar_coverage = crate::storage::SidecarCoverage::Complete;
		manifest.insert_live_run(run);
	}

	let snapshot = manager.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x55CA),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![source_slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(20),
		compaction_start_hlc: hlc(21),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let commit = CompactionCommit {
		job_id: plan.job_id,
		manifest_epoch_pre: plan.manifest_epoch_pre,
		slabs_to_retire: vec![source_slab_id],
		slabs_to_add: Vec::new(),
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: CompactionDigestSummary::zero(),
	};

	let err = manager
		.validate_lsm_sidecar_swap_readiness(&plan, &commit, false)
		.unwrap_err();
	assert!(matches!(err, CompactionFailure::SidecarDigestMismatch));
}

#[tokio::test]
async fn lsm_compaction_copies_whole_multirow_frame_into_output_run() {
	let (_manager_dir, manager) = fresh_manager().await;
	let first_key = id(0x6001);
	let second_key = id(0x6002);
	let frame = encode_test_multirow_storage_frame(&[
		(first_key, Locality::new(0x61), hlc(10), 11),
		(second_key, Locality::new(0x62), hlc(20), 22),
	]);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("test frame should be a storage multi-row frame");
	let source_location = manager
		.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let source_slab_id = manager.force_seal_active_for_test().await;
	assert_eq!(source_location.slab_id, source_slab_id);

	let snapshot = manager.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x6001),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![source_slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let worker = LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		manager.slab_pin_table(),
	)
	.unwrap();

	let outcome = manager
		.run_lsm_compaction_once(&worker, plan)
		.await
		.unwrap();

	assert_eq!(outcome.rows_out, 2);
	assert_eq!(outcome.output_slabs.len(), 1);
	let output_slab_id = outcome.output_slabs[0].slab_id;
	let first = manager.lookup(tenant(), first_key).unwrap();
	let second = manager.lookup(tenant(), second_key).unwrap();
	assert_eq!(first.slab_id, output_slab_id);
	assert_eq!(second.slab_id, output_slab_id);
	assert_ne!(output_slab_id, source_slab_id);
	assert_eq!(first.offset, second.offset);
	assert_eq!(first.length, second.length);
	assert_eq!(first.record_row_index, RecordRowIndex::ZERO);
	assert_eq!(second.record_row_index, RecordRowIndex::new(1));
	assert!(!manager.read(first).await.unwrap().is_empty());
	assert!(!manager.read(second).await.unwrap().is_empty());
}

#[tokio::test]
async fn migrated_range_tombstone_marks_tiered_slab_visibility_incomplete() {
	let (_manager_dir, manager) = fresh_manager().await;
	let key = id(0x5151);
	manager
		.append_durable(tenant(), key, hlc(10), b"migrated")
		.await
		.unwrap();
	let sealed_id = force_seal_active(&manager).await;
	{
		let state = manager.state.lock().await;
		state.sealed.remove(&sealed_id);
		state.tiered.insert(
			sealed_id,
			TieredSlabPointer {
				slab_id: sealed_id,
				l2_uri: "memory://tiered".to_owned(),
				payload_hash: [0xAA; 32],
				is_encrypted: false,
				tenant_id: Some(tenant()),
				tenant_kek_id: None,
				stats_format_version: None,
				stats_block_len: None,
				stats_block_hash128: None,
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			},
		);
	}

	let locality = Locality::from(key).get();
	let tombstone = MigratedRangeTombstone {
		migration_id: 9,
		range_start: locality,
		range_end: locality,
		to_epoch: 2,
		completed_by: NodeId {
			id: 7,
			_reserved: 0,
		},
		completed_at_hlc: hlc(50),
		migration_hlc_floor: hlc(0),
	};
	assert!(!manager.slab_has_migrated_range_tombstone_at(
		sealed_id,
		hlc(49),
		std::slice::from_ref(&tombstone)
	));
	assert!(manager.slab_has_migrated_range_tombstone_at(
		sealed_id,
		hlc(50),
		std::slice::from_ref(&tombstone)
	));

	let changed = manager
		.record_migrated_range_tombstone(&tombstone, Some(99))
		.await
		.unwrap();
	assert_eq!(changed, 1);
	let pointer = manager.tier_pointer(sealed_id).await.unwrap();
	assert_eq!(
		pointer.tombstone_metadata,
		TombstoneMetadata::Earliest(hlc(50))
	);
	assert!(crate::storage::slab_visibility_complete(
		hlc(10),
		pointer.tombstone_metadata,
		SupersessionMetadata::None,
		hlc(49)
	));
	assert!(!crate::storage::slab_visibility_complete(
		hlc(10),
		pointer.tombstone_metadata,
		SupersessionMetadata::None,
		hlc(50)
	));
}

#[tokio::test]
async fn post_tier_supersession_updates_tiered_pointer_visibility() {
	let (_manager_dir, manager) = fresh_manager().await;
	let key = id(0x5151);
	manager
		.append_durable(tenant(), key, hlc(10), b"v10")
		.await
		.unwrap();
	let sealed_id = force_seal_active(&manager).await;
	manager
		.apply_tier_slab(TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: "memory://supersession".into(),
			payload_hash: [0x55; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

	manager
		.append_durable(tenant(), key, hlc(20), b"v20")
		.await
		.unwrap();

	let pointer = manager.tier_pointer(sealed_id).await.unwrap();
	assert_eq!(
		pointer.supersession_metadata,
		SupersessionMetadata::Earliest(hlc(20))
	);
	assert!(!crate::storage::slab_visibility_complete(
		hlc(10),
		TombstoneMetadata::None,
		pointer.supersession_metadata,
		hlc(20)
	));
}

#[tokio::test]
async fn post_tier_row_tombstone_updates_tiered_pointer_visibility() {
	let (_manager_dir, manager) = fresh_manager().await;
	let key = id(0x5152);
	let locality = Locality::new(52);
	let data_frame = encode_test_multirow_storage_frame(&[(key, locality, hlc(10), 100)]);
	let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
		.unwrap()
		.expect("data entry");
	manager
		.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
		.await
		.unwrap();
	let sealed_id = force_seal_active(&manager).await;
	manager
		.apply_tier_slab(TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: "memory://row-tombstone".into(),
			payload_hash: [0x56; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

	let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
		TableObjectId::new([0x51; 16]),
		key,
		locality,
		hlc(20),
	);
	manager
		.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();

	let pointer = manager.tier_pointer(sealed_id).await.unwrap();
	assert_eq!(
		pointer.tombstone_metadata,
		TombstoneMetadata::Earliest(hlc(20))
	);
	assert!(!crate::storage::slab_visibility_complete(
		hlc(10),
		pointer.tombstone_metadata,
		SupersessionMetadata::None,
		hlc(20)
	));
}

#[tokio::test]
async fn tiered_pk_tombstone_slab_records_row_tombstone_and_resurrection_metadata() {
	let (_manager_dir, manager) = fresh_manager().await;
	let descriptor = pk_retention_descriptor(TableObjectId::new([0x5A; 16]));
	let key = id(0x5A01);
	let locality = Locality::new(0x5A);
	let data_frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(key, locality, hlc(10), 100, false)],
	);
	let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
		.unwrap()
		.expect("pk data entry");
	manager
		.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
		.await
		.unwrap();
	let (tombstone_frame, tombstone_entries) =
		encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20));
	manager
		.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
		.await
		.unwrap();
	let sealed_id = force_seal_active(&manager).await;
	assert_eq!(
		manager.slab_row_tombstone_metadata(sealed_id),
		TombstoneMetadata::Earliest(hlc(20))
	);
	manager
		.apply_tier_slab(TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: "memory://pk-row-tombstone".into(),
			payload_hash: [0x5A; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: manager.slab_row_tombstone_metadata(sealed_id),
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

	let resurrected_frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(key, locality, hlc(30), 200, false)],
	);
	let resurrected_entries = storage_multirow_index_entries_from_frame(&resurrected_frame)
		.unwrap()
		.expect("pk resurrection entry");
	manager
		.append_durable_multirow_with_locality(tenant(), &resurrected_frame, &resurrected_entries)
		.await
		.unwrap();

	let pointer = manager.tier_pointer(sealed_id).await.unwrap();
	assert_eq!(
		pointer.tombstone_metadata,
		TombstoneMetadata::Earliest(hlc(20))
	);
	assert_eq!(
		pointer.supersession_metadata,
		SupersessionMetadata::Earliest(hlc(30))
	);
	assert!(!crate::storage::slab_visibility_complete(
		hlc(20),
		pointer.tombstone_metadata,
		pointer.supersession_metadata,
		hlc(30)
	));
}

#[tokio::test]
async fn retention_pruning_keeps_latest_version_before_floor() {
	let (_manager_dir, manager) = fresh_manager().await;
	let key = id(0x4040);
	manager
		.append_durable(tenant(), key, hlc(10), b"v10")
		.await
		.unwrap();
	manager
		.append_durable(tenant(), key, hlc(20), b"v20")
		.await
		.unwrap();
	manager
		.append_durable(tenant(), key, hlc(30), b"v30")
		.await
		.unwrap();
	let sealed_id = force_seal_active(&manager).await;
	assert_eq!(manager.sealed_count().await, 1);

	let report = manager
		.prune_versions_older_than(
			hlc(25),
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			1,
		)
		.await
		.unwrap();
	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.pruned_versions, 1);
	assert!(report.reclaimed_bytes > 0);

	let as_of_25 = manager
		.lookup_as_of(tenant(), key, hlc(25))
		.expect("anchor version retained");
	assert_ne!(as_of_25.slab_id, sealed_id);
	assert_eq!(as_of_25.hlc.physical_ms, 20);
	assert_eq!(manager.read(as_of_25).await.unwrap(), b"v20");

	let latest = manager.lookup(tenant(), key).expect("latest retained");
	assert_eq!(latest.hlc.physical_ms, 30);
	assert_eq!(manager.read(latest).await.unwrap(), b"v30");

	assert!(
		manager.lookup_as_of(tenant(), key, hlc(15)).is_none(),
		"versions older than the retained window should be pruned"
	);
}

#[tokio::test]
async fn retention_pruning_keeps_latest_when_all_versions_precede_floor() {
	let (_manager_dir, manager) = fresh_manager().await;
	let key = id(0x5050);
	manager
		.append_durable(tenant(), key, hlc(10), b"v10")
		.await
		.unwrap();
	manager
		.append_durable(tenant(), key, hlc(20), b"v20")
		.await
		.unwrap();
	force_seal_active(&manager).await;

	let report = manager
		.prune_versions_older_than(
			hlc(100),
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			1,
		)
		.await
		.unwrap();
	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.pruned_versions, 1);

	let latest = manager.lookup(tenant(), key).expect("latest retained");
	assert_eq!(latest.hlc.physical_ms, 20);
	assert_eq!(manager.read(latest).await.unwrap(), b"v20");
	assert!(
		manager.lookup_as_of(tenant(), key, hlc(15)).is_none(),
		"earlier history should be removed once the full chain is outside retention"
	);
}

impl SlabManager {
	pub(crate) fn forget_routing_locality_for_test(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) {
		self.key_localities.remove(&(tenant_id, key_identity));
	}

	pub async fn force_seal_active_for_test(&self) -> SlabId {
		let mut state = self.state.lock().await;
		let sealed = state.active.clone();
		sealed
			.fsync()
			.await
			.expect("force_seal_active_for_test should fsync sealed slab");
		let sealed_id = sealed.slab_id();
		let sealed_size = sealed.write_cursor();
		let next_id = state.next_slab_id;
		let next = Arc::new(
			LocalSlabStore::open(
				slab_path(&self.config.dir, next_id),
				next_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await
			.expect("force_seal_active_for_test should open next slab"),
		);
		state.active = next;
		state.sealed.insert(sealed_id, sealed);
		state.next_slab_id = next_id.saturating_add(1);
		self.record_l0_compaction_run(sealed_id, sealed_size);
		sealed_id
	}

	pub(crate) async fn prepare_tiered_retention_rewrite(
		&self,
		pointer: &TieredSlabPointer,
		retention_floor: HybridTimestamp,
		table_schemas: &TableSchemaCatalog,
	) -> std::io::Result<Option<PreparedTieredRetentionRewrite>> {
		let old_blob = self
			.config
			.l2
			.clone()
			.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"cannot rewrite tiered slab {} without an L2 object store",
						pointer.slab_id
					),
				)
			})?
			.get(&pointer.l2_uri)
			.await?;
		validate_tiered_blob(pointer, &old_blob)?;
		let plaintext = Zeroizing::new(decode_tiered_blob(
			self.config.crypto.as_ref(),
			pointer,
			&old_blob,
			&self.acknowledged_offsets_in_slab(pointer.slab_id),
		)?);
		let mut records =
			recover_slab_entries_from_blob(&plaintext, pointer.slab_id, &BTreeSet::new())?
				.into_iter()
				.map(|entry| CompactionRecord {
					tenant_id: entry.tenant_id,
					key_identity: entry.key_identity,
					routing_locality: self.recovered_entry_routing_locality(&entry),
					old_location: entry.location,
					reclaim: self.is_retention_prunable(
						entry.tenant_id,
						entry.key_identity,
						entry.location,
						retention_floor,
					),
					retention_ambiguous: false,
					retention_lag_seconds: 0,
				})
				.collect::<Vec<_>>();
		Self::mark_retention_prunable_row_tombstone_pairs(&mut records, retention_floor);
		records.sort_by_key(|record| record.old_location.offset);
		let physical_groups = physical_compaction_groups(&records);
		let pruned_versions = records.iter().filter(|record| record.reclaim).count();
		let retention_stats = Self::retention_compaction_stats(&records);
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		if pruned_versions == 0 {
			return Ok(None);
		}
		if survivors == 0 {
			return Ok(Some(PreparedTieredRetentionRewrite::Retire {
				slab_id: pointer.slab_id,
				old_l2_uri: pointer.l2_uri.clone(),
				pruned_versions,
				retention_stats,
				reclaimed_bytes: old_blob.len() as u64,
			}));
		}

		let tmp_path = retention_rewrite_tmp_path(&self.config.dir, pointer.slab_id);
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let mut multirow_recompact_count = 0_u64;
		let rewrite_result: std::io::Result<Zeroizing<Vec<u8>>> = async {
			let rewritten = Arc::new(
				LocalSlabStore::open(
					&tmp_path,
					pointer.slab_id,
					self.file_ops.clone(),
					self.config.group_commit.clone(),
				)
				.await?,
			);
			for group in physical_groups.values() {
				let Some(first) = group.first().copied() else {
					continue;
				};
				let frame =
					Zeroizing::new(extract_record_from_blob(&plaintext, first.old_location)?);
				let Some(compacted) = compact_physical_record_frame(
					frame.as_ref(),
					group,
					Some(table_schemas),
					&self.config.row_group_writer,
					&self.config.encoding_writer,
					&self.config.l2_chunk_layout,
				)?
				else {
					continue;
				};
				if compacted.was_multirow {
					multirow_recompact_count = multirow_recompact_count.saturating_add(1);
				}
				rewritten
					.append(
						compacted.tenant_id,
						compacted.key_identity,
						compacted.hlc,
						&compacted.frame,
					)
					.await?;
			}
			rewritten.fsync().await?;
			Ok(Zeroizing::new(self.file_ops.read_all(&tmp_path).await?))
		}
		.await;
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let rewritten_plaintext = rewrite_result?;
		if multirow_recompact_count > 0 {
			metrics().observe_storage_multirow_recompact(multirow_recompact_count);
		}
		let encrypted = self.encrypt_tiered_object_blob_with_table_stats(
			pointer.slab_id,
			rewritten_plaintext.as_ref(),
			table_schemas,
		)?;
		let row_tombstone_metadata = row_tombstone_metadata_from_blob(
			rewritten_plaintext.as_ref(),
			pointer.slab_id,
			&BTreeSet::new(),
		)?;
		Ok(Some(PreparedTieredRetentionRewrite::Rewrite {
			slab_id: pointer.slab_id,
			old_l2_uri: pointer.l2_uri.clone(),
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			tombstone_metadata: merge_row_tombstone_metadata(
				pointer.tombstone_metadata,
				row_tombstone_metadata,
			),
			supersession_metadata: self.slab_supersession_metadata(pointer.slab_id),
			reclaimed_bytes: (old_blob.len() as u64).saturating_sub(encrypted.bytes.len() as u64),
			pruned_versions,
			retention_stats,
			encrypted_bytes: encrypted.bytes,
		}))
	}
}

/// ADR 0539 Phase 0 seam publication, now the discriminating control. Authored red
/// against production calls only — ordinary writes, seal, production backfill, then
/// retention pruning — when the compaction paths could still skip reconciliation via
/// an optional context and unlink unconditionally. Green requires what the ruling
/// requires: reconciliation proof before retirement, with the skip unrepresentable.
#[tokio::test]
async fn prune_compaction_must_not_orphan_secondary_index_sources() {
	let dir = fresh_dir("adr0539-prune-orphans-secondary-source");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();

	for id_value in 0..4_i64 {
		append_secondary_index_table_row_for_test(
			&manager,
			&table,
			id_value,
			Some(30 + id_value),
			hlc(6_000 + id_value as u64),
		)
		.await;
	}
	// A second version of key 0: its older version is the prunable record that makes
	// retention pruning take the with-replacement compaction path (survivors remain).
	append_secondary_index_table_row_for_test(&manager, &table, 0, Some(40), hlc(6_010)).await;
	let slab_id = manager.force_seal_active_for_test().await;

	// Production backfill: the durable segment now names `slab_id` as its source and
	// coverage records it covered.
	let segment = manager
		.build_secondary_index_segment_for_source_slab(tenant(), &table, &descriptor, slab_id)
		.await
		.unwrap();
	manager
		.persist_secondary_index_segment(&segment)
		.await
		.unwrap();

	// Production retention pruning over the sealed slab. The floor predates every row,
	// so nothing is prunable: this is the routine keep-everything compaction that
	// builds a replacement and retires the source.
	let _slab = {
		let state = manager.state.lock().await;
		let (_, slab) = state.sealed.remove(&slab_id).expect("sealed slab present");
		state.sealed.insert(slab_id, slab.clone());
		slab
	};
	let mut catalog_tables = TableSchemaCatalog::default();
	catalog_tables.upsert(tenant(), table.clone()).unwrap();
	let mut catalog_indexes = SecondaryIndexCatalog::default();
	catalog_indexes
		.create(tenant(), descriptor.clone(), &catalog_tables)
		.unwrap();
	let outcome = manager
		.prune_one_sealed_slab(
			slab_id,
			hlc(7_000),
			SecondaryIndexCompactionContext {
				table_schemas: &catalog_tables,
				secondary_indexes: &catalog_indexes,
			},
		)
		.await
		.unwrap();
	assert!(
		outcome.is_some(),
		"the prune must actually compact: a vacuous run proves nothing"
	);

	// The published-reference invariant: after retirement, no durable secondary-index
	// segment may still name the retired slab as a source it expects to resolve.
	let still_naming_source = manager.list_secondary_index_segments_for_source_slab(slab_id);
	let source_available = manager
		.ensure_secondary_index_source_slab_available(slab_id)
		.await;
	assert!(
		still_naming_source.is_empty() || source_available.is_ok(),
		"prune compaction retired slab {} while durable secondary-index segments still \
		 name it as their source: segments={}, availability={:?}",
		slab_id.get(),
		still_naming_source.len(),
		source_available.err().map(|error| error.to_string()),
	);
}

/// The recluster path carried the identical retire-without-proof shape as the proven
/// seam. Its sibling control: a durable segment names the source slab while the
/// catalogs do not know its index, so reconciliation cannot prove retirement safe.
/// The replacement may install, but the source file must be preserved, the slab
/// quarantined, and the integrity state recorded — containment, never a dangling ref.
#[tokio::test]
async fn recluster_refuses_retirement_without_reconciliation_proof() {
	let dir = fresh_dir("adr0539-recluster-refuses");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	let single_descriptor = layout_recluster_descriptor(0x64, 1, RecordLayoutPolicy::SingleRow);
	let multi_descriptor = layout_recluster_descriptor(
		0x64,
		2,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap()),
	);
	let batch = layout_recluster_batch(&[1, 2, 3, 4]);
	let meta = layout_recluster_meta(&single_descriptor, batch.num_rows());
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&single_descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	for row in 0..batch.num_rows() {
		let row_batch = batch.slice(row, 1);
		let frame = encode_test_batch(&row_batch);
		manager
			.append_durable_with_locality(
				tenant(),
				admission.key_identities[row],
				admission.localities[row],
				hlc(10 + row as u64),
				&frame,
			)
			.await
			.unwrap();
	}
	let slab_id = manager.force_seal_active_for_test().await;
	manager
		.persist_secondary_index_segment(&secondary_index_segment_for_test(slab_id))
		.await
		.unwrap();

	let mut catalog_tables = TableSchemaCatalog::default();
	catalog_tables
		.upsert(tenant(), multi_descriptor.clone())
		.unwrap();
	let catalog_indexes = SecondaryIndexCatalog::default();
	let slab = {
		let state = manager.state.lock().await;
		let (_, slab) = state.sealed.remove(&slab_id).expect("sealed slab present");
		state.sealed.insert(slab_id, slab.clone());
		slab
	};
	let outcome = manager
		.recluster_one_sealed_slab(
			slab_id,
			slab,
			tenant(),
			&multi_descriptor,
			SecondaryIndexCompactionContext {
				table_schemas: &catalog_tables,
				secondary_indexes: &catalog_indexes,
			},
		)
		.await
		.unwrap();
	assert!(outcome.is_some(), "the recluster itself proceeds");
	assert!(
		manager.is_compaction_slab_quarantined(slab_id),
		"unproven retirement must quarantine"
	);
	assert!(
		slab_path(&dir, slab_id).exists(),
		"the unproven slab file must be preserved on disk"
	);
	let segment_table = secondary_index_table_descriptor_for_test();
	assert!(
		manager
			.table_integrity_failure(tenant(), segment_table.table_object_id)
			.is_some(),
		"the affected table's integrity state must be recorded"
	);
}

/// ADR 0539: the read-side gate is the invariant — every historical source slab has
/// a durable segment — never the in-memory failure record. A slab sealed without a
/// segment is a gap regardless of whether this process observed any failure, and both
/// silent shapes must refuse: a key the covered segment CAN answer (silently
/// incomplete) and a key living only in the gap slab (a successful empty lie).
#[tokio::test]
async fn secondary_index_lookup_refuses_while_a_source_slab_lacks_a_segment() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	append_secondary_index_table_row_for_test(&manager, &table, 1, Some(10), hlc(10)).await;
	append_secondary_index_table_row_for_test(&manager, &table, 2, Some(20), hlc(20)).await;
	force_seal_active(&manager).await;
	manager
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.unwrap();

	// A second sealed slab with no segment: a durable gap, no failure record needed.
	append_secondary_index_table_row_for_test(&manager, &table, 3, Some(30), hlc(30)).await;
	manager.force_seal_active_for_test().await;

	for probe in [10, 30] {
		let request = TableLookupByIndexRequest::from_descriptor(
			tenant(),
			&descriptor,
			IndexQuery::PointEqual {
				encoded_key: secondary_index_encoded_i64_for_test(probe),
			},
			hlc(99),
		);
		let err = manager
			.table_lookup_by_index(&request, &descriptor, SecondaryIndexState::Ready)
			.await
			.expect_err("a lookup over a durable coverage gap must refuse");
		assert!(
			err.to_string().contains("uncovered"),
			"refusal must name the gap: {err}"
		);
	}
}

/// The gate must survive restart, because the failure record does not: a process that
/// never observed the failure still refuses, from durable completeness alone — the
/// restart hole that resurrected the silent wrong answer is closed by construction.
#[tokio::test]
async fn secondary_index_lookup_still_refuses_after_reopen() {
	let dir = fresh_dir("adr0539-lookup-gate-survives-reopen");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	append_secondary_index_table_row_for_test(&manager, &table, 1, Some(10), hlc(10)).await;
	force_seal_active(&manager).await;
	drop(manager);

	let request = TableLookupByIndexRequest::from_descriptor(
		tenant(),
		&descriptor,
		IndexQuery::PointEqual {
			encoded_key: secondary_index_encoded_i64_for_test(10),
		},
		hlc(99),
	);
	let reopened = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let err = reopened
		.table_lookup_by_index(&request, &descriptor, SecondaryIndexState::Ready)
		.await
		.expect_err("a fresh process must refuse over a durable gap it never observed");
	assert!(err.to_string().contains("uncovered"));

	// Backfill heals the gap; the healed state also survives reopen.
	reopened
		.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
		.await
		.unwrap();
	let report = reopened
		.table_lookup_by_index(&request, &descriptor, SecondaryIndexState::Ready)
		.await
		.unwrap();
	assert_eq!(report.rows.len(), 1);
	drop(reopened);
	let reopened_again = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	let report = reopened_again
		.table_lookup_by_index(&request, &descriptor, SecondaryIndexState::Ready)
		.await
		.unwrap();
	assert_eq!(report.rows.len(), 1);
}

/// Candidacy is budget-bounded, never class-gated: every failure retries until the
/// budget spends, so no misclassification can strand a transient error permanently,
/// and a structural failure burns its budget cheaply and lands where it should.
#[tokio::test]
async fn backfill_candidacy_survives_failures_until_the_budget_spends() {
	let (_manager_dir, manager) = fresh_manager().await;
	let table = secondary_index_table_descriptor_for_test();
	let descriptor = secondary_index_descriptor_for_test();
	append_secondary_index_table_row_for_test(&manager, &table, 1, Some(10), hlc(10)).await;
	let slab_id = manager.force_seal_active_for_test().await;
	let key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
		tenant(),
		&descriptor,
	));

	for attempt in 0..SECONDARY_INDEX_BACKFILL_ATTEMPT_BUDGET {
		let plan = manager
			.ensure_secondary_index_coverage(tenant(), &descriptor, 4096)
			.await
			.unwrap();
		assert!(
			plan.candidates
				.iter()
				.any(|candidate| candidate.source_slab_id() == slab_id),
			"attempt {attempt}: an unexhausted failure record must not remove candidacy"
		);
		manager.mark_secondary_index_backfill_uncovered(
			key,
			slab_id,
			SecondaryIndexBackfillError::new(
				SecondaryIndexBackfillErrorReason::PersistFailure,
				"injected failure".to_string(),
			),
		);
	}
	let plan = manager
		.ensure_secondary_index_coverage(tenant(), &descriptor, 4096)
		.await
		.unwrap();
	assert!(
		!plan
			.candidates
			.iter()
			.any(|candidate| candidate.source_slab_id() == slab_id),
		"a spent budget removes candidacy"
	);
	// The gate refuses throughout regardless of budget state: refusal is derived from
	// durable completeness, not from the retry bookkeeping.
	let request = TableLookupByIndexRequest::from_descriptor(
		tenant(),
		&descriptor,
		IndexQuery::PointEqual {
			encoded_key: secondary_index_encoded_i64_for_test(10),
		},
		hlc(99),
	);
	assert!(
		manager
			.table_lookup_by_index(&request, &descriptor, SecondaryIndexState::Ready)
			.await
			.is_err()
	);
}

async fn repair_fixture_manager(dir: &std::path::Path, io: IoRingHandle) -> Arc<SlabManager> {
	Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1,
			io,
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	)
}

fn slab_files_on_disk(dir: &std::path::Path) -> Vec<(SlabId, u64)> {
	let mut found = std::fs::read_dir(&dir)
		.unwrap()
		.filter_map(Result::ok)
		.filter_map(|entry| {
			let slab_id = parse_slab_filename(&entry.file_name().to_string_lossy())?;
			Some((slab_id, std::fs::metadata(entry.path()).unwrap().len()))
		})
		.collect::<Vec<_>>();
	found.sort_unstable();
	found
}

#[tokio::test]
async fn adr0084_repair_preserves_a_zero_length_active_slab() {
	let dir = fresh_dir("repair-active");
	let io = spawn_io_thread(16).unwrap();
	let manager = repair_fixture_manager(&dir, io.clone()).await;
	for n in 0..4 {
		manager
			.append(tenant(), id(0x9000 + n), hlc(n + 1), b"row")
			.await
			.unwrap();
	}
	// Seal so the active slab is a fresh, empty file — the state an affected
	// profile is left in, and the one `split_last` will promote on next open.
	manager.force_seal_active_for_test().await;
	let active_id = manager.active_slab_id().await;
	drop(manager);

	let active_path = slab_path(&dir, active_id);
	assert_eq!(
		std::fs::metadata(&active_path).unwrap().len(),
		0,
		"the fixture needs an empty active slab for this control to mean anything"
	);

	let report = SlabManager::repair_empty_slabs(&dir, io.clone())
		.await
		.expect("repair runs against a profile with a durable index");

	assert_eq!(report.preserved_active, Some(active_id));
	assert!(!report.removed.contains(&active_id));
	assert!(
		active_path.exists(),
		"split_last promotes the highest id to active, so an empty highest slab must survive"
	);

	let reopened = repair_fixture_manager(&dir, io).await;
	assert!(
		reopened.lookup(tenant(), id(0x9000)).is_some(),
		"a repaired profile still opens and keeps its rows"
	);
}

#[tokio::test]
async fn adr0084_repair_refuses_an_empty_slab_the_index_still_references() {
	let dir = fresh_dir("repair-referenced");
	let io = spawn_io_thread(16).unwrap();
	let manager = repair_fixture_manager(&dir, io.clone()).await;
	for n in 0..4 {
		manager
			.append(tenant(), id(0xa000 + n), hlc(n + 1), b"row")
			.await
			.unwrap();
	}
	let active_id = manager.active_slab_id().await;
	drop(manager);

	// Truncate a sealed slab that carries data. That is exactly the shape the
	// repair must decline: zero length on disk, but still named by the index.
	let victim = slab_files_on_disk(&dir)
		.into_iter()
		.find(|(slab_id, len)| *slab_id != active_id && *len > 0)
		.expect("a sealed slab with data")
		.0;
	std::fs::OpenOptions::new()
		.write(true)
		.open(slab_path(&dir, victim))
		.unwrap()
		.set_len(0)
		.unwrap();

	let report = SlabManager::repair_empty_slabs(&dir, io).await.unwrap();

	assert!(
		report.referenced_refused.contains(&victim),
		"an empty file the index still names is reported, not swept"
	);
	assert!(!report.removed.contains(&victim));
	assert!(slab_path(&dir, victim).exists());
}

#[tokio::test]
async fn adr0084_repair_removes_unreferenced_empties_and_is_idempotent() {
	let dir = fresh_dir("repair-idempotent");
	let io = spawn_io_thread(16).unwrap();
	let manager = repair_fixture_manager(&dir, io.clone()).await;
	manager
		.append(tenant(), id(0xb000), hlc(1), b"row")
		.await
		.unwrap();
	drop(manager);

	// Two empty slabs above every real one: the higher becomes active and is
	// preserved, the lower is unreferenced litter and must go.
	let litter = SlabId::new(0x100);
	let higher_active = SlabId::new(0x101);
	std::fs::File::create(slab_path(&dir, litter)).unwrap();
	std::fs::File::create(slab_path(&dir, higher_active)).unwrap();

	let first = SlabManager::repair_empty_slabs(&dir, io.clone())
		.await
		.unwrap();
	assert_eq!(first.preserved_active, Some(higher_active));
	assert_eq!(first.removed, vec![litter]);
	assert!(!slab_path(&dir, litter).exists());

	let second = SlabManager::repair_empty_slabs(&dir, io).await.unwrap();
	assert!(
		second.removed.is_empty(),
		"a second run has nothing left to remove"
	);
	assert_eq!(second.slab_files, first.slab_files - first.removed.len());
}

#[tokio::test]
async fn adr0084_repair_refuses_without_a_durable_index() {
	let dir = fresh_dir("repair-no-index");
	let io = spawn_io_thread(16).unwrap();
	let manager = repair_fixture_manager(&dir, io.clone()).await;
	manager
		.append(tenant(), id(0xc000), hlc(1), b"row")
		.await
		.unwrap();
	drop(manager);
	std::fs::File::create(slab_path(&dir, SlabId::new(0x200))).unwrap();
	std::fs::File::create(slab_path(&dir, SlabId::new(0x201))).unwrap();

	let index_path = dir.join("storage_index.redb");
	std::fs::remove_file(&index_path).unwrap();
	let before = slab_files_on_disk(&dir);

	let error = SlabManager::repair_empty_slabs(&dir, io)
		.await
		.expect_err("an absent index must refuse rather than assume nothing is referenced");

	assert_eq!(error.kind(), ErrorKind::NotFound);
	assert!(
		error.to_string().contains("refuses to run without"),
		"the refusal names the missing index: {error}"
	);
	assert_eq!(
		slab_files_on_disk(&dir),
		before,
		"a refused repair removes nothing"
	);
	assert!(
		!index_path.exists(),
		"the refusal must not create the index it declined to read"
	);
}

/// ADR 0089 range discoverability. Row membership in the locality index is
/// per row — `(routing_locality, key_identity)` is inserted individually — so a
/// scan interval must yield exactly the rows whose localities fall inside it,
/// each exactly once, whatever record holds them. These pin the interval
/// semantics the packing change depends on, without assuming a scan is aligned
/// to any prefix boundary.
#[tokio::test]
async fn adr0089_locality_intervals_yield_exactly_the_rows_inside_them() {
	let dir = fresh_dir("adr0089-range-discoverability");
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024,
			io: spawn_io_thread(64).unwrap(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);

	// A contiguous span of localities, the shape a packed record covers: one
	// prefix, consecutive per-row suffixes.
	const BASE: u64 = 0x5000;
	const SPAN: u64 = 8;
	for offset in 0..SPAN {
		manager
			.append_durable_with_locality(
				tenant(),
				id(0x9000 + offset),
				Locality::new(BASE + offset),
				hlc(10),
				format!("row-{offset}").as_bytes(),
			)
			.await
			.unwrap();
	}

	let scan = |start: u64, end: u64| {
		let hits = manager.scan_as_of_with_identity_and_locality(
			tenant(),
			Locality::new(start),
			Locality::new(end),
			hlc(20),
		);
		let mut localities: Vec<u64> = hits
			.iter()
			.map(|(_identity, locality, _location)| locality.get())
			.collect();
		let mut identities: Vec<_> = hits.iter().map(|(identity, _, _)| *identity).collect();
		identities.sort();
		let before = identities.len();
		identities.dedup();
		assert_eq!(
			identities.len(),
			before,
			"every row is emitted exactly once for [{start:#x}, {end:#x}]"
		);
		localities.sort_unstable();
		localities
	};

	// Crossing the whole span.
	assert_eq!(
		scan(BASE, BASE + SPAN - 1),
		(BASE..BASE + SPAN).collect::<Vec<_>>()
	);

	// Starting inside the span: the interval begins between two rows, which is
	// exactly the case prefix alignment would not cover.
	assert_eq!(
		scan(BASE + 3, BASE + SPAN - 1),
		(BASE + 3..BASE + SPAN).collect::<Vec<_>>()
	);

	// Ending inside the span.
	assert_eq!(scan(BASE, BASE + 4), (BASE..=BASE + 4).collect::<Vec<_>>());

	// Wholly inside, touching neither end.
	assert_eq!(
		scan(BASE + 2, BASE + 5),
		(BASE + 2..=BASE + 5).collect::<Vec<_>>()
	);

	// A single interior locality resolves to one row, not its neighbours.
	assert_eq!(scan(BASE + 6, BASE + 6), vec![BASE + 6]);

	// Intervals outside the span yield nothing rather than the nearest row.
	assert!(scan(BASE + SPAN, BASE + SPAN + 4).is_empty());
	assert!(scan(BASE - 4, BASE - 1).is_empty());
}

/// A PrimaryKey table whose spread factor is 1, so every row lands in ONE bucket
/// and packs into ONE physical frame. The default spread of 4 scatters three rows
/// across three frames, which is a valid layout but not the shape this test guards.
fn adr0097_colocated_table_descriptor_for_test() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("age", DataType::Int64, true),
	]);
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x97; 16]),
		1,
		"analytics",
		"colocated_events",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::TableSpreadProjection(
			crate::schema::table::TableSpreadLocalityDescriptor {
				spread_factor: 1,
				prefix_bits: crate::schema::table::TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			},
		),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

/// Build one multi-row frame holding `ids`, attributed to `table` so its rows
/// resolve to `VersionLifecycle::PrimaryKey` during compaction. A frame built by
/// `encode_test_multirow_storage_frame` carries no table attribution and would
/// resolve to `AmbiguousTable`, which elides nothing - the plan would then hold
/// every row and never reach the partial-survivor path.
async fn append_attributed_multirow_frame_for_test(
	manager: &Arc<SlabManager>,
	table: &TableSchemaDescriptor,
	ids: &[i64],
	row_hlc: HybridTimestamp,
) -> Vec<Identity> {
	let batch = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("age", DataType::Int64, true),
		])),
		vec![
			Arc::new(Int64Array::from(ids.to_vec())) as Arc<dyn Array>,
			Arc::new(Int64Array::from(
				ids.iter().map(|id| Some(id * 10)).collect::<Vec<_>>(),
			)) as Arc<dyn Array>,
		],
	)
	.unwrap();
	let meta = layout_recluster_meta(table, batch.num_rows());
	let packed =
		pack_record_batch_by_bucket(&meta, table, &batch, WriterNodeId::ZERO, row_hlc).unwrap();
	assert_eq!(
		packed.len(),
		1,
		"the fixture needs ONE physical frame holding every row"
	);
	let entries = storage_multirow_index_entries_from_frame(&packed[0].frame_bytes)
		.unwrap()
		.expect("packed frame is multi-row");
	assert_eq!(entries.len(), ids.len(), "one entry per row of the frame");
	let identities = entries
		.iter()
		.map(|entry| entry.key_identity)
		.collect::<Vec<_>>();
	manager
		.append_durable_multirow_with_locality(tenant(), &packed[0].frame_bytes, &entries)
		.await
		.unwrap();
	identities
}

/// ADR 0097 step 2, phase 1 - the REWRITER.
///
/// A three-row frame with only two survivors used to be refused with
/// `does not yet split multi-row frames`, after the whole working set had been
/// built. It is now rewritten to hold exactly the survivors.
///
/// Four preconditions make this discriminating, and missing any one lets the test
/// pass without entering the branch at all:
///   1. BOTH versions live in sealed slabs the plan touches -
///      `lsm_version_entries_for_plan` skips locations outside `touched_slabs`, so
///      a newer version left in the active slab is invisible to elision.
///   2. The rows carry real table attribution against a registered PrimaryKey
///      table, or `lsm_version_lifecycle_for_entry` returns `AmbiguousTable`, for
///      which `apply_elision` drops nothing.
///   3. Compaction runs through the table-schemas path, which is the only one that
///      resolves that lifecycle.
///   4. The retention floor sits above every row's HLC, because
///      `apply_primary_key_elision` only supersedes versions BELOW the floor.
#[tokio::test]
async fn adr0097_partial_survivor_frame_is_rewritten_before_installation() {
	let dir = fresh_dir("adr0097-partial-survivor-rewrite");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;
	let table = adr0097_colocated_table_descriptor_for_test();

	// Slab A: one physical frame holding k1, k2, k3.
	let identities =
		append_attributed_multirow_frame_for_test(&manager, &table, &[1, 2, 3], hlc(1_000)).await;
	let (k1, k2, k3) = (identities[0], identities[1], identities[2]);
	let slab_a = manager.force_seal_active_for_test().await;

	// Slab B: a newer k2. Sealed, so the plan can see it.
	let newer = append_attributed_multirow_frame_for_test(&manager, &table, &[2], hlc(2_000)).await;
	assert_eq!(newer[0], k2, "the newer row must supersede the same key");
	let slab_b = manager.force_seal_active_for_test().await;
	assert_ne!(
		slab_a, slab_b,
		"the fixture needs two distinct sealed slabs"
	);

	let mut catalog_tables = TableSchemaCatalog::default();
	catalog_tables.upsert(tenant(), table.clone()).unwrap();

	let snapshot = manager.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x0097_0001),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![slab_a, slab_b],
		overlapping_slabs: Vec::new(),
		// Above every row HLC: only versions below the floor are superseded.
		retention_floor_snapshot: hlc(9_000),
		compaction_start_hlc: hlc(9_001),
		dominant_reason: crate::storage::DominantReason::Operator,
	};

	// The plan must actually drop exactly one row, or the rest proves nothing.
	let entries = manager
		.lsm_version_entries_for_plan_with_table_schemas(&plan, &catalog_tables)
		.await
		.unwrap();
	assert_eq!(entries.len(), 4, "three frame rows plus the newer k2");
	let decisions = crate::storage::lsm::apply_elision(entries, plan.retention_floor_snapshot);
	let survivors = crate::storage::lsm::survivor_records(&decisions);
	assert_eq!(
		survivors.len(),
		3,
		"the older k2 is superseded; k1, k3 and the newer k2 survive"
	);
	let survivors_in_frame = survivors
		.iter()
		.filter(|record| record.location.slab_id == slab_a)
		.count();
	assert_eq!(
		survivors_in_frame, 2,
		"the fixture must reproduce the shape it guards: a three-row frame with \
		 two survivors"
	);

	let worker = LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		manager.slab_pin_table(),
	)
	.unwrap();
	let prepared = manager
		.prepare_lsm_compaction_outputs_with_table_schemas(&worker, &plan, &catalog_tables)
		.await
		.unwrap();
	let source_frames = manager
		.read_lsm_single_row_source_frames(&plan)
		.await
		.unwrap();
	let written = manager
		.write_lsm_compaction_temp_outputs_with_table_schemas(
			&plan,
			&prepared,
			&source_frames,
			Some(&catalog_tables),
		)
		.await
		.unwrap();

	// The source is still readable: nothing is destroyed before installation.
	for identity in [k1, k2, k3] {
		assert!(
			manager.lookup(tenant(), identity).is_some(),
			"source rows stay addressable until installation commits"
		);
	}
	let k1_location = manager.lookup(tenant(), k1).expect("k1 is indexed");
	assert_eq!(
		k1_location.slab_id, slab_a,
		"the index still points at the untouched source slab"
	);
	assert!(
		!manager
			.read_with_reference_origin(k1_location, PublishedReferenceOrigin::Compaction)
			.await
			.expect("the original frame is still readable at its source location")
			.is_empty()
	);

	// --- Physical inspection of what was written, not just what resolves. ---
	//
	// Group the written locations by physical frame. A logical lookup would pass
	// even if the rewrite had emitted three one-row frames, or had kept the old
	// k2 in the frame and merely stopped indexing it.
	let mut frames_written: BTreeMap<(SlabId, u64, u32), Vec<RecordLocation>> = BTreeMap::new();
	for run in &written {
		for location in &run.locations {
			frames_written
				.entry((location.slab_id, location.offset, location.length))
				.or_default()
				.push(*location);
		}
	}
	let multi_row_frames = frames_written
		.values()
		.filter(|locations| locations.len() > 1)
		.collect::<Vec<_>>();
	assert_eq!(
		multi_row_frames.len(),
		1,
		"exactly one rewritten multi-row frame: {:?}",
		frames_written
			.iter()
			.map(|(key, locations)| (*key, locations.len()))
			.collect::<Vec<_>>()
	);
	let rewritten_frame = multi_row_frames[0];
	assert_eq!(
		rewritten_frame.len(),
		2,
		"the frame holds exactly k1 and k3"
	);

	let mut indices = rewritten_frame
		.iter()
		.map(|location| location.record_row_index.get())
		.collect::<Vec<_>>();
	indices.sort_unstable();
	assert_eq!(
		indices,
		vec![0, 1],
		"the rewrite renumbers survivors densely from zero"
	);

	// The newer k2 survives, in its own frame; nothing carries the superseded
	// k2's HLC. These read locations only - the temp outputs are not published
	// yet, so their CONTENT is asserted in phase 2, after installation.
	let newer_k2_written = frames_written
		.values()
		.flatten()
		.filter(|location| location.hlc == hlc(2_000))
		.count();
	assert_eq!(newer_k2_written, 1, "exactly one surviving newer k2");
	assert_eq!(
		frames_written
			.values()
			.flatten()
			.filter(|location| location.hlc == hlc(1_000))
			.count(),
		2,
		"only k1 and k3 survive from the original frame's HLC"
	);

	// Digest invariance: the visible domain the outputs publish must equal the
	// visible domain the plan's survivors describe.
	let digest_summary = manager
		.lsm_single_row_bridge_digest_summary(&prepared, &written, &source_frames)
		.expect("the written outputs reproduce the planned visible domain");
	assert_eq!(
		digest_summary.visible_pre, digest_summary.visible_post,
		"a rewrite that preserves survivors is digest-invariant"
	);
	assert!(!written.is_empty(), "the compaction wrote at least one run");
}

/// ADR 0097 step 2, phase 2 - PUBLICATION and RECOVERY.
///
/// Phase 1 proves the rewriter emits the right shape into unpublished temp
/// outputs. This proves the rewritten frame survives installation and a manager
/// reopen: the same rows resolve, they resolve into the INSTALLED outputs rather
/// than the retired sources, and every per-row field is intact on the way back
/// off disk. Split from phase 1 so a failure localizes to one side or the other.
#[tokio::test]
async fn adr0097_rewritten_partial_survivor_frame_survives_install_and_reopen() {
	let dir = fresh_dir("adr0097-partial-survivor-install");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let table = adr0097_colocated_table_descriptor_for_test();

	let identities =
		append_attributed_multirow_frame_for_test(&manager, &table, &[1, 2, 3], hlc(1_000)).await;
	let (k1, k2, k3) = (identities[0], identities[1], identities[2]);
	let slab_a = manager.force_seal_active_for_test().await;
	append_attributed_multirow_frame_for_test(&manager, &table, &[2], hlc(2_000)).await;
	let slab_b = manager.force_seal_active_for_test().await;

	let mut catalog_tables = TableSchemaCatalog::default();
	catalog_tables.upsert(tenant(), table.clone()).unwrap();

	let snapshot = manager.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x0097_0002),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![slab_a, slab_b],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(9_000),
		compaction_start_hlc: hlc(9_001),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let worker = LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		manager.slab_pin_table(),
	)
	.unwrap();
	let prepared = manager
		.prepare_lsm_compaction_outputs_with_table_schemas(&worker, &plan, &catalog_tables)
		.await
		.unwrap();
	let source_frames = manager
		.read_lsm_single_row_source_frames(&plan)
		.await
		.unwrap();
	let written = manager
		.write_lsm_compaction_temp_outputs_with_table_schemas(
			&plan,
			&prepared,
			&source_frames,
			Some(&catalog_tables),
		)
		.await
		.unwrap();
	// Capture the survivors' per-row fields BEFORE the sources are retired, so the
	// post-reopen comparison is against what was actually stored rather than
	// against values re-derived by the same rules the rewrite itself uses.
	let before_rewrite = {
		let location = manager
			.lookup(tenant(), k1)
			.expect("k1 indexed pre-install");
		let frame = manager
			.read_with_reference_origin(location, PublishedReferenceOrigin::Compaction)
			.await
			.expect("source frame readable pre-install");
		storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("source frame is multi-row")
	};
	assert_eq!(
		before_rewrite.len(),
		3,
		"the source frame held all three rows"
	);

	let digest_summary = manager
		.lsm_single_row_bridge_digest_summary(&prepared, &written, &source_frames)
		.unwrap();
	let commit = worker.build_written_compaction_commit(&plan, &written, digest_summary);
	let installed_slabs = written.iter().map(|run| run.slab_id).collect::<Vec<_>>();
	manager
		.install_lsm_compaction_temp_outputs(&plan, &prepared, &written, &commit)
		.await
		.expect("the rewritten outputs install");

	// Reopen from disk: nothing below is served from in-memory state.
	drop(manager);
	let reopened = open_manager_at_dir(dir.to_path_buf(), io, crypto).await;

	for identity in [k1, k3] {
		let location = reopened
			.lookup(tenant(), identity)
			.unwrap_or_else(|| panic!("row still resolves after reopen"));
		assert!(
			installed_slabs.contains(&location.slab_id),
			"the row resolves into an INSTALLED output, not a retired source: {:?}",
			location.slab_id
		);
	}
	let k2_location = reopened.lookup(tenant(), k2).expect("k2 still resolves");
	assert_eq!(
		k2_location.hlc,
		hlc(2_000),
		"k2 resolves to the newer version; the superseded one is gone"
	);
	assert!(
		installed_slabs.contains(&k2_location.slab_id),
		"the newer k2 also resolves into an installed output"
	);

	// The rewritten frame recovered from disk holds exactly k1 and k3, densely
	// numbered, with every per-row field intact.
	let k1_location = reopened.lookup(tenant(), k1).unwrap();
	let k3_location = reopened.lookup(tenant(), k3).unwrap();
	assert_eq!(
		(k1_location.slab_id, k1_location.offset),
		(k3_location.slab_id, k3_location.offset),
		"k1 and k3 recovered into ONE physical frame"
	);
	let mut recovered_indices = vec![
		k1_location.record_row_index.get(),
		k3_location.record_row_index.get(),
	];
	recovered_indices.sort_unstable();
	assert_eq!(
		recovered_indices,
		vec![0, 1],
		"dense indices survive reopen"
	);

	let frame = reopened
		.read_with_reference_origin(k1_location, PublishedReferenceOrigin::Compaction)
		.await
		.expect("the installed frame reads back");
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("the installed frame is still multi-row");
	assert_eq!(entries.len(), 2, "exactly the two survivors");
	assert!(
		!entries.iter().any(|entry| entry.key_identity == k2),
		"no old k2 entry anywhere in the recovered frame"
	);
	let batch = crate::storage::multirow::decode_batch(&frame).unwrap();
	for identity in [k1, k3] {
		let entry = entries
			.iter()
			.find(|entry| entry.key_identity == identity)
			.expect("survivor present");
		let before = before_rewrite
			.iter()
			.find(|candidate| candidate.key_identity == identity)
			.expect("the source frame held this survivor");
		assert_eq!(
			entry.row_hlc, before.row_hlc,
			"HLC preserved through install"
		);
		assert_eq!(
			entry.is_tombstone, before.is_tombstone,
			"tombstone flag preserved"
		);
		assert_eq!(
			entry.routing_locality, before.routing_locality,
			"routing locality preserved"
		);
		assert_eq!(entry.partition, before.partition, "partition preserved");
		assert_eq!(
			storage_row_table_object_id_at(&batch, entry.record_row_index).unwrap(),
			Some(table.table_object_id),
			"per-row table attribution preserved through install and reopen"
		);
	}
}

/// ADR 0097: a plan naming several rows of one physical frame must read and
/// retain that frame ONCE. The per-row read this replaces retained a full copy
/// of the containing frame for every logical row it held, which is how a 2.2 GiB
/// slab became a 21 GiB working set before the run reported an unsupported plan.
///
/// The assertion is on buffer identity rather than on wall time or RSS: under the
/// per-row implementation every entry owned a distinct `Vec`, so the distinct
/// buffer count equalled the row count and this fails.
#[tokio::test]
async fn adr0097_source_frames_are_read_once_per_physical_frame() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let key_a = id(0x9A);
	let key_b = id(0x9B);
	let key_c = id(0x9C);
	let loc = Locality::new(11);
	let frame = encode_test_multirow_storage_frame(&[
		(key_a, loc, hlc(10), 100),
		(key_b, loc, hlc(11), 101),
		(key_c, loc, hlc(12), 102),
	]);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("multi-row entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let source_slab_id = force_seal_active(&mgr).await;

	let snapshot = mgr.compaction_manifest_snapshot();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(0x0097),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![source_slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(5),
		compaction_start_hlc: hlc(200),
		dominant_reason: crate::storage::DominantReason::Operator,
	};

	let frames = mgr.read_lsm_single_row_source_frames(&plan).await.unwrap();

	let physical: std::collections::HashSet<_> = frames
		.iter()
		.map(|source| {
			(
				source.entry.location.slab_id,
				source.entry.location.offset,
				source.entry.location.length,
			)
		})
		.collect();
	assert!(
		frames.len() > physical.len(),
		"the fixture must reproduce the shape it guards: {} rows over {} frames",
		frames.len(),
		physical.len()
	);

	let buffers: std::collections::HashSet<_> = frames
		.iter()
		.map(|source| std::sync::Arc::as_ptr(&source.frame))
		.collect();
	assert_eq!(
		buffers.len(),
		physical.len(),
		"one retained buffer per physical frame, not per row"
	);

	// Sharing must not change what any row sees.
	for source in &frames {
		let decoded = storage_multirow_index_entries_from_frame(&source.frame)
			.unwrap()
			.expect("shared frame still decodes as multi-row");
		assert!(
			decoded.iter().any(
				|entry| super::lsm_driver::lsm_multirow_index_entry_matches_version(
					entry,
					&source.entry
				)
			),
			"every row still resolves inside the frame it shares"
		);
	}
}

/// ADR 0090 regression control: compaction re-homes bytes, never placement.
/// The defect this pins: relocated locations built by struct-update from the
/// replacement slab's base location silently downgraded partitioned rows to
/// raw-locality, so the next read's placement cross-check refused the record —
/// surfacing as a dropped stream ("early eof") on the daemon path.
#[tokio::test]
async fn adr0090_compaction_preserves_partitioned_placement() {
	use crate::schema::table::PlacementSemantics;

	let dir = fresh_dir("adr0090-compaction-placement");
	let io = spawn_io_thread(64).unwrap();
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024,
			io: io.clone(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);

	// A partitioned multirow record plus an unrelated tombstoned row, sealed
	// together so reclaim compacts the shared slab.
	let doomed_key = id(0x1010);
	manager
		.append_durable(tenant(), doomed_key, hlc(10), b"drop-me")
		.await
		.unwrap();
	let partitioned_key = id(0x2020);
	let partition = crate::schema::PartitionId::new(0x00AB_0000_0000_0000);
	let batch = RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
		vec![Arc::new(arrow::array::Int64Array::from(vec![7_i64]))],
	)
	.unwrap();
	let source = vec![crate::storage::multirow::MultiRowSourceRow {
		source_row_index: 0,
		key_identity: partitioned_key,
		routing_locality: Locality::new(partition.get() | 0x1234),
		row_hlc: hlc(20),
		is_tombstone: false,
		partition: Some(partition),
		table_object_id: None,
	}];
	let encoded =
		crate::storage::multirow::encode_multirow_from_survivors(&batch, &source).unwrap();
	let entries = crate::storage::multirow::storage_index_entries_from_batch(
		&crate::storage::multirow::decode_batch(&encoded.frame).unwrap(),
	)
	.unwrap();
	manager
		.append_durable_multirow_with_locality(tenant(), &encoded.frame, &entries)
		.await
		.unwrap();
	let before = manager.lookup(tenant(), partitioned_key).unwrap();
	assert_eq!(before.placement, PlacementSemantics::Partitioned);
	assert_eq!(before.partition, Some(partition));

	force_seal_active(&manager).await;
	manager
		.append_durable(tenant(), id(0x3030), hlc(30), b"active-row")
		.await
		.unwrap();

	let target = Locality::from(doomed_key).get();
	let report = manager
		.reclaim_tombstoned_ranges(
			&[MigratedRangeTombstone {
				migration_id: 1,
				range_start: target,
				range_end: target,
				to_epoch: 2,
				completed_by: NodeId {
					id: 7,
					_reserved: 0,
				},
				completed_at_hlc: hlc(1),
				migration_hlc_floor: hlc(0),
			}],
			&empty_compaction_catalogs().0,
			&empty_compaction_catalogs().1,
			1,
		)
		.await
		.unwrap();
	assert_eq!(
		report.compacted_slabs, 1,
		"premise: the shared slab compacted"
	);

	// The partitioned row survived compaction at a NEW location, and its
	// placement survived with it.
	let after = manager.lookup(tenant(), partitioned_key).unwrap();
	assert_ne!(
		(after.slab_id, after.offset),
		(before.slab_id, before.offset),
		"premise: compaction relocated the row"
	);
	assert_eq!(
		after.placement,
		PlacementSemantics::Partitioned,
		"compaction must not downgrade placement"
	);
	assert_eq!(after.partition, Some(partition));
}

#[tokio::test]
async fn repro_drop_reclaim_keeps_pinned_slab_backing_readable() {
	// Discriminating repro for the 2026-08-24 missing-slab incident triage:
	// a compaction plan pins every touched slab before reading
	// (lsm_driver acquire_plan_pins); the invariant under test is that
	// table-drop reclaim retiring a fully-dead slab must not remove the
	// backing while such a pin is held. Failure here means the reclaim
	// retirement path bypasses the pin/delete protocol and is a causal
	// candidate for the live warnings.
	let (_mgr_dir, mgr) = fresh_manager().await;
	let target = append_only_retention_descriptor(TableObjectId::new([0x77; 16]));
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), target.clone()).unwrap();

	let key = id(0x771);
	let frame = encode_test_append_only_storage_frame(
		target.table_object_id,
		&[(key, Locality::new(10), hlc(10), 100, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("target table entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();

	let location = mgr.lookup(tenant(), key).expect("target row location");
	let pinned_slab = location.slab_id;
	let sanity_read = mgr.read(location).await;
	assert!(sanity_read.is_ok(), "pre-reclaim read must succeed");

	let pins = mgr.slab_pin_table();
	let pin_guard = pins.acquire(&[pinned_slab]).expect("pin acquisition");

	let report = mgr
		.reclaim_table_drop_storage(
			tenant(),
			&target,
			&schemas,
			&SecondaryIndexCatalog::default(),
			usize::MAX,
		)
		.await
		.unwrap();
	// The whole rewrite defers while the pin is held: index, records, and
	// backing all remain until the deferred pass. (The drop tombstone —
	// admission authority since ADR 0095 — keeps the lingering entries
	// unreachable from any table surface meanwhile.)
	assert_eq!(report.compacted_slabs, 0, "pinned slab must be deferred");
	assert_eq!(report.reclaimed_records, 0);
	assert!(mgr.lookup(tenant(), key).is_some());

	// The discriminating assertion: while the compaction pin is held, the
	// slab backing must remain readable at the pre-reclaim location.
	let read_while_pinned = mgr.read(location).await;
	assert!(
		read_while_pinned.is_ok(),
		"pinned slab {pinned_slab:?} backing became unreadable during drop \
		 reclaim; the retirement path bypasses the compaction pin protocol: {:?}",
		read_while_pinned.err()
	);
	// The deferred removal must not have marked the slab delete-pending
	// behind the live pin: an aborted gate leaves the slab pinnable.
	assert!(!pins.is_delete_pending(pinned_slab));
	assert!(pins.acquire(&[pinned_slab]).is_ok());

	// Retirement resumes after release: the next reclaim pass removes the
	// deferred backing.
	drop(pin_guard);
	let resumed = mgr
		.reclaim_table_drop_storage(
			tenant(),
			&target,
			&schemas,
			&SecondaryIndexCatalog::default(),
			usize::MAX,
		)
		.await
		.unwrap();
	assert_eq!(
		resumed.compacted_slabs, 1,
		"deferred slab reclaimed after pin release"
	);
	assert!(
		mgr.read(location).await.is_err(),
		"backing must be gone once no pin needs it"
	);
	assert!(
		!pins.is_delete_pending(pinned_slab),
		"mark cleared after removal"
	);
}

#[tokio::test]
async fn retention_prune_defers_pinned_slab_backing_until_release() {
	// The retention twin of the drop-reclaim pin contract: both routes
	// funnel through compact_sealed_slab_records, and both must honor a
	// compaction plan's pins before removing a backing.
	let (_mgr_dir, mgr) = fresh_manager().await;
	let descriptor = append_only_retention_descriptor(TableObjectId::new([0x79; 16]));
	let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
	let old_key = id(0x7901);
	let frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(old_key, Locality::new(11), hlc(30_000), 100, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("append-only storage entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let location = mgr.lookup(tenant(), old_key).expect("row location");
	let pinned_slab = force_seal_active(&mgr).await;
	assert_eq!(pinned_slab, location.slab_id);

	let pins = mgr.slab_pin_table();
	let pin_guard = pins.acquire(&[pinned_slab]).expect("pin acquisition");
	let deferred = mgr
		.prune_versions_for_retention(
			MvccRetentionPolicy::Disabled,
			&lifecycle,
			&schemas,
			&SecondaryIndexCatalog::default(),
			100_000,
			usize::MAX,
		)
		.await
		.unwrap();
	assert_eq!(deferred.compacted_slabs, 0, "pinned slab must be deferred");
	assert!(
		mgr.read(location).await.is_ok(),
		"pinned slab backing must remain readable through retention pruning"
	);

	drop(pin_guard);
	let resumed = mgr
		.prune_versions_for_retention(
			MvccRetentionPolicy::Disabled,
			&lifecycle,
			&schemas,
			&SecondaryIndexCatalog::default(),
			100_000,
			usize::MAX,
		)
		.await
		.unwrap();
	assert_eq!(
		resumed.compacted_slabs, 1,
		"retirement resumes after pin release"
	);
	assert!(mgr.lookup(tenant(), old_key).is_none());
}

#[tokio::test]
async fn missing_compaction_source_read_records_compaction_origin() {
	// A compaction source read of a missing backing must retain
	// Compaction-origin integrity evidence, not masquerade as a client
	// Read — the generic origin conflated the two during the 2026-08-24
	// incident triage.
	let (_mgr_dir, mgr) = fresh_manager().await;
	let descriptor = append_only_retention_descriptor(TableObjectId::new([0x7a; 16]));
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), descriptor.clone()).unwrap();
	let key = id(0x7a01);
	let frame = encode_test_append_only_storage_frame(
		descriptor.table_object_id,
		&[(key, Locality::new(12), hlc(10), 100, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let location = mgr.lookup(tenant(), key).expect("row location");
	let sealed_slab = force_seal_active(&mgr).await;
	assert_eq!(sealed_slab, location.slab_id);
	// Simulate the incident-class inconsistency: the backing vanishes while
	// the published reference remains.
	let removed = {
		let state = mgr.state.lock().await;
		state.sealed.remove(&sealed_slab)
	};
	assert!(removed.is_some(), "sealed backing removed for the probe");

	let err = mgr
		.read_with_reference_origin(location, PublishedReferenceOrigin::Compaction)
		.await
		.expect_err("missing backing must fail");
	assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
	let failure = mgr
		.table_integrity_failure(tenant(), descriptor.table_object_id)
		.expect("integrity failure retained for referencing table");
	assert_eq!(failure.origin, PublishedReferenceOrigin::Compaction);
	assert_eq!(failure.missing_slab_id, sealed_slab);
}

#[tokio::test]
async fn maintenance_rewrite_publishes_under_reader_pin_and_sweep_defers_disposal() {
	// The regression control for the pin-fix follow-up: a reader pin held
	// across a with-survivors maintenance rewrite must never be refused —
	// the rewrite publishes, both old and new locations stay readable, and
	// only Phase D (the GC sweep) defers to the pin, collecting after
	// release.
	let (_mgr_dir, mgr) = fresh_manager().await;
	let target = append_only_retention_descriptor(TableObjectId::new([0x81; 16]));
	let other =
		append_only_retention_descriptor_named(TableObjectId::new([0x82; 16]), "pinned_reader");
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), target.clone()).unwrap();
	schemas.upsert(tenant(), other.clone()).unwrap();

	let target_key = id(0x811);
	let other_key = id(0x821);
	let target_frame = encode_test_append_only_storage_frame(
		target.table_object_id,
		&[(target_key, Locality::new(10), hlc(10), 100, false)],
	);
	let target_entries = storage_multirow_index_entries_from_frame(&target_frame)
		.unwrap()
		.expect("target entries");
	mgr.append_durable_multirow_with_locality(tenant(), &target_frame, &target_entries)
		.await
		.unwrap();
	let other_frame = encode_test_append_only_storage_frame(
		other.table_object_id,
		&[(other_key, Locality::new(20), hlc(20), 200, false)],
	);
	let other_entries = storage_multirow_index_entries_from_frame(&other_frame)
		.unwrap()
		.expect("other entries");
	mgr.append_durable_multirow_with_locality(tenant(), &other_frame, &other_entries)
		.await
		.unwrap();

	let target_old = mgr.lookup(tenant(), target_key).expect("target location");
	let other_old = mgr.lookup(tenant(), other_key).expect("survivor location");
	let old_slab = other_old.slab_id;
	assert_eq!(old_slab, target_old.slab_id, "co-resident fixture");

	let pins = mgr.slab_pin_table();
	let reader_pin = pins.acquire(&[old_slab]).expect("reader pin");

	let report = mgr
		.reclaim_table_drop_storage(
			tenant(),
			&target,
			&schemas,
			&SecondaryIndexCatalog::default(),
			usize::MAX,
		)
		.await
		.expect("with-survivors rewrite must publish under a reader pin");
	assert_eq!(report.compacted_slabs, 1);
	assert_eq!(report.reclaimed_records, 1);

	// Reads stay whole throughout: the survivor via its relocated live
	// index entry, its stale pre-D3 location via relocation forwarding, and
	// the dropped row's old location at the byte level — the retained old
	// backing.
	let other_new = mgr.lookup(tenant(), other_key).expect("survivor relocated");
	assert_ne!(other_new.slab_id, old_slab);
	assert_eq!(mgr.read(other_new).await.unwrap().len() > 0, true);
	assert!(mgr.read(other_old).await.is_ok(), "stale location forwards");
	assert!(
		mgr.read(target_old).await.is_ok(),
		"old backing readable under the reader pin"
	);

	let manifest = mgr.compaction_manifest_snapshot();
	assert!(matches!(
		manifest.run(old_slab).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors.as_slice() == [other_new.slab_id]
	));
	assert!(
		manifest
			.run(other_new.slab_id)
			.is_some_and(|run| run.state.is_live())
	);

	// Phase D defers to the reader pin, then collects after release.
	let gc = LsmGcWorker::new(
		crate::storage::lsm::LsmConfig::default(),
		mgr.slab_pin_table(),
	)
	.unwrap();
	let deferred = mgr.sweep_lsm_delete_pending_once(&gc).await.unwrap();
	assert!(
		!deferred.contains(&old_slab),
		"sweep must defer disposal while the reader pin lives"
	);
	assert!(
		mgr.read_sealed_raw(old_slab).await.is_ok(),
		"old backing physically present through the deferred sweep"
	);

	drop(reader_pin);
	let collected = mgr.sweep_lsm_delete_pending_once(&gc).await.unwrap();
	assert!(
		collected.contains(&old_slab),
		"sweep collects the retired backing after pin release"
	);
	assert!(mgr.read_sealed_raw(old_slab).await.is_err());
}

#[tokio::test]
async fn maintenance_publication_refuses_relocation_survivor_mismatch() {
	// The publication validation's negative arm: a relocation set that does
	// not equal the declared survivor set is refused before any durable or
	// in-memory mutation.
	let (_mgr_dir, mgr) = fresh_manager().await;
	let table = append_only_retention_descriptor(TableObjectId::new([0x83; 16]));
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), table.clone()).unwrap();
	let key = id(0x831);
	let frame = encode_test_append_only_storage_frame(
		table.table_object_id,
		&[(key, Locality::new(30), hlc(30), 300, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let location = mgr.lookup(tenant(), key).expect("row location");
	let old_slab = force_seal_active(&mgr).await;
	assert_eq!(old_slab, location.slab_id);
	// The test seal helper does not register an L0 run; use the same bridge
	// the maintenance path uses so the manifest has a Live source to guard.
	mgr.ensure_live_lsm_run_for_maintenance(old_slab, 1024);

	let records = vec![CompactionRecord {
		tenant_id: tenant(),
		key_identity: key,
		routing_locality: Locality::new(30),
		old_location: location,
		reclaim: false,
		retention_ambiguous: false,
		retention_lag_seconds: 0,
	}];
	let replacement_id = mgr.reserve_slab_id().await;
	let staged_path = maintenance_staged_slab_path(&mgr.config.dir, old_slab, replacement_id);
	let replacement = Arc::new(
		LocalSlabStore::open(
			&staged_path,
			replacement_id,
			mgr.file_ops.clone(),
			mgr.config.group_commit.clone(),
		)
		.await
		.unwrap(),
	);

	// One declared survivor, zero relocations: the sets differ.
	let err = mgr
		.finish_compaction_with_replacement(
			old_slab,
			replacement_id,
			replacement,
			&records,
			&[],
			&[],
			&staged_path,
		)
		.await
		.expect_err("mismatched relocation set must be refused");
	assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

	// Nothing changed: the run is still Live, the row still reads.
	assert!(
		mgr.compaction_manifest_snapshot()
			.run(old_slab)
			.is_some_and(|run| run.state.is_live())
	);
	assert!(mgr.read(location).await.is_ok());
	let _ = unlink_local_file(mgr.file_ops.as_ref(), &staged_path).await;
}

#[tokio::test]
async fn maintenance_manifest_reapply_is_a_no_op() {
	// Idempotent re-apply: the same (old, replacement) publication applied
	// twice leaves the manifest byte-identical; a conflicting second
	// publication of a retired source is refused.
	let (_mgr_dir, mgr) = fresh_manager().await;
	let table = append_only_retention_descriptor(TableObjectId::new([0x84; 16]));
	let mut schemas = TableSchemaCatalog::default();
	schemas.upsert(tenant(), table.clone()).unwrap();
	let key = id(0x841);
	let frame = encode_test_append_only_storage_frame(
		table.table_object_id,
		&[(key, Locality::new(40), hlc(40), 400, false)],
	);
	let entries = storage_multirow_index_entries_from_frame(&frame)
		.unwrap()
		.expect("entries");
	mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
		.await
		.unwrap();
	let old_slab = force_seal_active(&mgr).await;
	mgr.ensure_live_lsm_run_for_maintenance(old_slab, 1024);
	let old_run = mgr
		.compaction_manifest_snapshot()
		.run(old_slab)
		.cloned()
		.expect("bridged sealed slab registers a Live run");

	let replacement = LsmRunSummary::new(
		SlabId::new(0x8400),
		old_run.compaction_level,
		old_run.min_row_hlc,
		old_run.max_row_hlc,
		old_run.size_bytes,
		old_run.row_count,
		old_run.manifest_epoch,
	);
	let first = mgr
		.apply_maintenance_manifest_publication(old_slab, Some(replacement.clone()))
		.unwrap();
	let second = mgr
		.apply_maintenance_manifest_publication(old_slab, Some(replacement.clone()))
		.unwrap();
	assert_eq!(
		first.runs(),
		second.runs(),
		"identical re-apply must not change the manifest"
	);

	let conflicting = LsmRunSummary::new(
		SlabId::new(0x8401),
		old_run.compaction_level,
		old_run.min_row_hlc,
		old_run.max_row_hlc,
		old_run.size_bytes,
		old_run.row_count,
		old_run.manifest_epoch,
	);
	let err = mgr
		.apply_maintenance_manifest_publication(old_slab, Some(conflicting))
		.expect_err("a retired source must refuse a different publication");
	assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

// ============================================================
//  ADR 0097 decision 4: every compaction tick funnels through the
//  maintenance policy, and the policy is hydrated from the index
//  the manager serves with.
// ============================================================

async fn open_policed_manager(
	dir: std::path::PathBuf,
	io: crate::storage::io_uring::IoRingHandle,
) -> Arc<SlabManager> {
	Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir,
			// Every append rotates, so five appends are five L0 runs.
			max_slab_bytes: 1,
			io,
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	)
}

async fn l0_pressure(manager: &SlabManager) {
	for n in 0..5 {
		manager
			.append(tenant(), id(0x9700 + n), hlc(n + 1), b"row")
			.await
			.unwrap();
	}
	manager.force_seal_active_for_test().await;
}

#[tokio::test]
async fn a_compaction_tick_is_policed_and_its_success_is_recorded() {
	let dir = fresh_dir("adr0097-policed-tick");
	let io = spawn_io_thread(16).unwrap();
	let manager = open_policed_manager(dir.to_path_buf(), io).await;
	let policy = manager.maintenance_policy();
	assert!(policy.is_enabled(), "{:?}", policy.diagnostic());
	l0_pressure(&manager).await;
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let metrics = crate::metrics::global();
	let ok_before = metrics
		.maintenance_attempts(crate::storage::maintenance_policy::MaintenanceAttemptResult::Ok);

	let outcome = manager
		.run_lsm_compaction_tick(&worker, hlc(20_000))
		.await
		.unwrap()
		.expect("five L0 runs produce a compaction");
	assert!(outcome.rows_out > 0);

	assert_eq!(
		metrics
			.maintenance_attempts(crate::storage::maintenance_policy::MaintenanceAttemptResult::Ok)
			- ok_before,
		1,
		"the tick went through the policy, not around it"
	);
	let report = policy.report();
	assert!(report.enabled);
	assert!(report.in_flight.is_empty(), "the reservation was released");
	assert!(report.receipts.is_empty());
	assert_eq!(policy.gauge_snapshot_for_test(), (1, 0));
}

#[tokio::test]
async fn a_quarantined_plan_is_skipped_by_the_tick_until_requeued() {
	let dir = fresh_dir("adr0097-quarantined-plan");
	let io = spawn_io_thread(16).unwrap();
	let worker_config = LsmConfig::default();

	// Learn which plan the picker chooses for this pressure.
	let touched = {
		let manager = open_policed_manager(dir.to_path_buf(), io.clone()).await;
		l0_pressure(&manager).await;
		let worker = LsmCompactionWorker::new(worker_config, manager.slab_pin_table()).unwrap();
		manager
			.pick_lsm_compaction_plan(&worker, hlc(20_000))
			.expect("pressure yields a plan")
			.touched_slabs()
	};

	// Quarantine exactly that work, durably, while the manager is closed.
	{
		let index = PersistentLocalIndex::open(persistent_index_path(&dir))
			.await
			.unwrap();
		let key = crate::storage::maintenance_policy::MaintenanceWorkKey::new(
			crate::storage::maintenance_policy::MaintenanceOperation::LsmCompaction,
			touched.iter().copied(),
			crate::storage::maintenance_policy::DeterministicOrigin::LsmInconsistentSourceFrames,
		)
		.unwrap();
		let receipt = {
			use crate::storage::maintenance_policy::DeterministicReceipt;
			let a = DeterministicReceipt::after_failure(None, 1_000, "a").unwrap();
			let b = DeterministicReceipt::after_failure(Some(&a), 2_000, "b").unwrap();
			DeterministicReceipt::after_failure(Some(&b), 3_000, "c").unwrap()
		};
		assert!(receipt.is_quarantined());
		index
			.put_maintenance_receipt(key.encode(), receipt.encode())
			.await
			.unwrap();
	}

	let manager = open_policed_manager(dir.to_path_buf(), io).await;
	let policy = manager.maintenance_policy();
	assert!(policy.is_enabled(), "{:?}", policy.diagnostic());
	assert_eq!(policy.report().quarantined().count(), 1);
	let worker = LsmCompactionWorker::new(worker_config, manager.slab_pin_table()).unwrap();

	// The unfiltered picker still wants exactly this work, so the receipt - and
	// nothing else - is what holds it back.
	let unfiltered = crate::storage::lsm::pick_compaction(
		&manager.compaction_manifest_snapshot(),
		&worker_config,
		hlc(20_000),
	)
	.expect("the pressure survived the reopen");
	assert_eq!(unfiltered.touched_slabs(), touched);
	assert!(
		manager
			.pick_lsm_compaction_plan(&worker, hlc(20_000))
			.is_none()
	);
	assert!(
		manager
			.run_lsm_compaction_tick(&worker, hlc(20_000))
			.await
			.unwrap()
			.is_none(),
		"a quarantined plan is not work this tick"
	);

	// Requeue readmits it, and the success clears the record.
	let expected = policy
		.quarantine_fingerprint_for_test(
			crate::storage::maintenance_policy::MaintenanceOperation::LsmCompaction,
			&touched,
		)
		.expect("the report carries the quarantine fingerprint");
	assert_eq!(
		policy
			.requeue(
				crate::storage::maintenance_policy::MaintenanceOperation::LsmCompaction,
				&touched,
				expected,
			)
			.await,
		Ok(1)
	);
	let outcome = manager
		.run_lsm_compaction_tick(&worker, hlc(20_001))
		.await
		.unwrap()
		.expect("requeued work runs");
	assert!(outcome.rows_out > 0);
	assert!(policy.report().receipts.is_empty());
	assert_eq!(policy.gauge_snapshot_for_test(), (1, 0));
}

#[tokio::test]
async fn a_manager_with_unreadable_receipt_history_serves_but_admits_no_maintenance() {
	let dir = fresh_dir("adr0097-unreadable-history");
	let io = spawn_io_thread(16).unwrap();
	{
		let manager = open_policed_manager(dir.to_path_buf(), io.clone()).await;
		manager
			.append(tenant(), id(0x9800), hlc(1), b"served")
			.await
			.unwrap();
		manager.force_seal_active_for_test().await;
	}
	{
		let index = PersistentLocalIndex::open(persistent_index_path(&dir))
			.await
			.unwrap();
		// A well-formed key over an undecodable value.
		index
			.put_maintenance_receipt(vec![0, 11, 0, 0, 0, 0, 0, 0, 0, 5], vec![0xFF])
			.await
			.unwrap();
	}

	let manager = open_policed_manager(dir.to_path_buf(), io).await;
	let policy = manager.maintenance_policy();
	assert!(!policy.is_enabled());
	assert!(
		policy.diagnostic().unwrap().contains("undecodable"),
		"{:?}",
		policy.diagnostic()
	);
	assert_eq!(policy.gauge_snapshot_for_test(), (0, 0));

	// Serving is intact.
	let location = manager
		.lookup(tenant(), id(0x9800))
		.expect("the row is still indexed");
	assert_eq!(manager.read(location).await.unwrap(), b"served");

	// Maintenance is not: pressure produces no plan.
	l0_pressure(&manager).await;
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	assert!(
		manager
			.pick_lsm_compaction_plan(&worker, hlc(20_000))
			.is_none()
	);
	assert!(
		manager
			.run_lsm_compaction_tick(&worker, hlc(20_000))
			.await
			.unwrap()
			.is_none()
	);
}

/// ADR 0097 decision 4: tombstone reclaim is a maintenance rewrite loop like the
/// other three, and it goes through the policy. A disabled policy admits no
/// reclaim; the same store, reopened with the policy enabled, reclaims the slab -
/// so it is the policy, not the fixture, that held it back.
#[tokio::test]
async fn tombstone_reclaim_is_policed() {
	let dir = fresh_dir("adr0097-policed-tombstone-reclaim");
	let io = spawn_io_thread(64).unwrap();
	let open = |dir: std::path::PathBuf, io: crate::storage::io_uring::IoRingHandle| async move {
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: dir.to_path_buf(),
				max_slab_bytes: 1024 * 1024,
				io,
				l2: None,
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		)
	};
	let removed_key = id(0x1010);
	let survivor_key = id(0x2020);
	let tombstone = |target: u64| MigratedRangeTombstone {
		migration_id: 1,
		range_start: target,
		range_end: target,
		to_epoch: 2,
		completed_by: NodeId {
			id: 7,
			_reserved: 0,
		},
		completed_at_hlc: hlc(1),
		migration_hlc_floor: hlc(0),
	};
	let target = Locality::from(removed_key).get();

	let manager = open(dir.to_path_buf(), io.clone()).await;
	manager
		.append_durable(tenant(), removed_key, hlc(10), b"drop-me")
		.await
		.unwrap();
	manager
		.append_durable(tenant(), survivor_key, hlc(20), b"keep-me")
		.await
		.unwrap();
	force_seal_active(&manager).await;
	let policy = manager.maintenance_policy();
	assert!(policy.is_enabled(), "{:?}", policy.diagnostic());
	policy.disable("test: no maintenance admitted");
	let (schemas, indexes) = empty_compaction_catalogs();
	let report = manager
		.reclaim_tombstoned_ranges(&[tombstone(target)], &schemas, &indexes, 1)
		.await
		.unwrap();
	assert_eq!(
		report.compacted_slabs, 0,
		"a disabled policy admits no reclaim"
	);
	assert!(
		manager.lookup(tenant(), removed_key).is_some(),
		"the tombstoned row was not reclaimed around the policy"
	);
	drop(manager);

	// Control arm: the same store, reopened, reclaims once the policy admits.
	let reopened = open(dir.to_path_buf(), io).await;
	assert!(reopened.maintenance_policy().is_enabled());
	let report = reopened
		.reclaim_tombstoned_ranges(&[tombstone(target)], &schemas, &indexes, 1)
		.await
		.unwrap();
	assert_eq!(report.compacted_slabs, 1);
	assert!(reopened.lookup(tenant(), removed_key).is_none());
	assert_eq!(
		reopened
			.read(reopened.lookup(tenant(), survivor_key).unwrap())
			.await
			.unwrap(),
		b"keep-me"
	);
	assert!(
		reopened.maintenance_policy().report().in_flight.is_empty(),
		"the reservation was released"
	);
}

/// ADR 0098 decision 1: with nothing prunable, a retention pass with
/// `max_slabs = 1` attempts exactly one slab, the next pass the next slab, and a
/// pass after every slab is cooling attempts none. A rewrite-only budget leaves
/// three cooldowns after the first pass.
#[tokio::test]
async fn retention_pass_budgets_attempts_and_walks_the_sealed_slabs() {
	use crate::storage::maintenance_policy::MaintenanceOperation;

	let (_mgr_dir, mgr) = fresh_manager().await;
	let mut sealed = Vec::new();
	for n in 0..3_u64 {
		mgr.append_durable(tenant(), id(0x9800 + n), hlc(10 + n), b"clean-row")
			.await
			.unwrap();
		sealed.push(force_seal_active(&mgr).await);
	}
	let (schemas, indexes) = empty_compaction_catalogs();
	let lifecycle = TableLifecycleCatalog::default();
	let policy = mgr.maintenance_policy();
	assert!(policy.is_enabled(), "{:?}", policy.diagnostic());

	let mut expected = Vec::new();
	for (pass, slab_id) in sealed.iter().enumerate() {
		let report = mgr
			.prune_versions_for_retention(
				MvccRetentionPolicy::Disabled,
				&lifecycle,
				&schemas,
				&indexes,
				100_000,
				1,
			)
			.await
			.unwrap();
		assert_eq!(report.compacted_slabs, 0, "pass {pass} rewrites nothing");
		expected.push((MaintenanceOperation::RetentionPrune, vec![*slab_id]));
		let cooling = policy
			.neutral_cooldowns_for_test()
			.into_iter()
			.map(|(operation, slabs, _)| (operation, slabs))
			.collect::<Vec<_>>();
		assert_eq!(
			cooling, expected,
			"pass {pass} attempted exactly the next slab and no other"
		);
	}
	let before = policy.neutral_cooldowns_for_test();
	let report = mgr
		.prune_versions_for_retention(
			MvccRetentionPolicy::Disabled,
			&lifecycle,
			&schemas,
			&indexes,
			100_000,
			1,
		)
		.await
		.unwrap();
	assert_eq!(report.compacted_slabs, 0);
	assert_eq!(
		policy.neutral_cooldowns_for_test(),
		before,
		"every slab is cooling: the pass attempted nothing"
	);
	assert!(policy.report().in_flight.is_empty());
}

/// ADR 0098 decision 1: the tombstone pass budgets attempts the same way.
#[tokio::test]
async fn tombstone_pass_budgets_attempts_and_walks_the_sealed_slabs() {
	use crate::storage::maintenance_policy::MaintenanceOperation;

	let (_mgr_dir, mgr) = fresh_manager().await;
	let mut sealed = Vec::new();
	for n in 0..2_u64 {
		mgr.append_durable(tenant(), id(0x9900 + n), hlc(10 + n), b"kept-row")
			.await
			.unwrap();
		sealed.push(force_seal_active(&mgr).await);
	}
	// A tombstone over a locality no row occupies: every scan is clean.
	let unoccupied = MigratedRangeTombstone {
		migration_id: 1,
		range_start: u64::MAX - 1,
		range_end: u64::MAX - 1,
		to_epoch: 2,
		completed_by: NodeId {
			id: 7,
			_reserved: 0,
		},
		completed_at_hlc: hlc(1),
		migration_hlc_floor: hlc(0),
	};
	let (schemas, indexes) = empty_compaction_catalogs();
	let policy = mgr.maintenance_policy();
	for (pass, slab_id) in sealed.iter().enumerate() {
		let report = mgr
			.reclaim_tombstoned_ranges(std::slice::from_ref(&unoccupied), &schemas, &indexes, 1)
			.await
			.unwrap();
		assert_eq!(report.compacted_slabs, 0, "pass {pass} rewrites nothing");
		let cooling = policy
			.neutral_cooldowns_for_test()
			.into_iter()
			.map(|(operation, slabs, _)| (operation, slabs))
			.collect::<Vec<_>>();
		assert_eq!(cooling.len(), pass + 1, "pass {pass} attempted one slab");
		assert_eq!(
			cooling[pass],
			(MaintenanceOperation::TombstoneReclaim, vec![*slab_id])
		);
	}
}

/// ADR 0098 decision 4: admission and reservation are one operation under the
/// manager lock, so the bound on overshoot is a property of the reservation and
/// not of a cursor read. When the guard returns, the cursor has already moved by
/// the padded record; a second admission taken before the first write lands is
/// judged against that reservation and rotates. A guard that reads the cursor
/// and lets the write reserve later passes the sequential test below and fails
/// here: the cursor is still zero after the first admission and the second
/// payload is admitted to the same slab.
#[tokio::test]
async fn append_admission_reserves_the_padded_record_under_the_lock() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let cap = mgr.config.max_slab_bytes;
	let frame_len = 600 * 1024;
	let (_, padded) = LocalSlabStore::padded_record_size(frame_len).unwrap();
	assert!((padded as u64) * 2 > cap && (padded as u64) < cap);

	let (first_slab, first) = mgr.target_for_append(frame_len, None).await.unwrap();
	assert_eq!(
		first_slab.write_cursor(),
		padded as u64,
		"the guard reserved the padded record before returning"
	);
	assert_eq!(first.padded_size(), padded);

	let (second_slab, second) = mgr.target_for_append(frame_len, None).await.unwrap();
	assert!(
		second_slab.slab_id() > first_slab.slab_id(),
		"the second admission saw the first reservation and rotated"
	);

	// Consume both tokens so the slabs hold what they reserved.
	let frame = vec![0x3C_u8; frame_len];
	let second_location = second_slab
		.append_reserved(second, tenant(), id(0x9A21), hlc(31), &frame)
		.await
		.unwrap();
	let first_location = first_slab
		.append_reserved(first, tenant(), id(0x9A20), hlc(30), &frame)
		.await
		.unwrap();
	assert_eq!(first_location.offset, 0);
	assert_eq!(second_location.offset, 0);
	assert_eq!(first_slab.write_cursor(), padded as u64);
}

/// ADR 0098 decision 4: the guard judges the record the slab reserves, header
/// and page padding included, not the raw frame. A frame whose raw length fits
/// under the cap but whose padded record does not is rotated. Under a raw-length
/// check it lands in the first slab and carries it over the cap.
#[tokio::test]
async fn append_admission_judges_the_padded_record_not_the_frame() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let cap = mgr.config.max_slab_bytes as usize;
	let page = crate::storage::io_uring::DIRECT_IO_ALIGN;

	// One page of prior record, then a frame chosen so that
	// cursor + frame_len < cap but cursor + padded > cap.
	let (_, small_padded) = LocalSlabStore::padded_record_size(16).unwrap();
	assert_eq!(small_padded, page);
	let header = LocalSlabStore::padded_record_size(0).unwrap().0;
	let frame_len = cap - page - header + 1;
	let (_, padded) = LocalSlabStore::padded_record_size(frame_len).unwrap();
	assert_eq!(padded, cap);
	assert!(page + frame_len < cap);

	let small = mgr
		.append_durable(tenant(), id(0x9A30), hlc(40), &[0x11_u8; 16])
		.await
		.unwrap();
	let big = mgr
		.append_durable(tenant(), id(0x9A31), hlc(41), &vec![0x22_u8; frame_len])
		.await
		.unwrap();
	assert!(
		big.slab_id > small.slab_id,
		"the padded record would cross the cap, so the frame rotated: {small:?} {big:?}"
	);
}

/// ADR 0098 decision 4: an append that would carry the active slab across the
/// cap rotates first; a cursor-only rule lands the second 600 KiB payload in
/// the first slab. A payload larger than the cap still enters an empty slab.
#[tokio::test]
async fn append_rotates_before_a_payload_would_cross_the_cap() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	assert_eq!(mgr.config.max_slab_bytes, 1 << 20);
	let payload = vec![0x5A_u8; 600 * 1024];
	let mut slab_ids = Vec::new();
	for n in 0..3_u64 {
		let location = mgr
			.append_durable(tenant(), id(0x9A00 + n), hlc(10 + n), &payload)
			.await
			.unwrap();
		slab_ids.push(location.slab_id);
	}
	assert!(
		slab_ids.windows(2).all(|pair| pair[0] < pair[1]),
		"each 600 KiB payload took its own slab under a 1 MiB cap: {slab_ids:?}"
	);

	let oversized = vec![0xA5_u8; 2 << 20];
	let big = mgr
		.append_durable(tenant(), id(0x9A10), hlc(20), &oversized)
		.await
		.unwrap();
	assert!(
		big.slab_id > slab_ids[2],
		"the third slab held 600 KiB, so the oversized payload rotated into a fresh one"
	);
	let small = mgr
		.append_durable(tenant(), id(0x9A11), hlc(21), b"after-oversized")
		.await
		.unwrap();
	assert!(
		small.slab_id > big.slab_id,
		"the slab holding the oversized payload is over the cap and rotates"
	);
}

/// ADR 0099: an append reservation that is never written leaves a zeroed,
/// page-aligned hole in the slab file. A later append lands after the hole,
/// is fdatasynced, and is acknowledged durable. Recovery must still find it:
/// an interior hole is not the end of valid data. Under the loop that stops
/// at the first page without a record magic, the acknowledged record is
/// invisible after reopen and the recovered write cursor sits at the hole, so
/// the next append overwrites it.
#[tokio::test]
async fn durable_append_after_an_unwritten_reservation_survives_reopen() {
	let dir = fresh_dir("adr0099-interior-hole");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;

	// A: admitted and reserved, never written. Three pages wide so the hole
	// is more than one probe page.
	let hole_frame_len = 2 * crate::storage::io_uring::DIRECT_IO_ALIGN + 16;
	let (slab, reservation) = manager
		.target_for_append(hole_frame_len, None)
		.await
		.unwrap();
	let hole_end = reservation.padded_size() as u64;
	drop(reservation);
	assert_eq!(slab.write_cursor(), hole_end);

	// B: written after the hole and acknowledged durable.
	let key_b = id(0x9900);
	let bytes_b = b"acknowledged-after-the-hole".to_vec();
	let location_b = manager
		.append_durable(tenant(), key_b, hlc(99), &bytes_b)
		.await
		.unwrap();
	assert_eq!(location_b.slab_id, slab.slab_id());
	assert_eq!(location_b.offset, hole_end);

	drop(manager);
	let reopened = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let recovered = reopened
		.lookup(tenant(), key_b)
		.expect("a durably acknowledged record is recoverable past an interior hole");
	assert_eq!(recovered, location_b);
	assert_eq!(reopened.read(recovered).await.unwrap(), bytes_b);

	// The recovered cursor resumed past B, so a new append cannot overwrite it.
	let location_c = reopened
		.append_durable(tenant(), id(0x9901), hlc(100), b"after-reopen")
		.await
		.unwrap();
	assert!(
		location_c.slab_id > location_b.slab_id || location_c.offset > location_b.offset,
		"the next append landed past the recovered record: {location_c:?} vs {location_b:?}"
	);
	assert_eq!(reopened.read(location_b).await.unwrap(), bytes_b);
}

/// ADR 0099 decision 3: the sealed-bytes walk resumes past an anomaly only
/// at an offset the caller vouches for. With the slab's acknowledged offsets
/// both records are recovered across a zero hole and across a torn page
/// alike; with no evidence the walk stops at the first anomaly either way. A
/// zero page on its own never carries the walk forward.
#[tokio::test]
async fn blob_recovery_resumes_only_at_acknowledged_offsets() {
	let dir = fresh_dir("adr0099-blob-hole");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let page = crate::storage::io_uring::DIRECT_IO_ALIGN;

	let before = manager
		.append_durable(tenant(), id(0x9910), hlc(110), b"before-the-hole")
		.await
		.unwrap();
	let (_, reservation) = manager.target_for_append(16, None).await.unwrap();
	assert_eq!(reservation.padded_size(), page);
	drop(reservation);
	let after = manager
		.append_durable(tenant(), id(0x9911), hlc(111), b"after-the-hole")
		.await
		.unwrap();
	assert_eq!(after.offset, before.offset + 2 * page as u64);
	drop(manager);

	let bytes = std::fs::read(slab_path(&dir, before.slab_id)).unwrap();
	let offsets = |entries: &[RecoveredSlabEntry]| {
		entries
			.iter()
			.map(|entry| entry.location.offset)
			.collect::<Vec<_>>()
	};
	let evidence = BTreeSet::from([before.offset, after.offset]);
	let none = BTreeSet::new();
	let mut torn = bytes.clone();
	torn[before.offset as usize + page + 7] = 0xFF;

	for (name, blob) in [("zero hole", &bytes), ("torn page", &torn)] {
		let recovered = recover_slab_entries_from_blob(blob, before.slab_id, &evidence).unwrap();
		assert_eq!(
			offsets(&recovered),
			vec![before.offset, after.offset],
			"{name}: with evidence both records are recovered"
		);
		let recovered = recover_slab_entries_from_blob(blob, before.slab_id, &none).unwrap();
		assert_eq!(
			offsets(&recovered),
			vec![before.offset],
			"{name}: without evidence the walk ends at the first anomaly"
		);
	}
}

/// ADR 0099 decision 3, the reviewer's direct-bytes reproducer: a torn
/// record whose lost first page is zero and whose persisted payload page
/// holds a byte-exact copy of another record. Fed straight to the sealed-bytes
/// walk with the slab's real evidence, nothing is promoted: the copy's offset
/// was never acknowledged. Without evidence the walk stops at the zero page.
#[tokio::test]
async fn blob_recovery_does_not_promote_a_payload_page_after_a_lost_header_page() {
	let dir = fresh_dir("adr0099-blob-embedded");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let page = crate::storage::io_uring::DIRECT_IO_ALIGN;
	let inner = manager
		.append_durable(tenant(), id(0x99F0), hlc(130), b"embedded-record")
		.await
		.unwrap();
	let inner_bytes = std::fs::read(slab_path(&dir, inner.slab_id)).unwrap();
	let header = LocalSlabStore::padded_record_size(0).unwrap().0;
	let mut payload = vec![0_u8; page - header];
	payload.extend_from_slice(&inner_bytes[..page]);
	let outer = manager
		.append_durable(tenant(), id(0x99F1), hlc(131), &payload)
		.await
		.unwrap();
	let evidence = manager.acknowledged_offsets_in_slab(outer.slab_id);
	assert_eq!(evidence, BTreeSet::from([inner.offset, outer.offset]));
	drop(manager);

	let bytes = std::fs::read(slab_path(&dir, outer.slab_id)).unwrap();
	let mut torn = bytes[outer.offset as usize..outer.offset as usize + 2 * page].to_vec();
	torn[..page].fill(0);
	// The torn slice starts at the outer record's offset; shift the evidence
	// to the slice's coordinates.
	let shifted = evidence
		.iter()
		.filter_map(|offset| offset.checked_sub(outer.offset))
		.collect::<BTreeSet<_>>();
	assert_eq!(shifted, BTreeSet::from([0]));
	for acknowledged in [&shifted, &BTreeSet::new()] {
		let recovered = recover_slab_entries_from_blob(&torn, outer.slab_id, acknowledged).unwrap();
		assert!(
			recovered.is_empty(),
			"payload was promoted to a record: {recovered:?}"
		);
	}
}

/// ADR 0099 decision 2: zero pages at the end of the file are a trailing
/// hole. Recovery keeps the records before them and resumes appending where
/// the last record ended, over the hole, rather than preserving it.
#[tokio::test]
async fn recovery_resumes_at_the_last_record_over_a_trailing_hole() {
	let dir = fresh_dir("adr0099-trailing-hole");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let page = crate::storage::io_uring::DIRECT_IO_ALIGN as u64;

	let last = manager
		.append_durable(tenant(), id(0x9920), hlc(120), b"last-record")
		.await
		.unwrap();
	drop(manager);

	// Extend the file by two zero pages, as a later write past a dropped
	// reservation would have, without any record following.
	let path = slab_path(&dir, last.slab_id);
	let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
	let len = file.metadata().unwrap().len();
	assert_eq!(len, last.offset + page);
	file.set_len(len + 2 * page).unwrap();
	drop(file);

	let reopened = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	assert_eq!(reopened.lookup(tenant(), id(0x9920)), Some(last));
	let next = reopened
		.append_durable(tenant(), id(0x9921), hlc(121), b"next-record")
		.await
		.unwrap();
	assert_eq!(next.slab_id, last.slab_id);
	assert_eq!(
		next.offset,
		last.offset + page,
		"the append resumed at the last record's end, reusing the trailing hole"
	);
	assert_eq!(reopened.read(last).await.unwrap(), b"last-record".to_vec());
}

/// ADR 0099 decision 1, the case a zero-page skip alone gets wrong: a record
/// whose payload carries a byte-exact copy of another record at its second
/// page, whose first page did not persist. The copy validates by checksum,
/// but its offset was never acknowledged, so recovery does not promote it;
/// the torn record's region is zeroed and the original stays the only
/// version of its key.
#[tokio::test]
async fn recovery_does_not_promote_a_payload_page_after_a_lost_header_page() {
	let dir = fresh_dir("adr0099-embedded-record");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let page = crate::storage::io_uring::DIRECT_IO_ALIGN;

	let inner_key = id(0x99F0);
	let inner = manager
		.append_durable(tenant(), inner_key, hlc(130), b"embedded-record")
		.await
		.unwrap();
	let inner_bytes = std::fs::read(slab_path(&dir, inner.slab_id)).unwrap();
	let header = LocalSlabStore::padded_record_size(0).unwrap().0;
	let mut payload = vec![0_u8; page - header];
	payload.extend_from_slice(&inner_bytes[..page]);
	let outer_key = id(0x99F1);
	let outer = manager
		.append_durable(tenant(), outer_key, hlc(131), &payload)
		.await
		.unwrap();
	assert_eq!(outer.offset, page as u64);
	drop(manager);

	// The outer record's first page is lost; its payload page persisted.
	let path = slab_path(&dir, outer.slab_id);
	let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
	std::os::unix::fs::FileExt::write_all_at(&file, &vec![0_u8; page], outer.offset).unwrap();
	drop(file);

	let reopened = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	assert_eq!(reopened.lookup(tenant(), inner_key), Some(inner));
	assert_eq!(reopened.lookup(tenant(), outer_key), None);
	assert_eq!(
		reopened.lookup_as_of(tenant(), inner_key, hlc(130)),
		Some(inner),
		"the embedded copy was not promoted to a second version of the key"
	);
	let bytes = std::fs::read(&path).unwrap();
	assert!(
		bytes[outer.offset as usize..].iter().all(|byte| *byte == 0),
		"the torn region was zeroed so the file is canonical"
	);
}

/// ADR 0099 decision 1: a torn region with bytes in it, ahead of a record
/// the node acknowledged, no longer ends the walk. The acknowledged record is
/// recovered, the torn region is zeroed, and appends resume past the record.
#[tokio::test]
async fn recovery_resumes_past_a_torn_region_at_the_next_acknowledged_record() {
	let dir = fresh_dir("adr0099-torn-then-acked");
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	let page = crate::storage::io_uring::DIRECT_IO_ALIGN;

	let torn_key = id(0x99A0);
	let torn = manager
		.append_durable(tenant(), torn_key, hlc(140), &vec![0x44_u8; 2 * page])
		.await
		.unwrap();
	let acked_key = id(0x99A1);
	let acked = manager
		.append_durable(tenant(), acked_key, hlc(141), b"acknowledged-after-torn")
		.await
		.unwrap();
	drop(manager);

	// Corrupt the first record's magic without zeroing the page.
	let path = slab_path(&dir, torn.slab_id);
	let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
	std::os::unix::fs::FileExt::write_all_at(&file, &[0xEE; 4], torn.offset).unwrap();
	drop(file);

	let reopened = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	assert_eq!(reopened.lookup(tenant(), torn_key), None);
	assert_eq!(reopened.lookup(tenant(), acked_key), Some(acked));
	assert_eq!(
		reopened.read(acked).await.unwrap(),
		b"acknowledged-after-torn".to_vec()
	);
	let bytes = std::fs::read(&path).unwrap();
	assert!(
		bytes[torn.offset as usize..acked.offset as usize]
			.iter()
			.all(|byte| *byte == 0),
		"the torn region was zeroed"
	);
	let next = reopened
		.append_durable(tenant(), id(0x99A2), hlc(142), b"after-reopen")
		.await
		.unwrap();
	assert!(next.offset > acked.offset);
}

/// ADR 0099 decision 1 without an oracle: when the durable index cannot be
/// opened, recovery has no evidence of boundaries and stops at the first
/// anomaly, as it did before this record. This is the pre-record exposure,
/// not a safe mode: the append is still acknowledged, and the warning at
/// open that the index failed is the operator's signal.
#[tokio::test]
async fn recovery_without_the_durable_index_stops_at_the_first_anomaly() {
	let dir = fresh_dir("adr0099-no-oracle");
	// Occupy the index path with a directory so redb cannot open it.
	std::fs::create_dir_all(persistent_index_path(&dir)).unwrap();
	let crypto = test_crypto();
	let io = spawn_io_thread(64).unwrap();
	let manager = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	assert!(manager.persistent_index.is_none());

	let (slab, reservation) = manager.target_for_append(16, None).await.unwrap();
	drop(reservation);
	let key = id(0x99B0);
	let after = manager
		.append_durable(tenant(), key, hlc(150), b"after-the-hole")
		.await
		.unwrap();
	assert_eq!(after.slab_id, slab.slab_id());
	drop(manager);

	let reopened = open_manager_at_dir(dir.to_path_buf(), io.clone(), crypto.clone()).await;
	assert_eq!(
		reopened.lookup(tenant(), key),
		None,
		"without boundary evidence the walk stops at the hole"
	);
}

/// ADR 0099 decision 3, the reviewer's round trip: a sealed slab whose first
/// reservation was dropped encrypts with the manager's evidence and must
/// decode with it too. The decoder's tenant check walks the decrypted bytes,
/// so it needs the same evidence to get past the leading hole; with none it
/// finds no record and refuses a blob the node itself produced.
#[tokio::test]
async fn encrypted_slab_with_a_leading_hole_round_trips_with_evidence() {
	let (_mgr_dir, mgr) = fresh_manager().await;
	let (_, reservation) = mgr.target_for_append(16, None).await.unwrap();
	drop(reservation);
	mgr.append_durable(tenant(), id(0x99C0), hlc(160), b"durable-after-hole")
		.await
		.unwrap();
	let slab_id = force_seal_active(&mgr).await;
	let plaintext = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &plaintext).unwrap();
	let pointer = TieredSlabPointer {
		slab_id,
		l2_uri: format!("memory://tiered/{slab_id:016x}"),
		payload_hash: crate::hash::content::digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(tenant()),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted.stats_locator.map(|v| v.stats_format_version),
		stats_block_len: encrypted.stats_locator.map(|v| v.stats_block_len),
		stats_block_hash128: encrypted.stats_locator.map(|v| v.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	};
	let evidence = mgr.acknowledged_offsets_in_slab(slab_id);
	let decoded = decode_tiered_blob(
		mgr.config.crypto.as_ref(),
		&pointer,
		&encrypted.bytes,
		&evidence,
	)
	.expect("a freshly encrypted slab with an acknowledged record after a hole decodes");
	assert_eq!(decoded, plaintext);
	let err = decode_tiered_blob(
		mgr.config.crypto.as_ref(),
		&pointer,
		&encrypted.bytes,
		&BTreeSet::new(),
	)
	.unwrap_err();
	assert!(
		err.to_string().contains("committed records"),
		"without evidence the leading hole ends the tenant walk: {err}"
	);
}

#[path = "counter_visibility/tests.rs"]
mod counter_visibility_tests;
