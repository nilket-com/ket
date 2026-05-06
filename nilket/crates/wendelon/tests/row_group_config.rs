use wendelon::schema::{EncryptedChunkCount, RecordBodyByteLen, RowCount, RowGroupRowCount};
use wendelon::storage::{
	L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR, L2RowGroupFetchConfig, RowGroupWriterConfig,
};

#[test]
fn row_group_writer_config_rejects_invalid_bounds() {
	let mut cfg = RowGroupWriterConfig {
		target_row_group_rows: RowGroupRowCount::new(0),
		..RowGroupWriterConfig::default()
	};
	assert!(cfg.validate().is_err());

	cfg = RowGroupWriterConfig {
		target_row_group_rows: RowGroupRowCount::new(8),
		max_row_group_rows: RowGroupRowCount::new(4),
		..RowGroupWriterConfig::default()
	};
	assert!(cfg.validate().is_err());

	cfg = RowGroupWriterConfig {
		min_paged_record_rows: RowCount::new(0),
		..RowGroupWriterConfig::default()
	};
	assert!(cfg.validate().is_err());

	cfg = RowGroupWriterConfig {
		min_paged_record_bytes: RecordBodyByteLen::new(0),
		..RowGroupWriterConfig::default()
	};
	assert!(cfg.validate().is_err());
}

#[test]
fn row_group_fetch_config_rejects_invalid_bounds() {
	let mut cfg = L2RowGroupFetchConfig {
		max_selected_row_group_fraction_micros: L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR,
		..L2RowGroupFetchConfig::default()
	};
	assert!(cfg.validate().is_err());

	cfg = L2RowGroupFetchConfig {
		max_selected_byte_fraction_micros: L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR,
		..L2RowGroupFetchConfig::default()
	};
	assert!(cfg.validate().is_err());

	cfg = L2RowGroupFetchConfig {
		max_ranges_per_record: EncryptedChunkCount::new(0),
		..L2RowGroupFetchConfig::default()
	};
	assert!(cfg.validate().is_err());

	cfg = L2RowGroupFetchConfig {
		max_parallel_range_gets: EncryptedChunkCount::new(0),
		..L2RowGroupFetchConfig::default()
	};
	assert!(cfg.validate().is_err());
}
