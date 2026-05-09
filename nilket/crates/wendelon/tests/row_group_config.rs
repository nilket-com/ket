use wendelon::schema::{EncryptedChunkCount, RecordBodyByteLen, RowCount, RowGroupRowCount};
use wendelon::storage::{
	L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR, L2RowGroupFetchConfig, ROW_GROUP_POINT_READ_MAX_ROWS,
	ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_BYTES, ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS,
	ROW_GROUP_POINT_READ_TARGET_ROWS, ROW_GROUP_SCAN_MAX_ROWS,
	ROW_GROUP_SCAN_MIN_PAGED_RECORD_BYTES, ROW_GROUP_SCAN_MIN_PAGED_RECORD_ROWS,
	ROW_GROUP_SCAN_TARGET_ROWS, RowGroupSizingProfile, RowGroupWriterConfig,
};

#[test]
fn row_group_writer_config_rejects_invalid_bounds() {
	let mut cfg = RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(0),
		RowGroupRowCount::new(8),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	);
	assert!(cfg.validate().is_err());

	cfg = RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(8),
		RowGroupRowCount::new(4),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	);
	assert!(cfg.validate().is_err());

	cfg = RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(4),
		RowGroupRowCount::new(8),
		RowCount::new(0),
		RecordBodyByteLen::new(1),
	);
	assert!(cfg.validate().is_err());

	cfg = RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(4),
		RowGroupRowCount::new(8),
		RowCount::new(8),
		RecordBodyByteLen::new(0),
	);
	assert!(cfg.validate().is_err());
}

#[test]
fn row_group_writer_profile_accessors_are_pinned() {
	let point_read = RowGroupWriterConfig::point_read();
	assert!(!point_read.enabled);
	assert_eq!(point_read.profile, RowGroupSizingProfile::PointRead);
	assert_eq!(
		point_read.target_row_group_rows().get(),
		ROW_GROUP_POINT_READ_TARGET_ROWS
	);
	assert_eq!(
		point_read.max_row_group_rows().get(),
		ROW_GROUP_POINT_READ_MAX_ROWS
	);
	assert_eq!(
		point_read.min_paged_record_rows().get(),
		ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS
	);
	assert_eq!(
		point_read.min_paged_record_bytes().get(),
		ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_BYTES
	);

	let scan = RowGroupWriterConfig::scan();
	assert!(!scan.enabled);
	assert_eq!(scan.profile, RowGroupSizingProfile::Scan);
	assert_eq!(
		scan.target_row_group_rows().get(),
		ROW_GROUP_SCAN_TARGET_ROWS
	);
	assert_eq!(scan.max_row_group_rows().get(), ROW_GROUP_SCAN_MAX_ROWS);
	assert_eq!(
		scan.min_paged_record_rows().get(),
		ROW_GROUP_SCAN_MIN_PAGED_RECORD_ROWS
	);
	assert_eq!(
		scan.min_paged_record_bytes().get(),
		ROW_GROUP_SCAN_MIN_PAGED_RECORD_BYTES
	);
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
