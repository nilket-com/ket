use wendelon::schema::{
	CdcCursor, HybridTimestamp, Identity, RecordRowIndex, cdc_cursor_in_catchup_window,
};

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 1,
	}
}

fn cursor(ms: u64, key: u8) -> CdcCursor {
	CdcCursor::new(hlc(ms), Identity::new([key; 32]), RecordRowIndex::ZERO)
}

#[test]
fn cdc_catchup_uses_full_tuple_bounds_for_same_hlc_suffixes() {
	let start = cursor(50, 5);
	let upper = CdcCursor::ceiling_for_hlc(hlc(50));

	assert!(!cdc_cursor_in_catchup_window(cursor(50, 5), start, upper));
	assert!(cdc_cursor_in_catchup_window(cursor(50, 7), start, upper));
	assert!(cdc_cursor_in_catchup_window(cursor(50, 9), start, upper));
	assert!(!cdc_cursor_in_catchup_window(cursor(51, 1), start, upper));
}
