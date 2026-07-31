use wendelon::schema::{
	CdcCursor, HybridTimestamp, Identity, RecordRowIndex, cdc_cursor_in_catchup_window,
};

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

fn row_key(byte: u8) -> Identity {
	Identity::new([byte; 32])
}

#[test]
fn cdc_cursor_ordering_orders_by_hlc_key_then_record_row_index() {
	let base = hlc(10, 0, 1);
	let mut cursors = [
		CdcCursor::new(base, row_key(7), RecordRowIndex::new(1)),
		CdcCursor::new(hlc(9, 9, 9), row_key(255), RecordRowIndex::MAX),
		CdcCursor::new(base, row_key(5), RecordRowIndex::new(9)),
		CdcCursor::new(base, row_key(5), RecordRowIndex::new(0)),
		CdcCursor::new(hlc(11, 0, 0), row_key(0), RecordRowIndex::ZERO),
	];

	cursors.sort();

	assert_eq!(cursors[0].row_hlc, hlc(9, 9, 9));
	assert_eq!(
		cursors[1],
		CdcCursor::new(base, row_key(5), RecordRowIndex::new(0))
	);
	assert_eq!(
		cursors[2],
		CdcCursor::new(base, row_key(5), RecordRowIndex::new(9))
	);
	assert_eq!(
		cursors[3],
		CdcCursor::new(base, row_key(7), RecordRowIndex::new(1))
	);
	assert_eq!(cursors[4].row_hlc, hlc(11, 0, 0));
}

#[test]
fn cdc_cursor_ordering_min_is_strict_floor() {
	let first = CdcCursor::new(hlc(0, 0, 0), row_key(0), RecordRowIndex::new(1));
	assert!(CdcCursor::MIN < first);
	assert!(CdcCursor::MIN < CdcCursor::MAX);
}

#[test]
fn cdc_cursor_ordering_catchup_window_uses_full_cursor_tuple_for_same_hlc_suffixes() {
	let t = hlc(75, 0, 1);
	let start = CdcCursor::new(t, row_key(5), RecordRowIndex::ZERO);
	let upper = CdcCursor::ceiling_for_hlc(t);
	let emitted: Vec<_> = [0_u8, 5, 7, 9]
		.into_iter()
		.map(|key| CdcCursor::new(t, row_key(key), RecordRowIndex::ZERO))
		.filter(|cursor| cdc_cursor_in_catchup_window(*cursor, start, upper))
		.collect();

	assert_eq!(
		emitted,
		vec![
			CdcCursor::new(t, row_key(7), RecordRowIndex::ZERO),
			CdcCursor::new(t, row_key(9), RecordRowIndex::ZERO),
		]
	);
}
