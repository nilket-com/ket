#[test]
fn row_tombstone_fixture_manifest_lists_required_cases() {
	let manifest = include_str!("fixtures/row_tombstones/cases.txt");
	for case in [
		"visibility_three_rows_delete_middle",
		"duplicate_row_ids_one_tombstone",
		"idempotency_retry_no_second_tombstone",
		"missing_row_no_tombstone",
		"already_deleted_row_no_second_tombstone",
		"tombstone_only_multirow_record",
		"footer_presence_bit",
		"digest_cancellation",
		"retention_pair_gc",
	] {
		assert!(
			manifest.contains(&format!("case={case}")),
			"missing row tombstone fixture case {case}"
		);
	}
}
