use wendelon::hash::state_digest::StateDigest;
use wendelon::schema::{HybridTimestamp, Identity, TableObjectId, TenantId};
use wendelon::storage::{
	CompactionDigestSummary, VisibleDomainRow, compaction_visible_domain_summary,
};

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 1,
	}
}

fn row(key: u8, ms: u64, is_tombstone: bool) -> VisibleDomainRow {
	VisibleDomainRow {
		tenant_id: TenantId::new(7),
		table_object_id: TableObjectId::new([3; 16]),
		row_key_id: Identity::new([key; 32]),
		row_hlc: hlc(ms),
		is_tombstone,
	}
}

#[test]
fn lsm_digest_visible_domain_ignores_rewritten_locations() {
	let before = [row(1, 10, false), row(2, 20, false)];
	let after = before.clone();

	let summary = compaction_visible_domain_summary(before, after);

	assert_ne!(summary.visible_pre, StateDigest::ZERO);
	assert_eq!(summary.visible_pre, summary.visible_post);
	assert_eq!(summary.version_entry_delta, StateDigest::ZERO);
}

#[test]
fn lsm_digest_latest_tombstone_is_absence_not_visible_row() {
	let data = row(1, 10, false);
	let tombstone = row(1, 20, true);

	let summary = compaction_visible_domain_summary([data, tombstone], []);

	assert_eq!(summary, CompactionDigestSummary::zero());
}

#[test]
fn lsm_digest_detects_tombstone_drop_that_resurrects_older_data() {
	let data = row(1, 10, false);
	let tombstone = row(1, 20, true);

	let summary = compaction_visible_domain_summary([data, tombstone], [data]);

	assert_eq!(summary.visible_pre, StateDigest::ZERO);
	assert_ne!(summary.visible_post, StateDigest::ZERO);
}
