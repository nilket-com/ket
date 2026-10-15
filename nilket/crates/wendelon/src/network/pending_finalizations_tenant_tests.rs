//! Local index reconstruction only; no mixed-binary or process-crash proof.
use super::*;
use crate::consensus::TenantFinalization;
use crate::schema::{DurabilityLevel, FinalizationMarker, HybridTimestamp, Identity, TenantId};
use redb::TableHandle;

fn path() -> PathBuf {
	std::env::temp_dir().join(format!(
		"wendelon-pending-tenant-index-{}.redb",
		uuid::Uuid::new_v4()
	))
}
fn record(tenant: u64, stamp: u64) -> PendingFinalizationRecord {
	PendingFinalizationRecord::new(
		ClientWriteId {
			client_uuid: [7; 16],
			sequence_no: 1,
			idempotency_ttl_ms: 1,
			_pad: 0,
		},
		true,
		TenantFinalization {
			marker: FinalizationMarker {
				log_index: 0,
				term: 0,
				epoch: 1,
				commit_hlc: HybridTimestamp {
					physical_ms: stamp,
					logical: 0,
					node_id: 0,
				},
				payload_hash: [0; 32],
				e_safe_index: 0,
				durability: DurabilityLevel::QUORUM,
				_pad: [0; 7],
			},
			tenant_id: TenantId::new(tenant),
			key_identity: Identity::new([3; 32]),
			charged_bytes: 1,
			record_count: 1,
		},
	)
}
fn raw_tables(path: &Path) -> Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)> {
	let db = Database::open(path).unwrap();
	let read = db.begin_read().unwrap();
	[
		PENDING_FINALIZATIONS_TABLE,
		PENDING_FINALIZATIONS_BY_WRITE_ID_LEGACY_TABLE,
		PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE,
		PENDING_FINALIZATION_DELETE_COUNTS_TABLE,
	]
	.into_iter()
	.filter_map(|definition| {
		let table = match read.open_table(definition) {
			Ok(table) => table,
			Err(redb::TableError::TableDoesNotExist(_)) => return None,
			Err(error) => panic!("table open: {error}"),
		};
		let values = table
			.iter()
			.unwrap()
			.map(|entry| {
				let (k, v) = entry.unwrap();
				(k.value().to_vec(), v.value().to_vec())
			})
			.collect();
		Some((definition.name().to_string(), values))
	})
	.collect()
}
fn format_marker(path: &Path) -> Option<u8> {
	let db = Database::open(path).unwrap();
	let read = db.begin_read().unwrap();
	let table = read.open_table(PENDING_FINALIZATION_META_TABLE).unwrap();
	table
		.get(PENDING_FINALIZATION_FORMAT_KEY)
		.unwrap()
		.map(|value| value.value())
}
fn write_fixture(
	path: &Path,
	rows: &[PendingFinalizationRecord],
	legacy: bool,
	current: bool,
	version: u8,
) {
	let db = Database::create(path).unwrap();
	let txn = db.begin_write().unwrap();
	{
		let mut primary = txn.open_table(PENDING_FINALIZATIONS_TABLE).unwrap();
		for row in rows {
			primary
				.insert(
					row.id.as_bytes().as_slice(),
					postcard::to_stdvec(row).unwrap().as_slice(),
				)
				.unwrap();
		}
	}
	if legacy {
		let mut table = txn
			.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_LEGACY_TABLE)
			.unwrap();
		table.insert(&[99u8; 24][..], &[99u8; 32][..]).unwrap();
	}
	if current {
		let mut table = txn
			.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE)
			.unwrap();
		table.insert(&[88u8; 32][..], &[88u8; 32][..]).unwrap();
	}
	{
		txn.open_table(PENDING_FINALIZATION_META_TABLE)
			.unwrap()
			.insert(PENDING_FINALIZATION_FORMAT_KEY, version)
			.unwrap();
	}
	txn.commit().unwrap();
}

#[test]
fn tenant_index_key_is_big_endian_tenant_then_canonical_identity() {
	let row = record(0x0102030405060708, 0);
	let mut expected = [0u8; 32];
	expected[..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
	expected[8..24].fill(7);
	expected[24..].copy_from_slice(&1u64.to_le_bytes());
	assert_eq!(
		encode_write_id_key(row.finalization.tenant_id, row.write_id),
		expected
	);
	let drift = ClientWriteId {
		idempotency_ttl_ms: u32::MAX,
		_pad: u32::MAX,
		..row.write_id
	};
	assert_eq!(
		encode_write_id_key(row.finalization.tenant_id, drift),
		expected
	);
	assert_ne!(encode_write_id_key(TenantId::new(8), drift), expected);
}

#[tokio::test]
async fn tenant_index_preserves_each_tenant_in_both_upsert_orders() {
	for reverse in [false, true] {
		let file = path();
		let store = PendingFinalizationStore::open(file.clone()).unwrap();
		let a = record(7, 7);
		let b = record(8, 8);
		let ordered = if reverse { [&b, &a] } else { [&a, &b] };
		for row in ordered {
			store.upsert(row.clone()).await.unwrap();
		}
		for row in [&a, &b] {
			assert_eq!(
				store
					.find_by_write_id(row.finalization.tenant_id, row.write_id)
					.await
					.unwrap(),
				Some(row.clone())
			);
		}
		assert_eq!(store.count(), 2);
		drop(store);
		std::fs::remove_file(file).unwrap();
	}
}

#[tokio::test]
async fn tenant_index_remove_repoints_only_own_tenant() {
	let file = path();
	let store = PendingFinalizationStore::open(file.clone()).unwrap();
	let a1 = record(7, 1);
	let a2 = record(7, 2);
	let b = record(8, 3);
	for row in [&a1, &a2, &b] {
		store.upsert(row.clone()).await.unwrap();
	}
	store.remove(a1.id).await.unwrap();
	assert_eq!(
		store
			.find_by_write_id(TenantId::new(7), a2.write_id)
			.await
			.unwrap(),
		Some(a2.clone())
	);
	store.upsert(a1.clone()).await.unwrap();
	store.upsert(a2.clone()).await.unwrap();
	store.remove(a2.id).await.unwrap();
	assert_eq!(
		store
			.find_by_write_id(TenantId::new(7), a1.write_id)
			.await
			.unwrap(),
		Some(a1.clone())
	);
	assert_eq!(
		store
			.find_by_write_id(TenantId::new(8), b.write_id)
			.await
			.unwrap(),
		Some(b.clone())
	);
	store.remove(a1.id).await.unwrap();
	store.remove(a1.id).await.unwrap();
	assert_eq!(
		store
			.find_by_write_id(TenantId::new(7), b.write_id)
			.await
			.unwrap(),
		None
	);
	assert_eq!(
		store
			.find_by_write_id(TenantId::new(8), b.write_id)
			.await
			.unwrap(),
		Some(b)
	);
	drop(store);
	std::fs::remove_file(file).unwrap();
}

#[tokio::test]
async fn tenant_index_rebuilds_v1_only_both_and_v2_only_on_true_repeat_open() {
	for (legacy, current) in [(true, false), (true, true), (false, true)] {
		let file = path();
		let a = record(7, 7);
		let b = record(8, 8);
		write_fixture(&file, &[a.clone(), b.clone()], legacy, current, 1);
		let original = raw_tables(&file)
			.into_iter()
			.find(|(n, _)| n == "pending_finalizations_v1")
			.unwrap();
		for _ in 0..2 {
			let store = PendingFinalizationStore::open(file.clone()).unwrap();
			for row in [&a, &b] {
				assert_eq!(
					store
						.find_by_write_id(row.finalization.tenant_id, row.write_id)
						.await
						.unwrap(),
					Some(row.clone())
				);
			}
			assert_eq!(store.count(), 2);
			assert_eq!(store.format_version, 1);
			drop(store);
			let tables = raw_tables(&file);
			assert!(
				!tables
					.iter()
					.any(|(n, _)| n == "pending_finalizations_by_write_id_v1")
			);
			assert_eq!(
				tables
					.iter()
					.find(|(n, _)| n == "pending_finalizations_v1")
					.unwrap(),
				&original
			);
			assert_eq!(
				tables
					.iter()
					.find(|(n, _)| n == "pending_finalizations_by_write_id_v2")
					.unwrap()
					.1
					.len(),
				2
			);
		}
		std::fs::remove_file(file).unwrap();
	}
}

#[test]
fn tenant_index_failed_open_keeps_committed_v1_v2_and_primary_values() {
	for corrupt in [false, true] {
		let file = path();
		let row = record(7, 7);
		write_fixture(
			&file,
			&[row.clone()],
			true,
			true,
			if corrupt { 1 } else { 9 },
		);
		if corrupt {
			let db = Database::open(&file).unwrap();
			let txn = db.begin_write().unwrap();
			{
				txn.open_table(PENDING_FINALIZATIONS_TABLE)
					.unwrap()
					.insert(row.id.as_bytes().as_slice(), &[255u8][..])
					.unwrap();
			}
			txn.commit().unwrap();
		}
		let before = raw_tables(&file);
		let before_format = format_marker(&file);
		let error = match PendingFinalizationStore::open(file.clone()) {
			Ok(_) => panic!("invalid fixture opened"),
			Err(e) => e,
		};
		assert_eq!(error.kind(), ErrorKind::InvalidData);
		assert_eq!(raw_tables(&file), before);
		assert_eq!(format_marker(&file), before_format);
		assert!(
			pending_store_registry()
				.lock()
				.unwrap()
				.get(&file)
				.and_then(Weak::upgrade)
				.is_none()
		);
		let db = Database::open(&file).unwrap();
		let txn = db.begin_write().unwrap();
		{
			txn.open_table(PENDING_FINALIZATIONS_TABLE)
				.unwrap()
				.insert(
					row.id.as_bytes().as_slice(),
					postcard::to_stdvec(&row).unwrap().as_slice(),
				)
				.unwrap();
			txn.open_table(PENDING_FINALIZATION_META_TABLE)
				.unwrap()
				.insert(PENDING_FINALIZATION_FORMAT_KEY, 1u8)
				.unwrap();
		}
		txn.commit().unwrap();
		drop(db);
		drop(PendingFinalizationStore::open(file.clone()).unwrap());
		assert!(
			!raw_tables(&file)
				.iter()
				.any(|(n, _)| n == "pending_finalizations_by_write_id_v1")
		);
		std::fs::remove_file(file).unwrap();
	}
}

#[test]
fn tenant_index_rebuild_and_legacy_delete_abort_together_without_crash_claim() {
	let file = path();
	write_fixture(&file, &[record(7, 7)], true, true, 1);
	let before = raw_tables(&file);
	{
		let db = Database::open(&file).unwrap();
		let txn = db.begin_write().unwrap();
		{
			txn.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE)
				.unwrap()
				.retain(|_, _| false)
				.unwrap();
		}
		assert!(
			txn.delete_table(PENDING_FINALIZATIONS_BY_WRITE_ID_LEGACY_TABLE)
				.unwrap()
		);
		txn.abort().unwrap();
	}
	assert_eq!(raw_tables(&file), before);
	std::fs::remove_file(file).unwrap();
}

#[tokio::test]
async fn tenant_index_lookup_rejects_wrong_tenant_and_canonical_pointer() {
	let file = path();
	let a = record(7, 7);
	let b = record(8, 8);
	let store = PendingFinalizationStore::open(file.clone()).unwrap();
	store.upsert(a.clone()).await.unwrap();
	store.upsert(b.clone()).await.unwrap();
	for canonical in [false, true] {
		let id = if canonical {
			ClientWriteId {
				sequence_no: 2,
				..a.write_id
			}
		} else {
			a.write_id
		};
		let txn = store.db.begin_write().unwrap();
		{
			txn.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE)
				.unwrap()
				.insert(
					encode_write_id_key(TenantId::new(7), id).as_slice(),
					(if canonical { a.id } else { b.id }).as_bytes().as_slice(),
				)
				.unwrap();
		}
		txn.commit().unwrap();
		let error = store
			.find_by_write_id(TenantId::new(7), id)
			.await
			.unwrap_err();
		assert_eq!(error.kind(), ErrorKind::InvalidData);
		assert_eq!(
			error.to_string(),
			"pending-finalization secondary index returned a mismatched tenant or write_id"
		);
	}
	drop(store);
	std::fs::remove_file(file).unwrap();
}

#[tokio::test]
async fn tenant_index_same_tenant_live_reopen_and_remove_order_is_preserved() {
	let file = path();
	let a = record(7, 1);
	let b = record(7, 2);
	let (first, last) = if a.id.as_bytes() < b.id.as_bytes() {
		(a, b)
	} else {
		(b, a)
	};
	let store = PendingFinalizationStore::open(file.clone()).unwrap();
	store.upsert(last.clone()).await.unwrap();
	store.upsert(first.clone()).await.unwrap();
	assert_eq!(
		store
			.find_by_write_id(TenantId::new(7), first.write_id)
			.await
			.unwrap(),
		Some(first.clone())
	);
	drop(store);
	let store = PendingFinalizationStore::open(file.clone()).unwrap();
	assert_eq!(
		store
			.find_by_write_id(TenantId::new(7), first.write_id)
			.await
			.unwrap(),
		Some(last.clone())
	);
	store.remove(last.id).await.unwrap();
	assert_eq!(
		store
			.find_by_write_id(TenantId::new(7), first.write_id)
			.await
			.unwrap(),
		Some(first)
	);
	drop(store);
	std::fs::remove_file(file).unwrap();
}

#[test]
fn tenant_index_change_keeps_primary_postcard_wire_and_format_golden() {
	let mut row = record(1, 0);
	row.id = PendingFinalizationId([0x55; 32]);
	row.finalization.marker.epoch = 0;
	let mut expected = vec![0x55; 32];
	expected.extend([7; 16]);
	expected.extend([1, 1, 0, 1]);
	expected.extend([0; 47]);
	expected.push(1);
	expected.extend([3; 32]);
	expected.extend([1, 1]);
	assert_eq!(postcard::to_stdvec(&row).unwrap(), expected);
	assert_eq!(
		postcard::from_bytes::<PendingFinalizationRecord>(&expected).unwrap(),
		row
	);
	assert_eq!(PENDING_FINALIZATION_FORMAT_VERSION, 1);
	let mut wire = vec![7; 16];
	wire.extend(1u64.to_le_bytes());
	wire.extend(1u32.to_le_bytes());
	wire.extend(0u32.to_le_bytes());
	assert_eq!(bytemuck::bytes_of(&row.write_id), wire.as_slice());
	assert_eq!(
		PENDING_FINALIZATIONS_TABLE.name(),
		"pending_finalizations_v1"
	);
}
