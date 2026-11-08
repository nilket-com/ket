//! ADR 0699 S3b: parent intents and row owners are durable authority.

use super::*;

fn temp_path(tag: &str) -> (crate::test_support::TestDir, PathBuf) {
	let dir = crate::test_support::TestDir::new(&format!("wendelon-parent-intents-{tag}"));
	let path = dir.join("index.redb");
	(dir, path)
}

const TENANT: TenantId = TenantId(7);

fn hlc(physical_ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical: 1,
		node_id: 2,
	}
}

fn version(key: u8, at: u64) -> OwnedVersion {
	OwnedVersion {
		table: TableObjectId::new([3; 16]),
		key_identity: [key; 32],
		hlc: hlc(at),
		row_digest: [key ^ 0x5A; 32],
	}
}

fn intent(rsv: u64, ordinal: u16, manifest: Vec<OwnedVersion>) -> ParentIntent {
	ParentIntent {
		tenant: TENANT,
		rsv,
		ordinal,
		claim_digest: [rsv as u8; 32],
		range_start: 0,
		range_end: u64::MAX,
		content: [9; 32],
		manifest,
		state: ParentIntentState::Intent,
	}
}

fn record(version: &OwnedVersion, slab: u64) -> PersistentRecordRow {
	let location = RecordLocation {
		slab_id: SlabId::new(slab),
		offset: 64,
		length: 128,
		record_row_index: RecordRowIndex::new(0),
		hlc: version.hlc,
		is_tombstone: false,
		placement: PlacementSemantics::RawLocality,
		partition: None,
	};
	PersistentRecordRow {
		tenant_id: TENANT,
		key_identity: Identity::new(version.key_identity),
		routing_locality: Locality::new(1),
		table_object_id: Some(version.table),
		placement: PlacementSemantics::RawLocality,
		partition: None,
		location,
	}
}

#[tokio::test]
async fn an_intent_and_its_owners_commit_together_and_retries_see_it() {
	let (_dir, path) = temp_path("begin");
	let index = PersistentLocalIndex::initialize_new(path, None)
		.await
		.unwrap();
	let manifest = vec![version(1, 10), version(2, 10)];
	assert_eq!(
		index
			.begin_parent_intent(intent(5, 0, manifest.clone()))
			.await
			.unwrap(),
		Ok(ParentIntentState::Intent)
	);
	assert_eq!(
		index
			.begin_parent_intent(intent(5, 0, manifest.clone()))
			.await
			.unwrap(),
		Ok(ParentIntentState::Intent),
		"an identical retry"
	);
	assert_eq!(
		index.version_owner(TENANT, manifest[1]).await.unwrap(),
		Some((5, 0))
	);
	// A different intent at the same (rsv, ordinal).
	assert_eq!(
		index
			.begin_parent_intent(intent(5, 0, vec![version(1, 10)]))
			.await
			.unwrap(),
		Err(IntentRefusal::ConflictingIntent)
	);
	// Another parent naming an owned version, even with identical bytes.
	assert_eq!(
		index
			.begin_parent_intent(intent(6, 0, vec![version(2, 10)]))
			.await
			.unwrap(),
		Err(IntentRefusal::ForeignOwner)
	);
	assert_eq!(
		index.parent_intent(6, 0).await.unwrap(),
		None,
		"nothing was written"
	);
}

#[tokio::test]
async fn an_existing_unowned_version_or_a_malformed_manifest_is_refused() {
	let (_dir, path) = temp_path("foreign");
	let index = PersistentLocalIndex::initialize_new(path, None)
		.await
		.unwrap();
	let foreign = version(4, 20);
	index
		.apply(PersistentIndexUpdate {
			put_records: vec![record(&foreign, 1)],
			..Default::default()
		})
		.await
		.unwrap();
	assert_eq!(
		index
			.begin_parent_intent(intent(7, 0, vec![foreign]))
			.await
			.unwrap(),
		Err(IntentRefusal::ForeignVersion)
	);
	// One version twice with different content; unsorted; empty.
	let mut twin = version(5, 30);
	twin.row_digest = [0; 32];
	for manifest in [
		vec![version(5, 30), twin],
		vec![version(6, 30), version(5, 30)],
		Vec::new(),
	] {
		assert_eq!(
			index
				.begin_parent_intent(intent(8, 0, manifest))
				.await
				.unwrap(),
			Err(IntentRefusal::MalformedManifest)
		);
	}
	assert_eq!(index.parent_intent(7, 0).await.unwrap(), None);
}

#[tokio::test]
async fn states_move_only_forward_and_refused_admits_nothing_more() {
	let (_dir, path) = temp_path("states");
	let index = PersistentLocalIndex::initialize_new(path, None)
		.await
		.unwrap();
	let manifest = vec![version(1, 40)];
	index
		.begin_parent_intent(intent(9, 1, manifest.clone()))
		.await
		.unwrap()
		.unwrap();
	use ParentIntentState::{Complete, Intent, Refused};
	assert_eq!(
		index
			.set_parent_intent_state(9, 1, &[Intent], Complete)
			.await
			.unwrap(),
		Ok(())
	);
	assert_eq!(
		index
			.set_parent_intent_state(9, 1, &[Intent], Complete)
			.await
			.unwrap(),
		Ok(()),
		"already there"
	);
	assert_eq!(
		index
			.set_parent_intent_state(10, 0, &[Intent], Complete)
			.await
			.unwrap(),
		Err(IntentRefusal::Missing)
	);
	assert_eq!(
		index
			.set_parent_intent_state(9, 1, &[Intent, Complete], Refused)
			.await
			.unwrap(),
		Ok(())
	);
	assert_eq!(
		index
			.set_parent_intent_state(9, 1, &[Intent], Complete)
			.await
			.unwrap(),
		Err(IntentRefusal::WrongState),
		"Refused is never completed"
	);
	assert_eq!(
		index
			.begin_parent_intent(intent(9, 1, manifest.clone()))
			.await
			.unwrap(),
		Err(IntentRefusal::Refused),
		"a late writer cannot restart a refused intent"
	);
	// The owner entries stay, so the versions remain identified for that owner.
	assert_eq!(
		index.version_owner(TENANT, manifest[0]).await.unwrap(),
		Some((9, 1))
	);
}

#[tokio::test]
async fn intents_and_owners_survive_the_index_rebuild_and_a_reopen() {
	let (_dir, path) = temp_path("reopen");
	let index = PersistentLocalIndex::initialize_new(path.clone(), None)
		.await
		.unwrap();
	let manifest = vec![version(1, 50)];
	index
		.begin_parent_intent(intent(11, 0, manifest.clone()))
		.await
		.unwrap()
		.unwrap();
	// The open-time rebuild rewrites the record tables from slab scans.
	index.rebuild(Vec::new(), Vec::new(), None).await.unwrap();
	drop(index);
	let index = PersistentLocalIndex::open(path.clone()).await.unwrap();
	assert_eq!(
		index
			.parent_intent(11, 0)
			.await
			.unwrap()
			.map(|p| p.manifest),
		Some(manifest.clone())
	);
	assert_eq!(
		index.version_owner(TENANT, manifest[0]).await.unwrap(),
		Some((11, 0))
	);
	assert_eq!(
		index.owned_version_copies(TENANT, manifest).await.unwrap(),
		vec![0],
		"no rows were appended"
	);
}

#[tokio::test]
async fn an_initialized_store_missing_its_intent_tables_refuses_to_open() {
	let (_dir, path) = temp_path("missing");
	let index = PersistentLocalIndex::initialize_new(path.clone(), None)
		.await
		.unwrap();
	index
		.begin_parent_intent(intent(12, 0, vec![version(1, 60)]))
		.await
		.unwrap()
		.unwrap();
	drop(index);
	{
		let db = Database::open(&path).unwrap();
		let tx = db.begin_write().unwrap();
		tx.delete_table(PARENT_ROW_OWNERS_TABLE).unwrap();
		tx.commit().unwrap();
	}
	let error = PersistentLocalIndex::open(path).await.err().unwrap();
	assert_eq!(error.kind(), ErrorKind::InvalidData);
	assert!(
		error.to_string().contains("parent intent authority"),
		"{error}"
	);
}

#[tokio::test]
async fn a_store_that_never_wrote_an_intent_has_no_intent_tables() {
	let (_dir, path) = temp_path("today");
	let index = PersistentLocalIndex::initialize_new(path.clone(), None)
		.await
		.unwrap();
	assert_eq!(index.parent_intent(1, 0).await.unwrap(), None);
	drop(index);
	let db = Database::open(&path).unwrap();
	let tx = db.begin_read().unwrap();
	let names: Vec<String> = tx
		.list_tables()
		.unwrap()
		.map(|table| table.name().to_owned())
		.collect();
	assert!(
		!names.iter().any(|name| name.starts_with("parent_")),
		"{names:?}"
	);
}
