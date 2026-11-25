//! Wendelon ADR 0130: a certification build's survivor proofs are read early, a bounded window
//! at a time, and used at a survivor's logical turn only if the ordinary proof would then read
//! exactly the same bytes.
//!
//! On a real one-voter ParentDecisions store, through the ordinary maintenance pass:
//! - the candidate equals the one computed from the same inputs with every survivor proved on
//!   the ordinary path, with about one frame read per frame per window, inside the scratch
//!   bound;
//! - a change made after the early proofs and before a survivor's turn (the store-wide
//!   retirement fence raised; a local store gone from under an unchanged index; a second copy
//!   changed with the first unchanged) gives the build the ordinary proof's outcome, never the
//!   early proof's;
//! - a version that cannot be proved early as a whole (too many copies, no room for its
//!   witnesses, a copy whose backing is not a local store, a differing later copy) is proved on
//!   the ordinary path, with no early accounting of a missing slab;
//! - the stop is observed between two early proofs.
//!
//! On a three-node cluster, a build that repairs a lost survivor from a peer drops the early
//! proofs made before the repair and completes.
//!
//! "The ordinary outcome" is taken from a second store built the same way whose builds prove
//! every survivor on the ordinary path, changed at the equivalent point.

use super::one_voter::{one_voter, open, until_settled as until_one_settled};
use super::verifier_frames::{lineitem_like, lineitem_rows};
use super::*;
use crate::network::data_server::certification::survivor_proofs::{
	PROOF_SCRATCH_BYTES, ProofReadStats,
};
use crate::network::data_server::child_writes::PauseStage;
use crate::schema::Identity;
use crate::storage::index::certified_base::BaseEntry;
use std::time::Duration;

type Version = (TenantId, [u8; 32], crate::schema::HybridTimestamp, [u8; 32]);

struct Store {
	_dir: crate::test_support::TestDir,
	node: WendelonNode,
	descriptor: TableSchemaDescriptor,
	rows: u64,
}

/// A one-voter store with the lineitem-like table, nothing written.
async fn store() -> Store {
	let (dir, cfg) = one_voter();
	let node = open(&cfg).await;
	until_one_settled(&node, None).await;
	let descriptor = lineitem_like();
	node.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_one_settled(&node, Some(descriptor.table_object_id)).await;
	Store {
		_dir: dir,
		node,
		descriptor,
		rows: 0,
	}
}

impl Store {
	/// One write of `rows` rows: one child.
	async fn write(&mut self, rows: usize) {
		assert_committed(
			&write_retrying(
				&self.node.data_server,
				write_meta(&self.descriptor, rows, uuid_v7()),
				lineitem_rows(&self.descriptor, self.rows as i64, rows),
			)
			.await,
			"a fixture write",
		);
		self.rows += rows as u64;
	}

	/// Every version the producers recorded, in the candidate's logical (key) order.
	async fn versions(&self) -> Vec<Version> {
		let index = self.node.slab.persistent_index().unwrap();
		let mut versions = Vec::new();
		for intent in complete_intents(&index).await {
			for version in intent.manifest {
				versions.push((
					intent.tenant,
					version.key_identity,
					version.hlc,
					version.row_digest,
				));
			}
		}
		versions.sort_by_key(|version| version.1);
		assert_eq!(versions.len() as u64, self.rows, "one version per row");
		versions
	}

	fn locations(&self, version: &Version) -> Vec<crate::storage::core::RecordLocation> {
		self.node
			.slab
			.version_locations(version.0, Identity::new(version.1), version.2)
			.unwrap()
	}

	/// A floor advance that makes a generation target due: the maintenance pass then builds.
	async fn due(&self) {
		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let floor_ms = now + 60_000;
		self.node
			.raft
			.client_write(crate::consensus::WendelonRequest::Decision(
				crate::consensus::decisions::DecisionCommand::AdvanceFloor {
					floor_ms,
					stamp_ms: floor_ms + crate::consensus::decisions::DECISION_EXPIRY_MS,
				},
			))
			.await
			.unwrap();
	}

	/// The first build's outcome: its candidate (staged, or already installed), or its refusal.
	async fn outcome(&self) -> Result<Vec<BaseEntry>, String> {
		let index = self.node.slab.persistent_index().unwrap();
		let refusals = self.node.data_server.build_refusals_for_test();
		let deadline = tokio::time::Instant::now() + BOUND;
		loop {
			if let Some(refusal) = refusals.lock().unwrap().first() {
				return Err(refusal.clone());
			}
			let meta = index.base_meta().await.unwrap();
			let entries = index.base_entries(meta.staged.is_some()).await.unwrap();
			if !entries.is_empty() {
				return Ok(entries);
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"no candidate and no refusal within the bound"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}

	/// The first build's proof counts.
	fn proofs(&self) -> ProofReadStats {
		self.node.data_server.proof_reads_for_test().lock().unwrap()[0]
	}

	/// List `location` as one more copy of `version`, last.
	fn list(&self, version: &Version, location: crate::storage::core::RecordLocation) {
		self.node
			.slab
			.list_location_for_test(version.0, Identity::new(version.1), location, false);
	}

	/// Another stored row's location, renamed to `version`'s HLC: a listed copy whose bytes are
	/// another row's.
	fn another_rows_location(
		&self,
		versions: &[Version],
		version: &Version,
	) -> crate::storage::core::RecordLocation {
		let own = self.locations(version)[0];
		let mut other = versions
			.iter()
			.filter(|other| other.1 != version.1)
			.map(|other| self.locations(other)[0])
			.find(|location| location.record_row_index != own.record_row_index)
			.expect("another row");
		other.hlc = version.2;
		other
	}

	/// `version`'s copies become its first copy and `second`.
	fn relist(&self, version: &Version, second: crate::storage::core::RecordLocation) {
		let first = self.locations(version)[0];
		self.node
			.slab
			.forget_version_for_test(version.0, Identity::new(version.1), version.2);
		self.list(version, first);
		self.list(version, second);
	}

	/// Hold the next build at `stage`; returns once it is there.
	fn hold(&self, stage: PauseStage) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
		self.node.data_server.pause_coordinator_for_test(stage)
	}
}

async fn reached(reached: &tokio::sync::Notify) {
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the build reaches its hold");
}

/// A store whose builds prove early and one whose builds prove every survivor on the ordinary
/// path, each with the same writes.
async fn pair(writes: &[usize]) -> (Store, Store) {
	let (mut early, mut ordinary) = (store().await, store().await);
	ordinary.node.data_server.prove_ungrouped_for_test();
	for rows in writes {
		early.write(*rows).await;
		ordinary.write(*rows).await;
	}
	(early, ordinary)
}

fn assert_candidate(entries: &[BaseEntry], versions: &[Version]) {
	assert_eq!(entries.len(), versions.len(), "one entry per survivor");
	for (entry, version) in entries.iter().zip(versions) {
		assert_eq!(
			(
				entry.tenant,
				entry.key_identity,
				entry.hlc,
				entry.row_digest
			),
			*version,
			"an entry is its recorded version"
		);
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn early_proofs_stage_the_ordinary_candidate_with_about_one_read_a_frame() {
	let mut store = store().await;
	for _ in 0..3 {
		store.write(2048).await;
	}
	let versions = store.versions().await;
	let frames: std::collections::BTreeSet<_> = versions
		.iter()
		.map(|version| {
			let location = store.locations(version)[0];
			(location.slab_id, location.offset)
		})
		.collect();
	store.node.data_server.compare_ungrouped_for_test();
	store.due().await;
	let entries = store.outcome().await.expect("a candidate");
	assert_candidate(&entries, &versions);
	assert_eq!(
		store.node.data_server.ungrouped_equal_for_test(),
		Some(true),
		"the same inputs proved on the ordinary path give the same entries and aged omission"
	);
	let proofs = store.proofs();
	eprintln!("ADR0130 early proofs: {proofs:?}; frames {}", frames.len());
	assert!(
		proofs.window_rows < store.rows
			&& proofs.windows == store.rows.div_ceil(proofs.window_rows),
		"the fixture takes more than one window: {proofs:?}"
	);
	assert_eq!(
		(proofs.scheduled, proofs.saved, proofs.used),
		(store.rows, store.rows, store.rows),
		"every survivor is proved early and its proof used"
	);
	assert_eq!(
		(
			proofs.fallbacks,
			proofs.not_saved,
			proofs.discarded,
			proofs.ordinary
		),
		(0, 0, 0, 0),
		"no ordinary proof"
	);
	assert!(
		proofs.frame_reads >= frames.len() as u64
			&& proofs.frame_reads <= proofs.windows * frames.len() as u64,
		"each window reads a frame at most once: {proofs:?}, {} frames",
		frames.len()
	);
	assert_eq!(proofs.frame_reads + proofs.frame_hits, store.rows);
	assert!(
		proofs.reserved_bytes <= PROOF_SCRATCH_BYTES as u64,
		"the scratch stays under its bound: {proofs:?}"
	);
	assert_eq!(proofs.stores, 1);
	assert_eq!(
		store.node.slab.verifier_frame_allowance().used(),
		0,
		"no frame stays charged"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_fence_raised_after_the_early_proofs_refuses_as_the_ordinary_proof_does() {
	let (early, ordinary) = pair(&[1024, 1024]).await;
	let slab = early.locations(&early.versions().await[0])[0].slab_id;

	let (held, release) = early.hold(PauseStage::CertificationEarlyProofsMade);
	early.due().await;
	reached(&held).await;
	assert!(!early.node.slab.retirement_fence_stands_for_test());
	early.node.slab.raise_retirement_fence_for_test(slab);
	assert!(early.node.slab.retirement_fence_stands_for_test());
	release.notify_one();
	let refused = early
		.outcome()
		.await
		.expect_err("the fenced build stages nothing");
	assert!(early.node.slab.retirement_fence_stands_for_test());

	let (held, release) = ordinary.hold(PauseStage::CertificationBetweenProofs);
	ordinary.due().await;
	reached(&held).await;
	ordinary.node.slab.raise_retirement_fence_for_test(slab);
	release.notify_one();
	let expected = ordinary
		.outcome()
		.await
		.expect_err("the ordinary build refuses");

	assert_eq!(refused, expected, "the ordinary proof's refusal");
	assert!(refused.contains("fenced"), "{refused}");
	let proofs = early.proofs();
	eprintln!("ADR0130 fence: {proofs:?}");
	assert_eq!(
		(proofs.saved, proofs.used, proofs.fallbacks, proofs.ordinary),
		(early.rows, 0, 0, 0),
		"every survivor was proved early; the first check's own listing refused"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_store_gone_from_under_an_unchanged_index_gives_the_ordinary_outcome() {
	let (early, ordinary) = pair(&[1024, 1024]).await;
	let first = early.versions().await[0];
	let location = early.locations(&first)[0];
	let slab = early.node.slab.force_seal_active_for_test().await;
	assert_eq!(slab, location.slab_id);
	assert_eq!(ordinary.node.slab.force_seal_active_for_test().await, slab);

	let (held, release) = early.hold(PauseStage::CertificationEarlyProofsMade);
	early.due().await;
	reached(&held).await;
	assert_eq!(
		early.node.slab.backing_class_for_test(location).await,
		"local"
	);
	assert!(early.node.slab.drop_sealed_store_for_test(slab).await);
	assert_eq!(
		early.node.slab.backing_class_for_test(location).await,
		"missing"
	);
	assert_eq!(
		early.locations(&first),
		vec![location],
		"the index is unchanged"
	);
	assert!(!early.node.slab.is_compaction_slab_quarantined(slab));
	release.notify_one();
	let refused = early.outcome().await.expect_err("nothing is staged");

	let (held, release) = ordinary.hold(PauseStage::CertificationBetweenProofs);
	ordinary.due().await;
	reached(&held).await;
	assert!(ordinary.node.slab.drop_sealed_store_for_test(slab).await);
	release.notify_one();
	let expected = ordinary
		.outcome()
		.await
		.expect_err("the ordinary build refuses");

	assert_eq!(refused, expected, "the ordinary proof's refusal");
	assert!(refused.contains("no slab with id"), "{refused}");
	let proofs = early.proofs();
	eprintln!("ADR0130 backing: {proofs:?}");
	assert_eq!(
		(proofs.saved, proofs.used, proofs.fallbacks, proofs.ordinary),
		(early.rows, 0, 1, 1),
		"the first early proof was rejected and its survivor went to the ordinary proof, which refused"
	);
	assert_eq!(
		early.node.slab.is_compaction_slab_quarantined(slab),
		ordinary.node.slab.is_compaction_slab_quarantined(slab),
		"the missing slab is accounted as the ordinary read accounts it"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_second_copy_changed_with_the_first_unchanged_gives_the_ordinary_outcome() {
	let (early, ordinary) = pair(&[1024, 1024]).await;
	for store in [&early, &ordinary] {
		let versions = store.versions().await;
		let last = *versions.last().unwrap();
		store.list(&last, store.locations(&last)[0]);
		assert_eq!(store.locations(&last).len(), 2);
	}
	let change = |store: &Store, versions: &[Version]| {
		let last = *versions.last().unwrap();
		let before = store.locations(&last);
		store.relist(&last, store.another_rows_location(versions, &last));
		let after = store.locations(&last);
		assert_eq!(after.len(), 2);
		assert_eq!(after[0], before[0], "the first copy is unchanged");
		assert_ne!(after[1], before[1], "the second copy changed");
	};

	let versions = early.versions().await;
	let (held, release) = early.hold(PauseStage::CertificationEarlyProofsMade);
	early.due().await;
	reached(&held).await;
	change(&early, &versions);
	release.notify_one();
	let refused = early.outcome().await.expect_err("nothing is staged");

	let versions = ordinary.versions().await;
	let (held, release) = ordinary.hold(PauseStage::CertificationBetweenProofs);
	ordinary.due().await;
	reached(&held).await;
	change(&ordinary, &versions);
	release.notify_one();
	let expected = ordinary
		.outcome()
		.await
		.expect_err("the ordinary build refuses");

	assert_eq!(refused, expected, "the ordinary proof's repair and refusal");
	let proofs = early.proofs();
	eprintln!("ADR0130 second copy: {proofs:?}");
	assert_eq!(
		(proofs.saved, proofs.used, proofs.fallbacks),
		(early.rows, early.rows - 1, 1),
		"only the changed version's early proof was rejected"
	);
	assert_eq!(
		proofs.witnesses,
		early.rows + 1,
		"both copies were witnessed"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn versions_that_do_not_fit_are_proved_on_the_ordinary_path_as_a_whole() {
	let mut store = store().await;
	store.write(512).await;
	let versions = store.versions().await;
	// One version of nine copies: more than a version may have witnessed.
	let nine = versions[0];
	for _ in 0..8 {
		store.list(&nine, store.locations(&nine)[0]);
	}
	// A hundred versions of two copies: more witnesses than the window's arena holds.
	for version in &versions[1..=100] {
		store.list(version, store.locations(version)[0]);
	}
	store.node.data_server.compare_ungrouped_for_test();
	store.due().await;
	let entries = store.outcome().await.expect("a candidate");
	assert_candidate(&entries, &versions);
	assert_eq!(
		store.node.data_server.ungrouped_equal_for_test(),
		Some(true)
	);
	let proofs = store.proofs();
	eprintln!("ADR0130 whole-version fallback: {proofs:?}");
	assert!(proofs.not_saved > 0, "the arena ran out: {proofs:?}");
	assert_eq!(
		proofs.saved + proofs.not_saved + 1,
		store.rows,
		"every survivor but the nine-copy one was proved early"
	);
	assert_eq!(proofs.used, proofs.saved);
	assert_eq!(
		proofs.ordinary,
		proofs.not_saved + 1,
		"each version that did not fit, and the nine-copy one, had one ordinary proof"
	);
	assert!(proofs.reserved_bytes <= PROOF_SCRATCH_BYTES as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_differing_later_copy_is_met_by_the_ordinary_proof() {
	let (early, ordinary) = pair(&[1024]).await;
	for store in [&early, &ordinary] {
		let versions = store.versions().await;
		let last = *versions.last().unwrap();
		store.relist(&last, store.another_rows_location(&versions, &last));
		store.due().await;
	}
	let refused = early.outcome().await.expect_err("nothing is staged");
	let expected = ordinary
		.outcome()
		.await
		.expect_err("the ordinary build refuses");
	assert_eq!(refused, expected, "the ordinary proof's repair and refusal");
	let proofs = early.proofs();
	eprintln!("ADR0130 differing later copy: {proofs:?}");
	assert_eq!(
		(proofs.saved, proofs.used),
		(early.rows - 1, early.rows - 1),
		"the differing version was not saved; every other early proof was used"
	);
	assert!(proofs.ordinary >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_copy_that_is_not_a_local_stores_leaves_its_version_to_the_ordinary_proof() {
	let (mut early, mut ordinary) = pair(&[1024]).await;
	let mut slabs = Vec::new();
	for store in [&mut early, &mut ordinary] {
		let sealed = store.node.slab.force_seal_active_for_test().await;
		let older = store.versions().await;
		store.write(1024).await;
		let versions = store.versions().await;
		// A version in the active slab with a second copy that names the sealed slab.
		let mixed = *versions
			.iter()
			.find(|version| store.locations(version)[0].slab_id != sealed)
			.unwrap();
		let mut second = store.locations(&older[0])[0];
		assert_eq!(second.slab_id, sealed);
		second.hlc = mixed.2;
		store.list(&mixed, second);
		assert!(store.node.slab.drop_sealed_store_for_test(sealed).await);
		slabs.push(sealed);
	}
	let sealed = slabs[0];

	let (held, release) = early.hold(PauseStage::CertificationEarlyProofsMade);
	early.due().await;
	reached(&held).await;
	let proofs = early.proofs();
	eprintln!("ADR0130 not a local store's, at the hold: {proofs:?}");
	assert_eq!(
		proofs.saved,
		1024 - 1,
		"only versions wholly in the local store were saved: not the sealed slab's, not the mixed one"
	);
	assert!(
		!early.node.slab.is_compaction_slab_quarantined(sealed),
		"an early proof accounts no missing slab"
	);
	release.notify_one();
	let refused = early.outcome().await.expect_err("nothing is staged");

	ordinary.due().await;
	let expected = ordinary
		.outcome()
		.await
		.expect_err("the ordinary build refuses");
	assert_eq!(refused, expected, "the ordinary proof's refusal");
	assert!(refused.contains("no slab with id"), "{refused}");
	assert_eq!(
		early.node.slab.is_compaction_slab_quarantined(sealed),
		ordinary.node.slab.is_compaction_slab_quarantined(sealed),
		"the missing slab is accounted by the ordinary proof, as on the ordinary path"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_stop_between_two_early_proofs_stages_nothing() {
	let mut store = store().await;
	store.write(1024).await;
	let (held, release) = store.hold(PauseStage::CertificationBetweenEarlyProofs);
	let mut stop = store.node.data_server.decision_maintenance_stop();
	let (refusals, proofs) = (
		store.node.data_server.build_refusals_for_test(),
		store.node.data_server.proof_reads_for_test(),
	);
	store.due().await;
	reached(&held).await;
	let Store {
		_dir, node, rows, ..
	} = store;
	let closing = tokio::spawn(node.shutdown());
	tokio::time::timeout(Duration::from_secs(5), stop.wait_for(|stopping| *stopping))
		.await
		.expect("the stop is visible")
		.unwrap();
	release.notify_one();
	tokio::time::timeout(Duration::from_secs(30), closing)
		.await
		.expect("a bounded close")
		.unwrap()
		.expect("an orderly close");
	assert_eq!(
		*refusals.lock().unwrap(),
		vec!["the maintenance stop was observed".to_owned()],
		"the build stopped and staged nothing"
	);
	let proofs = proofs.lock().unwrap()[0];
	eprintln!("ADR0130 stop: {proofs:?}");
	assert!(
		proofs.saved < rows && proofs.used == 0,
		"it stopped among its early proofs: {proofs:?}"
	);
}

/// THREE NODES, A REAL REPAIR: the leader has lost its logically first survivor. Its build
/// proves the others early, meets the lost one on the ordinary path, repairs its child from a
/// peer, drops every early proof made before the repair, proves them again and completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_repair_drops_the_early_proofs_made_before_it() {
	let (cluster, leader, descriptor) = ready().await;
	let mut expected = Expected::new();
	for id in 1..=8 {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "a".into());
	}
	let index = cluster.node(leader).slab.persistent_index().unwrap();
	let lost = complete_intents(&index)
		.await
		.into_iter()
		.flat_map(|intent| intent.manifest)
		.min_by_key(|version| version.key_identity)
		.expect("a survivor");
	cluster.node(leader).slab.forget_version_for_test(
		TenantId::ZERO,
		Identity::new(lost.key_identity),
		lost.hlc,
	);
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	assert!(
		!cluster
			.node(leader)
			.slab
			.version_locations(TenantId::ZERO, Identity::new(lost.key_identity), lost.hlc)
			.unwrap()
			.is_empty(),
		"the lost survivor was repaired"
	);
	let builds = cluster
		.node(leader)
		.data_server
		.proof_reads_for_test()
		.lock()
		.unwrap()
		.clone();
	eprintln!("ADR0130 repair: {builds:?}");
	let repairing = builds
		.iter()
		.find(|proofs| proofs.discarded > 0)
		.expect("a build that repaired");
	assert_eq!(
		(repairing.saved, repairing.discarded, repairing.used),
		(7, 7, 0),
		"the seven early proofs made before the repair were dropped, not used"
	);
	assert_eq!(
		repairing.ordinary, 9,
		"the lost survivor twice, around its repair, and the seven others once each"
	);
	assert_eq!(index.base_entries(false).await.unwrap().len(), 8);
	for node in NODES {
		assert_eq!(
			cluster.read_once_validated(node, &descriptor).await,
			expected,
			"at {node}"
		);
	}
}
