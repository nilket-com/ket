//! ADR 0111 regression (test-only). A first tier must never publish a sealed source that local
//! maintenance made non-Live, and maintenance must never make a sealed source non-Live while an
//! unresolved first-tier row names it. Two typed invariants, checked directly:
//! - I1: no published tier pointer names a slab whose manifest run is non-Live;
//! - I2: no maintenance pass makes S non-Live while an unresolved first-tier op row names S.
//!
//! The committer is a faithful one-node ring: it applies the real `SlabOp`, `RegisterIncarnation`
//! and `CancelOp` requests through `RaftStateMachine::apply` on a state machine carrying the real
//! `ManagerTierObserver`, answers `op_view` from that state machine, and checks receipts with the
//! production `verify_receipt`. Its declared fault modes act only on a Tier `submit_op`, after the
//! op row was allocated: `PauseTier` waits before the request is applied (before linearization);
//! `LoseTier` keeps the request unapplied and answers with an unknown outcome, and `deliver_held`
//! applies it later (a sent proposal that the ring decides after the caller gave up).

use super::*;
use crate::consensus::WendelonRequest;
use crate::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableRetentionPolicy, TableSchemaDescriptor,
};
use crate::storage::core::StorageEngine;
use crate::storage::index::LoadedOp;
use crate::storage::retention::MvccRetentionPolicy;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
	Pass,
	PauseTier,
	LoseTier,
	/// Keep the next Compaction request unapplied and answer with an unknown outcome.
	LoseCompaction,
}

struct RingFaultCommitter {
	sm: WendelonStateMachine,
	index: AtomicU64,
	mode: std::sync::Mutex<Mode>,
	/// When set, the Tier faults act only on a Tier naming this slab.
	target: std::sync::Mutex<Option<SlabId>>,
	entered: Arc<tokio::sync::Semaphore>,
	release: Arc<tokio::sync::Semaphore>,
	held: tokio::sync::Mutex<Option<WendelonRequest>>,
}

impl RingFaultCommitter {
	fn new(sm: WendelonStateMachine, mode: Mode) -> Self {
		Self {
			sm,
			index: AtomicU64::new(100),
			mode: std::sync::Mutex::new(mode),
			target: std::sync::Mutex::new(None),
			entered: Arc::new(tokio::sync::Semaphore::new(0)),
			release: Arc::new(tokio::sync::Semaphore::new(0)),
			held: tokio::sync::Mutex::new(None),
		}
	}

	async fn apply(
		&self,
		request: WendelonRequest,
	) -> std::io::Result<crate::consensus::WendelonResponse> {
		use openraft::storage::RaftStateMachine;
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
		let mut target = self.sm.clone();
		let mut responses = RaftStateMachine::apply(
			&mut target,
			[Entry {
				log_id: LogId::new(
					CommittedLeaderId::new(3, 1),
					self.index.fetch_add(1, Ordering::SeqCst),
				),
				payload: EntryPayload::Normal(request),
			}],
		)
		.await
		.map_err(|e| std::io::Error::other(e.to_string()))?;
		responses
			.pop()
			.ok_or_else(|| std::io::Error::other("the state machine returned no response"))
	}

	fn receipt(
		ticket: &OpTicket,
		response: crate::consensus::WendelonResponse,
	) -> std::io::Result<VerifiedReceipt> {
		use crate::consensus::WendelonResponse;
		use crate::consensus::slab_ops::OpResponse;
		match response {
			WendelonResponse::Op(OpResponse::Receipt(receipt)) => verify_receipt(ticket, receipt),
			other => Err(std::io::Error::other(format!(
				"unexpected response: {other:?}"
			))),
		}
	}

	/// Apply the request a `LoseTier` submission kept (the ring decides it late).
	async fn deliver_held(&self) -> std::io::Result<crate::consensus::WendelonResponse> {
		let request = self.held.lock().await.take().expect("a held Tier request");
		self.apply(request).await
	}
}

#[async_trait]
impl TierCommitter for RingFaultCommitter {
	async fn submit_op(&self, ticket: &OpTicket) -> std::io::Result<VerifiedReceipt> {
		let owner = ticket.key().owner;
		self.apply(WendelonRequest::RegisterIncarnation(
			crate::consensus::RegisterIncarnation {
				node: owner.node(),
				incarnation: owner.incarnation(),
				supersedes: None,
			},
		))
		.await?;
		let request = WendelonRequest::SlabOp(crate::consensus::slab_ops::SlabOp {
			op_seq: ticket.key().op_seq,
			command: ticket.command().clone(),
		});
		let mode = *self.mode.lock().unwrap();
		if mode == Mode::LoseCompaction
			&& matches!(
				ticket.command(),
				crate::consensus::slab_ops::SlabCommand::Compaction(_)
			) {
			*self.held.lock().await = Some(request);
			return Err(std::io::Error::other(
				"test harness: the Compaction proposal was sent and its answer was lost",
			));
		}
		let targeted = match ticket.command() {
			crate::consensus::slab_ops::SlabCommand::Tier(tier) => self
				.target
				.lock()
				.unwrap()
				.is_none_or(|target| tier.tier.slab.local_id() == target),
			_ => false,
		};
		if targeted {
			match mode {
				Mode::Pass | Mode::LoseCompaction => {}
				Mode::PauseTier => {
					self.entered.add_permits(1);
					self.release.acquire().await.unwrap().forget();
				}
				Mode::LoseTier => {
					*self.held.lock().await = Some(request);
					return Err(std::io::Error::other(
						"test harness: the Tier proposal was sent and its answer was lost",
					));
				}
			}
		}
		let response = self.apply(request).await?;
		Self::receipt(ticket, response)
	}

	async fn cancel_op(&self, ticket: &OpTicket) -> std::io::Result<VerifiedReceipt> {
		let response = self
			.apply(WendelonRequest::CancelOp(
				crate::consensus::slab_ops::CancelOp {
					key: ticket.key(),
					target_digest: ticket.digest(),
				},
			))
			.await?;
		Self::receipt(ticket, response)
	}

	async fn ack_outcomes(
		&self,
		owner: crate::consensus::OwnerRef,
		seqs: Vec<u64>,
	) -> std::io::Result<crate::consensus::slab_ops::AckReceipt> {
		use crate::consensus::WendelonResponse;
		use crate::consensus::slab_ops::{AckOutcomes, OpResponse};
		match self
			.apply(WendelonRequest::AckOutcomes(AckOutcomes {
				owner,
				seqs: seqs.clone(),
			}))
			.await?
		{
			WendelonResponse::Op(OpResponse::Ack(receipt))
				if ack_receipt_answers(&receipt, owner, &seqs) =>
			{
				Ok(receipt)
			}
			other => Err(std::io::Error::other(format!(
				"unexpected Ack response: {other:?}"
			))),
		}
	}

	async fn op_view(
		&self,
		key: crate::consensus::slab_ops::OpKey,
		digest: [u8; 32],
		slabs: Vec<crate::consensus::SlabRef>,
	) -> std::io::Result<crate::consensus::slab_ops::OpView> {
		Ok(self.sm.op_view(key, digest, slabs).await)
	}
}

fn append_only_descriptor() -> TableSchemaDescriptor {
	use arrow::datatypes::{DataType, Field, Schema};
	let schema = Schema::new(vec![Field::new("value", DataType::UInt64, false)]);
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x62; 16]),
		1,
		"analytics",
		"retired_source_events",
		&schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

/// One store in the frozen scenario's shape: an AppendOnly table inheriting a 15 s cluster window,
/// now = 100 s (floor 85 s). Three sealed slabs in id order:
/// - S: one row at 30 s (expired) and two at 90 and 95 s: L1 retention REPLACES it;
/// - P: one row at 31 s (expired) and one at 98 s: an independent eligible slab;
/// - L: two rows at 96 and 97 s, never maintained: the legacy missing-run control.
struct Fixture {
	manager: Arc<SlabManager>,
	sm: WendelonStateMachine,
	committer: Arc<RingFaultCommitter>,
	sweeper: SlabSweeper,
	l2: Arc<dyn ObjectStore>,
	store: Arc<PausingStore>,
	schemas: crate::schema::table::TableSchemaCatalog,
	lifecycle: crate::schema::TableLifecycleCatalog,
	source: SlabId,
	independent: SlabId,
	legacy: SlabId,
	keys: [crate::schema::Identity; 7],
	crypto: Arc<CryptoManager>,
	l1_dir: crate::test_support::TestDir,
	_l2_dir: crate::test_support::TestDir,
}

const NOW_MS: u64 = 100_000;

fn policy() -> MvccRetentionPolicy {
	MvccRetentionPolicy::FixedWindow(Duration::from_secs(15))
}

fn manager_config(
	dir: &std::path::Path,
	l2: Arc<dyn ObjectStore>,
	crypto: Arc<CryptoManager>,
) -> SlabManagerConfig {
	SlabManagerConfig {
		tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
		dir: dir.to_path_buf(),
		max_slab_bytes: 1 << 20,
		io: spawn_io_thread(64).unwrap(),
		l2: Some(l2),
		rehydration: Default::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: Default::default(),
		crypto,
	}
}

async fn fixture(tag: &str, mode: Mode) -> Fixture {
	fixture_with(tag, mode, false).await
}

/// `full_prune`: every S row is expired, so L1 retention retires S with no replacement.
async fn fixture_with(tag: &str, mode: Mode, full_prune: bool) -> Fixture {
	let l1_dir = fresh_dir(&format!("{tag}-l1"));
	let l2_dir = fresh_dir(&format!("{tag}-l2"));
	let plain: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
	let store = Arc::new(PausingStore::new(plain));
	let l2: Arc<dyn ObjectStore> = store.clone();
	let crypto = test_crypto();
	let manager =
		SlabManager::initialize_new_store(manager_config(&l1_dir, l2.clone(), crypto.clone()))
			.await
			.unwrap();
	manager.bind_control_node(1).unwrap();
	let descriptor = append_only_descriptor();
	let mut schemas = crate::schema::table::TableSchemaCatalog::default();
	schemas.upsert(tenant(), descriptor.clone()).unwrap();
	let mut lifecycle = crate::schema::TableLifecycleCatalog::default();
	lifecycle
		.set_next_generation(
			tenant(),
			descriptor.table_object_id,
			crate::schema::PolicyGeneration::ZERO,
			TableRetentionPolicy::InheritCluster,
			hlc_at(1),
		)
		.unwrap();
	let keys = [0x01, 0x02, 0x03, 0x11, 0x12, 0x21, 0x22].map(|n| id(0x6200 + n));
	let slabs = [
		if full_prune {
			vec![(keys[0], 30_000), (keys[1], 32_000), (keys[2], 33_000)]
		} else {
			vec![(keys[0], 30_000), (keys[1], 90_000), (keys[2], 95_000)]
		},
		vec![(keys[3], 31_000), (keys[4], 98_000)],
		vec![(keys[5], 96_000), (keys[6], 97_000)],
	];
	let mut sealed = Vec::new();
	for rows in slabs {
		let rows = rows
			.iter()
			.enumerate()
			.map(|(i, (key, at))| (*key, Locality::new(0x62), hlc_at(*at), i as u64 + 1, false))
			.collect::<Vec<_>>();
		let frame = crate::storage::manager::encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&rows,
		);
		let entries = crate::storage::core::storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.unwrap();
		manager
			.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();
		sealed.push(manager.force_seal_active_for_test().await);
	}
	let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
	let sm = WendelonStateMachine::new().with_tier_observer(observer);
	let mut state = sm.state_snapshot().await;
	state.table_schemas.upsert(tenant(), descriptor).unwrap();
	state.table_lifecycle = lifecycle.clone();
	sm.seed_complete_snapshot_state_for_test(state).await;
	// The ring holds the store's tenant keysets: an encrypted pointer is accepted only for a
	// present version (ADR 0728), as the offload fixtures seed them.
	sm.seed_tenant_keysets_for_test(
		manager
			.retained_cleanup_test_crypto()
			.tenant_keysets_snapshot()
			.unwrap(),
	)
	.await;
	let committer = Arc::new(RingFaultCommitter::new(sm.clone(), mode));
	let sweeper = SlabSweeper::new(
		manager.clone(),
		l2.clone(),
		committer.clone(),
		sm.clone(),
		SweeperConfig {
			scan_interval: Duration::from_millis(10),
			op_driver_interval: Duration::from_millis(500),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: "tier/".into(),
			max_plaintext_rewrites_per_pass: 1,
			max_dek_rewraps_per_pass: 1,
		},
	)
	.with_clock(Arc::new(FixedClock { now_ms: NOW_MS }))
	.with_mvcc_retention_window(Some(Duration::from_secs(15)));
	Fixture {
		manager,
		sm,
		committer,
		sweeper,
		l2,
		store,
		schemas,
		lifecycle,
		source: sealed[0],
		independent: sealed[1],
		legacy: sealed[2],
		keys,
		crypto,
		l1_dir,
		_l2_dir: l2_dir,
	}
}

impl Fixture {
	async fn l1_retention_pass_at(
		&self,
		now_ms: u64,
	) -> std::io::Result<crate::storage::manager::RetentionPruningReport> {
		self.manager
			.prune_versions_for_retention(
				policy(),
				&self.lifecycle,
				&self.schemas,
				&crate::schema::secondary_index::SecondaryIndexCatalog::default(),
				now_ms,
				1,
			)
			.await
	}

	async fn l1_retention_pass(
		&self,
	) -> std::io::Result<crate::storage::manager::RetentionPruningReport> {
		self.manager
			.prune_versions_for_retention(
				policy(),
				&self.lifecycle,
				&self.schemas,
				&crate::schema::secondary_index::SecondaryIndexCatalog::default(),
				NOW_MS,
				1,
			)
			.await
	}

	fn run_state(&self, slab: SlabId) -> String {
		format!(
			"{:?}",
			self.manager
				.compaction_manifest_snapshot()
				.run(slab)
				.map(|run| run.state.clone())
		)
	}

	fn non_live(&self, slab: SlabId) -> bool {
		self.manager
			.compaction_manifest_snapshot()
			.run(slab)
			.is_some_and(|run| !run.state.is_live())
	}

	/// I1: the slabs with a published pointer whose manifest run is non-Live.
	async fn i1_violations(&self) -> Vec<u64> {
		let mut out = Vec::new();
		for slab in [self.source, self.independent, self.legacy] {
			if self.manager.tier_pointer(slab).await.is_some() && self.non_live(slab) {
				out.push(slab.get());
			}
		}
		out
	}

	/// Unresolved first-tier rows (a Tier command with no expected generation) naming `slab`.
	async fn first_tier_rows(&self, slab: SlabId) -> usize {
		let ops = self
			.manager
			.require_op_index()
			.unwrap()
			.load_op_intents()
			.await
			.unwrap();
		ops.iter()
			.filter(|op| match op {
				LoadedOp::Submittable(ticket) => matches!(
					ticket.command(),
					crate::consensus::slab_ops::SlabCommand::Tier(tier) if tier.tier.slab.local_id() == slab
				),
				LoadedOp::Reserved(_) => false,
			})
			.count()
	}

	async fn info(&self, slab: SlabId) -> SealedSlabInfo {
		self.manager
			.list_sealed_slabs()
			.await
			.into_iter()
			.find(|info| info.slab_id == slab)
			.expect("the slab is listed as sealed")
	}

	async fn object_names(&self) -> Vec<String> {
		let mut names = self
			.l2
			.list("")
			.await
			.unwrap()
			.into_iter()
			.map(|object| format!("{object:?}"))
			.collect::<Vec<_>>();
		names.sort();
		names
	}

	async fn live_rows_exact(&self) {
		for key in [
			self.keys[1],
			self.keys[2],
			self.keys[4],
			self.keys[5],
			self.keys[6],
		] {
			let location = self
				.manager
				.lookup(tenant(), key)
				.unwrap()
				.expect("live row indexed");
			self.manager
				.read(location)
				.await
				.expect("live row readable");
		}
	}
}

/// Case 1, retention-first: L1 retention replaces S (S stays sealed, run Retired), then the ordinary
/// sweeper pass runs. Expected (the invariant): no pointer for S; the Live replacement, the
/// independent slab and the legacy missing-run slab are published. Before ADR 0111 S was published (I1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_first_then_ordinary_tiering_publishes_no_retired_source() {
	let f = fixture("retired-source-case1", Mode::Pass).await;
	let l1 = f.l1_retention_pass().await.unwrap();
	assert_eq!(l1.compacted_slabs, 1, "premise: L1 retention replaced S");
	let replacement = f
		.manager
		.lookup(tenant(), f.keys[1])
		.unwrap()
		.unwrap()
		.slab_id;
	assert!(
		f.non_live(f.source),
		"premise: S is non-Live: {}",
		f.run_state(f.source)
	);
	assert!(
		f.manager.read_sealed_raw(f.source).await.is_ok(),
		"premise: S is still sealed"
	);
	let report = f.sweeper.sweep_once().await;
	println!(
		"case RED1 sweep {report:?}; runs S={} R={}; objects {:?}",
		f.run_state(f.source),
		f.run_state(replacement),
		f.object_names().await
	);
	assert!(
		f.manager.tier_pointer(replacement).await.is_some(),
		"control: the Live replacement is published"
	);
	assert!(
		f.manager.tier_pointer(f.legacy).await.is_some(),
		"control: the legacy missing-run slab is published"
	);
	f.live_rows_exact().await;
	assert_eq!(
		f.i1_violations().await,
		Vec::<u64>::new(),
		"I1: a published pointer names a non-Live source"
	);
	assert!(
		f.manager.tier_pointer(f.source).await.is_none(),
		"no pointer for the retired source"
	);
	assert_eq!(
		f.first_tier_rows(f.source).await,
		0,
		"no first-tier row for the retired source"
	);
}

/// Case 2, tier-first paused before linearization: a first tier of S is paused inside its Tier
/// `submit_op` (row allocated, request not yet applied) while one L1 retention pass runs. Expected:
/// the pass does not make S non-Live (it is skipped, costing no attempt) and the independent slab P
/// is pruned in that same pass; S then tiers as a Live source. Before ADR 0111 the pass retired S, and the
/// tier then published it (I2, then I1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tier_paused_before_linearization_and_a_retention_pass() {
	let f = Arc::new(fixture("retired-source-case2", Mode::PauseTier).await);
	let info = f.info(f.source).await;
	let tier = {
		let f = f.clone();
		tokio::spawn(async move { f.sweeper.tier_one_slab(&info).await })
	};
	// Harness guard only: the tier must reach its paused Tier submission.
	tokio::time::timeout(Duration::from_secs(30), f.committer.entered.acquire())
		.await
		.expect("harness: the tier reached its Tier submission")
		.unwrap()
		.forget();
	let rows_during = f.first_tier_rows(f.source).await;
	let l1 = f.l1_retention_pass().await.unwrap();
	let source_after_pass = f.run_state(f.source);
	let independent_pruned = f.manager.lookup(tenant(), f.keys[3]).unwrap().is_none();
	println!(
		"case RED2 during pause: first-tier rows naming S={rows_during}; pass {l1:?}; S={source_after_pass}; P pruned={independent_pruned}"
	);
	f.committer.release.add_permits(1);
	let tiered = tier.await.unwrap();
	println!(
		"case RED2 tier result {tiered:?}; I1 {:?}",
		f.i1_violations().await
	);
	assert_eq!(
		rows_during, 1,
		"premise: the paused tier's first-tier row names S"
	);
	assert!(
		!f.non_live(f.source),
		"I2: the pass made S non-Live while its first tier was unresolved: {source_after_pass}"
	);
	assert!(
		independent_pruned,
		"the independent eligible slab progressed in the same pass"
	);
	assert_eq!(
		f.i1_violations().await,
		Vec::<u64>::new(),
		"I1 after the tier applied"
	);
	f.live_rows_exact().await;
}

/// Case 3, stale selection before acquisition: the tier's candidate was listed while S was Live, then
/// L1 retention replaced S, then the tier runs with that listing. Expected: it refuses before reading
/// or uploading (no object, no row, no pointer). Before ADR 0111 it published S (I1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_selection_before_acquisition_publishes_nothing() {
	let f = fixture("retired-source-case3", Mode::Pass).await;
	let info = f.info(f.source).await;
	let objects_before = f.object_names().await;
	assert_eq!(
		f.l1_retention_pass().await.unwrap().compacted_slabs,
		1,
		"premise: S replaced after listing"
	);
	let tiered = f.sweeper.tier_one_slab(&info).await;
	let objects_after = f.object_names().await;
	println!(
		"case RED3 tier {tiered:?}; objects before {objects_before:?} after {objects_after:?}"
	);
	assert_eq!(f.i1_violations().await, Vec::<u64>::new(), "I1");
	assert!(
		f.manager.tier_pointer(f.source).await.is_none(),
		"no pointer for the retired source"
	);
	assert_eq!(
		objects_after, objects_before,
		"nothing was uploaded for the stale candidate"
	);
	assert_eq!(f.first_tier_rows(f.source).await, 0, "no first-tier row");
}

/// Case 4, an undecided first tier: the Tier proposal is sent and its answer lost. The local caveat
/// is asserted first: a first-tier row names S, S is still sealed and Live, and neither a receipt nor
/// a delivery resolved it. Then one L1 retention pass, then the ring decides the request late.
/// Expected: the pass leaves S Live (I2) and the late decision publishes a Live source (I1).
/// Before ADR 0111 the pass retired S and the late apply published it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undecided_first_tier_then_retention_then_late_apply() {
	let f = fixture("retired-source-case4", Mode::LoseTier).await;
	let info = f.info(f.source).await;
	let tiered = f.sweeper.tier_one_slab(&info).await;
	let rows = f.first_tier_rows(f.source).await;
	let sealed = f.manager.read_sealed_raw(f.source).await.is_ok();
	let pointer = f.manager.tier_pointer(f.source).await;
	println!(
		"case RED4 tier {tiered:?}; rows {rows}; sealed {sealed}; pointer {}",
		pointer.is_some()
	);
	assert!(
		tiered.is_err(),
		"premise: the outcome is unknown to the caller"
	);
	assert_eq!(rows, 1, "premise: a first-tier row names S");
	assert!(
		sealed && pointer.is_none() && !f.non_live(f.source),
		"premise: S is sealed, Live, unpublished"
	);
	let l1 = f.l1_retention_pass().await.unwrap();
	let after_pass = f.run_state(f.source);
	let late = f.committer.deliver_held().await;
	println!(
		"case RED4 pass {l1:?}; S={after_pass}; late apply {late:?}; I1 {:?}",
		f.i1_violations().await
	);
	assert!(
		!f.non_live(f.source),
		"I2: the pass made S non-Live while its first-tier row was unresolved: {after_pass}"
	);
	assert_eq!(
		f.i1_violations().await,
		Vec::<u64>::new(),
		"I1 after the late decision"
	);
	// The exact late delivery: the ring's late decision published S as a Live source.
	assert!(
		f.manager.tier_pointer(f.source).await.is_some() && !f.non_live(f.source),
		"the late decision tiered S while its run stayed Live"
	);
	f.live_rows_exact().await;
}

/// Case 5, the same undecided first tier across a restart: the manager is reopened from its
/// directory with the row durable; the reopened manager's retention pass must not make S non-Live
/// while the row is unresolved (I2). Before ADR 0111 it did.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undecided_first_tier_survives_a_restart_and_still_fences_retention() {
	let f = fixture("retired-source-case5", Mode::LoseTier).await;
	let info = f.info(f.source).await;
	assert!(
		f.sweeper.tier_one_slab(&info).await.is_err(),
		"premise: unknown outcome"
	);
	assert_eq!(
		f.first_tier_rows(f.source).await,
		1,
		"premise: the row is durable before restart"
	);
	let Fixture {
		manager,
		sweeper,
		committer,
		sm,
		l2,
		schemas,
		lifecycle,
		source,
		keys,
		crypto,
		l1_dir,
		_l2_dir,
		..
	} = f;
	drop(sweeper);
	drop(committer);
	drop(sm);
	let weak = Arc::downgrade(&manager);
	drop(manager);
	tokio::time::timeout(Duration::from_secs(10), async {
		while weak.upgrade().is_some() {
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("harness: manager owners released before reopen");
	let reopened = SlabManager::open(manager_config(&l1_dir, l2.clone(), crypto))
		.await
		.unwrap();
	reopened.bind_control_node(1).unwrap();
	let ops = reopened
		.require_op_index()
		.unwrap()
		.load_op_intents()
		.await
		.unwrap();
	let rows = ops
		.iter()
		.filter(|op| matches!(op, LoadedOp::Submittable(t) if matches!(t.command(), crate::consensus::slab_ops::SlabCommand::Tier(tier) if tier.tier.slab.local_id() == source)))
		.count();
	let l1 = reopened
		.prune_versions_for_retention(
			policy(),
			&lifecycle,
			&schemas,
			&crate::schema::secondary_index::SecondaryIndexCatalog::default(),
			NOW_MS,
			1,
		)
		.await;
	let state = reopened
		.compaction_manifest_snapshot()
		.run(source)
		.map(|run| run.state.clone());
	println!("case RED5 after reopen: first-tier rows naming S={rows}; pass {l1:?}; S={state:?}");
	assert_eq!(rows, 1, "premise: the unresolved row survived the restart");
	assert!(
		state.as_ref().is_none_or(|state| state.is_live()),
		"I2 after restart: S made non-Live: {state:?}"
	);
	let _ = keys;
}

/// Control, verified Cancel: the undecided first tier is resolved by the op driver (the ring has no
/// decision, so it cancels; the receipt is a verified Rejected{Cancelled} and the row is handed off).
/// Then maintenance may proceed on the Live S, and the late request cannot publish it. Expected on
/// both the baseline and a fix (the fence must lift only through this verified resolution).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verified_cancel_lifts_the_first_tier_and_late_apply_publishes_nothing() {
	let f = fixture("retired-source-ctl-cancel", Mode::LoseTier).await;
	let info = f.info(f.source).await;
	assert!(
		f.sweeper.tier_one_slab(&info).await.is_err(),
		"premise: unknown outcome"
	);
	assert_eq!(f.first_tier_rows(f.source).await, 1, "premise: row pending");
	let driven = crate::storage::op_driver::OpDriver::new(f.manager.clone(), f.committer.clone())
		.drive_once()
		.await;
	let rows = f.first_tier_rows(f.source).await;
	let l1 = f.l1_retention_pass().await.unwrap();
	let late = f.committer.deliver_held().await;
	println!(
		"case CTL-CANCEL drive {driven:?}; rows after {rows}; pass {l1:?}; S={}; late {late:?}; I1 {:?}",
		f.run_state(f.source),
		f.i1_violations().await
	);
	assert_eq!(rows, 0, "the verified Cancel handed the row off");
	assert_eq!(
		l1.compacted_slabs, 1,
		"maintenance proceeds on the Live S once the row is resolved"
	);
	assert!(
		f.manager.tier_pointer(f.source).await.is_none(),
		"a cancelled request publishes nothing"
	);
	assert_eq!(f.i1_violations().await, Vec::<u64>::new(), "I1");
	f.live_rows_exact().await;
}

/// An object store that can pause once at its next whole-object fetch (the tier's post-upload
/// round-trip GET), so a test can impose a real precondition between upload and proposal.
struct PausingStore {
	inner: Arc<dyn ObjectStore>,
	pause_next_get: std::sync::atomic::AtomicBool,
	entered: Arc<tokio::sync::Semaphore>,
	release: Arc<tokio::sync::Semaphore>,
}

impl PausingStore {
	fn new(inner: Arc<dyn ObjectStore>) -> Self {
		Self {
			inner,
			pause_next_get: Default::default(),
			entered: Arc::new(tokio::sync::Semaphore::new(0)),
			release: Arc::new(tokio::sync::Semaphore::new(0)),
		}
	}
}

#[async_trait]
impl ObjectStore for PausingStore {
	async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
		self.inner.put(key, bytes).await
	}
	fn planned_upload_uri(&self, key: &str) -> std::io::Result<String> {
		self.inner.planned_upload_uri(key)
	}
	async fn head(&self, uri: &str) -> std::io::Result<crate::storage::l2::ObjectMetadata> {
		self.inner.head(uri).await
	}
	async fn get_range(
		&self,
		uri: &str,
		range: std::ops::Range<u64>,
	) -> std::io::Result<bytes::Bytes> {
		self.inner.get_range(uri, range).await
	}
	async fn get_bytes(&self, uri: &str) -> std::io::Result<bytes::Bytes> {
		if self.pause_next_get.swap(false, Ordering::AcqRel) {
			self.entered.add_permits(1);
			self.release.acquire().await.unwrap().forget();
		}
		self.inner.get_bytes(uri).await
	}
	async fn list(&self, prefix: &str) -> std::io::Result<Vec<crate::storage::l2::ObjectMetadata>> {
		self.inner.list(prefix).await
	}
	async fn delete(&self, uri: &str) -> std::io::Result<()> {
		self.inner.delete(uri).await
	}
}

/// Case 6, full prune, retention-first: every S row is expired, so L1 retention retires S with NO
/// replacement (`Retired { successors: [] }`); in a bound store S stays sealed. Then ordinary
/// tiering. Expected: no pointer for S (I1). Before ADR 0111 it was published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_prune_retention_first_then_ordinary_tiering() {
	let f = fixture_with("retired-source-case6", Mode::Pass, true).await;
	let l1 = f.l1_retention_pass().await.unwrap();
	let sealed = f.manager.read_sealed_raw(f.source).await.is_ok();
	println!(
		"case RED6 pass {l1:?}; S={}; S sealed {sealed}",
		f.run_state(f.source)
	);
	assert!(f.non_live(f.source), "premise: S retired by a full prune");
	assert!(sealed, "premise: S stays sealed in a bound store");
	let report = f.sweeper.sweep_once().await;
	println!(
		"case RED6 sweep {report:?}; I1 {:?}",
		f.i1_violations().await
	);
	assert!(
		f.manager.tier_pointer(f.legacy).await.is_some(),
		"control: the legacy slab is published"
	);
	assert_eq!(
		f.i1_violations().await,
		Vec::<u64>::new(),
		"I1: a fully pruned source was published"
	);
}

/// Case 7, full prune with the first tier paused before linearization. Expected: the pass does not
/// make S non-Live and returns no compaction outcome for it (I2). Before ADR 0111 it retired S.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_prune_with_a_paused_first_tier() {
	let f = Arc::new(fixture_with("retired-source-case7", Mode::PauseTier, true).await);
	let info = f.info(f.source).await;
	let tier = {
		let f = f.clone();
		tokio::spawn(async move { f.sweeper.tier_one_slab(&info).await })
	};
	tokio::time::timeout(Duration::from_secs(30), f.committer.entered.acquire())
		.await
		.expect("harness: the tier reached its Tier submission")
		.unwrap()
		.forget();
	let l1 = f.l1_retention_pass().await.unwrap();
	let after = f.run_state(f.source);
	f.committer.release.add_permits(1);
	let tiered = tier.await.unwrap();
	println!(
		"case RED7 pass {l1:?}; S={after}; tier {tiered:?}; I1 {:?}",
		f.i1_violations().await
	);
	assert!(
		!f.non_live(f.source),
		"I2: a full prune retired S while its first tier was unresolved: {after}"
	);
	assert!(
		f.manager.lookup(tenant(), f.keys[3]).unwrap().is_none(),
		"the pass skipped S (no outcome for it) and the independent eligible slab progressed"
	);
	assert_eq!(f.i1_violations().await, Vec::<u64>::new(), "I1");
}

/// Cases 8 and 9, LSM with an undecided first tier of S: the first tier's answer is lost (a
/// first-tier row names S; S sealed and Live), then an L0 job whose only input is S runs, locally
/// (`run_lsm_compaction_once`) or through the ring (`run_lsm_compaction_once_via_control_ring`).
/// Expected: the job does not make S non-Live (I2). Before ADR 0111 it did.
async fn lsm_after_an_undecided_first_tier(tag: &str, via_ring: bool) {
	let f = fixture(tag, Mode::LoseTier).await;
	let info = f.info(f.source).await;
	assert!(
		f.sweeper.tier_one_slab(&info).await.is_err(),
		"premise: unknown outcome"
	);
	assert_eq!(
		f.first_tier_rows(f.source).await,
		1,
		"premise: a first-tier row names S"
	);
	f.manager.adopt_l0_run_for_test(f.source).await;
	let snapshot = f.manager.compaction_manifest_snapshot();
	let plan = crate::storage::CompactionPlan {
		job_id: crate::schema::CompactionJobId::new(0x262),
		source_level: crate::schema::CompactionLevelIndex::L0,
		target_level: crate::schema::CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![f.source],
		overlapping_slabs: Vec::new(),
		elision: crate::storage::PlanElision::RetentionFloor(hlc_at(85_000)),
		compaction_start_hlc: hlc_at(NOW_MS),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let worker = crate::storage::LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		f.manager.slab_pin_table(),
	)
	.unwrap();
	let outcome = if via_ring {
		f.manager
			.run_lsm_compaction_once_via_control_ring(&worker, plan, f.committer.as_ref())
			.await
	} else {
		f.manager.run_lsm_compaction_once(&worker, plan).await
	};
	let after = f.run_state(f.source);
	println!(
		"case {tag} lsm {outcome:?}; S={after}; first-tier rows {}",
		f.first_tier_rows(f.source).await
	);
	assert!(
		!f.non_live(f.source),
		"I2: an LSM job made S non-Live while its first tier was unresolved: {after}"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_lsm_job_after_an_undecided_first_tier() {
	lsm_after_an_undecided_first_tier("case8-lsm-local", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ring_lsm_job_after_an_undecided_first_tier() {
	lsm_after_an_undecided_first_tier("case9-lsm-ring", true).await;
}

/// Case 10, the reciprocal order: a ring LSM job over S whose Compaction answer is lost (its
/// durable install intent names S; S still sealed and Live), then the first tier of S, then the
/// ring decides the held Compaction late. Expected: the tier refuses while the install names S, so
/// no published pointer ever names a source the install retires (I1). ADR 0111 keeps the existing store-wide refusal (LsmInstallUnresolved).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tier_after_an_undecided_lsm_install_naming_its_source() {
	let f = fixture("retired-source-case10", Mode::LoseCompaction).await;
	f.manager.adopt_l0_run_for_test(f.source).await;
	let snapshot = f.manager.compaction_manifest_snapshot();
	let plan = crate::storage::CompactionPlan {
		job_id: crate::schema::CompactionJobId::new(0x2620),
		source_level: crate::schema::CompactionLevelIndex::L0,
		target_level: crate::schema::CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![f.source],
		overlapping_slabs: Vec::new(),
		elision: crate::storage::PlanElision::RetentionFloor(hlc_at(85_000)),
		compaction_start_hlc: hlc_at(NOW_MS),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let worker = crate::storage::LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		f.manager.slab_pin_table(),
	)
	.unwrap();
	let lsm = f
		.manager
		.run_lsm_compaction_once_via_control_ring(&worker, plan, f.committer.as_ref())
		.await;
	let live_before_tier = !f.non_live(f.source);
	println!(
		"case RED10 lsm {lsm:?}; S Live before the tier: {live_before_tier}; S={}",
		f.run_state(f.source)
	);
	assert!(lsm.is_err(), "premise: the Compaction outcome is unknown");
	assert!(
		live_before_tier,
		"premise: S is still Live (the install is undecided)"
	);
	*f.committer.mode.lock().unwrap() = Mode::Pass;
	let info = f.info(f.source).await;
	let tiered = f.sweeper.tier_one_slab(&info).await;
	let published = f.manager.tier_pointer(f.source).await.is_some();
	let late = f.committer.deliver_held().await;
	println!(
		"case RED10 tier {tiered:?}; published {published}; late compaction {late:?}; S={}; I1 {:?}",
		f.run_state(f.source),
		f.i1_violations().await
	);
	assert!(
		!published,
		"the tier refuses while an undecided install names its source"
	);
	assert_eq!(f.i1_violations().await, Vec::<u64>::new(), "I1");
}

/// Control, a real pre-submit refusal: the tier is paused at its post-upload round-trip GET; one
/// in-flight append is then held on S (the forced quiescence precondition, checked), so the
/// existing `confirm_tier_candidate_quiescent` refuses. Expected (baseline and fix): the uploaded
/// object is removed as definitely unsubmitted, no durable op appears, nothing is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pre_submit_quiescence_refusal_removes_the_unsubmitted_upload() {
	let f = Arc::new(fixture("retired-source-ctl-quiesce", Mode::Pass).await);
	let info = f.info(f.source).await;
	let before = f.object_names().await;
	f.store.pause_next_get.store(true, Ordering::Release);
	let tier = {
		let f = f.clone();
		tokio::spawn(async move { f.sweeper.tier_one_slab(&info).await })
	};
	tokio::time::timeout(Duration::from_secs(30), f.store.entered.acquire())
		.await
		.expect("harness: the tier reached its round-trip GET")
		.unwrap()
		.forget();
	let uploaded = f.object_names().await;
	let hold = f
		.manager
		.hold_sealed_appends_for_test(f.source)
		.await
		.unwrap();
	let quiescent = f
		.manager
		.list_sealed_slabs()
		.await
		.iter()
		.any(|s| s.slab_id == f.source);
	f.store.release.add_permits(1);
	let tiered = tier.await.unwrap();
	drop(hold);
	let after = f.object_names().await;
	println!(
		"case CTL-QUIESCE uploaded {} object(s); listed-as-quiescent while held {quiescent}; tier {tiered:?}; after {} object(s); rows {}",
		uploaded.len() - before.len(),
		after.len() - before.len(),
		f.first_tier_rows(f.source).await
	);
	assert_eq!(
		uploaded.len(),
		before.len() + 1,
		"premise: the tier uploaded its object"
	);
	assert!(!quiescent, "premise: the held append makes S non-quiescent");
	assert!(tiered.is_err(), "the pre-submit recheck refused");
	assert_eq!(after, before, "the unsubmitted upload was removed");
	assert_eq!(
		f.first_tier_rows(f.source).await,
		0,
		"no durable op appeared"
	);
	assert!(
		f.manager.tier_pointer(f.source).await.is_none(),
		"nothing was published"
	);
}

/// Caller-level deferral, RetentionPrune: with an op row that does not decode, an admitted
/// retention attempt cannot rule a first tier out, so it defers (fail closed): S keeps its expired
/// row indexed and its run Live, and nothing is compacted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_defers_when_the_op_index_cannot_rule_out_a_first_tier() {
	let f = fixture("retired-source-unreadable-retention", Mode::Pass).await;
	f.manager
		.require_op_index()
		.unwrap()
		.put_undecodable_op_intent_for_test(u64::MAX - 7)
		.await
		.unwrap();
	let l1 = f.l1_retention_pass().await.unwrap();
	println!(
		"case unreadable-retention pass {l1:?}; S={}",
		f.run_state(f.source)
	);
	assert_eq!(
		l1.compacted_slabs, 0,
		"nothing is compacted while the first-tier state is unknown"
	);
	assert!(!f.non_live(f.source), "S was not retired");
	assert!(
		f.manager.lookup(tenant(), f.keys[0]).unwrap().is_some(),
		"no retirement effect: the expired row is still indexed"
	);
}

/// Caller-level deferral, LSM: the same unreadable row makes a local L0 job over S defer with a
/// RETRYABLE failure before it prepares outputs: no output run, no install intent, S still Live.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_lsm_job_defers_retryably_when_the_op_index_cannot_rule_out_a_first_tier() {
	let f = fixture("retired-source-unreadable-lsm", Mode::Pass).await;
	f.manager.adopt_l0_run_for_test(f.source).await;
	f.manager
		.require_op_index()
		.unwrap()
		.put_undecodable_op_intent_for_test(u64::MAX - 7)
		.await
		.unwrap();
	let sealed_before = f.manager.list_sealed_slabs().await.len();
	let epoch_before = f.manager.compaction_manifest_snapshot().manifest_epoch;
	let plan = crate::storage::CompactionPlan {
		job_id: crate::schema::CompactionJobId::new(0x2621),
		source_level: crate::schema::CompactionLevelIndex::L0,
		target_level: crate::schema::CompactionLevelIndex::L1,
		manifest_epoch_pre: epoch_before,
		input_slabs: vec![f.source],
		overlapping_slabs: Vec::new(),
		elision: crate::storage::PlanElision::RetentionFloor(hlc_at(85_000)),
		compaction_start_hlc: hlc_at(NOW_MS),
		dominant_reason: crate::storage::DominantReason::Operator,
	};
	let worker = crate::storage::LsmCompactionWorker::new(
		crate::storage::LsmConfig::default(),
		f.manager.slab_pin_table(),
	)
	.unwrap();
	let outcome = f.manager.run_lsm_compaction_once(&worker, plan).await;
	println!("case unreadable-lsm {outcome:?}");
	let failure = outcome.expect_err("the job defers");
	assert!(
		matches!(failure, crate::storage::lsm::CompactionFailure::IoError(_)),
		"{failure:?}"
	);
	assert_eq!(
		crate::storage::maintenance_policy::classify_compaction_failure(&failure),
		crate::storage::maintenance_policy::MaintenanceFailureClass::Retryable,
		"a read failure stays retryable"
	);
	assert!(!f.non_live(f.source), "S was not retired");
	assert_eq!(
		f.manager.compaction_manifest_snapshot().manifest_epoch,
		epoch_before,
		"no install"
	);
	assert_eq!(
		f.manager.list_sealed_slabs().await.len(),
		sealed_before,
		"no output run"
	);
	assert!(
		f.manager.lsm_install_unresolved_for_test().is_empty(),
		"no install intent"
	);
}

/// Policy refusal control: when the failure policy refuses a candidate (here every slab is in its
/// neutral cooldown after a clean scan), the pass performs NO op-index read and scans nothing.
/// Current-thread runtime: the read counter is per thread.
#[tokio::test(flavor = "current_thread")]
async fn a_policy_refused_candidate_reads_no_op_index_and_scans_nothing() {
	let f = fixture("retired-source-policy-refusal", Mode::Pass).await;
	// At 40 s the floor is 25 s: every row is newer, so each attempt is a clean scan that cools
	// its slab down. Budget 1: three passes attempt the three slabs in turn.
	for pass in 0..3 {
		let report = f.l1_retention_pass_at(40_000).await.unwrap();
		assert_eq!(report.compacted_slabs, 0, "premise: pass {pass} is clean");
	}
	let reads_before = SlabManager::first_tier_reads_for_test();
	let report = f.l1_retention_pass_at(40_000).await.unwrap();
	let reads = SlabManager::first_tier_reads_for_test() - reads_before;
	println!("case policy-refusal pass {report:?}; op-index reads {reads}");
	assert_eq!(reads, 0, "a refused candidate opens no op-index read");
	assert_eq!(report.compacted_slabs, 0);
	assert_eq!(report.max_lag_seconds, 0, "nothing was scanned");
}
