use crate::metrics::global as metrics;
use crate::schema::{HybridTimestamp, SlabId, TableLifecycleCatalog, TableObjectId, TenantId};
use crate::storage::lsm::SlabState;
use crate::storage::lsm::{
	CompactionCommit, CompactionDigestSummary, CompactionFailure, CompactionOutcome,
	CompactionPlan, LsmConfig, LsmWrittenOutputRun, PlannedOutputRun, SurvivorOrDrop, VersionEntry,
	apply_elision, plan_output_runs, survivor_records,
};
use crate::storage::manifest::{CompactionManifest, ManifestSnapshot};
use crate::storage::retention::{MvccRetentionPolicy, effective_table_retention};
use crate::storage::slab_pins::{AcquireError, SlabPinGuard, SlabPinTable};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct LsmCompactionWorker {
	config: LsmConfig,
	slab_pins: Arc<SlabPinTable>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LsmPreparedOutputs {
	pub decisions: Vec<SurvivorOrDrop>,
	pub output_runs: Vec<PlannedOutputRun>,
	pub(crate) input_authority: Option<Vec<crate::storage::index::LsmInputAuthority>>,
}

#[async_trait]
pub trait LsmCompactionDriver: Send + Sync {
	async fn run_lsm_compaction_tick(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Result<Option<CompactionOutcome>, CompactionFailure>;
}

#[async_trait]
pub trait LsmCompactionJobRunner: Send + Sync {
	async fn run_lsm_compaction_plan(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<CompactionOutcome, CompactionFailure>;
}

#[async_trait]
pub trait LsmGcDriver: Send + Sync {
	async fn run_lsm_gc_tick(&self, worker: &LsmGcWorker) -> std::io::Result<Vec<SlabId>>;
}

#[derive(Clone, Debug, Default)]
pub struct LsmCompactionServiceControl {
	paused: Arc<AtomicBool>,
}

impl LsmCompactionServiceControl {
	pub fn pause(&self) {
		self.paused.store(true, Ordering::SeqCst);
	}

	pub fn resume(&self) {
		self.paused.store(false, Ordering::SeqCst);
	}

	pub fn is_paused(&self) -> bool {
		self.paused.load(Ordering::SeqCst)
	}
}

pub struct LsmCompactionService<D> {
	driver: Arc<D>,
	worker: LsmCompactionWorker,
	control: LsmCompactionServiceControl,
}

impl<D> LsmCompactionService<D>
where
	D: LsmCompactionDriver + 'static,
{
	pub fn new(driver: Arc<D>, worker: LsmCompactionWorker) -> Self {
		Self {
			driver,
			worker,
			control: LsmCompactionServiceControl::default(),
		}
	}

	pub fn control(&self) -> LsmCompactionServiceControl {
		self.control.clone()
	}

	pub async fn tick_once(
		&self,
		now: HybridTimestamp,
	) -> Result<Option<CompactionOutcome>, CompactionFailure> {
		if self.control.is_paused() {
			return Ok(None);
		}
		self.driver.run_lsm_compaction_tick(&self.worker, now).await
	}

	pub fn spawn(self) -> JoinHandle<()> {
		tokio::spawn(async move {
			let tick = Duration::from_millis(self.worker.config().lsm_picker_tick_millis.max(1));
			loop {
				tokio::time::sleep(tick).await;
				if let Err(err) = self.tick_once(lsm_service_now_hlc()).await {
					tracing::warn!(error = %err, "LSM compaction service tick failed");
				}
			}
		})
	}
}

pub struct LsmGcService<D> {
	driver: Arc<D>,
	worker: LsmGcWorker,
}

impl<D> LsmGcService<D>
where
	D: LsmGcDriver + 'static,
{
	pub fn new(driver: Arc<D>, worker: LsmGcWorker) -> Self {
		Self { driver, worker }
	}

	pub async fn tick_once(&self) -> std::io::Result<Vec<SlabId>> {
		self.driver.run_lsm_gc_tick(&self.worker).await
	}

	pub fn spawn(self) -> JoinHandle<()> {
		tokio::spawn(async move {
			let tick = Duration::from_millis(self.worker.config().lsm_gc_tick_millis.max(1));
			loop {
				tokio::time::sleep(tick).await;
				if let Err(err) = self.tick_once().await {
					tracing::warn!(error = %err, "LSM GC service tick failed");
				}
			}
		})
	}
}

impl LsmCompactionWorker {
	pub fn new(config: LsmConfig, slab_pins: Arc<SlabPinTable>) -> std::io::Result<Self> {
		config.validate()?;
		Ok(Self { config, slab_pins })
	}

	pub fn config(&self) -> LsmConfig {
		self.config
	}

	pub fn slab_pins(&self) -> &Arc<SlabPinTable> {
		&self.slab_pins
	}

	pub fn retention_floor_from_catalog(
		&self,
		table_lifecycle: &TableLifecycleCatalog,
		cluster_retention_policy: &MvccRetentionPolicy,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		now_ms: u64,
	) -> Option<HybridTimestamp> {
		effective_table_retention(
			cluster_retention_policy,
			table_lifecycle.retention_policy(tenant_id, table_object_id),
			now_ms,
		)
		.floor
	}

	pub fn acquire_plan_pins(
		&self,
		plan: &CompactionPlan,
	) -> Result<SlabPinGuard, CompactionFailure> {
		self.slab_pins
			.acquire(&plan.touched_slabs())
			.map_err(compaction_pin_failure)
	}

	pub fn plan_survivor_outputs(
		&self,
		plan: &CompactionPlan,
		records: impl IntoIterator<Item = VersionEntry>,
		output_slab_ids: impl IntoIterator<Item = SlabId>,
	) -> Result<LsmPreparedOutputs, CompactionFailure> {
		let target_size = self
			.config
			.output_run_target_bytes(plan.target_level)
			.ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidInput,
					"invalid compaction output target level",
				)
			})?;
		let decisions = apply_elision(records, plan.retention_floor_snapshot);
		let output_runs = plan_output_runs(
			survivor_records(&decisions),
			plan.target_level,
			target_size,
			output_slab_ids,
		)?;
		Ok(LsmPreparedOutputs {
			decisions,
			output_runs,
			input_authority: None,
		})
	}

	pub fn build_compaction_commit(
		&self,
		owner: crate::consensus::OwnerRef,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		digest_summary: CompactionDigestSummary,
	) -> CompactionCommit {
		let mut slabs_to_retire = plan.touched_slabs();
		slabs_to_retire.sort_unstable();
		CompactionCommit {
			owner,
			job_id: plan.job_id,
			manifest_epoch_pre: plan.manifest_epoch_pre,
			slabs_to_retire,
			slabs_to_add: prepared
				.output_runs
				.iter()
				.map(|run| run.summary(plan.manifest_epoch_pre))
				.collect(),
			slabs_to_mark_delete_pending: Vec::new(),
			slabs_to_remove: Vec::new(),
			sidecar_swaps: Vec::new(),
			digest_summary,
		}
	}

	pub fn build_written_compaction_commit(
		&self,
		owner: crate::consensus::OwnerRef,
		plan: &CompactionPlan,
		written: &[LsmWrittenOutputRun],
		digest_summary: CompactionDigestSummary,
	) -> CompactionCommit {
		let mut slabs_to_retire = plan.touched_slabs();
		slabs_to_retire.sort_unstable();
		CompactionCommit {
			owner,
			job_id: plan.job_id,
			manifest_epoch_pre: plan.manifest_epoch_pre,
			slabs_to_retire,
			slabs_to_add: written.iter().map(|run| run.summary.clone()).collect(),
			slabs_to_mark_delete_pending: Vec::new(),
			slabs_to_remove: Vec::new(),
			sidecar_swaps: Vec::new(),
			digest_summary,
		}
	}

	pub fn compaction_outcome_from_commit(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		commit: &CompactionCommit,
		manifest_epoch_post: crate::schema::ManifestEpoch,
	) -> CompactionOutcome {
		let mut output_slabs = commit.slabs_to_add.clone();
		for run in &mut output_slabs {
			run.manifest_epoch = manifest_epoch_post;
		}
		let rows_in = prepared.decisions.len() as u64;
		let rows_out = prepared
			.output_runs
			.iter()
			.map(|run| run.rows.len() as u64)
			.sum();
		let bytes_in = prepared
			.decisions
			.iter()
			.map(|decision| match decision {
				SurvivorOrDrop::Survivor(record) => u64::from(record.location.length),
				SurvivorOrDrop::Drop { record, .. } => u64::from(record.location.length),
			})
			.sum();
		let bytes_out = prepared
			.output_runs
			.iter()
			.map(|run| run.estimated_size_bytes.get())
			.sum();
		CompactionOutcome {
			job_id: plan.job_id,
			manifest_epoch_pre: plan.manifest_epoch_pre,
			manifest_epoch_post,
			output_slabs,
			bytes_in,
			bytes_out,
			rows_in,
			rows_out,
			elided_versions: rows_in.saturating_sub(rows_out),
		}
	}

	pub async fn run_one<R>(
		&self,
		runner: &R,
		plan: CompactionPlan,
	) -> Result<CompactionOutcome, CompactionFailure>
	where
		R: LsmCompactionJobRunner + ?Sized,
	{
		runner.run_lsm_compaction_plan(self, plan).await
	}
}

fn compaction_pin_failure(_err: AcquireError) -> CompactionFailure {
	CompactionFailure::ManifestRace
}

fn lsm_service_now_hlc() -> HybridTimestamp {
	let physical_ms = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.min(u128::from(u64::MAX)) as u64;
	HybridTimestamp {
		physical_ms,
		logical: 0,
		node_id: 0,
	}
}

#[derive(Clone)]
pub struct LsmGcWorker {
	config: LsmConfig,
	slab_pins: Arc<SlabPinTable>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LsmGcPlan {
	pub retired_unpinned_slabs: Vec<SlabId>,
	pub delete_pending_slabs: Vec<SlabId>,
	pub retired_slab_pinned_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LsmGcSweepOutcome {
	pub marked_delete_pending: Vec<SlabId>,
	pub pending_delete_slabs: u64,
	pub retired_slab_pinned_bytes: u64,
}

impl LsmGcWorker {
	pub fn new(config: LsmConfig, slab_pins: Arc<SlabPinTable>) -> std::io::Result<Self> {
		config.validate()?;
		Ok(Self { config, slab_pins })
	}

	pub fn config(&self) -> LsmConfig {
		self.config
	}

	pub fn slab_pins(&self) -> &Arc<SlabPinTable> {
		&self.slab_pins
	}

	pub fn plan_sweep(&self, snapshot: &ManifestSnapshot) -> LsmGcPlan {
		let mut plan = LsmGcPlan::default();
		for run in snapshot.runs() {
			match &run.state {
				SlabState::Retired { .. } => {
					if self.slab_pins.pin_count(run.slab_id) == 0 {
						plan.retired_unpinned_slabs.push(run.slab_id);
					} else {
						plan.retired_slab_pinned_bytes = plan
							.retired_slab_pinned_bytes
							.saturating_add(run.size_bytes.get());
					}
				}
				SlabState::DeletePending => {
					plan.delete_pending_slabs.push(run.slab_id);
				}
				SlabState::Live => {}
			}
		}
		plan.retired_unpinned_slabs.sort_unstable();
		plan.delete_pending_slabs.sort_unstable();
		plan
	}

	pub fn observe_manifest_snapshot(&self, snapshot: &ManifestSnapshot) {
		let plan = self.plan_sweep(snapshot);
		metrics().set_lsm_gc_pressure(
			plan.delete_pending_slabs.len() as u64,
			plan.retired_slab_pinned_bytes,
		);
	}

	pub fn transition_retired_to_delete_pending(
		&self,
		manifest: &mut CompactionManifest,
	) -> LsmGcSweepOutcome {
		self.transition_retired_to_delete_pending_if(manifest, |_| true)
	}

	pub fn transition_retired_to_delete_pending_if(
		&self,
		manifest: &mut CompactionManifest,
		mut should_mark: impl FnMut(SlabId) -> bool,
	) -> LsmGcSweepOutcome {
		let snapshot = manifest.snapshot();
		let mut marked_delete_pending = Vec::new();
		for run in snapshot.retired_runs() {
			if self.slab_pins.pin_count(run.slab_id) == 0
				&& should_mark(run.slab_id)
				&& self.slab_pins.mark_delete_pending(run.slab_id)
			{
				let slab_id = run.slab_id;
				marked_delete_pending.push(slab_id);
			}
		}

		if !marked_delete_pending.is_empty() {
			manifest.advance_epoch();
			for slab_id in &marked_delete_pending {
				manifest.mark_delete_pending(*slab_id);
			}
		}

		let post_sweep = self.plan_sweep(&manifest.snapshot());
		let outcome = LsmGcSweepOutcome {
			marked_delete_pending,
			pending_delete_slabs: post_sweep.delete_pending_slabs.len() as u64,
			retired_slab_pinned_bytes: post_sweep.retired_slab_pinned_bytes,
		};
		metrics().set_lsm_gc_pressure(
			outcome.pending_delete_slabs,
			outcome.retired_slab_pinned_bytes,
		);
		outcome
	}

	pub fn complete_delete(
		&self,
		manifest: &mut CompactionManifest,
		slab_id: SlabId,
	) -> Option<SlabId> {
		if !matches!(
			manifest.get_run(slab_id).map(|run| &run.state),
			Some(SlabState::DeletePending)
		) {
			return None;
		}
		manifest.advance_epoch();
		let removed = manifest.remove_run(slab_id).map(|run| run.slab_id);
		self.slab_pins.clear_delete_pending(slab_id);
		let post_sweep = self.plan_sweep(&manifest.snapshot());
		metrics().set_lsm_gc_pressure(
			post_sweep.delete_pending_slabs.len() as u64,
			post_sweep.retired_slab_pinned_bytes,
		);
		removed
	}
}
