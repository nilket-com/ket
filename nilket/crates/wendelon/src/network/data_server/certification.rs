//! ADR 0699 S5a: a store's part in certified generations.
//!
//! For the pending target, a member builds its candidate base from its active
//! base and the winning versions of the closed set's Accepted parents (from
//! complete local intents, repaired if missing), stages it durably, and only
//! then reports it, from the staged metadata. After a generation commits, the
//! member activates the matching staged candidate in one transaction. A store
//! whose candidate is missing for a committed generation is behind the
//! boundary: it refuses to serve, with no reset or copy.

use super::*;
use crate::consensus::decisions::ParentStatus;
use crate::consensus::decisions::generations::{Pending, ReportBody};
use crate::network::decision_proposals::MaintenanceProposals;
use crate::storage::index::certified_base::{
	ActivateRefusal, Activation, BaseEntry, StagedCandidate, chunk_sums,
};
use std::collections::BTreeMap;

/// The pending target and the closed set's Accepted children, as recorded.
struct Work {
	pending: Pending,
	children: Vec<ClosedChild>,
}

/// A recorded child of an Accepted parent in the pending target's closed set.
struct ClosedChild {
	tenant: TenantId,
	table: TableObjectId,
	generation: u64,
	recorded: super::reader::AcceptedChild,
}

/// Why this store has no report for the pending target.
#[derive(Debug)]
pub(crate) enum BuildRefusal {
	/// A survivor's copy is missing and could not be repaired.
	Incomplete(String),
	/// Two versions of one key at an equal full HLC with other content.
	Conflict,
	/// The store does not hold a child of the closed set.
	NotInPlan,
	/// A covered table has a retention floor: the frozen policy is violated.
	Policy,
	Storage(String),
}

impl BuildRefusal {
	fn describe(&self) -> String {
		match self {
			BuildRefusal::Incomplete(why) => format!("incomplete: {why}"),
			BuildRefusal::Conflict => "an equal-HLC conflict".into(),
			BuildRefusal::NotInPlan => "a closed child outside this store's plan".into(),
			BuildRefusal::Policy => "a covered table has a retention floor".into(),
			BuildRefusal::Storage(why) => format!("storage: {why}"),
		}
	}
}

impl WendelonDataServer {
	/// One maintenance step under a lifecycle permit: activate a committed
	/// generation, then, only once this start has validated its certified
	/// base, build, stage and report the pending one. A step that is
	/// interrupted (cancelled at close) leaves the permit incomplete, which
	/// revokes this run's clean-shutdown record for good; none starts after
	/// close began. ADR 0707: the report goes through `maintenance`, whose wait
	/// is the step's one cut point; a stopped wait returns normally, so the
	/// permit completes through this ordinary path.
	pub(crate) async fn certification_step(
		&self,
		maintenance: &MaintenanceProposals<'_>,
	) -> std::io::Result<()> {
		self.under_permit(async {
			#[cfg(test)]
			self.decisions
				.pause_at(super::child_writes::PauseStage::CertificationStep)
				.await;
			self.activate_committed().await?;
			if self
				.decisions
				.base_validated
				.load(std::sync::atomic::Ordering::Acquire)
			{
				self.certify_pending(maintenance).await?;
			}
			Ok(())
		})
		.await
	}

	pub(super) async fn under_permit<T>(
		&self,
		work: impl std::future::Future<Output = std::io::Result<T>>,
	) -> std::io::Result<T> {
		let permit = match self.slab.directory_owner() {
			Some(owner) => Some(owner.lifecycle_permit()?),
			None => None,
		};
		let result = work.await;
		if let Some(permit) = permit {
			permit.complete();
		}
		result
	}

	/// ADR 0699 S5a: validates the certified base before this start serves
	/// reads or builds and reports a successor.
	/// - The leader's cut is applied first, so the committed generation is
	///   the cluster's, not a stale local view.
	/// - A committed generation still staged here is activated (the crash
	///   between Commit and activation).
	/// - The active base must hash to its recorded chunks and those must be
	///   the committed certificate's.
	/// - After an unclean start every entry must have a stored copy with its
	///   content, or typed tombstone locations; only that discharges the
	///   owner's base-recovery obligation, which gates a clean close.
	pub(crate) async fn validate_certified_base(&self) -> Result<(), String> {
		use std::sync::atomic::Ordering;
		if self.decisions.base_validated.load(Ordering::Acquire) {
			return Ok(());
		}
		let Some(index) = self.slab.persistent_index() else {
			return Ok(());
		};
		if self.decisions.signer.is_none() {
			return Ok(());
		}
		#[cfg(test)]
		if self.decisions.fail_base_validation.load(Ordering::Acquire) {
			return Err("test: base validation fails".into());
		}
		let proposals = self
			.decisions
			.proposals
			.clone()
			.ok_or("no decision proposals")?;
		let k = match proposals.cut().await {
			crate::network::decision_proposals::CutResponse::Cut(k) => k,
			_ => return Err("no leader cut yet".into()),
		};
		proposals
			.wait_applied(k, std::time::Duration::from_secs(5))
			.await
			.map_err(|error| error.to_string())?;
		self.under_permit(self.activate_committed())
			.await
			.map_err(|error| error.to_string())?;
		if self.decisions.behind_boundary.load(Ordering::Acquire) {
			return Err("behind the certified boundary".into());
		}
		let meta = index.base_meta().await.map_err(|error| error.to_string())?;
		let entries = index
			.base_entries(false)
			.await
			.map_err(|error| error.to_string())?;
		if chunk_sums(&entries) != meta.active_chunks {
			return Err("the active base does not hash to its recorded chunks".into());
		}
		let current = self
			.state_machine
			.read_state(|state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.generations().current.clone())
			})
			.await;
		match &current {
			Some(cert) if cert.gid == meta.active_gid && cert.chunks != meta.active_chunks => {
				return Err("the active base is not the committed certificate".into());
			}
			Some(cert) if cert.gid != meta.active_gid => {
				return Err("the committed generation is not active yet".into());
			}
			None if meta.active_gid != 0 => {
				return Err("an active base without a committed generation".into());
			}
			_ => {}
		}
		let owner = self.slab.directory_owner();
		let unclean = owner
			.as_ref()
			.is_some_and(|owner| !owner.previous_shutdown_clean());
		if unclean {
			for entry in &entries {
				self.entry_is_stored(entry).await?;
			}
			if let Some(owner) = &owner {
				owner.complete_base_recovery();
			}
		}
		self.decisions.base_validated.store(true, Ordering::Release);
		Ok(())
	}

	/// A certified entry's stored copy, with its content, or typed tombstone
	/// locations.
	async fn entry_is_stored(&self, entry: &BaseEntry) -> Result<(), String> {
		let key = Identity::new(entry.key_identity);
		let locations = self
			.slab
			.version_locations(entry.tenant, key, entry.hlc)
			.map_err(|error| error.to_string())?;
		if locations.is_empty() {
			return Err("a certified entry has no stored copy".into());
		}
		if entry.tombstone {
			if !locations.iter().all(|location| location.is_tombstone) {
				return Err("a certified tombstone is stored as a row".into());
			}
			return Ok(());
		}
		let (tenant, table) = (entry.tenant, entry.table);
		let descriptor = self
			.state_machine
			.read_state(move |state| {
				super::child_writes::table_descriptor(state, tenant, table, 0)
					.or_else(|| state.table_schemas.latest_by_id(tenant, table).cloned())
			})
			.await
			.ok_or("a certified entry's table is unknown")?;
		let stored = self
			.slab
			.stored_rows_at_version(entry.tenant, key, entry.hlc, None)
			.await
			.map_err(|error| error.to_string())?;
		if stored.is_empty() {
			return Err("a certified entry has no stored row".into());
		}
		for row in &stored {
			if super::child_writes::stored_row_digest(&descriptor, row)? != entry.row_digest {
				return Err("a certified entry's stored content differs".into());
			}
		}
		Ok(())
	}

	/// Activates the committed generation's staged candidate, if it is not
	/// active yet. A missing or mismatched candidate is behind the boundary.
	async fn activate_committed(&self) -> std::io::Result<()> {
		let Some(index) = self.slab.persistent_index() else {
			return Ok(());
		};
		let current = self
			.state_machine
			.read_state(|state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.generations().current.clone())
			})
			.await;
		let Some(cert) = current else {
			return Ok(());
		};
		#[cfg(test)]
		if self
			.decisions
			.hold_activation
			.load(std::sync::atomic::Ordering::Acquire)
		{
			return Ok(());
		}
		if index.base_meta().await?.active_gid == cert.gid {
			return Ok(());
		}
		// A candidate this run did not build and prove (one recovered from
		// before this start) inherits no validation: reads and readiness close
		// before it is installed, and after an unclean start its proof is owed
		// again before any close may record the directory clean.
		use std::sync::atomic::Ordering;
		if self.decisions.built_gid.load(Ordering::Acquire) != cert.gid {
			self.decisions
				.base_validated
				.store(false, Ordering::Release);
			if let Some(owner) = self.slab.directory_owner()
				&& !owner.previous_shutdown_clean()
			{
				owner.require_base_recovery();
			}
		}
		match index
			.activate_base(Activation {
				gid: cert.gid,
				target_digest: cert.target_digest,
				predecessor_gid: cert.predecessor_gid,
				chunks: cert.chunks.clone(),
				t: cert.t,
			})
			.await?
		{
			Ok(()) => {}
			Err(ActivateRefusal::NoMatchingCandidate | ActivateRefusal::StagedRowsMismatch) => {
				self.decisions
					.behind_boundary
					.store(true, std::sync::atomic::Ordering::Release);
			}
		}
		Ok(())
	}

	/// Builds, stages and reports the pending target's candidate. Only a
	/// candidate whose every survivor is proved stored here is staged, and a
	/// report is signed only from the durable staged metadata.
	async fn certify_pending(&self, maintenance: &MaintenanceProposals<'_>) -> std::io::Result<()> {
		let Some(signer) = self.decisions.signer.clone() else {
			return Ok(());
		};
		let Some(index) = self.slab.persistent_index() else {
			return Ok(());
		};
		let local = signer.signer;
		let work = self
			.state_machine
			.read_state(move |state| {
				let decisions = state.decisions.as_ref()?;
				let pending = decisions.generations().pending.clone()?;
				let mut children = Vec::new();
				for body in decisions
					.bodies()
					.range(..=pending.target.t)
					.map(|(_, body)| body)
				{
					if !matches!(body.status, ParentStatus::Accepted { .. }) {
						continue;
					}
					let tenant = body.claim.key.tenant;
					let table = super::child_writes::parent_table(body.claim.key.namespace);
					let generation = if table == TableObjectId::DEFAULT_KV {
						0
					} else {
						state
							.table_schemas
							.latest_by_id(tenant, table)
							.map_or(u64::MAX, |descriptor| descriptor.schema_generation)
					};
					for (ordinal, child) in body.claim.children.iter().enumerate() {
						if !child.plan.contains(&local) {
							// This store holds no copy of the closed set.
							return None;
						}
						children.push(ClosedChild {
							tenant,
							table,
							generation,
							recorded: super::reader::AcceptedChild {
								rsv: body.rsv,
								ordinal: ordinal as u16,
								claim_digest: body.claim_digest,
								rows: child.rows,
								content: child.content,
								commit_hlc: body.claim.commit_hlc,
							},
						});
					}
				}
				Some(Work { pending, children })
			})
			.await;
		let Some(Work { pending, children }) = work else {
			return Ok(());
		};
		let meta = index.base_meta().await?;
		if meta.active_gid != pending.target.predecessor_gid {
			return Ok(());
		}
		let gid = pending.target.gid;
		let built = &self.decisions.built_gid;
		match meta.staged.as_ref() {
			Some(staged) if staged.gid == gid => {
				// A candidate recovered from before this start: rebuilt with this
				// start's proofs, it must reproduce the staged evidence, and then
				// its staged rows are restored to exactly that inventory. A
				// mismatch fails closed: nothing is overwritten and nothing is
				// reported. This holds whether or not its report is recorded.
				if built.load(std::sync::atomic::Ordering::Acquire) != gid {
					let entries = match self.build_candidate(&index, &children).await {
						Ok(entries) => entries,
						Err(refusal) => {
							tracing::debug!(reason = %refusal.describe(), "a recovered candidate base is not proved");
							return Ok(());
						}
					};
					if let Err(refusal) = index.restage_base(staged.clone(), entries).await? {
						tracing::warn!(
							?refusal,
							gid,
							"a recovered candidate base does not match its staged evidence"
						);
						return Ok(());
					}
					built.store(gid, std::sync::atomic::Ordering::Release);
				}
			}
			_ => {
				let entries = match self.build_candidate(&index, &children).await {
					Ok(entries) => entries,
					Err(refusal) => {
						tracing::debug!(reason = %refusal.describe(), "no candidate base for the pending generation");
						return Ok(());
					}
				};
				let candidate = StagedCandidate {
					gid,
					target_digest: pending.digest,
					predecessor_gid: meta.active_gid,
					chunks: chunk_sums(&entries),
				};
				if index.stage_base(candidate, entries).await?.is_err() {
					return Ok(());
				}
				built.store(gid, std::sync::atomic::Ordering::Release);
			}
		}
		// Report only what is durably staged.
		if pending.reports.contains_key(&local.node) {
			return Ok(());
		}
		let meta = index.base_meta().await?;
		let Some(staged) = meta.staged.filter(|staged| {
			staged.gid == pending.target.gid && staged.target_digest == pending.digest
		}) else {
			return Ok(());
		};
		let report = crate::consensus::decisions::evidence::sign(
			&signer.key,
			ReportBody {
				gid: staged.gid,
				target_digest: staged.target_digest,
				reporter: local,
				chunks: staged.chunks,
			},
			local,
		);
		#[cfg(test)]
		self.decisions
			.pause_at(super::child_writes::PauseStage::CertificationBeforeReport)
			.await;
		// Replay basis: skipped above while applied state holds this node's
		// report, and signed from the durable staged metadata. An identical copy
		// returns ReportRecorded and inserts nothing; one after the target was
		// replaced or committed is refused StaleGeneration, and a different body
		// from the same reporter ReportMismatch
		// (`consensus/decisions/generations.rs:201-216`). A stopped wait does not
		// certify that the report failed: at reopen only the exact stored
		// candidate is restaged (`storage/index/certified_base.rs:464-466`) and a
		// different one for the same gid is refused (`certified_base.rs:410-415`),
		// so a report proposed again equals the committed one.
		let _ = maintenance.report(report).await;
		Ok(())
	}

	/// The candidate base: the active base, superseded per key by the newest
	/// winning version of the closed set's Accepted children.
	/// - Each child's complete local intent must match its recorded child, with
	///   a canonical manifest and every version owned by it (the S4 proof),
	///   repaired once if missing.
	/// - Each winning version from a child must be stored here with its
	///   content, or as typed tombstone locations, repaired once and proved
	///   again.
	/// - Every table in the candidate, inherited ones included, must keep the
	///   frozen policy.
	async fn build_candidate(
		&self,
		index: &crate::storage::index::PersistentLocalIndex,
		children: &[ClosedChild],
	) -> Result<Vec<BaseEntry>, BuildRefusal> {
		type Slot = (TenantId, TableObjectId, [u8; 32]);
		let storage = |error: std::io::Error| BuildRefusal::Storage(error.to_string());
		let unproved =
			|refusal: super::reader::ReadRefusal| BuildRefusal::Incomplete(format!("{refusal:?}"));
		// Per key: the entry, and the child it comes from (None: the base).
		let mut winners: BTreeMap<Slot, (BaseEntry, Option<usize>)> = index
			.base_entries(false)
			.await
			.map_err(storage)?
			.into_iter()
			.map(|entry| {
				(
					(entry.tenant, entry.table, entry.key_identity),
					(entry, None),
				)
			})
			.collect();
		let mut descriptors = BTreeMap::new();
		for (at, child) in children.iter().enumerate() {
			let (tenant, table, generation) = (child.tenant, child.table, child.generation);
			let descriptor = self
				.state_machine
				.read_state(move |state| {
					super::child_writes::table_descriptor(state, tenant, table, generation)
				})
				.await
				.ok_or(BuildRefusal::NotInPlan)?;
			let manifest = self
				.proved_manifest(index, table, tenant, generation, &child.recorded)
				.await
				.map_err(unproved)?;
			for version in &manifest {
				let tombstone = super::deletes::is_tombstone_version(&descriptor, version);
				let candidate = BaseEntry {
					tenant,
					table,
					key_identity: version.key_identity,
					hlc: version.hlc,
					row_digest: version.row_digest,
					tombstone,
					// Set once the survivor is proved below.
					position: 0,
				};
				let slot = (tenant, table, version.key_identity);
				match winners.get(&slot) {
					Some((current, _)) if current.hlc > candidate.hlc => {}
					Some((current, _)) if current.hlc == candidate.hlc => {
						if current.row_digest != candidate.row_digest
							|| current.tombstone != candidate.tombstone
						{
							return Err(BuildRefusal::Conflict);
						}
					}
					_ => {
						winners.insert(slot, (candidate, Some(at)));
					}
				}
			}
			descriptors.insert((tenant, table), descriptor);
		}
		// The frozen policy holds for every table in the candidate.
		let state = self.state_machine.state_snapshot().await;
		let tables: std::collections::BTreeSet<(TenantId, TableObjectId)> = winners
			.keys()
			.map(|(tenant, table, _)| (*tenant, *table))
			.collect();
		for (tenant, table) in tables {
			if self
				.table_effective_retention_from_state(&state, tenant, table)
				.floor
				.is_some()
			{
				return Err(BuildRefusal::Policy);
			}
		}
		// Prove every survivor that comes from a child; repair a child once.
		let mut repaired = std::collections::BTreeSet::new();
		for (entry, source) in winners.values_mut() {
			let Some(at) = *source else {
				continue;
			};
			let child = &children[at];
			let descriptor = &descriptors[&(entry.tenant, entry.table)];
			let key = Identity::new(entry.key_identity);
			let mut proved = self
				.proved_version(entry.tenant, descriptor, key, entry.hlc, entry.row_digest)
				.await
				.map_err(unproved)?;
			if proved.is_none() && repaired.insert(at) {
				self.repair_child(child.recorded.rsv, child.recorded.ordinal, child.generation)
					.await
					.map_err(|refusal| BuildRefusal::Incomplete(format!("{refusal:?}")))?;
				proved = self
					.proved_version(entry.tenant, descriptor, key, entry.hlc, entry.row_digest)
					.await
					.map_err(unproved)?;
			}
			let Some((locality, location)) = proved else {
				return Err(BuildRefusal::Incomplete(
					"a survivor is not stored here with its content".into(),
				));
			};
			if location.is_tombstone != entry.tombstone {
				return Err(BuildRefusal::Incomplete(
					"a survivor is stored as the other kind".into(),
				));
			}
			entry.position = if entry.tombstone {
				crate::consensus::decisions::delete_row_position(entry.tenant, descriptor, key)
					.ok_or(BuildRefusal::Conflict)?
			} else {
				super::coordinator::row_position(descriptor, locality).map_err(storage)?
			};
		}
		Ok(winners.into_values().map(|(entry, _)| entry).collect())
	}
}
