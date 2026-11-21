//! ADR 0106 (wendelon): a promoted replacement's restricted storage.
//!
//! A control-only member recovering its slot's data opens its own store's
//! slab manager, under the data directory owner it already holds, with the
//! cluster's root enrolled (ADR 0726: an enrolled artifact is read, a root is
//! never minted here). It builds the same slab I/O, object-store tier and
//! crypto as a full node's store and nothing else: no data or control server,
//! lease, takeover, generation, certification, sweep, tiering upload, reclaim,
//! serving or election. Only the replacement's recovery uses it.

use super::*;
use arrow::record_batch::RecordBatch;

/// Opens the promoted store's slab manager under `owner`. The store's index
/// must exist (gate 2 created it) and must not be held by another handle: the
/// caller releases the preparation's index first.
pub(crate) async fn open_recovery_storage(
	cfg: &NodeBootstrapConfig,
	owner: &DataDirectoryOwner,
) -> std::io::Result<Arc<SlabManager>> {
	if !cfg.data_dir.join("storage_index.redb").exists() {
		return Err(Error::new(
			ErrorKind::NotFound,
			format!(
				"ReplacementStoreLost: no storage index in {}",
				cfg.data_dir.display()
			),
		));
	}
	let slab_io = spawn_io_thread_named_for_class(
		cfg.slab_io_depth,
		"wendelon-io-uring-slab",
		IoUringClass::Slab,
	)?
	.with_data_directory_owner(owner.clone());
	let l2 = cfg
		.l2
		.as_ref()
		.map(|config| config.build_with_owner(owner.clone()))
		.transpose()?;
	let root_kek = cfg
		.root_kek_provider
		.build_with_owner(
			&cfg.data_dir,
			owner,
			crate::crypto::RootKekBootstrapMode::EnrollExisting,
		)
		.map_err(|error| {
			if error.kind() != ErrorKind::NotFound {
				return error;
			}
			Error::new(
				ErrorKind::NotFound,
				format!(
					"RootKekNotEnrolled: a recovering replacement needs the cluster's root KEK artifact in its data directory ({error})"
				),
			)
		})?;
	let crypto = Arc::new(CryptoManager::new_with_cache_config_and_owner(
		root_kek,
		cfg.crypto_cache,
		owner.clone(),
	));
	let config = SlabManagerConfig {
		tier_history_budget_bytes: cfg.tier_history_budget_bytes,
		dir: cfg.data_dir.clone(),
		max_slab_bytes: cfg.slab_max_bytes,
		io: slab_io,
		l2,
		rehydration: cfg.rehydration.clone(),
		l2_column_fetch: cfg.l2_column_fetch.clone(),
		row_group_writer: cfg.row_group_writer.clone(),
		l2_row_group_fetch: cfg.l2_row_group_fetch.clone(),
		encoding_writer: cfg.encoding_writer.clone(),
		l2_chunk_layout: cfg.l2_chunk_layout.clone(),
		l2_encoding_fetch: cfg.l2_encoding_fetch.clone(),
		encoding_compute: cfg.encoding_compute.clone(),
		group_commit: cfg.group_commit.clone(),
		crypto,
	};
	SlabManager::open_with_owner(config, owner.clone()).await
}

/// Test-only (ADR 0107): store directories whose committed candidate stays
/// staged, the crash row between Commit applied and local activation.
#[cfg(test)]
pub(crate) static HOLD_ACTIVATION: std::sync::Mutex<std::collections::BTreeSet<PathBuf>> =
	std::sync::Mutex::new(std::collections::BTreeSet::new());

/// Test-only (ADR 0107, Codex 01a1e309 R1): one step of the store in this
/// directory pauses after its applied view was read, before activation:
/// `reached` is notified, then the step waits for `release`. One-shot.
#[cfg(test)]
pub(crate) static PAUSE_AFTER_VIEW: std::sync::Mutex<
	std::collections::BTreeMap<PathBuf, (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

/// ADR 0108 (wendelon) test seam: an in-process replacement activates only
/// where its test enables it (by store directory), so the ADR 0104 to 0107
/// controls keep their ControlOnly member; the product always activates.
#[cfg(test)]
pub(crate) static ACTIVATION_ENABLED: std::sync::Mutex<std::collections::BTreeSet<PathBuf>> =
	std::sync::Mutex::new(std::collections::BTreeSet::new());

/// ADR 0106 (wendelon): what one recovery step found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Progress {
	/// Not yet: a source, a child or the record is still owed.
	Pending(String),
	/// Validated and saved: this statement is to be forwarded.
	Ready(Box<crate::network::decision_proposals::ForwardedDecision>),
	/// The recovery is recorded (now or earlier) and the delta is current.
	Recorded,
	/// ADR 0107 (wendelon): recorded, delta current, and this store's signed
	/// report of its durably staged candidate, to be forwarded. Its answer
	/// never stops recovery: a later step reconciles from applied state.
	Report(Box<crate::network::decision_proposals::ForwardedDecision>),
	/// ADR 0108 (wendelon): this store's activation is in its own applied
	/// state: the ControlOnly member hands over to a Full one.
	Activated,
}

/// ADR 0106 (wendelon): the promoted replacement's recovery, run by its
/// preparation task after promotion and binding, on the restricted storage.
pub(crate) struct RecoveryDriver {
	pub(crate) slot: u32,
	pub(crate) instance: u32,
	pub(crate) state_machine: WendelonStateMachine,
	pub(crate) raft: WendelonRaft,
	pub(crate) provider: Arc<dyn TransportProvider>,
	pub(crate) pool: Arc<BufPool>,
	pub(crate) slab: Arc<SlabManager>,
	pub(crate) locks: Arc<crate::network::data_server::child_writes::ParentLocks>,
	pub(crate) key: ed25519_dalek::SigningKey,
	/// ADR 0107 (wendelon): this store's certification progress in this
	/// process (created once per task, never per step). `validated`: this
	/// process validated the store's data at its active generation.
	pub(crate) progress: crate::network::data_server::certification::CertificationProgress,
	/// The configured cluster retention policy (the frozen-policy check).
	pub(crate) retention: crate::storage::retention::MvccRetentionPolicy,
	/// The node's wall clock (`SystemClock` in production; the cluster's in a test).
	pub(crate) clock: Arc<dyn crate::consensus::monitor::PhysicalClock>,
}

/// The applied facts one step works from.
struct View {
	token: u128,
	birth: crate::consensus::decisions::instances::Birth,
	store: crate::consensus::StoreIncarnation,
	recorded: Option<crate::consensus::decisions::instances::Recovery>,
	certificate: Option<crate::consensus::decisions::generations::Certificate>,
	applied: u64,
	/// Every other member's slot with its address.
	sources: Vec<(u32, String)>,
}

/// One child this store owes: an Accepted parent's child whose plan names the
/// slot at an ancestor-or-self of the current store.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OwedChild {
	pub(crate) rsv: u64,
	pub(crate) ordinal: u16,
	pub(crate) dec: u64,
	claim_digest: [u8; 32],
	content: [u8; 32],
	generation: u64,
}

impl RecoveryDriver {
	fn connect(&self) -> crate::network::data_server::repair::Connect {
		let provider = self.provider.clone();
		let raft = self.raft.clone();
		let state_machine = self.state_machine.clone();
		Arc::new(move |slot: u32| {
			let (provider, raft, state_machine) =
				(provider.clone(), raft.clone(), state_machine.clone());
			Box::pin(async move {
				let address = member_address(&raft, &state_machine, slot)
					.await
					.ok_or_else(|| Error::other(format!("slot {slot} has no member address")))?;
				provider
					.connect(
						NodeId {
							id: slot,
							_reserved: 0,
						},
						&address,
					)
					.await
			})
		})
	}

	async fn view(&self) -> std::io::Result<View> {
		let (slot, instance) = (self.slot, self.instance);
		let found = self
			.state_machine
			.read_state(move |state| {
				let decisions = state.decisions.as_ref()?;
				let instances = decisions.instances();
				let (token, replacement) =
					instances.replacements().iter().find(|(_, record)| {
						record.slot == slot
							&& record.phase
								== crate::consensus::decisions::instances::ReplacementPhase::Done {
									r_new: instance,
								}
					})?;
				let store = replacement.promoted?.incarnation;
				let birth = instances.record(instance)?.birth;
				let current = crate::consensus::decisions::current_replacement(
					state, slot, *token, instance, birth, store,
				);
				Some((
					current,
					*token,
					birth,
					store,
					replacement.recovered,
					decisions.generations().current.clone(),
					state.last_applied_log_id.map_or(0, |id| id.index),
				))
			})
			.await;
		let Some((current, token, birth, store, recorded, certificate, applied)) = found else {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"RecoveryNotAuthorized: no promoted replacement names this instance",
			));
		};
		if !current {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"RecoveryNotAuthorized: this store is no longer its slot's current replacement",
			));
		}
		let sources = other_members(&self.raft, &self.state_machine, self.slot).await;
		Ok(View {
			token,
			birth,
			store,
			recorded,
			certificate,
			applied,
			sources,
		})
	}

	/// One step. An error is final (the caller stops); `Pending` is retried.
	pub(crate) async fn step(&self) -> std::io::Result<Progress> {
		let view = self.view().await?;
		let index = self
			.slab
			.persistent_index()
			.ok_or_else(|| Error::other("the recovery's store has no index"))?;
		let saved = index.recovery_submission().await?;
		// The cut: the saved one, else the certificate now and the applied index.
		let (gid, digest, k) = match saved {
			Some(saved) => (saved.gid, saved.target_digest, saved.k),
			None => (
				view.certificate
					.as_ref()
					.map_or(0, |certificate| certificate.gid),
				view.certificate
					.as_ref()
					.map(|certificate| certificate.target_digest),
				view.applied,
			),
		};
		if let Some(recorded) = view.recorded {
			// The record is historical. The statement saved here must be exactly
			// it; and at each start the stored data is validated again, at the
			// store's active generation, before recovery counts as complete
			// (missing children are repaired; a damaged base fails closed).
			let reconciled = saved.is_some_and(|saved| {
				(saved.token, saved.r_new, saved.birth, saved.store)
					== (view.token, self.instance, view.birth, view.store)
					&& (saved.gid, saved.target_digest, saved.k)
						== (recorded.gid, recorded.target_digest, recorded.k)
			});
			if !reconciled {
				return Err(Error::other(
					"RecoveryConflict: the recorded statement is not the one saved here",
				));
			}
			return self.after_record(&view, &index, recorded).await;
		}
		// Before any record the certificate cannot move (this store's report is
		// owed to every commit), so G is fixed.
		if gid > 0 {
			let certificate = view
				.certificate
				.clone()
				.filter(|certificate| {
					certificate.gid == gid && Some(certificate.target_digest) == digest
				})
				.ok_or_else(|| {
					Error::other("RecoveryStale: the certificate moved before any record")
				})?;
			if let Progress::Pending(why) = self.install_base(&view, &certificate).await? {
				return Ok(Progress::Pending(why));
			}
		}
		if let Progress::Pending(why) = self.repair_owed(&view, 0, k).await? {
			return Ok(Progress::Pending(why));
		}
		self.validate(&index, gid, view.certificate.as_ref(), k)
			.await?;
		self.progress
			.validated
			.store(true, std::sync::atomic::Ordering::SeqCst);
		let submission = crate::storage::index::certified_base::RecoverySubmission {
			token: view.token,
			r_new: self.instance,
			birth: view.birth,
			store: view.store,
			gid,
			target_digest: digest,
			k,
		};
		if !index.save_recovery_submission(submission).await? {
			return Err(Error::other(
				"RecoveryConflict: another statement is saved here",
			));
		}
		let body = crate::consensus::decisions::RecoveryBody {
			token: submission.token,
			slot: self.slot,
			r_new: submission.r_new,
			birth: submission.birth,
			store: submission.store,
			gid,
			target_digest: digest,
			k,
		};
		let proof = crate::consensus::decisions::evidence::sign(
			&self.key,
			body,
			crate::consensus::decisions::Signer {
				node: self.slot,
				incarnation: view.store,
			},
		);
		Ok(Progress::Ready(Box::new(
			crate::network::decision_proposals::ForwardedDecision::RecoveryProved { proof },
		)))
	}

	/// What this store's repairs and its candidate build use: its own signer
	/// (slot at the promoted store) and the other members as sources.
	fn repair_context(&self, view: &View) -> crate::network::data_server::repair::RepairContext {
		let local = crate::consensus::decisions::Signer {
			node: self.slot,
			incarnation: view.store,
		};
		crate::network::data_server::repair::RepairContext {
			local,
			signer: Arc::new(crate::network::data_server::child_writes::DecisionSigner {
				key: self.key.clone(),
				signer: local,
			}),
			state_machine: self.state_machine.clone(),
			slab: self.slab.clone(),
			locks: self.locks.clone(),
			pool: self.pool.clone(),
			connect: self.connect(),
			index_buffers: false,
		}
	}

	/// ADR 0107 (wendelon) C4: one step after the exact reconciliation of the
	/// historical record, in this order, alike while running and after a
	/// reopen:
	/// 1. a certificate committed past this store's active base is activated
	///    from this store's own staged candidate first (its report was owed to
	///    that commit); no other state is accepted, and nothing is reset;
	/// 2. unless this process validated the store at its active generation:
	///    the owed children through the record's K are repaired, then the
	///    stored base is validated against the CURRENT certificate and every
	///    retained owed child through K is verified;
	/// 3. the delta after K is repaired;
	/// 4. only with the delta current, the pending target's candidate is
	///    built, staged and its report signed.
	async fn after_record(
		&self,
		view: &View,
		index: &crate::storage::index::PersistentLocalIndex,
		recorded: crate::consensus::decisions::instances::Recovery,
	) -> std::io::Result<Progress> {
		use crate::network::data_server::certification::{
			CandidateBuild, StoreCertification, under_permit,
		};
		use std::sync::atomic::Ordering;
		#[cfg(test)]
		{
			let paused = PAUSE_AFTER_VIEW
				.lock()
				.unwrap()
				.remove(self.slab.data_dir());
			if let Some((reached, release)) = paused {
				reached.notify_one();
				release.notified().await;
			}
		}
		let repair = self.repair_context(view);
		let certification = StoreCertification {
			slab: &self.slab,
			state_machine: &self.state_machine,
			progress: &self.progress,
			locks: &self.locks,
			build: Some(CandidateBuild {
				repair: &repair,
				retention: self.retention,
				now_ms: self.clock.now_ms(),
				// C5: prove inherited winners from this store's own data before
				// signing for them.
				reprove_inherited: true,
				#[cfg(test)]
				peer: None,
			}),
			#[cfg(test)]
			hold_activation: HOLD_ACTIVATION
				.lock()
				.unwrap()
				.contains(self.slab.data_dir()),
		};
		under_permit(&self.slab, certification.activate_committed()).await?;
		if self.progress.behind.load(Ordering::Acquire) {
			return Err(Error::other(
				"RecoveryBehind: a committed generation has no matching staged candidate here",
			));
		}
		// Codex 01a1e309 R1: the certificate is read again AFTER activation (the
		// view's may be older; activation read its own), so activation, the
		// active-base comparison and validation use one coherent certificate.
		// A mismatch then means a certificate committed after activation's own
		// read: not a store fault (missing or mismatched staging set `behind`
		// above, and that stays a stop); the next step activates it.
		let certificate = self
			.state_machine
			.read_state(|state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.generations().current.clone())
			})
			.await;
		let current = certificate
			.as_ref()
			.map_or(0, |certificate| certificate.gid);
		let active = index.base_meta().await?.active_gid;
		if active != current {
			#[cfg(test)]
			if certification.hold_activation {
				return Ok(Progress::Pending("activation held (test)".into()));
			}
			return Ok(Progress::Pending(format!(
				"the certificate moved to gid {current} after this step's activation (active {active})"
			)));
		}
		if !self.progress.validated.load(Ordering::SeqCst) {
			if let Progress::Pending(why) = self.repair_owed(view, 0, recorded.k).await? {
				return Ok(Progress::Pending(why));
			}
			self.validate(index, current, certificate.as_ref(), recorded.k)
				.await?;
			self.progress.validated.store(true, Ordering::SeqCst);
		}
		if let Progress::Pending(why) = self.repair_owed(view, recorded.k, u64::MAX).await? {
			return Ok(Progress::Pending(why));
		}
		if let Some(report) = under_permit(&self.slab, certification.prepare_report()).await? {
			return Ok(Progress::Report(Box::new(
				crate::network::decision_proposals::ForwardedDecision::Report { report },
			)));
		}
		self.activation(view, index, recorded).await
	}

	/// ADR 0108 (wendelon) D1: the activation from ONE view V read at applied
	/// index A: V's certificate C_A must be this store's active base, and every
	/// child V owes the slot through A (from V's retained bodies) must be held
	/// here as a Complete intent of its recorded content. The set is never
	/// sampled again: anything missing ends the step Pending, and a later step
	/// reads a new view. An activation already in applied state is `Activated`.
	async fn activation(
		&self,
		view: &View,
		index: &crate::storage::index::PersistentLocalIndex,
		recorded: crate::consensus::decisions::instances::Recovery,
	) -> std::io::Result<Progress> {
		#[cfg(test)]
		if !ACTIVATION_ENABLED
			.lock()
			.unwrap()
			.contains(self.slab.data_dir())
		{
			return Ok(Progress::Recorded);
		}
		let (slot, token) = (self.slot, view.token);
		let (applied, certified, owed, activated) = self
			.state_machine
			.read_state(move |state| {
				let decisions = state.decisions.as_ref();
				let applied = state.last_applied_log_id.map_or(0, |id| id.index);
				let certified = decisions
					.and_then(|decisions| decisions.generations().current.as_ref())
					.map_or(0, |certificate| certificate.gid);
				let activated = decisions
					.and_then(|decisions| decisions.instances().replacement(token))
					.and_then(|replacement| replacement.activated);
				(
					applied,
					certified,
					owed_children(state, slot, 0, applied),
					activated,
				)
			})
			.await;
		if let Some(activation) = activated {
			if activation.incarnation != view.store {
				return Err(Error::other(
					"ActivationConflict: another store's activation is recorded for this replacement",
				));
			}
			return Ok(Progress::Activated);
		}
		let active = index.base_meta().await?.active_gid;
		if active != certified {
			return Ok(Progress::Pending(format!(
				"activation waits for certificate gid {certified} to be active here (active {active})"
			)));
		}
		for child in &owed {
			let held = index
				.parent_intent(child.rsv, child.ordinal)
				.await?
				.is_some_and(|intent| {
					intent.state
						== crate::storage::index::parent_intents::ParentIntentState::Complete
						&& intent.claim_digest == child.claim_digest
						&& intent.content == child.content
				});
			if !held {
				return Ok(Progress::Pending(format!(
					"activation through {applied} waits for child ({}, {})",
					child.rsv, child.ordinal
				)));
			}
		}
		let store = crate::consensus::decisions::Signer {
			node: slot,
			incarnation: view.store,
		};
		let proof = crate::consensus::decisions::evidence::sign(
			&self.key,
			crate::consensus::decisions::ActivationBody {
				token,
				slot,
				r_new: self.instance,
				birth: view.birth,
				store: view.store,
				recovered_gid: recorded.gid,
				k: recorded.k,
				through: applied,
				certified,
			},
			store,
		);
		Ok(Progress::Ready(Box::new(
			crate::network::decision_proposals::ForwardedDecision::ActivateReplacement { proof },
		)))
	}

	/// Every child the store owes with `after < dec <= through`, by decision
	/// position: an Accepted parent's child whose plan names this slot at an
	/// ancestor-or-self of the store.
	async fn owed(&self, after: u64, through: u64) -> Vec<OwedChild> {
		let slot = self.slot;
		self.state_machine
			.read_state(move |state| owed_children(state, slot, after, through))
			.await
	}

	/// Repairs every owed child in the interval that this store does not hold
	/// as a Complete intent of the recorded content. None is skipped: a child
	/// still missing keeps the step pending.
	async fn repair_owed(
		&self,
		view: &View,
		after: u64,
		through: u64,
	) -> std::io::Result<Progress> {
		let index = self
			.slab
			.persistent_index()
			.ok_or_else(|| Error::other("the recovery's store has no index"))?;
		let context = self.repair_context(view);
		let mut missing = 0;
		for child in self.owed(after, through).await {
			let held = index
				.parent_intent(child.rsv, child.ordinal)
				.await?
				.is_some_and(|intent| {
					intent.state
						== crate::storage::index::parent_intents::ParentIntentState::Complete
						&& intent.claim_digest == child.claim_digest
						&& intent.content == child.content
				});
			if held {
				continue;
			}
			if crate::network::data_server::repair::repair_child_in(
				&context,
				child.rsv,
				child.ordinal,
				child.generation,
			)
			.await
			.is_err()
			{
				missing += 1;
			}
		}
		Ok(if missing == 0 {
			Progress::Recorded
		} else {
			Progress::Pending(format!("{missing} owed children not yet repaired"))
		})
	}

	/// Installs the certified base at `certificate`'s gid from the other
	/// members, unless it is already active here. Two phases, so untrusted data
	/// never reaches this store's versions:
	/// 1. one source's entries only, staged and checked whole against the
	///    certificate (counts and sums); a mismatch discards that staging (only
	///    metadata) and the next source is tried;
	/// 2. the rows of exactly those verified entries, page by page from any
	///    source (resumed at the cursor across sources): a page is stored only
	///    when its entries equal the staged ones and each row is its entry's.
	///
	/// Then the staged base is published. A certificate whose base is empty is
	/// staged and published empty (its sums are the empty ones).
	async fn install_base(
		&self,
		view: &View,
		certificate: &crate::consensus::decisions::generations::Certificate,
	) -> std::io::Result<Progress> {
		use crate::storage::index::certified_base::StagedCandidate;
		let index = self
			.slab
			.persistent_index()
			.ok_or_else(|| Error::other("the recovery's store has no index"))?;
		// DC-269: an already installed base is checked against this trusted
		// certificate's cut (written from it if unconfirmed); another fails closed.
		use crate::storage::index::certified_base::CutConfirmation;
		match index
			.confirm_installed_cut(certificate.gid, certificate.chunks.clone(), certificate.t)
			.await?
		{
			CutConfirmation::Confirmed => return Ok(Progress::Recorded),
			CutConfirmation::Upgraded => {
				tracing::info!(
					gid = certificate.gid,
					t = certificate.t,
					"the recovered base's cut was written from its certificate"
				);
				return Ok(Progress::Recorded);
			}
			// The certificate's generation is installed with other chunks: not
			// its base. Nothing is confirmed, written or reinstalled.
			CutConfirmation::OtherBase => {
				return Err(Error::new(
					std::io::ErrorKind::InvalidData,
					format!(
						"generation {} is installed with chunks other than its certificate's",
						certificate.gid
					),
				));
			}
			CutConfirmation::NotInstalled => {}
		}
		let candidate = StagedCandidate {
			gid: certificate.gid,
			target_digest: certificate.target_digest,
			predecessor_gid: certificate.predecessor_gid,
			chunks: certificate.chunks.clone(),
		};
		// Phase 1.
		if !index.recovered_staging_matches(candidate.clone()).await? {
			let mut verified = false;
			let mut last = String::from("no member to fetch the base from");
			for (source, address) in &view.sources {
				if let Err(refusal) = index.discard_recovered_staging().await? {
					return Err(Error::other(format!(
						"recovery staging refused: {refusal:?}"
					)));
				}
				if let Err(refusal) = index
					.stage_recovered_page(candidate.clone(), Vec::new())
					.await?
				{
					return Err(Error::other(format!(
						"recovery staging refused: {refusal:?}"
					)));
				}
				match self
					.stage_entries(view, &index, &candidate, certificate, *source, address)
					.await
				{
					Ok(()) if index.recovered_staging_matches(candidate.clone()).await? => {
						verified = true;
						break;
					}
					Ok(()) => {
						last = format!("slot {source}: its entries do not hash to the certificate")
					}
					Err(why) => last = format!("slot {source}: {why}"),
				}
			}
			if !verified {
				let _ = index.discard_recovered_staging().await?;
				return Ok(Progress::Pending(last));
			}
		}
		// Phase 2.
		let mut after: Option<Vec<u8>> = None;
		let mut last = String::from("no member to fetch the rows from");
		'pages: loop {
			for (source, address) in &view.sources {
				match self
					.fetch_rows(view, &index, certificate, *source, address, after.clone())
					.await
				{
					Ok(Some(next)) => {
						after = Some(next);
						continue 'pages;
					}
					Ok(None) => break 'pages,
					Err(why) => last = format!("slot {source}: {why}"),
				}
			}
			return Ok(Progress::Pending(last));
		}
		match index.publish_recovered(candidate, certificate.t).await? {
			Ok(()) => Ok(Progress::Recorded),
			Err(refusal) => Err(Error::other(format!(
				"publication of a verified recovery was refused: {refusal:?}"
			))),
		}
	}

	fn request(
		&self,
		view: &View,
		certificate: &crate::consensus::decisions::generations::Certificate,
		after: Option<Vec<u8>>,
		max_bytes: u32,
		with_rows: bool,
	) -> crate::network::data_server::base_fetch::BaseFetchRequest {
		crate::network::data_server::base_fetch::BaseFetchRequest {
			token: view.token,
			r_new: self.instance,
			birth: view.birth,
			store: view.store,
			gid: certificate.gid,
			target_digest: certificate.target_digest,
			after,
			max_rows: crate::network::data_server::base_fetch::MAX_PAGE_ROWS,
			max_bytes,
			with_rows,
		}
	}

	/// The table descriptor a page's entries name.
	async fn page_descriptor(
		&self,
		entry: crate::storage::index::certified_base::BaseEntry,
	) -> Option<crate::schema::TableSchemaDescriptor> {
		self.state_machine
			.read_state(move |state| {
				crate::network::data_server::child_writes::recorded_latest_table_descriptor(
					state,
					entry.tenant,
					entry.table,
				)
			})
			.await
	}

	/// Phase 1 from one source: every entry, checked page by page, staged.
	async fn stage_entries(
		&self,
		view: &View,
		index: &crate::storage::index::PersistentLocalIndex,
		candidate: &crate::storage::index::certified_base::StagedCandidate,
		certificate: &crate::consensus::decisions::generations::Certificate,
		source: u32,
		address: &str,
	) -> Result<(), String> {
		use crate::network::data_server::base_fetch::{
			BaseFetchAnswer, MAX_PAGE_BYTES, check_page,
		};
		let mut after = None;
		loop {
			let request = self.request(view, certificate, after.clone(), MAX_PAGE_BYTES, false);
			let (answer, rows) = fetch_page(&self.provider, &self.pool, source, address, &request)
				.await
				.map_err(|error| error.to_string())?;
			let (entries, next) = match answer {
				BaseFetchAnswer::Page { entries, next } => (entries, next),
				BaseFetchAnswer::Refused(refusal) => return Err(format!("refused: {refusal:?}")),
			};
			if let Some(first) = entries.first().copied() {
				let descriptor = self
					.page_descriptor(first)
					.await
					.ok_or("a page names an unknown table")?;
				check_page(&request, &entries, &next, rows.as_ref(), &descriptor)
					.map_err(|refusal| format!("page refused: {refusal:?}"))?;
				match index
					.stage_recovered_page(candidate.clone(), entries)
					.await
					.map_err(|error| error.to_string())?
				{
					Ok(()) => {}
					Err(refusal) => return Err(format!("staging refused: {refusal:?}")),
				}
			} else if next.is_some() || rows.is_some() {
				return Err("an empty page that is not the end".into());
			}
			match next {
				Some(next) => after = Some(next),
				None => return Ok(()),
			}
		}
	}

	/// Phase 2, one page from one source after `after`: its entries must be
	/// the verified staged ones and each row its entry's; only then are the rows
	/// stored. `Ok(None)` at the end. A row larger than a normal page is asked
	/// again at the largest page bound.
	async fn fetch_rows(
		&self,
		view: &View,
		index: &crate::storage::index::PersistentLocalIndex,
		certificate: &crate::consensus::decisions::generations::Certificate,
		source: u32,
		address: &str,
		after: Option<Vec<u8>>,
	) -> Result<Option<Vec<u8>>, String> {
		use crate::network::data_server::base_fetch::{
			BaseFetchAnswer, BaseFetchRefusal, MAX_PAGE_BYTES, MAX_ROW_PAGE_BYTES, check_page,
		};
		let mut max_bytes = MAX_PAGE_BYTES;
		let (request, entries, next, rows) = loop {
			let request = self.request(view, certificate, after.clone(), max_bytes, true);
			let (answer, rows) = fetch_page(&self.provider, &self.pool, source, address, &request)
				.await
				.map_err(|error| error.to_string())?;
			match answer {
				BaseFetchAnswer::Page { entries, next } => break (request, entries, next, rows),
				BaseFetchAnswer::Refused(BaseFetchRefusal::RowTooLarge)
					if max_bytes < MAX_ROW_PAGE_BYTES =>
				{
					max_bytes = MAX_ROW_PAGE_BYTES;
				}
				BaseFetchAnswer::Refused(refusal) => return Err(format!("refused: {refusal:?}")),
			}
		};
		// The page must be exactly the next verified staged entries after the
		// cursor, and a last page must reach the staging's end: a gap, a skip or
		// an early end is refused before anything is stored, and the next source
		// is asked from the same cursor.
		let cursor: Option<[u8; 56]> = match &after {
			Some(after) => Some(
				after
					.as_slice()
					.try_into()
					.map_err(|_| "a malformed cursor".to_string())?,
			),
			None => None,
		};
		let staged = index
			.recovered_page(cursor, entries.len() + 1)
			.await
			.map_err(|error| error.to_string())?;
		if staged.len() < entries.len() || staged[..entries.len()] != entries[..] {
			return Err("a page is not the next verified entries".into());
		}
		if next.is_none() && staged.len() > entries.len() {
			return Err("a page ends before every verified entry".into());
		}
		let Some(first) = entries.first().copied() else {
			return if next.is_none() && rows.is_none() {
				Ok(None)
			} else {
				Err("an empty page that is not the end".into())
			};
		};
		let descriptor = self
			.page_descriptor(first)
			.await
			.ok_or("a page names an unknown table")?;
		check_page(&request, &entries, &next, rows.as_ref(), &descriptor)
			.map_err(|refusal| format!("page refused: {refusal:?}"))?;
		self.ingest_page(&descriptor, &entries, rows.as_ref())
			.await
			.map_err(|error| error.to_string())?;
		// The source's continuation, or none: the page just stored was the last.
		Ok(next)
	}

	/// Stores a checked page's rows durably, at their exact versions, before
	/// its entries are staged: rows grouped by HLC, tombstones one by one.
	async fn ingest_page(
		&self,
		descriptor: &crate::schema::TableSchemaDescriptor,
		entries: &[crate::storage::index::certified_base::BaseEntry],
		rows: Option<&RecordBatch>,
	) -> std::io::Result<()> {
		let mut row = 0_u32;
		// Each HLC group's row indices and, index-aligned, their entries'
		// identities (wendelon ADR 0119 D3: an append-only row keeps its certified
		// identity, never one derived from its place in the group).
		let mut by_hlc: std::collections::BTreeMap<
			crate::schema::HybridTimestamp,
			(Vec<u32>, Vec<crate::schema::Identity>),
		> = std::collections::BTreeMap::new();
		for entry in entries {
			if entry.tombstone {
				let delete = crate::consensus::decisions::TombstoneTarget {
					table: descriptor.table_object_id,
					schema_generation: descriptor.schema_generation,
					row: crate::schema::Identity::new(entry.key_identity),
				};
				crate::network::data_server::deletes::append_tombstone(
					&self.slab,
					entry.tenant,
					descriptor,
					&delete,
					entry.hlc,
					&[],
				)
				.await?;
			} else {
				let group = by_hlc.entry(entry.hlc).or_default();
				group.0.push(row);
				group
					.1
					.push(crate::schema::Identity::new(entry.key_identity));
				row += 1;
			}
		}
		if let Some(rows) = rows {
			let tenant = entries[0].tenant;
			for (hlc, (indices, identities)) in by_hlc {
				let batch = arrow::compute::take_record_batch(
					rows,
					&arrow::array::UInt32Array::from(indices),
				)
				.map_err(Error::other)?;
				crate::network::data_server::child_writes::ingest_recovered_rows(
					&self.slab,
					tenant,
					descriptor,
					&batch,
					hlc,
					&identities,
				)
				.await?;
			}
		}
		Ok(())
	}

	/// The proof, from this store's actual data: its base is active at the cut's
	/// gid and hashes to that certificate (or genesis, empty), every entry is
	/// stored, and every owed child through `k` is a Complete intent whose rows
	/// verify.
	async fn validate(
		&self,
		index: &crate::storage::index::PersistentLocalIndex,
		gid: u64,
		certificate: Option<&crate::consensus::decisions::generations::Certificate>,
		k: u64,
	) -> std::io::Result<()> {
		let meta = index.base_meta().await?;
		if meta.active_gid != gid {
			return Err(Error::other(format!(
				"the base is not active at gid {gid} here (active {})",
				meta.active_gid
			)));
		}
		// The sums are recomputed from the stored entries, never taken from the
		// recorded metadata; genesis holds none. Streamed by bounded pages: only
		// the 256 accumulators and one page are held.
		let expected = match certificate {
			Some(certificate) if gid > 0 => certificate.chunks.clone(),
			_ => crate::storage::index::certified_base::chunk_sums(std::iter::empty()),
		};
		let mut sums = crate::consensus::decisions::generations::ChunkAccumulator::new();
		let mut after: Option<[u8; 56]> = None;
		loop {
			let page = index.base_page(after, 1024).await?;
			let Some(last) = page.last() else {
				break;
			};
			after = Some(last.key());
			for entry in &page {
				sums.add(entry.chunk(), &entry.preimage());
				crate::network::data_server::certification::entry_is_stored(
					&self.slab,
					&self.state_machine,
					entry,
				)
				.await
				.map_err(Error::other)?;
			}
		}
		if sums.finish() != expected {
			return Err(Error::other(format!(
				"RecoveryDamaged: the base stored here does not hash to gid {gid}'s certificate"
			)));
		}
		for child in self.owed(0, k).await {
			let intent = index
				.parent_intent(child.rsv, child.ordinal)
				.await?
				.filter(|intent| {
					intent.state
						== crate::storage::index::parent_intents::ParentIntentState::Complete
						&& intent.content == child.content
				})
				.ok_or_else(|| Error::other("an owed child is not held here"))?;
			let (tenant, table) = (
				intent.tenant,
				crate::network::data_server::child_writes::parent_table(
					self.namespace_of(child.rsv).await?,
				),
			);
			let descriptor = self
				.state_machine
				.read_state(move |state| {
					crate::network::data_server::child_writes::recorded_latest_table_descriptor(
						state, tenant, table,
					)
				})
				.await
				.ok_or_else(|| Error::other("an owed child's table is unknown"))?;
			crate::network::data_server::child_writes::verify_owned_rows(
				&self.slab,
				&index,
				&intent,
				&descriptor,
			)
			.await
			.map_err(|refusal| Error::other(format!("{refusal:?}")))?;
		}
		Ok(())
	}

	async fn namespace_of(
		&self,
		rsv: u64,
	) -> std::io::Result<crate::consensus::decisions::ParentNamespace> {
		self.state_machine
			.read_state(move |state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.body(rsv))
					.map(|body| body.claim.key.namespace)
			})
			.await
			.ok_or_else(|| Error::other("an owed child's parent is no longer retained"))
	}
}

/// One base page from `source` at `address`, within the decision exchange
/// deadline.
async fn fetch_page(
	provider: &Arc<dyn TransportProvider>,
	pool: &Arc<BufPool>,
	source: u32,
	address: &str,
	request: &crate::network::data_server::base_fetch::BaseFetchRequest,
) -> std::io::Result<(
	crate::network::data_server::base_fetch::BaseFetchAnswer,
	Option<RecordBatch>,
)> {
	let meta = postcard::to_stdvec(request).map_err(Error::other)?;
	let exchange = async {
		let connection = provider
			.connect(
				NodeId {
					id: source,
					_reserved: 0,
				},
				address,
			)
			.await?;
		let mut stream = connection.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: crate::consensus::decision_mode::RAFT_FRAME_VERSION_PARENT_DECISIONS,
			msg_type: MsgType::TABLE_BASE_FETCH_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, &meta, None, None).await?;
		stream.finish().await?;
		// The answer's size is checked from its header before its body is read:
		// meta plus Arrow payload within the request's byte bound.
		let header = crate::network::core::read_frame_header(&mut *stream).await?;
		// Wendelon ADR 0119 D6: the exact envelope version, before the body.
		crate::consensus::decision_mode::check_peer_exchange_version(header.version)?;
		let size = u64::from(header.meta_len)
			+ u64::from(header.arrow_len)
			+ u64::from(header.trailing_len);
		if size > crate::network::data_server::base_fetch::answer_bound(request) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("a base page of {size} bytes exceeds its bound"),
			));
		}
		let frame =
			crate::network::core::read_frame_from_header(&mut *stream, pool, header).await?;
		if frame.header.msg_type != MsgType::TABLE_BASE_FETCH_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected TABLE_BASE_FETCH_RESP, got 0x{:02x}",
					frame.header.msg_type
				),
			));
		}
		let answer = crate::network::decision_proposals::decode_bounded(frame.meta.as_ref())?;
		let rows = match frame.arrow_payload.as_ref() {
			Some(bytes) if !bytes.is_empty() => {
				Some(crate::storage::multirow::decode_batch(bytes)?)
			}
			_ => None,
		};
		Ok((answer, rows))
	};
	tokio::time::timeout(
		crate::network::decision_proposals::DECISION_RPC_DEADLINE,
		exchange,
	)
	.await
	.map_err(|_| Error::new(ErrorKind::TimedOut, "base page unanswered"))?
}

async fn member_address(
	raft: &WendelonRaft,
	state_machine: &WendelonStateMachine,
	slot: u32,
) -> Option<String> {
	let membership = raft.metrics().borrow().membership_config.clone();
	for (instance, node) in membership.membership().nodes() {
		let instance = *instance;
		let mapped = state_machine
			.read_state(move |state| {
				crate::consensus::decisions::instances::slot_of(state, instance).ok()
			})
			.await;
		if mapped == Some(slot) {
			return Some(node.addr.clone());
		}
	}
	None
}

async fn other_members(
	raft: &WendelonRaft,
	state_machine: &WendelonStateMachine,
	own: u32,
) -> Vec<(u32, String)> {
	let membership = raft.metrics().borrow().membership_config.clone();
	let mut members = Vec::new();
	for (instance, node) in membership.membership().nodes() {
		let instance = *instance;
		let slot = state_machine
			.read_state(move |state| {
				crate::consensus::decisions::instances::slot_of(state, instance).ok()
			})
			.await;
		if let Some(slot) = slot
			&& slot != own
		{
			members.push((slot, node.addr.clone()));
		}
	}
	members
}

/// Every child `slot`'s current store owes with `after < dec <= through`,
/// selected by decision position (a reservation at or below `after` that is
/// accepted above it is owed): an Accepted parent's child whose plan names the
/// slot at an ancestor-or-self of the store.
pub(crate) fn owed_children(
	state: &crate::consensus::raft_storage::SmState,
	slot: u32,
	after: u64,
	through: u64,
) -> Vec<OwedChild> {
	use crate::consensus::decisions::ParentStatus;
	let Some(decisions) = state.decisions.as_ref() else {
		return Vec::new();
	};
	let Some(store) = state
		.incarnations
		.get(&slot)
		.and_then(|registry| registry.current)
	else {
		return Vec::new();
	};
	let mut owed = Vec::new();
	for body in decisions.bodies().values() {
		let ParentStatus::Accepted { dec } = body.status else {
			continue;
		};
		if dec <= after || dec > through {
			continue;
		}
		let tenant = body.claim.key.tenant;
		let table =
			crate::network::data_server::child_writes::parent_table(body.claim.key.namespace);
		let generation = if table == crate::schema::TableObjectId::DEFAULT_KV {
			0
		} else {
			// Wendelon ADR 0120 D4: an owed CTAS chunk (provisional or retained)
			// has its real generation from the private lookup.
			crate::network::data_server::child_writes::recorded_latest_table_descriptor(
				state, tenant, table,
			)
			.map_or(u64::MAX, |descriptor| descriptor.schema_generation)
		};
		for (ordinal, child) in body.claim.children.iter().enumerate() {
			let names = child.plan.iter().any(|member| {
				member.node == slot
					&& crate::consensus::decisions::in_recorded_ancestry(
						state,
						slot,
						store,
						member.incarnation,
					)
			});
			if names {
				owed.push(OwedChild {
					rsv: body.rsv,
					ordinal: ordinal as u16,
					dec,
					claim_digest: body.claim_digest,
					content: child.content,
					generation,
				});
			}
		}
	}
	owed.sort_by_key(|child| (child.dec, child.rsv, child.ordinal));
	owed
}
