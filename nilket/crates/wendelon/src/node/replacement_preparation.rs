//! ADR 0105 (wendelon): a control-only replacement's preparation.
//!
//! After its replacement is Done, the replacement admits its new store J as
//! its slot's current incarnation, then binds J to its slot's configured
//! decision key. Each step is proposed only while applied state lacks it, so a
//! restart at any point resumes from applied state with the same J. Nothing
//! here serves, signs client work, takes a lease, coordinates or campaigns:
//! the member stays control-only.

use super::*;
use crate::consensus::StoreIncarnation;
use crate::consensus::decision_mode::DecisionMode;
use crate::consensus::decisions::instances::{Birth, ReplacementPhase};
use crate::network::decision_proposals::{
	DECISION_RPC_DEADLINE, ForwardedDecision, ProposalResponse, ProposeRequest,
	check_response_frame, decode_bounded,
};
use crate::storage::index::PersistentLocalIndex;

/// How often preparation re-reads applied state.
const TICK: std::time::Duration = std::time::Duration::from_millis(200);

/// A preparation step a test may stop before.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
	Promote,
	Bind,
	/// ADR 0106 (wendelon): the recovery's storage opens, but no recovery step
	/// runs.
	Recover,
}

/// What applied state says remains to do.
#[derive(Debug, PartialEq, Eq)]
enum Next {
	/// No Done replacement names this instance yet.
	Wait,
	Promote(ForwardedDecision),
	Bind(ForwardedDecision),
	/// Promoted and bound, or superseded by a later replacement.
	Finished,
}

pub(crate) struct Preparation {
	slot: u32,
	instance: u32,
	mode: DecisionMode,
	raft: WendelonRaft,
	state_machine: WendelonStateMachine,
	provider: Arc<dyn TransportProvider>,
	pool: Arc<BufPool>,
	owner: DataDirectoryOwner,
	data_dir: PathBuf,
	/// The verifying key of this slot's configured decision secret.
	key: [u8; 32],
	/// The store index, opened once and held until the owner closes (an owned
	/// redb releases only at that close).
	store: tokio::sync::Mutex<Option<PersistentLocalIndex>>,
	/// The promotion this task first proposed: proposed again unchanged until
	/// applied state records a promotion or a refusal is definitive.
	intent: std::sync::Mutex<Option<ForwardedDecision>>,
	/// ADR 0106 (wendelon): the index was handed over; it is never opened here
	/// again.
	handed_over: std::sync::atomic::AtomicBool,
	/// ADR 0108 (wendelon): set once the activation is applied here.
	activated: tokio::sync::watch::Sender<bool>,
	/// ADR 0106 (wendelon): the configuration and decision secret the recovery
	/// uses, and its restricted storage once opened.
	cfg: NodeBootstrapConfig,
	secret: [u8; 32],
	recovery_slab: std::sync::Mutex<Option<Arc<SlabManager>>>,
	/// Promoted and bound (gate 2 done here).
	prepared: std::sync::atomic::AtomicBool,
	/// ADR 0106 (wendelon): a cooperative stop, taken between steps.
	stopping: std::sync::atomic::AtomicBool,
	stop: tokio::sync::Notify,
	/// Test-only: the proposals this task sent.
	#[cfg(test)]
	sent: std::sync::atomic::AtomicU64,
}

/// Test-only: a stage preparation stops before, by data directory, so it holds
/// from the node's first step, across restarts.
#[cfg(test)]
static HALTS: std::sync::Mutex<std::collections::BTreeMap<PathBuf, Stage>> =
	std::sync::Mutex::new(std::collections::BTreeMap::new());

/// ADR 0108 (wendelon) test seam (Codex 01a1e41e R4): the preparation task of
/// these data directories panics right after it signals its activation.
#[cfg(test)]
pub(crate) static PANIC_AFTER_ACTIVATION: std::sync::Mutex<std::collections::BTreeSet<PathBuf>> =
	std::sync::Mutex::new(std::collections::BTreeSet::new());

/// ADR 0108 (wendelon) test seam: one-shot, by data directory; holds the
/// activation, built from its view, before it is forwarded.
#[cfg(test)]
pub(crate) static PAUSE_BEFORE_ACTIVATION: std::sync::Mutex<
	std::collections::BTreeMap<PathBuf, (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

/// Test-only (ADR 0107): how many more of a node's report answers are lost
/// after the report was sent, by data directory, as a dropped reply would.
/// Test-only: why a store's recovery stopped, by data directory (the error its
/// final step returned).
#[cfg(test)]
pub(crate) static STOP_REASONS: std::sync::Mutex<std::collections::BTreeMap<PathBuf, String>> =
	std::sync::Mutex::new(std::collections::BTreeMap::new());

#[cfg(test)]
pub(crate) static LOSE_REPORT_ANSWERS: std::sync::Mutex<std::collections::BTreeMap<PathBuf, u32>> =
	std::sync::Mutex::new(std::collections::BTreeMap::new());

#[cfg(test)]
pub(crate) fn halt_for_test(data_dir: &std::path::Path, stage: Option<Stage>) {
	let mut halts = HALTS.lock().unwrap();
	match stage {
		Some(stage) => halts.insert(data_dir.to_path_buf(), stage),
		None => halts.remove(data_dir),
	};
}

impl Preparation {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		slot: u32,
		instance: u32,
		mode: DecisionMode,
		raft: WendelonRaft,
		state_machine: WendelonStateMachine,
		provider: Arc<dyn TransportProvider>,
		pool: Arc<BufPool>,
		owner: DataDirectoryOwner,
		data_dir: PathBuf,
		key: [u8; 32],
		cfg: NodeBootstrapConfig,
		secret: [u8; 32],
	) -> Self {
		Self {
			cfg,
			secret,
			recovery_slab: std::sync::Mutex::new(None),
			prepared: std::sync::atomic::AtomicBool::new(false),
			slot,
			instance,
			mode,
			raft,
			state_machine,
			provider,
			pool,
			owner,
			data_dir,
			key,
			store: tokio::sync::Mutex::new(None),
			intent: std::sync::Mutex::new(None),
			handed_over: std::sync::atomic::AtomicBool::new(false),
			activated: tokio::sync::watch::channel(false).0,
			stopping: std::sync::atomic::AtomicBool::new(false),
			stop: tokio::sync::Notify::new(),
			#[cfg(test)]
			sent: std::sync::atomic::AtomicU64::new(0),
		}
	}

	#[cfg(test)]
	pub(crate) fn sent_for_test(&self) -> u64 {
		self.sent.load(std::sync::atomic::Ordering::SeqCst)
	}

	/// Runs until preparation is finished or fails closed. A failure stops
	/// preparation only; the member keeps its Raft obligations.
	pub(crate) async fn run(self: Arc<Self>) {
		loop {
			// ADR 0106 (wendelon): a stop is taken between steps, never inside
			// one, so an owner-bound open always completes into `store` (an
			// aborted open would leave its database held by the owner alone).
			if self.stopping.load(std::sync::atomic::Ordering::SeqCst) {
				return;
			}
			match self.step().await {
				Ok(true) => {
					tracing::info!(
						slot = self.slot,
						instance = self.instance,
						"replacement prepared: its store is current and bound; it stays control-only"
					);
					self.prepared
						.store(true, std::sync::atomic::Ordering::SeqCst);
					// ADR 0106 (wendelon): the same task recovers the slot's data.
					self.recover().await;
					return;
				}
				Ok(false) => {}
				Err(error) => {
					self.note_stop(format!("preparation: {error}"));
					tracing::error!(
						slot = self.slot,
						instance = self.instance,
						%error,
						"replacement preparation stopped"
					);
					return;
				}
			}
			tokio::select! {
				_ = tokio::time::sleep(TICK) => {}
				_ = self.stop.notified() => {}
			}
		}
	}

	/// Test-only: records why this task stopped ([`STOP_REASONS`]).
	#[allow(unused_variables)]
	fn note_stop(&self, why: String) {
		#[cfg(test)]
		STOP_REASONS
			.lock()
			.unwrap()
			.insert(self.data_dir.clone(), why);
	}

	/// ADR 0108 (wendelon): becomes true once this store's activation is in
	/// its own applied state and the task has ended.
	pub(crate) fn activated(&self) -> tokio::sync::watch::Receiver<bool> {
		self.activated.subscribe()
	}

	/// Whether the store is promoted and bound (gate 2 done by this task).
	pub(crate) fn prepared(&self) -> bool {
		self.prepared.load(std::sync::atomic::Ordering::SeqCst)
	}

	/// ADR 0106 (wendelon): the recovery's restricted storage, once opened.
	pub(crate) fn recovery_slab(&self) -> Option<Arc<SlabManager>> {
		self.recovery_slab.lock().unwrap().clone()
	}

	/// ADR 0106 (wendelon): hands the index over and opens the restricted
	/// storage (retried while the cluster's root is not enrolled here), then
	/// recovers: base, owed children, validation, the saved statement and its
	/// record, then the delta. Every step rechecks that this store is still its
	/// slot's current replacement; a definitive refusal ends it.
	async fn recover(&self) {
		let slab = loop {
			if self.stopping.load(std::sync::atomic::Ordering::SeqCst) {
				return;
			}
			match self.open_recovery().await {
				Ok(slab) => break slab,
				Err(error) if error.to_string().contains("RootKekNotEnrolled") => {
					tracing::warn!(slot = self.slot, %error, "recovery waits for the cluster's root");
				}
				Err(error) => {
					self.note_stop(format!("storage did not open: {error}"));
					tracing::error!(slot = self.slot, %error, "recovery stopped: its storage did not open");
					return;
				}
			}
			self.pause().await;
		};
		let driver = super::replacement_recovery::RecoveryDriver {
			clock: super::node_clock(&self.cfg),
			slot: self.slot,
			instance: self.instance,
			state_machine: self.state_machine.clone(),
			raft: self.raft.clone(),
			provider: self.provider.clone(),
			pool: self.pool.clone(),
			slab,
			locks: Arc::new(crate::network::data_server::child_writes::ParentLocks::new()),
			key: ed25519_dalek::SigningKey::from_bytes(&self.secret),
			progress: Default::default(),
			retention: self.cfg.mvcc_retention_policy,
			retention_window_ms: self.cfg.parent_decisions_retention_window_ms(),
		};
		#[cfg(test)]
		crate::network::data_server::child_writes::registry_probe::note(
			driver.slab.data_dir(),
			crate::network::data_server::child_writes::registry_probe::Role::RecoveryDriver,
			&driver.locks,
		);
		loop {
			if self.stopping.load(std::sync::atomic::Ordering::SeqCst) {
				return;
			}
			#[cfg(test)]
			let halted = HALTS.lock().unwrap().get(&self.data_dir).copied() == Some(Stage::Recover);
			#[cfg(not(test))]
			let halted = false;
			if !halted {
				use super::replacement_recovery::Progress;
				match driver.step().await {
					Ok(Progress::Ready(request)) => {
						#[cfg(test)]
						if matches!(
							*request,
							crate::network::decision_proposals::ForwardedDecision::ActivateReplacement { .. }
						) {
							let paused = PAUSE_BEFORE_ACTIVATION
								.lock()
								.unwrap()
								.remove(&self.data_dir);
							if let Some((reached, release)) = paused {
								reached.notify_one();
								release.notified().await;
							}
						}
						if let Disposition::Stop(why) = disposition(&self.forward(&request).await) {
							self.note_stop(format!("statement refused: {why}"));
							tracing::error!(slot = self.slot, %why, "recovery stopped: its statement was refused");
							return;
						}
					}
					Ok(Progress::Pending(why)) => {
						tracing::debug!(slot = self.slot, %why, "recovery pending");
					}
					Ok(Progress::Recorded) => {}
					// ADR 0108 (wendelon): the activation is applied here; this
					// task ends and the member hands over to a Full one.
					Ok(Progress::Activated) => {
						tracing::info!(
							slot = self.slot,
							instance = self.instance,
							"replacement activated: handing over to a full member"
						);
						// `send_replace` keeps the value for a receiver that
						// subscribes later; `send` drops it when none is
						// subscribed yet.
						self.activated.send_replace(true);
						#[cfg(test)]
						if PANIC_AFTER_ACTIVATION
							.lock()
							.unwrap()
							.contains(&self.data_dir)
						{
							panic!("test: the preparation task fails after its activation");
						}
						return;
					}
					// ADR 0107 (wendelon): a report's answer never stops
					// recovery. Applied state decides on the next step: a
					// recorded report is skipped, a superseded target is
					// replaced, and only lost authority stops (checked every
					// step).
					Ok(Progress::Report(request)) => {
						let answer = self.forward(&request).await;
						#[cfg(test)]
						let answer = {
							let mut lose = LOSE_REPORT_ANSWERS.lock().unwrap();
							match lose.get_mut(&self.data_dir) {
								Some(left) if *left > 0 => {
									*left -= 1;
									Err(Error::other("test: the report's answer is lost"))
								}
								_ => answer,
							}
						};
						tracing::debug!(slot = self.slot, ?answer, "report forwarded");
					}
					Err(error) => {
						self.note_stop(error.to_string());
						tracing::error!(slot = self.slot, %error, "recovery stopped");
						return;
					}
				}
			}
			self.pause().await;
		}
	}

	async fn open_recovery(&self) -> std::io::Result<Arc<SlabManager>> {
		if let Some(slab) = self.recovery_slab() {
			return Ok(slab);
		}
		self.release().await?;
		let slab =
			super::replacement_recovery::open_recovery_storage(&self.cfg, &self.owner).await?;
		*self.recovery_slab.lock().unwrap() = Some(slab.clone());
		Ok(slab)
	}

	async fn pause(&self) {
		tokio::select! {
			_ = tokio::time::sleep(TICK) => {}
			_ = self.stop.notified() => {}
		}
	}

	/// ADR 0106 (wendelon): asks the task to end after its current step.
	pub(crate) fn request_stop(&self) {
		self.stopping
			.store(true, std::sync::atomic::Ordering::SeqCst);
		self.stop.notify_one();
	}

	/// One step: `Ok(true)` once nothing remains.
	async fn step(&self) -> std::io::Result<bool> {
		let request = match self.next().await? {
			Next::Wait => return Ok(false),
			Next::Finished => return Ok(true),
			Next::Promote(request) | Next::Bind(request) => request,
		};
		#[cfg(test)]
		{
			let halted = HALTS.lock().unwrap().get(&self.data_dir).copied();
			let stage = match request {
				ForwardedDecision::PromoteIncarnation { .. } => Stage::Promote,
				_ => Stage::Bind,
			};
			if halted == Some(stage) {
				return Ok(false);
			}
		}
		// A success is not trusted either: the next step reads applied state
		// again. Only a definitive refusal ends preparation.
		match disposition(&self.forward(&request).await) {
			Disposition::Again => {}
			Disposition::Stop(why) => {
				return Err(Error::new(
					ErrorKind::PermissionDenied,
					format!("a preparation proposal was refused for good: {why}"),
				));
			}
		}
		Ok(false)
	}

	async fn next(&self) -> std::io::Result<Next> {
		let instance = self.instance;
		let slot = self.slot;
		let found = self
			.state_machine
			.read_state(move |state| {
				let decisions = state.decisions.as_ref()?;
				let instances = decisions.instances();
				let (token, replacement) =
					instances.replacements().iter().find(|(_, record)| {
						record.slot == slot
							&& record.phase == ReplacementPhase::Done { r_new: instance }
					})?;
				let birth: Birth = instances.record(instance)?.birth;
				let current = state
					.incarnations
					.get(&slot)
					.and_then(|registry| registry.current);
				Some((*token, birth, replacement.promoted, current))
			})
			.await;
		let Some((token, birth, promoted, current)) = found else {
			return Ok(Next::Wait);
		};
		let incarnation = self.store_incarnation(promoted.is_none()).await?;
		let Some(promoted) = promoted else {
			let Some(expected) = current else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("slot {slot} has no current incarnation to supersede"),
				));
			};
			let mut intent = self.intent.lock().unwrap();
			let request = intent.get_or_insert(ForwardedDecision::PromoteIncarnation {
				token,
				r_new: instance,
				birth,
				expected,
				incarnation,
			});
			return Ok(Next::Promote(request.clone()));
		};
		if promoted.incarnation != incarnation {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ReplacementStoreNotPromoted: this store is {incarnation:?}, the promoted one {:?}",
					promoted.incarnation
				),
			));
		}
		if current != Some(incarnation) {
			// A later replacement superseded this store.
			return Ok(Next::Finished);
		}
		let signer = crate::consensus::decisions::Signer {
			node: slot,
			incarnation,
		};
		let bound = self
			.state_machine
			.read_state(move |state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.signer_key(signer))
			})
			.await;
		Ok(match bound {
			None => Next::Bind(ForwardedDecision::BindSigner {
				incarnation,
				key: self.key,
			}),
			Some(key) if key == self.key => Next::Finished,
			Some(_) => {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!(
						"decision signing key refused: slot {slot} is configured with a key other than the one bound to its store"
					),
				));
			}
		})
	}

	/// This member's data incarnation J, read from its store's own index, never
	/// from its Raft store's birth or its directory owner. The index is opened
	/// once (a damaged one refuses, as at every open) and held; no slab manager
	/// is built. Only with `may_create` (no promotion recorded for this
	/// replacement), and only in a directory holding nothing but first-bootstrap
	/// artifacts and an enrolled root, is a new index created, its incarnation
	/// minted in the transaction that writes it. Otherwise a missing index is a
	/// lost store: refused, never minted again.
	pub(crate) async fn store_incarnation(
		&self,
		may_create: bool,
	) -> std::io::Result<StoreIncarnation> {
		let mut store = self.store.lock().await;
		if let Some(index) = store.as_ref() {
			return Ok(index.store_incarnation());
		}
		if self.handed_over.load(std::sync::atomic::Ordering::SeqCst) {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"ReplacementStoreHandedOver: the store's index belongs to its recovery now",
			));
		}
		let path = crate::storage::index::persistent_index_path(&self.data_dir);
		let index = if path.exists() {
			PersistentLocalIndex::open_with_owner(path, Some(self.owner.clone())).await?
		} else if !may_create {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"ReplacementStoreLost: no storage index in {}",
					self.data_dir.display()
				),
			));
		} else {
			for entry in std::fs::read_dir(&self.data_dir)? {
				let entry = entry?;
				if !is_control_only_artifact(&entry.path())? {
					return Err(Error::new(
						ErrorKind::AlreadyExists,
						format!(
							"ReplacementStoreUnexplained: {} holds {} and no storage index",
							self.data_dir.display(),
							entry.file_name().to_string_lossy()
						),
					));
				}
			}
			PersistentLocalIndex::initialize_new_with_high_water(path, Some(self.owner.clone()), 0)
				.await?
		};
		let incarnation = index.store_incarnation();
		*store = Some(index);
		Ok(incarnation)
	}

	/// Releases the store index before the owner closes.
	/// ADR 0106 (wendelon): hands the store's index over. No later step opens
	/// it again here. It is closed through its owner's closer and forgotten
	/// only once that close succeeded; a failed or cancelled close keeps it
	/// held and protected (and a begun drop is joined by the owner's close).
	pub(crate) async fn release(&self) -> std::io::Result<()> {
		let mut store = self.store.lock().await;
		self.handed_over
			.store(true, std::sync::atomic::Ordering::SeqCst);
		if let Some(index) = store.as_ref() {
			index.close().await?;
		}
		*store = None;
		Ok(())
	}

	/// One proposal to the current leader, at its address in the applied
	/// membership, within the decision exchange's deadline.
	async fn forward(&self, request: &ForwardedDecision) -> std::io::Result<ProposalResponse> {
		#[cfg(test)]
		self.sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		let metrics = self.raft.metrics().borrow().clone();
		let Some(leader) = metrics.current_leader else {
			return Ok(ProposalResponse::NotLeader { leader_hint: None });
		};
		let address = metrics
			.membership_config
			.membership()
			.get_node(&leader)
			.map(|node| node.addr.clone())
			.ok_or_else(|| Error::other(format!("leader {leader} has no address")))?;
		let slot = crate::consensus::decisions::instances::leader_slot(
			&self.raft,
			&self.state_machine,
			self.slot,
		)
		.await
		.ok_or_else(|| Error::other("no leader slot"))?;
		let meta = postcard::to_stdvec(&ProposeRequest {
			// The leader records this as this voter's clock sample: read from the node's
			// clock, as a Full node's proposals are (DST stage 1; `SystemClock` in production,
			// which also reads 0 before the epoch).
			sent_ms: super::node_clock(&self.cfg).now_ms(),
			request: request.clone(),
		})
		.map_err(Error::other)?;
		let version = self.mode.raft_frame_version();
		let exchange = async {
			let connection = self
				.provider
				.connect(
					NodeId {
						id: slot,
						_reserved: 0,
					},
					&address,
				)
				.await?;
			let mut stream = connection.open_bi().await?;
			let header = FrameHeader {
				magic: HRFT_MAGIC,
				version,
				msg_type: MsgType::DECISION_PROPOSE_REQ.0,
				flags: 0,
				stream_id: stream.stream_id() as u32,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			};
			write_frame(&mut *stream, &header, &meta, None, None).await?;
			stream.finish().await?;
			let frame = read_frame(&mut *stream, &self.pool).await?;
			check_response_frame(&frame.header, MsgType::DECISION_PROPOSE_RESP, version)?;
			decode_bounded(frame.meta.as_ref())
		};
		tokio::time::timeout(DECISION_RPC_DEADLINE, exchange)
			.await
			.map_err(|_| Error::new(ErrorKind::TimedOut, "preparation proposal unanswered"))?
	}
}

/// What may exist in a control-only member's directory besides its own data
/// store: exactly what a first bootstrap leaves before its storage index
/// (`startup::interrupted_bootstrap_artifact`), and an enrolled root.
fn is_control_only_artifact(path: &std::path::Path) -> std::io::Result<bool> {
	if crate::crypto::is_root_kek_artifact_dir(path)? {
		return Ok(true);
	}
	let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
		return Ok(false);
	};
	Ok(std::fs::symlink_metadata(path)?.is_file()
		&& super::startup::interrupted_bootstrap_artifact(name))
}

/// What one answer means for preparation.
#[derive(Debug, PartialEq, Eq)]
enum Disposition {
	/// Read applied state and propose the same intent again: a success yet to
	/// be applied here, a wait (lineages still held, a view behind the
	/// leader's), no leader, an unknown outcome, or an unhealthy leader clock.
	Again,
	/// A refusal no retry of the same intent can change.
	Stop(String),
}

fn disposition(answer: &std::io::Result<ProposalResponse>) -> Disposition {
	use crate::consensus::SlabRefusal;
	use crate::consensus::decisions::DecisionResult;
	use crate::network::decision_proposals::{ProposalOutcome, ProposalRefusal};
	let answer = match answer {
		Err(_) => return Disposition::Again,
		Ok(answer) => answer,
	};
	match answer {
		ProposalResponse::NotLeader { .. } | ProposalResponse::Indeterminate(_) => {
			Disposition::Again
		}
		ProposalResponse::Refused(ProposalRefusal::ClockUnhealthy) => Disposition::Again,
		ProposalResponse::Refused(refusal) => Disposition::Stop(format!("{refusal:?}")),
		ProposalResponse::Applied { outcome, .. } => match outcome {
			ProposalOutcome::Promotion { refused: None } => Disposition::Again,
			ProposalOutcome::Promotion {
				refused:
					Some(SlabRefusal::LineagesStillHeld { .. } | SlabRefusal::StaleSupersede { .. }),
			} => Disposition::Again,
			ProposalOutcome::Promotion {
				refused: Some(refusal),
			} => Disposition::Stop(format!("{refusal:?}")),
			ProposalOutcome::Decision(DecisionResult::SignerBound) => Disposition::Again,
			ProposalOutcome::Decision(DecisionResult::RecoveryRecorded { .. }) => {
				Disposition::Again
			}
			ProposalOutcome::Decision(DecisionResult::ActivationRecorded { .. }) => {
				Disposition::Again
			}
			ProposalOutcome::Decision(DecisionResult::Refused(refusal)) => {
				Disposition::Stop(format!("{refusal:?}"))
			}
			other => Disposition::Stop(format!("unexpected answer {other:?}")),
		},
		other => Disposition::Stop(format!("unexpected answer {other:?}")),
	}
}

#[cfg(test)]
mod tests {
	//! Which answers end preparation: a wait or an unknown outcome is proposed
	//! again; a definitive refusal is not.

	use super::*;
	use crate::consensus::SlabRefusal;
	use crate::consensus::decisions::{DecisionRefusal, DecisionResult};
	use crate::network::decision_proposals::{ProposalOutcome, ProposalRefusal};

	fn applied(outcome: ProposalOutcome) -> std::io::Result<ProposalResponse> {
		Ok(ProposalResponse::Applied {
			outcome,
			log_index: 1,
		})
	}

	fn promotion(refused: Option<SlabRefusal>) -> std::io::Result<ProposalResponse> {
		applied(ProposalOutcome::Promotion { refused })
	}

	#[test]
	fn waits_and_unknowns_are_proposed_again_and_definitive_refusals_end_preparation() {
		for again in [
			Err(Error::new(ErrorKind::TimedOut, "unanswered")),
			Ok(ProposalResponse::NotLeader { leader_hint: None }),
			Ok(ProposalResponse::Indeterminate("lost".into())),
			Ok(ProposalResponse::Refused(ProposalRefusal::ClockUnhealthy)),
			promotion(None),
			promotion(Some(SlabRefusal::LineagesStillHeld { count: 1 })),
			promotion(Some(SlabRefusal::StaleSupersede {
				node: 3,
				current: None,
			})),
			applied(ProposalOutcome::Decision(DecisionResult::SignerBound)),
		] {
			assert_eq!(disposition(&again), Disposition::Again, "{again:?}");
		}
		for stop in [
			promotion(Some(SlabRefusal::PromotionStale)),
			promotion(Some(SlabRefusal::PromotionConflict)),
			promotion(Some(SlabRefusal::PromotionNotAuthorized)),
			promotion(Some(SlabRefusal::ReplacementNotDone)),
			promotion(Some(SlabRefusal::RetiredIncarnation {
				node: 3,
				incarnation: StoreIncarnation::from_raw(1),
			})),
			applied(ProposalOutcome::Decision(DecisionResult::Refused(
				DecisionRefusal::SignerRebind,
			))),
			applied(ProposalOutcome::Incarnation { accepted: false }),
			Ok(ProposalResponse::Refused(
				ProposalRefusal::SignerKeyDisagrees,
			)),
			Ok(ProposalResponse::Refused(ProposalRefusal::ModeToday)),
		] {
			assert!(
				matches!(disposition(&stop), Disposition::Stop(_)),
				"{stop:?}"
			);
		}
	}
}
