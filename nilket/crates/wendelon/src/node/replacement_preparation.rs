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
	/// Test-only: the proposals this task sent.
	#[cfg(test)]
	sent: std::sync::atomic::AtomicU64,
}

/// Test-only: a stage preparation stops before, by data directory, so it holds
/// from the node's first step, across restarts.
#[cfg(test)]
static HALTS: std::sync::Mutex<std::collections::BTreeMap<PathBuf, Stage>> =
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
	) -> Self {
		Self {
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
			match self.step().await {
				Ok(true) => {
					tracing::info!(
						slot = self.slot,
						instance = self.instance,
						"replacement prepared: its store is current and bound; it stays control-only"
					);
					return;
				}
				Ok(false) => {}
				Err(error) => {
					tracing::error!(
						slot = self.slot,
						instance = self.instance,
						%error,
						"replacement preparation stopped"
					);
					return;
				}
			}
			tokio::time::sleep(TICK).await;
		}
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
	pub(crate) async fn release(&self) {
		self.store.lock().await.take();
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
			sent_ms: std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.map(|now| now.as_millis() as u64)
				.unwrap_or(0),
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
