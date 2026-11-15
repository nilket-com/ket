//! ADR 0104 (wendelon): a store's Raft identity, decided before any Raft
//! object or listener exists for it.
//!
//! A ParentDecisions store holds its identity in its vote page. A fresh store
//! gets one only through an explicit path: formation consumes a provisioned
//! formation intent; an ordinary join and a replacement each ask the leader
//! for a grant bound to this store's birth. A store that lost its Raft state
//! is born again and can never take back an identity issued to the old one.

use super::*;
use crate::consensus::decision_mode::DecisionMode;
use crate::consensus::decisions::instances::{GrantKind, Origin, RaftIdentity, Role, is_pd_slot};
use crate::network::decision_proposals::{InstanceGrantRequest, InstanceGrantResponse};

/// Provisioned beside a forming node's first store; consumed by the identity
/// write, before the node can serve any Raft request.
pub const FORMATION_INTENT: &str = "FORMATION_INTENT";
/// Provisioned beside a replacement's fresh store: the replacement's token, in
/// hexadecimal.
pub const REPLACEMENT_TOKEN: &str = "REPLACEMENT_TOKEN";

/// How long a fresh store asks for its grant before failing closed.
const GRANT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);
/// How long one ask (its dial, request and answer) may wait before the next
/// node is asked, within [`GRANT_DEADLINE`].
const GRANT_ATTEMPT_BOUND: std::time::Duration = std::time::Duration::from_secs(3);

fn sync_dir(dir: &std::path::Path) -> std::io::Result<()> {
	std::fs::File::open(dir)?.sync_all()
}

fn write_durably(dir: &std::path::Path, name: &str, body: &[u8]) -> std::io::Result<()> {
	std::fs::create_dir_all(dir)?;
	let path = dir.join(name);
	let mut file = std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(&path)?;
	std::io::Write::write_all(&mut file, body)?;
	file.sync_all()?;
	sync_dir(dir)
}

/// Provisions the formation intent: the one permission for a fresh store to
/// form a new ParentDecisions cluster.
pub fn provision_formation_intent(data_dir: &std::path::Path) -> std::io::Result<()> {
	write_durably(data_dir, FORMATION_INTENT, b"")
}

/// Provisions a replacement's token beside its fresh store.
pub fn provision_replacement_token(data_dir: &std::path::Path, token: u128) -> std::io::Result<()> {
	write_durably(
		data_dir,
		REPLACEMENT_TOKEN,
		format!("{token:032x}\n").as_bytes(),
	)
}

/// The provisioned replacement token, if any.
pub(crate) fn replacement_token(data_dir: &std::path::Path) -> std::io::Result<Option<u128>> {
	match std::fs::read_to_string(data_dir.join(REPLACEMENT_TOKEN)) {
		Ok(text) => u128::from_str_radix(text.trim(), 16)
			.map(Some)
			.map_err(|error| {
				Error::new(
					ErrorKind::InvalidData,
					format!("{REPLACEMENT_TOKEN} is not a replacement token: {error}"),
				)
			}),
		Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
		Err(error) => Err(error),
	}
}

fn consume(data_dir: &std::path::Path, name: &str) -> std::io::Result<()> {
	match std::fs::remove_file(data_dir.join(name)) {
		Ok(()) => sync_dir(data_dir),
		Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
		Err(error) => Err(error),
	}
}

/// What a process on this data directory must start as, read before any
/// store is opened for writing: control-only when its store says so, or when
/// a replacement token waits for a fresh store.
pub(crate) fn starts_control_only(cfg: &NodeBootstrapConfig) -> std::io::Result<bool> {
	if cfg.decision_mode != DecisionMode::ParentDecisions {
		return Ok(false);
	}
	let (_, vote_path, _) = control_artifact_paths(&cfg.data_dir);
	if let Some(identity) = crate::consensus::raft_storage::peek_identity(&vote_path)? {
		return Ok(identity.role == Role::ControlOnly);
	}
	Ok(replacement_token(&cfg.data_dir)?.is_some())
}

/// The process's Raft identity. Today's is its node id and nothing is written.
/// A ParentDecisions store returns its recorded identity, or obtains and
/// durably writes its first one; `control_only` says which entry is asking,
/// and an identity of the other role is refused.
pub(crate) async fn establish_identity(
	cfg: &NodeBootstrapConfig,
	log_store: &WendelonLogStore,
	provider: &Arc<dyn TransportProvider>,
	pool: &Arc<BufPool>,
	control_only: bool,
) -> std::io::Result<RaftIdentity> {
	if cfg.decision_mode != DecisionMode::ParentDecisions {
		return Ok(RaftIdentity {
			instance: cfg.node_id,
			origin: match cfg.cluster {
				ClusterBootstrapConfig::Bootstrap => Origin::Formation,
				ClusterBootstrapConfig::Join { .. } => Origin::Join,
			},
			role: Role::Full,
		});
	}
	if !is_pd_slot(cfg.node_id) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"node id {} is outside the ParentDecisions slot range 1..2^31",
				cfg.node_id
			),
		));
	}
	let identity = match log_store.identity().await {
		Some(identity) => identity,
		None => first_identity(cfg, log_store, provider, pool, control_only).await?,
	};
	let expected = if control_only {
		Role::ControlOnly
	} else {
		Role::Full
	};
	if identity.role != expected {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			format!(
				"RaftRoleMismatch: this store's identity is {identity:?}; it starts only as {:?}",
				identity.role
			),
		));
	}
	match identity.origin {
		Origin::Formation | Origin::Join if identity.instance != cfg.node_id => {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"this store's identity {identity:?} is not the configured node {}",
					cfg.node_id
				),
			));
		}
		// A leftover intent is consumed: the page is the authority now.
		Origin::Formation => consume(&cfg.data_dir, FORMATION_INTENT)?,
		Origin::Join => {}
		Origin::Replacement => consume(&cfg.data_dir, REPLACEMENT_TOKEN)?,
	}
	Ok(identity)
}

async fn first_identity(
	cfg: &NodeBootstrapConfig,
	log_store: &WendelonLogStore,
	provider: &Arc<dyn TransportProvider>,
	pool: &Arc<BufPool>,
	control_only: bool,
) -> std::io::Result<RaftIdentity> {
	let birth = log_store.birth().await.ok_or_else(|| {
		Error::other("a ParentDecisions store without a birth cannot be granted an identity")
	})?;
	let token = replacement_token(&cfg.data_dir)?;
	let identity = match (token, control_only, &cfg.cluster) {
		(Some(token), true, ClusterBootstrapConfig::Join { seeds }) => {
			let instance = request_grant(
				provider,
				pool,
				seeds,
				InstanceGrantRequest {
					birth,
					slot: cfg.node_id,
					kind: GrantKind::Replacement { token },
				},
			)
			.await?;
			RaftIdentity {
				instance,
				origin: Origin::Replacement,
				role: Role::ControlOnly,
			}
		}
		(Some(_), true, ClusterBootstrapConfig::Bootstrap) => {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"a replacement joins an existing cluster: configure cluster.join with its seeds",
			));
		}
		(Some(_), false, _) => {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"a replacement token waits here: this store starts only control-only",
			));
		}
		(None, true, _) => {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"a control-only start needs a replacement token or a control-only identity",
			));
		}
		(None, false, ClusterBootstrapConfig::Bootstrap) => {
			if !cfg.data_dir.join(FORMATION_INTENT).exists() {
				return Err(Error::new(
					ErrorKind::PermissionDenied,
					format!(
						"FormationIntentMissing: a fresh store forms a cluster only with a provisioned {FORMATION_INTENT}; a member that lost its store is replaced, not re-formed"
					),
				));
			}
			RaftIdentity {
				instance: cfg.node_id,
				origin: Origin::Formation,
				role: Role::Full,
			}
		}
		(None, false, ClusterBootstrapConfig::Join { seeds }) => {
			let instance = request_grant(
				provider,
				pool,
				seeds,
				InstanceGrantRequest {
					birth,
					slot: cfg.node_id,
					kind: GrantKind::Join,
				},
			)
			.await?;
			RaftIdentity {
				instance,
				origin: Origin::Join,
				role: Role::Full,
			}
		}
	};
	log_store.write_identity(identity).await?;
	Ok(identity)
}

/// Asks the seeds, then the leader they name, for this store's grant, until
/// [`GRANT_DEADLINE`]. Only a grant or a refusal ends it early.
async fn request_grant(
	provider: &Arc<dyn TransportProvider>,
	pool: &Arc<BufPool>,
	seeds: &[SeedNodeConfig],
	request: InstanceGrantRequest,
) -> std::io::Result<u32> {
	request_grant_within(
		provider,
		pool,
		seeds,
		&request,
		GRANT_DEADLINE,
		GRANT_ATTEMPT_BOUND,
	)
	.await
}

/// `request_grant` with its bounds. One absolute deadline, `bound` from the
/// call, covers every attempt with its dial, request and answer; each attempt
/// is also cut at `attempt_bound`, so one silent node does not keep the others
/// from being asked. Any answer, a grant included, that completes after the
/// deadline is not taken. Running out of time is an unknown outcome, never a
/// refusal: the leader may have recorded the grant. The store keeps its birth,
/// and its next start asks again under it, which the leader answers with the
/// instance already granted to that birth.
async fn request_grant_within(
	provider: &Arc<dyn TransportProvider>,
	pool: &Arc<BufPool>,
	seeds: &[SeedNodeConfig],
	request: &InstanceGrantRequest,
	bound: std::time::Duration,
	attempt_bound: std::time::Duration,
) -> std::io::Result<u32> {
	use tokio::time::Instant;
	let deadline = Instant::now() + bound;
	let unknown = |last: Option<Error>| {
		Error::new(
			ErrorKind::TimedOut,
			format!(
				"InstanceGrantUnknown: no grant within {bound:?}{}; the grant's outcome is unknown, \
				 and the next start asks again under this store's birth",
				last.map(|error| format!(" (last: {error})"))
					.unwrap_or_default()
			),
		)
	};
	let mut last: Option<Error> = None;
	loop {
		let mut targets: Vec<(u32, String)> = seeds
			.iter()
			.map(|seed| (seed.node_id, seed.address.clone()))
			.collect();
		let mut asked = 0;
		while asked < targets.len() && asked < 8 {
			let (slot, address) = targets[asked].clone();
			asked += 1;
			let now = Instant::now();
			if now >= deadline {
				return Err(unknown(last));
			}
			let attempt = tokio::time::timeout_at(
				deadline.min(now + attempt_bound),
				send_grant(provider, pool, slot, &address, request),
			)
			.await;
			if Instant::now() >= deadline {
				return Err(unknown(last));
			}
			let answer = match attempt {
				Ok(answer) => answer,
				Err(_) => {
					last = Some(Error::new(
						ErrorKind::TimedOut,
						format!("node {slot} did not answer within {attempt_bound:?}"),
					));
					continue;
				}
			};
			match answer {
				Ok(InstanceGrantResponse::Granted { instance }) => return Ok(instance),
				Ok(InstanceGrantResponse::Refused(refusal)) => {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						format!("InstanceGrantRefused: {refusal:?}"),
					));
				}
				Ok(InstanceGrantResponse::Unauthorized) => {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"InstanceGrantRefused: the leader does not authenticate this node as its slot",
					));
				}
				Ok(InstanceGrantResponse::ModeToday) => {
					return Err(Error::new(
						ErrorKind::Unsupported,
						"InstanceGrantRefused: the cluster does not run ParentDecisions",
					));
				}
				Ok(InstanceGrantResponse::NotLeader {
					leader: Some(leader),
				}) => {
					if !targets.contains(&leader) {
						targets.push(leader);
					}
					last = Some(Error::new(ErrorKind::NotConnected, "asked a non-leader"));
				}
				Ok(InstanceGrantResponse::NotLeader { leader: None }) => {
					last = Some(Error::new(ErrorKind::NotConnected, "no leader is known"));
				}
				Ok(InstanceGrantResponse::Indeterminate(message)) => {
					last = Some(Error::other(format!("grant indeterminate: {message}")));
				}
				Err(error) => last = Some(error),
			}
		}
		let now = Instant::now();
		if now >= deadline {
			return Err(unknown(last));
		}
		tokio::time::sleep_until(deadline.min(now + std::time::Duration::from_millis(100))).await;
	}
}

async fn send_grant(
	provider: &Arc<dyn TransportProvider>,
	pool: &Arc<BufPool>,
	slot: u32,
	address: &str,
	request: &InstanceGrantRequest,
) -> std::io::Result<InstanceGrantResponse> {
	let target = NodeId {
		id: slot,
		_reserved: 0,
	};
	let conn = provider.connect(target, address).await?;
	let mut stream = conn.open_bi().await?;
	let meta = wire::encode(request)?;
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: crate::schema::HRFT_ENVELOPE_VERSION,
		msg_type: MsgType::INSTANCE_GRANT_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: meta.len() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &header, &meta, None, None).await?;
	stream.finish().await?;
	let response = read_frame(&mut *stream, pool).await?;
	if response.header.msg_type != MsgType::INSTANCE_GRANT_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"node {slot} answered a grant with msg_type=0x{:02x}",
				response.header.msg_type
			),
		));
	}
	crate::network::decision_proposals::decode_bounded(&response.meta)
}

#[cfg(test)]
mod tests {
	//! The grant client's bounds over its real dial, request and answer, on the
	//! simulated network: a node that is never reached, one that never answers,
	//! one that answers after the deadline, and a silent seed before one that
	//! answers.

	use super::*;
	use crate::network::core::HrftFrame;
	use crate::network::pool::PooledBuf;
	use crate::network::server::{HrftService, RequestContext};
	use crate::simulator::network::{
		InjectedSimulatedTransportFault, SimulatedTransportFaultAction, SimulatedTransportFaultOp,
		SimulatedTransportNetwork, SimulatedTransportProvider,
	};
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::time::{Duration, Instant};

	const JOINER: u32 = 3;
	const BOUND: Duration = Duration::from_millis(600);
	/// Generous: the call must end by its deadline, not within a tight window.
	const SLACK: Duration = Duration::from_millis(1500);

	fn node(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn address(id: u32) -> String {
		format!("grant-node-{id}:7001")
	}

	/// A node's answer to a grant, after `delay`, or none at all.
	struct Grantor {
		answer: Option<(Duration, InstanceGrantResponse)>,
		asked: AtomicUsize,
	}

	#[async_trait::async_trait]
	impl HrftService for Grantor {
		async fn handle_frame(
			&self,
			_context: RequestContext,
			frame: HrftFrame,
		) -> std::io::Result<Option<HrftFrame>> {
			assert_eq!(frame.header.msg_type, MsgType::INSTANCE_GRANT_REQ.0);
			self.asked.fetch_add(1, Ordering::SeqCst);
			let Some((delay, answer)) = self.answer.clone() else {
				std::future::pending::<()>().await;
				unreachable!();
			};
			tokio::time::sleep(delay).await;
			let meta = wire::encode(&answer)?;
			Ok(Some(HrftFrame {
				header: FrameHeader {
					magic: HRFT_MAGIC,
					version: crate::schema::HRFT_ENVELOPE_VERSION,
					msg_type: MsgType::INSTANCE_GRANT_RESP.0,
					flags: 0,
					stream_id: frame.header.stream_id,
					meta_len: meta.len() as u32,
					arrow_len: 0,
					trailing_len: 0,
				},
				meta: PooledBuf::orphan(meta),
				arrow_payload: None,
				trailing: None,
			}))
		}
	}

	fn grantor(answer: Option<(Duration, InstanceGrantResponse)>) -> Arc<Grantor> {
		Arc::new(Grantor {
			answer,
			asked: AtomicUsize::new(0),
		})
	}

	struct Fixture {
		network: Arc<SimulatedTransportNetwork>,
		provider: Arc<dyn TransportProvider>,
		pool: Arc<BufPool>,
	}

	fn fixture() -> Fixture {
		let network = Arc::new(SimulatedTransportNetwork::default());
		let pool = BufPool::new(64, 1024 * 1024);
		let provider: Arc<dyn TransportProvider> = Arc::new(SimulatedTransportProvider::new(
			node(JOINER),
			network.clone(),
			pool.clone(),
			TransportTier::QuicIoUring,
		));
		Fixture {
			network,
			provider,
			pool,
		}
	}

	fn seeds(ids: &[u32]) -> Vec<SeedNodeConfig> {
		ids.iter()
			.map(|id| SeedNodeConfig {
				node_id: *id,
				address: address(*id),
			})
			.collect()
	}

	fn request() -> InstanceGrantRequest {
		InstanceGrantRequest {
			birth: [7; 16],
			slot: JOINER,
			kind: GrantKind::Join,
		}
	}

	async fn ask(
		fixture: &Fixture,
		seed_ids: &[u32],
		attempt_bound: Duration,
	) -> (std::io::Result<u32>, Duration) {
		let started = Instant::now();
		let result = request_grant_within(
			&fixture.provider,
			&fixture.pool,
			&seeds(seed_ids),
			&request(),
			BOUND,
			attempt_bound,
		)
		.await;
		(result, started.elapsed())
	}

	fn assert_unknown(result: &std::io::Result<u32>, elapsed: Duration, what: &str) {
		let error = result.as_ref().expect_err(what);
		assert_eq!(error.kind(), ErrorKind::TimedOut, "{what}: {error}");
		assert!(
			error.to_string().contains("InstanceGrantUnknown"),
			"{what}: {error}"
		);
		assert!(
			elapsed >= BOUND && elapsed < BOUND + SLACK,
			"{what}: ended after {elapsed:?}, deadline {BOUND:?}"
		);
	}

	#[tokio::test]
	async fn a_dial_that_never_completes_ends_unknown_at_the_deadline() {
		let fixture = fixture();
		let seed = grantor(Some((
			Duration::ZERO,
			InstanceGrantResponse::Granted { instance: 3 },
		)));
		fixture.network.publish(node(1), &address(1), seed.clone());
		fixture.network.inject(
			InjectedSimulatedTransportFault::on(
				SimulatedTransportFaultOp::Connect,
				SimulatedTransportFaultAction::Delay(Duration::from_secs(3600)),
			)
			.to(node(1)),
		);
		let (result, elapsed) = ask(&fixture, &[1], Duration::from_secs(3600)).await;
		assert_unknown(&result, elapsed, "a dial that never completes");
		assert_eq!(seed.asked.load(Ordering::SeqCst), 0, "nothing was sent");
	}

	#[tokio::test]
	async fn a_request_never_answered_ends_unknown_at_the_deadline() {
		let fixture = fixture();
		let seed = grantor(None);
		fixture.network.publish(node(1), &address(1), seed.clone());
		let (result, elapsed) = ask(&fixture, &[1], Duration::from_secs(3600)).await;
		assert_unknown(&result, elapsed, "a request never answered");
		assert_eq!(seed.asked.load(Ordering::SeqCst), 1, "the request arrived");
	}

	#[tokio::test]
	async fn a_grant_answered_after_the_deadline_is_not_taken() {
		let fixture = fixture();
		let seed = grantor(Some((
			BOUND + Duration::from_millis(400),
			InstanceGrantResponse::Granted {
				instance: crate::consensus::decisions::instances::ALLOCATED_BASE,
			},
		)));
		fixture.network.publish(node(1), &address(1), seed.clone());
		let (result, elapsed) = ask(&fixture, &[1], Duration::from_secs(3600)).await;
		assert_unknown(&result, elapsed, "a grant answered late");
		assert_eq!(seed.asked.load(Ordering::SeqCst), 1);
	}

	#[tokio::test]
	async fn a_silent_seed_is_left_at_its_attempt_bound_and_the_next_one_grants() {
		let fixture = fixture();
		let silent = grantor(None);
		let answering = grantor(Some((
			Duration::ZERO,
			InstanceGrantResponse::Granted { instance: JOINER },
		)));
		fixture
			.network
			.publish(node(1), &address(1), silent.clone());
		fixture
			.network
			.publish(node(2), &address(2), answering.clone());
		let (result, elapsed) = ask(&fixture, &[1, 2], Duration::from_millis(150)).await;
		assert_eq!(result.unwrap(), JOINER);
		assert!(elapsed < BOUND, "granted after {elapsed:?}");
		assert_eq!(silent.asked.load(Ordering::SeqCst), 1);
		assert_eq!(answering.asked.load(Ordering::SeqCst), 1);
	}
}
