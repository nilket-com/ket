//! ADR 0729: the public client's table mutation call when a node it was hinted
//! to does not answer. Scripted nodes on the simulated network and a provider
//! whose connect to one node never completes, as a dial to a stopped QUIC peer
//! does not (the simulated network refuses such a dial at once, which is why
//! no cluster test shows this). No server code runs; nothing here is about
//! who may commit.

use super::*;
use crate::client::{
	ClusterPeerWire, ClusterStatusWire, ControlRingTopologySource, StaticTopologySource,
	TableWriteRequest, TopologySource, WendelonClient, WendelonClientConfig,
};
use crate::network::core::{HRFT_MAGIC, HrftFrame};
use crate::network::pool::PooledBuf;
use crate::network::server::{HrftService, RequestContext};
use crate::network::transport::{
	NodeRole, NodeRoleSet, TransportConnection, TransportProvider, TransportTierSet,
};
use crate::schema::{
	FrameHeader, MsgType, MutationStopReason, NodeId, TableWriteResponseMeta, WriteResponseMeta,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const TIER: TransportTier = TransportTier::QuicIoUring;

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

fn status_of(leader: u32) -> ClusterStatusWire {
	ClusterStatusWire {
		epoch: 1,
		leader: Some(leader),
		replication_factor: NODES.len() as u32,
		peers: NODES
			.into_iter()
			.map(|node_id| ClusterPeerWire {
				node_id,
				address: address(node_id),
				tier_bits: TransportTierSet::empty().with(TIER).as_u8(),
				role_bits: NodeRoleSet::empty().with(NodeRole::Data).as_u8(),
				capability_bits: 0,
			})
			.collect(),
	}
}

/// One scripted answer to a table write.
#[derive(Clone, Copy)]
enum Answer {
	/// `status` with `knowledge`, hinting `hint`.
	Reply(WriteStatus, MutationCommitKnowledge, u32),
	/// The request is received and never answered.
	Silence,
}

/// A scripted node: table writes are answered from the script in order, and a
/// status request with the cluster's status. What it received is kept.
struct Scripted {
	leader: u32,
	answers: std::sync::Mutex<VecDeque<Answer>>,
	write_ids: std::sync::Mutex<Vec<crate::schema::ClientWriteId>>,
	attempts_remaining: std::sync::Mutex<Vec<u8>>,
	status_requests: AtomicUsize,
	/// A status request is counted and then fails; table writes are answered.
	fail_status: std::sync::atomic::AtomicBool,
	/// Wendelon ADR 0113: mutation-outcome resolves received, each withheld.
	resolve_requests: AtomicUsize,
	/// DC-281: each table query received, its `deadline_remaining_millis`; each
	/// is withheld (no first response).
	query_deadlines: std::sync::Mutex<Vec<u64>>,
}

impl Scripted {
	fn new(leader: u32, answers: &[Answer]) -> Arc<Self> {
		Arc::new(Self {
			leader,
			answers: std::sync::Mutex::new(answers.iter().copied().collect()),
			write_ids: Default::default(),
			attempts_remaining: Default::default(),
			status_requests: AtomicUsize::new(0),
			fail_status: Default::default(),
			resolve_requests: AtomicUsize::new(0),
			query_deadlines: Default::default(),
		})
	}

	fn sent(&self) -> Vec<u8> {
		self.attempts_remaining.lock().unwrap().clone()
	}
}

#[async_trait::async_trait]
impl HrftService for Scripted {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		let reply = |msg_type: MsgType, meta: Vec<u8>| HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: msg_type.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: PooledBuf::orphan(meta),
			arrow_payload: None,
			trailing: None,
		};
		if frame.header.msg_type == MsgType::STATUS_REQ.0 {
			self.status_requests.fetch_add(1, Ordering::SeqCst);
			if self.fail_status.load(Ordering::SeqCst) {
				return Err(std::io::Error::other("scripted: no status from this node"));
			}
			return Ok(Some(reply(
				MsgType::STATUS_RESP,
				crate::network::wire::encode(&status_of(self.leader))?,
			)));
		}
		if frame.header.msg_type == MsgType::TABLE_QUERY_REQ.0 {
			// DC-281: a node that receives a query and never sends its first frame.
			let request: crate::schema::TableQueryRequestMeta = *bytemuck::from_bytes(
				&frame.meta[..std::mem::size_of::<crate::schema::TableQueryRequestMeta>()],
			);
			self.query_deadlines
				.lock()
				.unwrap()
				.push(request.deadline_remaining_millis);
			std::future::pending::<()>().await;
		}
		if frame.header.msg_type == MsgType::MUTATION_OUTCOME_REQ.0 {
			// Wendelon ADR 0113: a node that receives a resolve and never answers.
			self.resolve_requests.fetch_add(1, Ordering::SeqCst);
			std::future::pending::<()>().await;
		}
		assert_eq!(frame.header.msg_type, MsgType::TABLE_WRITE_REQ.0);
		let request: TableWriteRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableWriteRequestMeta>()]);
		self.attempts_remaining
			.lock()
			.unwrap()
			.push(request.attempts_remaining());
		self.write_ids.lock().unwrap().push(request.parent_write_id);
		let answer = self
			.answers
			.lock()
			.unwrap()
			.pop_front()
			.expect("an attempt beyond the script");
		let Answer::Reply(status, knowledge, hint) = answer else {
			std::future::pending::<()>().await;
			unreachable!();
		};
		let mut write: WriteResponseMeta = bytemuck::Zeroable::zeroed();
		write.status = status;
		write.epoch = request.expected_epoch;
		write.commit_hlc = request.client_hlc;
		write.leader_hint = node(hint);
		write.echoed_write_id = request.parent_write_id;
		let response = TableWriteResponseMeta {
			write: write.with_mutation_outcome(knowledge, MutationStopReason::NONE),
			reject_code: 0,
			_pad: [0; 6],
		};
		Ok(Some(reply(
			MsgType::TABLE_WRITE_RESP,
			bytemuck::bytes_of(&response).to_vec(),
		)))
	}
}

/// The simulated provider, except that a connect to `pending` never completes.
struct PendingAt {
	inner: SimulatedTransportProvider,
	pending: std::sync::atomic::AtomicU32,
	dials: AtomicUsize,
	/// DC-281: a connect to this node completes, after [`SLOW_CONNECT`].
	slow: std::sync::atomic::AtomicU32,
}

/// DC-281: how long a connect to the slow node takes.
const SLOW_CONNECT: Duration = Duration::from_millis(200);

#[async_trait::async_trait]
impl TransportProvider for PendingAt {
	async fn connect(
		&self,
		target: NodeId,
		address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		if target.id == self.pending.load(Ordering::SeqCst) {
			self.dials.fetch_add(1, Ordering::SeqCst);
			std::future::pending::<()>().await;
		}
		if target.id == self.slow.load(Ordering::SeqCst) {
			tokio::time::sleep(SLOW_CONNECT).await;
		}
		self.inner.connect(target, address).await
	}

	fn withdraw_local(
		&self,
		node_id: NodeId,
		address: &str,
		service: &Arc<dyn HrftService>,
	) -> std::io::Result<()> {
		self.inner.withdraw_local(node_id, address, service)
	}
}

/// A topology source that never answers and cannot bound itself.
struct PendingTopology;

#[async_trait::async_trait]
impl TopologySource for PendingTopology {
	async fn fetch(&self) -> std::io::Result<ClusterStatusWire> {
		std::future::pending().await
	}
}

struct Fixture {
	entry: Arc<Scripted>,
	provider: Arc<PendingAt>,
	network: Arc<SimulatedTransportNetwork>,
}

const ENTER: u32 = NODES[0];
const OTHER: u32 = NODES[1];
const PENDING: u32 = NODES[2];

/// The entry node (the control leader) and the other live node answer from
/// `script`; a connect to `pending` never completes.
fn fixture(script: &[Answer], pending: u32) -> Fixture {
	let entry = Scripted::new(ENTER, script);
	let network = Arc::new(SimulatedTransportNetwork::default());
	for id in NODES {
		network.publish(node(id), &address(id), entry.clone());
	}
	let provider = Arc::new(PendingAt {
		inner: SimulatedTransportProvider::new(
			node(99),
			network.clone(),
			crate::network::pool::BufPool::new(64, 1024 * 1024),
			TIER,
		),
		pending: std::sync::atomic::AtomicU32::new(pending),
		dials: AtomicUsize::new(0),
		slow: std::sync::atomic::AtomicU32::new(u32::MAX),
	});
	Fixture {
		entry,
		provider,
		network,
	}
}

fn client_over(fixture: &Fixture, topology: Arc<dyn TopologySource>) -> Arc<WendelonClient> {
	let provider: Arc<dyn TransportProvider> = fixture.provider.clone();
	WendelonClient::new(WendelonClientConfig {
		local_tiers: TransportTierSet::empty().with(TIER),
		providers: [(TIER, provider)].into(),
		bootstrap: status_of(ENTER),
		topology,
		pool_max_retained: 64,
		pool_max_cap_bytes: 1024 * 1024,
	})
}

fn static_topology() -> Arc<dyn TopologySource> {
	Arc::new(StaticTopologySource::new(status_of(ENTER)))
}

/// One `table_write` call with `deadline_in`: its answer (or error), the
/// operation's identity, and how long the call took.
async fn call(
	client: &WendelonClient,
	deadline_in: Option<Duration>,
) -> (
	std::io::Result<WriteResponseMeta>,
	crate::schema::ClientWriteId,
	Duration,
) {
	call_as(client, deadline_in, uuid_v7()).await
}

/// `call`, with the caller's own operation identity.
async fn call_as(
	client: &WendelonClient,
	deadline_in: Option<Duration>,
	client_uuid: [u8; 16],
) -> (
	std::io::Result<WriteResponseMeta>,
	crate::schema::ClientWriteId,
	Duration,
) {
	let descriptor = descriptor();
	let meta = write_meta(&descriptor, 1, client_uuid);
	let started = Instant::now();
	let result = tokio::time::timeout(
		Duration::from_secs(20),
		client.table_write(TableWriteRequest {
			meta,
			payload: rows(&descriptor, &[1]),
			descriptor,
			deadline: deadline_in.map(|within| Instant::now() + within),
		}),
	)
	.await
	.expect("the call returns well inside the outer bound");
	(
		result.map(|result| result.response.write),
		meta.parent_write_id,
		started.elapsed(),
	)
}

fn redirect_to(hint: u32) -> Answer {
	Answer::Reply(
		WriteStatus::NOT_LEADER,
		MutationCommitKnowledge::UNSPECIFIED,
		hint,
	)
}

const OK: Answer = Answer::Reply(WriteStatus::OK, MutationCommitKnowledge::COMMITTED, ENTER);
const BUDGET: Duration = crate::client::MUTATION_CANDIDATE_BUDGET;

/// The data dial. The entry node redirects to a node whose connect never
/// completes; the topology is static, so the refresh is not what waits. The
/// call gives the hinted node its budget, then asks the entry node again with
/// the SAME identity, and is answered. Attempts: the redirect is the first,
/// the hinted node's connect that never completed is the second, the entry
/// node's second request is the third.
#[tokio::test]
async fn a_hinted_node_whose_connect_never_completes_does_not_consume_the_call() {
	let fixture = fixture(&[redirect_to(PENDING), OK], PENDING);
	let client = client_over(&fixture, static_topology());
	let (answer, write_id, took) = call(&client, Some(Duration::from_secs(8))).await;
	let answer = answer.unwrap();
	assert_eq!(answer.status, WriteStatus::OK, "{answer:?}");
	assert_eq!(answer.echoed_write_id, write_id);
	assert_eq!(fixture.entry.sent(), [3, 1]);
	assert_eq!(
		*fixture.entry.write_ids.lock().unwrap(),
		vec![write_id; 2],
		"one operation identity"
	);
	assert_eq!(fixture.provider.dials.load(Ordering::SeqCst), 1);
	assert!(
		took >= BUDGET && took < BUDGET * 3,
		"one hinted budget, not the deadline: {took:?}"
	);
}

/// The same with no caller deadline: the hinted budget is a policy of its own.
#[tokio::test]
async fn a_hinted_node_gets_a_finite_budget_when_the_caller_gave_no_deadline() {
	let fixture = fixture(&[redirect_to(PENDING), OK], PENDING);
	let client = client_over(&fixture, static_topology());
	let (answer, write_id, took) = call(&client, None).await;
	let answer = answer.unwrap();
	assert_eq!(answer.status, WriteStatus::OK, "{answer:?}");
	assert_eq!(fixture.entry.sent(), [3, 1]);
	assert_eq!(*fixture.entry.write_ids.lock().unwrap(), vec![write_id; 2]);
	assert!(took >= BUDGET && took < BUDGET * 3, "{took:?}");
}

/// The refresh and the attempt are separate questions. With the real
/// control-ring topology source the hinted node is asked for the cluster's
/// status FIRST; its connect never completes, and the refresh moves on within
/// the budget. That says nothing about a mutation, so the hinted node is then
/// tried for the write too, in a budget of its own, and only that failure
/// takes it out of the call's targets. Two bounded connection tries, one for
/// each purpose; the entry node's second request is the third attempt.
#[tokio::test]
async fn a_hinted_node_that_answers_neither_a_status_request_nor_a_connect_gets_one_bounded_try_for_each()
 {
	let fixture = fixture(&[redirect_to(PENDING), OK], PENDING);
	// A factory over the same provider, for the real topology source.
	let seed = client_over(&fixture, static_topology());
	let topology = Arc::new(ControlRingTopologySource::new(
		seed.factory().clone(),
		seed.pool().clone(),
		vec![node(ENTER)],
	));
	let client = client_over(&fixture, topology);
	let (answer, write_id, took) = call(&client, Some(Duration::from_secs(8))).await;
	let answer = answer.unwrap();
	assert_eq!(answer.status, WriteStatus::OK, "{answer:?}");
	assert_eq!(fixture.entry.sent(), [3, 1]);
	assert_eq!(*fixture.entry.write_ids.lock().unwrap(), vec![write_id; 2]);
	assert_eq!(
		fixture.provider.dials.load(Ordering::SeqCst),
		2,
		"asked once in the refresh and once for the attempt, never a third time"
	);
	assert_eq!(fixture.entry.status_requests.load(Ordering::SeqCst), 1);
	assert!(
		took >= BUDGET * 2 && took < BUDGET * 4,
		"two budgets, not the deadline: {took:?}"
	);
}

/// A node that fails a status request can still take the write (the real
/// three-daemon run of 2026-11-13: the leader redirected to the range's
/// holder, the holder failed the refresh, and the call spent its attempts on
/// the leader without ever asking the holder). The entry node redirects to
/// the holder; the holder fails STATUS and answers TABLE_WRITE. ONE call
/// reaches the holder with the same identity and commits there; the holder is
/// asked for the status once, and the entry node for the write once.
#[tokio::test]
async fn a_hinted_node_that_fails_a_status_request_is_still_asked_for_the_write() {
	// The entry node would redirect every attempt; only the first is wanted.
	let entry = Scripted::new(ENTER, &[redirect_to(OTHER); 3]);
	let holder = Scripted::new(
		ENTER,
		&[Answer::Reply(
			WriteStatus::OK,
			MutationCommitKnowledge::COMMITTED,
			OTHER,
		)],
	);
	holder.fail_status.store(true, Ordering::SeqCst);
	let network = Arc::new(SimulatedTransportNetwork::default());
	network.publish(node(ENTER), &address(ENTER), entry.clone());
	network.publish(node(OTHER), &address(OTHER), holder.clone());
	let fixture = Fixture {
		entry: entry.clone(),
		provider: Arc::new(PendingAt {
			inner: SimulatedTransportProvider::new(
				node(99),
				network.clone(),
				crate::network::pool::BufPool::new(64, 1024 * 1024),
				TIER,
			),
			// No node's connect is held.
			pending: std::sync::atomic::AtomicU32::new(0),
			dials: AtomicUsize::new(0),
			slow: std::sync::atomic::AtomicU32::new(u32::MAX),
		}),
		network,
	};
	let seed = client_over(&fixture, static_topology());
	let topology = Arc::new(ControlRingTopologySource::new(
		seed.factory().clone(),
		seed.pool().clone(),
		vec![node(ENTER)],
	));
	let client = client_over(&fixture, topology);
	let (answer, write_id, took) = call(&client, Some(Duration::from_secs(8))).await;
	let answer = answer.unwrap();
	assert_eq!(
		holder.sent(),
		[2],
		"the holder was asked for the write, as the second attempt: {answer:?}"
	);
	assert_eq!(*holder.write_ids.lock().unwrap(), vec![write_id]);
	assert_eq!(answer.status, WriteStatus::OK, "{answer:?}");
	assert_eq!(
		answer.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);
	assert_eq!(answer.echoed_write_id, write_id);
	assert_eq!(entry.sent(), [3], "the entry node was asked once");
	assert_eq!(*entry.write_ids.lock().unwrap(), vec![write_id]);
	assert_eq!(
		holder.status_requests.load(Ordering::SeqCst),
		1,
		"the holder was asked for the status once"
	);
	assert_eq!(entry.status_requests.load(Ordering::SeqCst), 1);
	assert!(took < BUDGET, "nothing waited: {took:?}");
}

/// Across two calls of one client. A call that ends on a last redirect has
/// remembered the redirect's node as the leader, and no refresh follows a last
/// attempt to correct that. The next call starts with no hint and takes the
/// remembered node as its first target (the real three-daemon run of
/// 2026-11-13, holder stopped: a first call exhausted, a second ran to its
/// deadline). The node's connect never completes; the entry node would answer.
/// The second call, with the SAME identity, gives that first node a finite
/// budget, falls back, and is answered by the entry node well inside its
/// deadline.
#[tokio::test]
async fn a_node_remembered_from_a_last_redirect_does_not_consume_the_next_call() {
	let fixture = fixture(&[redirect_to(PENDING), redirect_to(PENDING), OK], PENDING);
	let client = client_over(&fixture, static_topology());
	let uuid = uuid_v7();
	// Call 1: redirect, the hinted node's budget, the last redirect.
	let (first, write_id, _) = call_as(&client, Some(Duration::from_secs(8)), uuid).await;
	let first = first.unwrap();
	assert_eq!(
		first.status,
		WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
		"{first:?}"
	);
	assert_eq!(fixture.entry.sent(), [3, 1]);
	assert_eq!(fixture.provider.dials.load(Ordering::SeqCst), 1);
	// Where the next call starts from: the last answer's hint, kept as leader.
	assert_eq!(
		client.leader().map(|node| node.id),
		Some(PENDING),
		"the last redirect's node is what the client now believes leads"
	);
	// Call 2: the same client and identity.
	let deadline = Duration::from_secs(6);
	let (second, second_id, took) = call_as(&client, Some(deadline), uuid).await;
	let second = second.unwrap();
	assert_eq!(second_id, write_id, "one operation identity");
	assert_eq!(
		fixture.provider.dials.load(Ordering::SeqCst),
		2,
		"the second call began at the remembered node"
	);
	assert_eq!(
		second.status,
		WriteStatus::OK,
		"the entry node's answer is reached, after {took:?}: {second:?}"
	);
	assert_eq!(second.echoed_write_id, write_id);
	assert_eq!(
		*fixture.entry.write_ids.lock().unwrap(),
		vec![write_id; 3],
		"every request carried the one identity"
	);
	assert!(
		took >= BUDGET && took < BUDGET * 3,
		"one budget at the remembered node, not the deadline: {took:?}"
	);
}

/// The caller's deadline is shorter than the hinted budget: the call ends at
/// the deadline, not at the budget. Its stop is the client's own, with no
/// server answer to carry it, so it reports the operation's outcome as
/// indeterminate: this call's one answered request was a redirect, but a call
/// has no evidence about what an earlier call with the same identity did.
#[tokio::test]
async fn a_deadline_shorter_than_the_hinted_budget_ends_the_call_at_the_deadline() {
	let fixture = fixture(&[redirect_to(PENDING)], PENDING);
	let client = client_over(&fixture, static_topology());
	let deadline = Duration::from_millis(300);
	let (answer, write_id, took) = call(&client, Some(deadline)).await;
	let answer = answer.unwrap();
	assert_eq!(
		(
			answer.status,
			answer.mutation_stop_reason(),
			answer.mutation_commit_knowledge(),
			answer.echoed_write_id,
		),
		(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			MutationCommitKnowledge::INDETERMINATE,
			write_id,
		),
		"{answer:?}"
	);
	assert_eq!(
		fixture.entry.sent(),
		[3],
		"one request, answered NOT_LEADER"
	);
	assert!(took >= deadline && took < BUDGET, "{took:?}");
}

/// The first node of a call, chosen with no hint (here the leader the client
/// believes in), gets the same budget as any other: its connect never
/// completes, the attempt is spent, and the next replica answers. With a
/// caller deadline and with none: the first connect is no longer unbounded.
#[tokio::test]
async fn a_first_node_whose_connect_never_completes_gets_its_budget_and_the_next_node_answers() {
	for deadline in [Some(Duration::from_secs(8)), None] {
		let fixture = fixture(&[OK], ENTER);
		let client = client_over(&fixture, static_topology());
		let (answer, write_id, took) = call(&client, deadline).await;
		let answer = answer.unwrap();
		assert_eq!(answer.status, WriteStatus::OK, "{deadline:?}: {answer:?}");
		assert_eq!(answer.echoed_write_id, write_id);
		assert_eq!(
			fixture.entry.sent(),
			[2],
			"{deadline:?}: the pending connect was the first attempt"
		);
		assert_eq!(*fixture.entry.write_ids.lock().unwrap(), vec![write_id]);
		assert_eq!(fixture.provider.dials.load(Ordering::SeqCst), 1);
		assert!(
			took >= BUDGET && took < BUDGET * 3,
			"{deadline:?}: one budget: {took:?}"
		);
	}
}

/// A first node that refuses the connect at once (it is not on the network) is
/// handled the same way: the attempt is spent, the node is not asked again,
/// and the next replica answers under the same identity. Its error is not the
/// call's while another node can be asked.
#[tokio::test]
async fn a_first_node_that_refuses_its_connect_at_once_is_skipped_for_the_next_node() {
	let fixture = fixture(&[OK], 0);
	fixture.network.unpublish(node(ENTER));
	let client = client_over(&fixture, static_topology());
	let (answer, write_id, took) = call(&client, Some(Duration::from_secs(8))).await;
	let answer = answer.unwrap();
	assert_eq!(answer.status, WriteStatus::OK, "{answer:?}");
	assert_eq!(answer.echoed_write_id, write_id);
	assert_eq!(
		fixture.entry.sent(),
		[2],
		"the refused connect was the first attempt"
	);
	assert_eq!(*fixture.entry.write_ids.lock().unwrap(), vec![write_id]);
	assert!(took < BUDGET, "nothing waited: {took:?}");
}

/// When every node refuses, the call's error is the last node's own, and the
/// call does not outlast its attempts.
#[tokio::test]
async fn when_no_node_gives_a_stream_the_call_ends_with_the_last_nodes_error_or_its_attempts() {
	let fixture = fixture(&[], 0);
	for id in NODES {
		fixture.network.unpublish(node(id));
	}
	let client = client_over(&fixture, static_topology());
	let (answer, write_id, took) = call(&client, Some(Duration::from_secs(8))).await;
	// Three replicas, three attempts: the last attempt's refusal ends the call
	// as exhausted, indeterminate, with the operation's identity.
	let answer = answer.unwrap();
	assert_eq!(
		(
			answer.status,
			answer.mutation_stop_reason(),
			answer.mutation_commit_knowledge(),
			answer.echoed_write_id,
		),
		(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			MutationStopReason::ATTEMPT_EXHAUSTION,
			MutationCommitKnowledge::INDETERMINATE,
			write_id,
		)
	);
	assert!(fixture.entry.sent().is_empty());
	assert!(took < BUDGET, "{took:?}");
}

/// A caller deadline shorter than the budget still wins, for the first node's
/// connect and for a topology source that cannot bound itself.
#[tokio::test]
async fn the_callers_deadline_holds_while_the_first_node_or_a_topology_source_is_pending() {
	// The entry node itself never completes its connect: nothing was sent.
	let fixture_a = fixture(&[], ENTER);
	let client_a = client_over(&fixture_a, static_topology());
	let deadline = Duration::from_millis(300);
	let (answer, write_id, took) = call(&client_a, Some(deadline)).await;
	let answer = answer.unwrap();
	assert_eq!(
		(
			answer.status,
			answer.mutation_stop_reason(),
			answer.mutation_commit_knowledge(),
			answer.echoed_write_id,
		),
		(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			MutationCommitKnowledge::INDETERMINATE,
			write_id,
		)
	);
	assert!(
		fixture_a.entry.sent().is_empty(),
		"nothing was sent by this call; the stop still claims nothing about the operation"
	);
	assert!(
		took >= deadline && took < Duration::from_secs(2),
		"{took:?}"
	);

	// A redirect to a live node, then a topology refresh that never returns.
	let fixture_b = fixture(&[redirect_to(OTHER)], 0);
	let client_b = client_over(&fixture_b, Arc::new(PendingTopology));
	let (answer, write_id, took) = call(&client_b, Some(deadline)).await;
	let answer = answer.unwrap();
	assert_eq!(
		(
			answer.status,
			answer.mutation_stop_reason(),
			answer.mutation_commit_knowledge(),
			answer.echoed_write_id,
		),
		(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			MutationCommitKnowledge::INDETERMINATE,
			write_id,
		)
	);
	assert_eq!(fixture_b.entry.sent(), [3]);
	assert!(
		took >= deadline && took < Duration::from_secs(2),
		"{took:?}"
	);
	let _ = (&fixture_a.network, &fixture_b.network);
}

/// A request that was received and is never answered: at the caller's deadline
/// the call stops, and says it does not know what became of that request. It
/// does not say the write did not commit.
#[tokio::test]
async fn a_request_that_is_never_answered_ends_indeterminate_at_the_callers_deadline() {
	let fixture = fixture(&[Answer::Silence], 0);
	let client = client_over(&fixture, static_topology());
	let deadline = Duration::from_millis(400);
	let (answer, write_id, took) = call(&client, Some(deadline)).await;
	let answer = answer.unwrap();
	assert_eq!(
		(
			answer.status,
			answer.mutation_stop_reason(),
			answer.mutation_commit_knowledge(),
			answer.echoed_write_id,
		),
		(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			MutationCommitKnowledge::INDETERMINATE,
			write_id,
		),
		"{answer:?}"
	);
	assert_eq!(fixture.entry.sent(), [3], "the request reached its node");
	assert!(
		took >= deadline && took < Duration::from_secs(2),
		"{took:?}"
	);
}

/// An earlier attempt was answered WRITE_STALLED, so it may have been
/// submitted. A later hinted node that never connects, and a later answer
/// that carries no uncertainty, do not launder that: the call's exhaustion is
/// indeterminate. And when the call's LAST attempt is the hinted node that
/// never connects, the call still ends, with the same honesty.
#[tokio::test]
async fn an_unreached_node_neither_sets_nor_clears_what_an_earlier_answer_left_open() {
	let stalled = Answer::Reply(
		WriteStatus::WRITE_STALLED,
		MutationCommitKnowledge::INDETERMINATE,
		PENDING,
	);
	let backpressured = Answer::Reply(
		WriteStatus::BACKPRESSURED,
		MutationCommitKnowledge::UNSPECIFIED,
		ENTER,
	);
	// Stalled (hinting the pending node), the pending node, then backpressure.
	let fixture_a = fixture(&[stalled, backpressured], PENDING);
	let client_a = client_over(&fixture_a, static_topology());
	let (answer, write_id, _) = call(&client_a, Some(Duration::from_secs(8))).await;
	let answer = answer.unwrap();
	assert_eq!(
		(
			answer.status,
			answer.mutation_stop_reason(),
			answer.mutation_commit_knowledge(),
		),
		(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			MutationStopReason::ATTEMPT_EXHAUSTION,
			MutationCommitKnowledge::INDETERMINATE,
		),
		"{answer:?}"
	);
	assert_eq!(fixture_a.entry.sent(), [3, 1]);
	assert_eq!(
		*fixture_a.entry.write_ids.lock().unwrap(),
		vec![write_id; 2]
	);

	// Backpressure, then stalled hinting the pending node: the last attempt
	// is the pending node's connect.
	let fixture_b = fixture(&[backpressured, stalled], PENDING);
	let client_b = client_over(&fixture_b, static_topology());
	let (answer, write_id, took) = call(&client_b, Some(Duration::from_secs(8))).await;
	let answer = answer.unwrap();
	assert_eq!(
		(
			answer.status,
			answer.mutation_stop_reason(),
			answer.mutation_commit_knowledge(),
			answer.echoed_write_id,
		),
		(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			MutationStopReason::ATTEMPT_EXHAUSTION,
			MutationCommitKnowledge::INDETERMINATE,
			write_id,
		),
		"{answer:?}"
	);
	assert_eq!(fixture_b.entry.sent(), [3, 2]);
	assert!(took < Duration::from_secs(6), "{took:?}");

	// Control, the OLDER rule, unchanged: the call's last attempt was ANSWERED
	// (a redirect), so exhaustion is decided from this call's answered attempts
	// (its recorded cross-call limitation is DC-215's, not widened here).
	let fixture_c = fixture(&[redirect_to(PENDING), redirect_to(PENDING)], PENDING);
	let client_c = client_over(&fixture_c, static_topology());
	let (answer, _, _) = call(&client_c, Some(Duration::from_secs(8))).await;
	let answer = answer.unwrap();
	assert_eq!(
		(
			answer.status,
			answer.mutation_stop_reason(),
			answer.mutation_commit_knowledge(),
		),
		(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			MutationStopReason::ATTEMPT_EXHAUSTION,
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
		),
		"{answer:?}"
	);
	// The avoided node is not dialed a second time by the call.
	assert_eq!(fixture_c.entry.sent(), [3, 1]);
	assert_eq!(fixture_c.provider.dials.load(Ordering::SeqCst), 1);
}

/// One operation identity, two calls. The first call's request reached a node
/// and was never answered: it may have been submitted. The second call, with
/// the SAME identity, reaches its deadline before it reaches any node. It has
/// no evidence about the first call, and it does not claim the operation did
/// not commit.
#[tokio::test]
async fn a_second_call_that_reaches_no_node_does_not_claim_the_operation_did_not_commit() {
	let fixture = fixture(&[Answer::Silence], 0);
	let client = client_over(&fixture, static_topology());
	let identity = uuid_v7();
	let deadline = Duration::from_millis(300);
	let (first, write_id, _) = call_as(&client, Some(deadline), identity).await;
	let first = first.unwrap();
	assert_eq!(
		first.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	assert_eq!(
		fixture.entry.sent(),
		[3],
		"the first call's request arrived"
	);

	// Now the entry node's connect never completes, for a fresh client with no
	// connection of its own.
	fixture.provider.pending.store(ENTER, Ordering::SeqCst);
	let second_client = client_over(&fixture, static_topology());
	let (second, second_id, took) = call_as(&second_client, Some(deadline), identity).await;
	let second = second.unwrap();
	assert_eq!(second_id, write_id, "the same operation identity");
	assert_eq!(
		(
			second.status,
			second.mutation_stop_reason(),
			second.mutation_commit_knowledge(),
			second.echoed_write_id,
		),
		(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			MutationCommitKnowledge::INDETERMINATE,
			write_id,
		),
		"{second:?}"
	);
	assert_eq!(fixture.entry.sent(), [3], "the second call sent nothing");
	assert!(
		took >= deadline && took < Duration::from_secs(2),
		"{took:?}"
	);
}

/// The bounded refresh asks no excluded node, even one among the topology
/// source's own bootstrap peers, and even when every permitted candidate
/// fails: it returns the failure and names the candidates it asked.
#[tokio::test]
async fn a_bounded_refresh_never_asks_an_excluded_node_even_from_its_bootstrap_peers() {
	// Only the excluded node would hang; the permitted ones are not on the
	// network at all, so asking them fails at once.
	let network = Arc::new(SimulatedTransportNetwork::default());
	let fixture = Fixture {
		entry: Scripted::new(ENTER, &[]),
		provider: Arc::new(PendingAt {
			inner: SimulatedTransportProvider::new(
				node(99),
				network.clone(),
				crate::network::pool::BufPool::new(64, 1024 * 1024),
				TIER,
			),
			pending: std::sync::atomic::AtomicU32::new(PENDING),
			dials: AtomicUsize::new(0),
			slow: std::sync::atomic::AtomicU32::new(u32::MAX),
		}),
		network,
	};
	let seed = client_over(&fixture, static_topology());
	// The excluded node is FIRST among the source's own bootstrap peers.
	let source = ControlRingTopologySource::new(
		seed.factory().clone(),
		seed.pool().clone(),
		vec![node(PENDING), node(ENTER)],
	);
	let started = Instant::now();
	let (result, unanswered) = source
		.fetch_from_candidates_within(&[node(OTHER)], &[node(PENDING)], BUDGET, None)
		.await;
	assert!(result.is_err(), "every permitted candidate failed");
	assert_eq!(
		unanswered.iter().map(|node| node.id).collect::<Vec<_>>(),
		[OTHER, ENTER],
		"the candidate, then the permitted bootstrap peer"
	);
	assert_eq!(
		fixture.provider.dials.load(Ordering::SeqCst),
		0,
		"the excluded node was not asked"
	);
	assert!(started.elapsed() < BUDGET, "{:?}", started.elapsed());
	// An empty candidate list means the source's own peers, still minus the
	// excluded: it does not restore one.
	let (result, unanswered) = source
		.fetch_from_candidates_within(&[], &[node(PENDING)], BUDGET, None)
		.await;
	assert!(result.is_err());
	assert_eq!(
		unanswered.iter().map(|node| node.id).collect::<Vec<_>>(),
		[ENTER]
	);
	assert_eq!(fixture.provider.dials.load(Ordering::SeqCst), 0);
	// Control: without the exclusion the same source does ask it, and waits
	// out its budget there.
	let (_, unanswered) = source
		.fetch_from_candidates_within(&[], &[], Duration::from_millis(200), None)
		.await;
	assert_eq!(
		unanswered.iter().map(|node| node.id).collect::<Vec<_>>(),
		[PENDING, ENTER]
	);
	assert_eq!(fixture.provider.dials.load(Ordering::SeqCst), 1);
}

/// ADR 0105 (wendelon), DC-246: a control-only member's answer to a table write
/// (`NOT_LEADER`, no hint, outcome `INDETERMINATE`) is one the client already
/// handles: it sends the same identity again within its attempts, and the
/// commit that answers it is the call's answer. (The scripted service is shared
/// by every node, so which node the retry reached is not observed here.)
#[tokio::test]
async fn a_not_serving_answer_sends_the_same_identity_again() {
	let not_serving = Answer::Reply(
		WriteStatus::NOT_LEADER,
		MutationCommitKnowledge::INDETERMINATE,
		0,
	);
	let fixture = fixture(&[not_serving, OK], 0);
	let client = client_over(&fixture, static_topology());
	let (answer, write_id, _) = call(&client, Some(Duration::from_secs(8))).await;
	let answer = answer.unwrap();
	assert_eq!(answer.status, WriteStatus::OK, "{answer:?}");
	assert_eq!(answer.echoed_write_id, write_id);
	assert_eq!(
		*fixture.entry.write_ids.lock().unwrap(),
		vec![write_id, write_id],
		"the same identity, twice"
	);
}

/// ADR 0105 (wendelon), DC-246: when every attempt is answered so, the call
/// ends with the client's own typed exhaustion, the outcome unknown; never an
/// unclassified transport error or a receipt.
#[tokio::test]
async fn not_serving_answers_to_every_attempt_end_in_typed_exhaustion() {
	let not_serving = Answer::Reply(
		WriteStatus::NOT_LEADER,
		MutationCommitKnowledge::INDETERMINATE,
		0,
	);
	let fixture = fixture(&[not_serving; crate::client::DATA_RETRY_BUDGET], 0);
	let client = client_over(&fixture, static_topology());
	let (answer, write_id, _) = call(&client, Some(Duration::from_secs(8))).await;
	let answer = answer.expect("a typed answer, not a transport error");
	assert_eq!(
		answer.status,
		WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
		"{answer:?}"
	);
	assert_eq!(
		answer.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	assert_eq!(answer.echoed_write_id, write_id);
	assert_eq!(
		fixture.entry.write_ids.lock().unwrap().len(),
		crate::client::DATA_RETRY_BUDGET
	);
}

/// Wendelon ADR 0113: a node that receives the resolve and withholds its answer,
/// and another whose connect never completes, cannot hold the public resolve: it
/// ends at its one budget with a TimedOut error and no outcome.
#[tokio::test(start_paused = true)]
async fn a_withheld_resolve_answer_ends_the_public_call_at_its_budget_with_no_outcome() {
	let fixture = fixture(&[], PENDING);
	let client = client_over(&fixture, static_topology());
	let started = tokio::time::Instant::now();
	let answer = client
		.resolve_mutation_outcome(crate::client::core::MutationOutcomeRequest {
			tenant_id: TenantId::ZERO,
			operation_id: crate::schema::ClientWriteId {
				client_uuid: uuid_v7(),
				sequence_no: 1,
				idempotency_ttl_ms: 30_000,
				_pad: 0,
			},
		})
		.await;
	let elapsed = started.elapsed();
	let error = answer.expect_err("no outcome is asserted");
	assert_eq!(error.kind(), std::io::ErrorKind::TimedOut, "{error}");
	let budget = crate::client::core::MUTATION_RESOLVE_BUDGET;
	assert!(
		elapsed >= budget && elapsed < budget + Duration::from_secs(1),
		"{elapsed:?}"
	);
	assert!(
		fixture.entry.resolve_requests.load(Ordering::SeqCst) >= 1,
		"a node received the resolve and withheld its answer"
	);
}

/// DC-281: `node_id` alone, as the cluster's status: every call goes there.
fn status_only(node_id: u32) -> ClusterStatusWire {
	ClusterStatusWire {
		epoch: 1,
		leader: Some(node_id),
		replication_factor: 1,
		peers: vec![ClusterPeerWire {
			node_id,
			address: address(node_id),
			tier_bits: TransportTierSet::empty().with(TIER).as_u8(),
			role_bits: NodeRoleSet::empty().with(NodeRole::Data).as_u8(),
			capability_bits: 0,
		}],
	}
}

fn client_to_only(fixture: &Fixture, node_id: u32) -> Arc<WendelonClient> {
	let provider: Arc<dyn TransportProvider> = fixture.provider.clone();
	WendelonClient::new(WendelonClientConfig {
		local_tiers: TransportTierSet::empty().with(TIER),
		providers: [(TIER, provider)].into(),
		bootstrap: status_only(node_id),
		topology: Arc::new(StaticTopologySource::new(status_only(node_id))),
		pool_max_retained: 64,
		pool_max_cap_bytes: 1024 * 1024,
	})
}

/// An attempt guard that marks its drop.
struct DropMark(Arc<std::sync::atomic::AtomicBool>);

impl Drop for DropMark {
	fn drop(&mut self) {
		self.0.store(true, Ordering::SeqCst);
	}
}

/// A public table query with `deadline_ms` (0: no caller deadline); each attempt's
/// guard marks `dropped` when it is dropped.
async fn query_with(
	client: &WendelonClient,
	deadline_ms: u64,
	dropped: Arc<std::sync::atomic::AtomicBool>,
) -> std::io::Result<()> {
	let descriptor = descriptor();
	let mut meta: crate::schema::TableQueryRequestMeta = bytemuck::Zeroable::zeroed();
	meta.expected_epoch = client.epoch();
	meta.tenant_id = TenantId::ZERO;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.physical_descriptor_fingerprint = *descriptor.physical_descriptor_fingerprint.as_bytes();
	meta.consistency = crate::schema::Consistency::LINEARIZABLE;
	meta.row_identity_projection = crate::client::core::RowIdentityProjection::None.as_u8();
	meta.request_id = crate::schema::TableQueryRequestId::random();
	meta.deadline_remaining_millis = deadline_ms;
	client
		.table_query_with_attempt_hook(
			crate::client::core::TableQueryRequest {
				meta,
				descriptor,
				pbc: Vec::new(),
				runtime_filter: None,
				row_identity: crate::client::core::RowIdentityProjection::None,
			},
			move |_| {
				Ok(Box::new(DropMark(dropped.clone()))
					as crate::client::core::TableQueryAttemptGuard)
			},
		)
		.await
		.map(|_| ())
}

/// DC-281 (Codex 01a1fd19): a node reached through a slow connect (200 ms) that
/// receives the query and withholds its first response cannot hold the public call
/// past the caller's 500 ms deadline. The call ends at it with the typed deadline
/// error, the attempt's guard dropped, and the one frame sent carried the remainder
/// computed just before it was sent (after the slow connect), not the original.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withheld_first_query_response_ends_the_public_call_at_its_deadline() {
	let fixture = fixture(&[], u32::MAX);
	fixture.provider.slow.store(ENTER, Ordering::SeqCst);
	let client = client_to_only(&fixture, ENTER);
	let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let started = tokio::time::Instant::now();
	let error = query_with(&client, 500, dropped.clone())
		.await
		.expect_err("no response, no result");
	let elapsed = started.elapsed();
	assert!(
		error.to_string().contains("query_deadline_exceeded"),
		"{error}"
	);
	assert!(
		elapsed >= Duration::from_millis(500) && elapsed < Duration::from_millis(1_500),
		"at the deadline: {elapsed:?}"
	);
	assert!(
		dropped.load(Ordering::SeqCst),
		"the attempt's guard was dropped"
	);
	let sent = fixture.entry.query_deadlines.lock().unwrap().clone();
	assert_eq!(sent.len(), 1, "one frame: {sent:?}");
	assert!(
		sent[0] > 0 && sent[0] <= 500 - SLOW_CONNECT.as_millis() as u64 + 20,
		"the remainder computed just before the send: {sent:?}"
	);
}

/// DC-281 (Codex 01a1fd19): a connect that does not complete by the caller's
/// deadline ends the public call at it, and nothing is sent afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_query_connect_held_past_the_deadline_sends_nothing_late() {
	let fixture = fixture(&[], PENDING);
	let client = client_to_only(&fixture, PENDING);
	let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let started = tokio::time::Instant::now();
	let error = query_with(&client, 300, dropped.clone())
		.await
		.expect_err("no connection, no result");
	let elapsed = started.elapsed();
	assert!(
		error.to_string().contains("query_deadline_exceeded"),
		"{error}"
	);
	assert!(
		elapsed < Duration::from_millis(1_300),
		"at the deadline: {elapsed:?}"
	);
	assert!(
		dropped.load(Ordering::SeqCst),
		"the attempt's guard was dropped"
	);
	assert!(
		fixture.provider.dials.load(Ordering::SeqCst) >= 1,
		"it dialled"
	);
	tokio::time::sleep(Duration::from_millis(1_000)).await;
	assert!(
		fixture.entry.query_deadlines.lock().unwrap().is_empty(),
		"nothing sent, then or later"
	);
}

/// DC-281 (Codex 01a1fd19): zero is no caller deadline (the call stays with the
/// withholding node; the frame carries zero), and the largest finite input is
/// clamped (no overflow; the frame carries a large finite remainder).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_is_no_query_deadline_and_the_largest_is_clamped() {
	for (input, what) in [(0_u64, "zero"), (u64::MAX, "the largest")] {
		let fixture = fixture(&[], u32::MAX);
		let client = client_to_only(&fixture, ENTER);
		let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let pending = tokio::time::timeout(
			Duration::from_millis(1_500),
			query_with(&client, input, dropped.clone()),
		)
		.await;
		assert!(pending.is_err(), "{what}: no deadline ends the call early");
		let sent = fixture.entry.query_deadlines.lock().unwrap().clone();
		assert_eq!(sent.len(), 1, "{what}: {sent:?}");
		if input == 0 {
			assert_eq!(sent[0], 0, "zero stays no caller deadline");
		} else {
			assert!(
				sent[0] > 3_600_000 && sent[0] < u64::MAX,
				"the largest input, clamped: {sent:?}"
			);
		}
	}
}

/// DC-281 (Codex 01a1fd66): the budget is charged to the attempt's own preparation.
/// With a 100 ms deadline and 300 ms of synchronous work after the payload is ready
/// (in the same poll, as serializing a large payload would take), the deadline has
/// passed when the frame would be stamped: the call ends with the typed deadline
/// error and no frame is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_query_expired_during_its_send_preparation_writes_no_frame() {
	let fixture = fixture(&[], u32::MAX);
	let client = client_to_only(&fixture, ENTER);
	client.delay_query_send_for_test(Duration::from_millis(300));
	let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let error = query_with(&client, 100, dropped.clone())
		.await
		.expect_err("expired before its frame");
	assert!(
		error.to_string().contains("query_deadline_exceeded"),
		"{error}"
	);
	assert!(
		dropped.load(Ordering::SeqCst),
		"the attempt's guard was dropped"
	);
	tokio::time::sleep(Duration::from_millis(500)).await;
	assert!(
		fixture.entry.query_deadlines.lock().unwrap().is_empty(),
		"no frame written"
	);
}
