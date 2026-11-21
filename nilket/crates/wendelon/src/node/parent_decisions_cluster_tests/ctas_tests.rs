//! Wendelon ADR 0120 slice 2: a CTAS chain's coordinator, receivers, reader and
//! authority boundary, in process on a three-voter ParentDecisions cluster,
//! through the real routes (`CTAS_DECISION_REQ`, `TABLE_CTAS_CHUNK_WRITE_REQ`,
//! `CTAS_SAGA_REQ`) and the node's router.

use super::*;
use crate::consensus::decisions::DecisionResult;
use crate::consensus::decisions::ctas::{
	ChunkDecl, CtasAbandonCause, CtasChain, CtasPhase, encode_declaration,
};
use crate::network::control_server::{
	CtasDecisionResponseWire, CtasSagaRequestWire, CtasSagaResponseWire,
};
use crate::network::core::{HRFT_MAGIC, HrftFrame};
use crate::network::decision_proposals::{
	CtasBeginRequest, CtasProposal, ProposalOutcome, ProposalRefusal, ProposalResponse,
};
use crate::network::pool::PooledBuf;
use crate::network::server::HrftService;
use crate::network::server::RequestContext;
use crate::network::tls::ActorIdentity;
use crate::schema::{FrameHeader, MsgType, ReadStatus, TableWriteResponseMeta};

pub(super) const SECRET: [u8; 32] = [0x5e; 32];
const WRONG_SECRET: [u8; 32] = [0x5f; 32];
pub(super) const ROWS: [(i64, &str); 3] = [(1, "same"), (1, "same"), (2, "other")];
const OTHER_ROWS: [(i64, &str); 3] = [(1, "same"), (1, "same"), (3, "forged")];

/// The CTAS target: AppendOnly, as Toron materializes it, under a fresh id.
pub(super) fn target(object: u8, name: &str) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([object; 16]),
		SchemaGeneration::new(1).get(),
		"analytics",
		name,
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, true),
		]),
		RowIdentityPolicy::AppendOnly,
		crate::schema::LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(crate::schema::MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

/// The one chunk's write: the operation's id as its client id, sequence 1.
fn chunk_meta(descriptor: &TableSchemaDescriptor, op: [u8; 16]) -> TableWriteRequestMeta {
	write_meta(descriptor, ROWS.len(), op)
}

fn begin_request(descriptor: &TableSchemaDescriptor, op: [u8; 16]) -> CtasProposal {
	begin_chunks(descriptor, op, &[&ROWS])
}

/// The one chunk's write at sequence `sequence_no` (from 1).
pub(super) fn chunk_meta_at(
	descriptor: &TableSchemaDescriptor,
	op: [u8; 16],
	sequence_no: u64,
) -> TableWriteRequestMeta {
	let mut meta = chunk_meta(descriptor, op);
	meta.parent_write_id.sequence_no = sequence_no;
	meta
}

/// A Begin declaring one chunk per entry of `chunks`, at sequences 1, 2, ...
fn begin_chunks(
	descriptor: &TableSchemaDescriptor,
	op: [u8; 16],
	chunks: &[&[(i64, &str)]],
) -> CtasProposal {
	begin_chunks_in(TenantId::ZERO, descriptor, op, chunks)
}

/// [`begin_chunks`] in `tenant`.
pub(super) fn begin_chunks_in(
	tenant: TenantId,
	descriptor: &TableSchemaDescriptor,
	op: [u8; 16],
	chunks: &[&[(i64, &str)]],
) -> CtasProposal {
	let declared: Vec<ChunkDecl> = chunks
		.iter()
		.enumerate()
		.map(|(at, rows)| {
			let mut meta = chunk_meta_at(descriptor, op, at as u64 + 1);
			meta.row_count = encode_table_write_row_count(rows.len()).unwrap();
			ChunkDecl {
				sequence_no: meta.parent_write_id.sequence_no,
				rows: rows.len() as u32,
				request_digest: crate::network::data_server::coordinator::chunk_request_digest(
					tenant,
					descriptor,
					&ParentKey {
						tenant,
						..key_of(descriptor, &meta)
					},
					&named_rows(descriptor, rows),
				),
			}
		})
		.collect();
	CtasProposal::Begin(CtasBeginRequest {
		tenant,
		op,
		fingerprint: [0xf1; 32],
		descriptor: descriptor.clone(),
		declaration: encode_declaration(&declared).unwrap(),
		dispatch_hash: *blake3::hash(&SECRET).as_bytes(),
	})
}

/// A pause at `stage` armed on every node; the first node to reach it is sent
/// on the receiver. [`release_all`] releases it and disarms the rest.
fn arm_everywhere(
	cluster: &Cluster,
	stage: crate::network::data_server::child_writes::PauseStage,
) -> (
	Vec<Arc<tokio::sync::Notify>>,
	tokio::sync::mpsc::UnboundedReceiver<u32>,
) {
	let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
	let mut releases = Vec::new();
	for node in NODES {
		let (reached, release) = cluster
			.node(node)
			.data_server
			.pause_coordinator_for_test(stage);
		let sender = sender.clone();
		tokio::spawn(async move {
			reached.notified().await;
			let _ = sender.send(node);
		});
		releases.push(release);
	}
	(releases, receiver)
}

/// Releases a reached pause; a pause never reached passes when it is.
fn release_all(releases: &[Arc<tokio::sync::Notify>]) {
	for release in releases {
		release.notify_one();
	}
}

fn node_context(node: u32) -> RequestContext {
	let local = crate::schema::NodeId {
		id: node,
		_reserved: 0,
	};
	RequestContext {
		remote: Some(local),
		actor: ActorIdentity::data_node(local),
	}
}

fn frame(msg_type: MsgType, meta: Vec<u8>, arrow: Option<Vec<u8>>) -> HrftFrame {
	HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: msg_type.0,
			flags: 0,
			stream_id: 120,
			meta_len: meta.len() as u32,
			arrow_len: arrow.as_ref().map_or(0, |bytes| bytes.len() as u32),
			trailing_len: 0,
		},
		meta: PooledBuf::orphan(meta),
		arrow_payload: arrow.map(PooledBuf::orphan),
		trailing: None,
	}
}

/// One CTAS command through `node`'s router, as `context` presents it.
async fn ctas_command_as(
	cluster: &Cluster,
	node: u32,
	context: RequestContext,
	proposal: &CtasProposal,
) -> std::io::Result<ProposalResponse> {
	let request = frame(
		MsgType::CTAS_DECISION_REQ,
		crate::network::wire::encode(proposal)?,
		None,
	);
	let response = cluster
		.node(node)
		.router
		.handle_frame(context, request)
		.await?
		.expect("a response frame");
	assert_eq!(response.header.msg_type, MsgType::CTAS_DECISION_RESP.0);
	Ok(crate::network::wire::decode::<CtasDecisionResponseWire>(&response.meta)?.response)
}

/// A CTAS command entering at `entry`, following the leader hint.
pub(super) async fn ctas_command(
	cluster: &Cluster,
	entry: u32,
	proposal: &CtasProposal,
) -> ProposalResponse {
	let mut entry = entry;
	loop {
		match ctas_command_as(cluster, entry, node_context(entry), proposal)
			.await
			.unwrap()
		{
			ProposalResponse::NotLeader {
				leader_hint: Some(hint),
			} if hint != entry => entry = hint,
			other => return other,
		}
	}
}

pub(super) fn decided(response: ProposalResponse, what: &str) -> DecisionResult {
	match response {
		ProposalResponse::Applied {
			outcome: ProposalOutcome::Decision(result),
			..
		} => result,
		other => panic!("{what}: {other:?}"),
	}
}

/// One chunk write as a node sends it: a `TABLE_CTAS_CHUNK_WRITE_REQ` frame on
/// its own stream into `node`'s published service. The client's end and the
/// task running the service's handling of the stream.
async fn chunk_stream(
	cluster: &Cluster,
	node: u32,
	meta: TableWriteRequestMeta,
	batch: &RecordBatch,
	op: [u8; 16],
	secret: [u8; 32],
) -> super::write_deadline::StreamedWrite {
	let (service, pool) = (
		cluster.node(node).service.clone(),
		cluster.node(node).pool.clone(),
	);
	chunk_stream_into(service, pool, node, meta, batch, op, secret).await
}

/// [`chunk_stream`] into a given service.
async fn chunk_stream_into(
	service: Arc<dyn HrftService>,
	pool: Arc<BufPool>,
	node: u32,
	meta: TableWriteRequestMeta,
	batch: &RecordBatch,
	op: [u8; 16],
	secret: [u8; 32],
) -> super::write_deadline::StreamedWrite {
	use tokio::io::AsyncWriteExt;
	let (mut client, server) = super::write_deadline::pipe_pair();
	let mut bytes = bytemuck::bytes_of(&meta).to_vec();
	bytes.extend_from_slice(&op);
	bytes.extend_from_slice(&secret);
	let arrow = crate::client::encode_table_write_arrow_section(batch).unwrap();
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::TABLE_CTAS_CHUNK_WRITE_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: bytes.len() as u32,
		arrow_len: arrow.len() as u32,
		trailing_len: 0,
	};
	crate::network::core::write_frame(&mut client, &header, &bytes, Some(&arrow), None)
		.await
		.unwrap();
	client.shutdown().await.unwrap();
	let context = node_context(node);
	let handler =
		tokio::spawn(async move { service.handle_stream(Box::new(server), pool, context).await });
	super::write_deadline::StreamedWrite { client, handler }
}

/// One chunk write on its stream with `secret`, following redirects to its
/// coordinator and retrying a stall, as a client must.
async fn chunk_through(
	cluster: &Cluster,
	entry: u32,
	meta: TableWriteRequestMeta,
	batch: &RecordBatch,
	op: [u8; 16],
	secret: [u8; 32],
) -> TableWriteResponseMeta {
	let mut entry = entry;
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let mut write = chunk_stream(cluster, entry, meta, batch, op, secret).await;
		let pool = cluster.node(entry).pool.clone();
		let response = write.response(&pool).await.unwrap();
		write.handler.await.unwrap().unwrap();
		if response.write.status == WriteStatus::NOT_LEADER
			&& response.write.leader_hint.id != entry
		{
			entry = response.write.leader_hint.id;
			continue;
		}
		if response.write.status == WriteStatus::WRITE_STALLED
			&& tokio::time::Instant::now() < deadline
		{
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			continue;
		}
		return response;
	}
}

async fn chain_at(cluster: &Cluster, node: u32, op: [u8; 16]) -> Option<CtasChain> {
	cluster
		.node(node)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()?
				.ctas()
				.chain(TenantId::ZERO, op)
				.cloned()
		})
		.await
}

async fn last_index(cluster: &Cluster, node: u32) -> u64 {
	cluster
		.node(node)
		.raft
		.metrics()
		.borrow()
		.last_log_index
		.unwrap_or(0)
}

/// Every node answers a read of `descriptor` OK with exactly `expected`, as a
/// sorted multiset (an AppendOnly table keeps equal rows).
async fn read_multiset_everywhere(
	cluster: &Cluster,
	descriptor: &TableSchemaDescriptor,
	expected: &[(i64, &str)],
	what: &str,
) {
	let mut expected: Vec<(i64, String)> = expected
		.iter()
		.map(|(id, value)| (*id, (*value).to_owned()))
		.collect();
	expected.sort();
	for node in NODES {
		// Backpressure (a generation certifying) is transient: retried, bounded.
		let deadline = tokio::time::Instant::now() + BOUND;
		let answer = loop {
			let answer = read_at(cluster, node, descriptor).await;
			if answer.status != Some(ReadStatus::BACKPRESSURED)
				|| tokio::time::Instant::now() >= deadline
			{
				break answer;
			}
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		};
		assert_eq!(
			answer.status,
			Some(ReadStatus::OK),
			"{what}: at {node}: {answer:?}"
		);
		assert_eq!(multiset_of(&answer), expected, "{what}: at {node}");
	}
}

fn multiset_of(
	answer: &crate::network::data_server::reader::TestQueryAnswer,
) -> Vec<(i64, String)> {
	let mut rows = Vec::new();
	for batch in &answer.batches {
		let ids = batch
			.column_by_name("id")
			.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
			.expect("an id column");
		let values = arrow::compute::cast(
			batch.column_by_name("v").expect("a v column"),
			&DataType::Utf8,
		)
		.unwrap();
		let values = values.as_any().downcast_ref::<StringArray>().unwrap();
		for row in 0..batch.num_rows() {
			rows.push((ids.value(row), values.value(row).to_owned()));
		}
	}
	rows.sort();
	rows
}

/// A begun chain over `descriptor` under a fresh operation.
async fn begun(cluster: &Cluster, leader: u32, descriptor: &TableSchemaDescriptor) -> [u8; 16] {
	let op = uuid_v7();
	assert!(
		matches!(
			decided(
				ctas_command(cluster, leader, &begin_request(descriptor, op)).await,
				"Begin"
			),
			DecisionResult::CtasBegun {
				replayed: false,
				..
			}
		),
		"Begin"
	);
	op
}

/// D6 at the coordinator and the receivers. Ordinary public traffic cannot
/// reach the provisional table; a wrong credential and a wrong digest create no
/// reservation and no evidence; the owner's chunk commits through receivers that
/// resolve the private descriptor; a certified generation then erases its body
/// while its evidence stays; publication binds the exact multiset.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_ctas_chunk_needs_the_owner_credential_and_its_declared_digest() {
	let (cluster, leader, ordinary) = ready().await;
	// The owner's own Publish after the floor jump: recovery would take over.
	pause_ctas_maintenance(&cluster);
	let descriptor = target(0xC7, "ctas_copy");
	let op = begun(&cluster, leader, &descriptor).await;
	let meta = chunk_meta(&descriptor, op);
	let key = key_of(&descriptor, &meta);
	let batch = named_rows(&descriptor, &ROWS);

	// Ordinary public traffic: a table write and a read see no table.
	let public = write_through(&cluster, leader, meta, batch.clone()).await;
	assert_ne!(
		public.write.status,
		WriteStatus::OK,
		"a public write: {public:?}"
	);
	assert!(
		record_of(&cluster, key).await.is_none(),
		"a public write reserved"
	);
	for node in NODES {
		let answer = read_at(&cluster, node, &descriptor).await;
		assert_eq!(
			answer.status,
			Some(ReadStatus::QUERY_INVALID_PROGRAM),
			"a read before publication at {node}: {answer:?}"
		);
	}

	// A wrong credential, then the owner's credential with other rows (another
	// digest): nothing is reserved and no evidence is written.
	let wrong = chunk_through(&cluster, leader, meta, &batch, op, WRONG_SECRET).await;
	assert_eq!(
		wrong.write.status,
		WriteStatus::AUTHORIZATION_FAILED,
		"a wrong credential: {wrong:?}"
	);
	// The owner's credential under another client id is not a dispatch of
	// this operation.
	let foreign_key = chunk_through(
		&cluster,
		leader,
		chunk_meta(&descriptor, uuid_v7()),
		&batch,
		op,
		SECRET,
	)
	.await;
	assert_eq!(
		foreign_key.write.status,
		WriteStatus::AUTHORIZATION_FAILED,
		"the credential under another client id: {foreign_key:?}"
	);
	// The owner's credential naming another (public) table is not a dispatch of
	// this chain's object.
	let other_table = chunk_through(
		&cluster,
		leader,
		chunk_meta(&ordinary, op),
		&named_rows(&ordinary, &ROWS),
		op,
		SECRET,
	)
	.await;
	assert_eq!(
		other_table.write.status,
		WriteStatus::AUTHORIZATION_FAILED,
		"the credential naming another table: {other_table:?}"
	);
	let forged = chunk_through(
		&cluster,
		leader,
		meta,
		&named_rows(&descriptor, &OTHER_ROWS),
		op,
		SECRET,
	)
	.await;
	// A declared key with other rows: the declaration binds the key to its
	// digest, so the coordinator answers a key conflict before any Reserve.
	assert_eq!(
		forged.write.status,
		WriteStatus::WRITE_KEY_CONFLICT,
		"a wrong digest under a declared key: {forged:?}"
	);
	eprintln!(
		"ADR0120-S2 refused statuses: public {:?} wrong-credential {:?} wrong-digest {:?}",
		public.write.status, wrong.write.status, forged.write.status
	);
	assert!(
		record_of(&cluster, key).await.is_none(),
		"a refused chunk reserved"
	);
	let chain = chain_at(&cluster, leader, op).await.unwrap();
	assert_eq!(
		(chain.phase, chain.accepted, chain.refused),
		(CtasPhase::Dispatching, 0, 0),
		"evidence after the refused chunks"
	);

	// The owner's chunk commits: the receivers resolve the private descriptor.
	let owned = chunk_through(&cluster, leader, meta, &batch, op, SECRET).await;
	assert_committed(&owned, "the owner's chunk");
	cluster
		.until("the chunk's evidence", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.ctas().chain(TenantId::ZERO, op))
				.is_some_and(|chain| chain.accepted == 1)
		})
		.await;
	// Still unpublished: the chunk's rows are not readable anywhere.
	for node in NODES {
		let answer = read_at(&cluster, node, &descriptor).await;
		assert_eq!(
			answer.status,
			Some(ReadStatus::QUERY_INVALID_PROGRAM),
			"{answer:?}"
		);
	}

	// Certification resolves the private descriptor: the chunk's key retires,
	// a generation commits and activates everywhere, and its body is erased
	// while the chain's evidence stays.
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let leader = cluster.leader().await;
	assert!(
		record_of(&cluster, key).await.is_none(),
		"the certified body stays"
	);
	assert_eq!(chain_at(&cluster, leader, op).await.unwrap().accepted, 1);

	// A wrong owner secret cannot publish; the owner's can, from certified rows.
	let publish = |secret| CtasProposal::Publish {
		tenant: TenantId::ZERO,
		op,
		epoch: 1,
		secret,
	};
	let before = last_index(&cluster, leader).await;
	assert_eq!(
		ctas_command(&cluster, leader, &publish(WRONG_SECRET)).await,
		ProposalResponse::Refused(ProposalRefusal::CtasNotOwner)
	);
	assert_eq!(
		last_index(&cluster, leader).await,
		before,
		"a refused owner command appended"
	);
	assert!(matches!(
		decided(
			ctas_command(&cluster, leader, &publish(SECRET)).await,
			"Publish"
		),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
	read_multiset_everywhere(&cluster, &descriptor, &ROWS, "after publication").await;
	// The owner's re-dispatch after its key was retired by the floor and its body
	// certified: the ordinary expired answer, never a refusal or a second commit.
	let replay = chunk_through(&cluster, leader, meta, &batch, op, SECRET).await;
	assert_eq!(
		replay.write.status,
		WriteStatus::WRITE_OPERATION_EXPIRED,
		"the certified chunk's re-dispatch: {replay:?}"
	);
	read_multiset_everywhere(&cluster, &descriptor, &ROWS, "after the re-dispatch").await;
}

/// D8: a read whose cut precedes the Publish sees no table, never a prefix or
/// the whole; a fresh cut after it sees the exact multiset.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_cut_before_publication_sees_no_table() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, _) = ready().await;
	let descriptor = target(0xC8, "ctas_cut");
	let op = begun(&cluster, leader, &descriptor).await;
	let meta = chunk_meta(&descriptor, op);
	assert_committed(
		&chunk_through(
			&cluster,
			leader,
			meta,
			&named_rows(&descriptor, &ROWS),
			op,
			SECRET,
		)
		.await,
		"the owner's chunk",
	);
	cluster
		.until("the chunk's evidence", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.ctas().chain(TenantId::ZERO, op))
				.is_some_and(|chain| chain.accepted == 1)
		})
		.await;

	let reader = NODES.into_iter().find(|node| *node != leader).unwrap();
	let (reached, release) = cluster
		.node(reader)
		.data_server
		.pause_coordinator_for_test(PauseStage::ReaderAfterCut);
	let server = cluster.node(reader).data_server.clone();
	let pinned = query_meta(&cluster, reader, &descriptor);
	let reading = tokio::spawn(async move { server.table_query_for_test(pinned).await.unwrap() });
	reached.notified().await;
	assert!(matches!(
		decided(
			ctas_command(
				&cluster,
				leader,
				&CtasProposal::Publish {
					tenant: TenantId::ZERO,
					op,
					epoch: 1,
					secret: SECRET,
				},
			)
			.await,
			"Publish"
		),
		DecisionResult::CtasPublished { .. }
	));
	let object = descriptor.table_object_id;
	cluster
		.until("the publication", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.ctas().visible_from(TenantId::ZERO, object))
				.is_some()
		})
		.await;
	release.notify_one();
	let answer = reading.await.unwrap();
	assert_eq!(
		answer.status,
		Some(ReadStatus::QUERY_INVALID_PROGRAM),
		"the pre-publication cut: {answer:?}"
	);
	assert!(
		answer.batches.is_empty(),
		"the pre-publication cut answered rows"
	);
	read_multiset_everywhere(&cluster, &descriptor, &ROWS, "a fresh cut").await;
	// Codex 01a1fad7 §1: the chunk's lost acknowledgement after Publish (its
	// record still held) replays the commit through the owner's credential.
	let replay = chunk_through(
		&cluster,
		leader,
		meta,
		&named_rows(&descriptor, &ROWS),
		op,
		SECRET,
	)
	.await;
	assert!(
		matches!(
			replay.write.status,
			WriteStatus::OK | WriteStatus::DUPLICATE
		) && replay.write.mutation_commit_knowledge() == MutationCommitKnowledge::COMMITTED,
		"the chunk's lost acknowledgement after publication: {replay:?}"
	);
	read_multiset_everywhere(&cluster, &descriptor, &ROWS, "after the replay").await;
}

/// D6 admission: the route is tenant-bound; an owner command with a wrong
/// secret is refused at the leader without an entry; a follower answers with the
/// leader hint; a Today-shaped saga request at the ParentDecisions authority is
/// refused, typed, before any proposal and again at apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ctas_commands_are_tenant_bound_owner_checked_and_typed_by_mode() {
	let (cluster, leader, _) = ready().await;
	let descriptor = target(0xC9, "ctas_admission");
	let op = begun(&cluster, leader, &descriptor).await;

	// Another tenant's caller is refused before anything is read or proposed.
	let foreign = ActorIdentity::new(
		crate::network::tls::ActorRole::DataNode,
		Some(crate::schema::NodeId {
			id: leader,
			_reserved: 0,
		}),
		Some("7".to_owned()),
		vec!["cluster".to_owned()],
		b"adr0120-foreign".to_vec(),
	)
	.unwrap();
	let context = RequestContext {
		remote: Some(crate::schema::NodeId {
			id: leader,
			_reserved: 0,
		}),
		actor: foreign,
	};
	let error = ctas_command_as(
		&cluster,
		leader,
		context,
		&begin_request(&descriptor, uuid_v7()),
	)
	.await
	.expect_err("another tenant's caller");
	assert_eq!(
		error.kind(),
		std::io::ErrorKind::PermissionDenied,
		"{error}"
	);

	// Owner commands with a wrong secret: no entry at all.
	let before = last_index(&cluster, leader).await;
	for proposal in [
		CtasProposal::Renew {
			tenant: TenantId::ZERO,
			op,
			epoch: 1,
			secret: WRONG_SECRET,
		},
		CtasProposal::Abandon {
			tenant: TenantId::ZERO,
			op,
			epoch: 1,
			secret: WRONG_SECRET,
			cause: CtasAbandonCause::ChunkInadmissible,
		},
		CtasProposal::Renew {
			tenant: TenantId::ZERO,
			op: uuid_v7(),
			epoch: 1,
			secret: SECRET,
		},
	] {
		assert_eq!(
			ctas_command(&cluster, leader, &proposal).await,
			ProposalResponse::Refused(ProposalRefusal::CtasNotOwner),
			"{proposal:?}"
		);
	}
	assert_eq!(
		last_index(&cluster, leader).await,
		before,
		"a refused owner command appended"
	);

	// A follower answers with the leader hint and proposes nothing.
	let follower = NODES.into_iter().find(|node| *node != leader).unwrap();
	assert_eq!(
		ctas_command_as(
			&cluster,
			follower,
			node_context(follower),
			&CtasProposal::Renew {
				tenant: TenantId::ZERO,
				op,
				epoch: 1,
				secret: SECRET,
			},
		)
		.await
		.unwrap(),
		ProposalResponse::NotLeader {
			leader_hint: Some(leader)
		}
	);

	// The owner's renewal is admitted with the gated stamp.
	assert!(matches!(
		decided(
			ctas_command(
				&cluster,
				leader,
				&CtasProposal::Renew {
					tenant: TenantId::ZERO,
					op,
					epoch: 1,
					secret: SECRET,
				},
			)
			.await,
			"Renew"
		),
		DecisionResult::CtasRenewed { .. }
	));

	// A caller with a stale (Today) view sends a saga request: refused, typed,
	// before any proposal, at every member; at apply too.
	let saga =
		CtasSagaRequestWire::Mutate(crate::schema::CtasSagaMutation::MarkChainPinsReleased {
			tenant_id: TenantId::ZERO,
			operation_id: crate::schema::CtasOperationId::new(op),
			owner_epoch: 1,
		});
	let before = last_index(&cluster, leader).await;
	for node in NODES {
		let response = cluster
			.node(node)
			.router
			.handle_frame(
				node_context(node),
				frame(
					MsgType::CTAS_SAGA_REQ,
					crate::network::wire::encode(&saga).unwrap(),
					None,
				),
			)
			.await
			.unwrap()
			.expect("a response frame");
		let response: CtasSagaResponseWire = crate::network::wire::decode(&response.meta).unwrap();
		assert_eq!(
			response.response,
			crate::schema::CtasSagaResponse::Rejected(
				crate::schema::CtasSagaReject::WrongDecisionMode
			),
			"at {node}"
		);
	}
	assert_eq!(
		last_index(&cluster, leader).await,
		before,
		"a wrong-mode request appended"
	);
	let CtasSagaRequestWire::Mutate(mutation) = saga else {
		unreachable!()
	};
	let applied = cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::CtasSaga(mutation))
		.await
		.unwrap()
		.data;
	assert_eq!(
		applied,
		crate::consensus::WendelonResponse::Error(format!(
			"{}{}",
			crate::schema::CTAS_SAGA_REJECT_PREFIX,
			crate::schema::CtasSagaReject::WrongDecisionMode.code()
		))
	);
}

/// A chain begun at the leader whose one chunk commits, then abandoned by its
/// owner: its object is retired and its descriptor retained, privately. Its id.
pub(super) async fn retained_chunk(cluster: &Cluster) -> TableObjectId {
	let leader = cluster.leader().await;
	let descriptor = target(0xDA, "ctas_retained");
	let op = begun(cluster, leader, &descriptor).await;
	assert_committed(
		&chunk_through(
			cluster,
			leader,
			chunk_meta(&descriptor, op),
			&named_rows(&descriptor, &ROWS),
			op,
			SECRET,
		)
		.await,
		"the retained chunk",
	);
	cluster
		.until("the chunk's evidence", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.ctas().chain(TenantId::ZERO, op))
				.is_some_and(|chain| chain.accepted == 1)
		})
		.await;
	assert!(matches!(
		decided(
			ctas_command(
				cluster,
				leader,
				&CtasProposal::Abandon {
					tenant: TenantId::ZERO,
					op,
					epoch: 1,
					secret: SECRET,
					cause: CtasAbandonCause::ChunkInadmissible,
				},
			)
			.await,
			"Abandon"
		),
		DecisionResult::CtasAbandoned { .. }
	));
	// The owner's credential no longer dispatches once the chain left Dispatching.
	let mut late = chunk_meta(&descriptor, op);
	late.parent_write_id.sequence_no += 1;
	let after = chunk_through(
		cluster,
		leader,
		late,
		&named_rows(&descriptor, &ROWS),
		op,
		SECRET,
	)
	.await;
	assert_eq!(
		(after.write.status, after.write.mutation_commit_knowledge()),
		(
			WriteStatus::WRITE_REFUSED,
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED
		),
		"a chunk after Abandon: {after:?}"
	);
	descriptor.table_object_id
}

/// The other direction of the mode boundary: a Today authority answers a CTAS
/// decision request `Refused(ModeToday)`, typed, before any proposal, and does
/// not serve the chunk request; nothing is appended.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_today_authority_refuses_the_ctas_decision_route_before_proposal() {
	const NODE: u32 = 77;
	let dir = crate::test_support::TestDir::new("wendelon-adr0120-today");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		NODE,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while node.raft.current_leader().await != Some(NODE) {
		assert!(tokio::time::Instant::now() < deadline, "no leader");
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
	let descriptor = target(0xCA, "ctas_today");
	let op = uuid_v7();
	let index = || node.raft.metrics().borrow().last_log_index.unwrap_or(0);
	let before = index();

	let response = node
		.router
		.handle_frame(
			node_context(NODE),
			frame(
				MsgType::CTAS_DECISION_REQ,
				crate::network::wire::encode(&begin_request(&descriptor, op)).unwrap(),
				None,
			),
		)
		.await
		.unwrap()
		.expect("a response frame");
	assert_eq!(
		crate::network::wire::decode::<CtasDecisionResponseWire>(&response.meta)
			.unwrap()
			.response,
		ProposalResponse::Refused(ProposalRefusal::ModeToday)
	);

	let write = chunk_stream_into(
		node.service.clone(),
		node.pool.clone(),
		NODE,
		chunk_meta(&descriptor, op),
		&named_rows(&descriptor, &ROWS),
		op,
		SECRET,
	)
	.await;
	let error = write
		.handler
		.await
		.unwrap()
		.expect_err("a Today authority serves no CTAS chunk");
	assert_eq!(error.kind(), std::io::ErrorKind::Unsupported, "{error}");
	// The node may still append its own startup entries; none is a CTAS one.
	let after = index();
	if after > before {
		use openraft::storage::RaftLogReader;
		let mut reader = node.log_store.clone();
		for entry in reader
			.try_get_log_entries(before + 1..=after)
			.await
			.unwrap()
		{
			if let openraft::EntryPayload::Normal(request) = &entry.payload {
				assert!(
					!matches!(
						request,
						crate::consensus::WendelonRequest::Decision(_)
							| crate::consensus::WendelonRequest::CtasSaga(_)
					),
					"a Today refusal appended {request:?}"
				);
			}
		}
	}
}

/// Pauses every node's CTAS maintenance (a control drives the chain itself).
pub(super) fn pause_ctas_maintenance(cluster: &Cluster) {
	for node in NODES {
		cluster
			.node(node)
			.data_server
			.pause_ctas_maintenance_for_test(true);
	}
}

/// The owner's Publish through the route, as the given operation.
async fn publish_as_owner(cluster: &Cluster, leader: u32, op: [u8; 16]) {
	assert!(matches!(
		decided(
			ctas_command(
				cluster,
				leader,
				&CtasProposal::Publish {
					tenant: TenantId::ZERO,
					op,
					epoch: 1,
					secret: SECRET,
				},
			)
			.await,
			"Publish"
		),
		DecisionResult::CtasPublished { .. }
	));
}

/// Codex 01a1fad7 §1: a chunk outside the declaration never crosses publication
/// into an ordinary write. The chain declares no chunk; the owner's chunk at
/// sequence 1 is dispatched and, if it reaches the coordinator's pause after the
/// absent lookup, the chain is published before it resumes. Either way it is
/// refused, nothing is recorded under its key, and the published table holds
/// exactly its declared (empty) multiset; a later ordinary INSERT succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_undeclared_chunk_never_crosses_publication() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, _) = ready().await;
	let descriptor = target(0xCC, "ctas_crossing");
	let op = uuid_v7();
	assert!(matches!(
		decided(
			ctas_command(&cluster, leader, &begin_chunks(&descriptor, op, &[])).await,
			"Begin"
		),
		DecisionResult::CtasBegun { .. }
	));
	let meta = chunk_meta_at(&descriptor, op, 1);
	let batch = named_rows(&descriptor, &ROWS);
	let (releases, mut reached) = arm_everywhere(&cluster, PauseStage::AfterAbsent);
	let chunk = chunk_through(&cluster, leader, meta, &batch, op, SECRET);
	tokio::pin!(chunk);
	let (response, crossed) = tokio::select! {
		response = &mut chunk => (response, false),
		Some(node) = reached.recv() => {
			eprintln!("ADR0120-S2r2 crossing: the chunk paused at {node}; publishing");
			publish_as_owner(&cluster, leader, op).await;
			release_all(&releases);
			(chunk.await, true)
		}
	};
	release_all(&releases);
	eprintln!(
		"ADR0120-S2r2 crossing: crossed {crossed}, status {:?} knowledge {:?}",
		response.write.status,
		response.write.mutation_commit_knowledge()
	);
	assert_eq!(
		(
			response.write.status,
			response.write.mutation_commit_knowledge()
		),
		(
			WriteStatus::WRITE_REFUSED,
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED
		),
		"the undeclared chunk: {response:?}"
	);
	assert!(
		record_of(&cluster, key_of(&descriptor, &meta))
			.await
			.is_none()
	);
	if !crossed {
		publish_as_owner(&cluster, leader, op).await;
	}
	read_multiset_everywhere(&cluster, &descriptor, &[], "the published empty table").await;
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(9, "ordinary")]),
		)
		.await,
		"an ordinary INSERT after publication",
	);
	read_multiset_everywhere(
		&cluster,
		&descriptor,
		&[(9, "ordinary")],
		"after the INSERT",
	)
	.await;
}

/// Codex 01a1fad7 §1: a declared chunk whose chain is abandoned while it waits
/// after the absent lookup is refused with nothing recorded; the committed
/// chunk's lost acknowledgement replays its committed outcome after the
/// abandonment, through the same owner credential.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_new_chunk_crossing_abandonment_is_refused_and_a_committed_one_replays() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, _) = ready().await;
	let descriptor = target(0xCD, "ctas_abandoned_crossing");
	let op = uuid_v7();
	assert!(matches!(
		decided(
			ctas_command(
				&cluster,
				leader,
				&begin_chunks(&descriptor, op, &[&ROWS, &OTHER_ROWS])
			)
			.await,
			"Begin"
		),
		DecisionResult::CtasBegun { .. }
	));
	let first = chunk_meta_at(&descriptor, op, 1);
	let rows = named_rows(&descriptor, &ROWS);
	assert_committed(
		&chunk_through(&cluster, leader, first, &rows, op, SECRET).await,
		"the first chunk",
	);
	let second = chunk_meta_at(&descriptor, op, 2);
	let logged_before = last_index(&cluster, cluster.leader().await).await;
	let (releases, mut reached) = arm_everywhere(&cluster, PauseStage::AfterAbsent);
	let other_rows = named_rows(&descriptor, &OTHER_ROWS);
	let chunk = chunk_through(&cluster, leader, second, &other_rows, op, SECRET);
	tokio::pin!(chunk);
	let response = tokio::select! {
		response = &mut chunk => panic!("the second chunk did not reach the pause: {response:?}"),
		Some(_) = reached.recv() => {
			assert!(matches!(
				decided(
					ctas_command(
						&cluster,
						leader,
						&CtasProposal::Abandon {
							tenant: TenantId::ZERO,
							op,
							epoch: 1,
							secret: SECRET,
							cause: CtasAbandonCause::ChunkInadmissible,
						},
					)
					.await,
					"Abandon"
				),
				DecisionResult::CtasAbandoned { .. }
			));
			release_all(&releases);
			chunk.await
		}
	};
	release_all(&releases);
	assert_eq!(
		(
			response.write.status,
			response.write.mutation_commit_knowledge()
		),
		(
			WriteStatus::WRITE_REFUSED,
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED
		),
		"the second chunk after abandonment: {response:?}"
	);
	assert!(
		record_of(&cluster, key_of(&descriptor, &second))
			.await
			.is_none()
	);
	// The coordinator refused it itself: no Reserve for its key was proposed.
	assert_eq!(
		reserves_appended(&cluster, key_of(&descriptor, &second), logged_before).await,
		0,
		"a Reserve was proposed for a chunk of an abandoned chain"
	);
	let replay = chunk_through(&cluster, leader, first, &rows, op, SECRET).await;
	assert!(
		matches!(
			replay.write.status,
			WriteStatus::OK | WriteStatus::DUPLICATE
		) && replay.write.mutation_commit_knowledge() == MutationCommitKnowledge::COMMITTED,
		"the first chunk's lost acknowledgement: {replay:?}"
	);
}

/// Releases every armed pause when dropped, so no failure path leaves one held.
struct ReleaseAllOnDrop(Vec<Arc<tokio::sync::Notify>>);
impl Drop for ReleaseAllOnDrop {
	fn drop(&mut self) {
		release_all(&self.0);
	}
}

/// Codex 01a1fad7 §3: a CTAS chunk on its own stream, held after its Reserve,
/// answers its caller's deadline while it is still held, as a table write does;
/// released, the same attempt completes its reservation, the chain's evidence is
/// set, and the same key then answers its commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_chunk_held_after_its_reserve_answers_its_deadline_on_its_stream() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, _) = ready().await;
	let descriptor = target(0xCE, "ctas_deadline");
	let op = begun(&cluster, leader, &descriptor).await;
	let batch = named_rows(&descriptor, &ROWS);
	let mut meta = chunk_meta(&descriptor, op);
	meta.deadline_remaining_millis = 300;
	let key = key_of(&descriptor, &meta);
	let (releases, mut reached) = arm_everywhere(&cluster, PauseStage::AfterReserve);
	let held = ReleaseAllOnDrop(releases);

	// Into the coordinator: the entry, or the node its redirect names.
	let mut entry = leader;
	let (mut write, coordinator) = loop {
		let mut write = chunk_stream(&cluster, entry, meta, &batch, op, SECRET).await;
		let pool = cluster.node(entry).pool.clone();
		tokio::select! {
			Some(node) = reached.recv() => break (write, node),
			response = write.response(&pool) => {
				let response = response.unwrap();
				assert_eq!(response.write.status, WriteStatus::NOT_LEADER, "{response:?}");
				entry = response.write.leader_hint.id;
			}
		}
	};
	assert_eq!(coordinator, entry, "held at the node the stream entered");
	let pool = cluster.node(entry).pool.clone();
	let answer = tokio::time::timeout(std::time::Duration::from_secs(5), write.response(&pool))
		.await
		.expect("the deadline answer arrives while the chunk is held")
		.unwrap();
	assert_eq!(
		(
			answer.write.status,
			answer.write.mutation_commit_knowledge()
		),
		(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationCommitKnowledge::INDETERMINATE
		),
		"{answer:?}"
	);
	assert!(
		!write.handler.is_finished(),
		"the handler still owns the held coordination"
	);
	assert_eq!(
		record_of(&cluster, key).await.map(|record| record.status),
		Some(ParentStatus::Reserved)
	);

	drop(held);
	tokio::time::timeout(BOUND, &mut write.handler)
		.await
		.expect("the handler ends once released")
		.unwrap()
		.unwrap();
	assert!(matches!(
		record_of(&cluster, key).await.map(|record| record.status),
		Some(ParentStatus::Accepted { .. })
	));
	cluster
		.until("the chunk's evidence", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.ctas().chain(TenantId::ZERO, op))
				.is_some_and(|chain| chain.accepted == 1)
		})
		.await;
	let again = chunk_through(&cluster, entry, meta, &batch, op, SECRET).await;
	assert!(
		matches!(again.write.status, WriteStatus::OK | WriteStatus::DUPLICATE)
			&& again.write.mutation_commit_knowledge() == MutationCommitKnowledge::COMMITTED,
		"the same key after completion: {again:?}"
	);
}

/// Codex 01a1fad7 §2: a chain begun after the floor jumps, its operation issued
/// at the committed decision clock, whose one chunk commits and stays
/// uncertified and private (the chain still dispatches): an owed private chunk.
/// Its key.
pub(super) async fn owed_chunk(cluster: &Cluster) -> ParentKey {
	let leader = cluster.leader().await;
	let clock = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().clock_ms())
		.await;
	let descriptor = target(0xCF, "ctas_owed");
	let op = uuid_v7_at(clock);
	assert!(
		matches!(
			decided(
				ctas_command(cluster, leader, &begin_request(&descriptor, op)).await,
				"Begin"
			),
			DecisionResult::CtasBegun { .. }
		),
		"Begin after the floor jumps"
	);
	let meta = chunk_meta(&descriptor, op);
	assert_committed(
		&chunk_through(
			cluster,
			leader,
			meta,
			&named_rows(&descriptor, &ROWS),
			op,
			SECRET,
		)
		.await,
		"the owed chunk",
	);
	key_of(&descriptor, &meta)
}

/// Codex 01a1fad7 §5: an abandoned chain's chunk with physical copies (its
/// children written, then its parent concluded `Refused { CtasAbandoned }` by the
/// owner's Abandon before Finalize). At the next generation its versions are
/// retired-intent debt; certified cleanup reclaims every copy and every debt
/// record of that table through the retained (private) descriptor.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_abandoned_chunks_copies_are_reclaimed_by_certified_cleanup() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, _) = ready().await;
	let descriptor = target(0xD0, "ctas_reclaimed");
	let object = descriptor.table_object_id;
	let op = begun(&cluster, leader, &descriptor).await;
	let meta = chunk_meta(&descriptor, op);
	let key = key_of(&descriptor, &meta);
	let batch = named_rows(&descriptor, &ROWS);
	let (releases, mut reached) = arm_everywhere(&cluster, PauseStage::BeforeFinalize);
	let held = ReleaseAllOnDrop(releases);
	let chunk = chunk_through(&cluster, leader, meta, &batch, op, SECRET);
	tokio::pin!(chunk);
	let response = tokio::select! {
		response = &mut chunk => panic!("the chunk did not reach Finalize: {response:?}"),
		Some(_) = reached.recv() => {
			assert!(matches!(
				decided(
					ctas_command(
						&cluster,
						leader,
						&CtasProposal::Abandon {
							tenant: TenantId::ZERO,
							op,
							epoch: 1,
							secret: SECRET,
							cause: CtasAbandonCause::ChunkInadmissible,
						},
					)
					.await,
					"Abandon"
				),
				DecisionResult::CtasAbandoned { .. }
			));
			release_all(&held.0);
			chunk.await
		}
	};
	assert_ne!(response.write.status, WriteStatus::OK, "{response:?}");
	// The pauses the other nodes never reached would otherwise stay queued
	// ahead of the cleanup pauses below.
	for node in NODES {
		cluster.node(node).data_server.clear_pauses_for_test();
	}
	let record = record_of(&cluster, key)
		.await
		.expect("the abandoned chunk's record");
	assert!(
		matches!(
			record.status,
			ParentStatus::Refused {
				cause: crate::consensus::decisions::RefusalCause::CtasAbandoned,
				..
			}
		),
		"{record:?}"
	);
	let versions: Vec<_> = (0..ROWS.len() as u32)
		.map(|ordinal| super::append_only::identity_of(&meta, ordinal))
		.collect();
	let hlc = record.claim.commit_hlc;
	let copies = |node: u32| {
		let slab = cluster.node(node).slab.clone();
		let versions = versions.clone();
		move || {
			versions
				.iter()
				.map(|identity| {
					slab.version_locations(TenantId::ZERO, *identity, hlc)
						.unwrap()
						.len()
				})
				.sum::<usize>()
		}
	};
	// Read before the floor jump: maintenance may prune the record after it.
	assert!(
		chain_at(&cluster, cluster.leader().await, op)
			.await
			.is_some_and(|chain| (chain.accepted, chain.refused) == (0, 1)),
		"Abandon's closure set exactly the chunk's refused bit (D5)"
	);
	let before: usize = NODES.iter().map(|node| copies(*node)()).sum();
	assert!(before > 0, "the abandoned chunk left physical copies");
	// Cleanup rewrites sealed slabs, as the ordinary cleanup control seals them.
	for node in NODES {
		cluster.node(node).slab.force_seal_active_for_test().await;
	}
	// Each node's cleanup is held after its evidence (read through the private
	// descriptor) and before its rewrite: the debt is observed there.
	let (cleanups, mut cleaning) = arm_everywhere(&cluster, PauseStage::CleanupBeforeRewrite);
	let held_cleanups = ReleaseAllOnDrop(cleanups);

	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let table_debt = |node: u32| {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		async move {
			index
				.debt_page(None, 1_000)
				.await
				.unwrap()
				.into_iter()
				.filter(|(version, debt)| {
					version.table == object
						&& matches!(
							debt.source,
							crate::storage::index::certified_base::DebtSource::RetiredIntent { .. }
						)
				})
				.count()
		}
	};
	let mut held_at = std::collections::BTreeMap::new();
	while held_at.len() < NODES.len() {
		let node = tokio::time::timeout(BOUND, cleaning.recv())
			.await
			.expect("every node's cleanup reaches its rewrite")
			.unwrap();
		held_at.insert(node, table_debt(node).await);
	}
	assert!(
		held_at.values().all(|debt| *debt > 0),
		"retired-intent debt for the abandoned table on every node: {held_at:?}"
	);
	drop(held_cleanups);
	let deadline = tokio::time::Instant::now() + BOUND;
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		loop {
			let debt = index
				.debt_page(None, 1_000)
				.await
				.unwrap()
				.into_iter()
				.filter(|(version, _)| version.table == object)
				.count();
			let stored = copies(node)();
			if stored == 0 && debt == 0 {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"node {node}: {stored} abandoned copies and {debt} debt records remain"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}
	eprintln!(
		"ADR0120-S2r2 cleanup: {before} abandoned copies before; retired-intent debt held {held_at:?}; all reclaimed"
	);
}

/// The Reserve entries for `key` the leader's log appended after `from`.
async fn reserves_appended(cluster: &Cluster, key: ParentKey, from: u64) -> usize {
	use openraft::storage::RaftLogReader;
	let leader = cluster.leader().await;
	let to = last_index(cluster, leader).await;
	if to <= from {
		return 0;
	}
	let mut reader = cluster.node(leader).log_store.clone();
	reader
		.try_get_log_entries(from + 1..=to)
		.await
		.unwrap()
		.iter()
		.filter(|entry| {
			matches!(
				&entry.payload,
				openraft::EntryPayload::Normal(crate::consensus::WendelonRequest::Decision(
					crate::consensus::decisions::DecisionCommand::Reserve { claim, .. }
				)) if claim.body.key == key
			)
		})
		.count()
}

/// Codex 01a1fad7 §1, the deciding boundary in the cluster: a declared chunk
/// that passed its coordinator's phase check is held there while its chain is
/// abandoned. On release its one Reserve reaches the reducer, which refuses it
/// (`WrongPhase`), and the coordinator answers that refusal for good on the
/// first attempt: `WRITE_REFUSED`, known not committed, nothing recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_chunk_past_its_phase_check_meets_the_deciding_boundary() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, _) = ready().await;
	let descriptor = target(0xD1, "ctas_boundary");
	let op = begun(&cluster, leader, &descriptor).await;
	let meta = chunk_meta(&descriptor, op);
	let key = key_of(&descriptor, &meta);
	let batch = named_rows(&descriptor, &ROWS);
	let logged_before = last_index(&cluster, leader).await;
	let (releases, mut reached) = arm_everywhere(&cluster, PauseStage::AfterCtasPhase);
	let held = ReleaseAllOnDrop(releases);
	// One attempt into the coordinator: the entry, or the node its redirect names.
	let mut entry = leader;
	let mut write = loop {
		let mut write = chunk_stream(&cluster, entry, meta, &batch, op, SECRET).await;
		let pool = cluster.node(entry).pool.clone();
		tokio::select! {
			Some(_) = reached.recv() => break write,
			response = write.response(&pool) => {
				let response = response.unwrap();
				assert_eq!(response.write.status, WriteStatus::NOT_LEADER, "{response:?}");
				entry = response.write.leader_hint.id;
			}
		}
	};
	assert!(matches!(
		decided(
			ctas_command(
				&cluster,
				leader,
				&CtasProposal::Abandon {
					tenant: TenantId::ZERO,
					op,
					epoch: 1,
					secret: SECRET,
					cause: CtasAbandonCause::ChunkInadmissible,
				},
			)
			.await,
			"Abandon"
		),
		DecisionResult::CtasAbandoned { .. }
	));
	drop(held);
	let pool = cluster.node(entry).pool.clone();
	let response = tokio::time::timeout(BOUND, write.response(&pool))
		.await
		.expect("the held chunk answers")
		.unwrap();
	assert_eq!(
		(
			response.write.status,
			response.write.mutation_commit_knowledge()
		),
		(
			WriteStatus::WRITE_REFUSED,
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED
		),
		"the first attempt's answer: {response:?}"
	);
	assert!(record_of(&cluster, key).await.is_none(), "nothing recorded");
	assert_eq!(
		reserves_appended(&cluster, key, logged_before).await,
		1,
		"the one Reserve reached the reducer"
	);
	for node in NODES {
		cluster.node(node).data_server.clear_pauses_for_test();
	}
}

/// Codex 01a1fb24: a chunk A held after its absent lookup while another dispatch
/// B of the same key, digest and rows commits it and the chain is published.
/// Once A's coordinator has applied the Publish, A is released: it must answer
/// B's commit (the same log index and HLC), never a refusal, and the published
/// table holds exactly one copy of the declared multiset.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_held_chunk_whose_key_committed_meanwhile_answers_that_commit() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, _) = ready().await;
	let descriptor = target(0xD2, "ctas_same_key");
	let op = begun(&cluster, leader, &descriptor).await;
	let meta = chunk_meta(&descriptor, op);
	let batch = named_rows(&descriptor, &ROWS);
	let (releases, mut reached) = arm_everywhere(&cluster, PauseStage::AfterAbsent);
	let held = ReleaseAllOnDrop(releases);
	let first = chunk_through(&cluster, leader, meta, &batch, op, SECRET);
	tokio::pin!(first);
	let coordinator = tokio::select! {
		response = &mut first => panic!("A did not reach its pause: {response:?}"),
		Some(node) = reached.recv() => node,
	};
	// B: the same key, digest and rows, past every pause, commits.
	for node in NODES {
		cluster.node(node).data_server.clear_pauses_for_test();
	}
	let second = chunk_through(&cluster, leader, meta, &batch, op, SECRET).await;
	assert_committed(&second, "B");
	cluster
		.until("B's evidence", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.ctas().chain(TenantId::ZERO, op))
				.is_some_and(|chain| chain.accepted == 1)
		})
		.await;
	publish_as_owner(&cluster, leader, op).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	while !cluster
		.node(coordinator)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.ctas().chain(TenantId::ZERO, op))
				.is_some_and(|chain| {
					!matches!(
						chain.phase,
						crate::consensus::decisions::ctas::CtasPhase::Dispatching
					)
				})
		})
		.await
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"A's coordinator never applied the Publish"
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
	drop(held);
	let answer = first.await;
	eprintln!(
		"ADR0120-S2r3 same key: A {:?}/{:?} at {}, B at {}",
		answer.write.status,
		answer.write.mutation_commit_knowledge(),
		answer.write.log_index,
		second.write.log_index
	);
	assert!(
		matches!(
			answer.write.status,
			WriteStatus::OK | WriteStatus::DUPLICATE
		) && answer.write.mutation_commit_knowledge() == MutationCommitKnowledge::COMMITTED,
		"A answers the key's commit: {answer:?}"
	);
	assert_eq!(
		(answer.write.log_index, answer.write.commit_hlc),
		(second.write.log_index, second.write.commit_hlc),
		"A and B report the same commit"
	);
	read_multiset_everywhere(
		&cluster,
		&descriptor,
		&ROWS,
		"one copy of the declared rows",
	)
	.await;
}

fn resolved(response: ProposalResponse) -> (Option<CtasChain>, Option<u64>) {
	match response {
		ProposalResponse::CtasResolved {
			chain,
			visible_from,
			..
		} => (chain.map(|chain| *chain), visible_from),
		other => panic!("a Resolve: {other:?}"),
	}
}

/// Codex 01a1fad7 §4, 01a1fb24: Resolve, Recover and Conclude through the typed
/// route. Resolve answers no chain for a fresh operation, then (after Begin) the
/// chain with its statement fingerprint and the owner's commitment, before any
/// SELECT would run. Recover while the owner's lease is live is refused
/// `LeaseLive`; once it lapses (the real 60 s lease: no clock is moved), Recover
/// takes over, the old owner's Publish is refused, Conclude publishes the exact
/// multiset, and a repeated Conclude and a later Recover replay `Published`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn resolve_recover_and_conclude_through_the_typed_route() {
	let (cluster, leader, _) = ready().await;
	// The route itself drives Recover and Conclude here.
	pause_ctas_maintenance(&cluster);
	let descriptor = target(0xD3, "ctas_takeover");
	let op = uuid_v7();
	let resolve = CtasProposal::Resolve {
		tenant: TenantId::ZERO,
		op,
	};
	assert_eq!(
		resolved(ctas_command(&cluster, leader, &resolve).await),
		(None, None)
	);
	assert!(matches!(
		decided(
			ctas_command(&cluster, leader, &begin_request(&descriptor, op)).await,
			"Begin"
		),
		DecisionResult::CtasBegun { .. }
	));
	let (chain, visible_from) = resolved(ctas_command(&cluster, leader, &resolve).await);
	let chain = chain.expect("the begun chain");
	assert_eq!(chain.fingerprint, [0xf1; 32]);
	assert_eq!(
		chain.dispatch_hash,
		crate::consensus::decisions::ctas::ctas_owner_commitment(&SECRET)
	);
	assert_eq!(chain.phase, CtasPhase::Dispatching);
	assert_eq!(visible_from, None);
	assert_committed(
		&chunk_through(
			&cluster,
			leader,
			chunk_meta(&descriptor, op),
			&named_rows(&descriptor, &ROWS),
			op,
			SECRET,
		)
		.await,
		"the chunk",
	);

	let recover = |new_epoch| CtasProposal::Recover {
		tenant: TenantId::ZERO,
		op,
		new_epoch,
	};
	assert_eq!(
		decided(ctas_command(&cluster, leader, &recover(2)).await, "Recover"),
		DecisionResult::Refused(crate::consensus::decisions::DecisionRefusal::Ctas(
			crate::consensus::decisions::ctas::CtasRefusal::LeaseLive
		)),
		"a Recover while the owner's lease is live"
	);
	let lease_until = chain.lease_until_ms;
	loop {
		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		if now > lease_until + 1_000 {
			break;
		}
		tokio::time::sleep(std::time::Duration::from_millis(lease_until + 1_000 - now)).await;
	}
	assert!(matches!(
		decided(ctas_command(&cluster, leader, &recover(2)).await, "Recover"),
		DecisionResult::CtasRecovering { epoch: 2 }
	));
	assert!(
		matches!(
			decided(
				ctas_command(
					&cluster,
					leader,
					&CtasProposal::Publish {
						tenant: TenantId::ZERO,
						op,
						epoch: 1,
						secret: SECRET,
					},
				)
				.await,
				"Publish"
			),
			DecisionResult::Refused(_)
		),
		"the old owner's Publish after takeover"
	);
	let conclude = CtasProposal::Conclude {
		tenant: TenantId::ZERO,
		op,
		epoch: 2,
	};
	assert!(matches!(
		decided(ctas_command(&cluster, leader, &conclude).await, "Conclude"),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
	assert!(matches!(
		decided(
			ctas_command(&cluster, leader, &conclude).await,
			"Conclude again"
		),
		DecisionResult::CtasPublished { replayed: true, .. }
	));
	assert!(matches!(
		decided(
			ctas_command(&cluster, leader, &recover(3)).await,
			"Recover after"
		),
		DecisionResult::CtasPublished { replayed: true, .. }
	));
	read_multiset_everywhere(&cluster, &descriptor, &ROWS, "recovery's publication").await;
	let (chain, visible_from) = resolved(ctas_command(&cluster, leader, &resolve).await);
	assert!(matches!(
		chain.map(|chain| chain.phase),
		Some(CtasPhase::Published { .. })
	));
	assert!(visible_from.is_some());
}

/// Acceptance §10, the multi-chunk discriminator (in process: a public CTAS above
/// 65,536 rows exceeds today's read budget): three declared chunks, dispatched
/// in reverse order, publish exactly their combined multiset; before the last
/// chunk is accepted, Publish is refused (`ChunksIncomplete`) and nothing reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_chunks_publish_exactly_their_combined_multiset() {
	let (cluster, leader, _) = ready().await;
	let descriptor = target(0xD4, "ctas_chunks");
	let op = uuid_v7();
	let parts: [&[(i64, &str)]; 3] = [
		&[(1, "same"), (1, "same")],
		&[(2, "other"), (3, "x"), (3, "x")],
		&[(1, "same")],
	];
	assert!(matches!(
		decided(
			ctas_command(&cluster, leader, &begin_chunks(&descriptor, op, &parts)).await,
			"Begin"
		),
		DecisionResult::CtasBegun { .. }
	));
	let publish = CtasProposal::Publish {
		tenant: TenantId::ZERO,
		op,
		epoch: 1,
		secret: SECRET,
	};
	for at in [2usize, 1] {
		assert_committed(
			&chunk_through(
				&cluster,
				leader,
				chunk_meta_at(&descriptor, op, at as u64 + 1),
				&named_rows(&descriptor, parts[at]),
				op,
				SECRET,
			)
			.await,
			"a chunk",
		);
	}
	assert_eq!(
		decided(
			ctas_command(&cluster, leader, &publish).await,
			"early Publish"
		),
		DecisionResult::Refused(crate::consensus::decisions::DecisionRefusal::Ctas(
			crate::consensus::decisions::ctas::CtasRefusal::ChunksIncomplete
		))
	);
	assert_eq!(
		read_at(&cluster, leader, &descriptor).await.status,
		Some(ReadStatus::QUERY_INVALID_PROGRAM)
	);
	assert_committed(
		&chunk_through(
			&cluster,
			leader,
			chunk_meta_at(&descriptor, op, 1),
			&named_rows(&descriptor, parts[0]),
			op,
			SECRET,
		)
		.await,
		"the first chunk",
	);
	cluster
		.until("every chunk's evidence", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.ctas().chain(TenantId::ZERO, op))
				.is_some_and(|chain| chain.accepted == 0b111)
		})
		.await;
	assert!(matches!(
		decided(ctas_command(&cluster, leader, &publish).await, "Publish"),
		DecisionResult::CtasPublished { .. }
	));
	let combined: Vec<(i64, &str)> = parts.iter().flat_map(|part| part.iter().copied()).collect();
	read_multiset_everywhere(&cluster, &descriptor, &combined, "the combined multiset").await;
}

/// ADR 0120 D9 (S4): leader maintenance with no client traffic and no owner.
/// Two owners stop: chain A (one chunk, committed) and chain B (two chunks, the
/// second never dispatched). Once the committed clock passes their leases (a
/// floor jump), maintenance recovers both; A concludes Published with its exact
/// multiset, B concludes Abandoned (`SourceBytesLost`) and reads as no table.
/// Once their retention passes and the floor covers their operations,
/// maintenance prunes both records; A stays visible and exact, B stays retired.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn maintenance_recovers_concludes_and_prunes_chains_whose_owner_stopped() {
	use crate::consensus::decisions::ctas::CTAS_TERMINAL_RETENTION_MS;
	let (cluster, leader, _) = ready().await;
	let a = target(0xD5, "ctas_owner_gone");
	let op_a = begun(&cluster, leader, &a).await;
	assert_committed(
		&chunk_through(
			&cluster,
			leader,
			chunk_meta(&a, op_a),
			&named_rows(&a, &ROWS),
			op_a,
			SECRET,
		)
		.await,
		"A's chunk",
	);
	let b = target(0xD6, "ctas_source_lost");
	let op_b = uuid_v7();
	assert!(matches!(
		decided(
			ctas_command(
				&cluster,
				leader,
				&begin_chunks(&b, op_b, &[&ROWS, &OTHER_ROWS])
			)
			.await,
			"Begin B"
		),
		DecisionResult::CtasBegun { .. }
	));
	assert_committed(
		&chunk_through(
			&cluster,
			leader,
			chunk_meta_at(&b, op_b, 1),
			&named_rows(&b, &ROWS),
			op_b,
			SECRET,
		)
		.await,
		"B's first chunk",
	);

	// The owners stop; the committed clock passes their leases.
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(&cluster, now + 60_000).await;
	cluster
		.until("maintenance concluded both chains", move |state| {
			let Some(decisions) = state.decisions.as_ref() else {
				return false;
			};
			let ctas = decisions.ctas();
			matches!(
				ctas.chain(TenantId::ZERO, op_a).map(|chain| chain.phase),
				Some(CtasPhase::Published { .. })
			) && matches!(
				ctas.chain(TenantId::ZERO, op_b).map(|chain| chain.phase),
				Some(CtasPhase::Abandoned {
					cause: CtasAbandonCause::SourceBytesLost,
					..
				})
			)
		})
		.await;
	read_multiset_everywhere(&cluster, &a, &ROWS, "A, published by recovery").await;
	// Codex 01a1fc48 C: the chain is present before its prune.
	assert!(
		matches!(
			ctas_command(
				&cluster,
				leader,
				&CtasProposal::Resolve {
					tenant: TenantId::ZERO,
					op: op_a
				}
			)
			.await,
			ProposalResponse::CtasResolved { chain: Some(_), .. }
		),
		"A's chain before the prune"
	);
	assert_eq!(
		read_at(&cluster, leader, &b).await.status,
		Some(ReadStatus::QUERY_INVALID_PROGRAM),
		"B is no table"
	);

	// Past the retention, the floor covering both operations: both pruned.
	jump_floor(&cluster, now + 60_000 + CTAS_TERMINAL_RETENTION_MS + 1).await;
	let (object_a, object_b) = (a.table_object_id, b.table_object_id);
	cluster
		.until("maintenance pruned both records", move |state| {
			state.decisions.as_ref().is_some_and(|decisions| {
				let ctas = decisions.ctas();
				ctas.chain(TenantId::ZERO, op_a).is_none()
					&& ctas.chain(TenantId::ZERO, op_b).is_none()
					&& ctas.visible_from(TenantId::ZERO, object_a).is_some()
					&& ctas.retired(TenantId::ZERO, object_b).is_some()
			})
		})
		.await;
	read_multiset_everywhere(&cluster, &a, &ROWS, "A, after the prune").await;
	// Codex 01a1fc48 C: after the real prune (the applied floor covers both ids), each
	// chain reads absent, and the ordinary public resolution of the original id at
	// sequence zero (Toron's identity for it) is Expired through the real routes.
	let leader = cluster.leader().await;
	let client = super::backup_restore::client_advertised(
		&cluster,
		// A non-member data node with no tenant claim, as the profile's Toron storage
		// identity is: the transport's own id.
		ActorIdentity::data_node(crate::schema::NodeId {
			id: 99,
			_reserved: 0,
		}),
		crate::network::transport::NodeCapabilitySet::empty()
			.with(crate::network::transport::NodeCapability::ParentDecisions)
			.with(crate::network::transport::NodeCapability::ParentDecisionsBatchedLeases)
			.as_u8(),
	)
	.await;
	for (op, what) in [(op_a, "A"), (op_b, "B")] {
		let resolved = ctas_command(
			&cluster,
			leader,
			&CtasProposal::Resolve {
				tenant: TenantId::ZERO,
				op,
			},
		)
		.await;
		assert!(
			matches!(resolved, ProposalResponse::CtasResolved { chain: None, .. }),
			"{what}'s chain after its prune: {resolved:?}"
		);
		let outcome = client
			.resolve_mutation_outcome(crate::client::core::MutationOutcomeRequest {
				tenant_id: TenantId::ZERO,
				operation_id: crate::schema::ClientWriteId {
					client_uuid: op,
					sequence_no: 0,
					idempotency_ttl_ms: 300_000,
					_pad: 0,
				},
			})
			.await
			.expect("an outcome");
		assert_eq!(
			outcome.response.resolution,
			crate::schema::MutationResolutionStatus::EXPIRED,
			"{what}: {:?}",
			outcome.response
		);
	}
}

/// A chain begun at the leader whose one chunk commits and which keeps
/// dispatching (its owner alive, maintenance paused by the caller): a private,
/// provisional object with certifiable rows. Its id.
pub(super) async fn dispatching_chunk(cluster: &Cluster) -> TableObjectId {
	let leader = cluster.leader().await;
	let descriptor = target(0xCB, "ctas_dispatching");
	let op = begun(cluster, leader, &descriptor).await;
	assert_committed(
		&chunk_through(
			cluster,
			leader,
			chunk_meta(&descriptor, op),
			&named_rows(&descriptor, &ROWS),
			op,
			SECRET,
		)
		.await,
		"the dispatching chunk",
	);
	descriptor.table_object_id
}

/// ADR 0120 D9 (S4), floor-safe exclusion. An ordinary table U and a CTAS chunk
/// are certified into generation 1 while the chain dispatches. The owner then
/// abandons it: the retirement alone (no client traffic, no new rows) triggers
/// an excluding generation 2, whose base holds none of the abandoned object's
/// entries at any node, while U's entries are unchanged and U reads exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_abandoned_objects_rows_leave_the_next_generation_and_survivors_stay() {
	let (cluster, leader, ordinary) = ready().await;
	pause_ctas_maintenance(&cluster);
	let survivors = [(1_i64, "u1"), (2, "u2")];
	for (id, value) in survivors {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&ordinary, 1, uuid_v7()),
				named_rows(&ordinary, &[(id, value)]),
			)
			.await,
			"a survivor",
		);
	}
	let descriptor = target(0xDB, "ctas_excluded");
	let object = descriptor.table_object_id;
	let op = begun(&cluster, leader, &descriptor).await;
	assert_committed(
		&chunk_through(
			&cluster,
			leader,
			chunk_meta(&descriptor, op),
			&named_rows(&descriptor, &ROWS),
			op,
			SECRET,
		)
		.await,
		"the chunk",
	);
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let base_of = |node: u32| {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		async move { index.base_entries(false).await.unwrap() }
	};
	let mut ordinary_before = std::collections::BTreeMap::new();
	for node in NODES {
		let entries = base_of(node).await;
		assert!(
			entries.iter().any(|entry| entry.table == object),
			"node {node}: the dispatching chunk is certified into generation 1"
		);
		ordinary_before.insert(
			node,
			entries
				.into_iter()
				.filter(|entry| entry.table == ordinary.table_object_id)
				.collect::<Vec<_>>(),
		);
	}
	let leader = cluster.leader().await;
	assert!(matches!(
		decided(
			ctas_command(
				&cluster,
				leader,
				&CtasProposal::Abandon {
					tenant: TenantId::ZERO,
					op,
					epoch: 1,
					secret: SECRET,
					cause: CtasAbandonCause::ChunkInadmissible,
				},
			)
			.await,
			"Abandon"
		),
		DecisionResult::CtasAbandoned { .. }
	));
	// The retirement alone triggers the excluding generation.
	until_active(&cluster, 2).await;
	let policy = cluster
		.node(cluster.leader().await)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.generations().current.clone())
				// The policy its cut requires, which its target had to carry.
				.map(|certificate| state.pd_survivor_policy_at(certificate.t))
		})
		.await;
	assert_eq!(
		policy,
		Some(crate::consensus::decisions::generations::SURVIVOR_POLICY_EXCLUDING)
	);
	for node in NODES {
		let entries = base_of(node).await;
		assert!(
			!entries.iter().any(|entry| entry.table == object),
			"node {node}: the abandoned object's rows left the base"
		);
		let ordinary_after: Vec<_> = entries
			.into_iter()
			.filter(|entry| entry.table == ordinary.table_object_id)
			.collect();
		assert_eq!(
			ordinary_after, ordinary_before[&node],
			"node {node}: the survivors are unchanged"
		);
	}
	expect_everywhere(
		&cluster,
		&ordinary,
		&survivors
			.iter()
			.map(|(id, value)| (*id, (*value).to_owned()))
			.collect(),
		"the survivors",
	)
	.await;
}

/// The backup roster at the leader under the hold `token`.
async fn roster_under(
	cluster: &Cluster,
	token: (u128, u64),
) -> Result<crate::logical_backup::roster::BackupRoster, crate::logical_backup::roster::RosterRefusal>
{
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| crate::logical_backup::roster::roster_at(state, token))
		.await
}

/// ADR 0120 D9 (S4): the backup roster. Under a hold, a chain still dispatching
/// is refused now (`LifecycleNotSettled`: its publication is a binding the hold
/// blocks); once it is published, the roster lists its table as an ordinary
/// table; an abandoned object is never listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_backup_roster_refuses_an_unsettled_chain_and_lists_only_published_tables() {
	let (cluster, leader, _) = ready().await;
	pause_ctas_maintenance(&cluster);
	let published = target(0xDC, "ctas_backed_up");
	let op = begun(&cluster, leader, &published).await;
	assert_committed(
		&chunk_through(
			&cluster,
			leader,
			chunk_meta(&published, op),
			&named_rows(&published, &ROWS),
			op,
			SECRET,
		)
		.await,
		"the chunk",
	);
	let hold = begin_hold_through_raft(&cluster, 0xC7A5).await;
	let refused = roster_under(&cluster, hold).await;
	assert!(
		matches!(
			&refused,
			Err(crate::logical_backup::roster::RosterRefusal::LifecycleNotSettled { what })
				if what.contains("create-table-as-select")
		),
		"{refused:?}"
	);
	release_hold_through_raft(&cluster, hold).await;
	publish_as_owner(&cluster, leader, op).await;
	let abandoned = retained_chunk(&cluster).await;
	let hold = begin_hold_through_raft(&cluster, 0xC7A6).await;
	let roster = roster_under(&cluster, hold)
		.await
		.unwrap_or_else(|refusal| panic!("the settled roster: {refusal:?}"));
	let listed: Vec<_> = roster
		.tables
		.iter()
		.map(|table| table.descriptor.table_object_id)
		.collect();
	assert!(listed.contains(&published.table_object_id), "{listed:?}");
	assert!(!listed.contains(&abandoned), "{listed:?}");
	release_hold_through_raft(&cluster, hold).await;
}
