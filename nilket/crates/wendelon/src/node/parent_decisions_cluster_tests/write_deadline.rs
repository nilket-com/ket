//! A coordinated table write and its caller's deadline, through the stream
//! entry an inbound request takes: the node's published service (the
//! lifecycle wrapper, the router and the data server's handler), fed one
//! request frame on a stream and read for its one response frame.

use super::*;
use crate::network::data_server::child_writes::PauseStage;
use crate::network::transport::TransportStream;
use crate::schema::{MutationStopReason, TableWriteResponseMeta};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The deadline a held write names.
const DEADLINE_MS: u64 = 300;
/// How long after sending a held write's answer may take: its deadline plus
/// scheduling under suite load.
const ANSWER_BOUND: std::time::Duration = std::time::Duration::from_secs(5);

/// One end of an in-memory stream. With `writes_fail` every write and the
/// finish fail, as towards a peer that has gone; reads are unaffected.
pub(super) struct PipeStream {
	inner: tokio::io::DuplexStream,
	writes_fail: bool,
}

fn gone() -> std::io::Error {
	std::io::Error::new(std::io::ErrorKind::BrokenPipe, "the peer has gone")
}

impl AsyncRead for PipeStream {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_read(cx, buf)
	}
}

impl AsyncWrite for PipeStream {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		if self.writes_fail {
			return Poll::Ready(Err(gone()));
		}
		Pin::new(&mut self.inner).poll_write(cx, buf)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_flush(cx)
	}

	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		if self.writes_fail {
			return Poll::Ready(Err(gone()));
		}
		Pin::new(&mut self.inner).poll_shutdown(cx)
	}
}

#[async_trait::async_trait]
impl TransportStream for PipeStream {
	fn stream_id(&self) -> u64 {
		0
	}

	async fn finish(&mut self) -> std::io::Result<()> {
		use tokio::io::AsyncWriteExt;
		self.shutdown().await
	}
}

/// The request frame of one table write, as a client sends it.
async fn send_request(client: &mut PipeStream, meta: &TableWriteRequestMeta, batch: &RecordBatch) {
	use tokio::io::AsyncWriteExt;
	let arrow = crate::client::encode_table_write_arrow_section(batch).unwrap();
	let header = crate::schema::FrameHeader {
		magic: crate::network::core::HRFT_MAGIC,
		version: 1,
		msg_type: crate::schema::MsgType::TABLE_WRITE_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: std::mem::size_of::<TableWriteRequestMeta>() as u32,
		arrow_len: arrow.len() as u32,
		trailing_len: 0,
	};
	crate::network::core::write_frame(
		client,
		&header,
		bytemuck::bytes_of(meta),
		Some(&arrow),
		None,
	)
	.await
	.unwrap();
	client.shutdown().await.unwrap();
}

/// The two ends of a request stream: the client's, with the request already
/// written and finished, and the server's.
async fn request_stream(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	responses_fail: bool,
) -> (PipeStream, PipeStream) {
	let (client, server) = tokio::io::duplex(64 * 1024);
	let mut client = PipeStream {
		inner: client,
		writes_fail: false,
	};
	send_request(&mut client, meta, batch).await;
	(
		client,
		PipeStream {
			inner: server,
			writes_fail: responses_fail,
		},
	)
}

/// A node other than `node`, as the authenticated sender of a request.
fn sender_to(node: u32) -> crate::network::server::RequestContext {
	let sender = crate::schema::NodeId {
		id: NODES.into_iter().find(|other| *other != node).unwrap(),
		_reserved: 0,
	};
	crate::network::server::RequestContext {
		remote: Some(sender),
		actor: crate::network::tls::ActorIdentity::data_node(sender),
	}
}

/// One table write on a stream into `node`'s published service: the client's
/// end, and the task that runs the service's handling of the stream.
pub(super) struct StreamedWrite {
	pub(super) client: PipeStream,
	pub(super) handler: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl StreamedWrite {
	/// Sends `meta` and `batch` to `node`. With `responses_fail` the server's
	/// end cannot write.
	pub(super) async fn send(
		cluster: &Cluster,
		node: u32,
		meta: TableWriteRequestMeta,
		batch: RecordBatch,
		responses_fail: bool,
	) -> Self {
		Self::send_as(cluster, node, meta, batch, responses_fail, sender_to(node)).await
	}

	/// `send`, as the actor of `context`.
	async fn send_as(
		cluster: &Cluster,
		node: u32,
		meta: TableWriteRequestMeta,
		batch: RecordBatch,
		responses_fail: bool,
		context: crate::network::server::RequestContext,
	) -> Self {
		let (client, server) = request_stream(&meta, &batch, responses_fail).await;
		let service = cluster.node(node).service.clone();
		let pool = cluster.node(node).pool.clone();
		let handler =
			tokio::spawn(
				async move { service.handle_stream(Box::new(server), pool, context).await },
			);
		Self { client, handler }
	}

	/// The one response frame, then the end of the stream.
	pub(super) async fn response(
		&mut self,
		pool: &Arc<BufPool>,
	) -> std::io::Result<TableWriteResponseMeta> {
		read_response(&mut self.client, pool).await
	}
}

/// The one response frame of a table write, then the end of the stream.
async fn read_response(
	client: &mut PipeStream,
	pool: &Arc<BufPool>,
) -> std::io::Result<TableWriteResponseMeta> {
	use tokio::io::AsyncReadExt;
	let frame = crate::network::core::read_frame(client, pool).await?;
	assert_eq!(
		frame.header.msg_type,
		crate::schema::MsgType::TABLE_WRITE_RESP.0
	);
	let response: TableWriteResponseMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableWriteResponseMeta>()]);
	let mut rest = Vec::new();
	client.read_to_end(&mut rest).await?;
	assert!(rest.is_empty(), "one frame, then the end of the stream");
	Ok(response)
}

fn assert_deadline_answer(response: &TableWriteResponseMeta, what: &str) {
	assert_eq!(
		response.write.status,
		WriteStatus::WRITE_DEADLINE_EXCEEDED,
		"{what}: {response:?}"
	);
	assert_eq!(
		response.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE,
		"{what}: {response:?}"
	);
	assert_eq!(
		response.write.mutation_stop_reason(),
		MutationStopReason::DEADLINE,
		"{what}: {response:?}"
	);
}

/// Releases a held coordinator when dropped, so no failure path leaves it
/// parked.
pub(super) struct ReleaseOnDrop(pub(super) Arc<tokio::sync::Notify>);
impl Drop for ReleaseOnDrop {
	fn drop(&mut self) {
		self.0.notify_one();
	}
}

/// Waits for a pause to be reached.
pub(super) async fn until_reached(what: &str, reached: &tokio::sync::Notify) {
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.unwrap_or_else(|_| panic!("{what}: the pause was never reached"));
}

/// The handler's own end, bounded.
pub(super) async fn handler_end(what: &str, write: &mut StreamedWrite) -> std::io::Result<()> {
	tokio::time::timeout(BOUND, &mut write.handler)
		.await
		.unwrap_or_else(|_| panic!("{what}: the handler never returned"))
		.unwrap()
}

/// A write held after its Reserve answers its caller's deadline while it is
/// still held; released, the same attempt completes its reservation with no
/// retry, and the rows read everywhere under the one record.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_write_held_after_its_reserve_answers_by_its_deadline_and_is_completed_unretried() {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.deadline_remaining_millis = DEADLINE_MS;
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);
	let pool = cluster.node(primary).pool.clone();
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::AfterReserve);
	let held = ReleaseOnDrop(release);

	let mut write = StreamedWrite::send(&cluster, primary, meta, batch.clone(), false).await;
	until_reached("after Reserve", &reached).await;
	// The response frame arrives while the coordinator is still held.
	let answer = match tokio::time::timeout(ANSWER_BOUND, write.response(&pool)).await {
		Ok(answer) => answer.unwrap(),
		Err(_) => {
			drop(held);
			let late = tokio::time::timeout(BOUND, write.response(&pool)).await;
			panic!(
				"no response within {ANSWER_BOUND:?} of a {DEADLINE_MS} ms deadline while the \
				 coordinator was held after its Reserve; once released it answered {late:?}"
			);
		}
	};
	assert_deadline_answer(&answer, "held after Reserve");
	assert!(
		!write.handler.is_finished(),
		"the handler still owns the held coordination"
	);
	let reserved = record_of(&cluster, key).await.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);

	// Released and never retried, the same attempt completes its reservation.
	drop(held);
	handler_end("after release", &mut write).await.unwrap();
	let records = records_of(&cluster, key).await;
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	let ParentStatus::Accepted { dec } = records[0].status else {
		panic!("accepted with no retry: {:?}", records[0]);
	};
	assert_eq!(records[0].rsv, reserved.rsv);
	assert_eq!(records[0].claim, reserved.claim);
	expect_everywhere(
		&cluster,
		&descriptor,
		&[(id, format!("row-{id}"))].into(),
		"completed after its deadline answer",
	)
	.await;

	// The same identity now answers from the record.
	let mut again = StreamedWrite::send(&cluster, primary, meta, batch, false).await;
	let recorded = tokio::time::timeout(BOUND, again.response(&pool))
		.await
		.unwrap()
		.unwrap();
	assert_committed(&recorded, "the same identity after completion");
	assert_eq!(recorded.write.log_index, dec);
	assert_eq!(recorded.write.commit_hlc, reserved.claim.commit_hlc);
	handler_end("the recorded answer", &mut again)
		.await
		.unwrap();
	assert_eq!(records_of(&cluster, key).await.len(), 1);

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// The answer of a held write, which must arrive while it is held.
async fn answer_while_held(
	what: &str,
	write: &mut StreamedWrite,
	pool: &Arc<BufPool>,
) -> TableWriteResponseMeta {
	tokio::time::timeout(ANSWER_BOUND, write.response(pool))
		.await
		.unwrap_or_else(|_| panic!("{what}: no response within {ANSWER_BOUND:?} while held"))
		.unwrap()
}

pub(super) async fn shut_down(cluster: Cluster) {
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// A write whose deadline passes before it would reserve answers the deadline
/// and proposes nothing, then or after its hold is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_write_held_before_its_reserve_answers_its_deadline_and_never_reserves() {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.deadline_remaining_millis = DEADLINE_MS;
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);
	let pool = cluster.node(primary).pool.clone();
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::AfterAbsent);

	let mut write = StreamedWrite::send(&cluster, primary, meta, batch.clone(), false).await;
	until_reached("after Absent", &reached).await;
	let answer = answer_while_held("before Reserve", &mut write, &pool).await;
	assert_deadline_answer(&answer, "held before Reserve");
	// Nothing is left to own: the handler ends without any release.
	handler_end("before Reserve", &mut write).await.unwrap();
	assert!(record_of(&cluster, key).await.is_none());

	// Releasing the hold afterwards wakes nothing that could still propose.
	release.notify_one();
	tokio::time::sleep(std::time::Duration::from_millis(700)).await;
	assert!(
		record_of(&cluster, key).await.is_none(),
		"nothing was proposed after the deadline"
	);

	// The identity is untouched: a later call of it commits as a first one.
	let mut unhurried = meta;
	unhurried.deadline_remaining_millis = 0;
	let mut later = StreamedWrite::send(&cluster, primary, unhurried, batch, false).await;
	let committed = tokio::time::timeout(BOUND, later.response(&pool))
		.await
		.unwrap()
		.unwrap();
	assert_committed(&committed, "the same identity later");
	handler_end("the later call", &mut later).await.unwrap();
	assert_eq!(records_of(&cluster, key).await.len(), 1);
	shut_down(cluster).await;
}

/// An attempt that resumes a parent reserved by an earlier attempt claims at
/// the resume boundary: held after it, it answers its deadline while held and
/// completes the parent once released, with no further call.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_resumed_reservation_answers_its_deadline_and_is_completed_by_the_same_attempt() {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);
	let pool = cluster.node(primary).pool.clone();
	let server = &cluster.node(primary).data_server;
	// The first attempt names no deadline and stays held after its Reserve.
	let (first_reached, first_release) =
		server.pause_coordinator_for_test(PauseStage::AfterReserve);
	let first_held = ReleaseOnDrop(first_release);
	let (second_reached, second_release) =
		server.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let second_held = ReleaseOnDrop(second_release);
	let mut first = StreamedWrite::send(&cluster, primary, meta, batch.clone(), false).await;
	until_reached("the first attempt after Reserve", &first_reached).await;
	let reserved = record_of(&cluster, key).await.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);

	// The second finds the Reserved record, resumes it and is held before
	// Finalize, past its claim.
	let mut hurried = meta;
	hurried.deadline_remaining_millis = DEADLINE_MS;
	let mut second = StreamedWrite::send(&cluster, primary, hurried, batch, false).await;
	until_reached("the resuming attempt before Finalize", &second_reached).await;
	let answer = answer_while_held("resumed", &mut second, &pool).await;
	assert_deadline_answer(&answer, "held after the resume claim");
	assert!(!second.handler.is_finished());
	assert_eq!(record_of(&cluster, key).await.unwrap(), reserved);

	drop(second_held);
	handler_end("the resuming attempt", &mut second)
		.await
		.unwrap();
	let records = records_of(&cluster, key).await;
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	let ParentStatus::Accepted { dec } = records[0].status else {
		panic!("completed by the resuming attempt: {:?}", records[0]);
	};
	assert_eq!(records[0].claim, reserved.claim);
	assert!(
		!first.handler.is_finished(),
		"the first attempt is still held"
	);

	// The first attempt, released, answers from the record.
	drop(first_held);
	let original = tokio::time::timeout(BOUND, first.response(&pool))
		.await
		.unwrap()
		.unwrap();
	assert_committed(&original, "the first attempt");
	assert_eq!(original.write.log_index, dec);
	handler_end("the first attempt", &mut first).await.unwrap();
	expect_everywhere(
		&cluster,
		&descriptor,
		&[(id, format!("row-{id}"))].into(),
		"resumed and completed",
	)
	.await;
	shut_down(cluster).await;
}

/// The claim points themselves, with no timer racing them: an attempt whose
/// deadline has passed on the clock when it reaches a boundary neither
/// resumes a Reserved parent nor reserves a fresh one.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_attempt_past_its_deadline_at_a_claim_point_neither_resumes_nor_reserves() {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);
	let pool = cluster.node(primary).pool.clone();
	let server = &cluster.node(primary).data_server;
	let (reached, release) = server.pause_coordinator_for_test(PauseStage::AfterReserve);
	let held = ReleaseOnDrop(release);
	let mut first = StreamedWrite::send(&cluster, primary, meta, batch.clone(), false).await;
	until_reached("the first attempt after Reserve", &reached).await;
	let reserved = record_of(&cluster, key).await.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);

	// The resume boundary. A resumed attempt would write the child and
	// Finalize, and with every dependency healthy the record would be
	// Accepted.
	let late = server
		.coordinate_until_for_test(meta, batch.clone(), std::time::Instant::now())
		.await;
	assert_deadline_answer(&late, "past the deadline at the resume boundary");
	tokio::time::sleep(std::time::Duration::from_millis(500)).await;
	assert_eq!(
		record_of(&cluster, key).await.unwrap(),
		reserved,
		"no child or Finalize work by the late attempt"
	);

	// The Reserve boundary, under a fresh identity.
	let fresh = write_meta(&descriptor, 1, uuid_v7());
	let late = server
		.coordinate_until_for_test(fresh, batch, std::time::Instant::now())
		.await;
	assert_deadline_answer(&late, "past the deadline at the Reserve boundary");
	tokio::time::sleep(std::time::Duration::from_millis(500)).await;
	assert!(
		record_of(&cluster, key_of(&descriptor, &fresh))
			.await
			.is_none(),
		"no Reserve by the late attempt"
	);

	drop(held);
	let original = tokio::time::timeout(BOUND, first.response(&pool))
		.await
		.unwrap()
		.unwrap();
	assert_committed(&original, "the held first attempt");
	handler_end("the first attempt", &mut first).await.unwrap();
	shut_down(cluster).await;
}

/// An inbound connection that yields the streams queued on it.
struct QueuedConnection {
	remote: crate::schema::NodeId,
	actor: crate::network::tls::ActorIdentity,
	streams: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Box<dyn TransportStream>>>,
}

#[async_trait::async_trait]
impl crate::network::transport::TransportConnection for QueuedConnection {
	async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		Err(std::io::Error::new(
			std::io::ErrorKind::Unsupported,
			"an inbound connection",
		))
	}

	async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		self.streams.lock().await.recv().await.ok_or_else(|| {
			std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "no more streams")
		})
	}

	fn remote_id(&self) -> Option<crate::schema::NodeId> {
		Some(self.remote)
	}

	fn actor_identity(&self) -> &crate::network::tls::ActorIdentity {
		&self.actor
	}

	fn transport_type(&self) -> TransportTier {
		TransportTier::QuicIoUring
	}

	fn is_closed(&self) -> bool {
		false
	}
}

/// The answered write is still owned by its stream's task: through the
/// production accept loop, with the node's router behind a lifecycle of this
/// control's own (so its counts are exactly this stream's), the timed-out
/// write keeps its tracked work and its stream permit, and the lifecycle is
/// not quiescent, until the held coordination is released and ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_answered_write_keeps_its_stream_task_tracked_and_admitted_until_it_ends() {
	use crate::network::server::{
		LifecycleService, ServedConnectionForTest, ServerAdmissionConfig, ServerLifecycle,
	};
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.deadline_remaining_millis = DEADLINE_MS;
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);
	let pool = cluster.node(primary).pool.clone();
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::AfterReserve);
	let held = ReleaseOnDrop(release);

	let lifecycle = ServerLifecycle::new();
	let service = Arc::new(LifecycleService::new(
		cluster.node(primary).router.clone(),
		lifecycle.clone(),
	));
	let context = sender_to(primary);
	let (queue, streams) = tokio::sync::mpsc::unbounded_channel();
	let served = ServedConnectionForTest::serve(
		Arc::new(QueuedConnection {
			remote: context.remote.unwrap(),
			actor: context.actor,
			streams: tokio::sync::Mutex::new(streams),
		}),
		service,
		pool.clone(),
		lifecycle.clone(),
		ServerAdmissionConfig::default(),
	);
	assert_eq!(lifecycle.in_flight(), 0);
	assert_eq!(served.stream_tasks_admitted(), 0);

	let (mut client, server) = request_stream(&meta, &batch, false).await;
	assert!(
		queue
			.send(Box::new(server) as Box<dyn TransportStream>)
			.is_ok()
	);
	until_reached("after Reserve", &reached).await;
	let answer = tokio::time::timeout(ANSWER_BOUND, read_response(&mut client, &pool))
		.await
		.expect("answered while held")
		.unwrap();
	assert_deadline_answer(&answer, "held after Reserve");

	// Answered, and its stream finished; the task and what it holds remain.
	assert_eq!(
		lifecycle.in_flight(),
		2,
		"the stream task and its request are still tracked"
	);
	assert_eq!(
		served.stream_tasks_admitted(),
		1,
		"its permit is still taken"
	);
	assert!(
		tokio::time::timeout(
			std::time::Duration::from_millis(700),
			lifecycle.wait_for_quiescence()
		)
		.await
		.is_err(),
		"no quiescence while the coordination is held"
	);
	assert_eq!(lifecycle.in_flight(), 2);
	assert_eq!(served.stream_tasks_admitted(), 1);

	drop(held);
	tokio::time::timeout(BOUND, lifecycle.wait_for_quiescence())
		.await
		.expect("quiescent once the coordination has ended");
	assert_eq!(lifecycle.in_flight(), 0);
	assert_eq!(served.stream_tasks_admitted(), 0);
	let record = record_of(&cluster, key).await.unwrap();
	assert!(
		matches!(record.status, ParentStatus::Accepted { .. }),
		"{record:?}"
	);
	drop(queue);
	shut_down(cluster).await;
}

/// A response that cannot be written (the caller has gone) neither ends the
/// handler nor drops the coordination: it is still held after the write
/// failed, and released it completes the reservation.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_response_that_cannot_be_written_does_not_stop_the_coordination() {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.deadline_remaining_millis = DEADLINE_MS;
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::AfterReserve);
	let held = ReleaseOnDrop(release);

	let mut write = StreamedWrite::send(&cluster, primary, meta, batch, true).await;
	until_reached("after Reserve", &reached).await;
	// Well past the deadline: the response write has been tried and failed.
	tokio::time::sleep(std::time::Duration::from_millis(DEADLINE_MS + 1_200)).await;
	assert!(
		!write.handler.is_finished(),
		"the failed response did not end the handler"
	);
	assert_eq!(
		record_of(&cluster, key).await.unwrap().status,
		ParentStatus::Reserved
	);

	drop(held);
	let ended = handler_end("after release", &mut write).await;
	assert_eq!(
		ended.unwrap_err().kind(),
		std::io::ErrorKind::BrokenPipe,
		"the handler reports its failed response, after the coordination ended"
	);
	let records = records_of(&cluster, key).await;
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	assert!(
		matches!(records[0].status, ParentStatus::Accepted { .. }),
		"completed although its response was never delivered: {:?}",
		records[0]
	);
	expect_everywhere(
		&cluster,
		&descriptor,
		&[(id, format!("row-{id}"))].into(),
		"completed without a caller",
	)
	.await;
	shut_down(cluster).await;
}

/// The ordinary path on the stream: with a generous deadline, and with none,
/// a write commits on its first attempt and its handler ends with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_write_with_time_left_or_no_deadline_commits_on_its_first_attempt() {
	let (cluster, leader, descriptor) = ready().await;
	let pool = cluster.node(leader).pool.clone();
	let mut expected = Expected::new();
	let mut from = 1;
	for deadline_ms in [30_000, 0] {
		let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, from).await;
		from = id + 1;
		let mut meta = write_meta(&descriptor, 1, uuid_v7());
		meta.deadline_remaining_millis = deadline_ms;
		let key = key_of(&descriptor, &meta);
		let mut write =
			StreamedWrite::send(&cluster, primary, meta, rows(&descriptor, &[id]), false).await;
		let response = tokio::time::timeout(BOUND, write.response(&pool))
			.await
			.unwrap()
			.unwrap();
		assert_committed(&response, "the first attempt");
		assert_eq!(
			response.write.mutation_stop_reason(),
			MutationStopReason::NONE
		);
		handler_end("an ordinary write", &mut write).await.unwrap();
		let records = records_of(&cluster, key).await;
		assert_eq!(records.len(), 1, "one parent: {records:?}");
		assert_eq!(
			records[0].status,
			ParentStatus::Accepted {
				dec: response.write.log_index
			}
		);
		expected.insert(id, format!("row-{id}"));
	}
	expect_everywhere(&cluster, &descriptor, &expected, "ordinary writes").await;
	shut_down(cluster).await;
}

/// The stream entry keeps the router's ingress rule: a table write from an
/// actor that is not an authenticated node is refused before the data ring,
/// and nothing is coordinated.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_table_write_stream_from_a_non_node_actor_is_refused_at_ingress() {
	use crate::network::tls::{ActorIdentity, ActorRole};
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.deadline_remaining_millis = DEADLINE_MS;
	let operator = crate::network::server::RequestContext {
		remote: None,
		actor: ActorIdentity::new(
			ActorRole::Operator,
			None,
			None,
			vec!["cluster".to_owned()],
			vec![1, 2, 3, 4],
		)
		.unwrap(),
	};
	let mut write = StreamedWrite::send_as(
		&cluster,
		primary,
		meta,
		rows(&descriptor, &[id]),
		false,
		operator,
	)
	.await;
	let refused = handler_end("a non-node actor", &mut write)
		.await
		.unwrap_err();
	assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);
	assert!(
		record_of(&cluster, key_of(&descriptor, &meta))
			.await
			.is_none()
	);
	shut_down(cluster).await;
}
