//! ADR 0736, in process: a backup exported from one three-node ParentDecisions
//! cluster, verified, restored into a second, fresh cluster on its own network
//! (no shared store, network or keys), verified there by count and canonical
//! digest, and the restored cluster takes a new write.
//!
//! Credentials are the deployment's: an operator client (hold, roster, tenant
//! and schema administration) and a storage client, a `data_node` identity
//! confined to tenant 1, for every table read and write; the data lives in
//! tenant 1 because the default tenant 0 can have no storage credential. The
//! tenant and its table are created through the public admin routes.

use super::*;
use crate::client::core::TableWriteRequest;
use crate::client::{
	ClusterPeerWire, ClusterStatusWire, StaticTopologySource, WendelonClient, WendelonClientConfig,
};
use crate::logical_backup::export::{BackupRefusal, Clients, ExportError};
use crate::logical_backup::restore::{RestoreError, RestoreRefusal};
use crate::logical_backup::{export, format, restore};
use crate::network::control_server::TenantAdminRequestWire;
use crate::network::tls::{ActorIdentity, ActorRole};
use crate::network::transport::{
	NodeRole, NodeRoleSet, TransportConnection, TransportProvider, TransportTierSet,
};
use crate::schema::{MutationAnswer, mutation_answer};

const TIER: TransportTier = TransportTier::QuicIoUring;
const PASSPHRASE: &[u8] = b"adr-0736 in-process passphrase";
const TENANT: TenantId = TenantId(1);
/// The client's own node id on the network: not a member.
const CLIENT_NODE: u32 = 99;

fn operator_actor() -> ActorIdentity {
	ActorIdentity::new(
		ActorRole::Operator,
		None,
		None,
		Vec::new(),
		b"adr0736-operator".to_vec(),
	)
	.unwrap()
}

/// A storage certificate's identity: `data_node`, not a member, tenant 1.
fn storage_actor() -> ActorIdentity {
	ActorIdentity::new(
		ActorRole::DataNode,
		Some(crate::schema::NodeId {
			id: CLIENT_NODE,
			_reserved: 0,
		}),
		Some(TENANT.get().to_string()),
		vec!["cluster".to_owned()],
		b"adr0736-storage".to_vec(),
	)
	.unwrap()
}

/// A client on `cluster`'s network presenting `actor`.
async fn client_as(cluster: &Cluster, actor: ActorIdentity) -> Arc<WendelonClient> {
	let leader = cluster.leader().await;
	let status = ClusterStatusWire {
		epoch: cluster.node(leader).ring.current_epoch(),
		leader: Some(leader),
		replication_factor: NODES.len() as u32,
		peers: NODES
			.into_iter()
			.map(|node_id| ClusterPeerWire {
				node_id,
				address: address(node_id),
				tier_bits: TransportTierSet::empty().with(TIER).as_u8(),
				role_bits: NodeRoleSet::empty()
					.with(NodeRole::Data)
					.with(NodeRole::Control)
					.as_u8(),
				capability_bits: 0,
			})
			.collect(),
	};
	let provider: Arc<dyn TransportProvider> = Arc::new(
		SimulatedTransportProvider::new(
			crate::schema::NodeId {
				id: CLIENT_NODE,
				_reserved: 0,
			},
			cluster.network.clone(),
			crate::network::pool::BufPool::new(64, 1024 * 1024),
			TIER,
		)
		.with_caller(actor),
	);
	WendelonClient::new(WendelonClientConfig {
		local_tiers: TransportTierSet::empty().with(TIER),
		providers: [(TIER, provider)].into(),
		bootstrap: status.clone(),
		topology: Arc::new(StaticTopologySource::new(status)),
		pool_max_retained: 64,
		pool_max_cap_bytes: 1024 * 1024,
	})
}

async fn clients(cluster: &Cluster) -> Clients {
	Clients {
		admin: client_as(cluster, operator_actor()).await,
		data: [(TENANT, client_as(cluster, storage_actor()).await)].into(),
	}
}

async fn hold_of(cluster: &Cluster) -> Option<(u128, u64)> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().backup_hold())
		.await
}

/// A formed cluster with tenant 1 and its table, made through the public
/// admin routes as an operator, applied on every node.
async fn source_with_table() -> (Cluster, Clients, TableSchemaDescriptor) {
	let cluster = Cluster::start().await;
	until_ready(&cluster, None).await;
	let clients = clients(&cluster).await;
	clients
		.admin
		.tenant_admin(&TenantAdminRequestWire::Create {
			target_tenant_id: TENANT,
			quota_bytes: u64::MAX,
			replication_factor: NODES.len() as u32,
		})
		.await
		.unwrap();
	let descriptor = descriptor();
	clients
		.admin
		.submit_schema_update_envelope(&SchemaUpdateEnvelope::compute_client_intent(
			TENANT,
			SchemaObjectRef {
				kind: SchemaObjectKind::Table,
				namespace: descriptor.namespace.clone(),
				name: descriptor.table_name.clone(),
			},
			SchemaGeneration::ZERO,
			SchemaGeneration::new(descriptor.schema_generation),
			SchemaIntentId::new([0x36; 16]),
			SchemaMutationKind::Create,
			encode_table_schema_descriptor_v1(&descriptor).unwrap(),
		))
		.await
		.unwrap();
	// A prerequisite of these backup controls, not a guarantee of the schema route: every
	// node's own applied state holds tenant 1 and this exact table generation before the first
	// write, so a coordinating node never meets a catalog it has not applied yet (DC-242). One
	// bounded deadline over state predicates; the writes themselves are never retried for it.
	let (table, generation) = (descriptor.table_object_id, descriptor.schema_generation);
	let by = tokio::time::Instant::now() + BOUND;
	for id in NODES {
		loop {
			let applied = cluster
				.node(id)
				.state_machine
				.read_state(move |state| {
					state.tenants.contains_key(&TENANT)
						&& state.table_schemas.get(TENANT, table, generation).is_some()
				})
				.await;
			if applied {
				break;
			}
			assert!(
				tokio::time::Instant::now() < by,
				"node {id} never applied tenant 1 and the table"
			);
			tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		}
	}
	(cluster, clients, descriptor)
}

/// One table write as tenant 1's storage client; it must commit.
async fn write(clients: &Clients, descriptor: &TableSchemaDescriptor, rows: &[(i64, &str)]) {
	let mut meta = write_meta(descriptor, rows.len(), uuid_v7());
	meta.tenant_id = TENANT;
	let result = clients.data[&TENANT]
		.table_write(TableWriteRequest {
			meta,
			descriptor: descriptor.clone(),
			payload: named_rows(descriptor, rows),
			deadline: Some(std::time::Instant::now() + std::time::Duration::from_secs(30)),
		})
		.await
		.unwrap();
	let write = result.response.write;
	assert!(
		matches!(
			mutation_answer(write.status, write.mutation_commit_knowledge()),
			MutationAnswer::Committed { .. }
		),
		"{write:?}"
	);
}

/// Tenant 1's table, read through its storage client.
async fn read(clients: &Clients, descriptor: &TableSchemaDescriptor) -> Expected {
	let mut result = export::query_table(&clients.data[&TENANT], TENANT, descriptor)
		.await
		.unwrap();
	let mut rows = Expected::new();
	while let Some(batch) = result.next_batch().await.unwrap() {
		let ids = batch
			.column_by_name("id")
			.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
			.unwrap();
		let values = batch
			.column_by_name("v")
			.and_then(|column| column.as_any().downcast_ref::<StringArray>())
			.unwrap();
		for row in 0..batch.num_rows() {
			assert!(
				rows.insert(ids.value(row), values.value(row).to_owned())
					.is_none(),
				"a row answered twice"
			);
		}
	}
	rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_backup_restores_into_a_fresh_cluster_that_then_takes_new_writes() {
	let (source, clients, descriptor) = source_with_table().await;
	let mut expected = Expected::new();
	for id in 1..=20i64 {
		write(&clients, &descriptor, &[(id, "a")]).await;
		expected.insert(id, "a".into());
	}
	// An update: the backup holds each key's newest version only.
	write(&clients, &descriptor, &[(3, "b")]).await;
	expected.insert(3, "b".into());
	assert_eq!(read(&clients, &descriptor).await, expected);

	let root = crate::test_support::TestDir::new("adr0736-artifact");
	// Without tenant 1's storage credential: refused by name, nothing kept held.
	let no_data = Clients {
		admin: clients.admin.clone(),
		data: Default::default(),
	};
	assert!(matches!(
		export::export(&no_data, &root.to_path_buf().join("refused"), PASSPHRASE).await,
		Err(ExportError::Refused(BackupRefusal::NoDataCredential {
			tenant_id: TENANT
		}))
	));
	assert_eq!(hold_of(&source).await, None);

	let dir = root.to_path_buf().join("backup");
	let report = export::export(&clients, &dir, PASSPHRASE)
		.await
		.unwrap_or_else(|error| panic!("export: {error}"));
	assert_eq!((report.tables, report.rows), (1, 20), "{report:?}");
	assert!(report.continuity_cut >= report.cut);
	assert_eq!(hold_of(&source).await, None, "the export released its hold");
	// The directory exists: refused before any hold is entered.
	assert!(export::export(&clients, &dir, PASSPHRASE).await.is_err());
	assert_eq!(hold_of(&source).await, None);

	// R1.
	assert!(matches!(
		restore::verify(&dir, b"another passphrase"),
		Err(RestoreError::Refused(RestoreRefusal::Artifact { .. }))
	));
	let verified = restore::verify(&dir, PASSPHRASE).unwrap();
	assert_eq!(verified.manifest.tables[0].rows, 20);
	assert_eq!(verified.manifest.roster.tables[0].descriptor, descriptor);

	// A fresh cluster on its own network.
	let target = Cluster::start().await;
	until_ready(&target, None).await;
	let target_clients = self::clients(&target).await;
	let restored = restore::restore(&target_clients, &dir, &verified)
		.await
		.unwrap_or_else(|error| panic!("restore: {error}"));
	assert_eq!((restored.tables, restored.rows), (1, 20), "{restored:?}");
	assert_eq!(
		hold_of(&target).await,
		None,
		"the restore released its hold"
	);
	assert_eq!(read(&target_clients, &descriptor).await, expected);

	// The target is no longer fresh.
	assert!(matches!(
		restore::restore(&target_clients, &dir, &verified).await,
		Err(RestoreError::Refused(RestoreRefusal::TargetNotFresh { .. }))
	));
	assert_eq!(hold_of(&target).await, None);

	// The restored cluster takes a new write.
	write(&target_clients, &descriptor, &[(21, "new")]).await;
	expected.insert(21, "new".into());
	assert_eq!(read(&target_clients, &descriptor).await, expected);

	// One flipped byte in a chunk: verification refuses the backup.
	let chunk = format::chunk_path(&dir, 1);
	let mut bytes = std::fs::read(&chunk).unwrap();
	bytes[0] ^= 1;
	std::fs::write(&chunk, bytes).unwrap();
	assert!(matches!(
		restore::verify(&dir, PASSPHRASE),
		Err(RestoreError::Refused(RestoreRefusal::Artifact { .. }))
	));
}

// ============================================================
//  CONTROLS: an unanswered hold entry; a read without a successful terminal
// ============================================================

/// The simulated provider, except that the first `silent` streams it opens
/// lose their reply: the request reaches the server, which applies it, and
/// the reply is never read (`fail` false), or is read and replaced by an
/// `InvalidInput` error (`fail` true), as a handler's error for an
/// indeterminate proposal reaches the tool. A lost answer, not a lost request.
struct LostReplies {
	inner: SimulatedTransportProvider,
	silent: Arc<std::sync::atomic::AtomicUsize>,
	fail: bool,
}

struct LostReplyConnection {
	inner: Arc<dyn TransportConnection>,
	silent: Arc<std::sync::atomic::AtomicUsize>,
	fail: bool,
}

/// A stream whose writes reach the server and whose reply is lost.
struct Unanswered {
	inner: Box<dyn crate::network::transport::TransportStream>,
	fail: bool,
}

impl tokio::io::AsyncRead for Unanswered {
	fn poll_read(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		_buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		if !self.fail {
			return std::task::Poll::Pending;
		}
		// The reply's first bytes arrive only after the server answered, so
		// the command has applied; the caller is handed an error instead.
		let mut scratch = [0u8; 64];
		let mut read = tokio::io::ReadBuf::new(&mut scratch);
		match std::pin::Pin::new(&mut *self.inner).poll_read(cx, &mut read) {
			std::task::Poll::Pending => std::task::Poll::Pending,
			std::task::Poll::Ready(_) => std::task::Poll::Ready(Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"scripted: an error in place of the answer",
			))),
		}
	}
}

impl tokio::io::AsyncWrite for Unanswered {
	fn poll_write(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<std::io::Result<usize>> {
		std::pin::Pin::new(&mut *self.inner).poll_write(cx, buf)
	}

	fn poll_flush(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::pin::Pin::new(&mut *self.inner).poll_flush(cx)
	}

	fn poll_shutdown(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::pin::Pin::new(&mut *self.inner).poll_shutdown(cx)
	}
}

#[async_trait::async_trait]
impl crate::network::transport::TransportStream for Unanswered {
	fn stream_id(&self) -> u64 {
		self.inner.stream_id()
	}

	async fn finish(&mut self) -> std::io::Result<()> {
		self.inner.finish().await
	}
}

#[async_trait::async_trait]
impl TransportConnection for LostReplyConnection {
	async fn open_bi(
		&self,
	) -> std::io::Result<Box<dyn crate::network::transport::TransportStream>> {
		let stream = self.inner.open_bi().await?;
		let silenced = self
			.silent
			.fetch_update(
				std::sync::atomic::Ordering::SeqCst,
				std::sync::atomic::Ordering::SeqCst,
				|left| left.checked_sub(1),
			)
			.is_ok();
		Ok(if silenced {
			Box::new(Unanswered {
				inner: stream,
				fail: self.fail,
			})
		} else {
			stream
		})
	}

	async fn accept_bi(
		&self,
	) -> std::io::Result<Box<dyn crate::network::transport::TransportStream>> {
		self.inner.accept_bi().await
	}

	fn remote_id(&self) -> Option<crate::schema::NodeId> {
		self.inner.remote_id()
	}

	fn actor_identity(&self) -> &ActorIdentity {
		self.inner.actor_identity()
	}

	fn transport_type(&self) -> TransportTier {
		self.inner.transport_type()
	}

	fn is_closed(&self) -> bool {
		self.inner.is_closed()
	}
}

#[async_trait::async_trait]
impl TransportProvider for LostReplies {
	async fn connect(
		&self,
		target: crate::schema::NodeId,
		address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		Ok(Arc::new(LostReplyConnection {
			inner: self.inner.connect(target, address).await?,
			silent: self.silent.clone(),
			fail: self.fail,
		}))
	}

	fn withdraw_local(
		&self,
		node_id: crate::schema::NodeId,
		address: &str,
		service: &Arc<dyn crate::network::server::HrftService>,
	) -> std::io::Result<()> {
		self.inner.withdraw_local(node_id, address, service)
	}
}

/// A client like [`client_as`] over `provider`.
async fn client_over(
	cluster: &Cluster,
	provider: Arc<dyn TransportProvider>,
) -> Arc<WendelonClient> {
	let leader = cluster.leader().await;
	let status = status_at(leader, cluster.node(leader).ring.current_epoch());
	WendelonClient::new(WendelonClientConfig {
		local_tiers: TransportTierSet::empty().with(TIER),
		providers: [(TIER, provider)].into(),
		bootstrap: status.clone(),
		topology: Arc::new(StaticTopologySource::new(status)),
		pool_max_retained: 64,
		pool_max_cap_bytes: 1024 * 1024,
	})
}

fn status_at(leader: u32, epoch: u64) -> ClusterStatusWire {
	ClusterStatusWire {
		epoch,
		leader: Some(leader),
		replication_factor: NODES.len() as u32,
		peers: NODES
			.into_iter()
			.map(|node_id| ClusterPeerWire {
				node_id,
				address: address(node_id),
				tier_bits: TransportTierSet::empty().with(TIER).as_u8(),
				role_bits: NodeRoleSet::empty()
					.with(NodeRole::Data)
					.with(NodeRole::Control)
					.as_u8(),
				capability_bits: 0,
			})
			.collect(),
	}
}

const SHORT: crate::logical_backup::hold::Bounds = crate::logical_backup::hold::Bounds {
	settle: std::time::Duration::from_secs(30),
	call: std::time::Duration::from_secs(2),
	table: std::time::Duration::from_secs(30),
};

/// An operator client whose first `lost` streams lose their answer (`fail`:
/// an error in its place).
async fn lossy_admin(cluster: &Cluster, lost: usize, fail: bool) -> Arc<WendelonClient> {
	let provider: Arc<dyn TransportProvider> = Arc::new(LostReplies {
		inner: SimulatedTransportProvider::new(
			crate::schema::NodeId {
				id: CLIENT_NODE,
				_reserved: 0,
			},
			cluster.network.clone(),
			crate::network::pool::BufPool::new(64, 1024 * 1024),
			TIER,
		)
		.with_caller(operator_actor()),
		silent: Arc::new(std::sync::atomic::AtomicUsize::new(lost)),
		fail,
	});
	client_over(cluster, provider).await
}

/// The Begin applies but its answer is lost, or replaced by an error of a kind
/// a refusal could also have: the exporter does not claim the hold was not
/// entered; it reads the current hold, finds its own id, releases it with its
/// exact token and reports that. Nothing is published and no hold is left.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_lost_hold_entry_reply_is_read_back_and_released_by_its_exact_token() {
	use crate::logical_backup::hold::{AfterUnconfirmedEntry, HoldError};
	let (source, clients, _descriptor) = source_with_table().await;
	let root = crate::test_support::TestDir::new("adr0736-lost-entry");
	for fail in [false, true] {
		let lossy = Clients {
			admin: lossy_admin(&source, 1, fail).await,
			data: clients.data.clone(),
		};
		let dir = root.to_path_buf().join(format!("backup-{fail}"));
		let error = export::export_bounded(&lossy, &dir, PASSPHRASE, SHORT)
			.await
			.unwrap_err();
		let ExportError::Hold(HoldError::Unconfirmed { after, .. }) = &error else {
			panic!("fail {fail}: not an unconfirmed entry: {error}");
		};
		assert!(
			matches!(after, AfterUnconfirmedEntry::ReleasedOwn { .. }),
			"fail {fail}: {error}"
		);
		assert_eq!(hold_of(&source).await, None, "fail {fail}: no hold is left");
		assert!(!dir.join(format::MANIFEST_FILE).exists());
	}
}

/// A release that applied but whose answer is an error is reported
/// Unconfirmed, never as a release that did not apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_release_answered_by_an_error_is_unconfirmed_never_not_applied() {
	use crate::logical_backup::hold::{self, Release};
	let (source, clients, _descriptor) = source_with_table().await;
	let token = hold::enter(&clients.admin, 0x736_0001, SHORT)
		.await
		.unwrap();
	let failing = lossy_admin(&source, 1, true).await;
	assert!(matches!(
		hold::release(&failing, token, SHORT).await,
		Release::Unconfirmed { .. }
	));
	assert_eq!(hold_of(&source).await, None, "it had applied all the same");
}

/// How the scripted node ends a table query's stream after a first frame
/// that says OK with an empty batch.
#[derive(Clone, Copy, Debug)]
enum Ending {
	/// A STREAM_END continuation with no response metadata.
	BareEnd,
	/// A terminal whose status is not success.
	FailedTerminal,
	/// A successful terminal counting rows that never arrived.
	CountMismatch,
}

/// A node that answers every table query with `ending`.
struct ScriptedQueries {
	ending: Ending,
	schema: Arc<arrow::datatypes::Schema>,
}

#[async_trait::async_trait]
impl crate::network::server::HrftService for ScriptedQueries {
	async fn handle_frame(
		&self,
		_context: crate::network::server::RequestContext,
		frame: crate::network::core::HrftFrame,
	) -> std::io::Result<Option<crate::network::core::HrftFrame>> {
		Err(std::io::Error::other(format!(
			"scripted queries: unexpected msg_type 0x{:02x}",
			frame.header.msg_type
		)))
	}

	async fn handle_stream(
		&self,
		mut stream: Box<dyn crate::network::transport::TransportStream>,
		pool: Arc<crate::network::pool::BufPool>,
		_context: crate::network::server::RequestContext,
	) -> std::io::Result<()> {
		use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
		use crate::schema::{
			FRAME_FLAG_STREAM_END, FrameHeader, MsgType, QueryResponseMeta, ReadStatus,
		};
		let request = read_frame(&mut *stream, &pool).await?;
		assert_eq!(request.header.msg_type, MsgType::TABLE_QUERY_REQ.0);
		let header = |flags: u16, meta_len: usize, arrow_len: usize| FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_QUERY_RESP.0,
			flags,
			stream_id: request.header.stream_id,
			meta_len: meta_len as u32,
			arrow_len: arrow_len as u32,
			trailing_len: 0,
		};
		let empty =
			crate::network::ipc::wire_encode_batch(&RecordBatch::new_empty(self.schema.clone()))?;
		let mut first: QueryResponseMeta = bytemuck::Zeroable::zeroed();
		first.status = ReadStatus::OK;
		first.rows = 0;
		first.arrow_len = empty.len() as u32;
		write_frame(
			&mut *stream,
			&header(0, std::mem::size_of::<QueryResponseMeta>(), empty.len()),
			bytemuck::bytes_of(&first),
			Some(&empty),
			None,
		)
		.await?;
		match self.ending {
			Ending::BareEnd => {
				write_frame(
					&mut *stream,
					&header(FRAME_FLAG_STREAM_END, 0, 0),
					&[],
					None,
					None,
				)
				.await?
			}
			Ending::FailedTerminal | Ending::CountMismatch => {
				let mut terminal: QueryResponseMeta = bytemuck::Zeroable::zeroed();
				(terminal.status, terminal.rows) = match self.ending {
					Ending::FailedTerminal => (ReadStatus::QUERY_DEADLINE_EXCEEDED, 0),
					_ => (ReadStatus::OK, 5),
				};
				write_frame(
					&mut *stream,
					&header(
						FRAME_FLAG_STREAM_END,
						std::mem::size_of::<QueryResponseMeta>(),
						0,
					),
					bytemuck::bytes_of(&terminal),
					None,
					None,
				)
				.await?
			}
		}
		stream.finish().await
	}
}

/// A table read that ends without an explicit successful terminal refuses the
/// whole backup: a bare stream end, a failed terminal after a batch, and a
/// successful terminal whose count is not the rows received. Each time nothing
/// is published and the hold is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_table_read_without_a_successful_terminal_refuses_the_backup() {
	let (source, clients, descriptor) = source_with_table().await;
	write(&clients, &descriptor, &[(1, "a")]).await;
	let leader = source.leader().await;
	let epoch = source.node(leader).ring.current_epoch();
	let root = crate::test_support::TestDir::new("adr0736-terminal");
	for ending in [
		Ending::BareEnd,
		Ending::FailedTerminal,
		Ending::CountMismatch,
	] {
		// The data client reaches scripted nodes on a network of their own.
		let network = Arc::new(SimulatedTransportNetwork::default());
		let service: Arc<dyn crate::network::server::HrftService> = Arc::new(ScriptedQueries {
			ending,
			schema: Arc::new(descriptor.arrow_schema().unwrap()),
		});
		for id in NODES {
			network.publish(
				crate::schema::NodeId { id, _reserved: 0 },
				&address(id),
				service.clone(),
			);
		}
		let provider: Arc<dyn TransportProvider> = Arc::new(
			SimulatedTransportProvider::new(
				crate::schema::NodeId {
					id: CLIENT_NODE,
					_reserved: 0,
				},
				network,
				crate::network::pool::BufPool::new(64, 1024 * 1024),
				TIER,
			)
			.with_caller(storage_actor()),
		);
		let status = status_at(leader, epoch);
		let scripted = Clients {
			admin: clients.admin.clone(),
			data: [(
				TENANT,
				WendelonClient::new(WendelonClientConfig {
					local_tiers: TransportTierSet::empty().with(TIER),
					providers: [(TIER, provider)].into(),
					bootstrap: status.clone(),
					topology: Arc::new(StaticTopologySource::new(status)),
					pool_max_retained: 64,
					pool_max_cap_bytes: 1024 * 1024,
				}),
			)]
			.into(),
		};
		let dir = root.to_path_buf().join(format!("{ending:?}"));
		let answer = export::export_bounded(&scripted, &dir, PASSPHRASE, SHORT).await;
		assert!(
			matches!(
				answer,
				Err(ExportError::Refused(BackupRefusal::ReaderRefused { .. }))
			),
			"{ending:?}: {answer:?}"
		);
		assert!(!dir.join(format::MANIFEST_FILE).exists(), "{ending:?}");
		assert_eq!(hold_of(&source).await, None, "{ending:?}");
	}
}
