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
use crate::integration::compute::{
	DropOperationId, TableSchemaDropFields, TableSchemaDropRequest, WendelonAdminError,
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
	client_advertised(cluster, actor, 0).await
}

/// `client_as`, its topology advertising `capability_bits` for every peer.
pub(super) async fn client_advertised(
	cluster: &Cluster,
	actor: ActorIdentity,
	capability_bits: u8,
) -> Arc<WendelonClient> {
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
				capability_bits,
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
	source_with(descriptor()).await
}

/// `source_with_table` for a given table.
async fn source_with(
	descriptor: TableSchemaDescriptor,
) -> (Cluster, Clients, TableSchemaDescriptor) {
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

/// Tenant 1's table as a sorted row multiset (duplicates kept).
async fn read_multiset(
	clients: &Clients,
	descriptor: &TableSchemaDescriptor,
) -> Vec<(i64, String)> {
	let mut result = export::query_table(&clients.data[&TENANT], TENANT, descriptor)
		.await
		.unwrap();
	let mut rows = Vec::new();
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
			rows.push((ids.value(row), values.value(row).to_owned()));
		}
	}
	rows.sort();
	rows
}

/// Wendelon ADR 0119 (acceptance 13): a named AppendOnly table holding
/// duplicates (within one write and across two) is exported and restored into a
/// fresh ParentDecisions cluster with its exact row multiset; the restore writes
/// through fresh operations, so the restored rows carry the restore's own
/// identities, and the restored table takes a new duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_append_only_tables_duplicates_survive_a_backup_and_restore() {
	let (_source, clients, descriptor) =
		source_with(super::append_only::append_only_descriptor()).await;
	write(
		&clients,
		&descriptor,
		&[(1, "same"), (1, "same"), (2, "other")],
	)
	.await;
	write(&clients, &descriptor, &[(1, "same")]).await;
	let mut expected: Vec<(i64, String)> = [(1, "same"), (1, "same"), (1, "same"), (2, "other")]
		.iter()
		.map(|(id, v)| (*id, (*v).to_owned()))
		.collect();
	assert_eq!(read_multiset(&clients, &descriptor).await, expected);

	let root = crate::test_support::TestDir::new("adr0119-artifact");
	let dir = root.to_path_buf().join("backup");
	let report = export::export(&clients, &dir, PASSPHRASE)
		.await
		.unwrap_or_else(|error| panic!("export: {error}"));
	assert_eq!((report.tables, report.rows), (1, 4), "{report:?}");
	let verified = restore::verify(&dir, PASSPHRASE).unwrap();
	assert_eq!(verified.manifest.roster.tables[0].descriptor, descriptor);

	let target = Cluster::start().await;
	until_ready(&target, None).await;
	let target_clients = self::clients(&target).await;
	let restored = restore::restore(&target_clients, &dir, &verified)
		.await
		.unwrap_or_else(|error| panic!("restore: {error}"));
	assert_eq!((restored.tables, restored.rows), (1, 4), "{restored:?}");
	assert_eq!(read_multiset(&target_clients, &descriptor).await, expected);

	write(&target_clients, &descriptor, &[(1, "same")]).await;
	expected.insert(0, (1, "same".to_owned()));
	assert_eq!(read_multiset(&target_clients, &descriptor).await, expected);
}

/// Wendelon ADR 0121 D4: a fresh target whose store binds a positive retention window would hide
/// a restored AppendOnly table's rows by age, so restore checks the incoming roster against the
/// target's authoritative binding before it establishes or imports anything: the backup is
/// refused there, typed, with no tenant, descriptor or row imported and no hold left.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_append_only_backup_is_refused_by_a_windowed_target_before_any_import() {
	let (_source, clients, descriptor) =
		source_with(super::append_only::append_only_descriptor()).await;
	write(&clients, &descriptor, &[(1, "a"), (2, "b")]).await;
	let root = crate::test_support::TestDir::new("adr0121-artifact");
	let dir = root.to_path_buf().join("backup");
	export::export(&clients, &dir, PASSPHRASE)
		.await
		.unwrap_or_else(|error| panic!("export: {error}"));
	let verified = restore::verify(&dir, PASSPHRASE).unwrap();

	let windowed = Cluster::start_with(|_, cfg| {
		cfg.parent_decisions_retention_window = std::time::Duration::from_secs(15);
		None
	})
	.await;
	until_ready(&windowed, None).await;
	windowed
		.until("the window bound", |state| {
			state
				.decisions
				.as_ref()
				.is_some_and(|decisions| decisions.retention().is_some())
		})
		.await;
	let windowed_clients = self::clients(&windowed).await;
	let refused = restore::restore(&windowed_clients, &dir, &verified).await;
	assert!(
		matches!(
			&refused,
			Err(RestoreError::Refused(RestoreRefusal::RetentionWindowBound {
				tenant_id,
				table_object_id,
				window_ms: 15_000,
			})) if *tenant_id == TENANT && *table_object_id == descriptor.table_object_id
		),
		"{refused:?}"
	);
	let leader = windowed.leader().await;
	let table = descriptor.table_object_id;
	let imported = windowed
		.node(leader)
		.state_machine
		.read_state(move |state| {
			(
				state.tenants.contains_key(&TENANT),
				state.table_schemas.latest_by_id(TENANT, table).is_some(),
			)
		})
		.await;
	assert_eq!(imported, (false, false), "nothing established or imported");
	assert_eq!(hold_of(&windowed).await, None, "no hold left");
	// A fresh Disabled target restores an AppendOnly backup and verifies it:
	// `an_append_only_tables_duplicates_survive_a_backup_and_restore`.
}

/// One AppendOnly write of `rows` through `client`; it must commit.
async fn write_through_client(
	client: &WendelonClient,
	descriptor: &TableSchemaDescriptor,
	meta: crate::schema::TableWriteRequestMeta,
	rows: &[(i64, &str)],
) {
	let result = client
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

/// Wendelon ADR 0119 D7 (acceptance 11): an AppendOnly write converges from
/// either client preview. A client whose topology advertises no
/// `ParentDecisions` bit keeps Today's request-level preview, which for some of
/// these operations names another place than the ParentDecisions route (a
/// premise checked here); one whose every peer advertises it previews the
/// route. Every write through either commits within the client's own bounded
/// retries (the server admits at the lineage holder of row 0's position or
/// redirects there, hinted), and every row is read once. The number of
/// redirects is not observed: the client has no counter of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_append_only_write_converges_from_either_preview() {
	use crate::network::transport::{NodeCapability, NodeCapabilitySet};
	let (source, clients, descriptor) =
		source_with(super::append_only::append_only_descriptor()).await;
	let old = clients.data[&TENANT].clone();
	let advertised = NodeCapabilitySet::empty()
		.with(NodeCapability::ParentDecisions)
		.with(NodeCapability::ParentDecisionsBatchedLeases)
		.as_u8();
	let routed = client_advertised(&source, storage_actor(), advertised).await;
	let rows = [(1, "same"), (1, "same"), (2, "other")];
	let mut crossed = 0;
	let mut expected = Vec::new();
	for _ in 0..12 {
		for client in [&old, &routed] {
			let mut meta = write_meta(&descriptor, rows.len(), uuid_v7());
			meta.tenant_id = TENANT;
			let today = crate::schema::table::validate_table_write_request_batch(
				&meta,
				&named_rows(&descriptor, &rows),
				&descriptor,
				crate::schema::WriterNodeId::ZERO,
				meta.client_hlc,
			)
			.unwrap()
			.localities[0];
			// Both in position space: the partition each names.
			let today = match crate::schema::table::partitioned_prefix_width(&descriptor) {
				Some(bits) => crate::schema::Locality::new(
					crate::schema::table::partition_for_locality(today, bits)
						.unwrap()
						.get(),
				),
				None => today,
			};
			let route = crate::client::core::parent_decisions_append_only_route(&meta, &descriptor)
				.unwrap();
			crossed += usize::from(today != route);
			write_through_client(client, &descriptor, meta, &rows).await;
			expected.extend(rows.iter().map(|(id, v)| (*id, (*v).to_owned())));
		}
	}
	assert!(
		crossed > 0,
		"premise: Today's preview names another place for some operation"
	);
	expected.sort();
	assert_eq!(read_multiset(&clients, &descriptor).await, expected);
}

/// The lineage holder of an AppendOnly write's row 0 (the coordinator the server
/// admits at) and the nodes the client opened streams to while writing it, in
/// order. The write must commit.
async fn first_hops(
	cluster: &Cluster,
	client: &WendelonClient,
	descriptor: &TableSchemaDescriptor,
	rows: &[(i64, &str)],
) -> (u32, Vec<u32>) {
	let mut meta = write_meta(descriptor, rows.len(), uuid_v7());
	meta.tenant_id = TENANT;
	let position = crate::consensus::decisions::delete_row_position(
		TENANT,
		descriptor,
		super::append_only::identity_of(&meta, 0),
	)
	.unwrap();
	let leader = cluster.leader().await;
	let holder = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.lineages()
				.range(..=position)
				.next_back()
				.map(|(_, lineage)| lineage.holder.id)
				.unwrap()
		})
		.await;
	cluster.network.clear_events();
	write_through_client(client, descriptor, meta, rows).await;
	let hops = cluster
		.network
		.events()
		.into_iter()
		.filter(|event| {
			event.source.map(|node| node.id) == Some(CLIENT_NODE)
				&& event.kind == crate::simulator::network::SimulatedTransportEventKind::OpenBi
		})
		.filter_map(|event| event.target.map(|node| node.id))
		.collect();
	(holder, hops)
}

/// Wendelon ADR 0119 D7 (acceptance 11; Codex 01a1f9ca): the first responder
/// and the lease holder, observed. The streams the client opens for each write
/// are read from the simulated network. In a stable crossed case (Today's
/// preview, no ParentDecisions bit advertised, its first stream to a node other
/// than the holder) the write commits after the holder's hint is followed, its
/// last stream to the holder, within the retry budget. In a direct case (the
/// ParentDecisions route, its first stream to the holder) the write commits on
/// that one stream, with no redirect.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_append_only_write_reaches_its_holder_directly_or_by_its_hint() {
	use crate::network::transport::{NodeCapability, NodeCapabilitySet};
	let (source, clients, descriptor) =
		source_with(super::append_only::append_only_descriptor()).await;
	let old = clients.data[&TENANT].clone();
	let advertised = NodeCapabilitySet::empty()
		.with(NodeCapability::ParentDecisions)
		.with(NodeCapability::ParentDecisionsBatchedLeases)
		.as_u8();
	let routed = client_advertised(&source, storage_actor(), advertised).await;
	let rows = [(1, "same"), (1, "same"), (2, "other")];
	let mut crossed = None;
	let mut direct = None;
	for attempt in 0..48 {
		if crossed.is_none() {
			let (holder, hops) = first_hops(&source, &old, &descriptor, &rows).await;
			eprintln!("old preview, operation {attempt}: holder {holder}, streams {hops:?}");
			if hops.first().is_some_and(|first| *first != holder) {
				crossed = Some((holder, hops));
			}
		}
		if direct.is_none() {
			let (holder, hops) = first_hops(&source, &routed, &descriptor, &rows).await;
			eprintln!("PD route, operation {attempt}: holder {holder}, streams {hops:?}");
			if hops.first() == Some(&holder) {
				direct = Some((holder, hops));
			}
		}
		if crossed.is_some() && direct.is_some() {
			break;
		}
	}
	let (holder, hops) = crossed.expect("a crossed case among 48 operations");
	assert_ne!(hops[0], holder, "the first responder is not the holder");
	assert_eq!(
		hops.last(),
		Some(&holder),
		"the hint is followed to the holder"
	);
	assert!(
		hops.len() <= crate::client::core::DATA_RETRY_BUDGET,
		"within the budget: {hops:?}"
	);
	let (holder, hops) = direct.expect("a direct case among 48 operations");
	assert_eq!(
		hops,
		vec![holder],
		"the holder answers its one stream, no redirect"
	);
}

// ============================================================
//  WENDELON ADR 0118: A DROP SUCCEEDS AT ITS LOGICAL FINALIZE (THE INTERVAL)
// ============================================================
//
// In process, ordinary Full members only (the replacement recovery driver has a
// separate report route; nothing here is claimed for it). Certification is held
// on every member (`hold_certification`, also set in each member's start
// configuration so a reopen keeps it), and so is cleanup. The DROP goes through
// the control server's drop route (`SCHEMA_DROP_REQ`), the path SQL takes.

/// A tenant-1 table in the `analytics` namespace with the fixture's columns.
fn interval_table(id: u8, name: &str) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([id; 16]),
		SchemaGeneration::new(1).get(),
		"analytics",
		name,
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, true),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		crate::schema::LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::SingleRow,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

/// Creates `descriptor` through the operator's schema route; every node applies
/// it before this returns.
async fn create_table(cluster: &Cluster, clients: &Clients, descriptor: &TableSchemaDescriptor) {
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
			SchemaIntentId::new(*descriptor.table_object_id.as_bytes()),
			SchemaMutationKind::Create,
			encode_table_schema_descriptor_v1(descriptor).unwrap(),
		))
		.await
		.unwrap();
	let (table, generation) = (descriptor.table_object_id, descriptor.schema_generation);
	let by = tokio::time::Instant::now() + BOUND;
	for id in NODES {
		while !cluster
			.node(id)
			.state_machine
			.read_state(move |state| state.table_schemas.get(TENANT, table, generation).is_some())
			.await
		{
			assert!(
				tokio::time::Instant::now() < by,
				"node {id} never applied the table"
			);
			tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		}
	}
}

/// Stamps past a floor jump, one per write, increasing.
struct Stamps(std::sync::atomic::AtomicU64);

impl Stamps {
	fn next(&self) -> [u8; 16] {
		uuid_v7_at(self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
	}
}

/// `write`, stamped by `stamps`: the answer, or the client's error.
async fn write_answer(
	clients: &Clients,
	descriptor: &TableSchemaDescriptor,
	rows: &[(i64, &str)],
	stamps: &Stamps,
) -> std::io::Result<MutationAnswer> {
	let mut meta = write_meta(descriptor, rows.len(), stamps.next());
	meta.tenant_id = TENANT;
	let result = clients.data[&TENANT]
		.table_write(TableWriteRequest {
			meta,
			descriptor: descriptor.clone(),
			payload: named_rows(descriptor, rows),
			deadline: Some(std::time::Instant::now() + std::time::Duration::from_secs(10)),
		})
		.await?;
	let write = result.response.write;
	Ok(mutation_answer(
		write.status,
		write.mutation_commit_knowledge(),
	))
}

async fn write_stamped(
	clients: &Clients,
	descriptor: &TableSchemaDescriptor,
	rows: &[(i64, &str)],
	stamps: &Stamps,
) {
	let answer = write_answer(clients, descriptor, rows, stamps).await;
	assert!(
		matches!(answer, Ok(MutationAnswer::Committed { .. })),
		"{answer:?}"
	);
}

/// Whether tenant 1's read of `descriptor` is refused.
async fn read_refused(clients: &Clients, descriptor: &TableSchemaDescriptor) -> bool {
	match export::query_table(&clients.data[&TENANT], TENANT, descriptor).await {
		Err(_) => true,
		Ok(mut result) => loop {
			match result.next_batch().await {
				Err(_) => return true,
				Ok(None) => return false,
				Ok(Some(_)) => {}
			}
		},
	}
}

/// The drop route the control server serves to SQL (`SCHEMA_DROP_REQ`), sent to
/// the leader: success, or the typed admin error it answered.
async fn drop_through_control(
	cluster: &Cluster,
	clients: &Clients,
	descriptor: &TableSchemaDescriptor,
) -> Result<(), WendelonAdminError> {
	use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
	let generation = SchemaGeneration::new(descriptor.schema_generation);
	let request = TableSchemaDropRequest::current(TableSchemaDropFields {
		tenant_id: TENANT,
		table_object_id: descriptor.table_object_id,
		expected_generation: generation,
		operation_id: DropOperationId {
			tenant_id: TENANT,
			table_object_id: descriptor.table_object_id,
			schema_generation: generation,
		},
	});
	let payload = crate::network::wire::encode(&request).unwrap();
	let leader = cluster.leader().await;
	let connection = clients
		.admin
		.factory()
		.get_connection(crate::schema::NodeId {
			id: leader,
			_reserved: 0,
		})
		.await
		.unwrap();
	let mut stream = connection.open_bi().await.unwrap();
	let header = crate::schema::FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: crate::schema::MsgType::SCHEMA_DROP_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: payload.len() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &header, &payload, None, None)
		.await
		.unwrap();
	stream.finish().await.unwrap();
	let frame = read_frame(&mut *stream, clients.admin.pool())
		.await
		.unwrap();
	assert_eq!(
		frame.header.msg_type,
		crate::schema::MsgType::SCHEMA_DROP_RESP.0
	);
	let response: crate::network::control_server::TableSchemaDropResponseWire =
		crate::network::wire::decode(&frame.meta).unwrap();
	if response.accepted {
		Ok(())
	} else {
		Err(response
			.admin_error
			.unwrap_or(WendelonAdminError::Transport {
				message: response.message.unwrap_or_default(),
			}))
	}
}

/// One member's certified base at a checkpoint: its active generation, its
/// installed cut (typed), and how many of its base entries belong to the table.
#[derive(Debug)]
struct Checkpoint {
	active: u64,
	cut: crate::storage::index::certified_base::CutState,
	old_entries: usize,
}

async fn base_checkpoint(cluster: &Cluster, node: u32, table: TableObjectId) -> Checkpoint {
	let index = cluster.node(node).slab.persistent_index().unwrap();
	Checkpoint {
		active: index.base_meta().await.unwrap().active_gid,
		cut: index.installed_cut().await.unwrap(),
		old_entries: index
			.base_entries(false)
			.await
			.unwrap()
			.iter()
			.filter(|entry| entry.table == table)
			.count(),
	}
}

/// Every member's checkpoint, printed under `what`.
async fn checkpoints(cluster: &Cluster, what: &str, table: TableObjectId) -> Vec<Checkpoint> {
	let mut all = Vec::new();
	for node in NODES {
		let checkpoint = base_checkpoint(cluster, node, table).await;
		eprintln!("ADR 0118 interval {what}: node {node} {checkpoint:?}");
		all.push(checkpoint);
	}
	all
}

/// Every member holds G active, G's own installed cut, and old t's two base
/// entries: the old base is active.
async fn assert_old_base_active(
	cluster: &Cluster,
	what: &str,
	table: TableObjectId,
	g: crate::storage::index::certified_base::InstalledCut,
) {
	for (node, checkpoint) in NODES.iter().zip(checkpoints(cluster, what, table).await) {
		assert_eq!(checkpoint.active, g.gid, "{what}: node {node}: G is active");
		assert_eq!(
			checkpoint.cut,
			crate::storage::index::certified_base::CutState::Current(g),
			"{what}: node {node}: G's installed cut"
		);
		assert_eq!(
			checkpoint.old_entries, 2,
			"{what}: node {node}: the old base holds old t"
		);
	}
}

/// Every member holds a generation past G whose installed cut covers the
/// finalize `f`, and no entry of old t.
async fn assert_excluded(cluster: &Cluster, what: &str, table: TableObjectId, g: u64, f: u64) {
	for (node, checkpoint) in NODES.iter().zip(checkpoints(cluster, what, table).await) {
		let crate::storage::index::certified_base::CutState::Current(cut) = checkpoint.cut else {
			panic!("{what}: node {node}: no current installed cut: {checkpoint:?}");
		};
		assert!(checkpoint.active > g, "{what}: node {node}: past G");
		assert_eq!(
			cut.gid, checkpoint.active,
			"{what}: node {node}: the active cut"
		);
		assert!(
			cut.t >= f,
			"{what}: node {node}: the cut covers the finalize"
		);
		assert_eq!(
			checkpoint.old_entries, 0,
			"{what}: node {node}: old t is excluded"
		);
	}
}

/// Holds (or releases) certification and cleanup on every member, now and in
/// its start configuration.
fn hold_everywhere(cluster: &mut Cluster, certification: bool, cleanup: bool) {
	for id in NODES {
		let peer = cluster.node(id).data_server.decision_peer_for_test();
		peer.hold_certification
			.store(certification, std::sync::atomic::Ordering::Release);
		peer.pause_cleanup
			.store(cleanup, std::sync::atomic::Ordering::Release);
		let cfg = cluster.configs.get_mut(&id).unwrap();
		cfg.hold_certification_at_start_for_test = certification;
		cfg.hold_cleanup_at_start_for_test = cleanup;
	}
}

struct Interval {
	cluster: Cluster,
	clients: Clients,
	old: TableSchemaDescriptor,
	u: TableSchemaDescriptor,
	fresh: TableSchemaDescriptor,
	/// The generation active on every member when the DROP succeeded.
	g: u64,
	/// G's installed cut, on every member until the excluding generation.
	g_cut: crate::storage::index::certified_base::InstalledCut,
	/// The logical finalize's index.
	f: u64,
	now: u64,
	stamps: Stamps,
	expected_u: Expected,
	expected_fresh: Expected,
}

/// Old t and u certified into generation G on every member (old t's rows in
/// each member's active base), both holds armed on every member, then the DROP
/// of old t through the control route. At its success every member's active
/// base is still G and still holds old t's entries. Then a fresh t (another
/// id, the same name) is created; it and u are written and read exactly, and
/// the old id is refused.
async fn dropped_with_the_old_base_active() -> Interval {
	let (mut cluster, clients, old) = source_with_table().await;
	let u = interval_table(0x53, "events_u");
	create_table(&cluster, &clients, &u).await;
	write(&clients, &old, &[(1, "t1"), (2, "t2")]).await;
	write(&clients, &u, &[(1, "u1"), (2, "u2")]).await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let seeded = base_checkpoint(&cluster, NODES[0], old.table_object_id).await;
	let crate::storage::index::certified_base::CutState::Current(g_cut) = seeded.cut else {
		panic!("G's installed cut: {seeded:?}");
	};
	assert_old_base_active(&cluster, "seeded G", old.table_object_id, g_cut).await;
	let g = g_cut.gid;

	hold_everywhere(&mut cluster, true, true);
	drop_through_control(&cluster, &clients, &old)
		.await
		.expect("the DROP succeeds at its logical finalize");
	let leader = cluster.leader().await;
	let (f, current) = cluster
		.node(leader)
		.state_machine
		.read_state({
			let table = old.table_object_id;
			move |state| {
				(
					state
						.table_drop_outcomes
						.get(TENANT, table)
						.map(|outcome| outcome.finalize_log_index),
					state
						.decisions
						.as_ref()
						.unwrap()
						.generations()
						.current_gid(),
				)
			}
		})
		.await;
	let f = f.expect("the permanent outcome is recorded");
	assert_eq!(current, g, "no generation committed past G");
	assert_old_base_active(
		&cluster,
		"at the DROP's success",
		old.table_object_id,
		g_cut,
	)
	.await;

	let stamps = Stamps(std::sync::atomic::AtomicU64::new(now + 60_001));
	let fresh = interval_table(0x54, &old.table_name);
	assert_eq!(fresh.namespace, old.namespace);
	create_table(&cluster, &clients, &fresh).await;
	write_stamped(&clients, &fresh, &[(10, "f10")], &stamps).await;
	write_stamped(&clients, &u, &[(3, "u3")], &stamps).await;
	let expected_fresh: Expected = [(10, "f10".to_string())].into();
	let expected_u: Expected = [
		(1, "u1".to_string()),
		(2, "u2".to_string()),
		(3, "u3".to_string()),
	]
	.into();
	let interval = Interval {
		cluster,
		clients,
		old,
		u,
		fresh,
		g,
		g_cut,
		f,
		now,
		stamps,
		expected_u,
		expected_fresh,
	};
	assert_tables_exact(&interval, "after the DROP").await;
	interval
}

/// The fresh t and u read exactly, and the old id is refused for a read and a
/// write.
async fn assert_tables_exact(interval: &Interval, what: &str) {
	assert_eq!(
		read(&interval.clients, &interval.fresh).await,
		interval.expected_fresh,
		"{what}: the fresh t"
	);
	assert_eq!(
		read(&interval.clients, &interval.u).await,
		interval.expected_u,
		"{what}: u"
	);
	assert!(
		read_refused(&interval.clients, &interval.old).await,
		"{what}: a read of the old id is refused"
	);
	let old_write = write_answer(
		&interval.clients,
		&interval.old,
		&[(99, "late")],
		&interval.stamps,
	)
	.await;
	assert!(
		!matches!(old_write, Ok(MutationAnswer::Committed { .. })),
		"{what}: a write of the old id is refused: {old_write:?}"
	);
}

/// An unclean stop and reopen of every member (each start re-applies its holds:
/// cleanup always, certification when `certification_held`); returns once every
/// member validated its base again.
async fn reopen_every_member_uncleanly(interval: &mut Interval, certification_held: bool) {
	for id in NODES {
		interval.cluster.stop(id, true).await;
	}
	for id in NODES {
		interval.cluster.reopen(id).await;
	}
	until_ready(&interval.cluster, None).await;
	let by = tokio::time::Instant::now() + BOUND;
	for id in NODES {
		let validated = interval
			.cluster
			.node(id)
			.data_server
			.base_validated_for_test();
		while !validated.load(std::sync::atomic::Ordering::Acquire) {
			assert!(
				tokio::time::Instant::now() < by,
				"node {id} never validated its base"
			);
			tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		}
		let peer = interval
			.cluster
			.node(id)
			.data_server
			.decision_peer_for_test();
		assert_eq!(
			peer.hold_certification
				.load(std::sync::atomic::Ordering::Acquire),
			certification_held,
			"node {id}: the reopen re-applied its certification setting"
		);
		assert!(
			peer.pause_cleanup
				.load(std::sync::atomic::Ordering::Acquire),
			"node {id}: the reopen kept the cleanup hold"
		);
		let table = interval.old.table_object_id;
		assert!(
			interval
				.cluster
				.node(id)
				.state_machine
				.read_state(move |state| state.table_schemas.latest_by_id(TENANT, table).is_some())
				.await,
			"node {id}: the old descriptor is kept"
		);
	}
	interval.clients = clients(&interval.cluster).await;
}

/// Exports the source and restores it into a fresh cluster: the fresh t and u
/// with their exact rows, and nothing of the old t.
async fn assert_export_and_restore(interval: &Interval, what: &str) {
	let root = crate::test_support::TestDir::new("adr0118-interval-artifact");
	let dir = root.to_path_buf().join("backup");
	let report = export::export(&interval.clients, &dir, PASSPHRASE)
		.await
		.unwrap_or_else(|error| panic!("{what}: export: {error}"));
	let rows = (interval.expected_fresh.len() + interval.expected_u.len()) as u64;
	assert_eq!(
		(report.tables, report.rows),
		(2, rows),
		"{what}: {report:?}"
	);
	let verified = restore::verify(&dir, PASSPHRASE).unwrap();
	let ids: Vec<_> = verified
		.manifest
		.roster
		.tables
		.iter()
		.map(|table| table.descriptor.table_object_id)
		.collect();
	assert!(
		!ids.contains(&interval.old.table_object_id),
		"{what}: no old t"
	);
	assert!(
		ids.contains(&interval.fresh.table_object_id),
		"{what}: the fresh t"
	);
	assert!(ids.contains(&interval.u.table_object_id), "{what}: u");

	let target = Cluster::start().await;
	until_ready(&target, None).await;
	let target_clients = self::clients(&target).await;
	restore::restore(&target_clients, &dir, &verified)
		.await
		.unwrap_or_else(|error| panic!("{what}: restore: {error}"));
	assert_eq!(
		read(&target_clients, &interval.fresh).await,
		interval.expected_fresh,
		"{what}: the restored fresh t"
	);
	assert_eq!(
		read(&target_clients, &interval.u).await,
		interval.expected_u,
		"{what}: the restored u"
	);
	let leader = target.leader().await;
	let table = interval.old.table_object_id;
	assert!(
		target
			.node(leader)
			.state_machine
			.read_state(move |state| state.table_schemas.latest_by_id(TENANT, table).is_none())
			.await,
		"{what}: the restore holds nothing of the old t"
	);
}

/// Wendelon ADR 0118, the pre-exclusion interval: the DROP succeeds at its
/// logical finalize while the old base is active on every member (G holds old
/// t's entries; no excluding generation commits, certification held). The fresh
/// t and u are readable and writable exactly and the old id is refused; after
/// an unclean reopen of every member the old descriptor, its base entries and
/// the gates are kept; a logical export and restore under held cleanup carries
/// the fresh t and u exactly and nothing of the old t.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_drop_succeeds_at_its_logical_finalize_with_the_old_base_active() {
	let mut interval = dropped_with_the_old_base_active().await;
	reopen_every_member_uncleanly(&mut interval, true).await;
	assert_old_base_active(
		&interval.cluster,
		"after the unclean reopen",
		interval.old.table_object_id,
		interval.g_cut,
	)
	.await;
	assert_tables_exact(&interval, "after the unclean reopen").await;
	assert_old_base_active(
		&interval.cluster,
		"before the export",
		interval.old.table_object_id,
		interval.g_cut,
	)
	.await;
	assert_export_and_restore(&interval, "pre-exclusion").await;
	eprintln!(
		"ADR 0118 pre-exclusion interval: DROP finalized at {} with G = {} active on every member",
		interval.f, interval.g
	);
}

/// Wendelon ADR 0118, the post-exclusion interval: the certification hold is
/// released, and the excluding generation (cut at or past the finalize) is
/// observed active on every member with old t's entries gone from each base;
/// then, cleanup still held, the same checks hold (exact fresh t and u, the old
/// id refused, an unclean reopen, export and restore). Only the ADR 0117 local
/// cleanup follows the cleanup release; nothing of tier or L2 is claimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn after_the_excluding_generation_the_interval_checks_still_hold() {
	let mut interval = dropped_with_the_old_base_active().await;
	hold_everywhere(&mut interval.cluster, false, true);
	jump_floor(&interval.cluster, interval.now + 120_000).await;
	interval
		.stamps
		.0
		.store(interval.now + 120_001, std::sync::atomic::Ordering::SeqCst);
	until_active(&interval.cluster, interval.g + 1).await;
	let leader = interval.cluster.leader().await;
	let (covering_t, policy) = interval
		.cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			let generations = state.decisions.as_ref().unwrap().generations();
			(generations.fr(), state.survivor_policy_at(generations.fr()))
		})
		.await;
	assert!(
		covering_t >= interval.f,
		"the covering cut is past the finalize"
	);
	assert_eq!(policy, 2, "the excluding policy");
	assert_excluded(
		&interval.cluster,
		"after the excluding generation",
		interval.old.table_object_id,
		interval.g,
		interval.f,
	)
	.await;
	assert_tables_exact(&interval, "after the excluding generation").await;
	reopen_every_member_uncleanly(&mut interval, false).await;
	assert_tables_exact(&interval, "after the unclean reopen").await;
	assert_excluded(
		&interval.cluster,
		"before the export",
		interval.old.table_object_id,
		interval.g,
		interval.f,
	)
	.await;
	assert_export_and_restore(&interval, "post-exclusion").await;
}

// ============================================================
//  WENDELON ADR 0118 (r2): OVERLAPPING ATTEMPTS AT ONE DROP, AND THE HOLD
// ============================================================
//
// Attempt B is held by a one-shot pause right after its control-route snapshot
// (it saw the table unfenced); attempt A then moves the replicated state on;
// B is released. B's public answer must follow the replicated state.

/// Attempt A's Prepare, proposed directly; its fence index.
async fn prepare_through_raft(cluster: &Cluster, descriptor: &TableSchemaDescriptor) -> u64 {
	let generation = SchemaGeneration::new(descriptor.schema_generation);
	let operation = DropOperationId {
		tenant_id: TENANT,
		table_object_id: descriptor.table_object_id,
		schema_generation: generation,
	};
	let leader = cluster.leader().await;
	let prepared = cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::TableDropPrepare(
			crate::consensus::TableDropPrepareRequest::current(operation, generation),
		))
		.await
		.unwrap();
	assert!(
		matches!(prepared.data, crate::consensus::WendelonResponse::Success),
		"{:?}",
		prepared.data
	);
	prepared.log_id.index
}

/// The recorded outcome's fence index for `descriptor`, from the leader.
async fn recorded_fence(cluster: &Cluster, descriptor: &TableSchemaDescriptor) -> Option<u64> {
	let leader = cluster.leader().await;
	let table = descriptor.table_object_id;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.table_drop_tombstones
				.get(TENANT, table)
				.map(|tombstone| tombstone.fence_log_index)
		})
		.await
}

/// Attempt B, started and held after its unfenced snapshot.
async fn held_attempt(
	cluster: &Arc<Cluster>,
	clients: &Arc<Clients>,
	descriptor: &TableSchemaDescriptor,
) -> (
	tokio::task::JoinHandle<Result<(), WendelonAdminError>>,
	Arc<tokio::sync::Notify>,
) {
	let (reached, release) =
		crate::network::control_server::adr0118_test_support::pause_next_drop_after_snapshot(
			descriptor.table_object_id,
		);
	let attempt = tokio::spawn({
		let (cluster, clients, descriptor) = (cluster.clone(), clients.clone(), descriptor.clone());
		async move { drop_through_control(&cluster, &clients, &descriptor).await }
	});
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("attempt B reached its snapshot");
	(attempt, release)
}

/// A finishes the whole DROP while B is held: B, released, succeeds (the same
/// operation's recorded outcome), and the outcome is A's.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_held_attempt_succeeds_after_the_other_attempt_finished() {
	let (cluster, clients, descriptor) = source_with_table().await;
	let (cluster, clients) = (Arc::new(cluster), Arc::new(clients));
	let (b, release) = held_attempt(&cluster, &clients, &descriptor).await;
	drop_through_control(&cluster, &clients, &descriptor)
		.await
		.expect("attempt A");
	let fence = recorded_fence(&cluster, &descriptor).await;
	release.notify_one();
	assert_eq!(b.await.unwrap(), Ok(()), "attempt B");
	assert_eq!(
		recorded_fence(&cluster, &descriptor).await,
		fence,
		"A's outcome"
	);
}

/// A only prepares while B is held: B, released, finalizes against A's fence.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_held_attempt_finalizes_against_the_other_attempts_fence() {
	let (cluster, clients, descriptor) = source_with_table().await;
	let (cluster, clients) = (Arc::new(cluster), Arc::new(clients));
	let (b, release) = held_attempt(&cluster, &clients, &descriptor).await;
	let fence = prepare_through_raft(&cluster, &descriptor).await;
	release.notify_one();
	assert_eq!(b.await.unwrap(), Ok(()), "attempt B");
	assert_eq!(
		recorded_fence(&cluster, &descriptor).await,
		Some(fence),
		"finalized against A's fence"
	);
}

/// A prepares and a hold is entered while B is held: B answers Fenced (the
/// table is fenced; never NotBegun). After the release the same DROP completes,
/// against A's fence.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_held_attempt_under_a_later_hold_reports_the_fence() {
	let (cluster, clients, descriptor) = source_with_table().await;
	let (cluster, clients) = (Arc::new(cluster), Arc::new(clients));
	let (b, release) = held_attempt(&cluster, &clients, &descriptor).await;
	let fence = prepare_through_raft(&cluster, &descriptor).await;
	let hold = begin_hold_through_raft(&cluster, 18).await;
	release.notify_one();
	assert_eq!(
		b.await.unwrap(),
		Err(WendelonAdminError::BackupHoldActive {
			progress: crate::integration::compute::HeldDropProgress::Fenced
		})
	);
	release_hold_through_raft(&cluster, hold).await;
	drop_through_control(&cluster, &clients, &descriptor)
		.await
		.expect("the same DROP after the release");
	assert_eq!(recorded_fence(&cluster, &descriptor).await, Some(fence));
}

/// A finishes and a hold is entered while B is held: B succeeds (the drop is
/// recorded; the hold labels nothing).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_held_attempt_after_the_outcome_succeeds_under_a_hold() {
	let (cluster, clients, descriptor) = source_with_table().await;
	let (cluster, clients) = (Arc::new(cluster), Arc::new(clients));
	let (b, release) = held_attempt(&cluster, &clients, &descriptor).await;
	drop_through_control(&cluster, &clients, &descriptor)
		.await
		.expect("attempt A");
	let hold = begin_hold_through_raft(&cluster, 19).await;
	release.notify_one();
	assert_eq!(b.await.unwrap(), Ok(()), "attempt B");
	release_hold_through_raft(&cluster, hold).await;
}

/// The two plain orderings at the public boundary. Hold first: NotBegun, and no
/// fence. Prepare first, then the hold: Fenced. After each release the same
/// DROP completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_public_drop_answer_under_a_hold_follows_its_progress() {
	let (cluster, clients, descriptor) = source_with_table().await;
	let hold = begin_hold_through_raft(&cluster, 20).await;
	assert_eq!(
		drop_through_control(&cluster, &clients, &descriptor).await,
		Err(WendelonAdminError::BackupHoldActive {
			progress: crate::integration::compute::HeldDropProgress::NotBegun
		})
	);
	let leader = cluster.leader().await;
	let table = descriptor.table_object_id;
	assert!(
		cluster
			.node(leader)
			.state_machine
			.read_state(move |state| state.table_drop_fences.get(TENANT, table).is_none())
			.await,
		"no fence"
	);
	release_hold_through_raft(&cluster, hold).await;

	let fence = prepare_through_raft(&cluster, &descriptor).await;
	let hold = begin_hold_through_raft(&cluster, 21).await;
	assert_eq!(
		drop_through_control(&cluster, &clients, &descriptor).await,
		Err(WendelonAdminError::BackupHoldActive {
			progress: crate::integration::compute::HeldDropProgress::Fenced
		})
	);
	release_hold_through_raft(&cluster, hold).await;
	drop_through_control(&cluster, &clients, &descriptor)
		.await
		.expect("the same DROP after the release");
	assert_eq!(recorded_fence(&cluster, &descriptor).await, Some(fence));
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

/// ADR 0106 (wendelon) control 9: a logical backup while a lost member's slot
/// is replaced by a control-only member. The exporter reads every table through
/// the ordinary table query; a query attempt that reaches the replacement's
/// slot is answered NOT_LEADER (NotServing) and the client asks another member.
/// The export completes with every row, and its artifact verifies.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_backup_completes_while_a_replacement_is_control_only() {
	use crate::consensus::decisions::instances::ReplacementPhase;
	use crate::consensus::decisions::{DecisionCommand, DecisionResult};
	let (mut source, clients, descriptor) = source_with_table().await;
	for id in 1..=10i64 {
		write(&clients, &descriptor, &[(id, "a")]).await;
	}
	let leader = source.leader().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	source.stop(lost, true).await;
	let cfg = source.configs[&lost].clone();
	std::fs::remove_dir_all(&cfg.data_dir).unwrap();
	std::fs::create_dir_all(&cfg.data_dir).unwrap();
	const TOKEN: u128 = 0x0106_0009;
	assert!(matches!(
		super::replacement::decide(
			&source,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			},
		)
		.await,
		DecisionResult::ReplacementRecorded { .. }
	));
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	let provider: Arc<dyn TransportProvider> = Arc::new(SimulatedTransportProvider::new(
		crate::schema::NodeId {
			id: lost,
			_reserved: 0,
		},
		source.network.clone(),
		crate::network::pool::BufPool::new(64, 1024 * 1024),
		TIER,
	));
	let control = crate::node::ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider)
		.await
		.unwrap();
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
	loop {
		let leader = source.leader().await;
		let done = source
			.node(leader)
			.state_machine
			.read_state(|state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.instances().replacement(TOKEN))
					.map(|record| matches!(record.phase, ReplacementPhase::Done { .. }))
			})
			.await == Some(true);
		if done {
			break;
		}
		assert!(tokio::time::Instant::now() < deadline, "never Done");
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
	let root = crate::test_support::TestDir::new("adr0106-backup");
	let dir = root.to_path_buf().join("backup");
	let report = export::export(&clients, &dir, PASSPHRASE)
		.await
		.unwrap_or_else(|error| panic!("export with a control-only member: {error}"));
	assert_eq!((report.tables, report.rows), (1, 10), "{report:?}");
	assert_eq!(hold_of(&source).await, None, "the export released its hold");
	let verified = restore::verify(&dir, PASSPHRASE).unwrap();
	assert_eq!(verified.manifest.tables[0].rows, 10);
	control.shutdown().await.unwrap();
	for node in std::mem::take(&mut source.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// One CTAS chunk of operation `op` at `sequence_no`, written by tenant 1's
/// storage client with the owner's credential (Toron's path); it must commit.
async fn ctas_chunk(
	clients: &Clients,
	descriptor: &TableSchemaDescriptor,
	op: [u8; 16],
	sequence_no: u64,
	rows: &[(i64, &str)],
) {
	let mut meta = super::ctas::chunk_meta_at(descriptor, op, sequence_no);
	meta.tenant_id = TENANT;
	meta.row_count = encode_table_write_row_count(rows.len()).unwrap();
	let result = clients.data[&TENANT]
		.ctas_chunk_write(
			TableWriteRequest {
				meta,
				descriptor: descriptor.clone(),
				payload: named_rows(descriptor, rows),
				deadline: Some(std::time::Instant::now() + std::time::Duration::from_secs(30)),
			},
			crate::client::core::CtasChunkCredential {
				op,
				secret: super::ctas::SECRET,
			},
		)
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

/// Tenant 1's CTAS operation declaring `chunks`, begun at the leader. Its id.
async fn ctas_begun(
	cluster: &Cluster,
	descriptor: &TableSchemaDescriptor,
	chunks: &[&[(i64, &str)]],
) -> [u8; 16] {
	use crate::consensus::decisions::DecisionResult;
	let op = uuid_v7();
	let leader = cluster.leader().await;
	let begin = super::ctas::begin_chunks_in(TENANT, descriptor, op, chunks);
	assert!(matches!(
		super::ctas::decided(
			super::ctas::ctas_command(cluster, leader, &begin).await,
			"Begin"
		),
		DecisionResult::CtasBegun { .. }
	));
	op
}

/// Tenant 1's chain `op` on the leader, by `phase`.
async fn ctas_phase(
	cluster: &Cluster,
	op: [u8; 16],
) -> Option<(crate::consensus::decisions::ctas::CtasPhase, u128)> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			let chain = state.decisions.as_ref()?.ctas().chain(TENANT, op)?;
			Some((chain.phase, chain.accepted))
		})
		.await
}

/// Wendelon ADR 0120 S4 (Codex 01a1fbf1): CTAS through a real export and
/// restore, as tenant 1 with the deployment's credentials. On the source:
/// P is published by its owner (duplicates within and across its two chunks);
/// Q's owner stops after its one chunk commits, and A's after the first of its
/// two. With no client traffic, once the committed clock passes their leases,
/// maintenance publishes Q and abandons A (its accepted rows never visible). The
/// backup holds the ordinary table, P and Q with their exact multisets and not
/// A; restored into a fresh cluster, P and Q read exactly with the target's
/// storage credential, A has no table there, and P takes an ordinary AppendOnly
/// append.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_published_ctas_round_trips_and_an_abandoned_ones_rows_never_do() {
	use super::ctas::ROWS;
	use crate::consensus::decisions::DecisionResult;
	use crate::consensus::decisions::ctas::{CtasAbandonCause, CtasPhase};
	use crate::network::decision_proposals::CtasProposal;
	const MORE: [(i64, &str); 2] = [(1, "same"), (4, "late")];
	let sorted = |parts: &[&[(i64, &str)]]| {
		let mut rows: Vec<(i64, String)> = parts
			.iter()
			.flat_map(|part| part.iter().map(|(id, v)| (*id, (*v).to_owned())))
			.collect();
		rows.sort();
		rows
	};
	let (source, clients, ordinary) = source_with_table().await;
	write(&clients, &ordinary, &[(1, "a")]).await;

	let p = super::ctas::target(0xE1, "ctas_published");
	let op_p = ctas_begun(&source, &p, &[&ROWS, &MORE]).await;
	ctas_chunk(&clients, &p, op_p, 1, &ROWS).await;
	ctas_chunk(&clients, &p, op_p, 2, &MORE).await;
	let leader = source.leader().await;
	let publish = CtasProposal::Publish {
		tenant: TENANT,
		op: op_p,
		epoch: 1,
		secret: super::ctas::SECRET,
	};
	assert_eq!(ctas_phase(&source, op_p).await.map(|(_, a)| a), Some(0b11));
	assert!(matches!(
		super::ctas::decided(
			super::ctas::ctas_command(&source, leader, &publish).await,
			"Publish"
		),
		DecisionResult::CtasPublished { .. }
	));
	let q = super::ctas::target(0xE2, "ctas_recovered");
	let op_q = ctas_begun(&source, &q, &[&ROWS]).await;
	ctas_chunk(&clients, &q, op_q, 1, &ROWS).await;
	let a = super::ctas::target(0xE3, "ctas_abandoned");
	let op_a = ctas_begun(&source, &a, &[&ROWS, &MORE]).await;
	ctas_chunk(&clients, &a, op_a, 1, &ROWS).await;

	// The owners of Q and A stop. No client traffic: the committed clock passes
	// their leases and maintenance settles both.
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(&source, now + 60_000).await;
	source
		.until("maintenance settled Q and A", move |state| {
			let Some(decisions) = state.decisions.as_ref() else {
				return false;
			};
			let ctas = decisions.ctas();
			matches!(
				ctas.chain(TENANT, op_q).map(|chain| chain.phase),
				Some(CtasPhase::Published { .. })
			) && matches!(
				ctas.chain(TENANT, op_a).map(|chain| chain.phase),
				Some(CtasPhase::Abandoned {
					cause: CtasAbandonCause::SourceBytesLost,
					..
				})
			)
		})
		.await;
	assert_eq!(
		ctas_phase(&source, op_a)
			.await
			.map(|(_, accepted)| accepted),
		Some(0b01),
		"A had accepted rows"
	);
	let expected_p = sorted(&[&ROWS, &MORE]);
	let expected_q = sorted(&[&ROWS]);
	// DC-281, the captured failure: a node activated the newly committed
	// generation (here the one A's retirement triggers) some 50-100 ms after the
	// others, and a read it received meanwhile was refused on each of the
	// client's three attempts (two 20 ms backoffs; the refusal hints the same
	// node). Its fix is a separate follow-up; this control reads once every node
	// serves the committed generation.
	let committed = source
		.node(leader)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.generations()
				.current_gid()
		})
		.await;
	until_active(&source, committed).await;
	assert_eq!(read_multiset(&clients, &p).await, expected_p);
	assert_eq!(read_multiset(&clients, &q).await, expected_q);

	let root = crate::test_support::TestDir::new("adr0120-artifact");
	let dir = root.to_path_buf().join("backup");
	let report = export::export(&clients, &dir, PASSPHRASE)
		.await
		.unwrap_or_else(|error| panic!("export: {error}"));
	assert_eq!((report.tables, report.rows), (3, 1 + 5 + 3), "{report:?}");
	let verified = restore::verify(&dir, PASSPHRASE).unwrap();
	let in_backup = |object: TableObjectId| {
		verified
			.manifest
			.roster
			.tables
			.iter()
			.find(|table| table.descriptor.table_object_id == object)
			.map(|table| table.descriptor.clone())
	};
	let (Some(backup_p), Some(backup_q)) =
		(in_backup(p.table_object_id), in_backup(q.table_object_id))
	else {
		panic!(
			"the backup lists P and Q: {:?}",
			verified.manifest.roster.tables
		);
	};
	assert!(in_backup(ordinary.table_object_id).is_some());
	assert_eq!(in_backup(a.table_object_id), None, "the abandoned object");
	assert_eq!(backup_p.table_name, "ctas_published");

	let target = Cluster::start().await;
	until_ready(&target, None).await;
	let target_clients = self::clients(&target).await;
	let restored = restore::restore(&target_clients, &dir, &verified)
		.await
		.unwrap_or_else(|error| panic!("restore: {error}"));
	assert_eq!((restored.tables, restored.rows), (3, 9), "{restored:?}");
	assert_eq!(read_multiset(&target_clients, &backup_p).await, expected_p);
	assert_eq!(read_multiset(&target_clients, &backup_q).await, expected_q);
	assert_eq!(
		read(&target_clients, &ordinary).await,
		Expected::from([(1, "a".to_owned())])
	);
	let a_object = a.table_object_id;
	for id in NODES {
		assert!(
			target
				.node(id)
				.state_machine
				.read_state(move |state| state.table_schemas.get(TENANT, a_object, 1).is_none())
				.await,
			"node {id}: no table for the abandoned object"
		);
	}

	write(&target_clients, &backup_p, &[(1, "same")]).await;
	let mut appended = expected_p.clone();
	appended.push((1, "same".to_owned()));
	appended.sort();
	assert_eq!(read_multiset(&target_clients, &backup_p).await, appended);
}
