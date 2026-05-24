//! Server-side Control Ring surface.
//!
//! This module hangs off the existing HRFT listener and
//! [`MsgRouter`](crate::network::router::MsgRouter) demux rather than
//! introducing a separate control-ring bind. It assumes callers are
//! already cluster-internal and mTLS-authenticated at the transport
//! layer; the handler here focuses on routing, wire compatibility, and
//! leader-aware Control Ring behavior.

use crate::audit::{AuditChainHead, AuditSink};
use crate::client::topology::{ClusterPeerWire, ClusterStatusWire};
use crate::consensus::{
	AdvanceEpochRecord, CreateTenant, EpochConfig, EpochPeer, MigrationLedgerEntry, MigrationState,
	RevokeActorKey, SeedMigration, TableDropFinalizeRequest, TableDropPrepareRequest,
	TableReclaimCompletionRequest, TenantLifecycleState, TenantRecord, UpdateTenantQuota,
	WendelonRaft, WendelonRequest, WendelonResponse,
};
use crate::crypto::{CryptoManager, RetireTenantKekVersion};
use crate::governance::{AdministrativeAction, require_administrative_action};
use crate::integration::compute::{
	SchemaMutationKindV1, TableSchemaDropRequest, WendelonAdminError,
	decode_schema_update_envelope_v1, encode_logical_type_definition_v1,
	encode_logical_type_definitions_v1,
};
use crate::metrics::{TableRetentionMetricPolicy, global as metrics};
use crate::network::core::{HRFT_MAGIC, HrftFrame, read_frame, write_frame};
use crate::network::factory::TransportProviderFactory;
use crate::network::lease_signing::LeaseAuthority;
use crate::network::pool::BufPool;
use crate::network::ring::RingState;
use crate::network::router::ControlRingServer;
use crate::network::server::RequestContext;
use crate::network::tls::{ActorIdentity, ActorPermission, ActorTrustDomain, authorize_actor};
use crate::network::traits::{TransferLeadershipRequestMeta, TransferLeadershipResponseMeta};
use crate::network::transfer_signing::TransferAuthority;
use crate::network::transport::{
	MembershipEpoch, NodeCapabilitySet, NodeRole, NodeRoleSet, PeerCapabilities, TransportTier,
	TransportTierSet,
};
use crate::network::wire;
use crate::schema::table::decode_descriptor_wire_name;
use crate::schema::{
	AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, FrameHeader, HybridTimestamp,
	LeaseDescriptor, LeaseStatus, LogicalTypeGetRequestMeta, LogicalTypeGetResponseMeta,
	LogicalTypeListRequestMeta, LogicalTypeListResponseMeta, MsgType, NodeId, PolicyGeneration,
	ProtocolVersionWindow, ReleaseLeaseRequestMeta, ReleaseLeaseResponseMeta,
	TableDescriptorGetRequestMeta, TableDescriptorGetResponseMeta, TableObjectId,
	TableRetentionPolicy, TableSchemaDescriptor, TenantId, TenantKekId, TransferToken,
	decode_table_schema_descriptor_v1, encode_table_schema_descriptor_v1, epoch_entry_var_schema,
};
#[cfg(test)]
use crate::schema::{SupersessionMetadata, TombstoneMetadata};
use crate::storage::{RaftCommitter, manager::SlabManager};
use arrow::array::{Array, ListArray, StringArray, UInt32Array};
use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, watch};

pub const CONTROL_REASON_GRANTED: u8 = 0;
pub const CONTROL_REASON_NOT_LEADER: u8 = 1;
pub const CONTROL_REASON_EPOCH_STALE: u8 = 2;
pub const CONTROL_REASON_INVALID_TARGET: u8 = 3;
pub const CONTROL_REASON_UNSUPPORTED: u8 = 4;
pub const CONTROL_REASON_INVALID_CONFIG: u8 = 5;
pub const CONTROL_REASON_INTERNAL_ERROR: u8 = 6;
pub const CONTROL_REASON_INVALID_TOKEN: u8 = 7;
pub const CONTROL_REASON_INCOMPATIBLE_VERSION: u8 = 8;
pub const CONTROL_REASON_AUTHORIZATION_FAILED: u8 = 9;
pub const CONTROL_REASON_NOT_FOUND: u8 = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ControlOperation {
	Status,
	AuditChainHead,
	RevocationAdmin,
	Lease,
	LeaseRelease,
	Transfer,
	AdvanceEpoch,
	Join,
	SchemaUpdate,
	SchemaDrop,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TableDescriptorGetRequest {
	tenant_id: TenantId,
	namespace: String,
	table_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogicalTypeGetRequest {
	tenant_id: TenantId,
	name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DescriptorMutationMetric {
	LogicalOnly,
	PhysicalOnly,
	Combined,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvanceEpochResponseWire {
	pub accepted: bool,
	pub reason_code: u8,
	pub leader_hint: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinClusterRequestWire {
	pub node_id: u32,
	pub advertise_address: String,
	pub tier_bits: u8,
	pub role_bits: u8,
	#[serde(default)]
	pub capability_bits: u8,
	#[serde(default)]
	pub protocol: ProtocolVersionWindow,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinClusterResponseWire {
	pub accepted: bool,
	pub reason_code: u8,
	pub leader_hint: Option<u32>,
	#[serde(default)]
	pub local_protocol: ProtocolVersionWindow,
	#[serde(default)]
	pub negotiated_protocol: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditChainHeadResponseWire {
	#[serde(default)]
	pub head: Option<AuditChainHead>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaUpdateResponseWire {
	pub accepted: bool,
	pub reason_code: u8,
	pub leader_hint: Option<u32>,
	#[serde(default)]
	pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchemaDropResponseWire {
	pub accepted: bool,
	pub reason_code: u8,
	pub leader_hint: Option<u32>,
	#[serde(default)]
	pub admin_error: Option<WendelonAdminError>,
	#[serde(default)]
	pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenantAdminRequestWire {
	Create {
		target_tenant_id: TenantId,
		quota_bytes: u64,
		replication_factor: u32,
	},
	UpdateQuota {
		target_tenant_id: TenantId,
		quota_bytes: u64,
	},
	Suspend {
		target_tenant_id: TenantId,
	},
	Resume {
		target_tenant_id: TenantId,
	},
	Delete {
		target_tenant_id: TenantId,
	},
	RotateTenantKek {
		target_tenant_id: TenantId,
	},
	RetireTenantKek {
		target_tenant_id: TenantId,
		tenant_kek_id: TenantKekId,
	},
	Get {
		target_tenant_id: TenantId,
	},
	List,
	SetTableRetentionPolicy {
		target_tenant_id: TenantId,
		table_object_id: TableObjectId,
		expected_policy_generation: PolicyGeneration,
		new_policy: TableRetentionPolicy,
	},
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantAdminTenantWire {
	pub tenant_id: TenantId,
	pub quota_bytes: u64,
	pub used_bytes: u64,
	pub replication_factor: u32,
	pub state: TenantLifecycleState,
	#[serde(default)]
	pub active_tenant_kek_id: Option<TenantKekId>,
	#[serde(default)]
	pub tenant_kek_accounting_complete: bool,
	#[serde(default)]
	pub tenant_kek_versions: Vec<TenantAdminTenantKekVersionWire>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantAdminTenantKekVersionWire {
	pub tenant_kek_id: TenantKekId,
	pub is_active: bool,
	pub tiered_slab_ref_count: u64,
	#[serde(default)]
	pub safe_to_retire: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantAdminResponseWire {
	pub accepted: bool,
	pub reason_code: u8,
	pub leader_hint: Option<u32>,
	#[serde(default)]
	pub tenant: Option<TenantAdminTenantWire>,
	#[serde(default)]
	pub tenants: Vec<TenantAdminTenantWire>,
	#[serde(default)]
	pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RevocationAdminRequestWire {
	Revoke {
		trust_domain: ActorTrustDomain,
		#[serde(with = "serde_bytes")]
		key_id: Vec<u8>,
	},
	List,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevokedActorKeyWire {
	pub trust_domain: ActorTrustDomain,
	#[serde(with = "serde_bytes")]
	pub key_id: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevocationAdminResponseWire {
	pub accepted: bool,
	pub reason_code: u8,
	pub leader_hint: Option<u32>,
	#[serde(default)]
	pub entries: Vec<RevokedActorKeyWire>,
	#[serde(default)]
	pub message: Option<String>,
}

pub struct WendelonControlServer {
	raft: WendelonRaft,
	state_machine: crate::consensus::raft_storage::WendelonStateMachine,
	crypto: Arc<CryptoManager>,
	local_node: NodeId,
	epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	epoch_rx: watch::Receiver<Arc<MembershipEpoch>>,
	factory: Arc<TransportProviderFactory>,
	pool: Arc<BufPool>,
	replication_factor: u32,
	lease_ttl_cap_ms: u32,
	audit_sink: Arc<dyn AuditSink>,
	lease_authority: Arc<LeaseAuthority>,
	transfer_authority: Arc<TransferAuthority>,
	storage_manager: Option<Arc<SlabManager>>,
	protocol: ProtocolVersionWindow,
	reconcile_lock: Mutex<()>,
}

impl WendelonControlServer {
	pub fn new(
		raft: WendelonRaft,
		state_machine: crate::consensus::raft_storage::WendelonStateMachine,
		crypto: Arc<CryptoManager>,
		local_node: NodeId,
		epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
		epoch_rx: watch::Receiver<Arc<crate::network::transport::MembershipEpoch>>,
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
		replication_factor: u32,
		lease_ttl_cap_ms: u32,
		audit_sink: Arc<dyn AuditSink>,
	) -> Self {
		Self {
			raft,
			state_machine,
			crypto,
			local_node,
			epoch_tx,
			epoch_rx,
			factory,
			pool,
			replication_factor,
			lease_ttl_cap_ms,
			audit_sink,
			lease_authority: Arc::new(LeaseAuthority::dev_single_node(local_node)),
			transfer_authority: Arc::new(TransferAuthority::dev_single_node(local_node)),
			storage_manager: None,
			protocol: ProtocolVersionWindow::default(),
			reconcile_lock: Mutex::new(()),
		}
	}

	pub fn with_audit_sink(mut self, audit_sink: Arc<dyn AuditSink>) -> Self {
		self.audit_sink = audit_sink;
		self
	}

	pub fn with_lease_authority(mut self, authority: Arc<LeaseAuthority>) -> Self {
		self.lease_authority = authority;
		self
	}

	pub fn with_transfer_authority(mut self, authority: Arc<TransferAuthority>) -> Self {
		self.transfer_authority = authority;
		self
	}

	pub fn with_storage_manager(mut self, storage_manager: Arc<SlabManager>) -> Self {
		self.storage_manager = Some(storage_manager);
		self
	}

	pub fn with_protocol_window(mut self, protocol: ProtocolVersionWindow) -> Self {
		self.protocol = protocol;
		self
	}

	fn authorize_control_operation(
		&self,
		context: &RequestContext,
		op: ControlOperation,
	) -> std::io::Result<()> {
		// This is the coarse ring-level gate. Action-specific
		// overlays like tenant-scope rejection, human-only admin
		// restrictions, and break-glass-only destructive paths are
		// enforced later by `require_administrative_action(...)`.
		let permission = match op {
			ControlOperation::Status => ActorPermission::ControlStatus,
			ControlOperation::AuditChainHead => ActorPermission::AuditRead,
			ControlOperation::RevocationAdmin => ActorPermission::ClusterControl,
			ControlOperation::Lease
			| ControlOperation::LeaseRelease
			| ControlOperation::Transfer
			| ControlOperation::AdvanceEpoch
			| ControlOperation::Join => ActorPermission::ClusterControl,
			ControlOperation::SchemaUpdate | ControlOperation::SchemaDrop => {
				ActorPermission::TenantAdminMutation
			}
		};
		authorize_actor(&context.actor, permission).map_err(|error| {
			Error::new(
				ErrorKind::PermissionDenied,
				format!("{op:?} authorization failed: {error}"),
			)
		})
	}

	fn build_tenant_admin_frame(
		&self,
		stream_id: u32,
		resp: &TenantAdminResponseWire,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, resp)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TENANT_ADMIN_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	fn build_revocation_admin_frame(
		&self,
		stream_id: u32,
		resp: &RevocationAdminResponseWire,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, resp)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::REVOCATION_ADMIN_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	fn build_schema_update_frame(
		&self,
		stream_id: u32,
		resp: &SchemaUpdateResponseWire,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, resp)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::SCHEMA_UPDATE_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	fn build_schema_drop_frame(
		&self,
		stream_id: u32,
		resp: &TableSchemaDropResponseWire,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, resp)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::SCHEMA_DROP_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	fn build_table_descriptor_get_frame(
		&self,
		stream_id: u32,
		meta: TableDescriptorGetResponseMeta,
		descriptor: Option<&[u8]>,
	) -> std::io::Result<HrftFrame> {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let trailing = descriptor.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(bytes);
			buf
		});
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_DESCRIPTOR_GET_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len: trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0),
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing,
		})
	}

	fn build_logical_type_get_frame(
		&self,
		stream_id: u32,
		meta: LogicalTypeGetResponseMeta,
		definition: Option<&[u8]>,
	) -> std::io::Result<HrftFrame> {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let trailing = definition.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(bytes);
			buf
		});
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::LOGICAL_TYPE_GET_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len: trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0),
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing,
		})
	}

	fn build_logical_type_list_frame(
		&self,
		stream_id: u32,
		meta: LogicalTypeListResponseMeta,
		definitions: Option<&[u8]>,
	) -> std::io::Result<HrftFrame> {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let trailing = definitions.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(bytes);
			buf
		});
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::LOGICAL_TYPE_LIST_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len: trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0),
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing,
		})
	}

	fn tenant_admin_rejection(
		&self,
		frame: &HrftFrame,
		reason_code: u8,
		message: impl Into<String>,
		leader_hint: Option<NodeId>,
	) -> std::io::Result<HrftFrame> {
		self.build_tenant_admin_frame(
			frame.header.stream_id,
			&TenantAdminResponseWire {
				accepted: false,
				reason_code,
				leader_hint: leader_hint.map(|node| node.id),
				tenant: None,
				tenants: Vec::new(),
				message: Some(message.into()),
			},
		)
	}

	fn revocation_admin_rejection(
		&self,
		frame: &HrftFrame,
		reason_code: u8,
		message: impl Into<String>,
		leader_hint: Option<NodeId>,
	) -> std::io::Result<HrftFrame> {
		self.build_revocation_admin_frame(
			frame.header.stream_id,
			&RevocationAdminResponseWire {
				accepted: false,
				reason_code,
				leader_hint: leader_hint.map(|node| node.id),
				entries: Vec::new(),
				message: Some(message.into()),
			},
		)
	}

	fn schema_update_rejection(
		&self,
		frame: &HrftFrame,
		reason_code: u8,
		message: impl Into<String>,
		leader_hint: Option<NodeId>,
	) -> std::io::Result<HrftFrame> {
		self.build_schema_update_frame(
			frame.header.stream_id,
			&SchemaUpdateResponseWire {
				accepted: false,
				reason_code,
				leader_hint: leader_hint.map(|node| node.id),
				message: Some(message.into()),
			},
		)
	}

	fn schema_drop_rejection(
		&self,
		frame: &HrftFrame,
		reason_code: u8,
		admin_error: WendelonAdminError,
		leader_hint: Option<NodeId>,
	) -> std::io::Result<HrftFrame> {
		self.build_schema_drop_frame(
			frame.header.stream_id,
			&TableSchemaDropResponseWire {
				accepted: false,
				reason_code,
				leader_hint: leader_hint.map(|node| node.id),
				message: Some(format!("{admin_error:?}")),
				admin_error: Some(admin_error),
			},
		)
	}

	fn table_descriptor_get_rejection(
		&self,
		frame: &HrftFrame,
		reason_code: u8,
		leader_hint: Option<NodeId>,
	) -> std::io::Result<HrftFrame> {
		Self::observe_table_descriptor_get_reason(reason_code);
		self.build_table_descriptor_get_frame(
			frame.header.stream_id,
			TableDescriptorGetResponseMeta {
				accepted: 0,
				reason_code,
				_pad1: [0; 6],
				leader_hint: leader_hint.unwrap_or(NodeId {
					id: 0,
					_reserved: 0,
				}),
				descriptor_len: 0,
				_pad2: [0; 4],
			},
			None,
		)
	}

	fn logical_type_get_rejection(
		&self,
		frame: &HrftFrame,
		reason_code: u8,
		leader_hint: Option<NodeId>,
	) -> std::io::Result<HrftFrame> {
		self.build_logical_type_get_frame(
			frame.header.stream_id,
			LogicalTypeGetResponseMeta {
				accepted: 0,
				reason_code,
				_pad1: [0; 6],
				leader_hint: leader_hint.unwrap_or(NodeId {
					id: 0,
					_reserved: 0,
				}),
				definition_len: 0,
				_pad2: [0; 4],
			},
			None,
		)
	}

	fn logical_type_list_rejection(
		&self,
		frame: &HrftFrame,
		reason_code: u8,
		leader_hint: Option<NodeId>,
	) -> std::io::Result<HrftFrame> {
		self.build_logical_type_list_frame(
			frame.header.stream_id,
			LogicalTypeListResponseMeta {
				accepted: 0,
				reason_code,
				_pad1: [0; 6],
				leader_hint: leader_hint.unwrap_or(NodeId {
					id: 0,
					_reserved: 0,
				}),
				definitions_len: 0,
				_pad2: [0; 4],
			},
			None,
		)
	}

	fn observe_table_descriptor_get_reason(reason_code: u8) {
		match reason_code {
			CONTROL_REASON_GRANTED => metrics().control_descriptor_get_success_total.inc(),
			CONTROL_REASON_NOT_FOUND => metrics().control_descriptor_get_not_found_total.inc(),
			CONTROL_REASON_NOT_LEADER => metrics().control_descriptor_get_not_leader_total.inc(),
			CONTROL_REASON_AUTHORIZATION_FAILED => metrics()
				.control_descriptor_get_authorization_failed_total
				.inc(),
			CONTROL_REASON_INVALID_CONFIG => {
				metrics().control_descriptor_get_invalid_request_total.inc()
			}
			_ => {}
		}
	}

	fn revoked_actor_keys_wire(
		snapshot: &crate::consensus::raft_storage::SmState,
	) -> Vec<RevokedActorKeyWire> {
		snapshot
			.revoked_actor_keys
			.iter()
			.map(|(entry, _)| RevokedActorKeyWire {
				trust_domain: entry.trust_domain,
				key_id: entry.key_id.clone(),
			})
			.collect()
	}

	async fn revocation_admin_success_frame(&self, stream_id: u32) -> std::io::Result<HrftFrame> {
		let snapshot = self.state_machine.state_snapshot().await;
		self.build_revocation_admin_frame(
			stream_id,
			&RevocationAdminResponseWire {
				accepted: true,
				reason_code: CONTROL_REASON_GRANTED,
				leader_hint: Some(self.local_node.id),
				entries: Self::revoked_actor_keys_wire(&snapshot),
				message: None,
			},
		)
	}

	fn tenant_admin_tenant_wire(
		snapshot: &crate::consensus::raft_storage::SmState,
		record: &TenantRecord,
	) -> TenantAdminTenantWire {
		let (tenant_kek_accounting_complete, tenant_kek_versions) =
			Self::tenant_kek_versions_wire(snapshot, record.tenant_id);
		TenantAdminTenantWire {
			tenant_id: record.tenant_id,
			quota_bytes: record.quota_bytes,
			used_bytes: record.used_bytes,
			replication_factor: record.replication_factor,
			state: record.state,
			active_tenant_kek_id: snapshot
				.tenant_keysets
				.get(&record.tenant_id)
				.and_then(|keyset| keyset.active_tenant_kek_id),
			tenant_kek_accounting_complete,
			tenant_kek_versions,
		}
	}

	fn tenant_kek_versions_wire(
		snapshot: &crate::consensus::raft_storage::SmState,
		tenant_id: TenantId,
	) -> (bool, Vec<TenantAdminTenantKekVersionWire>) {
		let status =
			crate::consensus::raft_storage::tenant_kek_retirement_status(snapshot, tenant_id);
		(
			status.accounting_complete,
			status
				.versions
				.into_iter()
				.map(|version| TenantAdminTenantKekVersionWire {
					tenant_kek_id: version.tenant_kek_id,
					is_active: version.is_active,
					tiered_slab_ref_count: version.tiered_slab_ref_count,
					safe_to_retire: version.safe_to_retire,
				})
				.collect(),
		)
	}

	fn build_create_tenant_request(
		&self,
		snapshot: &crate::consensus::raft_storage::SmState,
		tenant_id: TenantId,
		quota_bytes: u64,
		replication_factor: u32,
	) -> std::io::Result<CreateTenant> {
		if snapshot.tenants.contains_key(&tenant_id) {
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				format!("tenant {} already exists", tenant_id.get()),
			));
		}
		if let Some(keyset) = snapshot.tenant_keysets.get(&tenant_id) {
			keyset.active_tenant_kek_id.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"tenant {} keyset exists but has no active tenant_kek_id",
						tenant_id.get()
					),
				)
			})?;
			return Ok(CreateTenant {
				tenant_id,
				quota_bytes,
				replication_factor,
				tenant_keyset: keyset.clone(),
			});
		}
		let rotation = self.crypto.prepare_rotation(tenant_id)?;
		let mut tenant_keyset = crate::crypto::TenantKeyset::default();
		tenant_keyset.active_tenant_kek_id = Some(rotation.tenant_kek_id);
		tenant_keyset.keys.insert(
			rotation.tenant_kek_id,
			crate::crypto::WrappedTenantKek {
				wrapped_key: rotation.wrapped_tenant_kek,
			},
		);
		Ok(CreateTenant {
			tenant_id,
			quota_bytes,
			replication_factor,
			tenant_keyset,
		})
	}

	fn tenant_admin_error_reason(message: &str) -> u8 {
		if message.contains("does not exist") {
			CONTROL_REASON_NOT_FOUND
		} else if message.contains("already exists")
			|| message.contains("cannot be retired")
			|| message.contains("still referenced")
		{
			CONTROL_REASON_INVALID_TARGET
		} else {
			CONTROL_REASON_INVALID_CONFIG
		}
	}

	fn table_schema_drop_reason(error: &WendelonAdminError) -> u8 {
		match error {
			WendelonAdminError::TableMissing { .. }
			| WendelonAdminError::TableAlreadyDropped { .. } => CONTROL_REASON_NOT_FOUND,
			WendelonAdminError::GenerationMismatch { .. }
			| WendelonAdminError::TableAlreadyDropping { .. }
			| WendelonAdminError::DependentIndexesPresent { .. } => CONTROL_REASON_INVALID_TARGET,
			WendelonAdminError::Transport { .. } => CONTROL_REASON_INTERNAL_ERROR,
		}
	}

	fn current_control_hlc(&self) -> HybridTimestamp {
		let physical_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
			.unwrap_or(0);
		HybridTimestamp {
			physical_ms,
			logical: 0,
			node_id: self.local_node.id,
		}
	}

	fn table_retention_metric_policy(policy: TableRetentionPolicy) -> TableRetentionMetricPolicy {
		match policy {
			TableRetentionPolicy::InheritCluster => TableRetentionMetricPolicy::InheritCluster,
			TableRetentionPolicy::Disabled => TableRetentionMetricPolicy::Disabled,
			TableRetentionPolicy::FixedWindow(_) => TableRetentionMetricPolicy::FixedWindow,
		}
	}

	fn observe_descriptor_compute_metrics(tenant_id: TenantId, descriptor: &TableSchemaDescriptor) {
		let recompute_started = Instant::now();
		if let Ok(logical) = descriptor.compute_logical_schema_fingerprint() {
			if descriptor
				.compute_physical_descriptor_fingerprint(logical)
				.is_ok()
			{
				metrics().observe_storage_descriptor_recompute(
					tenant_id,
					descriptor.table_object_id,
					recompute_started.elapsed(),
				);
			}
		}
		let validate_started = Instant::now();
		if descriptor.validate().is_ok() {
			metrics().observe_storage_descriptor_validate(
				tenant_id,
				descriptor.table_object_id,
				validate_started.elapsed(),
			);
		}
	}

	fn descriptor_mutation_metric(
		previous: Option<&TableSchemaDescriptor>,
		next: &TableSchemaDescriptor,
	) -> Option<DescriptorMutationMetric> {
		let Some(previous) = previous else {
			return Some(DescriptorMutationMetric::Combined);
		};
		let logical_changed =
			previous.logical_schema_fingerprint != next.logical_schema_fingerprint;
		let physical_changed =
			previous.physical_descriptor_fingerprint != next.physical_descriptor_fingerprint;
		match (logical_changed, physical_changed) {
			(true, true) => Some(DescriptorMutationMetric::Combined),
			(true, false) => Some(DescriptorMutationMetric::LogicalOnly),
			(false, true) => Some(DescriptorMutationMetric::PhysicalOnly),
			(false, false) => None,
		}
	}

	fn observe_descriptor_mutation_metric(
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		metric: DescriptorMutationMetric,
	) {
		match metric {
			DescriptorMutationMetric::LogicalOnly => {
				metrics()
					.observe_storage_descriptor_logical_only_mutation(tenant_id, table_object_id);
			}
			DescriptorMutationMetric::PhysicalOnly => {
				metrics()
					.observe_storage_descriptor_physical_only_mutation(tenant_id, table_object_id);
			}
			DescriptorMutationMetric::Combined => {
				metrics().observe_storage_descriptor_combined_mutation(tenant_id, table_object_id);
			}
		}
	}

	async fn tenant_admin_success_for_target(
		&self,
		stream_id: u32,
		target_tenant_id: TenantId,
	) -> std::io::Result<HrftFrame> {
		let snapshot = self.state_machine.state_snapshot().await;
		let tenant = snapshot
			.tenants
			.get(&target_tenant_id)
			.map(|record| Self::tenant_admin_tenant_wire(&snapshot, record));
		self.build_tenant_admin_frame(
			stream_id,
			&TenantAdminResponseWire {
				accepted: true,
				reason_code: CONTROL_REASON_GRANTED,
				leader_hint: Some(self.local_node.id),
				tenant,
				tenants: Vec::new(),
				message: None,
			},
		)
	}

	async fn submit_revocation_mutation(
		&self,
		frame: &HrftFrame,
		revocation: RevokeActorKey,
	) -> std::io::Result<HrftFrame> {
		match self
			.raft
			.client_write(WendelonRequest::RevokeActorKey(revocation))
			.await
		{
			Ok(resp) => match resp.data {
				WendelonResponse::Success => {
					self.revocation_admin_success_frame(frame.header.stream_id)
						.await
				}
				WendelonResponse::Error(message) => self.revocation_admin_rejection(
					frame,
					CONTROL_REASON_INVALID_CONFIG,
					message,
					None,
				),
				WendelonResponse::LeaseDenied => self.revocation_admin_rejection(
					frame,
					CONTROL_REASON_INTERNAL_ERROR,
					"unexpected LeaseDenied response from revocation mutation",
					None,
				),
			},
			Err(err) if !self.is_leader().await => self.revocation_admin_rejection(
				frame,
				CONTROL_REASON_NOT_LEADER,
				err.to_string(),
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			),
			Err(err) => self.revocation_admin_rejection(
				frame,
				CONTROL_REASON_INTERNAL_ERROR,
				err.to_string(),
				None,
			),
		}
	}

	async fn handle_revocation_admin(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"REVOCATION_ADMIN_REQ must not carry Arrow or trailing payloads",
			));
		}
		let request: RevocationAdminRequestWire = wire::decode(&frame.meta)?;
		if !self.is_leader().await {
			return self.revocation_admin_rejection(
				&frame,
				CONTROL_REASON_NOT_LEADER,
				"revocation admin requests must be sent to the current leader",
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			);
		}
		match request {
			RevocationAdminRequestWire::Revoke {
				trust_domain,
				key_id,
			} => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::RevokeActorKey,
				) {
					return self.revocation_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				if key_id.is_empty() {
					return self.revocation_admin_rejection(
						&frame,
						CONTROL_REASON_INVALID_CONFIG,
						"actor certificate key_id must not be empty",
						None,
					);
				}
				self.submit_revocation_mutation(
					&frame,
					RevokeActorKey {
						trust_domain,
						key_id,
						revoked_at_ms: SystemTime::now()
							.duration_since(UNIX_EPOCH)
							.unwrap_or_default()
							.as_millis() as u64,
					},
				)
				.await
			}
			RevocationAdminRequestWire::List => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::RevocationList,
				) {
					return self.revocation_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				self.revocation_admin_success_frame(frame.header.stream_id)
					.await
			}
		}
	}

	async fn handle_schema_update(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"SCHEMA_UPDATE_REQ must not carry Arrow or trailing payloads",
			));
		}
		let envelope = match decode_schema_update_envelope_v1(&frame.meta) {
			Ok(envelope) => envelope,
			Err(err) => {
				return self.schema_update_rejection(
					&frame,
					CONTROL_REASON_INVALID_CONFIG,
					err.to_string(),
					None,
				);
			}
		};
		// Tenant-scoped actors are limited to their claimed tenant.
		// Cluster/platform actors have no tenant claim and are already
		// gated by TenantAdminMutation above.
		if let Some(claimed_tenant) = context.actor.claimed_tenant_id()? {
			if claimed_tenant != envelope.tenant_id {
				return self.schema_update_rejection(
					&frame,
					CONTROL_REASON_AUTHORIZATION_FAILED,
					format!(
						"actor tenant {} is not allowed to update schema for tenant {}",
						claimed_tenant.get(),
						envelope.tenant_id.get()
					),
					None,
				);
			}
		}
		if !self.is_leader().await {
			return self.schema_update_rejection(
				&frame,
				CONTROL_REASON_NOT_LEADER,
				"schema updates must be sent to the current leader",
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			);
		}

		let descriptor_metric = if envelope.object.kind
			== crate::integration::compute::SchemaObjectKind::Table
			&& matches!(
				envelope.mutation,
				SchemaMutationKindV1::Create | SchemaMutationKindV1::Alter
			) {
			match decode_table_schema_descriptor_v1(&envelope.payload) {
				Ok(descriptor) => {
					Self::observe_descriptor_compute_metrics(envelope.tenant_id, &descriptor);
					let snapshot = self.state_machine.state_snapshot().await;
					let previous = snapshot.table_schemas.latest_by_name(
						envelope.tenant_id,
						envelope.object.namespace.as_str(),
						envelope.object.name.as_str(),
					);
					Self::descriptor_mutation_metric(previous, &descriptor)
						.map(|metric| (envelope.tenant_id, descriptor.table_object_id, metric))
				}
				Err(_) => None,
			}
		} else {
			None
		};

		match self
			.raft
			.client_write(WendelonRequest::SchemaUpdate(frame.meta.to_vec()))
			.await
		{
			Ok(resp) => match resp.data {
				WendelonResponse::Success => {
					if let Some((tenant_id, table_object_id, metric)) = descriptor_metric {
						Self::observe_descriptor_mutation_metric(
							tenant_id,
							table_object_id,
							metric,
						);
					}
					self.build_schema_update_frame(
						frame.header.stream_id,
						&SchemaUpdateResponseWire {
							accepted: true,
							reason_code: CONTROL_REASON_GRANTED,
							leader_hint: Some(self.local_node.id),
							message: None,
						},
					)
				}
				WendelonResponse::Error(message) => self.schema_update_rejection(
					&frame,
					CONTROL_REASON_INVALID_CONFIG,
					message,
					None,
				),
				WendelonResponse::LeaseDenied => self.schema_update_rejection(
					&frame,
					CONTROL_REASON_INTERNAL_ERROR,
					"unexpected LeaseDenied response from schema update",
					None,
				),
			},
			Err(err) if !self.is_leader().await => self.schema_update_rejection(
				&frame,
				CONTROL_REASON_NOT_LEADER,
				err.to_string(),
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			),
			Err(err) => self.schema_update_rejection(
				&frame,
				CONTROL_REASON_INTERNAL_ERROR,
				err.to_string(),
				None,
			),
		}
	}

	async fn handle_schema_drop(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"SCHEMA_DROP_REQ must not carry Arrow or trailing payloads",
			));
		}
		let request = match wire::decode::<TableSchemaDropRequest>(&frame.meta) {
			Ok(request) => request,
			Err(err) => {
				return self.schema_drop_rejection(
					&frame,
					CONTROL_REASON_INVALID_CONFIG,
					WendelonAdminError::Transport {
						message: err.to_string(),
					},
					None,
				);
			}
		};
		let tenant_id = match request {
			TableSchemaDropRequest::V1 { tenant_id, .. } => tenant_id,
		};
		if let Some(claimed_tenant) = context.actor.claimed_tenant_id()? {
			if claimed_tenant != tenant_id {
				return self.schema_drop_rejection(
					&frame,
					CONTROL_REASON_AUTHORIZATION_FAILED,
					WendelonAdminError::Transport {
						message: format!(
							"actor tenant {} is not allowed to drop schema for tenant {}",
							claimed_tenant.get(),
							tenant_id.get()
						),
					},
					None,
				);
			}
		}
		if !self.is_leader().await {
			return self.schema_drop_rejection(
				&frame,
				CONTROL_REASON_NOT_LEADER,
				WendelonAdminError::Transport {
					message: "schema drops must be sent to the current leader".to_string(),
				},
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			);
		}

		match self.run_table_schema_drop(request).await {
			Ok(()) => self.build_schema_drop_frame(
				frame.header.stream_id,
				&TableSchemaDropResponseWire {
					accepted: true,
					reason_code: CONTROL_REASON_GRANTED,
					leader_hint: Some(self.local_node.id),
					admin_error: None,
					message: None,
				},
			),
			Err(error) => self.schema_drop_rejection(
				&frame,
				Self::table_schema_drop_reason(&error),
				error,
				None,
			),
		}
	}

	fn decode_table_descriptor_get_request(
		meta: TableDescriptorGetRequestMeta,
	) -> std::io::Result<TableDescriptorGetRequest> {
		Ok(TableDescriptorGetRequest {
			tenant_id: meta.tenant_id,
			namespace: decode_descriptor_wire_name("namespace", &meta.namespace)?,
			table_name: decode_descriptor_wire_name("table name", &meta.table_name)?,
		})
	}

	fn decode_logical_type_get_request(
		meta: LogicalTypeGetRequestMeta,
	) -> std::io::Result<LogicalTypeGetRequest> {
		Ok(LogicalTypeGetRequest {
			tenant_id: meta.tenant_id,
			name: decode_descriptor_wire_name("logical type name", &meta.name)?,
		})
	}

	fn authorize_descriptor_get_actor(
		actor: &ActorIdentity,
		tenant_id: TenantId,
	) -> std::io::Result<()> {
		authorize_actor(actor, ActorPermission::TenantAdminRead)?;
		if let Some(claimed_tenant) = actor.claimed_tenant_id()? {
			if claimed_tenant != tenant_id {
				return Err(Error::new(
					ErrorKind::PermissionDenied,
					format!(
						"actor tenant {} is not allowed to read table descriptors for tenant {}",
						claimed_tenant.get(),
						tenant_id.get()
					),
				));
			}
		}
		Ok(())
	}

	async fn handle_table_descriptor_get(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		metrics().control_descriptor_get_requests_total.inc();
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return self.table_descriptor_get_rejection(
				&frame,
				CONTROL_REASON_INVALID_CONFIG,
				None,
			);
		}

		let raw_meta: TableDescriptorGetRequestMeta =
			if frame.meta.len() == std::mem::size_of::<TableDescriptorGetRequestMeta>() {
				*bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<TableDescriptorGetRequestMeta>()],
				)
			} else {
				return self.table_descriptor_get_rejection(
					&frame,
					CONTROL_REASON_INVALID_CONFIG,
					None,
				);
			};
		if Self::authorize_descriptor_get_actor(&context.actor, raw_meta.tenant_id).is_err() {
			return self.table_descriptor_get_rejection(
				&frame,
				CONTROL_REASON_AUTHORIZATION_FAILED,
				None,
			);
		}

		let request = match Self::decode_table_descriptor_get_request(raw_meta) {
			Ok(request) => request,
			Err(_) => {
				return self.table_descriptor_get_rejection(
					&frame,
					CONTROL_REASON_INVALID_CONFIG,
					None,
				);
			}
		};
		if !self.is_leader().await {
			return self.table_descriptor_get_rejection(
				&frame,
				CONTROL_REASON_NOT_LEADER,
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			);
		}

		let snapshot = self.state_machine.state_snapshot().await;
		let Some(descriptor) = snapshot.table_schemas.latest_by_name(
			request.tenant_id,
			request.namespace.as_str(),
			request.table_name.as_str(),
		) else {
			return self.table_descriptor_get_rejection(&frame, CONTROL_REASON_NOT_FOUND, None);
		};
		let descriptor_bytes = encode_table_schema_descriptor_v1(descriptor)?;
		let descriptor_len = u32::try_from(descriptor_bytes.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"encoded table descriptor exceeds u32 wire length",
			)
		})?;
		Self::observe_table_descriptor_get_reason(CONTROL_REASON_GRANTED);
		self.build_table_descriptor_get_frame(
			frame.header.stream_id,
			TableDescriptorGetResponseMeta {
				accepted: 1,
				reason_code: CONTROL_REASON_GRANTED,
				_pad1: [0; 6],
				leader_hint: self.local_node,
				descriptor_len,
				_pad2: [0; 4],
			},
			Some(&descriptor_bytes),
		)
	}

	async fn handle_logical_type_get(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return self.logical_type_get_rejection(&frame, CONTROL_REASON_INVALID_CONFIG, None);
		}

		let raw_meta: LogicalTypeGetRequestMeta = if frame.meta.len()
			== std::mem::size_of::<LogicalTypeGetRequestMeta>()
		{
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<LogicalTypeGetRequestMeta>()])
		} else {
			return self.logical_type_get_rejection(&frame, CONTROL_REASON_INVALID_CONFIG, None);
		};
		if Self::authorize_descriptor_get_actor(&context.actor, raw_meta.tenant_id).is_err() {
			return self.logical_type_get_rejection(
				&frame,
				CONTROL_REASON_AUTHORIZATION_FAILED,
				None,
			);
		}

		let request = match Self::decode_logical_type_get_request(raw_meta) {
			Ok(request) => request,
			Err(_) => {
				return self.logical_type_get_rejection(
					&frame,
					CONTROL_REASON_INVALID_CONFIG,
					None,
				);
			}
		};
		if !self.is_leader().await {
			return self.logical_type_get_rejection(
				&frame,
				CONTROL_REASON_NOT_LEADER,
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			);
		}

		let snapshot = self.state_machine.state_snapshot().await;
		let Some(definition) = snapshot
			.logical_types
			.get_by_name(request.tenant_id, request.name.as_str())
		else {
			return self.logical_type_get_rejection(&frame, CONTROL_REASON_NOT_FOUND, None);
		};
		let definition_bytes = encode_logical_type_definition_v1(definition)?;
		let definition_len = u32::try_from(definition_bytes.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"encoded logical type definition exceeds u32 wire length",
			)
		})?;
		self.build_logical_type_get_frame(
			frame.header.stream_id,
			LogicalTypeGetResponseMeta {
				accepted: 1,
				reason_code: CONTROL_REASON_GRANTED,
				_pad1: [0; 6],
				leader_hint: self.local_node,
				definition_len,
				_pad2: [0; 4],
			},
			Some(&definition_bytes),
		)
	}

	async fn handle_logical_type_list(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return self.logical_type_list_rejection(&frame, CONTROL_REASON_INVALID_CONFIG, None);
		}

		let raw_meta: LogicalTypeListRequestMeta = if frame.meta.len()
			== std::mem::size_of::<LogicalTypeListRequestMeta>()
		{
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<LogicalTypeListRequestMeta>()])
		} else {
			return self.logical_type_list_rejection(&frame, CONTROL_REASON_INVALID_CONFIG, None);
		};
		if Self::authorize_descriptor_get_actor(&context.actor, raw_meta.tenant_id).is_err() {
			return self.logical_type_list_rejection(
				&frame,
				CONTROL_REASON_AUTHORIZATION_FAILED,
				None,
			);
		}
		if !self.is_leader().await {
			return self.logical_type_list_rejection(
				&frame,
				CONTROL_REASON_NOT_LEADER,
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			);
		}

		let snapshot = self.state_machine.state_snapshot().await;
		let definitions = snapshot
			.logical_types
			.definitions_for_tenant(raw_meta.tenant_id);
		let definitions_bytes = encode_logical_type_definitions_v1(&definitions)?;
		let definitions_len = u32::try_from(definitions_bytes.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"encoded logical type definitions exceed u32 wire length",
			)
		})?;
		self.build_logical_type_list_frame(
			frame.header.stream_id,
			LogicalTypeListResponseMeta {
				accepted: 1,
				reason_code: CONTROL_REASON_GRANTED,
				_pad1: [0; 6],
				leader_hint: self.local_node,
				definitions_len,
				_pad2: [0; 4],
			},
			Some(&definitions_bytes),
		)
	}

	async fn run_table_schema_drop(
		&self,
		request: TableSchemaDropRequest,
	) -> Result<(), WendelonAdminError> {
		let TableSchemaDropRequest::V1 {
			tenant_id,
			table_object_id,
			expected_generation,
			operation_id,
		} = request;
		if operation_id.tenant_id != tenant_id
			|| operation_id.table_object_id != table_object_id
			|| operation_id.schema_generation != expected_generation
		{
			return Err(WendelonAdminError::Transport {
				message: "DROP operation_id must match request target".to_string(),
			});
		}

		let snapshot = self.state_machine.state_snapshot().await;
		let descriptor = snapshot
			.table_schemas
			.latest_by_id(tenant_id, table_object_id)
			.ok_or(WendelonAdminError::TableMissing { table_object_id })?;
		let descriptor = descriptor.clone();
		if descriptor.schema_generation != expected_generation.get() {
			return Err(WendelonAdminError::GenerationMismatch {
				expected: expected_generation,
				committed: crate::integration::compute::SchemaGeneration::new(
					descriptor.schema_generation,
				),
			});
		}
		if !snapshot.table_schemas.is_latest_name_bound(
			tenant_id,
			&descriptor.namespace,
			&descriptor.table_name,
			table_object_id,
			descriptor.schema_generation,
		) {
			return Err(WendelonAdminError::TableAlreadyDropped { table_object_id });
		}
		let dependent_indexes = snapshot
			.secondary_indexes
			.for_table(tenant_id, table_object_id)
			.into_iter()
			.map(|row| row.descriptor.index_object_id)
			.collect::<Vec<_>>();
		if !dependent_indexes.is_empty() {
			return Err(WendelonAdminError::DependentIndexesPresent {
				indexes: dependent_indexes,
			});
		}

		let existing_fence = snapshot
			.table_drop_fences
			.get(tenant_id, table_object_id)
			.copied();
		let fence = if let Some(fence) = existing_fence {
			if fence.operation_id != operation_id {
				return Err(WendelonAdminError::TableAlreadyDropping {
					table_object_id,
					existing_operation_id: fence.operation_id,
				});
			}
			fence
		} else {
			let log_index = self
				.submit_table_drop_stage(WendelonRequest::TableDropPrepare(
					TableDropPrepareRequest::current(operation_id, expected_generation),
				))
				.await?;
			crate::schema::TableDropFence {
				operation_id,
				fence_log_index: log_index,
				reclaim_completion_log_index: None,
			}
		};

		self.drive_table_drop_physical_reclaim(tenant_id, &descriptor)
			.await?;

		let reclaim_completion_log_index =
			if let Some(reclaim_completion_log_index) = fence.reclaim_completion_log_index {
				reclaim_completion_log_index
			} else {
				self.submit_table_drop_stage(WendelonRequest::TableReclaimCompletion(
					TableReclaimCompletionRequest::current(operation_id, fence.fence_log_index),
				))
				.await?
			};

		self.submit_table_drop_stage(WendelonRequest::TableDropFinalize(
			TableDropFinalizeRequest::current(
				operation_id,
				fence.fence_log_index,
				reclaim_completion_log_index,
			),
		))
		.await?;
		Ok(())
	}

	async fn drive_table_drop_physical_reclaim(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
	) -> Result<(), WendelonAdminError> {
		let Some(storage_manager) = self.storage_manager.as_ref() else {
			return Err(WendelonAdminError::Transport {
				message: "schema drop requires a configured Wendelon storage manager".to_string(),
			});
		};
		let snapshot = self.state_machine.state_snapshot().await;
		storage_manager
			.reclaim_tombstoned_ranges(&snapshot.tombstones, usize::MAX)
			.await
			.map_err(|err| WendelonAdminError::Transport {
				message: format!("schema drop tombstone reclaim failed: {err}"),
			})?;
		storage_manager
			.reclaim_table_drop_storage(
				tenant_id,
				descriptor,
				&snapshot.table_schemas,
				&snapshot.secondary_indexes,
				usize::MAX,
			)
			.await
			.map_err(|err| WendelonAdminError::Transport {
				message: format!("schema drop table storage reclaim failed: {err}"),
			})?;
		let committer = RaftCommitter::new(self.raft.clone());
		storage_manager
			.reclaim_table_drop_tiered_storage(
				tenant_id,
				descriptor,
				&snapshot.table_schemas,
				&committer,
				usize::MAX,
			)
			.await
			.map_err(|err| WendelonAdminError::Transport {
				message: format!("schema drop tiered table storage reclaim failed: {err}"),
			})?;
		Ok(())
	}

	async fn submit_table_drop_stage(
		&self,
		request: WendelonRequest,
	) -> Result<u64, WendelonAdminError> {
		match self.raft.client_write(request).await {
			Ok(resp) => match resp.data {
				WendelonResponse::Success => Ok(resp.log_id.index),
				WendelonResponse::Error(message) => Err(WendelonAdminError::Transport { message }),
				WendelonResponse::LeaseDenied => Err(WendelonAdminError::Transport {
					message: "unexpected LeaseDenied response from schema drop".to_string(),
				}),
			},
			Err(err) if !self.is_leader().await => Err(WendelonAdminError::Transport {
				message: format!("schema drop leadership changed: {err}"),
			}),
			Err(err) => Err(WendelonAdminError::Transport {
				message: err.to_string(),
			}),
		}
	}

	async fn submit_tenant_admin_mutation(
		&self,
		frame: &HrftFrame,
		request: WendelonRequest,
		target_tenant_id: TenantId,
	) -> std::io::Result<HrftFrame> {
		match self.raft.client_write(request).await {
			Ok(resp) => match resp.data {
				WendelonResponse::Success => {
					self.tenant_admin_success_for_target(frame.header.stream_id, target_tenant_id)
						.await
				}
				WendelonResponse::Error(message) => self.tenant_admin_rejection(
					frame,
					Self::tenant_admin_error_reason(&message),
					message,
					None,
				),
				WendelonResponse::LeaseDenied => self.tenant_admin_rejection(
					frame,
					CONTROL_REASON_INTERNAL_ERROR,
					"unexpected LeaseDenied response from tenant mutation",
					None,
				),
			},
			Err(err) if !self.is_leader().await => self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_NOT_LEADER,
				err.to_string(),
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			),
			Err(err) => self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_INTERNAL_ERROR,
				err.to_string(),
				None,
			),
		}
	}

	async fn submit_table_retention_policy_update(
		&self,
		frame: &HrftFrame,
		update: crate::consensus::SetTableRetentionPolicy,
	) -> std::io::Result<HrftFrame> {
		let target_tenant_id = update.tenant_id;
		let metric_policy = Self::table_retention_metric_policy(update.new_policy);
		match self
			.raft
			.client_write(WendelonRequest::SetTableRetentionPolicy(update))
			.await
		{
			Ok(resp) => match resp.data {
				WendelonResponse::Success => {
					metrics().observe_storage_table_retention_policy_update(metric_policy);
					self.tenant_admin_success_for_target(frame.header.stream_id, target_tenant_id)
						.await
				}
				WendelonResponse::Error(message) => self.tenant_admin_rejection(
					frame,
					Self::tenant_admin_error_reason(&message),
					message,
					None,
				),
				WendelonResponse::LeaseDenied => self.tenant_admin_rejection(
					frame,
					CONTROL_REASON_INTERNAL_ERROR,
					"unexpected LeaseDenied response from table retention policy update",
					None,
				),
			},
			Err(err) if !self.is_leader().await => self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_NOT_LEADER,
				err.to_string(),
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			),
			Err(err) => self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_INTERNAL_ERROR,
				err.to_string(),
				None,
			),
		}
	}

	async fn submit_tenant_kek_rotation(
		&self,
		frame: &HrftFrame,
		target_tenant_id: TenantId,
	) -> std::io::Result<HrftFrame> {
		let snapshot = self.state_machine.state_snapshot().await;
		if !snapshot.tenants.contains_key(&target_tenant_id) {
			return self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_NOT_FOUND,
				format!("tenant {} does not exist", target_tenant_id.get()),
				None,
			);
		}
		let rotation = match self.crypto.prepare_rotation(target_tenant_id) {
			Ok(rotation) => rotation,
			Err(err) => {
				let message = err.to_string();
				let reason = Self::tenant_admin_error_reason(&message);
				return self.tenant_admin_rejection(frame, reason, message, None);
			}
		};
		let tenant_kek_id = rotation.tenant_kek_id;
		match self
			.raft
			.client_write(WendelonRequest::InstallTenantKekVersion(rotation))
			.await
		{
			Ok(resp) => match resp.data {
				WendelonResponse::Success => {
					let local_active = match self.crypto.active_tenant_kek_id(target_tenant_id) {
						Ok(tenant_kek_id) => tenant_kek_id,
						Err(err) => {
							return self.tenant_admin_rejection(
								frame,
								CONTROL_REASON_INTERNAL_ERROR,
								format!(
									"rotate_tenant_kek: committed tenant_kek_id {} but local crypto lookup failed: {err}",
									tenant_kek_id
								),
								None,
							);
						}
					};
					if local_active != tenant_kek_id {
						return self.tenant_admin_rejection(
							frame,
							CONTROL_REASON_INTERNAL_ERROR,
							format!(
								"rotate_tenant_kek: committed tenant_kek_id {} but local crypto active_tenant_kek_id is {}",
								tenant_kek_id, local_active
							),
							None,
						);
					}
					self.tenant_admin_success_for_target(frame.header.stream_id, target_tenant_id)
						.await
				}
				WendelonResponse::Error(message) => self.tenant_admin_rejection(
					frame,
					Self::tenant_admin_error_reason(&message),
					message,
					None,
				),
				WendelonResponse::LeaseDenied => self.tenant_admin_rejection(
					frame,
					CONTROL_REASON_INTERNAL_ERROR,
					"unexpected LeaseDenied response from InstallTenantKekVersion",
					None,
				),
			},
			Err(err) if !self.is_leader().await => self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_NOT_LEADER,
				err.to_string(),
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			),
			Err(err) => self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_INTERNAL_ERROR,
				err.to_string(),
				None,
			),
		}
	}

	async fn submit_tenant_kek_retirement(
		&self,
		frame: &HrftFrame,
		target_tenant_id: TenantId,
		tenant_kek_id: TenantKekId,
	) -> std::io::Result<HrftFrame> {
		let snapshot = self.state_machine.state_snapshot().await;
		if !snapshot.tenants.contains_key(&target_tenant_id) {
			return self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_NOT_FOUND,
				format!("tenant {} does not exist", target_tenant_id.get()),
				None,
			);
		}
		match self
			.raft
			.client_write(WendelonRequest::RetireTenantKekVersion(
				RetireTenantKekVersion {
					tenant_id: target_tenant_id,
					tenant_kek_id,
				},
			))
			.await
		{
			Ok(resp) => match resp.data {
				WendelonResponse::Success => {
					let local_keysets = match self.crypto.tenant_keysets_snapshot() {
						Ok(snapshot) => snapshot,
						Err(err) => {
							return self.tenant_admin_rejection(
								frame,
								CONTROL_REASON_INTERNAL_ERROR,
								format!(
									"retire_tenant_kek: committed tenant_kek_id {} but local crypto snapshot failed: {err}",
									tenant_kek_id.get()
								),
								None,
							);
						}
					};
					let Some(local_keyset) = local_keysets.get(&target_tenant_id) else {
						return self.tenant_admin_rejection(
							frame,
							CONTROL_REASON_INTERNAL_ERROR,
							format!(
								"retire_tenant_kek: committed tenant_kek_id {} but local tenant keyset is missing",
								tenant_kek_id.get()
							),
							None,
						);
					};
					if local_keyset.keys.contains_key(&tenant_kek_id) {
						return self.tenant_admin_rejection(
							frame,
							CONTROL_REASON_INTERNAL_ERROR,
							format!(
								"retire_tenant_kek: committed tenant_kek_id {} but local crypto still retains it",
								tenant_kek_id.get()
							),
							None,
						);
					}
					self.tenant_admin_success_for_target(frame.header.stream_id, target_tenant_id)
						.await
				}
				WendelonResponse::Error(message) => self.tenant_admin_rejection(
					frame,
					Self::tenant_admin_error_reason(&message),
					message,
					None,
				),
				WendelonResponse::LeaseDenied => self.tenant_admin_rejection(
					frame,
					CONTROL_REASON_INTERNAL_ERROR,
					"unexpected LeaseDenied response from RetireTenantKekVersion",
					None,
				),
			},
			Err(err) if !self.is_leader().await => self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_NOT_LEADER,
				err.to_string(),
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			),
			Err(err) => self.tenant_admin_rejection(
				frame,
				CONTROL_REASON_INTERNAL_ERROR,
				err.to_string(),
				None,
			),
		}
	}

	async fn handle_tenant_admin(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TENANT_ADMIN_REQ must not carry Arrow or trailing payloads",
			));
		}
		let request: TenantAdminRequestWire = wire::decode(&frame.meta)?;
		let claimed_tenant = context.actor.claimed_tenant_id()?;
		if claimed_tenant.is_some() {
			return self.tenant_admin_rejection(
				&frame,
				CONTROL_REASON_AUTHORIZATION_FAILED,
				"tenant-scoped principals may not use TenantAdmin",
				None,
			);
		}
		if matches!(
			request,
			TenantAdminRequestWire::Get { .. } | TenantAdminRequestWire::List
		) && authorize_actor(&context.actor, ActorPermission::TenantAdminRead).is_err()
		{
			return self.tenant_admin_rejection(
				&frame,
				CONTROL_REASON_AUTHORIZATION_FAILED,
				format!(
					"actor role {} is not allowed for TenantAdmin",
					context.actor.role
				),
				None,
			);
		}

		if !self.is_leader().await {
			return self.tenant_admin_rejection(
				&frame,
				CONTROL_REASON_NOT_LEADER,
				"tenant admin requests must be sent to the current leader",
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			);
		}

		match request {
			TenantAdminRequestWire::Create {
				target_tenant_id,
				quota_bytes,
				replication_factor,
			} => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::TenantCreate,
				) {
					return self.tenant_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				let snapshot = self.state_machine.state_snapshot().await;
				let create = match self.build_create_tenant_request(
					&snapshot,
					target_tenant_id,
					quota_bytes,
					replication_factor,
				) {
					Ok(create) => create,
					Err(err) => {
						let message = err.to_string();
						let reason = Self::tenant_admin_error_reason(&message);
						return self.tenant_admin_rejection(&frame, reason, message, None);
					}
				};
				self.submit_tenant_admin_mutation(
					&frame,
					WendelonRequest::CreateTenant(create),
					target_tenant_id,
				)
				.await
			}
			TenantAdminRequestWire::UpdateQuota {
				target_tenant_id,
				quota_bytes,
			} => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::TenantUpdateQuota,
				) {
					return self.tenant_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				self.submit_tenant_admin_mutation(
					&frame,
					WendelonRequest::UpdateTenantQuota(UpdateTenantQuota {
						tenant_id: target_tenant_id,
						quota_bytes,
					}),
					target_tenant_id,
				)
				.await
			}
			TenantAdminRequestWire::Suspend { target_tenant_id } => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::TenantSuspend,
				) {
					return self.tenant_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				self.submit_tenant_admin_mutation(
					&frame,
					WendelonRequest::SuspendTenant(target_tenant_id),
					target_tenant_id,
				)
				.await
			}
			TenantAdminRequestWire::Resume { target_tenant_id } => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::TenantResume,
				) {
					return self.tenant_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				self.submit_tenant_admin_mutation(
					&frame,
					WendelonRequest::ResumeTenant(target_tenant_id),
					target_tenant_id,
				)
				.await
			}
			TenantAdminRequestWire::Delete { target_tenant_id } => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::TenantDelete,
				) {
					return self.tenant_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				self.submit_tenant_admin_mutation(
					&frame,
					WendelonRequest::DeleteTenant(target_tenant_id),
					target_tenant_id,
				)
				.await
			}
			TenantAdminRequestWire::RotateTenantKek { target_tenant_id } => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::TenantKekRotate,
				) {
					return self.tenant_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				self.submit_tenant_kek_rotation(&frame, target_tenant_id)
					.await
			}
			TenantAdminRequestWire::RetireTenantKek {
				target_tenant_id,
				tenant_kek_id,
			} => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::TenantKekRetire,
				) {
					return self.tenant_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				self.submit_tenant_kek_retirement(&frame, target_tenant_id, tenant_kek_id)
					.await
			}
			TenantAdminRequestWire::SetTableRetentionPolicy {
				target_tenant_id,
				table_object_id,
				expected_policy_generation,
				new_policy,
			} => {
				if let Err(error) = require_administrative_action(
					&context.actor,
					AdministrativeAction::TableRetentionPolicyUpdate,
				) {
					return self.tenant_admin_rejection(
						&frame,
						CONTROL_REASON_AUTHORIZATION_FAILED,
						error.to_string(),
						None,
					);
				}
				self.submit_table_retention_policy_update(
					&frame,
					crate::consensus::SetTableRetentionPolicy {
						tenant_id: target_tenant_id,
						table_object_id,
						expected_policy_generation,
						new_policy,
						updated_at_hlc: self.current_control_hlc(),
					},
				)
				.await
			}
			TenantAdminRequestWire::Get { target_tenant_id } => {
				let snapshot = self.state_machine.state_snapshot().await;
				let Some(record) = snapshot.tenants.get(&target_tenant_id) else {
					return self.tenant_admin_rejection(
						&frame,
						CONTROL_REASON_NOT_FOUND,
						format!("tenant {} does not exist", target_tenant_id.get()),
						None,
					);
				};
				self.build_tenant_admin_frame(
					frame.header.stream_id,
					&TenantAdminResponseWire {
						accepted: true,
						reason_code: CONTROL_REASON_GRANTED,
						leader_hint: Some(self.local_node.id),
						tenant: Some(Self::tenant_admin_tenant_wire(&snapshot, record)),
						tenants: Vec::new(),
						message: None,
					},
				)
			}
			TenantAdminRequestWire::List => {
				let snapshot = self.state_machine.state_snapshot().await;
				let mut tenants = snapshot
					.tenants
					.values()
					.map(|record| Self::tenant_admin_tenant_wire(&snapshot, record))
					.collect::<Vec<_>>();
				tenants.sort_by_key(|tenant| tenant.tenant_id);
				self.build_tenant_admin_frame(
					frame.header.stream_id,
					&TenantAdminResponseWire {
						accepted: true,
						reason_code: CONTROL_REASON_GRANTED,
						leader_hint: Some(self.local_node.id),
						tenant: None,
						tenants,
						message: None,
					},
				)
			}
		}
	}

	pub async fn advance_membership_epoch(&self, next: MembershipEpoch) -> std::io::Result<()> {
		let _guard = self.reconcile_lock.lock().await;
		if !self.is_leader().await {
			return Err(not_leader_error(self.leader_hint().await));
		}

		self.reconcile_pending_epoch_locked().await?;

		let visible = self.epoch_rx.borrow().clone();
		if next.epoch == visible.epoch && membership_eq(&visible, &next) {
			return Ok(());
		}
		if next.epoch != visible.epoch + 1 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"epoch advance must be sequential: visible={}, requested={}",
					visible.epoch, next.epoch
				),
			));
		}

		self.commit_epoch_transition_locked(&visible, &next).await?;
		self.reconcile_pending_epoch_locked().await
	}

	pub fn spawn_epoch_reconciler(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
		tokio::spawn(async move {
			let mut ticker = tokio::time::interval(Duration::from_millis(50));
			loop {
				ticker.tick().await;
				let _ = self.reconcile_pending_epoch().await;
			}
		})
	}

	pub async fn reconcile_pending_epoch(&self) -> std::io::Result<()> {
		let _guard = self.reconcile_lock.lock().await;
		self.reconcile_pending_epoch_locked().await
	}

	async fn handle_status(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if !frame.meta.is_empty() || frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"STATUS_REQ must not carry meta, arrow, or trailing payloads",
			));
		}
		let snapshot = self.cluster_status_wire().await;
		let meta = wire::encode_into(&self.pool, &snapshot)?;
		let leases = self.cluster_status_leases().await;
		let trailing = if leases.is_empty() {
			None
		} else {
			Some(wire::encode_into(&self.pool, &leases)?)
		};
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::STATUS_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0),
			},
			meta,
			arrow_payload: None,
			trailing,
		})
	}

	async fn handle_audit_chain_head(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if !frame.meta.is_empty() || frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"AUDIT_CHAIN_HEAD_REQ must not carry meta, arrow, or trailing payloads",
			));
		}
		let meta = wire::encode_into(
			&self.pool,
			&AuditChainHeadResponseWire {
				head: self.audit_sink.chain_head()?,
			},
		)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::AUDIT_CHAIN_HEAD_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	async fn handle_lease(&self, remote: NodeId, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<AcquireLeaseRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LEASE_REQ meta too small",
			));
		}
		let req: AcquireLeaseRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<AcquireLeaseRequestMeta>()]);

		if req.epoch != self.current_epoch() {
			return self.lease_response(&frame, LeaseStatus::DENIED_EPOCH_STALE, None);
		}
		if !self.is_leader().await {
			return self.lease_response(&frame, LeaseStatus::DENIED_NOT_LEADER, None);
		}
		if self
			.validate_acquire_lease_target(remote, &req)
			.await
			.is_err()
		{
			return self.lease_response(&frame, LeaseStatus::DENIED_INVALID_TARGET, None);
		}

		let ttl_ms = req.ttl_ms.clamp(1, self.lease_ttl_cap_ms);
		let lease = self.lease_authority.sign_lease(LeaseDescriptor {
			epoch: req.epoch,
			holder: req.holder,
			range_start: req.range_start,
			range_end: req.range_end,
			lease_start: req.requested_hlc,
			lease_expiry: HybridTimestamp {
				physical_ms: req.requested_hlc.physical_ms.saturating_add(ttl_ms as u64),
				logical: req.requested_hlc.logical,
				node_id: req.requested_hlc.node_id,
			},
			min_commit_index: 0,
			signature: [0; 64],
			issuer: self.local_node,
		});

		let status = match self
			.raft
			.client_write(WendelonRequest::AcquireLease(lease))
			.await
		{
			Ok(resp) => match resp.data {
				WendelonResponse::Success => {
					return self.lease_response(&frame, LeaseStatus::GRANTED, Some(&lease));
				}
				WendelonResponse::LeaseDenied => LeaseStatus::DENIED_OVERLAP,
				WendelonResponse::Error(_) => LeaseStatus::INTERNAL_ERROR,
			},
			Err(_) if !self.is_leader().await => LeaseStatus::DENIED_NOT_LEADER,
			Err(_) => LeaseStatus::INTERNAL_ERROR,
		};
		self.lease_response(&frame, status, None)
	}

	async fn handle_release(&self, remote: NodeId, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<ReleaseLeaseRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LEASE_RELEASE_REQ meta too small",
			));
		}
		let req: ReleaseLeaseRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReleaseLeaseRequestMeta>()]);

		if req.epoch != self.current_epoch() {
			return self.release_response(&frame, LeaseStatus::DENIED_EPOCH_STALE);
		}
		if !self.is_leader().await {
			return self.release_response(&frame, LeaseStatus::DENIED_NOT_LEADER);
		}
		if remote != req.holder {
			return self.release_response(&frame, LeaseStatus::DENIED_INVALID_TARGET);
		}

		let status = match self
			.raft
			.client_write(WendelonRequest::ReleaseLease(req))
			.await
		{
			Ok(resp) => match resp.data {
				WendelonResponse::Success => LeaseStatus::GRANTED,
				WendelonResponse::LeaseDenied => LeaseStatus::DENIED_INVALID_TARGET,
				WendelonResponse::Error(_) => LeaseStatus::INTERNAL_ERROR,
			},
			Err(_) if !self.is_leader().await => LeaseStatus::DENIED_NOT_LEADER,
			Err(_) => LeaseStatus::INTERNAL_ERROR,
		};
		self.release_response(&frame, status)
	}

	async fn handle_transfer(
		&self,
		context: &RequestContext,
		remote: NodeId,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TRANSFER_REQ must not carry an Arrow payload",
			));
		}
		if let Err(_error) =
			require_administrative_action(&context.actor, AdministrativeAction::TransferLeadership)
		{
			return self.transfer_response(
				&frame,
				TransferLeadershipResponseMeta {
					accepted: false,
					reason_code: CONTROL_REASON_AUTHORIZATION_FAILED,
				},
				None,
			);
		}
		let req: TransferLeadershipRequestMeta = wire::decode(&frame.meta)?;
		let token = match self.decode_transfer_token(&frame) {
			Ok(token) => token,
			Err(_) => {
				return self.transfer_response(
					&frame,
					TransferLeadershipResponseMeta {
						accepted: false,
						reason_code: CONTROL_REASON_INVALID_TOKEN,
					},
					None,
				);
			}
		};
		if self
			.validate_transfer_request(&req, &token, remote)
			.await
			.is_err()
		{
			return self.transfer_response(
				&frame,
				TransferLeadershipResponseMeta {
					accepted: false,
					reason_code: CONTROL_REASON_INVALID_TOKEN,
				},
				None,
			);
		}

		let resp = if req.source_epoch != self.current_epoch() {
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_EPOCH_STALE,
			}
		} else if req.to_leader == self.local_node {
			match self.accept_transfer_target(&token).await {
				Ok(()) => TransferLeadershipResponseMeta {
					accepted: true,
					reason_code: CONTROL_REASON_GRANTED,
				},
				Err(err) if err.kind() == ErrorKind::NotConnected => {
					TransferLeadershipResponseMeta {
						accepted: false,
						reason_code: CONTROL_REASON_NOT_LEADER,
					}
				}
				Err(_) => TransferLeadershipResponseMeta {
					accepted: false,
					reason_code: CONTROL_REASON_INTERNAL_ERROR,
				},
			}
		} else if !self.is_leader().await {
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_NOT_LEADER,
			}
		} else if req.from_leader != self.local_node
			|| req.to_leader == self.local_node
			|| !self.transfer_target_known(&req.to_leader)
		{
			TransferLeadershipResponseMeta {
				accepted: false,
				reason_code: CONTROL_REASON_INVALID_TARGET,
			}
		} else {
			match self.forward_transfer_request(&frame, &req, &token).await {
				Ok(resp) => resp,
				Err(err) if err.kind() == ErrorKind::NotConnected => {
					TransferLeadershipResponseMeta {
						accepted: false,
						reason_code: CONTROL_REASON_NOT_LEADER,
					}
				}
				Err(err) if err.kind() == ErrorKind::NotFound => TransferLeadershipResponseMeta {
					accepted: false,
					reason_code: CONTROL_REASON_INVALID_TARGET,
				},
				Err(_) => TransferLeadershipResponseMeta {
					accepted: false,
					reason_code: CONTROL_REASON_INTERNAL_ERROR,
				},
			}
		};
		let accepted = resp.accepted;
		self.transfer_response(&frame, resp, accepted.then_some(&token))
	}

	async fn handle_advance_epoch(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let leader_hint = self.leader_hint().await;
		let resp = if let Err(_error) =
			require_administrative_action(&context.actor, AdministrativeAction::AdvanceEpoch)
		{
			AdvanceEpochResponseWire {
				accepted: false,
				reason_code: CONTROL_REASON_AUTHORIZATION_FAILED,
				leader_hint: None,
			}
		} else if !self.is_leader().await {
			AdvanceEpochResponseWire {
				accepted: false,
				reason_code: CONTROL_REASON_NOT_LEADER,
				leader_hint,
			}
		} else {
			match frame.arrow_payload.as_ref() {
				None => AdvanceEpochResponseWire {
					accepted: false,
					reason_code: CONTROL_REASON_INVALID_CONFIG,
					leader_hint: None,
				},
				Some(bytes) => {
					let batch = decode_epoch_batch(bytes)?;
					if batch.schema().as_ref() != &epoch_entry_var_schema() {
						AdvanceEpochResponseWire {
							accepted: false,
							reason_code: CONTROL_REASON_INVALID_CONFIG,
							leader_hint: None,
						}
					} else {
						let next_epoch = self.current_epoch().saturating_add(1);
						let visible = self.epoch_rx.borrow().clone();
						match membership_from_epoch_batch(&batch, next_epoch, &visible) {
							Ok(next) => match self.advance_membership_epoch(next).await {
								Ok(()) => AdvanceEpochResponseWire {
									accepted: true,
									reason_code: CONTROL_REASON_GRANTED,
									leader_hint: None,
								},
								Err(err) if err.kind() == ErrorKind::InvalidInput => {
									AdvanceEpochResponseWire {
										accepted: false,
										reason_code: CONTROL_REASON_INVALID_CONFIG,
										leader_hint: None,
									}
								}
								Err(err) if err.kind() == ErrorKind::NotConnected => {
									AdvanceEpochResponseWire {
										accepted: false,
										reason_code: CONTROL_REASON_NOT_LEADER,
										leader_hint: self.leader_hint().await,
									}
								}
								Err(_) => AdvanceEpochResponseWire {
									accepted: false,
									reason_code: CONTROL_REASON_INTERNAL_ERROR,
									leader_hint: None,
								},
							},
							Err(_) => AdvanceEpochResponseWire {
								accepted: false,
								reason_code: CONTROL_REASON_INVALID_CONFIG,
								leader_hint: None,
							},
						}
					}
				}
			}
		};

		let meta = wire::encode_into(&self.pool, &resp)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::EPOCH_ADV_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	async fn handle_join(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"JOIN_REQ must not carry Arrow or trailing payloads",
			));
		}
		let req: JoinClusterRequestWire = wire::decode(&frame.meta)?;
		let resp =
			if require_administrative_action(&context.actor, AdministrativeAction::JoinCluster)
				.is_err()
			{
				JoinClusterResponseWire {
					accepted: false,
					reason_code: CONTROL_REASON_AUTHORIZATION_FAILED,
					leader_hint: None,
					local_protocol: self.protocol,
					negotiated_protocol: None,
				}
			} else {
				match self.validate_join_request(&req) {
					Err(_) => JoinClusterResponseWire {
						accepted: false,
						reason_code: CONTROL_REASON_INVALID_CONFIG,
						leader_hint: self.leader_hint().await,
						local_protocol: self.protocol,
						negotiated_protocol: None,
					},
					Ok(()) if self.is_leader().await => {
						match wire::negotiate_protocol(self.protocol, req.protocol) {
							Ok(negotiated_protocol) => match self.accept_join(&req).await {
								Ok(()) => JoinClusterResponseWire {
									accepted: true,
									reason_code: CONTROL_REASON_GRANTED,
									leader_hint: Some(self.local_node.id),
									local_protocol: self.protocol,
									negotiated_protocol: Some(negotiated_protocol),
								},
								Err(err) if err.kind() == ErrorKind::InvalidInput => {
									JoinClusterResponseWire {
										accepted: false,
										reason_code: CONTROL_REASON_INVALID_CONFIG,
										leader_hint: Some(self.local_node.id),
										local_protocol: self.protocol,
										negotiated_protocol: None,
									}
								}
								Err(err) if err.kind() == ErrorKind::NotConnected => {
									JoinClusterResponseWire {
										accepted: false,
										reason_code: CONTROL_REASON_NOT_LEADER,
										leader_hint: self.leader_hint().await,
										local_protocol: self.protocol,
										negotiated_protocol: None,
									}
								}
								Err(_) => JoinClusterResponseWire {
									accepted: false,
									reason_code: CONTROL_REASON_INTERNAL_ERROR,
									leader_hint: self.leader_hint().await,
									local_protocol: self.protocol,
									negotiated_protocol: None,
								},
							},
							Err(_err) if _err.kind() == ErrorKind::Unsupported => {
								JoinClusterResponseWire {
									accepted: false,
									reason_code: CONTROL_REASON_INCOMPATIBLE_VERSION,
									leader_hint: Some(self.local_node.id),
									local_protocol: self.protocol,
									negotiated_protocol: None,
								}
							}
							Err(_) => JoinClusterResponseWire {
								accepted: false,
								reason_code: CONTROL_REASON_INVALID_CONFIG,
								leader_hint: Some(self.local_node.id),
								local_protocol: self.protocol,
								negotiated_protocol: None,
							},
						}
					}
					Ok(()) => match self.forward_join_request(&frame, &req).await {
						Ok(resp) => resp,
						Err(err) if err.kind() == ErrorKind::NotConnected => {
							JoinClusterResponseWire {
								accepted: false,
								reason_code: CONTROL_REASON_NOT_LEADER,
								leader_hint: self.leader_hint().await,
								local_protocol: self.protocol,
								negotiated_protocol: None,
							}
						}
						Err(_) => JoinClusterResponseWire {
							accepted: false,
							reason_code: CONTROL_REASON_INTERNAL_ERROR,
							leader_hint: self.leader_hint().await,
							local_protocol: self.protocol,
							negotiated_protocol: None,
						},
					},
				}
			};
		self.join_response(&frame, resp)
	}

	async fn commit_epoch_transition_locked(
		&self,
		from: &MembershipEpoch,
		to: &MembershipEpoch,
	) -> std::io::Result<()> {
		let from_cfg = epoch_config_from_membership(from);
		let to_cfg = epoch_config_from_membership(to);
		let snapshot = self.state_machine.state_snapshot().await;
		if let Some(existing) = snapshot.epoch_history.get(&to.epoch) {
			if existing != &to_cfg {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("epoch {} already committed with different config", to.epoch),
				));
			}
			return Ok(());
		}
		drop(snapshot);
		match self
			.raft
			.client_write(WendelonRequest::AdvanceEpoch(AdvanceEpochRecord {
				from: from_cfg,
				to: to_cfg,
			}))
			.await
		{
			Ok(_) => {}
			Err(_err) if !self.is_leader().await => {
				return Err(not_leader_error(self.leader_hint().await));
			}
			Err(err) => {
				return Err(Error::new(
					ErrorKind::Other,
					format!("advance_epoch commit failed: {err}"),
				));
			}
		}
		Ok(())
	}

	async fn reconcile_pending_epoch_locked(&self) -> std::io::Result<()> {
		let snapshot = self.state_machine.state_snapshot().await;
		let Some((&latest_epoch, _)) = snapshot.epoch_history.iter().next_back() else {
			return Ok(());
		};
		let visible = self.epoch_rx.borrow().clone();
		if latest_epoch <= visible.epoch {
			return Ok(());
		}
		let next_epoch = visible.epoch.saturating_add(1);
		let Some(next_cfg) = snapshot.epoch_history.get(&next_epoch) else {
			return Ok(());
		};

		let previous_cfg = snapshot
			.epoch_history
			.get(&visible.epoch)
			.cloned()
			.unwrap_or_else(|| epoch_config_from_membership(&visible));
		let target = membership_from_epoch_config(next_cfg);
		let expected = build_seed_batch(
			&membership_from_epoch_config(&previous_cfg),
			&target,
			self.replication_factor as usize,
			&snapshot,
		)?;
		let missing = missing_seed_batch(&expected, &snapshot.migrations)?;
		drop(snapshot);

		if !missing.is_empty() {
			if !self.is_leader().await {
				return Err(not_leader_error(self.leader_hint().await));
			}
			for seed in missing {
				match self
					.raft
					.client_write(WendelonRequest::SeedMigration(seed))
					.await
				{
					Ok(_) => {}
					Err(_err) if !self.is_leader().await => {
						return Err(not_leader_error(self.leader_hint().await));
					}
					Err(err) => {
						return Err(Error::new(
							ErrorKind::Other,
							format!("seed migration commit failed: {err}"),
						));
					}
				}
			}
		}

		self.epoch_tx
			.send(Arc::new(target))
			.map_err(|_| Error::new(ErrorKind::BrokenPipe, "epoch watch receiver closed"))?;
		Ok(())
	}

	async fn cluster_status_wire(&self) -> ClusterStatusWire {
		let membership = self.epoch_rx.borrow().clone();
		let mut peers: Vec<ClusterPeerWire> = membership
			.peers
			.iter()
			.map(|(node_id, caps)| ClusterPeerWire {
				node_id: node_id.id,
				address: caps.address.clone(),
				tier_bits: caps.tiers.as_u8(),
				role_bits: caps.roles.as_u8(),
				capability_bits: caps.capabilities.as_u8(),
			})
			.collect();
		peers.sort_by_key(|peer| peer.node_id);

		ClusterStatusWire {
			epoch: membership.epoch,
			leader: self.raft.current_leader().await,
			replication_factor: self.replication_factor,
			peers,
		}
	}

	async fn cluster_status_leases(&self) -> Vec<LeaseDescriptor> {
		let mut leases: Vec<LeaseDescriptor> = self
			.state_machine
			.state_snapshot()
			.await
			.leases
			.values()
			.copied()
			.collect();
		leases.sort_by_key(|lease| (lease.range_start, lease.range_end, lease.holder.id));
		leases
	}

	fn current_epoch(&self) -> u64 {
		self.epoch_rx.borrow().epoch
	}

	async fn leader_hint(&self) -> Option<u32> {
		self.raft.current_leader().await
	}

	async fn is_leader(&self) -> bool {
		self.raft.current_leader().await == Some(self.local_node.id)
	}

	fn lease_response(
		&self,
		frame: &HrftFrame,
		status: LeaseStatus,
		lease: Option<&LeaseDescriptor>,
	) -> std::io::Result<HrftFrame> {
		let meta = AcquireLeaseResponseMeta {
			status,
			_pad: [0; 7],
		};
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let trailing = lease.map(|lease| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(lease));
			buf
		});
		let trailing_len = trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0);

		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::LEASE_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing,
		})
	}

	fn release_response(
		&self,
		frame: &HrftFrame,
		status: LeaseStatus,
	) -> std::io::Result<HrftFrame> {
		let meta = ReleaseLeaseResponseMeta {
			status,
			_pad: [0; 7],
		};
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::LEASE_RELEASE_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing: None,
		})
	}

	async fn validate_acquire_lease_target(
		&self,
		remote: NodeId,
		req: &AcquireLeaseRequestMeta,
	) -> std::io::Result<()> {
		if remote != req.holder {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"lease holder {} does not match remote caller {}",
					req.holder.id, remote.id
				),
			));
		}
		if req.range_end < req.range_start {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"lease range_end must be greater than or equal to range_start",
			));
		}
		let membership = self.epoch_rx.borrow().clone();
		let ring = RingState::build(&membership, self.replication_factor as usize);
		let canonical = ring
			.primary_ranges_for(req.holder)
			.into_iter()
			.any(|(start, end)| start == req.range_start && end == req.range_end);
		if canonical {
			Ok(())
		} else {
			Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"lease range [{}, {}] is not a canonical primary range for holder {} in epoch {}",
					req.range_start, req.range_end, req.holder.id, req.epoch
				),
			))
		}
	}

	fn transfer_response(
		&self,
		frame: &HrftFrame,
		resp: TransferLeadershipResponseMeta,
		token: Option<&TransferToken>,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, &resp)?;
		let trailing = token.map(|token| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(token));
			buf
		});
		let trailing_len = trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0);
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TRANSFER_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len,
			},
			meta,
			arrow_payload: None,
			trailing,
		})
	}

	fn join_response(
		&self,
		frame: &HrftFrame,
		resp: JoinClusterResponseWire,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, &resp)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::JOIN_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}

	fn decode_transfer_token(&self, frame: &HrftFrame) -> std::io::Result<TransferToken> {
		let bytes = frame.trailing.as_deref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"TRANSFER_REQ missing TransferToken trailing payload",
			)
		})?;
		if bytes.len() != std::mem::size_of::<TransferToken>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"TRANSFER_REQ TransferToken size mismatch: got {}, expected {}",
					bytes.len(),
					std::mem::size_of::<TransferToken>()
				),
			));
		}
		Ok(*bytemuck::from_bytes(bytes))
	}

	async fn validate_transfer_request(
		&self,
		req: &TransferLeadershipRequestMeta,
		token: &TransferToken,
		remote: NodeId,
	) -> std::io::Result<()> {
		if req.source_epoch == 0
			|| req.from_leader.id == 0
			|| req.to_leader.id == 0
			|| req.from_leader == req.to_leader
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"invalid transfer request identity fields",
			));
		}
		if token.source_epoch != req.source_epoch
			|| token.from_leader != req.from_leader
			|| token.to_leader != req.to_leader
			|| token.target_epoch != token.source_epoch.saturating_add(1)
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"transfer token does not match request metadata",
			));
		}
		let now_ms = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_millis() as u64)
			.unwrap_or(0);
		if now_ms >= token.expires_at.physical_ms {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"transfer token has expired",
			));
		}
		if token.source_epoch != self.current_epoch() {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"transfer token epoch is stale",
			));
		}
		if self.local_node == token.to_leader && remote != token.from_leader {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"transfer token was not forwarded by leader {}",
					token.from_leader.id
				),
			));
		}
		if self.local_node == token.from_leader
			&& self.raft.current_leader().await != Some(token.from_leader.id)
		{
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"transfer token issuer is not the current leader",
			));
		}
		self.transfer_authority.verify_token(token)?;
		Ok(())
	}

	fn transfer_target_known(&self, target: &NodeId) -> bool {
		self.epoch_rx
			.borrow()
			.peers
			.get(target)
			.map(|caps| caps.roles.contains(NodeRole::Control))
			.unwrap_or(false)
	}

	async fn forward_transfer_request(
		&self,
		frame: &HrftFrame,
		req: &TransferLeadershipRequestMeta,
		token: &TransferToken,
	) -> std::io::Result<TransferLeadershipResponseMeta> {
		let conn = self.factory.get_connection(req.to_leader).await?;
		let mut stream = conn.open_bi().await?;
		let meta = wire::encode(req)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TRANSFER_REQ.0,
			flags: 0,
			stream_id: frame.header.stream_id,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: std::mem::size_of::<TransferToken>() as u32,
		};
		write_frame(
			&mut *stream,
			&header,
			&meta,
			None,
			Some(bytemuck::bytes_of(token)),
		)
		.await?;
		stream.finish().await?;
		let resp = read_frame(&mut *stream, &self.pool).await?;
		if resp.header.msg_type != MsgType::TRANSFER_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"transfer target returned wrong msg_type=0x{:02x}",
					resp.header.msg_type
				),
			));
		}
		wire::decode(&resp.meta)
	}

	fn validate_join_request(&self, req: &JoinClusterRequestWire) -> std::io::Result<()> {
		if req.node_id == 0 || req.node_id == self.local_node.id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"join request must target a non-zero remote node id",
			));
		}
		if req.advertise_address.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"join request advertise_address must not be empty",
			));
		}
		let tiers = TransportTierSet::from_u8(req.tier_bits);
		if tiers.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"join request must advertise at least one transport tier",
			));
		}
		let roles = NodeRoleSet::from_u8(req.role_bits);
		if !roles.contains(NodeRole::Control) || !roles.contains(NodeRole::Data) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"join request must advertise Control Ring and Data Ring roles",
			));
		}
		wire::validate_protocol_window(req.protocol)?;
		Ok(())
	}

	async fn accept_join(&self, req: &JoinClusterRequestWire) -> std::io::Result<()> {
		let joining = NodeId {
			id: req.node_id,
			_reserved: 0,
		};
		let joining_caps = PeerCapabilities {
			address: req.advertise_address.clone(),
			tiers: TransportTierSet::from_u8(req.tier_bits),
			roles: NodeRoleSet::from_u8(req.role_bits),
			capabilities: NodeCapabilitySet::from_u8(req.capability_bits),
		};
		let joining_node = BasicNode {
			addr: req.advertise_address.clone(),
		};

		match self.raft.add_learner(req.node_id, joining_node, true).await {
			Ok(_) => {}
			Err(_err) if !self.is_leader().await => {
				return Err(not_leader_error(self.leader_hint().await));
			}
			Err(err) => {
				return Err(Error::new(
					ErrorKind::Other,
					format!("add_learner failed: {err}"),
				));
			}
		}

		let snapshot = self.state_machine.state_snapshot().await;
		let mut desired_voters: BTreeSet<u32> = snapshot.last_membership.voter_ids().collect();
		if desired_voters.is_empty() {
			desired_voters.insert(self.local_node.id);
		}
		let promote_to_voter = desired_voters.insert(req.node_id);
		drop(snapshot);

		if promote_to_voter {
			match self.raft.change_membership(desired_voters, true).await {
				Ok(_) => {}
				Err(_err) if !self.is_leader().await => {
					return Err(not_leader_error(self.leader_hint().await));
				}
				Err(err) => {
					return Err(Error::new(
						ErrorKind::Other,
						format!("change_membership failed: {err}"),
					));
				}
			}
		}

		let visible = self.epoch_rx.borrow().clone();
		let needs_epoch_advance = match visible.peers.get(&joining) {
			Some(existing) => {
				existing.address != joining_caps.address
					|| existing.tiers.as_u8() != joining_caps.tiers.as_u8()
					|| existing.roles.as_u8() != joining_caps.roles.as_u8()
			}
			None => true,
		};
		if needs_epoch_advance {
			let mut next = visible.as_ref().clone();
			next.epoch = next.epoch.saturating_add(1);
			next.peers.insert(joining, joining_caps);
			self.advance_membership_epoch(next).await?;
		}

		Ok(())
	}

	async fn forward_join_request(
		&self,
		frame: &HrftFrame,
		req: &JoinClusterRequestWire,
	) -> std::io::Result<JoinClusterResponseWire> {
		let leader_id = self
			.leader_hint()
			.await
			.filter(|leader_id| *leader_id != self.local_node.id)
			.ok_or_else(|| not_leader_error(None))?;
		let conn = self
			.factory
			.get_connection(NodeId {
				id: leader_id,
				_reserved: 0,
			})
			.await?;
		let mut stream = conn.open_bi().await?;
		let meta = wire::encode(req)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: crate::schema::HRFT_ENVELOPE_VERSION,
			msg_type: MsgType::JOIN_REQ.0,
			flags: 0,
			stream_id: frame.header.stream_id,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, &meta, None, None).await?;
		stream.finish().await?;
		let resp = read_frame(&mut *stream, &self.pool).await?;
		if resp.header.msg_type != MsgType::JOIN_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"join target returned wrong msg_type=0x{:02x}",
					resp.header.msg_type
				),
			));
		}
		wire::decode(&resp.meta)
	}

	async fn accept_transfer_target(&self, token: &TransferToken) -> std::io::Result<()> {
		self.raft
			.trigger()
			.elect()
			.await
			.map_err(|err| Error::new(ErrorKind::Other, format!("trigger_elect failed: {err}")))?;
		let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
		loop {
			if self.raft.current_leader().await == Some(self.local_node.id) {
				return Ok(());
			}
			if tokio::time::Instant::now() >= deadline {
				return Err(Error::new(
					ErrorKind::TimedOut,
					format!(
						"target node {} did not become leader for transfer from {} in epoch {}",
						self.local_node.id, token.from_leader.id, token.source_epoch
					),
				));
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}
}

#[async_trait]
impl ControlRingServer for WendelonControlServer {
	async fn handle_control_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		let msg_type = MsgType(frame.header.msg_type);
		let resp = match msg_type {
			MsgType::STATUS_REQ => {
				self.authorize_control_operation(&context, ControlOperation::Status)?;
				self.handle_status(frame).await?
			}
			MsgType::AUDIT_CHAIN_HEAD_REQ => {
				self.authorize_control_operation(&context, ControlOperation::AuditChainHead)?;
				self.handle_audit_chain_head(frame).await?
			}
			MsgType::REVOCATION_ADMIN_REQ => {
				self.authorize_control_operation(&context, ControlOperation::RevocationAdmin)?;
				self.handle_revocation_admin(&context, frame).await?
			}
			MsgType::LEASE_REQ => {
				self.authorize_control_operation(&context, ControlOperation::Lease)?;
				self.handle_lease(context.remote, frame).await?
			}
			MsgType::LEASE_RELEASE_REQ => {
				self.authorize_control_operation(&context, ControlOperation::LeaseRelease)?;
				self.handle_release(context.remote, frame).await?
			}
			MsgType::TRANSFER_REQ => {
				self.authorize_control_operation(&context, ControlOperation::Transfer)?;
				self.handle_transfer(&context, context.remote, frame)
					.await?
			}
			MsgType::EPOCH_ADV_REQ => {
				self.authorize_control_operation(&context, ControlOperation::AdvanceEpoch)?;
				self.handle_advance_epoch(&context, frame).await?
			}
			MsgType::JOIN_REQ => {
				self.authorize_control_operation(&context, ControlOperation::Join)?;
				self.handle_join(&context, frame).await?
			}
			MsgType::SCHEMA_UPDATE_REQ => {
				self.authorize_control_operation(&context, ControlOperation::SchemaUpdate)?;
				self.handle_schema_update(&context, frame).await?
			}
			MsgType::SCHEMA_DROP_REQ => {
				self.authorize_control_operation(&context, ControlOperation::SchemaDrop)?;
				self.handle_schema_drop(&context, frame).await?
			}
			MsgType::TABLE_DESCRIPTOR_GET_REQ => {
				self.handle_table_descriptor_get(&context, frame).await?
			}
			MsgType::LOGICAL_TYPE_GET_REQ => self.handle_logical_type_get(&context, frame).await?,
			MsgType::LOGICAL_TYPE_LIST_REQ => {
				self.handle_logical_type_list(&context, frame).await?
			}
			MsgType::TENANT_ADMIN_REQ => self.handle_tenant_admin(&context, frame).await?,
			_ => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("unsupported Control Ring msg_type=0x{:02x}", msg_type.0),
				));
			}
		};
		Ok(Some(resp))
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeedPlanSegment {
	start: u64,
	end: u64,
	logical_owner: NodeId,
	source_replicas: Vec<NodeId>,
	upstream_migration_id: Option<u64>,
	migration_hlc_floor: HybridTimestamp,
}

#[derive(Default)]
struct PeerOverrides {
	tier_bits: Option<u8>,
	role_bits: Option<u8>,
}

pub(crate) fn select_bootstrap_epoch(
	snapshot: &crate::consensus::raft_storage::SmState,
	fallback: &MembershipEpoch,
	rf: usize,
) -> std::io::Result<MembershipEpoch> {
	let Some((&latest_epoch, latest_cfg)) = snapshot.epoch_history.iter().next_back() else {
		return Ok(fallback.clone());
	};
	let latest = membership_from_epoch_config(latest_cfg);
	let Some((_, previous_cfg)) = snapshot.epoch_history.range(..latest_epoch).next_back() else {
		return Ok(latest);
	};
	let previous = membership_from_epoch_config(previous_cfg);
	let expected = build_seed_batch(&previous, &latest, rf, snapshot)?;
	if missing_seed_batch(&expected, &snapshot.migrations)?.is_empty() {
		Ok(latest)
	} else {
		Ok(previous)
	}
}

pub(crate) fn epoch_config_from_membership(epoch: &MembershipEpoch) -> EpochConfig {
	let mut peers = BTreeMap::new();
	for (node_id, caps) in &epoch.peers {
		peers.insert(
			node_id.id,
			EpochPeer {
				node_id: *node_id,
				address: caps.address.clone(),
				tier_bits: caps.tiers.as_u8(),
				role_bits: caps.roles.as_u8(),
				capability_bits: caps.capabilities.as_u8(),
			},
		);
	}
	EpochConfig {
		epoch: epoch.epoch,
		peers,
	}
}

pub(crate) fn membership_from_epoch_config(config: &EpochConfig) -> MembershipEpoch {
	let mut peers = HashMap::with_capacity(config.peers.len());
	for peer in config.peers.values() {
		peers.insert(
			peer.node_id,
			PeerCapabilities {
				address: peer.address.clone(),
				tiers: TransportTierSet::from_u8(peer.tier_bits),
				roles: NodeRoleSet::from_u8(peer.role_bits),
				capabilities: NodeCapabilitySet::from_u8(peer.capability_bits),
			},
		);
	}
	MembershipEpoch {
		epoch: config.epoch,
		peers,
	}
}

pub(crate) fn build_seed_batch(
	from: &MembershipEpoch,
	to: &MembershipEpoch,
	rf: usize,
	snapshot: &crate::consensus::raft_storage::SmState,
) -> std::io::Result<Vec<SeedMigration>> {
	let old_ring = RingState::build(from, rf);
	let new_ring = RingState::build(to, rf);
	let old_segments = old_ring.replica_segments();
	let new_segments = new_ring.replica_segments();
	if old_segments.is_empty() || new_segments.is_empty() {
		return Ok(Vec::new());
	}

	let mut planned: Vec<SeedPlanSegment> = Vec::new();
	let mut i = 0usize;
	let mut j = 0usize;
	while i < old_segments.len() && j < new_segments.len() {
		let old_seg = &old_segments[i];
		let new_seg = &new_segments[j];
		let start = old_seg.start.max(new_seg.start);
		let end = old_seg.end.min(new_seg.end);
		if start <= end {
			let old_primary = old_seg.replicas.first().copied();
			let new_primary = new_seg.replicas.first().copied();
			if let (Some(old_owner), Some(new_owner)) = (old_primary, new_primary) {
				if old_owner.id != new_owner.id {
					let (source_replicas, upstream_migration_id, migration_hlc_floor) =
						if let Some(upstream) =
							find_upstream_migration(snapshot, start, end, old_owner, to.epoch)
						{
							(
								upstream.source_replicas.clone(),
								Some(upstream.migration_id),
								upstream.migration_hlc_floor,
							)
						} else {
							(
								old_seg.replicas.clone(),
								None,
								HybridTimestamp {
									physical_ms: 0,
									logical: 0,
									node_id: 0,
								},
							)
						};
					push_or_merge_seed_segment(
						&mut planned,
						SeedPlanSegment {
							start,
							end,
							logical_owner: new_owner,
							source_replicas,
							upstream_migration_id,
							migration_hlc_floor,
						},
					);
				}
			}
		}
		if old_seg.end == end {
			i += 1;
		}
		if new_seg.end == end {
			j += 1;
		}
	}

	let mut out = Vec::with_capacity(planned.len());
	for segment in planned {
		out.push(SeedMigration {
			entry: MigrationLedgerEntry {
				migration_id: deterministic_migration_id(to.epoch, segment.start, segment.end),
				range_start: segment.start,
				range_end: segment.end,
				from_epoch: from.epoch,
				to_epoch: to.epoch,
				logical_owner: segment.logical_owner,
				upstream_migration_id: segment.upstream_migration_id,
				source_replicas: segment.source_replicas,
				migration_hlc_floor: segment.migration_hlc_floor,
				state: MigrationState::Pending,
				completed_by: None,
				completed_at_hlc: None,
			},
		});
	}
	Ok(out)
}

fn missing_seed_batch(
	expected: &[SeedMigration],
	existing: &HashMap<u64, MigrationLedgerEntry>,
) -> std::io::Result<Vec<SeedMigration>> {
	let mut missing = Vec::new();
	for seed in expected {
		match existing.get(&seed.entry.migration_id) {
			Some(entry) if entry == &seed.entry => {}
			Some(_) => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"migration_id {} already exists with different contents",
						seed.entry.migration_id
					),
				));
			}
			None => missing.push(seed.clone()),
		}
	}
	Ok(missing)
}

fn decode_epoch_batch(bytes: &[u8]) -> std::io::Result<arrow::record_batch::RecordBatch> {
	let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| {
		crate::network::ipc::observe_wire_decode_error_for_bytes(bytes);
		Error::new(ErrorKind::InvalidData, format!("epoch batch decode: {e}"))
	})?;
	let batch = reader
		.next()
		.transpose()
		.map_err(|e| {
			crate::network::ipc::observe_wire_decode_error_for_bytes(bytes);
			Error::new(ErrorKind::InvalidData, format!("epoch batch read: {e}"))
		})?
		.ok_or_else(|| {
			crate::network::ipc::observe_wire_decode_error_for_bytes(bytes);
			Error::new(ErrorKind::InvalidData, "epoch batch stream is empty")
		})?;
	Ok(batch)
}

fn membership_from_epoch_batch(
	batch: &arrow::record_batch::RecordBatch,
	epoch: u64,
	current: &MembershipEpoch,
) -> std::io::Result<MembershipEpoch> {
	if batch.num_rows() != 1 {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"epoch batch must contain exactly one row, got {}",
				batch.num_rows()
			),
		));
	}

	let voter_ids = required_list_u32(batch, "voter_ids")?;
	let voter_addresses = required_list_string(batch, "voter_addresses")?;
	let learner_ids = optional_list_u32(batch, "learner_ids")?;
	let learner_addresses = optional_list_string(batch, "learner_addresses")?;
	if voter_ids.len() != voter_addresses.len() || learner_ids.len() != learner_addresses.len() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"epoch batch id/address list lengths do not match",
		));
	}

	let mut overrides = HashMap::<u32, PeerOverrides>::new();
	for (key, value) in optional_list_string(batch, "meta_keys")?
		.into_iter()
		.zip(optional_list_string(batch, "meta_values")?)
	{
		if let Some(node_id) = key
			.strip_prefix("node.")
			.and_then(|rest| rest.strip_suffix(".tier_bits"))
		{
			let bits = value.parse::<u8>().map_err(|e| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("invalid tier_bits override {value:?}: {e}"),
				)
			})?;
			overrides
				.entry(node_id.parse().map_err(|e| {
					Error::new(
						ErrorKind::InvalidInput,
						format!("invalid node id in meta key {key:?}: {e}"),
					)
				})?)
				.or_default()
				.tier_bits = Some(bits);
		} else if let Some(node_id) = key
			.strip_prefix("node.")
			.and_then(|rest| rest.strip_suffix(".role_bits"))
		{
			let bits = value.parse::<u8>().map_err(|e| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("invalid role_bits override {value:?}: {e}"),
				)
			})?;
			overrides
				.entry(node_id.parse().map_err(|e| {
					Error::new(
						ErrorKind::InvalidInput,
						format!("invalid node id in meta key {key:?}: {e}"),
					)
				})?)
				.or_default()
				.role_bits = Some(bits);
		}
	}

	let mut peers = HashMap::new();
	for (id, address) in voter_ids.into_iter().zip(voter_addresses) {
		insert_epoch_peer(&mut peers, current, &overrides, id, address)?;
	}
	for (id, address) in learner_ids.into_iter().zip(learner_addresses) {
		insert_epoch_peer(&mut peers, current, &overrides, id, address)?;
	}
	if peers.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"epoch batch defines no peers",
		));
	}

	Ok(MembershipEpoch { epoch, peers })
}

fn insert_epoch_peer(
	peers: &mut HashMap<NodeId, PeerCapabilities>,
	current: &MembershipEpoch,
	overrides: &HashMap<u32, PeerOverrides>,
	id: u32,
	address: String,
) -> std::io::Result<()> {
	let node_id = NodeId { id, _reserved: 0 };
	if peers.contains_key(&node_id) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("duplicate node {id} in epoch batch"),
		));
	}
	let mut caps = current
		.peers
		.get(&node_id)
		.cloned()
		.unwrap_or(PeerCapabilities {
			address: address.clone(),
			tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
			roles: NodeRoleSet::empty()
				.with(NodeRole::Data)
				.with(NodeRole::Control),
			capabilities: NodeCapabilitySet::empty(),
		});
	caps.address = address;
	if let Some(override_bits) = overrides.get(&id).and_then(|o| o.tier_bits) {
		caps.tiers = TransportTierSet::from_u8(override_bits);
	}
	if let Some(override_bits) = overrides.get(&id).and_then(|o| o.role_bits) {
		caps.roles = NodeRoleSet::from_u8(override_bits);
	}
	peers.insert(node_id, caps);
	Ok(())
}

fn required_list_u32(
	batch: &arrow::record_batch::RecordBatch,
	name: &str,
) -> std::io::Result<Vec<u32>> {
	let idx = batch.schema().index_of(name).map_err(|e| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("missing epoch batch column {name:?}: {e}"),
		)
	})?;
	list_u32_values(batch.column(idx).as_ref(), name, false)
}

fn optional_list_u32(
	batch: &arrow::record_batch::RecordBatch,
	name: &str,
) -> std::io::Result<Vec<u32>> {
	let idx = batch.schema().index_of(name).map_err(|e| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("missing epoch batch column {name:?}: {e}"),
		)
	})?;
	list_u32_values(batch.column(idx).as_ref(), name, true)
}

fn required_list_string(
	batch: &arrow::record_batch::RecordBatch,
	name: &str,
) -> std::io::Result<Vec<String>> {
	let idx = batch.schema().index_of(name).map_err(|e| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("missing epoch batch column {name:?}: {e}"),
		)
	})?;
	list_string_values(batch.column(idx).as_ref(), name, false)
}

fn optional_list_string(
	batch: &arrow::record_batch::RecordBatch,
	name: &str,
) -> std::io::Result<Vec<String>> {
	let idx = batch.schema().index_of(name).map_err(|e| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("missing epoch batch column {name:?}: {e}"),
		)
	})?;
	list_string_values(batch.column(idx).as_ref(), name, true)
}

fn list_u32_values(array: &dyn Array, name: &str, nullable: bool) -> std::io::Result<Vec<u32>> {
	let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("epoch batch column {name:?} is not a ListArray"),
		)
	})?;
	if list.len() != 1 {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("epoch batch column {name:?} must contain exactly one row"),
		));
	}
	if nullable && list.is_null(0) {
		return Ok(Vec::new());
	}
	let values = list.value(0);
	let ints = values
		.as_any()
		.downcast_ref::<UInt32Array>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("epoch batch column {name:?} does not contain UInt32 values"),
			)
		})?;
	Ok((0..ints.len()).map(|idx| ints.value(idx)).collect())
}

fn list_string_values(
	array: &dyn Array,
	name: &str,
	nullable: bool,
) -> std::io::Result<Vec<String>> {
	let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("epoch batch column {name:?} is not a ListArray"),
		)
	})?;
	if list.len() != 1 {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("epoch batch column {name:?} must contain exactly one row"),
		));
	}
	if nullable && list.is_null(0) {
		return Ok(Vec::new());
	}
	let values = list.value(0);
	let strings = values
		.as_any()
		.downcast_ref::<StringArray>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("epoch batch column {name:?} does not contain Utf8 values"),
			)
		})?;
	Ok((0..strings.len())
		.map(|idx| strings.value(idx).to_string())
		.collect())
}

fn find_upstream_migration(
	snapshot: &crate::consensus::raft_storage::SmState,
	range_start: u64,
	range_end: u64,
	old_primary: NodeId,
	to_epoch: u64,
) -> Option<MigrationLedgerEntry> {
	snapshot
		.migrations
		.values()
		.filter(|entry| {
			entry.state != MigrationState::Completed
				&& entry.to_epoch < to_epoch
				&& entry.logical_owner.id == old_primary.id
				&& entry.range_start <= range_end
				&& range_start <= entry.range_end
		})
		.cloned()
		.max_by_key(|entry| (entry.to_epoch, entry.migration_id))
}

fn push_or_merge_seed_segment(out: &mut Vec<SeedPlanSegment>, segment: SeedPlanSegment) {
	match out.last_mut() {
		Some(last)
			if last.end != u64::MAX
				&& last.end.saturating_add(1) == segment.start
				&& last.logical_owner == segment.logical_owner
				&& last.source_replicas == segment.source_replicas
				&& last.upstream_migration_id == segment.upstream_migration_id
				&& last.migration_hlc_floor == segment.migration_hlc_floor =>
		{
			last.end = segment.end;
		}
		_ => out.push(segment),
	}
}

fn deterministic_migration_id(to_epoch: u64, range_start: u64, range_end: u64) -> u64 {
	let mut input = [0u8; 24];
	input[..8].copy_from_slice(&to_epoch.to_le_bytes());
	input[8..16].copy_from_slice(&range_start.to_le_bytes());
	input[16..24].copy_from_slice(&range_end.to_le_bytes());
	crate::hash::sha256::prefix_u64(&input)
}

fn membership_eq(left: &MembershipEpoch, right: &MembershipEpoch) -> bool {
	epoch_config_from_membership(left) == epoch_config_from_membership(right)
}

fn not_leader_error(leader_hint: Option<u32>) -> Error {
	Error::new(
		ErrorKind::NotConnected,
		format!("not leader (leader_hint={leader_hint:?})"),
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::consensus::TieredSlab;
	use crate::crypto::{TenantKeyset, WrappedTenantKek};
	use crate::network::tls::{ActorIdentity, ActorRole, ActorTrustDomain};
	use crate::network::transport::{TransportTier, TransportTierSet};
	use crate::schema::table::{decode_descriptor_wire_name, encode_descriptor_wire_name};
	use crate::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RecordBodyCompressionPolicy, RetentionWindowMillis,
		RowIdentityPolicy, SchemaEvolutionPolicyV1, SlabId, TableSchemaCatalogV1,
		decode_table_schema_descriptor_v1, encode_table_schema_descriptor_v1,
	};
	use arrow::datatypes::{DataType, Field, Schema};

	fn n(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn hlc(physical_ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms,
			logical: 0,
			node_id: 7,
		}
	}

	fn descriptor_for_get(
		table_object_id: TableObjectId,
		schema_generation: u64,
		namespace: &str,
		table_name: &str,
	) -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::Utf8, true),
		]);
		TableSchemaDescriptor::new(
			table_object_id,
			schema_generation,
			namespace,
			table_name,
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn membership(epoch: u64, node_ids: &[u32]) -> MembershipEpoch {
		let mut peers = HashMap::new();
		for id in node_ids {
			peers.insert(
				n(*id),
				PeerCapabilities {
					address: format!("127.0.0.1:{id}"),
					tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
					roles: NodeRoleSet::empty()
						.with(NodeRole::Data)
						.with(NodeRole::Control),
					capabilities: NodeCapabilitySet::empty(),
				},
			);
		}
		MembershipEpoch { epoch, peers }
	}

	#[test]
	fn descriptor_get_leader_returns_latest_descriptor() {
		let tenant_id = TenantId::new(7);
		let table_object_id = TableObjectId::new([0x72; 16]);
		let v1 = descriptor_for_get(table_object_id, 1, "analytics", "events");
		let v2 = descriptor_for_get(table_object_id, 2, "analytics", "events");
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant_id, v1).unwrap();
		catalog.upsert(tenant_id, v2.clone()).unwrap();

		assert_eq!(
			catalog.latest_by_name(tenant_id, "analytics", "events"),
			Some(&v2)
		);
	}

	#[test]
	fn descriptor_get_returns_latest_not_historical_after_update() {
		let tenant_id = TenantId::new(7);
		let table_object_id = TableObjectId::new([0x73; 16]);
		let old = descriptor_for_get(table_object_id, 1, "analytics", "events");
		let latest = descriptor_for_get(table_object_id, 3, "analytics", "events");
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant_id, old.clone()).unwrap();
		catalog.upsert(tenant_id, latest.clone()).unwrap();

		let actual = catalog
			.latest_by_name(tenant_id, "analytics", "events")
			.unwrap();
		assert_eq!(actual.schema_generation, 3);
		assert_ne!(actual.schema_generation, old.schema_generation);
	}

	#[test]
	fn descriptor_get_follower_returns_not_leader_with_hint() {
		let leader = NodeId {
			id: 9,
			_reserved: 0,
		};
		let response = TableDescriptorGetResponseMeta {
			accepted: 0,
			reason_code: CONTROL_REASON_NOT_LEADER,
			_pad1: [0; 6],
			leader_hint: leader,
			descriptor_len: 0,
			_pad2: [0; 4],
		};

		assert_eq!(response.accepted, 0);
		assert_eq!(response.reason_code, CONTROL_REASON_NOT_LEADER);
		assert_eq!(response.leader_hint, leader);
		assert_eq!(response.descriptor_len, 0);
	}

	#[test]
	fn descriptor_get_missing_table_is_not_found() {
		let tenant_id = TenantId::new(7);
		let table_object_id = TableObjectId::new([0x74; 16]);
		let descriptor = descriptor_for_get(table_object_id, 1, "analytics", "events");
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant_id, descriptor).unwrap();

		assert!(
			catalog
				.latest_by_name(TenantId::new(8), "analytics", "events")
				.is_none()
		);
		assert!(catalog.latest_by_name(tenant_id, "ops", "events").is_none());
		assert!(
			catalog
				.latest_by_name(tenant_id, "analytics", "missing")
				.is_none()
		);

		catalog
			.unbind_latest_name(tenant_id, table_object_id, 1)
			.unwrap();
		assert!(
			catalog
				.latest_by_name(tenant_id, "analytics", "events")
				.is_none()
		);
	}

	#[test]
	fn descriptor_get_uses_descriptor_authorization_helper() {
		let tenant_id = TenantId::new(7);
		let unauthorized = ActorIdentity {
			role: ActorRole::QuerySession,
			trust_domain: ActorTrustDomain::Data,
			cert_subject: None,
			node_id: None,
			tenant_id: Some("7".to_string()),
			scopes: vec!["query_session".to_string()],
			key_id: vec![1],
			break_glass: false,
		};
		let authorized = ActorIdentity {
			role: ActorRole::TenantAdmin,
			trust_domain: ActorTrustDomain::Ops,
			cert_subject: None,
			node_id: None,
			tenant_id: Some("7".to_string()),
			scopes: Vec::new(),
			key_id: vec![2],
			break_glass: false,
		};

		assert!(
			WendelonControlServer::authorize_descriptor_get_actor(&unauthorized, tenant_id)
				.is_err()
		);
		assert!(
			WendelonControlServer::authorize_descriptor_get_actor(&authorized, tenant_id).is_ok()
		);
	}

	#[test]
	fn descriptor_get_cross_tenant_claim_is_authorization_failed() {
		let actor = ActorIdentity {
			role: ActorRole::TenantAdmin,
			trust_domain: ActorTrustDomain::Ops,
			cert_subject: None,
			node_id: None,
			tenant_id: Some("7".to_string()),
			scopes: Vec::new(),
			key_id: vec![3],
			break_glass: false,
		};

		let err = WendelonControlServer::authorize_descriptor_get_actor(&actor, TenantId::new(8))
			.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::PermissionDenied);
	}

	#[test]
	fn descriptor_get_cluster_actor_reads_any_tenant() {
		let actor = ActorIdentity {
			role: ActorRole::Operator,
			trust_domain: ActorTrustDomain::Ops,
			cert_subject: None,
			node_id: None,
			tenant_id: None,
			scopes: Vec::new(),
			key_id: vec![4],
			break_glass: false,
		};

		assert!(
			WendelonControlServer::authorize_descriptor_get_actor(&actor, TenantId::new(7)).is_ok()
		);
		assert!(
			WendelonControlServer::authorize_descriptor_get_actor(&actor, TenantId::new(8)).is_ok()
		);
	}

	#[test]
	fn descriptor_get_overlong_or_invalid_name_is_rejected() {
		let overlong = "a".repeat(129);
		assert!(encode_descriptor_wire_name("namespace", &overlong).is_err());

		let mut invalid_utf8 = [0u8; 128];
		invalid_utf8[0] = 0xFF;
		assert!(decode_descriptor_wire_name("namespace", &invalid_utf8).is_err());

		let invalid_character = encode_descriptor_wire_name("table name", "bad/name");
		assert!(invalid_character.is_err());
	}

	#[test]
	fn descriptor_get_returns_catalog_descriptor_unchanged() {
		let tenant_id = TenantId::new(7);
		let descriptor =
			descriptor_for_get(TableObjectId::new([0x75; 16]), 4, "analytics", "events");
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant_id, descriptor.clone()).unwrap();

		let latest = catalog
			.latest_by_name(tenant_id, "analytics", "events")
			.unwrap();
		let encoded = encode_table_schema_descriptor_v1(latest).unwrap();
		let decoded = decode_table_schema_descriptor_v1(&encoded).unwrap();

		assert_eq!(decoded, descriptor);
		assert_eq!(decoded.table_object_id, descriptor.table_object_id);
		assert_eq!(decoded.schema_generation, descriptor.schema_generation);
		assert_eq!(
			decoded.physical_descriptor_fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
	}

	#[test]
	fn descriptor_get_metrics_count_aggregate_outcomes() {
		let registry = crate::metrics::MetricsRegistry::default();
		registry.control_descriptor_get_requests_total.inc();
		registry.control_descriptor_get_success_total.inc();
		registry.control_descriptor_get_not_found_total.inc();
		registry.control_descriptor_get_not_leader_total.inc();
		registry
			.control_descriptor_get_authorization_failed_total
			.inc();
		registry.control_descriptor_get_invalid_request_total.inc();

		let rendered = registry.render_prometheus();
		for metric in [
			"wendelon_control_descriptor_get_requests_total 1",
			"wendelon_control_descriptor_get_success_total 1",
			"wendelon_control_descriptor_get_not_found_total 1",
			"wendelon_control_descriptor_get_not_leader_total 1",
			"wendelon_control_descriptor_get_authorization_failed_total 1",
			"wendelon_control_descriptor_get_invalid_request_total 1",
		] {
			assert!(rendered.contains(metric), "missing metric sample {metric}");
		}
		for line in rendered
			.lines()
			.filter(|line| line.contains("wendelon_control_descriptor_get"))
		{
			assert!(!line.contains('{'));
		}
	}

	#[test]
	fn tenant_admin_table_retention_policy_request_roundtrips() {
		let request = TenantAdminRequestWire::SetTableRetentionPolicy {
			target_tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([9; 16]),
			expected_policy_generation: PolicyGeneration::new(3),
			new_policy: TableRetentionPolicy::FixedWindow(
				RetentionWindowMillis::try_new(90_000).unwrap(),
			),
		};

		let encoded = wire::encode(&request).unwrap();
		let decoded: TenantAdminRequestWire = wire::decode(&encoded).unwrap();

		assert_eq!(decoded, request);
	}

	#[test]
	fn descriptor_mutation_metric_classifies_axes() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("age", DataType::Int64, true),
		]);
		let base = TableSchemaDescriptor::new(
			TableObjectId::new([0x71; 16]),
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		assert_eq!(
			WendelonControlServer::descriptor_mutation_metric(None, &base),
			Some(DescriptorMutationMetric::Combined)
		);

		let mut physical = base.clone();
		physical.schema_generation = 2;
		physical.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
		physical.refresh_descriptor_fingerprints().unwrap();
		assert_eq!(
			WendelonControlServer::descriptor_mutation_metric(Some(&base), &physical),
			Some(DescriptorMutationMetric::PhysicalOnly)
		);

		let evolved_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("age", DataType::Int64, true),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let logical = TableSchemaDescriptor::new(
			base.table_object_id,
			3,
			"analytics",
			"events",
			&evolved_schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		assert_eq!(
			WendelonControlServer::descriptor_mutation_metric(Some(&base), &logical),
			Some(DescriptorMutationMetric::Combined)
		);

		let mut generation_only = base.clone();
		generation_only.schema_generation = 4;
		generation_only.refresh_descriptor_fingerprints().unwrap();
		assert_eq!(
			WendelonControlServer::descriptor_mutation_metric(Some(&base), &generation_only),
			None
		);
	}

	#[test]
	fn build_seed_batch_reuses_upstream_lineage_for_chained_ranges() {
		let epoch1 = membership(1, &[1]);
		let epoch2 = membership(2, &[1, 2]);
		let epoch3 = membership(3, &[1, 3]);

		let mut snapshot = crate::consensus::raft_storage::SmState::default();
		let mut first_batch = build_seed_batch(&epoch1, &epoch2, 1, &snapshot).unwrap();
		assert!(!first_batch.is_empty());
		for seed in &mut first_batch {
			seed.entry.migration_hlc_floor = hlc(42);
			snapshot
				.migrations
				.insert(seed.entry.migration_id, seed.entry.clone());
		}

		let second_batch = build_seed_batch(&epoch2, &epoch3, 1, &snapshot).unwrap();
		assert!(!second_batch.is_empty());

		let chained: Vec<&SeedMigration> = second_batch
			.iter()
			.filter(|seed| seed.entry.upstream_migration_id.is_some())
			.collect();
		assert!(
			!chained.is_empty(),
			"expected at least one range to carry forward upstream lineage"
		);

		for seed in chained {
			let upstream_id = seed.entry.upstream_migration_id.unwrap();
			let upstream = snapshot.migrations.get(&upstream_id).unwrap();
			assert_eq!(upstream.logical_owner.id, 2);
			assert_ne!(seed.entry.logical_owner.id, upstream.logical_owner.id);
			assert_eq!(seed.entry.to_epoch, 3);
			assert_eq!(seed.entry.source_replicas, upstream.source_replicas);
			assert_eq!(seed.entry.migration_hlc_floor, upstream.migration_hlc_floor);
		}
	}

	#[test]
	fn tenant_kek_versions_wire_marks_old_unreferenced_keys_safe_to_retire() {
		let tenant_id = TenantId::new(7);
		let old_kek_id = TenantKekId::try_from(11).unwrap();
		let active_kek_id = TenantKekId::try_from(12).unwrap();
		let mut snapshot = crate::consensus::raft_storage::SmState::default();
		let mut keyset = TenantKeyset::default();
		keyset.active_tenant_kek_id = Some(active_kek_id);
		keyset.keys.insert(
			old_kek_id,
			WrappedTenantKek {
				wrapped_key: vec![1; 40],
			},
		);
		keyset.keys.insert(
			active_kek_id,
			WrappedTenantKek {
				wrapped_key: vec![2; 40],
			},
		);
		snapshot.tenant_keysets.insert(tenant_id, keyset);

		let (complete, versions) =
			WendelonControlServer::tenant_kek_versions_wire(&snapshot, tenant_id);
		assert!(complete);
		assert_eq!(versions.len(), 2);
		assert_eq!(versions[0].tenant_kek_id, old_kek_id);
		assert!(!versions[0].is_active);
		assert_eq!(versions[0].tiered_slab_ref_count, 0);
		assert!(versions[0].safe_to_retire);
		assert_eq!(versions[1].tenant_kek_id, active_kek_id);
		assert!(versions[1].is_active);
		assert!(!versions[1].safe_to_retire);
	}

	#[test]
	fn tenant_kek_versions_wire_blocks_retirement_when_refs_or_metadata_gaps_exist() {
		let tenant_id = TenantId::new(9);
		let old_kek_id = TenantKekId::try_from(21).unwrap();
		let active_kek_id = TenantKekId::try_from(22).unwrap();
		let mut snapshot = crate::consensus::raft_storage::SmState::default();
		let mut keyset = TenantKeyset::default();
		keyset.active_tenant_kek_id = Some(active_kek_id);
		keyset.keys.insert(
			old_kek_id,
			WrappedTenantKek {
				wrapped_key: vec![3; 40],
			},
		);
		keyset.keys.insert(
			active_kek_id,
			WrappedTenantKek {
				wrapped_key: vec![4; 40],
			},
		);
		snapshot.tenant_keysets.insert(tenant_id, keyset);
		snapshot.tiered_slabs.insert(
			SlabId::new(100),
			TieredSlab {
				slab_id: SlabId::new(100),
				l2_uri: "memory://tier/slab_100".into(),
				payload_hash: [7; 32],
				is_encrypted: true,
				tenant_id: Some(tenant_id),
				tenant_kek_id: Some(old_kek_id),
				stats_format_version: None,
				stats_block_len: None,
				stats_block_hash128: None,
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			},
		);
		snapshot.tiered_slabs.insert(
			SlabId::new(101),
			TieredSlab {
				slab_id: SlabId::new(101),
				l2_uri: "memory://tier/slab_101".into(),
				payload_hash: [8; 32],
				is_encrypted: true,
				tenant_id: None,
				tenant_kek_id: None,
				stats_format_version: None,
				stats_block_len: None,
				stats_block_hash128: None,
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			},
		);

		let (complete, versions) =
			WendelonControlServer::tenant_kek_versions_wire(&snapshot, tenant_id);
		assert!(!complete);
		let old = versions
			.iter()
			.find(|version| version.tenant_kek_id == old_kek_id)
			.unwrap();
		assert_eq!(old.tiered_slab_ref_count, 1);
		assert!(!old.safe_to_retire);
		let active = versions
			.iter()
			.find(|version| version.tenant_kek_id == active_kek_id)
			.unwrap();
		assert!(!active.safe_to_retire);
	}
}
