//! Server-side Control Ring surface.
//!
//! This module hangs off the existing HRFT listener and
//! [`MsgRouter`](crate::network::router::MsgRouter) demux rather than
//! introducing a separate control-ring bind. It assumes callers are
//! already cluster-internal and mTLS-authenticated at the transport
//! layer; the handler here focuses on routing, wire compatibility, and
//! leader-aware Control Ring behavior.

use crate::audit::{
	AdministrativeControlAction, AdministrativeControlSchema, AuditAuthority, AuditChainHead,
	AuditEvent, AuditOperation, AuditOutcome, AuditSchema, AuditSink,
};
use crate::client::topology::{ClusterPeerWire, ClusterStatusWire};
use crate::consensus::{
	AdvanceEpochRecord, CreateTenant, EpochConfig, EpochPeer, MigrationLedgerEntry, MigrationState,
	RevokeActorKey, SeedMigration, TableDropLogicalFinalizeRequest, TableDropPrepareRequest,
	TenantLifecycleState, TenantRecord, UpdateTenantQuota, WendelonRaft, WendelonRequest,
	WendelonResponse,
};
use crate::crypto::{CryptoManager, RetireTenantKekVersion};
use crate::governance::{AdministrativeAction, require_administrative_action};
use crate::integration::compute::{
	SchemaMutationKind, SchemaUpdateEnvelope, TableSchemaDropRequest, WendelonAdminError,
	decode_schema_update_envelope_v1, encode_logical_type_definition,
	encode_logical_type_definitions, encode_schema_update_envelope_v1,
};
use crate::metrics::{TableRetentionMetricPolicy, global as metrics};
use crate::network::core::{HRFT_MAGIC, HrftFrame, read_frame, write_frame};
use crate::network::factory::TransportProviderFactory;
use crate::network::lease_signing::LeaseAuthority;
use crate::network::pool::BufPool;
use crate::network::ring::{RingState, membership_from_epoch_config};
use crate::network::router::ControlRingServer;
use crate::network::server::RequestContext;
use crate::network::tls::{
	ActorIdentity, ActorPermission, ActorRole, ActorTrustDomain, authorize_actor,
};
use crate::network::traits::{TransferLeadershipRequestMeta, TransferLeadershipResponseMeta};
use crate::network::transfer_signing::TransferAuthority;
use crate::network::transport::{
	MembershipEpoch, NodeCapabilitySet, NodeRole, NodeRoleSet, PeerCapabilities, TransportTier,
	TransportTierSet,
};
use crate::network::wire;
use crate::schema::table::{decode_descriptor_wire_name, encode_table_schema_descriptors};
use crate::schema::{
	AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, CTAS_SAGA_REJECT_PREFIX, CtasOperationId,
	CtasSagaMutation, CtasSagaReject, CtasSagaResponse, FrameHeader, HybridTimestamp,
	LeaseDescriptor, LeaseStatus, LogicalTypeGetRequestMeta, LogicalTypeGetResponseMeta,
	LogicalTypeListRequestMeta, LogicalTypeListResponseMeta, MsgType, NodeId, PolicyGeneration,
	ProtocolVersionWindow, ReleaseLeaseRequestMeta, ReleaseLeaseResponseMeta,
	SecondaryIndexReplicaStatus, SecondaryIndexStatus, SecondaryIndexStatusRequest,
	TableDescriptorGetRequestMeta, TableDescriptorGetResponseMeta, TableDescriptorListRequestMeta,
	TableDescriptorListResponseMeta, TableObjectId, TableOptimizerFactsGetRequestMeta,
	TableOptimizerFactsGetResponseMeta, TableRetentionPolicy, TableSchemaDescriptor, TenantId,
	TenantKekId, TransferToken, decode_secondary_index_lifecycle_complete,
	decode_table_schema_descriptor_v1, encode_table_optimizer_facts,
	encode_table_schema_descriptor_v1, epoch_entry_var_schema,
};
#[cfg(test)]
use crate::schema::{SupersessionMetadata, TombstoneMetadata};
use crate::storage::maintenance_policy::{
	MAINTENANCE_REPORT_MAX_BYTES, MAINTENANCE_REPORT_MAX_ITEMS, MaintenanceFingerprint,
	MaintenanceOperation, MaintenancePolicyInner, MaintenanceReportItem,
	MaintenanceReportPageError, RequeueRefusal,
};
use crate::storage::manager::SlabManager;
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

#[cfg(any(test, feature = "test-support"))]
pub mod adr0093_test_support {
	use crate::schema::TableObjectId;
	use std::collections::BTreeSet;
	use std::sync::{Mutex, OnceLock};

	#[derive(Default)]
	struct Hooks {
		sever_response_for: BTreeSet<TableObjectId>,
	}

	fn hooks() -> &'static Mutex<Hooks> {
		static HOOKS: OnceLock<Mutex<Hooks>> = OnceLock::new();
		HOOKS.get_or_init(|| Mutex::new(Hooks::default()))
	}

	pub struct HookGuard {
		table_object_id: TableObjectId,
	}

	impl Drop for HookGuard {
		fn drop(&mut self) {
			let mut hooks = hooks().lock().expect("ADR 0093 test hooks lock poisoned");
			hooks.sever_response_for.remove(&self.table_object_id);
		}
	}

	pub fn sever_next_drop_response(table_object_id: TableObjectId) -> HookGuard {
		let mut hooks = hooks().lock().expect("ADR 0093 test hooks lock poisoned");
		hooks.sever_response_for.insert(table_object_id);
		HookGuard { table_object_id }
	}

	pub(super) fn take_sever_response(table_object_id: TableObjectId) -> bool {
		let mut hooks = hooks().lock().expect("ADR 0093 test hooks lock poisoned");
		hooks.sever_response_for.remove(&table_object_id)
	}
}

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
/// ADR 0720: the cluster has admitted a write, so its membership is fixed.
pub const CONTROL_REASON_MEMBERSHIP_SEALED: u8 = 11;
/// ADR 0726: the cluster has not established its root check yet. Retryable.
pub const CONTROL_REASON_FORMATION_INCOMPLETE: u8 = 12;

/// ADR 0720: the typed cause of a refusal made because the membership is sealed,
/// so a handler maps it to its own reason code without reading message text.
#[derive(Debug)]
pub(crate) struct MembershipSealed(String);

impl std::fmt::Display for MembershipSealed {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "MembershipSealed: {}", self.0)
	}
}

impl std::error::Error for MembershipSealed {}

fn membership_sealed(detail: impl Into<String>) -> Error {
	Error::new(ErrorKind::PermissionDenied, MembershipSealed(detail.into()))
}

fn is_membership_sealed(error: &Error) -> bool {
	error
		.get_ref()
		.is_some_and(|cause| cause.is::<MembershipSealed>())
}

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
	SchemaDrop,
	IndexStatus,
	MaintenanceReport,
	MaintenanceRequeue,
	DecisionPropose,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TableDescriptorGetRequest {
	tenant_id: TenantId,
	namespace: String,
	table_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TableOptimizerFactsGetRequest {
	tenant_id: TenantId,
	namespace: String,
	table_name: String,
	schema_generation: u64,
	physical_descriptor_fingerprint: [u8; 32],
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
pub enum CtasSagaRequestWire {
	Mutate(CtasSagaMutation),
	Resolve {
		tenant_id: TenantId,
		operation_id: CtasOperationId,
	},
}

impl CtasSagaRequestWire {
	pub const fn tenant_id(&self) -> TenantId {
		match self {
			Self::Mutate(mutation) => mutation.tenant_id(),
			Self::Resolve { tenant_id, .. } => *tenant_id,
		}
	}

	pub const fn operation_id(&self) -> CtasOperationId {
		match self {
			Self::Mutate(mutation) => mutation.operation_id(),
			Self::Resolve { operation_id, .. } => *operation_id,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasSagaResponseWire {
	pub response: CtasSagaResponse,
	pub leader_hint: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexStatusResponseWire {
	pub accepted: bool,
	pub reason_code: u8,
	pub leader_hint: Option<u32>,
	#[serde(default)]
	pub indexes: Vec<SecondaryIndexStatus>,
	#[serde(default)]
	pub message: Option<String>,
}

pub const MAINTENANCE_CONTROL_WIRE_VERSION: u8 = 1;
pub const MAINTENANCE_CONTROL_MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAINTENANCE_CONTROL_DIAGNOSTIC_MAX_BYTES: usize = 512;

fn bounded_maintenance_diagnostic(message: impl Into<String>) -> String {
	let mut message = message.into();
	if message.len() <= MAINTENANCE_CONTROL_DIAGNOSTIC_MAX_BYTES {
		return message;
	}
	let mut end = MAINTENANCE_CONTROL_DIAGNOSTIC_MAX_BYTES;
	while !message.is_char_boundary(end) {
		end -= 1;
	}
	message.truncate(end);
	message
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceReportRequestWire {
	pub version: u8,
	pub target_node_id: u32,
	pub max_items: u16,
	#[serde(default)]
	pub cursor: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceOriginWire {
	pub origin_code: u16,
	pub reason: String,
	pub attempts: u32,
	pub last_failure_ms: u64,
	pub next_eligible_at_ms: Option<u64>,
	pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceReportItemWire {
	QuarantinedWork {
		operation_code: u16,
		slabs: Vec<u64>,
		origins: Vec<MaintenanceOriginWire>,
		quarantine_fingerprint: Vec<u8>,
	},
	RetryableCooldown {
		operation_code: u16,
		slabs: Vec<u64>,
		consecutive_failures: u32,
		next_eligible_at_ms: u64,
	},
	InFlight {
		operation_code: u16,
		slabs: Vec<u64>,
	},
	/// ADR 0098 decision 3: deterministic receipts inside their backoff budget.
	/// Every origin's `next_eligible_at_ms` is set; there is no fingerprint
	/// because there is nothing to requeue.
	BackingOff {
		operation_code: u16,
		slabs: Vec<u64>,
		origins: Vec<MaintenanceOriginWire>,
	},
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceReportOutcomeWire {
	Page,
	PolicyDisabled { diagnostic: String },
	StaleCursor,
	InvalidRequest { diagnostic: String },
	Unavailable { diagnostic: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceReportResponseWire {
	pub version: u8,
	pub serving_node_id: u32,
	pub outcome: MaintenanceReportOutcomeWire,
	pub incarnation: [u8; 16],
	pub generation: u64,
	#[serde(default)]
	pub items: Vec<MaintenanceReportItemWire>,
	#[serde(default)]
	pub next_cursor: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceRequeueRequestWire {
	pub version: u8,
	pub target_node_id: u32,
	pub operation_code: u16,
	pub slabs: Vec<u64>,
	pub expected_fingerprint: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceRequeueOutcomeWire {
	Cleared { origins: u16 },
	PolicyDisabled { diagnostic: String },
	InFlight,
	NotQuarantined,
	StaleReport { quarantined_origins: u16 },
	AuditUnavailable,
	StoreFailure { maintenance_disabled: bool },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceCompletionAuditWire {
	NotAttempted,
	Recorded,
	Failed { diagnostic: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceRequeueResponseWire {
	pub version: u8,
	pub serving_node_id: u32,
	pub operation_code: u16,
	pub slab_count: u32,
	pub slab_digest: [u8; 32],
	pub expected_fingerprint: Vec<u8>,
	pub outcome: MaintenanceRequeueOutcomeWire,
	pub completion_audit: MaintenanceCompletionAuditWire,
}

fn maintenance_origins_wire(
	origins: Vec<crate::storage::maintenance_policy::MaintenanceOriginView>,
) -> Vec<MaintenanceOriginWire> {
	origins
		.into_iter()
		.map(|origin| MaintenanceOriginWire {
			origin_code: origin.origin.code(),
			reason: origin.reason.label().to_owned(),
			attempts: origin.attempts,
			last_failure_ms: origin.last_failure_ms,
			next_eligible_at_ms: origin.next_eligible_at_ms,
			detail: origin.detail,
		})
		.collect()
}

fn maintenance_item_wire(item: MaintenanceReportItem) -> MaintenanceReportItemWire {
	match item {
		MaintenanceReportItem::QuarantinedWork {
			operation,
			slabs,
			origins,
			quarantine_fingerprint,
		} => MaintenanceReportItemWire::QuarantinedWork {
			operation_code: operation.code(),
			slabs: slabs.into_iter().map(|slab| slab.get()).collect(),
			origins: maintenance_origins_wire(origins),
			quarantine_fingerprint: quarantine_fingerprint.to_bytes().to_vec(),
		},
		MaintenanceReportItem::BackingOff {
			operation,
			slabs,
			origins,
		} => MaintenanceReportItemWire::BackingOff {
			operation_code: operation.code(),
			slabs: slabs.into_iter().map(|slab| slab.get()).collect(),
			origins: maintenance_origins_wire(origins),
		},
		MaintenanceReportItem::RetryableCooldown {
			operation,
			slabs,
			consecutive_failures,
			next_eligible_at_ms,
		} => MaintenanceReportItemWire::RetryableCooldown {
			operation_code: operation.code(),
			slabs: slabs.into_iter().map(|slab| slab.get()).collect(),
			consecutive_failures,
			next_eligible_at_ms,
		},
		MaintenanceReportItem::InFlight { operation, slabs } => {
			MaintenanceReportItemWire::InFlight {
				operation_code: operation.code(),
				slabs: slabs.into_iter().map(|slab| slab.get()).collect(),
			}
		}
	}
}

fn maintenance_slab_digest(slabs: &[u64]) -> [u8; 32] {
	let mut canonical = Vec::with_capacity(4 + slabs.len() * 8);
	canonical.extend_from_slice(&(slabs.len() as u32).to_be_bytes());
	for slab in slabs {
		canonical.extend_from_slice(&slab.to_be_bytes());
	}
	crate::hash::sha256::digest(&canonical)
}

fn maintenance_requeue_outcome_label(outcome: &MaintenanceRequeueOutcomeWire) -> &'static str {
	match outcome {
		MaintenanceRequeueOutcomeWire::Cleared { .. } => "cleared",
		MaintenanceRequeueOutcomeWire::PolicyDisabled { .. } => "policy_disabled",
		MaintenanceRequeueOutcomeWire::InFlight => "in_flight",
		MaintenanceRequeueOutcomeWire::NotQuarantined => "not_quarantined",
		MaintenanceRequeueOutcomeWire::StaleReport { .. } => "stale_report",
		MaintenanceRequeueOutcomeWire::AuditUnavailable => "audit_unavailable",
		MaintenanceRequeueOutcomeWire::StoreFailure { .. } => "store_failure",
	}
}

fn maintenance_requeue_audit_event(
	audit_operation: AuditOperation,
	node: NodeId,
	actor: &ActorIdentity,
	operation: MaintenanceOperation,
	slabs: &[u64],
	slab_digest: [u8; 32],
	expected_fingerprint: &[u8],
	result: &str,
	outcome: AuditOutcome,
) -> AuditEvent {
	AuditEvent::new(
		AuditAuthority::NodeLocal,
		audit_operation,
		node,
		format!("maintenance requeue {result}"),
	)
	.with_actor(actor)
	.with_outcome(outcome)
	.with_parameter("maintenance_operation", operation.label())
	.with_parameter("slab_count", slabs.len() as u64)
	.with_parameter("slab_digest", crate::audit::hex_string(&slab_digest))
	.with_parameter(
		"expected_fingerprint",
		crate::audit::hex_string(expected_fingerprint),
	)
	.with_parameter("result", result)
	.with_schema(AuditSchema::AdministrativeControl {
		action: AdministrativeControlAction::MaintenanceRequeue,
		detail: AdministrativeControlSchema {
			target_node_id: Some(node.format_wire()),
			..AdministrativeControlSchema::default()
		},
	})
}

async fn execute_maintenance_requeue_control(
	audit_sink: &dyn AuditSink,
	policy: Option<&Arc<MaintenancePolicyInner>>,
	node: NodeId,
	actor: &ActorIdentity,
	operation: MaintenanceOperation,
	slabs_wire: &[u64],
	slabs: &[crate::schema::SlabId],
	slab_digest: [u8; 32],
	expected_wire: &[u8],
	expected: MaintenanceFingerprint,
) -> (
	MaintenanceRequeueOutcomeWire,
	MaintenanceCompletionAuditWire,
) {
	let attempt = maintenance_requeue_audit_event(
		AuditOperation::MaintenanceRequeueAttempt,
		node,
		actor,
		operation,
		slabs_wire,
		slab_digest,
		expected_wire,
		"attempted",
		AuditOutcome::Allowed,
	);
	if let Err(error) = audit_sink.emit(attempt) {
		tracing::error!(error = %error, "maintenance requeue attempt audit failed");
		return (
			MaintenanceRequeueOutcomeWire::AuditUnavailable,
			MaintenanceCompletionAuditWire::NotAttempted,
		);
	}

	let outcome = match policy {
		Some(policy) => match policy.requeue(operation, slabs, expected).await {
			Ok(origins) => MaintenanceRequeueOutcomeWire::Cleared {
				origins: u16::try_from(origins).unwrap_or(u16::MAX),
			},
			Err(RequeueRefusal::PolicyDisabled(diagnostic)) => {
				MaintenanceRequeueOutcomeWire::PolicyDisabled { diagnostic }
			}
			Err(RequeueRefusal::InFlight) => MaintenanceRequeueOutcomeWire::InFlight,
			Err(RequeueRefusal::NotQuarantined) => MaintenanceRequeueOutcomeWire::NotQuarantined,
			Err(RequeueRefusal::StaleReport {
				quarantined_origins,
			}) => MaintenanceRequeueOutcomeWire::StaleReport {
				quarantined_origins: u16::try_from(quarantined_origins).unwrap_or(u16::MAX),
			},
			Err(RequeueRefusal::StoreFailure(_)) => MaintenanceRequeueOutcomeWire::StoreFailure {
				maintenance_disabled: true,
			},
		},
		None => MaintenanceRequeueOutcomeWire::PolicyDisabled {
			diagnostic: "maintenance requeue requires local storage".to_owned(),
		},
	};
	let audit_outcome = if matches!(outcome, MaintenanceRequeueOutcomeWire::Cleared { .. }) {
		AuditOutcome::Allowed
	} else {
		AuditOutcome::Error
	};
	let completion = maintenance_requeue_audit_event(
		AuditOperation::MaintenanceRequeueCompletion,
		node,
		actor,
		operation,
		slabs_wire,
		slab_digest,
		expected_wire,
		maintenance_requeue_outcome_label(&outcome),
		audit_outcome,
	);
	let completion_audit = match audit_sink.emit(completion) {
		Ok(()) => MaintenanceCompletionAuditWire::Recorded,
		Err(error) => {
			let diagnostic = bounded_maintenance_diagnostic(format!(
				"maintenance requeue completion audit failed: {error}"
			));
			tracing::error!(diagnostic = %diagnostic);
			MaintenanceCompletionAuditWire::Failed { diagnostic }
		}
	};
	(outcome, completion_audit)
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

#[cfg(feature = "test-support")]
pub mod join_test_support;

pub struct WendelonControlServer {
	/// A joiner on another decision authority is refused before it is a learner.
	decision_mode: crate::consensus::decision_mode::DecisionMode,
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
	/// ADR 0699 S3a: the leader side of decision proposals; ParentDecisions only.
	decision_proposals: Option<Arc<crate::network::decision_proposals::DecisionProposals>>,
	#[cfg(feature = "test-support")]
	join_promotion_gate: Option<join_test_support::JoinPromotionGate>,
}

fn control_operation_permission(operation: ControlOperation) -> ActorPermission {
	match operation {
		ControlOperation::Status => ActorPermission::ControlStatus,
		ControlOperation::AuditChainHead => ActorPermission::AuditRead,
		ControlOperation::RevocationAdmin => ActorPermission::ClusterControl,
		ControlOperation::Lease
		| ControlOperation::LeaseRelease
		| ControlOperation::Transfer
		| ControlOperation::AdvanceEpoch
		| ControlOperation::Join => ActorPermission::ClusterControl,
		ControlOperation::SchemaDrop => ActorPermission::TenantAdminMutation,
		ControlOperation::IndexStatus => ActorPermission::TenantAdminRead,
		ControlOperation::MaintenanceReport => ActorPermission::ControlStatus,
		ControlOperation::MaintenanceRequeue => ActorPermission::ClusterControl,
		ControlOperation::DecisionPropose => ActorPermission::ClusterControl,
	}
}

impl WendelonControlServer {
	/// Fixture teardown only; observers retain a Weak control server until bootstrap ends.
	#[cfg(test)]
	pub(crate) async fn shutdown_startup_fixture_raft(&self) -> std::io::Result<()> {
		self.raft.shutdown().await.map_err(std::io::Error::other)
	}

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
			decision_mode: Default::default(),
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
			decision_proposals: None,
			#[cfg(feature = "test-support")]
			join_promotion_gate: None,
		}
	}

	pub(crate) fn with_decision_proposals(
		mut self,
		proposals: Option<Arc<crate::network::decision_proposals::DecisionProposals>>,
	) -> Self {
		self.decision_proposals = proposals;
		self
	}

	pub(crate) fn with_decision_mode(
		mut self,
		decision_mode: crate::consensus::decision_mode::DecisionMode,
	) -> Self {
		self.decision_mode = decision_mode;
		self
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

	#[cfg(feature = "test-support")]
	pub fn with_join_promotion_gate(
		mut self,
		gate: Option<join_test_support::JoinPromotionGate>,
	) -> Self {
		self.join_promotion_gate = gate;
		self
	}

	#[cfg(feature = "test-support")]
	async fn record_join_observation(&self, req: &JoinClusterRequestWire, stage: &str) {
		if let Some(gate) = &self.join_promotion_gate {
			let snapshot = self.state_machine.state_snapshot().await;
			gate.record(format!("receiver={} request={req:?} stage={stage} leader={:?} membership={:?} epoch_history={:?} visible_epoch={:?}", self.local_node.id, self.raft.current_leader().await, snapshot.last_membership, snapshot.epoch_history, self.epoch_rx.borrow().clone()));
		}
	}

	pub fn with_protocol_window(mut self, protocol: ProtocolVersionWindow) -> Self {
		self.protocol = protocol;
		self
	}

	pub async fn submit_secondary_index_lifecycle(
		&self,
		envelope: SchemaUpdateEnvelope,
	) -> std::io::Result<()> {
		if envelope.object.kind != crate::integration::compute::SchemaObjectKind::Index
			|| !matches!(
				envelope.mutation,
				SchemaMutationKind::IndexBackfillComplete | SchemaMutationKind::IndexDropGcComplete
			) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"internal lifecycle submission requires an index lifecycle mutation",
			));
		}
		let payload = encode_schema_update_envelope_v1(&envelope)?;
		if self.is_leader().await {
			let response = self
				.raft
				.client_write(WendelonRequest::SchemaUpdate(payload))
				.await
				.map_err(|error| Error::new(ErrorKind::Other, error.to_string()))?;
			return match response.data {
				WendelonResponse::Success => Ok(()),
				WendelonResponse::Error(message) => Err(Error::new(ErrorKind::Other, message)),
				WendelonResponse::Refused(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::Op(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::LeaseResults(results) => Err(Error::other(format!(
					"unexpected lease results for a non-lease command: {results:?}"
				))),
				WendelonResponse::Decision(result) => Err(Error::other(format!(
					"unexpected decision result for a non-decision command: {result:?}"
				))),
				WendelonResponse::Root2Dormant => {
					let message = String::from("Root2 carrier is dormant, not applied");
					Err(Error::new(ErrorKind::Other, message))
				}
				WendelonResponse::LeaseDenied => Err(Error::other(
					"unexpected lease denial for secondary-index lifecycle mutation",
				)),
			};
		}
		let leader = self
			.leader_hint()
			.await
			.map(|id| NodeId { id, _reserved: 0 })
			.ok_or_else(|| Error::new(ErrorKind::NotConnected, "index lifecycle has no leader"))?;
		let conn = self.factory.get_connection(leader).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::SCHEMA_UPDATE_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: payload.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, &payload, None, None).await?;
		stream.finish().await?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		if frame.header.msg_type != MsgType::SCHEMA_UPDATE_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"index lifecycle target returned the wrong response type",
			));
		}
		let response: SchemaUpdateResponseWire = wire::decode(&frame.meta)?;
		if response.accepted {
			Ok(())
		} else {
			Err(Error::new(
				ErrorKind::Other,
				response.message.unwrap_or_else(|| {
					format!("index lifecycle rejected: {}", response.reason_code)
				}),
			))
		}
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
		let permission = control_operation_permission(op);
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

	fn build_ctas_saga_frame(
		&self,
		stream_id: u32,
		response: &CtasSagaResponseWire,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, response)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::CTAS_SAGA_RESP.0,
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

	async fn handle_ctas_saga(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"CTAS_SAGA_REQ must not carry Arrow or trailing payloads",
			));
		}
		let request: CtasSagaRequestWire = wire::decode(&frame.meta)?;
		// CTAS is driven by Toron's authenticated node-to-node catalog adapter in
		// production, while administrative clients use the tenant mutation role.
		// Requiring only the latter silently closed the simulated (and node-cert)
		// transport with no response, which surfaced to callers as `early eof`.
		if context.actor.role == ActorRole::DataNode {
			authorize_actor(&context.actor, ActorPermission::InterNodeTransport)?;
		} else {
			authorize_actor(&context.actor, ActorPermission::TenantAdminMutation)?;
		}
		if let Some(claimed_tenant) = context.actor.claimed_tenant_id()?
			&& claimed_tenant != request.tenant_id()
		{
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"actor tenant {} cannot access CTAS saga for tenant {}",
					claimed_tenant.get(),
					request.tenant_id().get()
				),
			));
		}
		if !self.is_leader().await {
			return self.build_ctas_saga_frame(
				frame.header.stream_id,
				&CtasSagaResponseWire {
					response: CtasSagaResponse::Rejected(CtasSagaReject::StaleOwner),
					leader_hint: self.leader_hint().await,
				},
			);
		}

		let tenant_id = request.tenant_id();
		let operation_id = request.operation_id();
		let response = match request {
			CtasSagaRequestWire::Resolve { .. } => self
				.state_machine
				.state_snapshot()
				.await
				.ctas_sagas
				.get(tenant_id, operation_id)
				.cloned()
				.map_or(
					CtasSagaResponse::Rejected(CtasSagaReject::NotFound),
					CtasSagaResponse::Applied,
				),
			CtasSagaRequestWire::Mutate(mutation) => match self
				.raft
				.client_write(WendelonRequest::CtasSaga(mutation))
				.await
			{
				Ok(response) => match response.data {
					WendelonResponse::Success => self
						.state_machine
						.state_snapshot()
						.await
						.ctas_sagas
						.get(tenant_id, operation_id)
						.cloned()
						.map_or(
							CtasSagaResponse::Rejected(CtasSagaReject::NotFound),
							CtasSagaResponse::Applied,
						),
					WendelonResponse::Error(message) => message
						.strip_prefix(CTAS_SAGA_REJECT_PREFIX)
						.and_then(CtasSagaReject::from_code)
						.map_or(
							CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition),
							CtasSagaResponse::Rejected,
						),
					WendelonResponse::Refused(_) => {
						CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition)
					}
					WendelonResponse::Op(_) => {
						CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition)
					}
					WendelonResponse::Decision(_) | WendelonResponse::LeaseResults(_) => {
						CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition)
					}
					WendelonResponse::Root2Dormant => {
						let message = String::from("Root2 carrier is dormant, not applied");
						message
							.strip_prefix(CTAS_SAGA_REJECT_PREFIX)
							.and_then(CtasSagaReject::from_code)
							.map_or(
								CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition),
								CtasSagaResponse::Rejected,
							)
					}
					WendelonResponse::LeaseDenied => {
						CtasSagaResponse::Rejected(CtasSagaReject::StaleOwner)
					}
				},
				Err(error) => {
					return Err(Error::other(format!("submit CTAS saga mutation: {error}")));
				}
			},
		};
		self.build_ctas_saga_frame(
			frame.header.stream_id,
			&CtasSagaResponseWire {
				response,
				leader_hint: Some(self.local_node.id),
			},
		)
	}

	fn build_secondary_index_status_frame(
		&self,
		stream_id: u32,
		response: &SecondaryIndexStatusResponseWire,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, response)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::SECONDARY_INDEX_STATUS_RESP.0,
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

	fn build_maintenance_frame<T: Serialize>(
		&self,
		stream_id: u32,
		msg_type: MsgType,
		response: &T,
	) -> std::io::Result<HrftFrame> {
		let meta = wire::encode_into(&self.pool, response)?;
		if meta.len() > MAINTENANCE_REPORT_MAX_BYTES {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"maintenance control response exceeded its encoded byte budget",
			));
		}
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: msg_type.0,
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

	fn build_table_descriptor_list_frame(
		&self,
		stream_id: u32,
		meta: TableDescriptorListResponseMeta,
		descriptors: Option<&[u8]>,
	) -> std::io::Result<HrftFrame> {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let trailing = descriptors.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(bytes);
			buf
		});
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_DESCRIPTOR_LIST_RESP.0,
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

	fn build_table_optimizer_facts_get_frame(
		&self,
		stream_id: u32,
		meta: TableOptimizerFactsGetResponseMeta,
		facts: Option<&[u8]>,
	) -> std::io::Result<HrftFrame> {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let trailing = facts.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(bytes);
			buf
		});
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_OPTIMIZER_FACTS_GET_RESP.0,
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

	fn table_descriptor_list_rejection(
		&self,
		frame: &HrftFrame,
		reason_code: u8,
		leader_hint: Option<NodeId>,
	) -> std::io::Result<HrftFrame> {
		self.build_table_descriptor_list_frame(
			frame.header.stream_id,
			TableDescriptorListResponseMeta {
				accepted: 0,
				reason_code,
				_pad1: [0; 6],
				leader_hint: leader_hint.unwrap_or(NodeId {
					id: 0,
					_reserved: 0,
				}),
				descriptors_len: 0,
				_pad2: [0; 4],
			},
			None,
		)
	}

	fn table_optimizer_facts_get_rejection(
		&self,
		frame: &HrftFrame,
		reason_code: u8,
		leader_hint: Option<NodeId>,
	) -> std::io::Result<HrftFrame> {
		Self::observe_table_optimizer_facts_get_reason(reason_code);
		self.build_table_optimizer_facts_get_frame(
			frame.header.stream_id,
			TableOptimizerFactsGetResponseMeta {
				accepted: 0,
				reason_code,
				_pad1: [0; 6],
				leader_hint: leader_hint.unwrap_or(NodeId {
					id: 0,
					_reserved: 0,
				}),
				facts_len: 0,
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

	fn observe_table_optimizer_facts_get_reason(reason_code: u8) {
		match reason_code {
			CONTROL_REASON_GRANTED => metrics().control_optimizer_facts_get_success_total.inc(),
			CONTROL_REASON_NOT_FOUND => metrics().control_optimizer_facts_get_not_found_total.inc(),
			CONTROL_REASON_NOT_LEADER => {
				metrics().control_optimizer_facts_get_not_leader_total.inc()
			}
			CONTROL_REASON_AUTHORIZATION_FAILED => metrics()
				.control_optimizer_facts_get_authorization_failed_total
				.inc(),
			CONTROL_REASON_INVALID_CONFIG => metrics()
				.control_optimizer_facts_get_invalid_request_total
				.inc(),
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
				under_root: self.crypto.verified_root_check_id()?,
			});
		}
		let rotation = self.crypto.prepare_rotation(tenant_id)?;
		let under_root = rotation.under_root;
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
			under_root,
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
				WendelonResponse::Refused(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::Op(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::LeaseResults(results) => Err(Error::other(format!(
					"unexpected lease results for a non-lease command: {results:?}"
				))),
				WendelonResponse::Decision(result) => Err(Error::other(format!(
					"unexpected decision result for a non-decision command: {result:?}"
				))),
				WendelonResponse::Root2Dormant => {
					let message = String::from("Root2 carrier is dormant, not applied");
					self.revocation_admin_rejection(
						frame,
						CONTROL_REASON_INVALID_CONFIG,
						message,
						None,
					)
				}
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
		let lifecycle_replica = if envelope.object.kind
			== crate::integration::compute::SchemaObjectKind::Index
			&& matches!(
				envelope.mutation,
				SchemaMutationKind::IndexBackfillComplete | SchemaMutationKind::IndexDropGcComplete
			) {
			Some(decode_secondary_index_lifecycle_complete(
				&envelope.payload,
			)?)
		} else {
			None
		};
		if let Some(complete) = lifecycle_replica {
			authorize_actor(&context.actor, ActorPermission::InterNodeTransport)?;
			if context.actor.role != ActorRole::DataNode
				|| context.actor.node_id != Some(complete.replica_id)
			{
				return self.schema_update_rejection(
					&frame,
					CONTROL_REASON_AUTHORIZATION_FAILED,
					"secondary-index lifecycle completion must identify the authenticated data node",
					None,
				);
			}
		} else {
			authorize_actor(&context.actor, ActorPermission::TenantAdminMutation)?;
		}
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
				SchemaMutationKind::Create | SchemaMutationKind::Alter
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
				WendelonResponse::Refused(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::Op(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::LeaseResults(results) => Err(Error::other(format!(
					"unexpected lease results for a non-lease command: {results:?}"
				))),
				WendelonResponse::Decision(result) => Err(Error::other(format!(
					"unexpected decision result for a non-decision command: {result:?}"
				))),
				WendelonResponse::Root2Dormant => {
					let message = String::from("Root2 carrier is dormant, not applied");
					self.schema_update_rejection(
						&frame,
						CONTROL_REASON_INVALID_CONFIG,
						message,
						None,
					)
				}
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

	async fn handle_secondary_index_status(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"SECONDARY_INDEX_STATUS_REQ must not carry Arrow or trailing payloads",
			));
		}
		let request: SecondaryIndexStatusRequest = wire::decode(&frame.meta)?;
		if let Some(claimed_tenant) = context.actor.claimed_tenant_id()? {
			if claimed_tenant != request.tenant_id {
				return self.build_secondary_index_status_frame(
					frame.header.stream_id,
					&SecondaryIndexStatusResponseWire {
						accepted: false,
						reason_code: CONTROL_REASON_AUTHORIZATION_FAILED,
						leader_hint: self.leader_hint().await,
						indexes: Vec::new(),
						message: Some("actor tenant does not match index-status tenant".to_owned()),
					},
				);
			}
		}
		let Some(storage) = self.storage_manager.as_ref() else {
			return self.build_secondary_index_status_frame(
				frame.header.stream_id,
				&SecondaryIndexStatusResponseWire {
					accepted: false,
					reason_code: CONTROL_REASON_INTERNAL_ERROR,
					leader_hint: self.leader_hint().await,
					indexes: Vec::new(),
					message: Some("secondary-index status requires local storage".to_owned()),
				},
			);
		};
		let snapshot = self.state_machine.state_snapshot().await;
		let mut indexes = Vec::new();
		for row in snapshot.secondary_indexes.for_tenant(request.tenant_id) {
			let gaps = storage
				.secondary_index_durable_coverage_gaps(request.tenant_id, &row.descriptor)
				.await;
			indexes.push(SecondaryIndexStatus {
				descriptor: row.descriptor.clone(),
				state: row.state,
				backfill_completed_by: row.backfill_completed_by(),
				replicas: vec![SecondaryIndexReplicaStatus {
					replica_id: self.local_node,
					initial_backfill_complete: row.has_backfill_completion(self.local_node),
					coverage_gap_slabs: gaps,
				}],
			});
		}
		indexes.sort_by_key(|status| status.descriptor.index_object_id);
		self.build_secondary_index_status_frame(
			frame.header.stream_id,
			&SecondaryIndexStatusResponseWire {
				accepted: true,
				reason_code: CONTROL_REASON_GRANTED,
				leader_hint: self.leader_hint().await,
				indexes,
				message: None,
			},
		)
	}

	async fn handle_maintenance_report(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"MAINTENANCE_REPORT_REQ must not carry Arrow or trailing payloads",
			));
		}
		if frame.meta.len() > MAINTENANCE_CONTROL_MAX_REQUEST_BYTES {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"maintenance report request exceeds the wire byte bound",
			));
		}
		let request: MaintenanceReportRequestWire = wire::decode(&frame.meta)?;
		let invalid = if request.version != MAINTENANCE_CONTROL_WIRE_VERSION {
			Some("maintenance control wire version is unsupported".to_owned())
		} else if request.target_node_id != self.local_node.id {
			Some(format!(
				"maintenance report targeted node {} but reached node {}",
				request.target_node_id, self.local_node.id
			))
		} else if request.max_items == 0
			|| usize::from(request.max_items) > MAINTENANCE_REPORT_MAX_ITEMS
		{
			Some(format!(
				"maintenance report max_items must be 1..={MAINTENANCE_REPORT_MAX_ITEMS}"
			))
		} else {
			None
		};
		if let Some(diagnostic) = invalid {
			return self.build_maintenance_frame(
				frame.header.stream_id,
				MsgType::MAINTENANCE_REPORT_RESP,
				&MaintenanceReportResponseWire {
					version: MAINTENANCE_CONTROL_WIRE_VERSION,
					serving_node_id: self.local_node.id,
					outcome: MaintenanceReportOutcomeWire::InvalidRequest { diagnostic },
					incarnation: [0; 16],
					generation: 0,
					items: Vec::new(),
					next_cursor: Vec::new(),
				},
			);
		}
		let Some(storage) = self.storage_manager.as_ref() else {
			return self.build_maintenance_frame(
				frame.header.stream_id,
				MsgType::MAINTENANCE_REPORT_RESP,
				&MaintenanceReportResponseWire {
					version: MAINTENANCE_CONTROL_WIRE_VERSION,
					serving_node_id: self.local_node.id,
					outcome: MaintenanceReportOutcomeWire::Unavailable {
						diagnostic: "maintenance report requires local storage".to_owned(),
					},
					incarnation: [0; 16],
					generation: 0,
					items: Vec::new(),
					next_cursor: Vec::new(),
				},
			);
		};
		let cursor = (!request.cursor.is_empty()).then_some(request.cursor.as_slice());
		let page = storage.maintenance_policy().report_page(
			cursor,
			usize::from(request.max_items),
			MAINTENANCE_REPORT_MAX_BYTES,
		);
		let response = match page {
			Ok(page) => MaintenanceReportResponseWire {
				version: MAINTENANCE_CONTROL_WIRE_VERSION,
				serving_node_id: self.local_node.id,
				outcome: if page.enabled {
					MaintenanceReportOutcomeWire::Page
				} else {
					MaintenanceReportOutcomeWire::PolicyDisabled {
						diagnostic: page.diagnostic.unwrap_or_default(),
					}
				},
				incarnation: page.incarnation,
				generation: page.generation,
				items: page.items.into_iter().map(maintenance_item_wire).collect(),
				next_cursor: page.next_cursor.unwrap_or_default(),
			},
			Err(MaintenanceReportPageError::StaleCursor) => MaintenanceReportResponseWire {
				version: MAINTENANCE_CONTROL_WIRE_VERSION,
				serving_node_id: self.local_node.id,
				outcome: MaintenanceReportOutcomeWire::StaleCursor,
				incarnation: [0; 16],
				generation: 0,
				items: Vec::new(),
				next_cursor: Vec::new(),
			},
			Err(MaintenanceReportPageError::InvalidCursor(diagnostic))
			| Err(MaintenanceReportPageError::InvalidBound(diagnostic)) => MaintenanceReportResponseWire {
				version: MAINTENANCE_CONTROL_WIRE_VERSION,
				serving_node_id: self.local_node.id,
				outcome: MaintenanceReportOutcomeWire::InvalidRequest { diagnostic },
				incarnation: [0; 16],
				generation: 0,
				items: Vec::new(),
				next_cursor: Vec::new(),
			},
		};
		self.build_maintenance_frame(
			frame.header.stream_id,
			MsgType::MAINTENANCE_REPORT_RESP,
			&response,
		)
	}

	async fn handle_maintenance_requeue(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"MAINTENANCE_REQUEUE_REQ must not carry Arrow or trailing payloads",
			));
		}
		if frame.meta.len() > MAINTENANCE_CONTROL_MAX_REQUEST_BYTES {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"maintenance requeue request exceeds the wire byte bound",
			));
		}
		let request: MaintenanceRequeueRequestWire = wire::decode(&frame.meta)?;
		if request.version != MAINTENANCE_CONTROL_WIRE_VERSION
			|| request.target_node_id != self.local_node.id
			|| request.slabs.is_empty()
			|| request.slabs.len() > crate::storage::maintenance_policy::MAX_RECEIPT_KEY_SLABS
			|| request.slabs.windows(2).any(|pair| pair[0] >= pair[1])
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"maintenance requeue request has invalid version, target, or canonical slab set",
			));
		}
		let operation =
			MaintenanceOperation::from_code(request.operation_code).ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"maintenance operation code is unknown",
				)
			})?;
		let expected = MaintenanceFingerprint::from_bytes(&request.expected_fingerprint)
			.map_err(|message| Error::new(ErrorKind::InvalidInput, message))?;
		let slabs = request
			.slabs
			.iter()
			.copied()
			.map(crate::schema::SlabId::new)
			.collect::<Vec<_>>();
		let slab_digest = maintenance_slab_digest(&request.slabs);
		let response_base = |outcome, completion_audit| MaintenanceRequeueResponseWire {
			version: MAINTENANCE_CONTROL_WIRE_VERSION,
			serving_node_id: self.local_node.id,
			operation_code: request.operation_code,
			slab_count: request.slabs.len() as u32,
			slab_digest,
			expected_fingerprint: request.expected_fingerprint.clone(),
			outcome,
			completion_audit,
		};

		let policy = self
			.storage_manager
			.as_ref()
			.map(|storage| storage.maintenance_policy());
		let (outcome, completion_audit) = execute_maintenance_requeue_control(
			self.audit_sink.as_ref(),
			policy,
			self.local_node,
			&context.actor,
			operation,
			&request.slabs,
			&slabs,
			slab_digest,
			&request.expected_fingerprint,
			expected,
		)
		.await;
		self.build_maintenance_frame(
			frame.header.stream_id,
			MsgType::MAINTENANCE_REQUEUE_RESP,
			&response_base(outcome, completion_audit),
		)
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
		#[cfg(any(test, feature = "test-support"))]
		let table_object_id = match &request {
			TableSchemaDropRequest::V1 {
				table_object_id, ..
			} => *table_object_id,
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
			Ok(()) => {
				#[cfg(any(test, feature = "test-support"))]
				if adr0093_test_support::take_sever_response(table_object_id) {
					return Err(Error::new(
						ErrorKind::ConnectionAborted,
						"ADR 0093 test seam severed DROP response after durable finalize",
					));
				}
				self.build_schema_drop_frame(
					frame.header.stream_id,
					&TableSchemaDropResponseWire {
						accepted: true,
						reason_code: CONTROL_REASON_GRANTED,
						leader_hint: Some(self.local_node.id),
						admin_error: None,
						message: None,
					},
				)
			}
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

	fn decode_table_optimizer_facts_get_request(
		meta: TableOptimizerFactsGetRequestMeta,
	) -> std::io::Result<TableOptimizerFactsGetRequest> {
		Ok(TableOptimizerFactsGetRequest {
			tenant_id: meta.tenant_id,
			namespace: decode_descriptor_wire_name("namespace", &meta.namespace)?,
			table_name: decode_descriptor_wire_name("table name", &meta.table_name)?,
			schema_generation: meta.schema_generation,
			physical_descriptor_fingerprint: meta.physical_descriptor_fingerprint,
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
		// Descriptor and logical-type synchronization is an authenticated
		// node-to-node read for Toron, while external readers use the tenant-admin
		// surface. A node certificate intentionally carries no tenant claim.
		if actor.role == ActorRole::DataNode {
			authorize_actor(actor, ActorPermission::InterNodeTransport)?;
		} else {
			authorize_actor(actor, ActorPermission::TenantAdminRead)?;
		}
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

	async fn handle_table_descriptor_list(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return self.table_descriptor_list_rejection(
				&frame,
				CONTROL_REASON_INVALID_CONFIG,
				None,
			);
		}

		let raw_meta: TableDescriptorListRequestMeta =
			if frame.meta.len() == std::mem::size_of::<TableDescriptorListRequestMeta>() {
				*bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<TableDescriptorListRequestMeta>()],
				)
			} else {
				return self.table_descriptor_list_rejection(
					&frame,
					CONTROL_REASON_INVALID_CONFIG,
					None,
				);
			};
		if Self::authorize_descriptor_get_actor(&context.actor, raw_meta.tenant_id).is_err() {
			return self.table_descriptor_list_rejection(
				&frame,
				CONTROL_REASON_AUTHORIZATION_FAILED,
				None,
			);
		}
		if !self.is_leader().await {
			return self.table_descriptor_list_rejection(
				&frame,
				CONTROL_REASON_NOT_LEADER,
				self.leader_hint()
					.await
					.map(|id| NodeId { id, _reserved: 0 }),
			);
		}

		let snapshot = self.state_machine.state_snapshot().await;
		let descriptors = snapshot
			.table_schemas
			.latest_descriptors_for_tenant(raw_meta.tenant_id);
		let descriptors_bytes = encode_table_schema_descriptors(&descriptors)?;
		let descriptors_len = u32::try_from(descriptors_bytes.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"encoded table descriptors exceed u32 wire length",
			)
		})?;
		self.build_table_descriptor_list_frame(
			frame.header.stream_id,
			TableDescriptorListResponseMeta {
				accepted: 1,
				reason_code: CONTROL_REASON_GRANTED,
				_pad1: [0; 6],
				leader_hint: self.local_node,
				descriptors_len,
				_pad2: [0; 4],
			},
			Some(&descriptors_bytes),
		)
	}

	async fn handle_table_optimizer_facts_get(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		metrics().control_optimizer_facts_get_requests_total.inc();
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return self.table_optimizer_facts_get_rejection(
				&frame,
				CONTROL_REASON_INVALID_CONFIG,
				None,
			);
		}

		let raw_meta: TableOptimizerFactsGetRequestMeta =
			if frame.meta.len() == std::mem::size_of::<TableOptimizerFactsGetRequestMeta>() {
				*bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<TableOptimizerFactsGetRequestMeta>()],
				)
			} else {
				return self.table_optimizer_facts_get_rejection(
					&frame,
					CONTROL_REASON_INVALID_CONFIG,
					None,
				);
			};
		if Self::authorize_descriptor_get_actor(&context.actor, raw_meta.tenant_id).is_err() {
			return self.table_optimizer_facts_get_rejection(
				&frame,
				CONTROL_REASON_AUTHORIZATION_FAILED,
				None,
			);
		}

		let request = match Self::decode_table_optimizer_facts_get_request(raw_meta) {
			Ok(request) => request,
			Err(_) => {
				return self.table_optimizer_facts_get_rejection(
					&frame,
					CONTROL_REASON_INVALID_CONFIG,
					None,
				);
			}
		};
		if !self.is_leader().await {
			return self.table_optimizer_facts_get_rejection(
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
			return self.table_optimizer_facts_get_rejection(
				&frame,
				CONTROL_REASON_NOT_FOUND,
				None,
			);
		};
		if descriptor.schema_generation != request.schema_generation
			|| descriptor.physical_descriptor_fingerprint.as_bytes()
				!= &request.physical_descriptor_fingerprint
		{
			Self::observe_table_optimizer_facts_get_reason(CONTROL_REASON_GRANTED);
			return self.build_table_optimizer_facts_get_frame(
				frame.header.stream_id,
				TableOptimizerFactsGetResponseMeta {
					accepted: 1,
					reason_code: CONTROL_REASON_GRANTED,
					_pad1: [0; 6],
					leader_hint: self.local_node,
					facts_len: 0,
					_pad2: [0; 4],
				},
				None,
			);
		}
		let Some(storage_manager) = self.storage_manager.as_ref() else {
			Self::observe_table_optimizer_facts_get_reason(CONTROL_REASON_GRANTED);
			return self.build_table_optimizer_facts_get_frame(
				frame.header.stream_id,
				TableOptimizerFactsGetResponseMeta {
					accepted: 1,
					reason_code: CONTROL_REASON_GRANTED,
					_pad1: [0; 6],
					leader_hint: self.local_node,
					facts_len: 0,
					_pad2: [0; 4],
				},
				None,
			);
		};
		let facts = storage_manager
			.table_optimizer_facts(request.tenant_id, descriptor)
			.await?;
		let facts_bytes = facts
			.as_ref()
			.map(encode_table_optimizer_facts)
			.transpose()?;
		let facts_len = facts_bytes
			.as_ref()
			.map(|bytes| u32::try_from(bytes.len()))
			.transpose()
			.map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"encoded optimizer facts exceed u32 wire length",
				)
			})?
			.unwrap_or(0);
		Self::observe_table_optimizer_facts_get_reason(CONTROL_REASON_GRANTED);
		self.build_table_optimizer_facts_get_frame(
			frame.header.stream_id,
			TableOptimizerFactsGetResponseMeta {
				accepted: 1,
				reason_code: CONTROL_REASON_GRANTED,
				_pad1: [0; 6],
				leader_hint: self.local_node,
				facts_len,
				_pad2: [0; 4],
			},
			facts_bytes.as_deref(),
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
		let definition_bytes = encode_logical_type_definition(definition)?;
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
		let definitions_bytes = encode_logical_type_definitions(&definitions)?;
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
		if let Some(outcome) = snapshot.table_drop_outcomes.get(tenant_id, table_object_id) {
			return if outcome.operation_id == operation_id {
				Ok(())
			} else {
				Err(WendelonAdminError::TableAlreadyDropped { table_object_id })
			};
		}
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
		let name_is_bound = snapshot.table_schemas.is_latest_name_bound(
			tenant_id,
			&descriptor.namespace,
			&descriptor.table_name,
			table_object_id,
			descriptor.schema_generation,
		);
		let is_ctas_retirement = snapshot.ctas_sagas.retiring_old_binding(
			tenant_id,
			table_object_id,
			expected_generation,
		);
		if !name_is_bound && !is_ctas_retirement {
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

		self.submit_table_drop_stage(WendelonRequest::TableDropLogicalFinalize(
			TableDropLogicalFinalizeRequest {
				operation_id,
				fence_log_index: fence.fence_log_index,
			},
		))
		.await?;
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
				WendelonResponse::Refused(refusal) => Err(WendelonAdminError::Transport {
					message: format!("unexpected slab refusal for a non-slab command: {refusal:?}"),
				}),
				WendelonResponse::Op(refusal) => Err(WendelonAdminError::Transport {
					message: format!("unexpected slab refusal for a non-slab command: {refusal:?}"),
				}),
				WendelonResponse::LeaseResults(results) => Err(WendelonAdminError::Transport {
					message: format!(
						"unexpected lease results for a non-lease command: {results:?}"
					),
				}),
				WendelonResponse::Decision(result) => Err(WendelonAdminError::Transport {
					message: format!(
						"unexpected decision result for a non-decision command: {result:?}"
					),
				}),
				WendelonResponse::Root2Dormant => {
					let message = String::from("Root2 carrier is dormant, not applied");
					Err(WendelonAdminError::Transport { message })
				}
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
				WendelonResponse::Refused(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::Op(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::LeaseResults(results) => Err(Error::other(format!(
					"unexpected lease results for a non-lease command: {results:?}"
				))),
				WendelonResponse::Decision(result) => Err(Error::other(format!(
					"unexpected decision result for a non-decision command: {result:?}"
				))),
				WendelonResponse::Root2Dormant => {
					let message = String::from("Root2 carrier is dormant, not applied");
					self.tenant_admin_rejection(
						frame,
						Self::tenant_admin_error_reason(&message),
						message,
						None,
					)
				}
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
				WendelonResponse::Refused(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::Op(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::LeaseResults(results) => Err(Error::other(format!(
					"unexpected lease results for a non-lease command: {results:?}"
				))),
				WendelonResponse::Decision(result) => Err(Error::other(format!(
					"unexpected decision result for a non-decision command: {result:?}"
				))),
				WendelonResponse::Root2Dormant => {
					let message = String::from("Root2 carrier is dormant, not applied");
					self.tenant_admin_rejection(
						frame,
						Self::tenant_admin_error_reason(&message),
						message,
						None,
					)
				}
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
				WendelonResponse::Refused(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::Op(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::LeaseResults(results) => Err(Error::other(format!(
					"unexpected lease results for a non-lease command: {results:?}"
				))),
				WendelonResponse::Decision(result) => Err(Error::other(format!(
					"unexpected decision result for a non-decision command: {result:?}"
				))),
				WendelonResponse::Root2Dormant => {
					let message = String::from("Root2 carrier is dormant, not applied");
					self.tenant_admin_rejection(
						frame,
						Self::tenant_admin_error_reason(&message),
						message,
						None,
					)
				}
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
				WendelonResponse::Refused(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::Op(refusal) => Err(Error::other(format!(
					"unexpected slab refusal for a non-slab command: {refusal:?}"
				))),
				WendelonResponse::LeaseResults(results) => Err(Error::other(format!(
					"unexpected lease results for a non-lease command: {results:?}"
				))),
				WendelonResponse::Decision(result) => Err(Error::other(format!(
					"unexpected decision result for a non-decision command: {result:?}"
				))),
				WendelonResponse::Root2Dormant => {
					let message = String::from("Root2 carrier is dormant, not applied");
					self.tenant_admin_rejection(
						frame,
						Self::tenant_admin_error_reason(&message),
						message,
						None,
					)
				}
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
				WendelonResponse::Refused(_) => LeaseStatus::INTERNAL_ERROR,
				WendelonResponse::Op(_) => LeaseStatus::INTERNAL_ERROR,
				WendelonResponse::Decision(_) | WendelonResponse::LeaseResults(_) => {
					LeaseStatus::INTERNAL_ERROR
				}
				WendelonResponse::Root2Dormant => LeaseStatus::INTERNAL_ERROR,
			},
			Err(_) if !self.is_leader().await => LeaseStatus::DENIED_NOT_LEADER,
			Err(_) => LeaseStatus::INTERNAL_ERROR,
		};
		self.lease_response(&frame, status, None)
	}

	/// ADR 0701: one holder's batched renewal, ParentDecisions only. Every range
	/// passes the single grant's checks before anything is signed; the leases
	/// are proposed as one `AcquireLeases` entry, and each gets its own result.
	async fn handle_leases(&self, remote: NodeId, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		use crate::schema::{
			AcquireLeasesRequestHeader, LeaseRangeWire, LeaseResultWire, decode_lease_batch,
		};
		if self.decision_mode != crate::consensus::decision_mode::DecisionMode::ParentDecisions {
			return Err(Error::new(
				ErrorKind::Unsupported,
				"LEASES_REQ is served only under ParentDecisions",
			));
		}
		let (header, ranges): (AcquireLeasesRequestHeader, Vec<LeaseRangeWire>) =
			decode_lease_batch(&frame.meta, |header: &AcquireLeasesRequestHeader| {
				header.count
			})?;
		if header.epoch != self.current_epoch() {
			return self.leases_response(&frame, LeaseStatus::DENIED_EPOCH_STALE, &[]);
		}
		if !self.is_leader().await {
			return self.leases_response(&frame, LeaseStatus::DENIED_NOT_LEADER, &[]);
		}
		let mut starts = std::collections::BTreeSet::new();
		for range in &ranges {
			let single = AcquireLeaseRequestMeta {
				range_start: range.range_start,
				range_end: range.range_end,
				epoch: header.epoch,
				holder: header.holder,
				requested_hlc: header.requested_hlc,
				ttl_ms: header.ttl_ms,
				_pad: 0,
			};
			if !starts.insert(range.range_start)
				|| self
					.validate_acquire_lease_target(remote, &single)
					.await
					.is_err()
			{
				return self.leases_response(&frame, LeaseStatus::DENIED_INVALID_TARGET, &[]);
			}
		}
		let ttl_ms = header.ttl_ms.clamp(1, self.lease_ttl_cap_ms);
		let leases: Vec<LeaseDescriptor> = ranges
			.iter()
			.map(|range| {
				self.lease_authority.sign_lease(LeaseDescriptor {
					epoch: header.epoch,
					holder: header.holder,
					range_start: range.range_start,
					range_end: range.range_end,
					lease_start: header.requested_hlc,
					lease_expiry: HybridTimestamp {
						physical_ms: header
							.requested_hlc
							.physical_ms
							.saturating_add(ttl_ms as u64),
						logical: header.requested_hlc.logical,
						node_id: header.requested_hlc.node_id,
					},
					min_commit_index: 0,
					signature: [0; 64],
					issuer: self.local_node,
				})
			})
			.collect();
		let batch = crate::consensus::LeaseBatch::new(leases.clone())
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "lease batch over its bound"))?;
		let status = match self
			.raft
			.client_write(WendelonRequest::AcquireLeases(batch))
			.await
		{
			Ok(resp) => match resp.data {
				WendelonResponse::LeaseResults(results)
					if results.results().len() == leases.len()
						&& results
							.results()
							.iter()
							.zip(&leases)
							.all(|(result, lease)| result.range_start == lease.range_start) =>
				{
					let entries: Vec<LeaseResultWire> = results
						.results()
						.iter()
						.zip(&leases)
						.map(|(result, lease)| LeaseResultWire {
							range_start: lease.range_start,
							status: if result.granted {
								LeaseStatus::GRANTED
							} else {
								LeaseStatus::DENIED_OVERLAP
							},
							_pad: [0; 7],
							lease: if result.granted {
								*lease
							} else {
								bytemuck::Zeroable::zeroed()
							},
						})
						.collect();
					return self.leases_response(&frame, LeaseStatus::GRANTED, &entries);
				}
				_ => LeaseStatus::INTERNAL_ERROR,
			},
			Err(_) if !self.is_leader().await => LeaseStatus::DENIED_NOT_LEADER,
			Err(_) => LeaseStatus::INTERNAL_ERROR,
		};
		self.leases_response(&frame, status, &[])
	}

	/// ADR 0701: a LEASES_RESP: the header, then one result per requested range
	/// when the batch was proposed (`status` GRANTED), else none.
	fn leases_response(
		&self,
		frame: &HrftFrame,
		status: LeaseStatus,
		results: &[crate::schema::LeaseResultWire],
	) -> std::io::Result<HrftFrame> {
		let header = crate::schema::AcquireLeasesResponseHeader {
			status,
			_pad: [0; 3],
			count: results.len() as u32,
		};
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&header));
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::cast_slice(results));
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::LEASES_RESP.0,
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
				WendelonResponse::Refused(_) => LeaseStatus::INTERNAL_ERROR,
				WendelonResponse::Op(_) => LeaseStatus::INTERNAL_ERROR,
				WendelonResponse::Decision(_) | WendelonResponse::LeaseResults(_) => {
					LeaseStatus::INTERNAL_ERROR
				}
				WendelonResponse::Root2Dormant => LeaseStatus::INTERNAL_ERROR,
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
					Err(_err) => {
						#[cfg(feature = "test-support")]
						self.record_join_observation(&req, &format!("validation rejected: {_err}"))
							.await;
						JoinClusterResponseWire {
							accepted: false,
							reason_code: CONTROL_REASON_INVALID_CONFIG,
							leader_hint: self.leader_hint().await,
							local_protocol: self.protocol,
							negotiated_protocol: None,
						}
					}
					// ADR 0726: no member joins before the cluster's root check
					// exists, so a join can never race formation. Retryable.
					Ok(())
						if self.is_leader().await
							&& self
								.state_machine
								.state_snapshot()
								.await
								.root_check
								.is_none() =>
					{
						#[cfg(feature = "test-support")]
						self.record_join_observation(&req, "refused: root check not established")
							.await;
						JoinClusterResponseWire {
							accepted: false,
							reason_code: CONTROL_REASON_FORMATION_INCOMPLETE,
							leader_hint: Some(self.local_node.id),
							local_protocol: self.protocol,
							negotiated_protocol: None,
						}
					}
					Ok(()) if self.is_leader().await => {
						match wire::negotiate_protocol(self.protocol, req.protocol) {
							Ok(negotiated_protocol) => match self.accept_join(&req).await {
								Ok(()) => {
									#[cfg(feature = "test-support")]
									if let Some(gate) = &self.join_promotion_gate {
										self.record_join_observation(
											&req,
											"epoch complete before success response",
										)
										.await;
										gate.pause(
											join_test_support::JoinGateStage::BeforeSuccess,
											&req,
										)
										.await?;
									}
									JoinClusterResponseWire {
										accepted: true,
										reason_code: CONTROL_REASON_GRANTED,
										leader_hint: Some(self.local_node.id),
										local_protocol: self.protocol,
										negotiated_protocol: Some(negotiated_protocol),
									}
								}
								Err(err) if err.kind() == ErrorKind::InvalidInput => {
									JoinClusterResponseWire {
										accepted: false,
										reason_code: CONTROL_REASON_INVALID_CONFIG,
										leader_hint: Some(self.local_node.id),
										local_protocol: self.protocol,
										negotiated_protocol: None,
									}
								}
								Err(err) if is_membership_sealed(&err) => JoinClusterResponseWire {
									accepted: false,
									reason_code: CONTROL_REASON_MEMBERSHIP_SEALED,
									leader_hint: Some(self.local_node.id),
									local_protocol: self.protocol,
									negotiated_protocol: None,
								},
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
			// ADR 0720: an advance the state machine refused did not happen.
			Ok(response) => {
				if let WendelonResponse::Error(refusal) = response.data {
					// The reducer's only refusal of an advance is the seal.
					return Err(membership_sealed(
						refusal.trim_start_matches("MembershipSealed: "),
					));
				}
			}
			Err(openraft::error::RaftError::APIError(
				openraft::error::ClientWriteError::ForwardToLeader(forward),
			)) => {
				return Err(not_leader_error(forward.leader_id));
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
		let visible = self.epoch_rx.borrow().clone();
		let Some((&latest_epoch, _)) = snapshot.epoch_history.iter().next_back() else {
			drop(snapshot);
			if !self.is_leader().await {
				return Ok(());
			}
			if visible.epoch == 0 {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"cannot seed durable membership history from epoch zero",
				));
			}
			// The runtime ring has always started from a genesis epoch, but older
			// profiles did not copy that first epoch into replicated history until
			// a later membership transition. ADR 0095 participant stamping cannot
			// depend on that accident: commit the genesis edge before any DROP can
			// need it.
			let result = self
				.commit_epoch_transition_locked(
					&MembershipEpoch {
						epoch: visible.epoch - 1,
						peers: HashMap::new(),
					},
					&visible,
				)
				.await;
			if result
				.as_ref()
				.is_err_and(|error| error.kind() == ErrorKind::NotConnected)
			{
				// Leadership can change between the check above and the Raft
				// write. The new leader will reconcile the same deterministic
				// edge; a node that lost leadership must not fail bootstrap.
				return Ok(());
			}
			return result;
		};
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
		// ADR 0699: migration initiation is disabled under ParentDecisions. The
		// fixed membership forms before any write is served, so no data moves;
		// an in-flight migration would refuse every Reserve over its range.
		let missing = if self.decision_mode
			== crate::consensus::decision_mode::DecisionMode::ParentDecisions
		{
			Vec::new()
		} else {
			missing_seed_batch(&expected, &snapshot.migrations)?
		};
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

	/// ADR 0699 S3a: admit and propose for the authenticated sender, or say why
	/// not. A store on today's authority answers `ModeToday`.
	async fn handle_decision_propose(
		&self,
		remote: NodeId,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		use crate::network::decision_proposals::{
			ProposalRefusal, ProposalResponse, ProposeRequest, decode_bounded,
		};
		// ADR 0721: the frame's version is judged against this node's mode before
		// the body is decoded. A ParentDecisions node takes only its own envelope;
		// a Today node takes only the one HRFT version (and then answers
		// `ModeToday`, as before). The frame reader accepts either on this type.
		self.decision_mode.check_raft_frame(frame.header.version)?;
		let response = match &self.decision_proposals {
			None => ProposalResponse::Refused(ProposalRefusal::ModeToday),
			Some(proposals) => match decode_bounded::<ProposeRequest>(frame.meta.as_ref()) {
				Ok(propose) => {
					proposals.observe_sender_clock(remote.id, propose.sent_ms);
					#[cfg(test)]
					proposals.pause_forwarded(remote.id, &propose.request).await;
					proposals.propose_as(remote.id, propose.request).await
				}
				Err(_) => ProposalResponse::Refused(ProposalRefusal::Malformed),
			},
		};
		let bytes = postcard::to_stdvec(&response).map_err(Error::other)?;
		let mut meta = self.pool.acquire();
		meta.as_mut_vec().extend_from_slice(&bytes);
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: self.decision_mode.raft_frame_version(),
				msg_type: MsgType::DECISION_PROPOSE_RESP.0,
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

	/// ADR 0699 S3c: an authoritative read of a parent's recorded decision.
	async fn handle_decision_lookup(
		&self,
		remote: NodeId,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		use crate::network::decision_proposals::{
			LookupRequest, LookupResponse, ProposalRefusal, decode_bounded,
		};
		let response = match &self.decision_proposals {
			None => LookupResponse::Refused(ProposalRefusal::ModeToday),
			Some(proposals) => match decode_bounded::<LookupRequest>(frame.meta.as_ref()) {
				Ok(request) => {
					proposals.observe_sender_clock(remote.id, request.sent_ms);
					proposals.lookup_as_leader(request).await
				}
				Err(_) => LookupResponse::Refused(ProposalRefusal::Malformed),
			},
		};
		let bytes = postcard::to_stdvec(&response).map_err(Error::other)?;
		let mut meta = self.pool.acquire();
		meta.as_mut_vec().extend_from_slice(&bytes);
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::DECISION_LOOKUP_RESP.0,
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

	/// ADR 0699 S4: a reader's linearizable cut, answered only by the leader.
	async fn handle_decision_cut(
		&self,
		remote: NodeId,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		use crate::network::decision_proposals::{
			CutRequest, CutResponse, ProposalRefusal, decode_bounded,
		};
		let response = match &self.decision_proposals {
			None => CutResponse::Refused(ProposalRefusal::ModeToday),
			Some(proposals) => match decode_bounded::<CutRequest>(frame.meta.as_ref()) {
				Ok(request) => {
					proposals.observe_sender_clock(remote.id, request.sent_ms);
					proposals.cut_as_leader().await
				}
				Err(_) => CutResponse::Refused(ProposalRefusal::Malformed),
			},
		};
		let bytes = postcard::to_stdvec(&response).map_err(Error::other)?;
		let mut meta = self.pool.acquire();
		meta.as_mut_vec().extend_from_slice(&bytes);
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::DECISION_CUT_RESP.0,
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
		if req.node_id == 0 {
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
		if req.node_id == self.local_node.id {
			#[cfg(feature = "test-support")]
			self.record_join_observation(req, "self JOIN handler ingress")
				.await;
			#[cfg(feature = "test-support")]
			if let Some(gate) = &self.join_promotion_gate {
				gate.pause(join_test_support::JoinGateStage::BeforeSelfFence, req)
					.await?;
			}
			// A cached leader ID is insufficient: a removed/partitioned former
			// leader can retain locally tempting completed membership forever.
			// Fence before taking either local lock, including the applied-index
			// wait provided by OpenRaft's ensure_linearizable implementation.
			self.raft.ensure_linearizable().await.map_err(|err| {
				Error::new(
					ErrorKind::NotConnected,
					format!("self JOIN freshness fence failed: {err}"),
				)
			})?;
			let _reconcile = self.reconcile_lock.lock().await;
			let (membership, latest_epoch) = self.state_machine.join_completion_snapshot().await;
			let visible = self.epoch_rx.borrow().clone();
			// Production epoch publication holds reconcile_lock and publishes
			// only after transition seeds are committed. Current full equality
			// is its completion certificate; mature migration rows need not
			// equal their original seeds. No state is repaired by this read.
			return validate_completed_self_join(
				req,
				self.local_node.id,
				self.protocol,
				&membership,
				latest_epoch.as_ref(),
				&visible,
			);
		}
		self.decision_mode
			.check_joiner(NodeCapabilitySet::from_u8(req.capability_bits))?;
		// ADR 0720: once a write has been admitted the membership is fixed. A
		// join is then answered from applied state alone and starts no
		// membership entry: a member repeating its completed join succeeds, and
		// anything else is refused. This is a courtesy that keeps entries out of
		// the log; the fence is in the reducer, which a race here cannot pass.
		if self.decision_mode == crate::consensus::decision_mode::DecisionMode::ParentDecisions {
			if let Err(err) = self.raft.ensure_linearizable().await {
				if !self.is_leader().await {
					return Err(not_leader_error(self.leader_hint().await));
				}
				return Err(Error::new(
					ErrorKind::NotConnected,
					format!("JOIN freshness fence failed: {err}"),
				));
			}
			let _reconcile = self.reconcile_lock.lock().await;
			let sealed = self
				.state_machine
				.read_state(|state| {
					state
						.decisions
						.as_ref()
						.is_some_and(|decisions| decisions.sealed().is_some())
				})
				.await;
			if sealed {
				let (membership, latest_epoch) =
					self.state_machine.join_completion_snapshot().await;
				let visible = self.epoch_rx.borrow().clone();
				return validate_completed_member_join(
					req,
					self.protocol,
					&membership,
					latest_epoch.as_ref(),
					&visible,
				);
			}
		}
		#[cfg(feature = "test-support")]
		if let Some(gate) = &self.join_promotion_gate {
			gate.record_mutation_path(req.node_id);
		}
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

		#[cfg(feature = "test-support")]
		if let Some(gate) = &self.join_promotion_gate {
			self.record_join_observation(req, "after voter promotion before epoch completion")
				.await;
			gate.pause(join_test_support::JoinGateStage::AfterPromotion, req)
				.await?;
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
		let target = NodeId {
			id: leader_id,
			_reserved: 0,
		};
		let visible_has_leader = self.epoch_rx.borrow().peers.contains_key(&target);
		let conn = if visible_has_leader {
			// Preserve normal epoch capabilities and its errors; a failed dial or
			// authentication must never select the membership fallback.
			self.factory.get_connection(target).await?
		} else {
			// A promoted voter can become leader before its epoch edge exists.
			// Read applied membership only, never an address supplied by this JOIN.
			let (membership, _) = self.state_machine.join_completion_snapshot().await;
			let current_leader = self.leader_hint().await;
			let now_visible = self.epoch_rx.borrow().peers.contains_key(&target);
			match join_forwarding_address(
				self.local_node.id,
				leader_id,
				current_leader,
				now_visible,
				&membership,
			)? {
				None => self.factory.get_connection(target).await?,
				Some(address) => self.factory.get_connection_to(target, &address).await?,
			}
		};
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
				let remote = context.require_authenticated_node("LEASE_REQ")?;
				self.handle_lease(remote, frame).await?
			}
			MsgType::LEASES_REQ => {
				self.authorize_control_operation(&context, ControlOperation::Lease)?;
				let remote = context.require_authenticated_node("LEASES_REQ")?;
				self.handle_leases(remote, frame).await?
			}
			MsgType::DECISION_CUT_REQ => {
				self.authorize_control_operation(&context, ControlOperation::DecisionPropose)?;
				let remote = context.require_authenticated_node("DECISION_CUT_REQ")?;
				self.handle_decision_cut(remote, frame).await?
			}
			MsgType::DECISION_LOOKUP_REQ => {
				self.authorize_control_operation(&context, ControlOperation::DecisionPropose)?;
				let remote = context.require_authenticated_node("DECISION_LOOKUP_REQ")?;
				self.handle_decision_lookup(remote, frame).await?
			}
			MsgType::DECISION_PROPOSE_REQ => {
				self.authorize_control_operation(&context, ControlOperation::DecisionPropose)?;
				let remote = context.require_authenticated_node("DECISION_PROPOSE_REQ")?;
				self.handle_decision_propose(remote, frame).await?
			}
			MsgType::LEASE_RELEASE_REQ => {
				self.authorize_control_operation(&context, ControlOperation::LeaseRelease)?;
				let remote = context.require_authenticated_node("LEASE_RELEASE_REQ")?;
				self.handle_release(remote, frame).await?
			}
			MsgType::TRANSFER_REQ => {
				self.authorize_control_operation(&context, ControlOperation::Transfer)?;
				let remote = context.require_authenticated_node("TRANSFER_REQ")?;
				self.handle_transfer(&context, remote, frame).await?
			}
			MsgType::EPOCH_ADV_REQ => {
				self.authorize_control_operation(&context, ControlOperation::AdvanceEpoch)?;
				self.handle_advance_epoch(&context, frame).await?
			}
			MsgType::JOIN_REQ => {
				self.authorize_control_operation(&context, ControlOperation::Join)?;
				self.handle_join(&context, frame).await?
			}
			MsgType::SCHEMA_UPDATE_REQ => self.handle_schema_update(&context, frame).await?,
			MsgType::CTAS_SAGA_REQ => self.handle_ctas_saga(&context, frame).await?,
			MsgType::SCHEMA_DROP_REQ => {
				if context.actor.role == ActorRole::DataNode {
					authorize_actor(&context.actor, ActorPermission::InterNodeTransport)?;
				} else {
					self.authorize_control_operation(&context, ControlOperation::SchemaDrop)?;
				}
				self.handle_schema_drop(&context, frame).await?
			}
			MsgType::SECONDARY_INDEX_STATUS_REQ => {
				self.authorize_control_operation(&context, ControlOperation::IndexStatus)?;
				self.handle_secondary_index_status(&context, frame).await?
			}
			MsgType::MAINTENANCE_REPORT_REQ => {
				self.authorize_control_operation(&context, ControlOperation::MaintenanceReport)?;
				self.handle_maintenance_report(frame).await?
			}
			MsgType::MAINTENANCE_REQUEUE_REQ => {
				self.authorize_control_operation(&context, ControlOperation::MaintenanceRequeue)?;
				self.handle_maintenance_requeue(&context, frame).await?
			}
			MsgType::TABLE_DESCRIPTOR_GET_REQ => {
				self.handle_table_descriptor_get(&context, frame).await?
			}
			MsgType::TABLE_DESCRIPTOR_LIST_REQ => {
				self.handle_table_descriptor_list(&context, frame).await?
			}
			MsgType::TABLE_OPTIMIZER_FACTS_GET_REQ => {
				self.handle_table_optimizer_facts_get(&context, frame)
					.await?
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

	// ADR 0090: partitioned rows move by PARTITION, not by arc. A moved segment
	// changes the owner of every partition whose representative position lies
	// inside it, and those partitions' rows live across their full spans — far
	// outside the segment. Each qualifying intersection therefore also seeds,
	// per live partition width, one span-shaped entry covering the contiguous
	// run of moved partitions. Without these, a membership change re-owns a
	// partition before its bytes transfer, and the new owner serves misses.
	let partition_widths: std::collections::BTreeSet<u8> = snapshot
		.table_schemas
		.live_descriptors()
		.filter_map(crate::schema::table::partitioned_prefix_width)
		.collect();
	let mut partition_planned: Vec<SeedPlanSegment> = Vec::new();
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
							source_replicas: source_replicas.clone(),
							upstream_migration_id,
							migration_hlc_floor,
						},
					);
					for width in &partition_widths {
						let step = 1u64 << (64 - u32::from(*width));
						let lattice_start = start.div_ceil(step).saturating_mul(step);
						let lattice_end = (end / step).saturating_mul(step);
						if lattice_start > end || lattice_end < start {
							continue;
						}
						// One entry per contiguous run of moved partitions: it
						// covers every representative in [start, end] and every
						// row locality of those partitions, and nothing else.
						partition_planned.push(SeedPlanSegment {
							start: lattice_start,
							end: lattice_end | (step - 1),
							logical_owner: new_owner,
							source_replicas: source_replicas.clone(),
							upstream_migration_id,
							migration_hlc_floor,
						});
					}
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

	let mut out = Vec::with_capacity(planned.len() + partition_planned.len());
	for segment in planned.into_iter().chain(partition_planned) {
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
	crate::storage::ipc::validate_ipc_stream_envelope(bytes).map_err(|error| {
		crate::network::ipc::observe_wire_decode_error_for_bytes(bytes);
		error
	})?;
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

/// JOIN-only routing fallback. None retains the ordinary visible-epoch route.
/// A routing snapshot grants no JOIN acceptance; the authenticated destination
/// still runs all leader, authorization and completion checks.
fn join_forwarding_address(
	local_id: u32,
	leader_id: u32,
	current_leader: Option<u32>,
	visible_has_leader: bool,
	membership: &openraft::StoredMembership<u32, BasicNode>,
) -> std::io::Result<Option<String>> {
	let unavailable = || not_leader_error(current_leader);
	if leader_id == 0 || leader_id == local_id || current_leader != Some(leader_id) {
		return Err(unavailable());
	}
	if visible_has_leader {
		return Ok(None);
	}
	let configs = membership.membership().get_joint_config();
	if membership.log_id().is_none() || configs.len() != 1 || !configs[0].contains(&leader_id) {
		return Err(unavailable());
	}
	let node = membership
		.membership()
		.get_node(&leader_id)
		.ok_or_else(unavailable)?;
	if node.addr.trim().is_empty() {
		return Err(unavailable());
	}
	Ok(Some(node.addr.clone()))
}

#[cfg(test)]
mod join_forwarding_tests;

/// Current completed-state idempotence, not recognition of a historical actor
/// key or operation receipt. The caller supplies a fresh quorum/applied fence.
fn validate_completed_self_join(
	req: &JoinClusterRequestWire,
	local_id: u32,
	protocol: ProtocolVersionWindow,
	membership: &openraft::StoredMembership<u32, BasicNode>,
	latest_epoch: Option<&(u64, EpochConfig)>,
	visible: &MembershipEpoch,
) -> std::io::Result<()> {
	let mismatch = || {
		Error::new(
			ErrorKind::InvalidInput,
			"self JOIN does not match current completed membership",
		)
	};
	let configs = membership.membership().get_joint_config();
	if req.node_id != local_id
		|| membership.log_id().is_none()
		|| configs.len() != 1
		|| !configs[0].contains(&local_id)
		|| membership
			.membership()
			.get_node(&local_id)
			.map(|node| node.addr.as_str())
			!= Some(req.advertise_address.as_str())
		|| req.protocol != protocol
	{
		return Err(mismatch());
	}
	wire::negotiate_protocol(protocol, req.protocol)?;
	let (epoch_key, durable) = latest_epoch.ok_or_else(mismatch)?;
	if *epoch_key != durable.epoch || *durable != epoch_config_from_membership(visible) {
		return Err(mismatch());
	}
	let peer = durable.peers.get(&local_id).ok_or_else(mismatch)?;
	if peer.node_id.id != req.node_id
		|| peer.address != req.advertise_address
		|| peer.tier_bits != req.tier_bits
		|| peer.role_bits != req.role_bits
		|| peer.capability_bits != req.capability_bits
	{
		return Err(mismatch());
	}
	Ok(())
}

/// ADR 0720: a join request to a sealed cluster succeeds only as the repeat of a
/// completed join. The applied membership must be uniform with no learner, its
/// voters exactly the latest epoch's peers, that epoch the published one, and the
/// requester a member whose address, tiers, roles and capabilities are the ones
/// on record. A non-member, a changed property or an incomplete view is refused.
fn validate_completed_member_join(
	req: &JoinClusterRequestWire,
	protocol: ProtocolVersionWindow,
	membership: &openraft::StoredMembership<u32, BasicNode>,
	latest_epoch: Option<&(u64, EpochConfig)>,
	visible: &MembershipEpoch,
) -> std::io::Result<()> {
	let sealed = || {
		membership_sealed(
			"a write has been admitted, so membership is fixed; only a member's identical \
			 completed join is answered",
		)
	};
	wire::negotiate_protocol(protocol, req.protocol)?;
	let configs = membership.membership().get_joint_config();
	let voters: BTreeSet<u32> = membership.membership().voter_ids().collect();
	let nodes: BTreeSet<u32> = membership.membership().nodes().map(|(id, _)| *id).collect();
	let (epoch_key, durable) = latest_epoch.ok_or_else(sealed)?;
	let peers: BTreeSet<u32> = durable.peers.values().map(|peer| peer.node_id.id).collect();
	if membership.log_id().is_none()
		|| configs.len() != 1
		|| nodes != voters
		|| peers != voters
		|| *epoch_key != durable.epoch
		|| *durable != epoch_config_from_membership(visible)
		|| membership
			.membership()
			.get_node(&req.node_id)
			.map(|node| node.addr.as_str())
			!= Some(req.advertise_address.as_str())
	{
		return Err(sealed());
	}
	let peer = durable.peers.get(&req.node_id).ok_or_else(sealed)?;
	if peer.node_id.id != req.node_id
		|| peer.address != req.advertise_address
		|| peer.tier_bits != req.tier_bits
		|| peer.role_bits != req.role_bits
		|| peer.capability_bits != req.capability_bits
	{
		return Err(sealed());
	}
	Ok(())
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
	use crate::network::server::HrftService;
	use crate::network::tls::{ActorIdentity, ActorRole, ActorTrustDomain};
	use crate::network::transport::{TransportTier, TransportTierSet};
	use crate::node::{NodeBootstrapConfig, WendelonNode};
	use crate::schema::table::{decode_descriptor_wire_name, encode_descriptor_wire_name};
	use crate::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RecordBodyCompressionPolicy, RetentionWindowMillis,
		RowIdentityPolicy, SchemaEvolutionPolicy, SlabId, TableSchemaCatalog,
		decode_table_schema_descriptor_v1, encode_table_schema_descriptor_v1,
	};
	use crate::storage::index::{PersistentLocalIndex, persistent_index_path};
	use crate::storage::maintenance_policy::{
		DeterministicOrigin, DeterministicReceipt, MaintenanceWorkKey, SystemMaintenanceClock,
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
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

	fn actor(role: ActorRole) -> ActorIdentity {
		ActorIdentity {
			role,
			trust_domain: role.trust_domain(),
			cert_subject: None,
			node_id: None,
			tenant_id: (role == ActorRole::TenantAdmin).then(|| "7".to_owned()),
			scopes: Vec::new(),
			key_id: vec![role as u8],
			break_glass: role.break_glass(),
		}
	}

	struct NthFailAuditSink {
		calls: std::sync::atomic::AtomicUsize,
		fail_at: Option<usize>,
		events: std::sync::Mutex<Vec<AuditEvent>>,
	}

	impl NthFailAuditSink {
		fn new(fail_at: usize) -> Self {
			Self {
				calls: std::sync::atomic::AtomicUsize::new(0),
				fail_at: Some(fail_at),
				events: std::sync::Mutex::new(Vec::new()),
			}
		}

		fn never_fails() -> Self {
			Self {
				calls: std::sync::atomic::AtomicUsize::new(0),
				fail_at: None,
				events: std::sync::Mutex::new(Vec::new()),
			}
		}
	}

	impl AuditSink for NthFailAuditSink {
		fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
			let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
			if self.fail_at == Some(call) {
				return Err(Error::other("injected audit refusal"));
			}
			self.events.lock().unwrap().push(event);
			Ok(())
		}
	}

	async fn quarantined_policy_for_control_test() -> (
		crate::test_support::TestDir,
		Arc<MaintenancePolicyInner>,
		Vec<SlabId>,
		MaintenanceFingerprint,
	) {
		let path_dir = crate::test_support::TestDir::new("wendelon-adr0582-control");
		let path = path_dir.join("store.redb");
		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::initialize_new(path, None)
				.await
				.unwrap(),
		);
		let slabs = vec![SlabId::new(21), SlabId::new(22)];
		let first = DeterministicReceipt::after_failure(None, 1_000, "shape").unwrap();
		let second = DeterministicReceipt::after_failure(Some(&first), 2_000, "shape").unwrap();
		let receipt = DeterministicReceipt::after_failure(Some(&second), 3_000, "shape").unwrap();
		let key = MaintenanceWorkKey::new(
			MaintenanceOperation::LsmCompaction,
			slabs.iter().copied(),
			DeterministicOrigin::LsmInconsistentSourceFrames,
		)
		.unwrap();
		let mut receipts = BTreeMap::new();
		receipts.insert(key, receipt);
		let policy = MaintenancePolicyInner::enabled(
			store,
			receipts,
			Arc::new(SystemMaintenanceClock),
			crate::metrics::isolated_for_test(),
		);
		let fingerprint = policy
			.quarantine_fingerprint_for_test(MaintenanceOperation::LsmCompaction, &slabs)
			.unwrap();
		(path_dir, policy, slabs, fingerprint)
	}

	async fn seed_quarantined_receipt(
		dir: &std::path::Path,
		operation: MaintenanceOperation,
		slabs: &[SlabId],
		origin: DeterministicOrigin,
		detail: &str,
	) {
		std::fs::create_dir_all(dir).unwrap();
		let path = persistent_index_path(dir);
		let store = if path.exists() {
			PersistentLocalIndex::open(path).await
		} else {
			PersistentLocalIndex::initialize_new(path, None).await
		}
		.unwrap();
		let first = DeterministicReceipt::after_failure(None, 1_000, detail).unwrap();
		let second = DeterministicReceipt::after_failure(Some(&first), 2_000, detail).unwrap();
		let receipt = DeterministicReceipt::after_failure(Some(&second), 3_000, detail).unwrap();
		let key = MaintenanceWorkKey::new(operation, slabs.iter().copied(), origin).unwrap();
		store
			.put_maintenance_receipt(key.encode(), receipt.encode())
			.await
			.unwrap();
	}

	fn maintenance_control_frame<T: Serialize>(
		node: &WendelonNode,
		stream_id: u32,
		msg_type: MsgType,
		request: &T,
	) -> HrftFrame {
		let meta = wire::encode_into(&node.pool, request).unwrap();
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: msg_type.0,
				flags: 0,
				stream_id,
				meta_len: u32::try_from(meta.len()).unwrap(),
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		}
	}

	async fn report_real_node(
		node: &WendelonNode,
		role: ActorRole,
		stream_id: u32,
	) -> std::io::Result<MaintenanceReportResponseWire> {
		let frame = maintenance_control_frame(
			node,
			stream_id,
			MsgType::MAINTENANCE_REPORT_REQ,
			&MaintenanceReportRequestWire {
				version: MAINTENANCE_CONTROL_WIRE_VERSION,
				target_node_id: node.local_node.id,
				max_items: MAINTENANCE_REPORT_MAX_ITEMS as u16,
				cursor: Vec::new(),
			},
		);
		let response = node
			.router
			.handle_frame(
				RequestContext {
					remote: Some(node.local_node),
					actor: actor(role),
				},
				frame,
			)
			.await?
			.ok_or_else(|| Error::other("maintenance report returned no frame"))?;
		wire::decode(&response.meta)
	}

	async fn bootstrap_maintenance_test_node(
		node_id: u32,
		dir: std::path::PathBuf,
	) -> WendelonNode {
		let mut config =
			NodeBootstrapConfig::dev(node_id, dir, format!("127.0.0.1:{}", 30_000 + node_id));
		config.lsm_compaction_paused = true;
		config.mvcc_retention_paused = true;
		// These fixtures seed storage before the node's first start, so the
		// directory is not a fresh forming store and generates no root KEK
		// (ADR 0726): one is placed, once.
		if !config.data_dir.join(".wendelon/dev-root-kek.bin").exists() {
			crate::crypto::test_support::place_fresh_dev_root_artifact(&config.data_dir).unwrap();
		}
		WendelonNode::bootstrap_single_node(config).await.unwrap()
	}

	/// ADR 0582's decisive boundary: receipts are hydrated from two separate
	/// indexes, the real authenticated router reports only the addressed node,
	/// and an operation-scoped clear survives another reopen without touching a
	/// sibling operation over the same slabs.
	#[tokio::test]
	async fn maintenance_control_is_node_local_authenticated_and_durable_across_reopen() {
		let root = crate::test_support::TestDir::new("wendelon-adr0582-two-node");
		let dir_a = root.join("node-a");
		let dir_b = root.join("node-b");
		let slabs_a = [SlabId::new(11), SlabId::new(12)];
		let slabs_b = [SlabId::new(99)];
		seed_quarantined_receipt(
			&dir_a,
			MaintenanceOperation::LsmCompaction,
			&slabs_a,
			DeterministicOrigin::LsmInconsistentSourceFrames,
			"node-a-lsm",
		)
		.await;
		seed_quarantined_receipt(
			&dir_a,
			MaintenanceOperation::RetentionPrune,
			&slabs_a,
			DeterministicOrigin::MaintenanceRewriteWithoutClaim,
			"node-a-retention",
		)
		.await;
		seed_quarantined_receipt(
			&dir_b,
			MaintenanceOperation::SecondaryIndexBackfill,
			&slabs_b,
			DeterministicOrigin::MaintenanceRewriteWithoutClaim,
			"node-b-backfill",
		)
		.await;

		let node_a = bootstrap_maintenance_test_node(101, dir_a.clone()).await;
		let node_b = bootstrap_maintenance_test_node(102, dir_b).await;
		let report_a = report_real_node(&node_a, ActorRole::ReadOnlyObserver, 1)
			.await
			.unwrap();
		let report_b = report_real_node(&node_b, ActorRole::ReadOnlyObserver, 2)
			.await
			.unwrap();
		assert_eq!(report_a.serving_node_id, node_a.local_node.id);
		assert_eq!(report_b.serving_node_id, node_b.local_node.id);
		assert!(matches!(
			report_a.outcome,
			MaintenanceReportOutcomeWire::Page
		));
		assert!(matches!(
			report_b.outcome,
			MaintenanceReportOutcomeWire::Page
		));
		assert_eq!(report_a.items.len(), 2);
		assert_eq!(report_b.items.len(), 1);
		assert!(report_a.items.iter().all(|item| match item {
			MaintenanceReportItemWire::QuarantinedWork { slabs, .. }
			| MaintenanceReportItemWire::BackingOff { slabs, .. }
			| MaintenanceReportItemWire::RetryableCooldown { slabs, .. }
			| MaintenanceReportItemWire::InFlight { slabs, .. } => slabs == &[11, 12],
		}));
		assert!(report_b.items.iter().all(|item| match item {
			MaintenanceReportItemWire::QuarantinedWork { slabs, .. }
			| MaintenanceReportItemWire::BackingOff { slabs, .. }
			| MaintenanceReportItemWire::RetryableCooldown { slabs, .. }
			| MaintenanceReportItemWire::InFlight { slabs, .. } => slabs == &[99],
		}));

		let (fingerprint, lsm_operation) = report_a
			.items
			.iter()
			.find_map(|item| match item {
				MaintenanceReportItemWire::QuarantinedWork {
					operation_code,
					quarantine_fingerprint,
					..
				} if *operation_code == MaintenanceOperation::LsmCompaction.code() => {
					Some((quarantine_fingerprint.clone(), *operation_code))
				}
				_ => None,
			})
			.expect("node A reports its LSM quarantine");
		let requeue_request = MaintenanceRequeueRequestWire {
			version: MAINTENANCE_CONTROL_WIRE_VERSION,
			target_node_id: node_a.local_node.id,
			operation_code: lsm_operation,
			slabs: vec![11, 12],
			expected_fingerprint: fingerprint,
		};
		for role in [ActorRole::ReadOnlyObserver, ActorRole::TenantAdmin] {
			let denied = node_a
				.router
				.handle_frame(
					RequestContext {
						remote: Some(node_a.local_node),
						actor: actor(role),
					},
					maintenance_control_frame(
						&node_a,
						3,
						MsgType::MAINTENANCE_REQUEUE_REQ,
						&requeue_request,
					),
				)
				.await;
			assert!(
				denied.is_err(),
				"{role:?} unexpectedly requeued node-wide work"
			);
		}

		let response = node_a
			.router
			.handle_frame(
				RequestContext {
					remote: Some(node_a.local_node),
					actor: actor(ActorRole::Operator),
				},
				maintenance_control_frame(
					&node_a,
					4,
					MsgType::MAINTENANCE_REQUEUE_REQ,
					&requeue_request,
				),
			)
			.await
			.unwrap()
			.unwrap();
		let response: MaintenanceRequeueResponseWire = wire::decode(&response.meta).unwrap();
		assert_eq!(
			response.outcome,
			MaintenanceRequeueOutcomeWire::Cleared { origins: 1 }
		);
		assert_eq!(
			response.completion_audit,
			MaintenanceCompletionAuditWire::Recorded
		);

		// A restart test needs every manager/index owner gone. Dropping the node
		// and sleeping only hopes that its aborted and blocking workers have let
		// go of the directory owner; on a loaded host they have not, and the
		// reopen sees DataDirectoryBusy. The joined shutdown closes the owner.
		node_a.shutdown().await.unwrap();
		let reopened_a = bootstrap_maintenance_test_node(101, dir_a).await;
		let after = report_real_node(&reopened_a, ActorRole::Operator, 5)
			.await
			.unwrap();
		assert_eq!(
			after.items.len(),
			1,
			"reopened node A report outcome: {:?}",
			after.outcome
		);
		assert!(matches!(
			&after.items[0],
			MaintenanceReportItemWire::QuarantinedWork { operation_code, slabs, .. }
				if *operation_code == MaintenanceOperation::RetentionPrune.code()
					&& slabs == &[11, 12]
		));
		let still_b = report_real_node(&node_b, ActorRole::Operator, 6)
			.await
			.unwrap();
		assert_eq!(still_b.items, report_b.items);

		reopened_a.handles.abort();
		node_b.handles.abort();
	}

	/// ADR 0582: viewing node-local maintenance state is diagnostic, while
	/// resetting a quarantine is cluster control. Tenant administration confers
	/// neither authority over a node-wide maintenance policy.
	#[test]
	fn maintenance_control_permissions_separate_report_from_requeue() {
		let report = control_operation_permission(ControlOperation::MaintenanceReport);
		let requeue = control_operation_permission(ControlOperation::MaintenanceRequeue);
		let observer = actor(ActorRole::ReadOnlyObserver);
		let tenant_admin = actor(ActorRole::TenantAdmin);
		let operator = actor(ActorRole::Operator);

		assert!(authorize_actor(&observer, report).is_ok());
		assert!(authorize_actor(&observer, requeue).is_err());
		assert!(authorize_actor(&tenant_admin, report).is_err());
		assert!(authorize_actor(&tenant_admin, requeue).is_err());
		assert!(authorize_actor(&operator, report).is_ok());
		assert!(authorize_actor(&operator, requeue).is_ok());
	}

	#[test]
	fn one_maximal_maintenance_report_item_fits_the_control_frame() {
		let response = MaintenanceReportResponseWire {
			version: MAINTENANCE_CONTROL_WIRE_VERSION,
			serving_node_id: 7,
			outcome: MaintenanceReportOutcomeWire::Page,
			incarnation: [0xff; 16],
			generation: u64::MAX,
			items: vec![MaintenanceReportItemWire::QuarantinedWork {
				operation_code: u16::MAX,
				slabs: vec![u64::MAX; crate::storage::maintenance_policy::MAX_RECEIPT_KEY_SLABS],
				origins: (1..=13)
					.map(|origin_code| MaintenanceOriginWire {
						origin_code,
						reason: "corrupt_source_frame".to_owned(),
						attempts: u32::MAX,
						last_failure_ms: u64::MAX,
						next_eligible_at_ms: None,
						detail: "x".repeat(256),
					})
					.collect(),
				quarantine_fingerprint: vec![0xff; 33],
			}],
			next_cursor: vec![0xff; 32 + 4096 * 8],
		};
		let encoded = wire::encode(&response).unwrap();
		assert!(
			encoded.len() <= MAINTENANCE_REPORT_MAX_BYTES,
			"maximal legal item is {} bytes, above the {}-byte frame",
			encoded.len(),
			MAINTENANCE_REPORT_MAX_BYTES
		);
	}

	#[tokio::test]
	async fn maintenance_report_stops_before_the_encoded_frame_budget() {
		let path_dir = crate::test_support::TestDir::new("wendelon-adr0582-page-budget");
		let path = path_dir.join("store.redb");
		let store = Arc::new(
			PersistentLocalIndex::initialize_new(path, None)
				.await
				.unwrap(),
		);
		let receipt = DeterministicReceipt::after_failure(
			Some(
				&DeterministicReceipt::after_failure(
					Some(
						&DeterministicReceipt::after_failure(None, 1_000, &"x".repeat(256))
							.unwrap(),
					),
					2_000,
					&"x".repeat(256),
				)
				.unwrap(),
			),
			3_000,
			&"x".repeat(256),
		)
		.unwrap();
		let mut receipts = BTreeMap::new();
		for item in 0..8_u64 {
			let base = item * 10_000;
			let slabs = (1..=crate::storage::maintenance_policy::MAX_RECEIPT_KEY_SLABS)
				.map(|offset| SlabId::new(base + offset as u64))
				.collect::<Vec<_>>();
			receipts.insert(
				MaintenanceWorkKey::new(
					MaintenanceOperation::LsmCompaction,
					slabs,
					DeterministicOrigin::LsmInconsistentSourceFrames,
				)
				.unwrap(),
				receipt.clone(),
			);
		}
		let policy = MaintenancePolicyInner::enabled(
			store,
			receipts,
			Arc::new(SystemMaintenanceClock),
			crate::metrics::isolated_for_test(),
		);
		let page = policy
			.report_page(
				None,
				MAINTENANCE_REPORT_MAX_ITEMS,
				MAINTENANCE_REPORT_MAX_BYTES,
			)
			.unwrap();
		assert!(
			page.next_cursor.is_some(),
			"the byte bound did not cut the eight maximal work items"
		);
		assert!(page.items.len() > 1, "the bound became a one-item limit");
		let response = MaintenanceReportResponseWire {
			version: MAINTENANCE_CONTROL_WIRE_VERSION,
			serving_node_id: 7,
			outcome: MaintenanceReportOutcomeWire::Page,
			incarnation: page.incarnation,
			generation: page.generation,
			items: page.items.into_iter().map(maintenance_item_wire).collect(),
			next_cursor: page.next_cursor.unwrap(),
		};
		let encoded = wire::encode(&response).unwrap();
		assert!(
			encoded.len() <= MAINTENANCE_REPORT_MAX_BYTES,
			"policy admitted {} encoded bytes into a {}-byte report frame",
			encoded.len(),
			MAINTENANCE_REPORT_MAX_BYTES
		);
	}

	#[test]
	fn maintenance_requeue_audit_is_bounded_and_never_names_the_slab_list() {
		let slabs = [11, 12, 13, 14, 15];
		let event = maintenance_requeue_audit_event(
			AuditOperation::MaintenanceRequeueAttempt,
			n(7),
			&actor(ActorRole::Operator),
			MaintenanceOperation::LsmCompaction,
			&slabs,
			maintenance_slab_digest(&slabs),
			&[0xab; 33],
			"attempted",
			AuditOutcome::Allowed,
		);
		let encoded = serde_json::to_string(&event).unwrap();
		assert!(encoded.len() < 2_048, "audit event is not bounded");
		assert!(!encoded.contains("[11,12,13,14,15]"));
		assert_eq!(
			event.parameters.get("slab_count"),
			Some(&serde_json::json!(5))
		);
		assert!(event.parameters.contains_key("slab_digest"));
	}

	#[tokio::test]
	async fn maintenance_requeue_audit_brackets_mutation_and_fails_closed() {
		let (_ordered_dir, ordered_policy, ordered_slabs, ordered_fingerprint) =
			quarantined_policy_for_control_test().await;
		let ordered_wire = ordered_slabs
			.iter()
			.map(|slab| slab.get())
			.collect::<Vec<_>>();
		let ordered_sink = NthFailAuditSink::never_fails();
		let (ordered_outcome, ordered_completion) = execute_maintenance_requeue_control(
			&ordered_sink,
			Some(&ordered_policy),
			n(7),
			&actor(ActorRole::Operator),
			MaintenanceOperation::LsmCompaction,
			&ordered_wire,
			&ordered_slabs,
			maintenance_slab_digest(&ordered_wire),
			&ordered_fingerprint.to_bytes(),
			ordered_fingerprint,
		)
		.await;
		assert_eq!(
			ordered_outcome,
			MaintenanceRequeueOutcomeWire::Cleared { origins: 1 }
		);
		assert_eq!(ordered_completion, MaintenanceCompletionAuditWire::Recorded);
		let operations = ordered_sink
			.events
			.lock()
			.unwrap()
			.iter()
			.map(|event| event.operation)
			.collect::<Vec<_>>();
		assert_eq!(
			operations,
			[
				AuditOperation::MaintenanceRequeueAttempt,
				AuditOperation::MaintenanceRequeueCompletion,
			]
		);

		let (_dir, policy, slabs, fingerprint) = quarantined_policy_for_control_test().await;
		let slabs_wire = slabs.iter().map(|slab| slab.get()).collect::<Vec<_>>();
		let digest = maintenance_slab_digest(&slabs_wire);
		let expected_wire = fingerprint.to_bytes();
		let refusing_attempt = NthFailAuditSink::new(1);
		let (outcome, completion) = execute_maintenance_requeue_control(
			&refusing_attempt,
			Some(&policy),
			n(7),
			&actor(ActorRole::Operator),
			MaintenanceOperation::LsmCompaction,
			&slabs_wire,
			&slabs,
			digest,
			&expected_wire,
			fingerprint,
		)
		.await;
		assert_eq!(outcome, MaintenanceRequeueOutcomeWire::AuditUnavailable);
		assert_eq!(completion, MaintenanceCompletionAuditWire::NotAttempted);
		assert_eq!(
			policy.quarantine_fingerprint_for_test(MaintenanceOperation::LsmCompaction, &slabs),
			Some(fingerprint),
			"attempt-audit refusal cleared state"
		);

		let failing_completion = NthFailAuditSink::new(2);
		let (outcome, completion) = execute_maintenance_requeue_control(
			&failing_completion,
			Some(&policy),
			n(7),
			&actor(ActorRole::Operator),
			MaintenanceOperation::LsmCompaction,
			&slabs_wire,
			&slabs,
			digest,
			&expected_wire,
			fingerprint,
		)
		.await;
		assert_eq!(
			outcome,
			MaintenanceRequeueOutcomeWire::Cleared { origins: 1 }
		);
		assert!(matches!(
			completion,
			MaintenanceCompletionAuditWire::Failed { .. }
		));
		let events = failing_completion.events.lock().unwrap();
		assert_eq!(events.len(), 1);
		assert_eq!(
			events[0].operation,
			AuditOperation::MaintenanceRequeueAttempt
		);
		assert!(
			policy
				.quarantine_fingerprint_for_test(MaintenanceOperation::LsmCompaction, &slabs)
				.is_none(),
			"successful clear was rolled back when completion audit failed"
		);
	}

	#[test]
	fn descriptor_get_leader_returns_latest_descriptor() {
		let tenant_id = TenantId::new(7);
		let table_object_id = TableObjectId::new([0x72; 16]);
		let v1 = descriptor_for_get(table_object_id, 1, "analytics", "events");
		let v2 = descriptor_for_get(table_object_id, 2, "analytics", "events");
		let mut catalog = TableSchemaCatalog::default();
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
		let mut catalog = TableSchemaCatalog::default();
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
		let mut catalog = TableSchemaCatalog::default();
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
		let mut catalog = TableSchemaCatalog::default();
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
	fn optimizer_facts_get_metrics_count_aggregate_outcomes() {
		let registry = crate::metrics::MetricsRegistry::default();
		registry.control_optimizer_facts_get_requests_total.inc();
		registry.control_optimizer_facts_get_success_total.inc();
		registry.control_optimizer_facts_get_not_found_total.inc();
		registry.control_optimizer_facts_get_not_leader_total.inc();
		registry
			.control_optimizer_facts_get_authorization_failed_total
			.inc();
		registry
			.control_optimizer_facts_get_invalid_request_total
			.inc();

		let rendered = registry.render_prometheus();
		for metric in [
			"wendelon_control_optimizer_facts_get_requests_total 1",
			"wendelon_control_optimizer_facts_get_success_total 1",
			"wendelon_control_optimizer_facts_get_not_found_total 1",
			"wendelon_control_optimizer_facts_get_not_leader_total 1",
			"wendelon_control_optimizer_facts_get_authorization_failed_total 1",
			"wendelon_control_optimizer_facts_get_invalid_request_total 1",
		] {
			assert!(rendered.contains(metric), "missing metric sample {metric}");
		}
		for line in rendered
			.lines()
			.filter(|line| line.contains("wendelon_control_optimizer_facts_get"))
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
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
			crate::consensus::OwnerRef::for_test(1, 1).slab(SlabId::new(100)),
			TieredSlab {
				slab: crate::consensus::OwnerRef::for_test(1, 1).slab(SlabId::new(100)),
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
				certified: None,
			},
		);
		snapshot.tiered_slabs.insert(
			crate::consensus::OwnerRef::for_test(1, 1).slab(SlabId::new(101)),
			TieredSlab {
				slab: crate::consensus::OwnerRef::for_test(1, 1).slab(SlabId::new(101)),
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
				certified: None,
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

	/// ADR 0090: a moved segment must also seed span-shaped entries for the
	/// partitions whose representatives it covers — without them, a membership
	/// change re-owns a partition before its bytes transfer. The trigger is the
	/// live catalog: no partitioned tables, no partition seeds.
	#[test]
	fn adr0090_epoch_seeder_emits_partition_spans_for_partitioned_tables() {
		let epoch1 = membership(1, &[1]);
		let epoch2 = membership(2, &[1, 2, 3, 4, 5]);

		// Without partitioned tables, only segment seeds are planned.
		let empty = crate::consensus::raft_storage::SmState::default();
		let segment_only = build_seed_batch(&epoch1, &epoch2, 1, &empty).unwrap();
		assert!(
			!segment_only.is_empty(),
			"premise: this membership change moves segments"
		);
		let width: u8 = 16;
		let step = 1u64 << (64 - u32::from(width));
		assert!(
			segment_only
				.iter()
				.all(
					|seed| (seed.entry.range_end - seed.entry.range_start + 1) % step != 0
						|| seed.entry.range_start % step != 0
				),
			"premise: no segment seed is accidentally span-aligned"
		);

		// A live partitioned table puts its width in play.
		let mut snapshot = crate::consensus::raft_storage::SmState::default();
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let mut descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([0x51; 16]),
			1,
			"analytics",
			"partitioned_events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_layout_policy = crate::schema::RecordLayoutPolicy::MultiRow(
			crate::schema::MultiRowLayoutDescriptor::default(),
		);
		descriptor.record_body_compression_policy =
			RecordBodyCompressionPolicy::default_for_layout(&descriptor.record_layout_policy);
		descriptor.refresh_descriptor_fingerprints().unwrap();
		assert_eq!(
			crate::schema::table::partitioned_prefix_width(&descriptor),
			Some(width),
			"premise: the fixture table is partitioned at the default width"
		);
		snapshot
			.table_schemas
			.upsert(TenantId::new(7), descriptor)
			.unwrap();

		let with_partitions = build_seed_batch(&epoch1, &epoch2, 1, &snapshot).unwrap();
		let span_seeds: Vec<_> = with_partitions
			.iter()
			.filter(|seed| {
				seed.entry.range_start % step == 0
					&& (seed.entry.range_end.wrapping_sub(seed.entry.range_start) + 1) % step == 0
			})
			.collect();
		assert!(
			!span_seeds.is_empty(),
			"a partitioned table makes moved segments seed partition spans"
		);
		let new_ring = RingState::build(&epoch2, 1);
		for seed in &span_seeds {
			// Every span covers whole partitions, and its owner is the new
			// ring's owner at each representative inside it — the partition
			// moves to where its position now routes, with its full span of
			// row localities carried along.
			let mut representative = seed.entry.range_start;
			while representative <= seed.entry.range_end {
				assert_eq!(
					new_ring
						.replicas_for_partition(crate::schema::PartitionId::new(representative))
						.first()
						.copied()
						.unwrap(),
					seed.entry.logical_owner,
					"span seed owner matches the new ring at representative {representative:#x}"
				);
				let Some(next) = representative.checked_add(step) else {
					break;
				};
				representative = next;
			}
		}
		// And the segment seeds are unchanged by the addition.
		for seed in &segment_only {
			assert!(
				with_partitions
					.iter()
					.any(|candidate| candidate.entry == seed.entry),
				"segment seeds survive alongside partition seeds"
			);
		}
	}
	#[tokio::test]
	async fn existing_node_missing_index_refuses_maintenance_and_reads() {
		use crate::storage::core::StorageEngine;
		let root = crate::test_support::TestDir::new("dc112-missing-index-maintenance");
		let dir = root.join("node");
		let slabs = [SlabId::new(11), SlabId::new(12)];
		seed_quarantined_receipt(
			&dir,
			MaintenanceOperation::LsmCompaction,
			&slabs,
			DeterministicOrigin::LsmInconsistentSourceFrames,
			"missing-index-control",
		)
		.await;
		let node = bootstrap_maintenance_test_node(7191, dir.clone()).await;
		let original = report_real_node(&node, ActorRole::Operator, 1)
			.await
			.unwrap();
		let fingerprint = original
			.items
			.iter()
			.find_map(|item| match item {
				MaintenanceReportItemWire::QuarantinedWork {
					quarantine_fingerprint,
					..
				} => Some(quarantine_fingerprint.clone()),
				_ => None,
			})
			.expect("fixture must have a real quarantine");
		node.shutdown().await.unwrap();
		std::fs::remove_file(persistent_index_path(&dir)).unwrap();
		let node = bootstrap_maintenance_test_node(7191, dir.clone()).await;
		let before = report_real_node(&node, ActorRole::Operator, 2).await;
		let request = MaintenanceRequeueRequestWire {
			version: MAINTENANCE_CONTROL_WIRE_VERSION,
			target_node_id: node.local_node.id,
			operation_code: MaintenanceOperation::LsmCompaction.code(),
			slabs: vec![11, 12],
			expected_fingerprint: fingerprint,
		};
		let reply = node
			.router
			.handle_frame(
				RequestContext {
					remote: Some(node.local_node),
					actor: actor(ActorRole::Operator),
				},
				maintenance_control_frame(&node, 3, MsgType::MAINTENANCE_REQUEUE_REQ, &request),
			)
			.await;
		let requeue = reply.map(|frame| {
			let frame = frame.expect("maintenance reply");
			wire::decode::<MaintenanceRequeueResponseWire>(&frame.meta).unwrap()
		});
		let after = report_real_node(&node, ActorRole::Operator, 4).await;
		let tenant = crate::schema::TenantId::new(1);
		let read = node.slab.lookup(
			tenant,
			crate::schema::Identity::blake3_tenant(tenant, b"missing-index-read"),
		);
		eprintln!(
			"MISSING_INDEX_CONTROL before={before:?} requeue={requeue:?} after={after:?} read={read:?}"
		);
		let report_refused = |report: &std::io::Result<MaintenanceReportResponseWire>| match report
		{
			Err(error) => error.kind() == std::io::ErrorKind::WouldBlock,
			Ok(report) => {
				matches!(
					report.outcome,
					MaintenanceReportOutcomeWire::Unavailable { .. }
						| MaintenanceReportOutcomeWire::PolicyDisabled { .. }
				) && report.items.is_empty()
			}
		};
		let requeue_refused = match &requeue {
			Err(error) => error.kind() == std::io::ErrorKind::WouldBlock,
			Ok(reply) => matches!(
				reply.outcome,
				MaintenanceRequeueOutcomeWire::PolicyDisabled { .. }
					| MaintenanceRequeueOutcomeWire::StoreFailure { .. }
			),
		};
		assert!(report_refused(&before), "report must refuse missing index");
		assert!(requeue_refused, "requeue must refuse missing index");
		assert!(report_refused(&after), "report must still refuse");
		assert!(read.is_err(), "missing index must not answer a read");
		assert!(
			!persistent_index_path(&dir).exists(),
			"maintenance must not recreate index"
		);
		if let (Ok(before), Ok(after)) = (&before, &after) {
			assert_eq!(before.generation, after.generation);
			assert_eq!(before.items, after.items);
		}
		node.shutdown().await.unwrap();
	}
}

#[cfg(test)]
#[path = "control_server/completed_join_tests.rs"]
mod completed_join_tests;
