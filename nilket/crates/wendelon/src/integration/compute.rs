//! Wendelon-owned side of the compute/storage integration contract.
//!
//! Toron is the first intended consumer, but the public contract
//! deliberately uses generic compute-client language so Wendelon is
//! not tied to one compute implementation.

use crate::network::cert_issuance_policy::{
	CertIssuancePolicy, CertIssueRequest, PreparedCertIssue, QUERY_SESSION_CERT_MAX_TTL,
};
use crate::network::tls::{ACTOR_CLAIMS_OID_DOTTED, ActorIdentity, ActorRole};
use crate::schema::{ReadStatus, TenantId, WriteStatus};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};
use std::time::Duration;

pub const COMPUTE_STORAGE_CONTRACT_VERSION: u16 = 1;
pub const SCHEMA_UPDATE_ENVELOPE_V1_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const TRACEPARENT_LEN: usize = 55;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeQuerySessionIssueRequest {
	pub tenant_id: TenantId,
	pub key_id: Vec<u8>,
	pub ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeQuerySessionHandoff {
	pub contract_version: u16,
	pub actor_claims_oid: &'static str,
	pub tenant_id: TenantId,
	pub policy_max_ttl: Duration,
	pub prepared_cert: PreparedCertIssue,
}

pub fn prepare_query_session_handoff(
	policy: &CertIssuancePolicy,
	request: ComputeQuerySessionIssueRequest,
) -> std::io::Result<ComputeQuerySessionHandoff> {
	let prepared_cert = policy.prepare_issue(CertIssueRequest::QuerySession {
		tenant_id: request.tenant_id,
		key_id: request.key_id,
		ttl: request.ttl,
	})?;
	if prepared_cert.role != ActorRole::QuerySession {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"query-session handoff produced a non-query-session role",
		));
	}
	if prepared_cert.tenant_id.as_deref() != Some(&request.tenant_id.get().to_string()) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"query-session handoff must be tenant-bound",
		));
	}
	Ok(ComputeQuerySessionHandoff {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		actor_claims_oid: ACTOR_CLAIMS_OID_DOTTED,
		tenant_id: request.tenant_id,
		policy_max_ttl: QUERY_SESSION_CERT_MAX_TTL,
		prepared_cert,
	})
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaIntentOrigin {
	ComputeClient,
	WendelonOperator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaObjectKind {
	Namespace,
	Table,
	Index,
	View,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaObjectRefV1 {
	pub kind: SchemaObjectKind,
	pub namespace: String,
	pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMutationKindV1 {
	Create,
	Alter,
	Drop,
	IndexBackfillComplete,
	IndexDropGcComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaGeneration(u64);

impl SchemaGeneration {
	pub const ZERO: Self = Self(0);

	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}
}

impl From<u64> for SchemaGeneration {
	fn from(value: u64) -> Self {
		Self::new(value)
	}
}

impl From<SchemaGeneration> for u64 {
	fn from(value: SchemaGeneration) -> Self {
		value.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaIntentId([u8; 16]);

impl SchemaIntentId {
	pub const ZERO: Self = Self([0; 16]);

	pub const fn new(bytes: [u8; 16]) -> Self {
		Self(bytes)
	}

	pub const fn as_bytes(&self) -> &[u8; 16] {
		&self.0
	}

	pub const fn into_bytes(self) -> [u8; 16] {
		self.0
	}
}

impl From<[u8; 16]> for SchemaIntentId {
	fn from(value: [u8; 16]) -> Self {
		Self::new(value)
	}
}

impl From<SchemaIntentId> for [u8; 16] {
	fn from(value: SchemaIntentId) -> Self {
		value.0
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaUpdateEnvelopeV1 {
	pub contract_version: u16,
	pub origin: SchemaIntentOrigin,
	pub tenant_id: TenantId,
	pub object: SchemaObjectRefV1,
	pub expected_generation: SchemaGeneration,
	pub requested_generation: SchemaGeneration,
	pub intent_id: SchemaIntentId,
	pub mutation: SchemaMutationKindV1,
	#[serde(with = "serde_bytes")]
	pub payload: Vec<u8>,
}

impl SchemaUpdateEnvelopeV1 {
	pub fn compute_client_intent(
		tenant_id: TenantId,
		object: SchemaObjectRefV1,
		expected_generation: SchemaGeneration,
		requested_generation: SchemaGeneration,
		intent_id: SchemaIntentId,
		mutation: SchemaMutationKindV1,
		payload: Vec<u8>,
	) -> Self {
		Self {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::ComputeClient,
			tenant_id,
			object,
			expected_generation,
			requested_generation,
			intent_id,
			mutation,
			payload,
		}
	}

	pub fn validate(&self) -> std::io::Result<()> {
		if self.contract_version != COMPUTE_STORAGE_CONTRACT_VERSION {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"unsupported compute/storage schema envelope version",
			));
		}
		validate_contract_name("namespace", self.object.namespace.as_str())?;
		validate_contract_name("object name", self.object.name.as_str())?;
		if self.requested_generation <= self.expected_generation {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"schema requested_generation must advance expected_generation",
			));
		}
		if self.payload.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"schema payload must not be empty",
			));
		}
		if self.payload.len() > SCHEMA_UPDATE_ENVELOPE_V1_MAX_PAYLOAD_BYTES {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"schema payload exceeds envelope maximum",
			));
		}
		Ok(())
	}
}

pub fn encode_schema_update_envelope_v1(
	envelope: &SchemaUpdateEnvelopeV1,
) -> std::io::Result<Vec<u8>> {
	envelope.validate()?;
	postcard::to_allocvec(envelope).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode schema update envelope: {err}"),
		)
	})
}

pub fn decode_schema_update_envelope_v1(bytes: &[u8]) -> std::io::Result<SchemaUpdateEnvelopeV1> {
	let envelope: SchemaUpdateEnvelopeV1 = postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to decode schema update envelope: {err}"),
		)
	})?;
	envelope.validate()?;
	Ok(envelope)
}

fn validate_contract_name(label: &str, value: &str) -> std::io::Result<()> {
	if value.is_empty() || value.len() > 128 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{label} length is out of bounds"),
		));
	}
	if !value
		.bytes()
		.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{label} contains unsupported characters"),
		));
	}
	Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageStatusKind {
	Success,
	Absent,
	RetryableRouteRefresh,
	RetryableBackpressure,
	FinalizationPending,
	QuotaExceeded,
	TenantUnavailable,
	AuthorizationFailed,
	InvalidQueryProgram,
	QueryBudgetExceeded,
	RetentionExpired,
	TerminalStorageError,
}

impl StorageStatusKind {
	/// Documented invariant: every `StorageStatusKind` value is
	/// storage-authored by construction. Compute callers can use
	/// this as a cheap assertion when handling generic outcomes.
	pub const fn storage_authored(self) -> bool {
		true
	}

	pub const fn retryable(self) -> bool {
		matches!(
			self,
			Self::RetryableRouteRefresh | Self::RetryableBackpressure | Self::FinalizationPending
		)
	}

	pub const fn quota_failure(self) -> bool {
		matches!(self, Self::QuotaExceeded)
	}
}

pub fn map_write_status_for_compute(status: WriteStatus) -> StorageStatusKind {
	match status {
		WriteStatus::OK | WriteStatus::DUPLICATE => StorageStatusKind::Success,
		WriteStatus::EPOCH_STALE
		| WriteStatus::NOT_LEADER
		| WriteStatus::LEASE_EXPIRED
		| WriteStatus::PROXY_LIMIT => StorageStatusKind::RetryableRouteRefresh,
		WriteStatus::BACKPRESSURED => StorageStatusKind::RetryableBackpressure,
		WriteStatus::FINALIZATION_PENDING => StorageStatusKind::FinalizationPending,
		WriteStatus::QUOTA_EXCEEDED => StorageStatusKind::QuotaExceeded,
		WriteStatus::TENANT_SUSPENDED | WriteStatus::TENANT_UNAVAILABLE => {
			StorageStatusKind::TenantUnavailable
		}
		WriteStatus::AUTHORIZATION_FAILED => StorageStatusKind::AuthorizationFailed,
		WriteStatus::PAYLOAD_TOO_LARGE
		| WriteStatus::INVALID_LEASE_SIGNATURE
		| WriteStatus::INTERNAL_ERROR => StorageStatusKind::TerminalStorageError,
		_ => StorageStatusKind::TerminalStorageError,
	}
}

pub fn map_read_status_for_compute(status: ReadStatus) -> StorageStatusKind {
	match status {
		ReadStatus::OK => StorageStatusKind::Success,
		ReadStatus::KEY_NOT_FOUND => StorageStatusKind::Absent,
		ReadStatus::EPOCH_STALE
		| ReadStatus::NOT_LEADER
		| ReadStatus::LEASE_EXPIRED
		| ReadStatus::E_SAFE_BEHIND
		| ReadStatus::MIGRATION_INCOMPLETE => StorageStatusKind::RetryableRouteRefresh,
		ReadStatus::BACKPRESSURED => StorageStatusKind::RetryableBackpressure,
		ReadStatus::TENANT_UNAVAILABLE => StorageStatusKind::TenantUnavailable,
		ReadStatus::AUTHORIZATION_FAILED => StorageStatusKind::AuthorizationFailed,
		ReadStatus::QUERY_INVALID_PROGRAM => StorageStatusKind::InvalidQueryProgram,
		ReadStatus::QUERY_BUDGET_EXCEEDED => StorageStatusKind::QueryBudgetExceeded,
		ReadStatus::RETENTION_EXPIRED => StorageStatusKind::RetentionExpired,
		ReadStatus::TABLE_DESCRIPTOR_EVOLVED => StorageStatusKind::TerminalStorageError,
		ReadStatus::INVALID_LEASE_SIGNATURE => StorageStatusKind::TerminalStorageError,
		_ => StorageStatusKind::TerminalStorageError,
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageQuotaMetadataV1 {
	pub tenant_id: TenantId,
	pub epoch: u64,
	pub quota_bytes: Option<u64>,
	pub used_bytes: Option<u64>,
	pub charged_bytes: Option<u64>,
}

impl StorageQuotaMetadataV1 {
	pub fn validate_for_tenant(&self, tenant_id: TenantId) -> std::io::Result<()> {
		if self.tenant_id != tenant_id {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"quota metadata tenant does not match request tenant",
			));
		}
		if let (Some(used), Some(quota)) = (self.used_bytes, self.quota_bytes)
			&& used > quota
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"quota metadata reports used bytes above quota",
			));
		}
		Ok(())
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeTraceContextV1 {
	pub traceparent: String,
	pub tracestate: Option<String>,
}

impl ComputeTraceContextV1 {
	pub fn validate(&self) -> std::io::Result<()> {
		validate_traceparent(self.traceparent.as_str())?;
		if let Some(tracestate) = &self.tracestate {
			if tracestate.len() > 512 || tracestate.chars().any(char::is_control) {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"tracestate is out of bounds",
				));
			}
		}
		Ok(())
	}
}

pub fn validate_traceparent(traceparent: &str) -> std::io::Result<()> {
	let bytes = traceparent.as_bytes();
	if bytes.len() != TRACEPARENT_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"traceparent must be exactly 55 bytes",
		));
	}
	if bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"traceparent separators are invalid",
		));
	}
	let version = &traceparent[0..2];
	let trace_id = &traceparent[3..35];
	let span_id = &traceparent[36..52];
	let flags = &traceparent[53..55];
	if version != "00" {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"only W3C traceparent version 00 is supported",
		));
	}
	if !is_lower_hex(trace_id) || trace_id.bytes().all(|byte| byte == b'0') {
		return Err(Error::new(ErrorKind::InvalidData, "trace id is invalid"));
	}
	if !is_lower_hex(span_id) || span_id.bytes().all(|byte| byte == b'0') {
		return Err(Error::new(ErrorKind::InvalidData, "span id is invalid"));
	}
	if !is_lower_hex(flags) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"trace flags are invalid",
		));
	}
	Ok(())
}

fn is_lower_hex(value: &str) -> bool {
	value
		.bytes()
		.all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

pub fn validate_query_session_audit_attribution(
	actor: &ActorIdentity,
	tenant_id: TenantId,
) -> std::io::Result<()> {
	if actor.role != ActorRole::QuerySession {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			"compute-originated storage access must use query_session identity",
		));
	}
	let Some(actor_tenant) = actor.claimed_tenant_id()? else {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			"query_session identity must be tenant-bound",
		));
	};
	if actor_tenant.get() != tenant_id.get() {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			"query_session tenant does not match request tenant",
		));
	}
	if !actor.scopes.iter().any(|scope| scope == "query_session") {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			"query_session identity is missing query_session scope",
		));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::network::tls::{ActorIdentity, ActorTrustDomain};

	#[test]
	fn traceparent_validation_rejects_uppercase_and_zero_ids() {
		assert!(
			validate_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").is_ok()
		);
		assert!(
			validate_traceparent("00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01")
				.is_err()
		);
		assert!(
			validate_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
				.is_err()
		);
	}

	#[test]
	fn schema_envelope_roundtrips_and_rejects_stale_generation() {
		let envelope = SchemaUpdateEnvelopeV1::compute_client_intent(
			TenantId::new(7),
			SchemaObjectRefV1 {
				kind: SchemaObjectKind::Table,
				namespace: "analytics".to_string(),
				name: "events".to_string(),
			},
			1.into(),
			2.into(),
			SchemaIntentId::new([9; 16]),
			SchemaMutationKindV1::Alter,
			b"arrow-schema-v2".to_vec(),
		);
		let encoded = encode_schema_update_envelope_v1(&envelope).unwrap();
		assert_eq!(
			decode_schema_update_envelope_v1(&encoded).unwrap(),
			envelope
		);

		let mut stale = envelope;
		stale.requested_generation = stale.expected_generation;
		assert!(stale.validate().is_err());
	}

	#[test]
	fn audit_attribution_requires_tenant_bound_query_session() {
		let actor = ActorIdentity {
			role: ActorRole::QuerySession,
			trust_domain: ActorTrustDomain::Data,
			cert_subject: None,
			node_id: None,
			tenant_id: Some("7".to_string()),
			scopes: vec!["query_session".to_string()],
			key_id: vec![1, 2, 3],
			break_glass: false,
		};
		assert!(validate_query_session_audit_attribution(&actor, TenantId::new(7)).is_ok());
		assert!(validate_query_session_audit_attribution(&actor, TenantId::new(8)).is_err());
	}
}
