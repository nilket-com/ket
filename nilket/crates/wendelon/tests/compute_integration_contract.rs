use std::time::Duration;
use wendelon::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, ComputeQuerySessionHandoff, ComputeQuerySessionIssueRequest,
	ComputeTraceContextV1, SCHEMA_UPDATE_ENVELOPE_V1_MAX_PAYLOAD_BYTES, SchemaGeneration,
	SchemaIntentId, SchemaMutationKindV1, SchemaObjectKind, SchemaObjectRefV1,
	SchemaUpdateEnvelopeV1, StorageQuotaMetadataV1, StorageStatusKind,
	decode_schema_update_envelope_v1, encode_schema_update_envelope_v1,
	map_read_status_for_compute, map_write_status_for_compute, prepare_query_session_handoff,
	validate_query_session_audit_attribution,
};
use wendelon::network::cert_issuance_policy::{
	CertAuthorityKind, CertIssuancePolicy, QUERY_SESSION_CERT_MAX_TTL,
};
use wendelon::network::tls::{ActorIdentity, ActorRole, ActorTrustDomain};
use wendelon::schema::{ReadStatus, TenantId, WriteStatus};
use wendelon_client::{ClientError, StorageStatusKind as ClientStorageStatusKind};

struct MockComputeClient {
	tenant_id: TenantId,
	policy: CertIssuancePolicy,
}

impl MockComputeClient {
	fn new(tenant_id: TenantId) -> Self {
		Self {
			tenant_id,
			policy: CertIssuancePolicy::new("wendelon.test"),
		}
	}

	fn query_session_handoff(&self) -> std::io::Result<ComputeQuerySessionHandoff> {
		prepare_query_session_handoff(
			&self.policy,
			ComputeQuerySessionIssueRequest {
				tenant_id: self.tenant_id,
				key_id: vec![1, 2, 3, 4],
				ttl: Duration::from_secs(60),
			},
		)
	}
}

#[test]
fn mock_compute_client_gets_tenant_bound_query_session_handoff() {
	let client = MockComputeClient::new(TenantId::new(42));
	let handoff = client.query_session_handoff().unwrap();

	assert_eq!(handoff.prepared_cert.authority, CertAuthorityKind::NodeCa);
	assert_eq!(handoff.prepared_cert.role, ActorRole::QuerySession);
	assert_eq!(
		handoff.prepared_cert.tenant_id.as_deref(),
		Some("42"),
		"query_session must be tenant-bound"
	);
	assert_eq!(
		handoff.prepared_cert.scopes,
		vec!["query_session".to_string()]
	);
	assert_eq!(handoff.policy_max_ttl, QUERY_SESSION_CERT_MAX_TTL);
}

#[test]
fn compute_schema_intent_roundtrips_as_versioned_storage_envelope() {
	let envelope = SchemaUpdateEnvelopeV1::compute_client_intent(
		TenantId::new(42),
		SchemaObjectRefV1 {
			kind: SchemaObjectKind::Table,
			namespace: "analytics".to_string(),
			name: "events".to_string(),
		},
		SchemaGeneration::new(7),
		SchemaGeneration::new(8),
		SchemaIntentId::new([0xA5; 16]),
		SchemaMutationKindV1::Alter,
		br#"{"columns":["tenant_id","event_time","payload"]}"#.to_vec(),
	);

	let encoded = encode_schema_update_envelope_v1(&envelope).unwrap();
	let decoded = decode_schema_update_envelope_v1(&encoded).unwrap();

	assert_eq!(decoded, envelope);
	assert_eq!(decoded.tenant_id, TenantId::new(42));
	assert_eq!(decoded.requested_generation, SchemaGeneration::new(8));
}

#[test]
fn schema_envelope_rejects_bad_versions_and_oversized_payloads() {
	let mut envelope = SchemaUpdateEnvelopeV1::compute_client_intent(
		TenantId::new(42),
		SchemaObjectRefV1 {
			kind: SchemaObjectKind::Table,
			namespace: "analytics".to_string(),
			name: "events".to_string(),
		},
		SchemaGeneration::new(7),
		SchemaGeneration::new(8),
		SchemaIntentId::new([0xA5; 16]),
		SchemaMutationKindV1::Alter,
		vec![b'x'; SCHEMA_UPDATE_ENVELOPE_V1_MAX_PAYLOAD_BYTES + 1],
	);
	assert!(encode_schema_update_envelope_v1(&envelope).is_err());

	envelope.payload = b"valid-payload".to_vec();
	envelope.contract_version = COMPUTE_STORAGE_CONTRACT_VERSION + 1;
	let encoded = postcard::to_allocvec(&envelope).unwrap();
	assert!(decode_schema_update_envelope_v1(&encoded).is_err());
}

#[test]
fn schema_intent_id_type_enforces_sixteen_byte_shape() {
	let id = SchemaIntentId::new([0x7A; 16]);
	assert_eq!(id.as_bytes().len(), 16);
	assert_eq!(id.into_bytes(), [0x7A; 16]);
}

#[test]
fn storage_status_mapping_keeps_quota_and_retry_semantics_distinct() {
	let quota = map_write_status_for_compute(WriteStatus::QUOTA_EXCEEDED);
	assert_eq!(quota, StorageStatusKind::QuotaExceeded);
	assert!(quota.quota_failure());
	assert!(!quota.retryable());

	let backpressure = map_read_status_for_compute(ReadStatus::BACKPRESSURED);
	assert_eq!(backpressure, StorageStatusKind::RetryableBackpressure);
	assert!(backpressure.retryable());

	let invalid_program = map_read_status_for_compute(ReadStatus::QUERY_INVALID_PROGRAM);
	assert_eq!(invalid_program, StorageStatusKind::InvalidQueryProgram);
	assert!(!invalid_program.retryable());
}

#[test]
fn compute_trace_context_must_be_valid_w3c_traceparent() {
	let context = ComputeTraceContextV1 {
		traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
		tracestate: Some("vendor=value".to_string()),
	};
	assert!(context.validate().is_ok());

	let invalid = ComputeTraceContextV1 {
		traceparent: "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01".to_string(),
		tracestate: None,
	};
	assert!(invalid.validate().is_err());
}

#[test]
fn storage_quota_metadata_validates_tenant_and_usage_bounds() {
	let metadata = StorageQuotaMetadataV1 {
		tenant_id: TenantId::new(42),
		epoch: 9,
		quota_bytes: Some(100),
		used_bytes: Some(99),
		charged_bytes: Some(1),
	};
	assert!(metadata.validate_for_tenant(TenantId::new(42)).is_ok());
	assert!(metadata.validate_for_tenant(TenantId::new(43)).is_err());

	let over = StorageQuotaMetadataV1 {
		used_bytes: Some(101),
		..metadata
	};
	assert!(over.validate_for_tenant(TenantId::new(42)).is_err());
}

#[test]
fn audit_attribution_rejects_wrong_tenant_query_session() {
	let actor = ActorIdentity {
		role: ActorRole::QuerySession,
		trust_domain: ActorTrustDomain::Data,
		cert_subject: None,
		node_id: None,
		tenant_id: Some("42".to_string()),
		scopes: vec!["query_session".to_string()],
		key_id: vec![9, 9, 9],
		break_glass: false,
	};

	assert!(validate_query_session_audit_attribution(&actor, TenantId::new(42)).is_ok());
	assert!(validate_query_session_audit_attribution(&actor, TenantId::new(43)).is_err());
}

#[test]
fn client_error_exposes_generic_storage_status_classification() {
	let quota = ClientError::QuotaExceeded {
		tenant_id: TenantId::new(42),
	};
	assert_eq!(
		quota.storage_status_kind(),
		ClientStorageStatusKind::QuotaExceeded
	);

	let retry = ClientError::ReadFailed {
		status: ReadStatus::BACKPRESSURED,
	};
	assert_eq!(
		retry.storage_status_kind(),
		ClientStorageStatusKind::RetryableBackpressure
	);

	let io = ClientError::Io(std::io::Error::other("disk closed"));
	assert_eq!(
		io.storage_status_kind(),
		ClientStorageStatusKind::TerminalStorageError
	);
}
