use std::time::Duration;
use wendelon::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, ComputeQuerySessionHandoff, ComputeQuerySessionIssueRequest,
	ComputeTraceContextV1, LOGICAL_TYPE_INTERNAL_NAMESPACE, LOGICAL_TYPE_RESERVED_STATIC_MAX_ID,
	LogicalTypeCatalog, LogicalTypeDefinitionCreate, LogicalTypePhysical,
	LogicalTypeRefinementComparisonOp, LogicalTypeRefinementPredicate, LogicalTypeRefinementValue,
	LogicalTypeUnit, SCHEMA_UPDATE_ENVELOPE_V1_MAX_PAYLOAD_BYTES, SchemaGeneration, SchemaIntentId,
	SchemaMutationKindV1, SchemaObjectKind, SchemaObjectRefV1, SchemaUpdateEnvelopeV1,
	StorageQuotaMetadataV1, StorageStatusKind, UnitOffset, UnitQuantityKind, UnitScale,
	decode_logical_type_definition_create, decode_logical_type_definitions,
	decode_schema_update_envelope_v1, encode_logical_type_definition_create,
	encode_logical_type_definitions, encode_schema_update_envelope_v1, map_read_status_for_compute,
	map_write_status_for_compute, prepare_query_session_handoff,
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
fn logical_type_definition_payload_roundtrips_and_uses_schema_object_kind() {
	let create = LogicalTypeDefinitionCreate {
		name: "CustomerId".to_string(),
		physical: LogicalTypePhysical::Int64,
		refinement: None,
		unit: None,
	};
	let payload = encode_logical_type_definition_create(&create).unwrap();
	let envelope = SchemaUpdateEnvelopeV1::compute_client_intent(
		TenantId::new(42),
		SchemaObjectRefV1 {
			kind: SchemaObjectKind::LogicalType,
			namespace: LOGICAL_TYPE_INTERNAL_NAMESPACE.to_string(),
			name: "CustomerId".to_string(),
		},
		SchemaGeneration::ZERO,
		SchemaGeneration::new(1),
		SchemaIntentId::new([0x90; 16]),
		SchemaMutationKindV1::Create,
		payload.clone(),
	);

	let encoded = encode_schema_update_envelope_v1(&envelope).unwrap();
	let decoded = decode_schema_update_envelope_v1(&encoded).unwrap();

	assert_eq!(decoded.object.kind, SchemaObjectKind::LogicalType);
	assert_eq!(decoded.object.namespace, LOGICAL_TYPE_INTERNAL_NAMESPACE);
	assert_eq!(
		decode_logical_type_definition_create(&payload).unwrap(),
		create
	);
}

#[test]
fn logical_type_refinement_payload_roundtrips_and_participates_in_idempotence() {
	let refinement = LogicalTypeRefinementPredicate::Comparison {
		left: LogicalTypeRefinementValue::Value,
		op: LogicalTypeRefinementComparisonOp::Gt,
		right: LogicalTypeRefinementValue::Int64(0),
	};
	let create = LogicalTypeDefinitionCreate {
		name: "PersonId".to_string(),
		physical: LogicalTypePhysical::Int64,
		refinement: Some(refinement.clone()),
		unit: None,
	};

	let payload = encode_logical_type_definition_create(&create).unwrap();
	assert_eq!(
		decode_logical_type_definition_create(&payload).unwrap(),
		create
	);

	let mut catalog = LogicalTypeCatalog::default();
	let tenant = TenantId::new(7);
	let first = catalog.create(tenant, create.clone()).unwrap();
	let duplicate = catalog.create(tenant, create).unwrap();
	assert_eq!(first, duplicate);
	assert_eq!(first.refinement, Some(refinement));
	assert_eq!(
		catalog
			.create(
				tenant,
				LogicalTypeDefinitionCreate {
					name: "PersonId".to_string(),
					physical: LogicalTypePhysical::Int64,
					refinement: None,
					unit: None,
				},
			)
			.unwrap_err()
			.kind(),
		std::io::ErrorKind::AlreadyExists
	);
}

#[test]
fn logical_type_unit_metadata_roundtrips_and_participates_in_idempotence() {
	let unit = LogicalTypeUnit {
		dimension: "Length".to_string(),
		scale_to_base: UnitScale {
			numerator: 201_168,
			denominator: 125,
		},
		quantity_kind: UnitQuantityKind::Delta,
		offset_to_base: UnitOffset::ZERO,
		delta_unit: None,
	};
	let create = LogicalTypeDefinitionCreate {
		name: "Mile".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(unit.clone()),
	};

	let payload = encode_logical_type_definition_create(&create).unwrap();
	assert_eq!(
		decode_logical_type_definition_create(&payload).unwrap(),
		create
	);

	let mut catalog = LogicalTypeCatalog::default();
	let tenant = TenantId::new(7);
	let first = catalog.create(tenant, create.clone()).unwrap();
	let duplicate = catalog.create(tenant, create).unwrap();
	assert_eq!(first, duplicate);
	assert_eq!(first.unit, Some(unit));
}

#[test]
fn logical_type_unit_affine_metadata_roundtrips() {
	let unit = LogicalTypeUnit {
		dimension: "Temperature".to_string(),
		scale_to_base: UnitScale {
			numerator: 1,
			denominator: 1,
		},
		quantity_kind: UnitQuantityKind::Point,
		offset_to_base: UnitOffset {
			numerator: 5463,
			denominator: 20,
		},
		delta_unit: None,
	};
	let create = LogicalTypeDefinitionCreate {
		name: "Celsius".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(unit.clone()),
	};

	let payload = encode_logical_type_definition_create(&create).unwrap();
	assert_eq!(
		decode_logical_type_definition_create(&payload).unwrap(),
		create
	);

	let mut catalog = LogicalTypeCatalog::default();
	let tenant = TenantId::new(7);
	let definition = catalog.create(tenant, create).unwrap();
	assert_eq!(definition.unit, Some(unit));
}

#[test]
fn logical_type_unit_delta_pair_metadata_validates_and_roundtrips() {
	let delta_unit = LogicalTypeUnit {
		dimension: "Temperature".to_string(),
		scale_to_base: UnitScale {
			numerator: 1,
			denominator: 1,
		},
		quantity_kind: UnitQuantityKind::Delta,
		offset_to_base: UnitOffset::ZERO,
		delta_unit: None,
	};
	let point_unit = LogicalTypeUnit {
		dimension: "Temperature".to_string(),
		scale_to_base: UnitScale {
			numerator: 1,
			denominator: 1,
		},
		quantity_kind: UnitQuantityKind::Point,
		offset_to_base: UnitOffset {
			numerator: 5463,
			denominator: 20,
		},
		delta_unit: Some("CelsiusDelta".to_string()),
	};
	let delta = LogicalTypeDefinitionCreate {
		name: "CelsiusDelta".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(delta_unit.clone()),
	};
	let point = LogicalTypeDefinitionCreate {
		name: "Celsius".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(point_unit.clone()),
	};

	let payload = encode_logical_type_definition_create(&point).unwrap();
	assert_eq!(
		decode_logical_type_definition_create(&payload).unwrap(),
		point
	);

	let tenant = TenantId::new(7);
	let mut catalog = LogicalTypeCatalog::default();
	assert!(catalog.create(tenant, point.clone()).is_err());
	catalog.create(tenant, delta).unwrap();
	let definition = catalog.create(tenant, point.clone()).unwrap();
	assert_eq!(definition.unit, Some(point_unit));
	assert_eq!(catalog.create(tenant, point).unwrap(), definition);

	let wrong_kind = LogicalTypeDefinitionCreate {
		name: "Kelvin".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Temperature".to_string(),
			scale_to_base: UnitScale {
				numerator: 1,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Point,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: None,
		}),
	};
	catalog.create(tenant, wrong_kind).unwrap();
	let wrong_kind_point = LogicalTypeDefinitionCreate {
		name: "WrongKindPoint".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Temperature".to_string(),
			scale_to_base: UnitScale {
				numerator: 1,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Point,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: Some("Kelvin".to_string()),
		}),
	};
	assert!(catalog.create(tenant, wrong_kind_point).is_err());

	let wrong_dimension = LogicalTypeDefinitionCreate {
		name: "LengthDelta".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Length".to_string(),
			scale_to_base: UnitScale {
				numerator: 1,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Delta,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: None,
		}),
	};
	catalog.create(tenant, wrong_dimension).unwrap();
	let wrong_dimension_point = LogicalTypeDefinitionCreate {
		name: "WrongDimensionPoint".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Temperature".to_string(),
			scale_to_base: UnitScale {
				numerator: 1,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Point,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: Some("LengthDelta".to_string()),
		}),
	};
	assert!(catalog.create(tenant, wrong_dimension_point).is_err());

	let wrong_scale = LogicalTypeDefinitionCreate {
		name: "FahrenheitDelta".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Temperature".to_string(),
			scale_to_base: UnitScale {
				numerator: 5,
				denominator: 9,
			},
			quantity_kind: UnitQuantityKind::Delta,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: None,
		}),
	};
	catalog.create(tenant, wrong_scale).unwrap();
	let wrong_scale_point = LogicalTypeDefinitionCreate {
		name: "WrongScalePoint".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Temperature".to_string(),
			scale_to_base: UnitScale {
				numerator: 1,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Point,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: Some("FahrenheitDelta".to_string()),
		}),
	};
	assert!(catalog.create(tenant, wrong_scale_point).is_err());
}

#[test]
fn logical_type_unit_metadata_requires_float64_positive_scale_and_valid_offset() {
	let invalid_physical = LogicalTypeDefinitionCreate {
		name: "Mile".to_string(),
		physical: LogicalTypePhysical::Int64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Length".to_string(),
			scale_to_base: UnitScale {
				numerator: 1,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Delta,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: None,
		}),
	};
	assert!(encode_logical_type_definition_create(&invalid_physical).is_err());

	let invalid_scale = LogicalTypeDefinitionCreate {
		name: "Mile".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Length".to_string(),
			scale_to_base: UnitScale {
				numerator: 0,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Delta,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: None,
		}),
	};
	assert!(encode_logical_type_definition_create(&invalid_scale).is_err());

	let invalid_offset_denominator = LogicalTypeDefinitionCreate {
		name: "Celsius".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Temperature".to_string(),
			scale_to_base: UnitScale {
				numerator: 1,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Point,
			offset_to_base: UnitOffset {
				numerator: 1,
				denominator: 0,
			},
			delta_unit: None,
		}),
	};
	assert!(encode_logical_type_definition_create(&invalid_offset_denominator).is_err());

	let invalid_delta_offset = LogicalTypeDefinitionCreate {
		name: "CelsiusDelta".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Temperature".to_string(),
			scale_to_base: UnitScale {
				numerator: 1,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Delta,
			offset_to_base: UnitOffset {
				numerator: 1,
				denominator: 1,
			},
			delta_unit: None,
		}),
	};
	assert!(encode_logical_type_definition_create(&invalid_delta_offset).is_err());

	let invalid_delta_pairing_on_delta = LogicalTypeDefinitionCreate {
		name: "CelsiusDelta".to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Temperature".to_string(),
			scale_to_base: UnitScale {
				numerator: 1,
				denominator: 1,
			},
			quantity_kind: UnitQuantityKind::Delta,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: Some("CelsiusDelta".to_string()),
		}),
	};
	assert!(encode_logical_type_definition_create(&invalid_delta_pairing_on_delta).is_err());
}

#[test]
fn logical_type_catalog_allocates_durable_ids_and_is_idempotent() {
	let mut catalog = LogicalTypeCatalog::default();
	let tenant = TenantId::new(7);
	let first = catalog
		.create(
			tenant,
			LogicalTypeDefinitionCreate {
				name: "PersonId".to_string(),
				physical: LogicalTypePhysical::Int64,
				refinement: None,
				unit: None,
			},
		)
		.unwrap();
	let duplicate = catalog
		.create(
			tenant,
			LogicalTypeDefinitionCreate {
				name: "PersonId".to_string(),
				physical: LogicalTypePhysical::Int64,
				refinement: None,
				unit: None,
			},
		)
		.unwrap();

	assert_eq!(
		first.logical_type_id,
		LOGICAL_TYPE_RESERVED_STATIC_MAX_ID + 1
	);
	assert_eq!(duplicate, first);
	assert_eq!(catalog.get_by_name(tenant, "PersonId"), Some(&first));
	let listed = catalog.definitions_for_tenant(tenant);
	assert_eq!(listed, vec![first.clone()]);
	let listed_payload = encode_logical_type_definitions(&listed).unwrap();
	assert_eq!(
		decode_logical_type_definitions(&listed_payload).unwrap(),
		listed
	);
	assert_eq!(
		catalog
			.create(
				tenant,
				LogicalTypeDefinitionCreate {
					name: "PersonId".to_string(),
					physical: LogicalTypePhysical::Int32,
					refinement: None,
					unit: None,
				},
			)
			.unwrap_err()
			.kind(),
		std::io::ErrorKind::AlreadyExists
	);
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
