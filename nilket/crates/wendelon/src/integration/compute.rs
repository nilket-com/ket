//! Wendelon-owned side of the compute/storage integration contract.
//!
//! Toron is the first intended consumer, but the public contract
//! deliberately uses generic compute-client language so Wendelon is
//! not tied to one compute implementation.

use crate::network::cert_issuance_policy::{
	CertIssuancePolicy, CertIssueRequest, PreparedCertIssue, QUERY_SESSION_CERT_MAX_TTL,
};
use crate::network::tls::{ACTOR_CLAIMS_OID_DOTTED, ActorIdentity, ActorRole};
use crate::schema::{IndexObjectId, ReadStatus, TableObjectId, TenantId, WriteStatus};
use observability::telemetry as shared_telemetry;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::time::Duration;

pub const COMPUTE_STORAGE_CONTRACT_VERSION: u16 = 1;
pub const SCHEMA_UPDATE_ENVELOPE_V1_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const TRACEPARENT_LEN: usize = shared_telemetry::TRACEPARENT_LEN;

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
	LogicalType,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropOperationId {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub schema_generation: SchemaGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableSchemaDropRequest {
	V1 {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		expected_generation: SchemaGeneration,
		operation_id: DropOperationId,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableSchemaDropFields {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub expected_generation: SchemaGeneration,
	pub operation_id: DropOperationId,
}

impl TableSchemaDropRequest {
	pub fn current(fields: TableSchemaDropFields) -> Self {
		Self::V1 {
			tenant_id: fields.tenant_id,
			table_object_id: fields.table_object_id,
			expected_generation: fields.expected_generation,
			operation_id: fields.operation_id,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WendelonAdminError {
	TableMissing {
		table_object_id: TableObjectId,
	},
	GenerationMismatch {
		expected: SchemaGeneration,
		committed: SchemaGeneration,
	},
	TableAlreadyDropping {
		table_object_id: TableObjectId,
		existing_operation_id: DropOperationId,
	},
	TableAlreadyDropped {
		table_object_id: TableObjectId,
	},
	DependentIndexesPresent {
		indexes: Vec<IndexObjectId>,
	},
	Transport {
		message: String,
	},
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

pub const LOGICAL_TYPE_INTERNAL_NAMESPACE: &str = "_toron_logical_types";
pub const LOGICAL_TYPE_RESERVED_STATIC_MAX_ID: u32 = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalTypePhysical {
	Int64,
	Int32,
	Float64,
	Float32,
	Utf8,
	Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UnitScale {
	pub numerator: u64,
	pub denominator: u64,
}

impl UnitScale {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.numerator == 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"logical type unit scale numerator must be nonzero",
			));
		}
		if self.denominator == 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"logical type unit scale denominator must be nonzero",
			));
		}
		Ok(())
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitQuantityKind {
	Delta,
	Point,
}

impl Default for UnitQuantityKind {
	fn default() -> Self {
		Self::Delta
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UnitOffset {
	pub numerator: i64,
	pub denominator: u64,
}

impl UnitOffset {
	pub const ZERO: Self = Self {
		numerator: 0,
		denominator: 1,
	};

	pub fn is_zero(&self) -> bool {
		self.numerator == 0
	}

	pub fn validate(&self) -> std::io::Result<()> {
		if self.denominator == 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"logical type unit offset denominator must be nonzero",
			));
		}
		Ok(())
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogicalTypeUnit {
	pub dimension: String,
	pub scale_to_base: UnitScale,
	pub quantity_kind: UnitQuantityKind,
	pub offset_to_base: UnitOffset,
}

impl LogicalTypeUnit {
	pub fn validate_for_physical(&self, physical: LogicalTypePhysical) -> std::io::Result<()> {
		if physical != LogicalTypePhysical::Float64 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"logical type unit metadata requires Float64 physical carrier",
			));
		}
		validate_logical_type_unit_dimension(&self.dimension)?;
		self.scale_to_base.validate()?;
		self.offset_to_base.validate()?;
		if self.quantity_kind == UnitQuantityKind::Delta && !self.offset_to_base.is_zero() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"logical type delta unit offset must be zero",
			));
		}
		Ok(())
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalTypeRefinementPredicate {
	Comparison {
		left: LogicalTypeRefinementValue,
		op: LogicalTypeRefinementComparisonOp,
		right: LogicalTypeRefinementValue,
	},
	And(Vec<LogicalTypeRefinementPredicate>),
}

impl LogicalTypeRefinementPredicate {
	pub fn validate_for_physical(&self, physical: LogicalTypePhysical) -> std::io::Result<()> {
		match self {
			LogicalTypeRefinementPredicate::Comparison { left, op, right } => {
				validate_logical_type_refinement_comparison(physical, left, *op, right)
			}
			LogicalTypeRefinementPredicate::And(predicates) => {
				if predicates.is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"logical type refinement AND must contain at least one predicate",
					));
				}
				for predicate in predicates {
					predicate.validate_for_physical(physical)?;
				}
				Ok(())
			}
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalTypeRefinementValue {
	Value,
	Int64(i64),
	Int32(i32),
	Float64(u64),
	Float32(u32),
	Utf8(String),
	Boolean(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalTypeRefinementComparisonOp {
	Eq,
	Ne,
	Lt,
	Le,
	Gt,
	Ge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalTypeDefinitionCreate {
	pub name: String,
	pub physical: LogicalTypePhysical,
	pub refinement: Option<LogicalTypeRefinementPredicate>,
	#[serde(default)]
	pub unit: Option<LogicalTypeUnit>,
}

impl LogicalTypeDefinitionCreate {
	pub fn validate(&self) -> std::io::Result<()> {
		validate_logical_type_name(self.name.as_str())?;
		if let Some(refinement) = &self.refinement {
			refinement.validate_for_physical(self.physical)?;
		}
		if let Some(unit) = &self.unit {
			unit.validate_for_physical(self.physical)?;
		}
		Ok(())
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalTypeDefinition {
	pub logical_type_id: u32,
	pub name: String,
	pub physical: LogicalTypePhysical,
	pub refinement: Option<LogicalTypeRefinementPredicate>,
	#[serde(default)]
	pub unit: Option<LogicalTypeUnit>,
}

impl LogicalTypeDefinition {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.logical_type_id <= LOGICAL_TYPE_RESERVED_STATIC_MAX_ID {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"logical_type_id is in the reserved static seed range",
			));
		}
		validate_logical_type_name(self.name.as_str())?;
		if let Some(refinement) = &self.refinement {
			refinement.validate_for_physical(self.physical)?;
		}
		if let Some(unit) = &self.unit {
			unit.validate_for_physical(self.physical)?;
		}
		Ok(())
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalTypeCatalog {
	#[serde(default)]
	by_id: BTreeMap<LogicalTypeIdCatalogKey, LogicalTypeDefinition>,
	#[serde(default)]
	by_name: BTreeMap<LogicalTypeNameCatalogKey, u32>,
}

impl LogicalTypeCatalog {
	pub fn create(
		&mut self,
		tenant_id: TenantId,
		create: LogicalTypeDefinitionCreate,
	) -> std::io::Result<LogicalTypeDefinition> {
		create.validate()?;
		let name_key = LogicalTypeNameCatalogKey {
			tenant_id,
			name: create.name.clone(),
		};
		if let Some(existing_id) = self.by_name.get(&name_key).copied() {
			let existing = self
				.by_id
				.get(&LogicalTypeIdCatalogKey {
					tenant_id,
					logical_type_id: existing_id,
				})
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"logical type catalog name index references a missing id",
					)
				})?;
			if existing.physical == create.physical
				&& existing.refinement == create.refinement
				&& existing.unit == create.unit
			{
				return Ok(existing.clone());
			}
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				"logical type name already exists with a different physical type, refinement, or unit metadata",
			));
		}
		let definition = LogicalTypeDefinition {
			logical_type_id: self.next_id_for_tenant(tenant_id)?,
			name: create.name,
			physical: create.physical,
			refinement: create.refinement,
			unit: create.unit,
		};
		definition.validate()?;
		let id_key = LogicalTypeIdCatalogKey {
			tenant_id,
			logical_type_id: definition.logical_type_id,
		};
		if self.by_id.contains_key(&id_key) {
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				"logical type id already exists",
			));
		}
		self.by_name.insert(
			LogicalTypeNameCatalogKey {
				tenant_id,
				name: definition.name.clone(),
			},
			definition.logical_type_id,
		);
		self.by_id.insert(id_key, definition.clone());
		Ok(definition)
	}

	pub fn get_by_name(&self, tenant_id: TenantId, name: &str) -> Option<&LogicalTypeDefinition> {
		let logical_type_id = self.by_name.get(&LogicalTypeNameCatalogKey {
			tenant_id,
			name: name.to_owned(),
		})?;
		self.by_id.get(&LogicalTypeIdCatalogKey {
			tenant_id,
			logical_type_id: *logical_type_id,
		})
	}

	pub fn get_by_id(
		&self,
		tenant_id: TenantId,
		logical_type_id: u32,
	) -> Option<&LogicalTypeDefinition> {
		self.by_id.get(&LogicalTypeIdCatalogKey {
			tenant_id,
			logical_type_id,
		})
	}

	pub fn definitions_for_tenant(&self, tenant_id: TenantId) -> Vec<LogicalTypeDefinition> {
		let start = LogicalTypeIdCatalogKey {
			tenant_id,
			logical_type_id: LOGICAL_TYPE_RESERVED_STATIC_MAX_ID + 1,
		};
		let end = LogicalTypeIdCatalogKey {
			tenant_id,
			logical_type_id: u32::MAX,
		};
		self.by_id
			.range(start..=end)
			.map(|(_, definition)| definition.clone())
			.collect()
	}

	fn next_id_for_tenant(&self, tenant_id: TenantId) -> std::io::Result<u32> {
		let start = LogicalTypeIdCatalogKey {
			tenant_id,
			logical_type_id: LOGICAL_TYPE_RESERVED_STATIC_MAX_ID + 1,
		};
		let end = LogicalTypeIdCatalogKey {
			tenant_id,
			logical_type_id: u32::MAX,
		};
		let next = self
			.by_id
			.range(start..=end)
			.next_back()
			.map(|(key, _)| key.logical_type_id.saturating_add(1))
			.unwrap_or(LOGICAL_TYPE_RESERVED_STATIC_MAX_ID + 1);
		if next <= LOGICAL_TYPE_RESERVED_STATIC_MAX_ID {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"logical type id allocator overflowed",
			));
		}
		Ok(next)
	}
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct LogicalTypeIdCatalogKey {
	tenant_id: TenantId,
	logical_type_id: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct LogicalTypeNameCatalogKey {
	tenant_id: TenantId,
	name: String,
}

pub fn encode_logical_type_definition_create(
	create: &LogicalTypeDefinitionCreate,
) -> std::io::Result<Vec<u8>> {
	create.validate()?;
	postcard::to_allocvec(create).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode logical type create: {err}"),
		)
	})
}

pub fn decode_logical_type_definition_create(
	bytes: &[u8],
) -> std::io::Result<LogicalTypeDefinitionCreate> {
	let create: LogicalTypeDefinitionCreate = postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to decode logical type create: {err}"),
		)
	})?;
	create.validate()?;
	Ok(create)
}

pub fn encode_logical_type_definition(
	definition: &LogicalTypeDefinition,
) -> std::io::Result<Vec<u8>> {
	definition.validate()?;
	postcard::to_allocvec(definition).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode logical type definition: {err}"),
		)
	})
}

pub fn decode_logical_type_definition(bytes: &[u8]) -> std::io::Result<LogicalTypeDefinition> {
	let definition: LogicalTypeDefinition = postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to decode logical type definition: {err}"),
		)
	})?;
	definition.validate()?;
	Ok(definition)
}

pub fn encode_logical_type_definitions(
	definitions: &[LogicalTypeDefinition],
) -> std::io::Result<Vec<u8>> {
	for definition in definitions {
		definition.validate()?;
	}
	postcard::to_allocvec(definitions).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode logical type definitions: {err}"),
		)
	})
}

pub fn decode_logical_type_definitions(
	bytes: &[u8],
) -> std::io::Result<Vec<LogicalTypeDefinition>> {
	let definitions: Vec<LogicalTypeDefinition> = postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to decode logical type definitions: {err}"),
		)
	})?;
	for definition in &definitions {
		definition.validate()?;
	}
	Ok(definitions)
}

fn validate_logical_type_refinement_comparison(
	physical: LogicalTypePhysical,
	left: &LogicalTypeRefinementValue,
	op: LogicalTypeRefinementComparisonOp,
	right: &LogicalTypeRefinementValue,
) -> std::io::Result<()> {
	let left_is_value = matches!(left, LogicalTypeRefinementValue::Value);
	let right_is_value = matches!(right, LogicalTypeRefinementValue::Value);
	if left_is_value == right_is_value {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"logical type refinement comparison must compare VALUE with a literal",
		));
	}
	let literal = if left_is_value { right } else { left };
	validate_logical_type_refinement_literal(physical, literal)?;
	if !logical_type_refinement_op_allowed(physical, op) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ordered logical type refinement comparisons require a numeric physical type",
		));
	}
	Ok(())
}

fn validate_logical_type_refinement_literal(
	physical: LogicalTypePhysical,
	literal: &LogicalTypeRefinementValue,
) -> std::io::Result<()> {
	let valid = match (physical, literal) {
		(LogicalTypePhysical::Int64, LogicalTypeRefinementValue::Int64(_)) => true,
		(LogicalTypePhysical::Int32, LogicalTypeRefinementValue::Int32(_)) => true,
		(LogicalTypePhysical::Float64, LogicalTypeRefinementValue::Float64(bits)) => {
			f64::from_bits(*bits).is_finite()
		}
		(LogicalTypePhysical::Float32, LogicalTypeRefinementValue::Float32(bits)) => {
			f32::from_bits(*bits).is_finite()
		}
		(LogicalTypePhysical::Utf8, LogicalTypeRefinementValue::Utf8(_)) => true,
		(LogicalTypePhysical::Boolean, LogicalTypeRefinementValue::Boolean(_)) => true,
		_ => false,
	};
	if valid {
		return Ok(());
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		"logical type refinement literal must match the physical carrier type",
	))
}

fn logical_type_refinement_op_allowed(
	physical: LogicalTypePhysical,
	op: LogicalTypeRefinementComparisonOp,
) -> bool {
	match op {
		LogicalTypeRefinementComparisonOp::Eq | LogicalTypeRefinementComparisonOp::Ne => true,
		LogicalTypeRefinementComparisonOp::Lt
		| LogicalTypeRefinementComparisonOp::Le
		| LogicalTypeRefinementComparisonOp::Gt
		| LogicalTypeRefinementComparisonOp::Ge => matches!(
			physical,
			LogicalTypePhysical::Int64
				| LogicalTypePhysical::Int32
				| LogicalTypePhysical::Float64
				| LogicalTypePhysical::Float32
		),
	}
}

fn validate_logical_type_name(value: &str) -> std::io::Result<()> {
	validate_contract_name("logical type name", value)?;
	if value.eq_ignore_ascii_case("naked") {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"naked is a reserved logical type sentinel",
		));
	}
	Ok(())
}

fn validate_logical_type_unit_dimension(value: &str) -> std::io::Result<()> {
	if value.is_empty() || value.len() > 128 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"logical type unit dimension length is out of bounds",
		));
	}
	if value.eq_ignore_ascii_case("naked") {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"naked is a reserved logical type unit dimension",
		));
	}
	if !value
		.bytes()
		.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"logical type unit dimension must be a one-part ASCII identifier",
		));
	}
	Ok(())
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
		ReadStatus::QUERY_CANCELLED => StorageStatusKind::TerminalStorageError,
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
		shared_telemetry::TraceContext {
			traceparent: self.traceparent.clone(),
			tracestate: self.tracestate.clone(),
		}
		.validate()
	}
}

pub fn validate_traceparent(traceparent: &str) -> std::io::Result<()> {
	shared_telemetry::validate_traceparent(traceparent)
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
