use std::io::{Error, ErrorKind};
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

pub const SQL_REQUEST_FRAME_BYTES_HARD_CEILING: u32 = 64 * 1024 * 1024;
pub const SQL_STATEMENT_BYTES_HARD_CEILING: u32 = 64 * 1024 * 1024;
pub const DEFAULT_SQL_REQUEST_FRAME_BYTES: u32 = 4 * 1024 * 1024;
pub const DEFAULT_SQL_STATEMENT_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QueryId(Uuid);

impl QueryId {
	pub fn new() -> Self {
		Self(Uuid::now_v7())
	}

	pub const fn from_uuid(value: Uuid) -> Self {
		Self(value)
	}

	pub const fn from_u128(value: u128) -> Self {
		Self(Uuid::from_u128(value))
	}

	pub const fn get(self) -> Uuid {
		self.0
	}
}

impl Default for QueryId {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlFrame {
	pub protocol_version: SqlProtocolVersion,
	pub query_id: QueryId,
	pub message: SqlMessage,
}

impl SqlFrame {
	pub fn execute(query_id: QueryId, statement: String, timeout_seconds: Option<u32>) -> Self {
		Self::execute_with_timezone(query_id, statement, timeout_seconds, "UTC")
	}

	pub fn execute_with_timezone(
		query_id: QueryId,
		statement: String,
		timeout_seconds: Option<u32>,
		session_timezone: impl Into<String>,
	) -> Self {
		Self {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message: SqlMessage::Execute(ExecuteRequest {
				statement,
				statement_timeout_seconds: timeout_seconds,
				session_timezone: session_timezone.into(),
			}),
		}
	}

	pub const fn cancel(query_id: QueryId) -> Self {
		Self {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message: SqlMessage::Cancel(CancelRequest { query_id }),
		}
	}

	pub const fn health(query_id: QueryId) -> Self {
		Self {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message: SqlMessage::Health(HealthRequest),
		}
	}

	pub fn execute_rune(query_id: QueryId, source: String, timeout_seconds: Option<u32>) -> Self {
		Self::execute_rune_request(
			query_id,
			ExecuteRuneRequest::current(source, timeout_seconds),
		)
	}

	pub const fn execute_rune_request(query_id: QueryId, request: ExecuteRuneRequest) -> Self {
		Self {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message: SqlMessage::ExecuteRune(request),
		}
	}

	/// Resolve a retained mutation outcome without submitting the mutation again.
	/// The frame query id identifies this resolution exchange; the body retains the
	/// original SQL/Rune query id that named the mutation.
	pub const fn resolve_mutation(query_id: QueryId, operation_query_id: QueryId) -> Self {
		Self {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message: SqlMessage::ResolveMutation(ResolveMutationRequest { operation_query_id }),
		}
	}

	pub const fn response(query_id: QueryId, message: SqlMessage) -> Self {
		Self {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlProtocolVersion {
	Current,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlMessage {
	Execute(ExecuteRequest),
	Cancel(CancelRequest),
	ResultBatch(ResultBatch),
	Complete(Complete),
	Error(SqlError),
	// Append-only: postcard encodes enum discriminants by declaration order.
	Health(HealthRequest),
	Healthy(HealthyResponse),
	ExecuteRune(ExecuteRuneRequest),
	RuneError(RuneFailure),
	ResolveMutation(ResolveMutationRequest),
	MutationOutcome(MutationOutcomeResponse),
	// Append-only ADR 548 composite CTAS resolution.
	CtasOutcome(CtasOutcomeResponse),
	// Append-only ADR 574 materialization receipt.
	MaterializeOutcome(MaterializeOutcomeResponse),
}

/// ADR 574: routine status for a successful `materialize`. Every field is
/// derived from the publish result, so the receipt reports what was recorded
/// rather than what this build would have written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializeOutcomeResponse {
	/// Resolved namespace of the created table.
	pub namespace: String,
	/// Resolved table name. This is the identity `ctx.table(...)` accepts;
	/// the namespace travels beside it rather than dotted into it, because
	/// `TableRef::new` does not split a qualified string.
	pub table_name: String,
	/// Format version decoded from the attached certificate bytes.
	pub certificate_format_version: u16,
	/// Rows written to the new table. A CTAS returns no rows to the caller,
	/// so this deliberately does not share the `row_count` name that every
	/// other completion on this protocol uses for rows delivered.
	pub rows_written: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecuteRequest {
	pub statement: String,
	pub statement_timeout_seconds: Option<u32>,
	pub session_timezone: String,
}

macro_rules! rune_version {
	($name:ident, $current:expr) => {
		#[derive(
			Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
		)]
		pub struct $name(pub u16);

		impl $name {
			pub const CURRENT: Self = Self($current);
		}
	};
}

// ADR 574: the request-protocol axis alone advances for the materialization
// receipt. A response-shape change alters neither the Rune language nor the
// sandbox contract, and moving those axes would misreport what changed.
rune_version!(RuneRequestProtocolVersion, 2);
rune_version!(RuneLanguageSurfaceVersion, 1);
rune_version!(RuneSandboxContractVersion, 1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecuteRuneRequest {
	pub source: String,
	pub request_protocol_version: RuneRequestProtocolVersion,
	pub language_surface_version: RuneLanguageSurfaceVersion,
	pub sandbox_contract_version: RuneSandboxContractVersion,
	pub statement_timeout_seconds: Option<u32>,
}

impl ExecuteRuneRequest {
	pub fn current(source: String, statement_timeout_seconds: Option<u32>) -> Self {
		Self {
			source,
			request_protocol_version: RuneRequestProtocolVersion::CURRENT,
			language_surface_version: RuneLanguageSurfaceVersion::CURRENT,
			sandbox_contract_version: RuneSandboxContractVersion::CURRENT,
			statement_timeout_seconds,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuneSourceSpan {
	pub start: u32,
	pub end: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuneVersionAxis {
	RequestProtocol,
	LanguageSurface,
	SandboxContract,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuneVersionSkew {
	pub axis: RuneVersionAxis,
	pub requested: u16,
	pub supported: Vec<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuneCapability {
	Filesystem,
	Process,
	Network,
	Environment,
	Time,
	Random,
	Threading,
	Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuneSandboxViolation {
	FuelExhausted {
		fuel_consumed: u64,
		fuel_budget: u64,
	},
	MemoryExhausted {
		bytes_used: u64,
		bytes_budget: u64,
	},
	CapabilityDenied {
		capability: RuneCapability,
	},
	SourceTooLarge {
		bytes: u64,
		budget: u64,
	},
	Timeout {
		elapsed_millis: u64,
	},
	InvalidEntrypoint,
	InternalSandboxError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuneFailureKind {
	Compile,
	Runtime,
	Sandbox(RuneSandboxViolation),
	VersionSkew(RuneVersionSkew),
	Execution(SqlErrorCode),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuneFailure {
	pub kind: RuneFailureKind,
	pub message: String,
	pub span: Option<RuneSourceSpan>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest {
	pub query_id: QueryId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthyResponse;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveMutationRequest {
	pub operation_query_id: QueryId,
}

/// Whether durable evidence for this operation is still retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationResolution {
	Present,
	Expired,
	Unknown,
}

/// Why the mutation attempt stopped. This axis is deliberately independent of
/// [`MutationCommitKnowledge`]: a deadline can expire before or after commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationStopReason {
	None,
	Deadline,
	NoProgress,
	AttemptsExhausted,
	Other(u8),
}

/// What the durable outcome ledger proves about the mutation's commit state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationCommitKnowledge {
	Unspecified,
	Committed,
	KnownNotCommitted,
	Indeterminate,
	Other(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationOutcomeResponse {
	pub operation_query_id: QueryId,
	pub resolution: MutationResolution,
	/// Wendelon's typed write status, retained as its stable numeric code so a
	/// newer server never has to collapse an unfamiliar status into another one.
	pub write_status: u8,
	pub stop_reason: MutationStopReason,
	pub commit_knowledge: MutationCommitKnowledge,
	/// DELETE reports deleted/already-deleted/not-found. Other mutations use None.
	pub delete_counts: Option<[u32; 3]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasCompositePhase {
	Provisioned,
	RowDispatched,
	RowsCommitted,
	Published,
	RetiringOld,
	Complete,
	Conflict,
	Reclaimed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasTerminalCause {
	PublicationConflict,
	RowKnownNotCommitted,
	DeadlineBeforeRowDispatch,
	OwnerAbandonedBeforeRowDispatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasPriorBinding {
	pub table_object_id: [u8; 16],
	pub schema_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasCatalogCause {
	DeadlineBeforeRowDispatch,
	OwnerAbandonedBeforeRowDispatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasRowCause {
	KnownNotCommitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasPublicationCause {
	BindingConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasRetirementCause {
	RetryDeferred,
}

/// Durable four-phase CTAS outcome: provisioning, rows, publication, retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasOutcomeResponse {
	pub operation_query_id: QueryId,
	pub phase: CtasCompositePhase,
	pub provisional_table_object_id: [u8; 16],
	pub prior_binding: Option<CtasPriorBinding>,
	pub row_count: Option<u64>,
	pub terminal_cause: Option<CtasTerminalCause>,
	pub catalog_commit_knowledge: MutationCommitKnowledge,
	pub catalog_cause: Option<CtasCatalogCause>,
	pub row_commit_knowledge: MutationCommitKnowledge,
	pub row_cause: Option<CtasRowCause>,
	pub publication_commit_knowledge: MutationCommitKnowledge,
	pub publication_cause: Option<CtasPublicationCause>,
	pub retirement_commit_knowledge: MutationCommitKnowledge,
	pub retirement_cause: Option<CtasRetirementCause>,
	pub owner_epoch: u64,
	pub retry_debt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedOperationOutcome {
	Mutation(MutationOutcomeResponse),
	Ctas(CtasOutcomeResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultBatch {
	pub batch_index: u32,
	pub arrow_ipc_bytes: ByteBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Complete {
	pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlError {
	pub code: SqlErrorCode,
	pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlErrorCode {
	SyntaxError,
	PlanningError,
	BindingError,
	CatalogError,
	StorageError,
	TenantMismatch,
	StatementTimeout,
	Cancelled,
	CancellationUnsupported,
	NoLiveWorker,
	ResourceExhausted,
	Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestFrameTooLarge {
	pub declared_len: u32,
	pub max_len: NonZeroU32,
}

pub async fn write_frame<W>(writer: &mut W, frame: &SqlFrame) -> std::io::Result<()>
where
	W: AsyncWrite + Unpin,
{
	let payload =
		postcard::to_allocvec(frame).map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	let len = u32::try_from(payload.len())
		.map_err(|_| Error::new(ErrorKind::InvalidData, "sql frame too large"))?;
	writer.write_all(&len.to_le_bytes()).await?;
	writer.write_all(&payload).await?;
	writer.flush().await
}

pub async fn read_frame<R>(reader: &mut R) -> std::io::Result<SqlFrame>
where
	R: AsyncRead + Unpin,
{
	let mut len_buf = [0u8; 4];
	reader.read_exact(&mut len_buf).await?;
	let len = usize::try_from(u32::from_le_bytes(len_buf)).expect("u32 fits usize");
	let mut payload = vec![0u8; len];
	reader.read_exact(&mut payload).await?;
	postcard::from_bytes(&payload).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

pub async fn read_request_frame_bounded<R>(
	reader: &mut R,
	max_request_frame_bytes: NonZeroU32,
) -> std::io::Result<Result<SqlFrame, RequestFrameTooLarge>>
where
	R: AsyncRead + Unpin,
{
	let mut len_buf = [0u8; 4];
	reader.read_exact(&mut len_buf).await?;
	let declared_len = u32::from_le_bytes(len_buf);
	if declared_len > max_request_frame_bytes.get() {
		return Ok(Err(RequestFrameTooLarge {
			declared_len,
			max_len: max_request_frame_bytes,
		}));
	}
	let len = usize::try_from(declared_len).expect("u32 fits usize");
	let mut payload = vec![0u8; len];
	reader.read_exact(&mut payload).await?;
	postcard::from_bytes(&payload)
		.map(Ok)
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn historical_postcard_discriminants_remain_stable() {
		let query_id = QueryId::from_u128(1);
		let variants = [
			SqlMessage::Execute(ExecuteRequest {
				statement: String::new(),
				statement_timeout_seconds: None,
				session_timezone: String::new(),
			}),
			SqlMessage::Cancel(CancelRequest { query_id }),
			SqlMessage::ResultBatch(ResultBatch {
				batch_index: 0,
				arrow_ipc_bytes: ByteBuf::new(),
			}),
			SqlMessage::Complete(Complete { row_count: 0 }),
			SqlMessage::Error(SqlError {
				code: SqlErrorCode::SyntaxError,
				message: String::new(),
			}),
			SqlMessage::Health(HealthRequest),
			SqlMessage::Healthy(HealthyResponse),
		];
		let discriminants = variants
			.iter()
			.map(|message| postcard::to_allocvec(message).expect("encode")[0])
			.collect::<Vec<_>>();
		assert_eq!(discriminants, [0, 1, 2, 3, 4, 5, 6]);
	}

	#[test]
	fn execute_request_carries_session_timezone() {
		let explicit = SqlFrame::execute_with_timezone(
			QueryId::from_u128(2),
			"SELECT 1".to_string(),
			Some(7),
			"America/Chicago",
		);
		let SqlMessage::Execute(request) = explicit.message else {
			panic!("expected execute request");
		};
		assert_eq!(request.statement_timeout_seconds, Some(7));
		assert_eq!(request.session_timezone, "America/Chicago");
	}

	#[test]
	fn rune_request_round_trips_all_three_versions() {
		let frame = SqlFrame::execute_rune(
			QueryId::from_u128(3),
			"pub fn main(workspace) { workspace.table(\"t\") }".to_owned(),
			Some(9),
		);
		let encoded = postcard::to_allocvec(&frame).expect("encode");
		let decoded: SqlFrame = postcard::from_bytes(&encoded).expect("decode");
		let SqlMessage::ExecuteRune(request) = decoded.message else {
			panic!("expected Rune request");
		};
		assert_eq!(
			request.request_protocol_version,
			RuneRequestProtocolVersion::CURRENT
		);
		assert_eq!(
			request.language_surface_version,
			RuneLanguageSurfaceVersion::CURRENT
		);
		assert_eq!(
			request.sandbox_contract_version,
			RuneSandboxContractVersion::CURRENT
		);
		assert_eq!(request.statement_timeout_seconds, Some(9));
	}

	#[test]
	fn rune_variants_append_after_historical_discriminants() {
		let query_id = QueryId::from_u128(4);
		let request = SqlFrame::execute_rune(query_id, String::new(), None);
		let failure = RuneFailure {
			kind: RuneFailureKind::Compile,
			message: String::new(),
			span: None,
		};
		let request_discriminant = postcard::to_allocvec(&request.message).expect("encode")[0];
		let error_discriminant =
			postcard::to_allocvec(&SqlMessage::RuneError(failure)).expect("encode")[0];
		assert_eq!((request_discriminant, error_discriminant), (7, 8));
	}

	#[test]
	fn mutation_resolution_variants_append_after_rune_without_renumbering_history() {
		let operation_query_id = QueryId::from_u128(5);
		let request = SqlFrame::resolve_mutation(QueryId::from_u128(6), operation_query_id);
		let response = SqlMessage::MutationOutcome(MutationOutcomeResponse {
			operation_query_id,
			resolution: MutationResolution::Present,
			write_status: 19,
			stop_reason: MutationStopReason::Deadline,
			commit_knowledge: MutationCommitKnowledge::Indeterminate,
			delete_counts: None,
		});
		let request_discriminant = postcard::to_allocvec(&request.message).expect("encode")[0];
		let response_discriminant = postcard::to_allocvec(&response).expect("encode")[0];
		assert_eq!((request_discriminant, response_discriminant), (9, 10));

		let encoded = postcard::to_allocvec(&response).expect("encode");
		assert_eq!(
			postcard::from_bytes::<SqlMessage>(&encoded).unwrap(),
			response
		);
	}

	#[test]
	fn adr548_composite_ctas_resolution_round_trips_without_renumbering_history() {
		let operation_query_id = QueryId::from_u128(7);
		let response = SqlMessage::CtasOutcome(CtasOutcomeResponse {
			operation_query_id,
			phase: CtasCompositePhase::RetiringOld,
			provisional_table_object_id: [0x54; 16],
			prior_binding: Some(CtasPriorBinding {
				table_object_id: [0x41; 16],
				schema_generation: 9,
			}),
			row_count: Some(0),
			terminal_cause: None,
			catalog_commit_knowledge: MutationCommitKnowledge::Committed,
			catalog_cause: None,
			row_commit_knowledge: MutationCommitKnowledge::Committed,
			row_cause: None,
			publication_commit_knowledge: MutationCommitKnowledge::Committed,
			publication_cause: None,
			retirement_commit_knowledge: MutationCommitKnowledge::Indeterminate,
			retirement_cause: Some(CtasRetirementCause::RetryDeferred),
			owner_epoch: 3,
			retry_debt: 2,
		});
		assert_eq!(postcard::to_allocvec(&response).expect("encode")[0], 11);
		let encoded = postcard::to_allocvec(&response).expect("encode");
		assert_eq!(
			postcard::from_bytes::<SqlMessage>(&encoded).unwrap(),
			response
		);
	}
}
