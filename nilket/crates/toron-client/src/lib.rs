//! Bounded public client for Toron's SQL service.
//!
//! This crate owns the client-facing protocol and transport behavior. It is
//! intentionally free of Arrow interpretation, analytics engines, editor
//! crates, and Toron server internals (TORON-532).

#![deny(unsafe_code)]

mod client;
mod liveness;
mod protocol;
mod temporal_interval;

pub use client::{
	ClientError, ClientOperation, QueryResult, RuneQueryResult, ToronClient, ToronClientTls,
	blocking_health_loopback,
};
pub use liveness::{
	TORON_QUIC_KEEP_ALIVE_INTERVAL, TORON_QUIC_MAX_IDLE_TIMEOUT, apply_toron_quic_liveness_client,
	apply_toron_quic_liveness_server, toron_quic_transport_config,
};
pub use protocol::{
	CancelRequest, Complete, CtasCatalogCause, CtasCompositePhase, CtasOutcomeResponse,
	CtasPriorBinding, CtasPublicationCause, CtasRetirementCause, CtasRowCause, CtasTerminalCause,
	DEFAULT_SQL_REQUEST_FRAME_BYTES, DEFAULT_SQL_STATEMENT_BYTES, ExecuteRequest,
	ExecuteRuneRequest, HealthRequest, HealthyResponse, MaterializeOutcomeResponse,
	MutationCommitKnowledge, MutationOutcomeResponse, MutationResolution, MutationStopReason,
	QueryId, RequestFrameTooLarge, ResolveMutationRequest, ResolvedOperationOutcome, ResultBatch,
	RuneCapability, RuneFailure, RuneFailureKind, RuneLanguageSurfaceVersion,
	RuneRequestProtocolVersion, RuneSandboxContractVersion, RuneSandboxViolation, RuneSourceSpan,
	RuneVersionAxis, RuneVersionSkew, SQL_REQUEST_FRAME_BYTES_HARD_CEILING,
	SQL_STATEMENT_BYTES_HARD_CEILING, SqlError, SqlErrorCode, SqlFrame, SqlMessage,
	SqlProtocolVersion, read_frame, read_request_frame_bounded, write_frame,
};
pub use temporal_interval::{TemporalInterval, TemporalIntervalError};
