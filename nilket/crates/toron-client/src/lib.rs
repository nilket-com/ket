//! Bounded public client for Toron's SQL service.
//!
//! This crate owns the client-facing protocol and transport behavior. It is
//! intentionally free of Arrow interpretation, analytics engines, editor
//! crates, and Toron server internals (TORON-532).

#![deny(unsafe_code)]

mod client;
mod protocol;

pub use client::{
	ClientError, ClientOperation, QueryResult, ToronClient, ToronClientTls,
	blocking_health_loopback,
};
pub use protocol::{
	CancelRequest, Complete, DEFAULT_SQL_REQUEST_FRAME_BYTES, DEFAULT_SQL_STATEMENT_BYTES,
	ExecuteRequest, ExecuteRuneRequest, HealthRequest, HealthyResponse, QueryId,
	RequestFrameTooLarge, ResultBatch, RuneCapability, RuneFailure, RuneFailureKind,
	RuneLanguageSurfaceVersion, RuneRequestProtocolVersion, RuneSandboxContractVersion,
	RuneSandboxViolation, RuneSourceSpan, RuneVersionAxis, RuneVersionSkew,
	SQL_REQUEST_FRAME_BYTES_HARD_CEILING, SQL_STATEMENT_BYTES_HARD_CEILING, SqlError, SqlErrorCode,
	SqlFrame, SqlMessage, SqlProtocolVersion, read_frame, read_request_frame_bounded, write_frame,
};
