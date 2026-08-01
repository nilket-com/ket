//! Compatibility re-exports for the SQL wire now owned by `toron-client`.
//!
//! TORON-532 moves protocol ownership beside the public client. Existing Toron
//! paths remain available so server internals and downstream tests migrate
//! without a flag day.

pub use toron_client::{
	CancelRequest, Complete, DEFAULT_SQL_REQUEST_FRAME_BYTES, DEFAULT_SQL_STATEMENT_BYTES,
	ExecuteRequest, HealthRequest, HealthyResponse, RequestFrameTooLarge, ResultBatch,
	SQL_REQUEST_FRAME_BYTES_HARD_CEILING, SQL_STATEMENT_BYTES_HARD_CEILING, SqlError, SqlErrorCode,
	SqlFrame, SqlMessage, SqlProtocolVersion, read_frame, read_request_frame_bounded, write_frame,
};
