use std::io::{Error, ErrorKind};
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::distributed::identity::QueryId;
use crate::wire::{decode_payload, encode_payload};

pub const SQL_REQUEST_FRAME_BYTES_HARD_CEILING: u32 = 64 * 1024 * 1024;
pub const SQL_STATEMENT_BYTES_HARD_CEILING: u32 = 64 * 1024 * 1024;
pub const DEFAULT_SQL_REQUEST_FRAME_BYTES: u32 = 4 * 1024 * 1024;
pub const DEFAULT_SQL_STATEMENT_BYTES: u32 = 4 * 1024 * 1024;

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

	pub fn response(query_id: QueryId, message: SqlMessage) -> Self {
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecuteRequest {
	pub statement: String,
	pub statement_timeout_seconds: Option<u32>,
	pub session_timezone: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest {
	pub query_id: QueryId,
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
	let payload = encode_payload(frame);
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
	decode_payload(&payload).map_err(|error| Error::new(ErrorKind::InvalidData, error))
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
	decode_payload(&payload)
		.map(Ok)
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn execute_request_carries_session_timezone() {
		let default = SqlFrame::execute(QueryId::from_u128(1), "SELECT 1".to_string(), None);
		let SqlMessage::Execute(default_request) = default.message else {
			panic!("expected execute request");
		};
		assert_eq!(default_request.session_timezone, "UTC");

		let explicit = SqlFrame::execute_with_timezone(
			QueryId::from_u128(2),
			"SELECT 1".to_string(),
			Some(7),
			"America/Chicago",
		);
		let SqlMessage::Execute(explicit_request) = explicit.message else {
			panic!("expected execute request");
		};
		assert_eq!(explicit_request.statement, "SELECT 1");
		assert_eq!(explicit_request.statement_timeout_seconds, Some(7));
		assert_eq!(explicit_request.session_timezone, "America/Chicago");
	}
}
