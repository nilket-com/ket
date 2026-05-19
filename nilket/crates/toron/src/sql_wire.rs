use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::distributed::identity::QueryId;
use crate::wire::{decode_payload, encode_payload};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlFrame {
	pub protocol_version: SqlProtocolVersion,
	pub query_id: QueryId,
	pub message: SqlMessage,
}

impl SqlFrame {
	pub fn execute(query_id: QueryId, statement: String, timeout_seconds: Option<u32>) -> Self {
		Self {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message: SqlMessage::Execute(ExecuteRequest {
				statement,
				statement_timeout_seconds: timeout_seconds,
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
	Internal,
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
