use quinn::crypto::rustls::QuicClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

const LOOPBACK_QUIC_ALPN: &[u8] = b"sql-task-loopback";
const LOOPBACK_QUIC_SERVER_NAME: &str = "sql-task-loopback.local";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const QUERY_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_REQUEST_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESULT_BYTES: usize = 128 * 1024 * 1024;
const MAX_RESPONSE_FRAME_BYTES: usize = MAX_RESULT_BYTES + 1024 * 1024;
static INSTALL_RUSTLS_PROVIDER: Once = Once::new();

#[derive(Clone)]
pub(super) struct ToronClient {
	endpoint: quinn::Endpoint,
	remote: SocketAddr,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct QueryResult {
	pub batches: Vec<Vec<u8>>,
	pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClientError {
	Configuration(String),
	Transport(String),
	Protocol(String),
	Remote { code: SqlErrorCode, message: String },
	TimedOut,
}

impl fmt::Display for ClientError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Configuration(message) => write!(formatter, "Toron configuration: {message}"),
			Self::Transport(message) => write!(formatter, "Toron transport: {message}"),
			Self::Protocol(message) => write!(formatter, "Toron protocol: {message}"),
			Self::Remote { code, message } => write!(formatter, "Toron {code:?}: {message}"),
			Self::TimedOut => write!(formatter, "Toron query timed out"),
		}
	}
}

impl std::error::Error for ClientError {}

impl ToronClient {
	pub async fn connect(endpoint: &str) -> Result<Self, ClientError> {
		let remote: SocketAddr = endpoint.parse().map_err(|error| {
			ClientError::Configuration(format!("invalid endpoint {endpoint:?}: {error}"))
		})?;
		if !remote.ip().is_loopback() {
			return Err(ClientError::Configuration(
				"first-contact support is loopback-only; remote mTLS is deferred".to_owned(),
			));
		}
		let mut endpoint =
			quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
				.map_err(|error| ClientError::Transport(error.to_string()))?;
		endpoint.set_default_client_config(loopback_client_config()?);
		let client = Self { endpoint, remote };
		let connection = tokio::time::timeout(CONNECT_TIMEOUT, client.open_connection())
			.await
			.map_err(|_| ClientError::TimedOut)??;
		connection.close(0_u32.into(), b"connection probe complete");
		Ok(client)
	}

	pub async fn execute(&self, statement: &str) -> Result<QueryResult, ClientError> {
		let query_id = QueryId::new();
		let frame = SqlFrame::execute(query_id, statement.to_owned(), None);
		tokio::time::timeout(QUERY_TIMEOUT, self.exchange(frame, ExpectedResponse::Query))
			.await
			.map_err(|_| ClientError::TimedOut)?
	}

	#[allow(dead_code)] // Exercised directly until the adapter boundary can carry cancellation.
	pub async fn cancel(&self, query_id: QueryId) -> Result<(), ClientError> {
		let frame = SqlFrame::cancel(query_id);
		let result = tokio::time::timeout(
			CONNECT_TIMEOUT,
			self.exchange(frame, ExpectedResponse::Cancellation),
		)
		.await
		.map_err(|_| ClientError::TimedOut)??;
		if !result.batches.is_empty() || result.row_count != 0 {
			return Err(ClientError::Protocol(
				"cancellation returned query data".to_owned(),
			));
		}
		Ok(())
	}

	async fn open_connection(&self) -> Result<quinn::Connection, ClientError> {
		self.endpoint
			.connect(self.remote, LOOPBACK_QUIC_SERVER_NAME)
			.map_err(|error| ClientError::Transport(error.to_string()))?
			.await
			.map_err(|error| ClientError::Transport(error.to_string()))
	}

	async fn exchange(
		&self,
		request: SqlFrame,
		expected: ExpectedResponse,
	) -> Result<QueryResult, ClientError> {
		let connection = self.open_connection().await?;
		let (mut send, mut recv) = connection
			.open_bi()
			.await
			.map_err(|error| ClientError::Transport(error.to_string()))?;
		write_frame(&mut send, &request).await?;
		send.finish()
			.map_err(|error| ClientError::Transport(error.to_string()))?;
		let result = collect_response(&mut recv, request.query_id, expected).await;
		// Toron writes a terminal frame and then finishes its send half. Let that
		// FIN arrive before closing the connection; otherwise a successful query
		// can look like a lost worker connection in Toron's logs.
		let _ = tokio::time::timeout(Duration::from_millis(250), recv.read_to_end(0)).await;
		connection.close(0_u32.into(), b"request complete");
		result
	}
}

#[derive(Clone, Copy)]
enum ExpectedResponse {
	Query,
	Cancellation,
}

async fn collect_response<R>(
	reader: &mut R,
	query_id: QueryId,
	expected: ExpectedResponse,
) -> Result<QueryResult, ClientError>
where
	R: AsyncRead + Unpin,
{
	collect_response_bounded(reader, query_id, expected, MAX_RESULT_BYTES).await
}

async fn collect_response_bounded<R>(
	reader: &mut R,
	query_id: QueryId,
	expected: ExpectedResponse,
	max_result_bytes: usize,
) -> Result<QueryResult, ClientError>
where
	R: AsyncRead + Unpin,
{
	let mut batches = Vec::new();
	let mut batch_bytes = 0usize;
	let mut expected_batch = 0u32;
	loop {
		let frame = read_frame(reader).await?;
		if frame.protocol_version != SqlProtocolVersion::Current {
			return Err(ClientError::Protocol(
				"unsupported SQL protocol version".to_owned(),
			));
		}
		if frame.query_id != query_id {
			return Err(ClientError::Protocol(format!(
				"response query id {} did not match request {}",
				frame.query_id.get(),
				query_id.get()
			)));
		}
		match frame.message {
			SqlMessage::ResultBatch(batch) if matches!(expected, ExpectedResponse::Query) => {
				if batch.batch_index != expected_batch {
					return Err(ClientError::Protocol(format!(
						"result batch {} arrived while expecting {}",
						batch.batch_index, expected_batch
					)));
				}
				expected_batch = expected_batch.checked_add(1).ok_or_else(|| {
					ClientError::Protocol("result batch index overflow".to_owned())
				})?;
				batch_bytes = batch_bytes
					.checked_add(batch.arrow_ipc_bytes.len())
					.ok_or_else(|| ClientError::Protocol("result size overflow".to_owned()))?;
				if batch_bytes > max_result_bytes {
					return Err(ClientError::Protocol(format!(
						"result exceeded the {} byte first-contact limit",
						max_result_bytes
					)));
				}
				batches.push(batch.arrow_ipc_bytes.into_vec());
			}
			SqlMessage::Complete(complete) if matches!(expected, ExpectedResponse::Query) => {
				return Ok(QueryResult {
					batches,
					row_count: complete.row_count,
				});
			}
			SqlMessage::Error(error)
				if matches!(expected, ExpectedResponse::Cancellation)
					&& error.code == SqlErrorCode::Cancelled =>
			{
				return Ok(QueryResult {
					batches: Vec::new(),
					row_count: 0,
				});
			}
			SqlMessage::Error(error) => {
				return Err(ClientError::Remote {
					code: error.code,
					message: error.message,
				});
			}
			other => {
				return Err(ClientError::Protocol(format!(
					"unexpected response message: {other:?}"
				)));
			}
		}
	}
}

async fn write_frame<W>(writer: &mut W, frame: &SqlFrame) -> Result<(), ClientError>
where
	W: AsyncWrite + Unpin,
{
	let payload =
		postcard::to_stdvec(frame).map_err(|error| ClientError::Protocol(error.to_string()))?;
	if payload.len() > MAX_REQUEST_FRAME_BYTES {
		return Err(ClientError::Protocol(format!(
			"request exceeded the {} MiB first-contact limit",
			MAX_REQUEST_FRAME_BYTES / (1024 * 1024)
		)));
	}
	let len = u32::try_from(payload.len())
		.map_err(|_| ClientError::Protocol("request frame exceeded u32".to_owned()))?;
	writer
		.write_all(&len.to_le_bytes())
		.await
		.map_err(|error| ClientError::Transport(error.to_string()))?;
	writer
		.write_all(&payload)
		.await
		.map_err(|error| ClientError::Transport(error.to_string()))?;
	writer
		.flush()
		.await
		.map_err(|error| ClientError::Transport(error.to_string()))
}

async fn read_frame<R>(reader: &mut R) -> Result<SqlFrame, ClientError>
where
	R: AsyncRead + Unpin,
{
	let mut len = [0_u8; 4];
	reader
		.read_exact(&mut len)
		.await
		.map_err(|error| ClientError::Transport(error.to_string()))?;
	let len = usize::try_from(u32::from_le_bytes(len)).expect("u32 fits usize");
	if len > MAX_RESPONSE_FRAME_BYTES {
		return Err(ClientError::Protocol(format!(
			"response frame exceeded the {} MiB limit",
			MAX_RESPONSE_FRAME_BYTES / (1024 * 1024)
		)));
	}
	let mut payload = vec![0_u8; len];
	reader
		.read_exact(&mut payload)
		.await
		.map_err(|error| ClientError::Transport(error.to_string()))?;
	postcard::from_bytes(&payload).map_err(|error| ClientError::Protocol(error.to_string()))
}

fn loopback_client_config() -> Result<quinn::ClientConfig, ClientError> {
	INSTALL_RUSTLS_PROVIDER.call_once(|| {
		let _ = ring::default_provider().install_default();
	});
	let mut rustls_config = ClientConfig::builder()
		.dangerous()
		.with_custom_certificate_verifier(Arc::new(LoopbackServerVerifier))
		.with_no_client_auth();
	rustls_config.alpn_protocols = vec![LOOPBACK_QUIC_ALPN.to_vec()];
	let quic_config = QuicClientConfig::try_from(rustls_config)
		.map_err(|error| ClientError::Configuration(format!("QUIC client config: {error}")))?;
	Ok(quinn::ClientConfig::new(Arc::new(quic_config)))
}

#[derive(Debug)]
struct LoopbackServerVerifier;

impl ServerCertVerifier for LoopbackServerVerifier {
	fn verify_server_cert(
		&self,
		_end_entity: &CertificateDer<'_>,
		_intermediates: &[CertificateDer<'_>],
		_server_name: &ServerName<'_>,
		_ocsp_response: &[u8],
		_now: UnixTime,
	) -> Result<ServerCertVerified, RustlsError> {
		Ok(ServerCertVerified::assertion())
	}

	fn verify_tls12_signature(
		&self,
		_message: &[u8],
		_cert: &CertificateDer<'_>,
		_dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, RustlsError> {
		Ok(HandshakeSignatureValid::assertion())
	}

	fn verify_tls13_signature(
		&self,
		_message: &[u8],
		_cert: &CertificateDer<'_>,
		_dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, RustlsError> {
		Ok(HandshakeSignatureValid::assertion())
	}

	fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
		vec![
			SignatureScheme::ECDSA_NISTP256_SHA256,
			SignatureScheme::ECDSA_NISTP384_SHA384,
			SignatureScheme::ED25519,
			SignatureScheme::RSA_PSS_SHA256,
			SignatureScheme::RSA_PSS_SHA384,
			SignatureScheme::RSA_PSS_SHA512,
			SignatureScheme::RSA_PKCS1_SHA256,
			SignatureScheme::RSA_PKCS1_SHA384,
			SignatureScheme::RSA_PKCS1_SHA512,
		]
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct QueryId(Uuid);

impl QueryId {
	fn new() -> Self {
		Self(Uuid::now_v7())
	}

	#[cfg(test)]
	pub(super) const fn from_u128(value: u128) -> Self {
		Self(Uuid::from_u128(value))
	}

	fn get(self) -> Uuid {
		self.0
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SqlFrame {
	protocol_version: SqlProtocolVersion,
	query_id: QueryId,
	message: SqlMessage,
}

impl SqlFrame {
	fn execute(query_id: QueryId, statement: String, timeout_seconds: Option<u32>) -> Self {
		Self {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message: SqlMessage::Execute(ExecuteRequest {
				statement,
				statement_timeout_seconds: timeout_seconds,
				session_timezone: "UTC".to_owned(),
			}),
		}
	}

	fn cancel(query_id: QueryId) -> Self {
		Self {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message: SqlMessage::Cancel(CancelRequest { query_id }),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum SqlProtocolVersion {
	Current,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum SqlMessage {
	Execute(ExecuteRequest),
	Cancel(CancelRequest),
	ResultBatch(ResultBatch),
	Complete(Complete),
	Error(SqlError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ExecuteRequest {
	statement: String,
	statement_timeout_seconds: Option<u32>,
	session_timezone: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct CancelRequest {
	query_id: QueryId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ResultBatch {
	batch_index: u32,
	arrow_ipc_bytes: ByteBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Complete {
	row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SqlError {
	code: SqlErrorCode,
	message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SqlErrorCode {
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

#[cfg(test)]
mod tests {
	use super::*;

	async fn framed_bytes(frames: &[SqlFrame]) -> tokio::io::DuplexStream {
		let (mut writer, reader) = tokio::io::duplex(4096);
		for frame in frames {
			write_frame(&mut writer, frame).await.expect("write frame");
		}
		drop(writer);
		reader
	}

	#[tokio::test]
	async fn execute_wire_shape_round_trips() {
		let id = QueryId::from_u128(7);
		let frame = SqlFrame::execute(id, "SELECT 1".to_owned(), Some(9));
		let mut reader = framed_bytes(std::slice::from_ref(&frame)).await;
		assert_eq!(read_frame(&mut reader).await.expect("read"), frame);
	}

	#[tokio::test]
	async fn cancellation_wire_shape_round_trips() {
		let id = QueryId::from_u128(8);
		let frame = SqlFrame::cancel(id);
		let mut reader = framed_bytes(std::slice::from_ref(&frame)).await;
		assert_eq!(read_frame(&mut reader).await.expect("read"), frame);
	}

	#[tokio::test]
	async fn collector_preserves_remote_error_code_and_message() {
		let id = QueryId::from_u128(9);
		let error = SqlFrame {
			protocol_version: SqlProtocolVersion::Current,
			query_id: id,
			message: SqlMessage::Error(SqlError {
				code: SqlErrorCode::SyntaxError,
				message: "bad syntax".to_owned(),
			}),
		};
		let mut reader = framed_bytes(&[error]).await;
		assert_eq!(
			collect_response(&mut reader, id, ExpectedResponse::Query).await,
			Err(ClientError::Remote {
				code: SqlErrorCode::SyntaxError,
				message: "bad syntax".to_owned(),
			})
		);
	}

	#[tokio::test]
	async fn collector_rejects_out_of_order_batches() {
		let id = QueryId::from_u128(10);
		let batch = SqlFrame {
			protocol_version: SqlProtocolVersion::Current,
			query_id: id,
			message: SqlMessage::ResultBatch(ResultBatch {
				batch_index: 1,
				arrow_ipc_bytes: ByteBuf::from(vec![1, 2, 3]),
			}),
		};
		let mut reader = framed_bytes(&[batch]).await;
		let error = collect_response(&mut reader, id, ExpectedResponse::Query)
			.await
			.expect_err("out of order");
		assert!(error.to_string().contains("expecting 0"));
	}

	#[tokio::test]
	async fn collector_refuses_results_before_crossing_its_memory_bound() {
		let id = QueryId::from_u128(11);
		let batch = SqlFrame {
			protocol_version: SqlProtocolVersion::Current,
			query_id: id,
			message: SqlMessage::ResultBatch(ResultBatch {
				batch_index: 0,
				arrow_ipc_bytes: ByteBuf::from(vec![1, 2, 3]),
			}),
		};
		let mut reader = framed_bytes(&[batch]).await;
		let error = collect_response_bounded(&mut reader, id, ExpectedResponse::Query, 2)
			.await
			.expect_err("bounded before retaining the batch");
		assert!(error.to_string().contains("2 byte"));
	}

	#[tokio::test]
	async fn remote_connections_are_honestly_refused() {
		let Err(error) = ToronClient::connect("192.0.2.1:7742").await else {
			panic!("remote mTLS is deferred");
		};
		assert!(error.to_string().contains("loopback-only"));
	}
}
