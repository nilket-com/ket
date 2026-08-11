use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Once};
use std::time::Duration;

use quinn::crypto::rustls::QuicClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use crate::protocol::{
	ExecuteRuneRequest, HealthyResponse, MutationOutcomeResponse, QueryId, ResultBatch,
	RuneFailure, SqlErrorCode, SqlFrame, SqlMessage, SqlProtocolVersion,
};

const LOOPBACK_QUIC_ALPN: &[u8] = b"sql-task-loopback";
const AUTH_QUIC_ALPN: &[u8] = b"sql-task-auth";
const LOOPBACK_QUIC_SERVER_NAME: &str = "sql-task-loopback.local";
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_REQUEST_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESULT_BYTES: usize = 128 * 1024 * 1024;
const MAX_RESPONSE_FRAME_BYTES: usize = MAX_RESULT_BYTES + 1024 * 1024;
static INSTALL_RUSTLS_PROVIDER: Once = Once::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToronClientTls {
	pub server_name: String,
	pub cert_path: PathBuf,
	pub key_path: PathBuf,
	pub ca_path: PathBuf,
}

#[derive(Clone)]
pub struct ToronClient {
	endpoint: quinn::Endpoint,
	remote: SocketAddr,
	server_name: String,
	connect_timeout: Duration,
	health_timeout: Duration,
	query_timeout: Duration,
}

#[derive(Debug, PartialEq, Eq)]
pub struct QueryResult {
	pub batches: Vec<Vec<u8>>,
	pub row_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientOperation {
	Connect,
	Health,
	Query,
	RuneQuery,
	Cancellation,
	MutationResolution,
}

impl fmt::Display for ClientOperation {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::Connect => "connection",
			Self::Health => "health probe",
			Self::Query => "query",
			Self::RuneQuery => "Rune query",
			Self::Cancellation => "cancellation",
			Self::MutationResolution => "mutation resolution",
		})
	}
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ClientError {
	#[error("Toron configuration: {0}")]
	Configuration(String),
	#[error("Toron transport: {0}")]
	Transport(String),
	#[error("Toron protocol: {0}")]
	Protocol(String),
	#[error("Toron {code:?}: {message}")]
	Remote { code: SqlErrorCode, message: String },
	#[error("Toron Rune {0:?}")]
	Rune(RuneFailure),
	#[error("Toron {0} timed out")]
	TimedOut(ClientOperation),
}

impl ToronClient {
	pub fn loopback(endpoint: impl AsRef<str>) -> Result<Self, ClientError> {
		let remote = parse_endpoint(endpoint.as_ref())?;
		if !remote.ip().is_loopback() {
			return Err(ClientError::Configuration(
				"loopback client requires a loopback endpoint; use authenticated mTLS remotely"
					.to_owned(),
			));
		}
		let mut endpoint =
			quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
				.map_err(|error| ClientError::Transport(error.to_string()))?;
		endpoint.set_default_client_config(loopback_client_config()?);
		Ok(Self::new(
			endpoint,
			remote,
			LOOPBACK_QUIC_SERVER_NAME.to_owned(),
		))
	}

	pub fn authenticated(
		endpoint: impl AsRef<str>,
		tls: &ToronClientTls,
	) -> Result<Self, ClientError> {
		let remote = parse_endpoint(endpoint.as_ref())?;
		let material = tls_pki::TlsMaterial::load(&tls.cert_path, &tls.key_path, &tls.ca_path)
			.map_err(|error| ClientError::Configuration(error.to_string()))?;
		let verifier = tls_pki::build_server_cert_chain_verifier(material.ca_roots.clone())
			.map_err(|error| ClientError::Configuration(error.to_string()))?;
		let rustls = tls_pki::build_rustls_client_config(&material, verifier, AUTH_QUIC_ALPN)
			.map_err(|error| ClientError::Configuration(error.to_string()))?;
		let quic = QuicClientConfig::try_from(rustls)
			.map_err(|error| ClientError::Configuration(format!("QUIC client config: {error}")))?;
		let mut endpoint =
			quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
				.map_err(|error| ClientError::Transport(error.to_string()))?;
		endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic)));
		Ok(Self::new(endpoint, remote, tls.server_name.clone()))
	}

	fn new(endpoint: quinn::Endpoint, remote: SocketAddr, server_name: String) -> Self {
		Self {
			endpoint,
			remote,
			server_name,
			connect_timeout: DEFAULT_CONNECT_TIMEOUT,
			health_timeout: DEFAULT_HEALTH_TIMEOUT,
			query_timeout: DEFAULT_QUERY_TIMEOUT,
		}
	}

	pub async fn health(&self) -> Result<(), ClientError> {
		let query_id = QueryId::new();
		let response = self
			.exchange(
				SqlFrame::health(query_id),
				ExpectedResponse::Health,
				self.health_timeout,
				ClientOperation::Health,
			)
			.await?;
		if !response.batches.is_empty() || response.row_count != 0 {
			return Err(ClientError::Protocol(
				"health returned query data".to_owned(),
			));
		}
		Ok(())
	}

	pub async fn execute(&self, statement: &str) -> Result<QueryResult, ClientError> {
		self.execute_with_timezone(statement, "UTC").await
	}

	pub async fn execute_with_timezone(
		&self,
		statement: &str,
		session_timezone: &str,
	) -> Result<QueryResult, ClientError> {
		self.execute_with_query_id(QueryId::new(), statement, session_timezone)
			.await
	}

	/// Execute with an explicit caller-retained identity. Retrying a mutation after
	/// reconnect or process restart reuses this id; ADR 542 therefore never relies
	/// on a server-minted identity to distinguish replay from a new statement.
	pub async fn execute_with_query_id(
		&self,
		query_id: QueryId,
		statement: &str,
		session_timezone: &str,
	) -> Result<QueryResult, ClientError> {
		self.exchange(
			SqlFrame::execute_with_timezone(query_id, statement.to_owned(), None, session_timezone),
			ExpectedResponse::Query,
			self.query_timeout,
			ClientOperation::Query,
		)
		.await
	}

	pub async fn execute_rune(&self, source: &str) -> Result<QueryResult, ClientError> {
		self.execute_rune_request(ExecuteRuneRequest::current(source.to_owned(), None))
			.await
	}

	/// Executes source against an explicitly named Rune compatibility
	/// contract. The server may reject version skew, but negotiation never
	/// weakens its sandbox policy.
	pub async fn execute_rune_request(
		&self,
		request: ExecuteRuneRequest,
	) -> Result<QueryResult, ClientError> {
		self.execute_rune_request_with_query_id(QueryId::new(), request)
			.await
	}

	pub async fn execute_rune_request_with_query_id(
		&self,
		query_id: QueryId,
		request: ExecuteRuneRequest,
	) -> Result<QueryResult, ClientError> {
		self.exchange(
			SqlFrame::execute_rune_request(query_id, request),
			ExpectedResponse::RuneQuery,
			self.query_timeout,
			ClientOperation::RuneQuery,
		)
		.await
	}

	pub async fn cancel(&self, query_id: QueryId) -> Result<(), ClientError> {
		let result = self
			.exchange(
				SqlFrame::cancel(query_id),
				ExpectedResponse::Cancellation,
				self.health_timeout,
				ClientOperation::Cancellation,
			)
			.await?;
		if !result.batches.is_empty() || result.row_count != 0 {
			return Err(ClientError::Protocol(
				"cancellation returned query data".to_owned(),
			));
		}
		Ok(())
	}

	/// Resolve the durable outcome named by an earlier SQL or Rune query id
	/// without re-submitting that mutation's statement or payload.
	pub async fn resolve_mutation(
		&self,
		operation_query_id: QueryId,
	) -> Result<MutationOutcomeResponse, ClientError> {
		let request_id = QueryId::new();
		let request = SqlFrame::resolve_mutation(request_id, operation_query_id);
		let connection = self.open_connection().await?;
		let exchange = async {
			let (mut send, mut recv) = connection
				.open_bi()
				.await
				.map_err(|error| ClientError::Transport(error.to_string()))?;
			write_client_frame(&mut send, &request).await?;
			send.finish()
				.map_err(|error| ClientError::Transport(error.to_string()))?;
			let result = collect_mutation_outcome(&mut recv, request_id, operation_query_id).await;
			let _ = tokio::time::timeout(Duration::from_millis(250), recv.read_to_end(0)).await;
			result
		};
		let result = tokio::time::timeout(self.query_timeout, exchange)
			.await
			.map_err(|_| ClientError::TimedOut(ClientOperation::MutationResolution))?;
		connection.close(0_u32.into(), b"request complete");
		result
	}

	async fn open_connection(&self) -> Result<quinn::Connection, ClientError> {
		let connecting = self
			.endpoint
			.connect(self.remote, &self.server_name)
			.map_err(|error| ClientError::Transport(error.to_string()))?;
		tokio::time::timeout(self.connect_timeout, connecting)
			.await
			.map_err(|_| ClientError::TimedOut(ClientOperation::Connect))?
			.map_err(|error| ClientError::Transport(error.to_string()))
	}

	async fn exchange(
		&self,
		request: SqlFrame,
		expected: ExpectedResponse,
		response_timeout: Duration,
		operation: ClientOperation,
	) -> Result<QueryResult, ClientError> {
		let connection = self.open_connection().await?;
		let exchange = async {
			let (mut send, mut recv) = connection
				.open_bi()
				.await
				.map_err(|error| ClientError::Transport(error.to_string()))?;
			write_client_frame(&mut send, &request).await?;
			send.finish()
				.map_err(|error| ClientError::Transport(error.to_string()))?;
			let result = collect_response(&mut recv, request.query_id, expected).await;
			let _ = tokio::time::timeout(Duration::from_millis(250), recv.read_to_end(0)).await;
			result
		};
		let result = tokio::time::timeout(response_timeout, exchange)
			.await
			.map_err(|_| ClientError::TimedOut(operation))?;
		connection.close(0_u32.into(), b"request complete");
		result
	}
}

pub fn blocking_health_loopback(endpoint: &str) -> Result<(), ClientError> {
	let endpoint = endpoint.to_owned();
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|error| ClientError::Configuration(format!("health runtime: {error}")))?;
	runtime.block_on(async move {
		let client = ToronClient::loopback(endpoint)?;
		client.health().await
	})
}

#[derive(Clone, Copy)]
enum ExpectedResponse {
	Query,
	RuneQuery,
	Cancellation,
	Health,
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
		let frame = read_client_frame(reader).await?;
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
			SqlMessage::ResultBatch(ResultBatch {
				batch_index,
				arrow_ipc_bytes,
			}) if matches!(
				expected,
				ExpectedResponse::Query | ExpectedResponse::RuneQuery
			) =>
			{
				if batch_index != expected_batch {
					return Err(ClientError::Protocol(format!(
						"result batch {batch_index} arrived while expecting {expected_batch}"
					)));
				}
				expected_batch = expected_batch.checked_add(1).ok_or_else(|| {
					ClientError::Protocol("result batch index overflow".to_owned())
				})?;
				batch_bytes = batch_bytes
					.checked_add(arrow_ipc_bytes.len())
					.ok_or_else(|| ClientError::Protocol("result size overflow".to_owned()))?;
				if batch_bytes > max_result_bytes {
					return Err(ClientError::Protocol(format!(
						"result exceeded the {max_result_bytes} byte limit"
					)));
				}
				batches.push(arrow_ipc_bytes.into_vec());
			}
			SqlMessage::Complete(complete)
				if matches!(
					expected,
					ExpectedResponse::Query | ExpectedResponse::RuneQuery
				) =>
			{
				return Ok(QueryResult {
					batches,
					row_count: complete.row_count,
				});
			}
			SqlMessage::Healthy(HealthyResponse)
				if matches!(expected, ExpectedResponse::Health) =>
			{
				return Ok(QueryResult {
					batches: Vec::new(),
					row_count: 0,
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
			SqlMessage::RuneError(error) if matches!(expected, ExpectedResponse::RuneQuery) => {
				return Err(ClientError::Rune(error));
			}
			other => {
				return Err(ClientError::Protocol(format!(
					"unexpected response message: {other:?}"
				)));
			}
		}
	}
}

async fn collect_mutation_outcome<R>(
	reader: &mut R,
	request_id: QueryId,
	operation_query_id: QueryId,
) -> Result<MutationOutcomeResponse, ClientError>
where
	R: AsyncRead + Unpin,
{
	let frame = read_client_frame(reader).await?;
	if frame.protocol_version != SqlProtocolVersion::Current {
		return Err(ClientError::Protocol(
			"unsupported SQL protocol version".to_owned(),
		));
	}
	if frame.query_id != request_id {
		return Err(ClientError::Protocol(format!(
			"response query id {} did not match request {}",
			frame.query_id.get(),
			request_id.get()
		)));
	}
	match frame.message {
		SqlMessage::MutationOutcome(outcome)
			if outcome.operation_query_id == operation_query_id =>
		{
			Ok(outcome)
		}
		SqlMessage::MutationOutcome(outcome) => Err(ClientError::Protocol(format!(
			"mutation outcome id {} did not match requested operation {}",
			outcome.operation_query_id.get(),
			operation_query_id.get()
		))),
		SqlMessage::Error(error) => Err(ClientError::Remote {
			code: error.code,
			message: error.message,
		}),
		other => Err(ClientError::Protocol(format!(
			"unexpected mutation resolution response: {other:?}"
		))),
	}
}

async fn write_client_frame<W>(writer: &mut W, frame: &SqlFrame) -> Result<(), ClientError>
where
	W: tokio::io::AsyncWrite + Unpin,
{
	let payload =
		postcard::to_allocvec(frame).map_err(|error| ClientError::Protocol(error.to_string()))?;
	if payload.len() > MAX_REQUEST_FRAME_BYTES {
		return Err(ClientError::Protocol(format!(
			"request exceeded the {} MiB limit",
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

async fn read_client_frame<R>(reader: &mut R) -> Result<SqlFrame, ClientError>
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

fn parse_endpoint(endpoint: &str) -> Result<SocketAddr, ClientError> {
	endpoint.parse().map_err(|error| {
		ClientError::Configuration(format!("invalid endpoint {endpoint:?}: {error}"))
	})
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::protocol::{
		CancelRequest, Complete, HealthRequest, MutationCommitKnowledge, MutationResolution,
		MutationStopReason, RuneFailure, RuneFailureKind, RuneSourceSpan, SqlError, write_frame,
	};

	async fn framed_bytes(frames: &[SqlFrame]) -> tokio::io::DuplexStream {
		let (mut writer, reader) = tokio::io::duplex(4096);
		for frame in frames {
			write_frame(&mut writer, frame).await.expect("write frame");
		}
		drop(writer);
		reader
	}

	#[tokio::test]
	async fn collector_preserves_remote_error_code_and_message() {
		let id = QueryId::from_u128(9);
		let error = SqlFrame::response(
			id,
			SqlMessage::Error(SqlError {
				code: SqlErrorCode::SyntaxError,
				message: "bad syntax".to_owned(),
			}),
		);
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
	async fn mutation_collector_preserves_both_outcome_axes_and_checks_identity() {
		let request_id = QueryId::from_u128(10);
		let operation_id = QueryId::from_u128(11);
		let outcome = MutationOutcomeResponse {
			operation_query_id: operation_id,
			resolution: MutationResolution::Present,
			write_status: 19,
			stop_reason: MutationStopReason::Deadline,
			commit_knowledge: MutationCommitKnowledge::Indeterminate,
			delete_counts: Some([3, 2, 1]),
		};
		let mut reader = framed_bytes(&[SqlFrame::response(
			request_id,
			SqlMessage::MutationOutcome(outcome),
		)])
		.await;
		assert_eq!(
			collect_mutation_outcome(&mut reader, request_id, operation_id).await,
			Ok(outcome)
		);

		let mut mismatched = framed_bytes(&[SqlFrame::response(
			request_id,
			SqlMessage::MutationOutcome(MutationOutcomeResponse {
				operation_query_id: QueryId::from_u128(12),
				..outcome
			}),
		)])
		.await;
		assert!(
			collect_mutation_outcome(&mut mismatched, request_id, operation_id)
				.await
				.unwrap_err()
				.to_string()
				.contains("did not match requested operation")
		);
	}

	#[tokio::test]
	async fn collector_preserves_typed_rune_failure_and_span() {
		let id = QueryId::from_u128(91);
		let failure = RuneFailure {
			kind: RuneFailureKind::Compile,
			message: "bad Rune".to_owned(),
			span: Some(RuneSourceSpan { start: 7, end: 11 }),
		};
		let mut reader = framed_bytes(&[SqlFrame::response(
			id,
			SqlMessage::RuneError(failure.clone()),
		)])
		.await;
		assert_eq!(
			collect_response(&mut reader, id, ExpectedResponse::RuneQuery).await,
			Err(ClientError::Rune(failure))
		);
	}

	#[tokio::test]
	async fn sql_collector_rejects_a_rune_only_failure_frame() {
		let id = QueryId::from_u128(93);
		let failure = RuneFailure {
			kind: RuneFailureKind::Compile,
			message: "wrong frontend".to_owned(),
			span: None,
		};
		let mut reader =
			framed_bytes(&[SqlFrame::response(id, SqlMessage::RuneError(failure))]).await;
		let error = collect_response(&mut reader, id, ExpectedResponse::Query)
			.await
			.expect_err("SQL must not accept a Rune-only failure");
		assert!(error.to_string().contains("unexpected response message"));
	}

	#[tokio::test]
	async fn rune_results_inherit_the_bounded_collector_ceiling() {
		let id = QueryId::from_u128(92);
		let batch = SqlFrame::response(
			id,
			SqlMessage::ResultBatch(ResultBatch {
				batch_index: 0,
				arrow_ipc_bytes: serde_bytes::ByteBuf::from(vec![1, 2, 3, 4]),
			}),
		);
		let mut reader = framed_bytes(&[batch]).await;
		let error = collect_response_bounded(&mut reader, id, ExpectedResponse::RuneQuery, 3)
			.await
			.expect_err("bounded Rune result");
		assert!(error.to_string().contains("3 byte limit"));
	}

	#[tokio::test]
	async fn collector_rejects_out_of_order_batches() {
		let id = QueryId::from_u128(10);
		let batch = SqlFrame::response(
			id,
			SqlMessage::ResultBatch(ResultBatch {
				batch_index: 1,
				arrow_ipc_bytes: serde_bytes::ByteBuf::from(vec![1, 2, 3]),
			}),
		);
		let mut reader = framed_bytes(&[batch]).await;
		let error = collect_response(&mut reader, id, ExpectedResponse::Query)
			.await
			.expect_err("out of order");
		assert!(error.to_string().contains("expecting 0"));
	}

	#[tokio::test]
	async fn collector_accepts_health_only_as_health() {
		let id = QueryId::from_u128(11);
		let healthy = SqlFrame::response(id, SqlMessage::Healthy(HealthyResponse));
		let mut reader = framed_bytes(std::slice::from_ref(&healthy)).await;
		assert_eq!(
			collect_response(&mut reader, id, ExpectedResponse::Health).await,
			Ok(QueryResult {
				batches: Vec::new(),
				row_count: 0,
			})
		);
		let mut reader = framed_bytes(&[healthy]).await;
		assert!(
			collect_response(&mut reader, id, ExpectedResponse::Query)
				.await
				.is_err()
		);
	}

	#[tokio::test]
	async fn collector_rejects_query_terminal_for_health() {
		let id = QueryId::from_u128(12);
		let complete = SqlFrame::response(id, SqlMessage::Complete(Complete { row_count: 0 }));
		let mut reader = framed_bytes(&[complete]).await;
		assert!(
			collect_response(&mut reader, id, ExpectedResponse::Health)
				.await
				.is_err()
		);
	}

	#[test]
	fn remote_loopback_configuration_is_refused_honestly() {
		let Err(error) = ToronClient::loopback("192.0.2.1:7742") else {
			panic!("remote loopback mode must be refused");
		};
		assert!(error.to_string().contains("authenticated mTLS"));
	}

	#[test]
	fn request_types_remain_used_by_the_client_surface() {
		let _ = CancelRequest {
			query_id: QueryId::from_u128(1),
		};
		let _ = HealthRequest;
	}
}
