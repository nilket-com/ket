use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use toron::config::{
	CoordinatorConfig, ObservabilityConfig, SqlConfig, SqlTransport, ToronBootstrapConfig,
	ToronCatalogConfig, ToronNodeConfig, ToronStorageConfig, ToronWendelonBootstrapPeer,
	ToronWendelonStorageConfig, ToronWorkerConfig, TracingFormat,
};
use toron::daemon::ToronDaemon;
use toron::distributed::identity::{AttemptId, QueryId, StageId, TaskId, TaskIdentity, WorkerId};
use toron::sql_wire::{
	SqlErrorCode, SqlFrame, SqlMessage, read_frame as read_sql_frame,
	write_frame as write_sql_frame,
};
use toron::storage::TenantId;
use toron::wire::{
	CancelTaskPayload, EnvelopeIdentity, MessageKind, TaskCancelledPayload, WireEnvelope,
	decode_payload, encode_payload,
};

fn config() -> ToronBootstrapConfig {
	ToronBootstrapConfig {
		node: ToronNodeConfig {
			worker_id: WorkerId::from_u128(42),
			tenant_id: TenantId(1),
			drain_timeout: Duration::from_secs(1),
		},
		worker: ToronWorkerConfig {
			task_listener_bind: "127.0.0.1:0".parse().expect("loopback addr"),
			advertised_endpoint: None,
			memory_budget_bytes: 1024 * 1024,
		},
		storage: ToronStorageConfig {
			wendelon: ToronWendelonStorageConfig {
				bootstrap_peers: vec![ToronWendelonBootstrapPeer {
					node_id: 1,
					address: "127.0.0.1:7001".to_string(),
				}],
				client_bind: "127.0.0.1:0".parse().expect("client bind"),
				tls: None,
				cluster_domain: wendelon::network::tls::DEFAULT_CLUSTER_DOMAIN.to_string(),
				pool_max_retained: 64,
				pool_max_cap_bytes: 8 * 1024 * 1024,
			},
		},
		catalog: ToronCatalogConfig {
			descriptor_bootstrap_path: None,
		},
		sql: None,
		coordinator: CoordinatorConfig {
			include_self_worker: true,
			static_workers: Vec::new(),
		},
		tls: None,
		observability: ObservabilityConfig {
			level: "info".to_string(),
			format: TracingFormat::Pretty,
			listeners: None,
		},
	}
}

fn task_identity() -> TaskIdentity {
	TaskIdentity::new(
		QueryId::from_u128(100),
		StageId::new(1),
		TaskId::new(2),
		AttemptId::first(),
	)
}

fn config_with_sql() -> ToronBootstrapConfig {
	let mut config = config();
	config.sql = Some(SqlConfig {
		transport: SqlTransport::Quic,
		listener_bind: "127.0.0.1:0".parse().expect("sql bind"),
		statement_timeout: Duration::from_secs(1),
	});
	config
}

#[tokio::test]
async fn toron_daemon_binds_loopback_ephemeral_port() {
	let daemon = ToronDaemon::start(config()).await.expect("start daemon");
	let endpoint = daemon.handle().worker_endpoint;

	assert!(endpoint.ip().is_loopback());
	assert_ne!(endpoint.port(), 0);

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn toron_daemon_binds_sql_loopback_ephemeral_port() {
	let daemon = ToronDaemon::start(config_with_sql())
		.await
		.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	assert!(endpoint.ip().is_loopback());
	assert_ne!(endpoint.port(), 0);

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn toron_sql_syntax_error_emits_stable_error_code() {
	let daemon = ToronDaemon::start(config_with_sql())
		.await
		.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = toron::quic::QuicEndpoint::client().expect("sql client");
	let mut stream = client.connect_stream(endpoint).await.expect("connect sql");
	let query_id = QueryId::new();

	write_sql_frame(
		&mut stream,
		&SqlFrame::execute(query_id, "SELEC * FROM nope".to_string(), None),
	)
	.await
	.expect("write sql");
	stream.finish().await.expect("finish sql request");
	let response = read_sql_frame(&mut stream)
		.await
		.expect("read sql response");

	match response.message {
		SqlMessage::Error(error) => assert_eq!(error.code, SqlErrorCode::SyntaxError),
		other => panic!("unexpected SQL response: {other:?}"),
	}

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn toron_daemon_accepts_cancel_task_over_loopback() {
	let daemon = ToronDaemon::start(config()).await.expect("start daemon");
	let endpoint = daemon.handle().worker_endpoint;
	let identity = task_identity();
	let mut stream = TcpStream::connect(endpoint).await.expect("connect worker");
	let payload = CancelTaskPayload { identity };
	let envelope = WireEnvelope::try_new(
		MessageKind::CancelTask,
		EnvelopeIdentity::task(identity),
		None,
		None,
		encode_payload(&payload),
	)
	.expect("cancel envelope");

	write_envelope(&mut stream, &envelope).await;
	let response = read_envelope(&mut stream).await;
	let cancelled: TaskCancelledPayload =
		decode_payload(&response.payload).expect("decode cancelled payload");

	assert_eq!(response.kind, MessageKind::TaskCancelled);
	assert_eq!(response.identity, EnvelopeIdentity::task(identity));
	assert_eq!(cancelled.identity, identity);

	drop(stream);
	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn toron_daemon_shutdown_stops_accepting_connections() {
	let daemon = ToronDaemon::start(config()).await.expect("start daemon");
	let endpoint = daemon.handle().worker_endpoint;

	daemon.shutdown().await.expect("shutdown daemon");

	let result = TcpStream::connect(endpoint).await;
	assert!(result.is_err(), "daemon accepted connection after shutdown");
}

async fn write_envelope(stream: &mut TcpStream, envelope: &WireEnvelope) {
	let bytes = envelope.encode();
	stream
		.write_all(
			&u32::try_from(bytes.len())
				.expect("frame length fits u32")
				.to_le_bytes(),
		)
		.await
		.expect("write frame length");
	stream.write_all(&bytes).await.expect("write frame body");
	stream.flush().await.expect("flush frame");
}

async fn read_envelope(stream: &mut TcpStream) -> WireEnvelope {
	let mut len = [0u8; 4];
	stream
		.read_exact(&mut len)
		.await
		.expect("read frame length");
	let len = u32::from_le_bytes(len);
	let mut bytes = vec![0u8; usize::try_from(len).expect("frame length fits usize")];
	stream
		.read_exact(&mut bytes)
		.await
		.expect("read frame body");
	WireEnvelope::decode(&bytes).expect("decode wire envelope")
}
