use std::io::{Error, ErrorKind};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rcgen::{
	BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
	KeyUsagePurpose,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use toron::config::{
	CoordinatorConfig, SqlConfig, SqlTransport, TlsConfig, ToronBootstrapConfig,
	ToronCatalogConfig, ToronNodeConfig, ToronObservabilityConfig, ToronStorageConfig,
	ToronTracingFormat, ToronWendelonBootstrapPeer, ToronWendelonStorageConfig, ToronWorkerConfig,
};
use toron::daemon::ToronDaemon;
use toron::distributed::identity::{AttemptId, QueryId, StageId, TaskId, TaskIdentity, WorkerId};
use toron::network::tls::{
	DEFAULT_CLUSTER_DOMAIN, canonical_client_dns_name, canonical_daemon_dns_name,
	client_config_from_paths,
};
use toron::quic::QuicEndpoint;
use toron::sql_wire::{
	SqlErrorCode, SqlFrame, SqlMessage, read_frame as read_sql_frame,
	write_frame as write_sql_frame,
};
use toron::storage::TenantId;
use toron::wire::{
	CancelTaskPayload, EnvelopeIdentity, MessageKind, TaskCancelledPayload, WireEnvelope,
	decode_payload, encode_payload,
};

struct TestPki {
	ca_cert: PathBuf,
	daemon_cert: PathBuf,
	daemon_key: PathBuf,
	client_cert: PathBuf,
	client_key: PathBuf,
	peer_daemon_cert: PathBuf,
	peer_daemon_key: PathBuf,
	wrong_prefix_client_cert: PathBuf,
	wrong_prefix_client_key: PathBuf,
	subdomain_client_cert: PathBuf,
	subdomain_client_key: PathBuf,
	subdomain_daemon_cert: PathBuf,
	subdomain_daemon_key: PathBuf,
	unknown_ca_client_cert: PathBuf,
	unknown_ca_client_key: PathBuf,
}

#[tokio::test]
async fn remote_sql_listener_accepts_client_san_and_rejects_toron_san() {
	let worker_id = WorkerId::from_u128(44);
	let pki = TestPki::generate(worker_id, "analytics");
	let daemon = ToronDaemon::start(tls_config(
		worker_id,
		&pki,
		"127.0.0.1:0".parse().expect("worker bind"),
		Some("0.0.0.0:0".parse().expect("sql bind")),
	))
	.await
	.expect("start TLS SQL daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().sql_endpoint.expect("sql endpoint"));
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let response = execute_sql_with_tls(
		endpoint,
		&server_name,
		&pki.client_cert,
		&pki.client_key,
		&pki.ca_cert,
	)
	.await
	.expect("SQL response over mTLS");
	match response {
		SqlMessage::Error(error) => assert_eq!(error.code, SqlErrorCode::SyntaxError),
		other => panic!("unexpected SQL response: {other:?}"),
	}

	let rejected = tokio::time::timeout(
		Duration::from_secs(2),
		execute_sql_with_tls(
			endpoint,
			&server_name,
			&pki.daemon_cert,
			&pki.daemon_key,
			&pki.ca_cert,
		),
	)
	.await;
	match rejected {
		Ok(Ok(message)) => panic!("SQL listener accepted toron-* daemon cert: {message:?}"),
		Ok(Err(_)) | Err(_) => {}
	}

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn remote_worker_task_listener_accepts_toron_peer_san() {
	let worker_id = WorkerId::from_u128(45);
	let pki = TestPki::generate(worker_id, "analytics");
	let daemon = ToronDaemon::start(tls_config(
		worker_id,
		&pki,
		"0.0.0.0:0".parse().expect("worker bind"),
		None,
	))
	.await
	.expect("start TLS worker daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().worker_endpoint);
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
	let identity = task_identity();
	let response = try_cancel_task_with_tls(
		endpoint,
		&server_name,
		&pki.peer_daemon_cert,
		&pki.peer_daemon_key,
		&pki.ca_cert,
	)
	.await
	.expect("worker response over mTLS");
	let cancelled: TaskCancelledPayload =
		decode_payload(&response.payload).expect("decode cancelled payload");

	assert_eq!(response.kind, MessageKind::TaskCancelled);
	assert_eq!(response.identity, EnvelopeIdentity::task(identity));
	assert_eq!(cancelled.identity, identity);

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn remote_worker_task_listener_rejects_client_san() {
	let worker_id = WorkerId::from_u128(46);
	let pki = TestPki::generate(worker_id, "analytics");
	let daemon = ToronDaemon::start(tls_config(
		worker_id,
		&pki,
		"0.0.0.0:0".parse().expect("worker bind"),
		None,
	))
	.await
	.expect("start TLS worker daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().worker_endpoint);
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let rejected = tokio::time::timeout(
		Duration::from_secs(2),
		try_cancel_task_with_tls(
			endpoint,
			&server_name,
			&pki.client_cert,
			&pki.client_key,
			&pki.ca_cert,
		),
	)
	.await;
	match rejected {
		Ok(Ok(response)) => panic!("worker listener accepted client-* cert: {response:?}"),
		Ok(Err(_)) | Err(_) => {}
	}

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn remote_worker_task_listener_rejects_subdomain_injection_san() {
	let worker_id = WorkerId::from_u128(47);
	let pki = TestPki::generate(worker_id, "analytics");
	let daemon = ToronDaemon::start(tls_config(
		worker_id,
		&pki,
		"0.0.0.0:0".parse().expect("worker bind"),
		None,
	))
	.await
	.expect("start TLS worker daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().worker_endpoint);
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let rejected = tokio::time::timeout(
		Duration::from_secs(2),
		try_cancel_task_with_tls(
			endpoint,
			&server_name,
			&pki.subdomain_daemon_cert,
			&pki.subdomain_daemon_key,
			&pki.ca_cert,
		),
	)
	.await;
	match rejected {
		Ok(Ok(response)) => panic!("worker listener accepted subdomain SAN: {response:?}"),
		Ok(Err(_)) | Err(_) => {}
	}

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn remote_sql_listener_rejects_malformed_client_sans_and_unknown_ca() {
	let worker_id = WorkerId::from_u128(48);
	let pki = TestPki::generate(worker_id, "analytics");
	let daemon = ToronDaemon::start(tls_config(
		worker_id,
		&pki,
		"127.0.0.1:0".parse().expect("worker bind"),
		Some("0.0.0.0:0".parse().expect("sql bind")),
	))
	.await
	.expect("start TLS SQL daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().sql_endpoint.expect("sql endpoint"));
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	for (label, cert, key) in [
		(
			"unknown prefix",
			&pki.wrong_prefix_client_cert,
			&pki.wrong_prefix_client_key,
		),
		(
			"subdomain injection",
			&pki.subdomain_client_cert,
			&pki.subdomain_client_key,
		),
		(
			"toron subdomain injection",
			&pki.subdomain_daemon_cert,
			&pki.subdomain_daemon_key,
		),
		(
			"unknown client CA",
			&pki.unknown_ca_client_cert,
			&pki.unknown_ca_client_key,
		),
	] {
		let rejected = tokio::time::timeout(
			Duration::from_secs(2),
			execute_sql_with_tls(endpoint, &server_name, cert, key, &pki.ca_cert),
		)
		.await;
		match rejected {
			Ok(Ok(message)) => panic!("SQL listener accepted {label} cert: {message:?}"),
			Ok(Err(_)) | Err(_) => {}
		}
	}

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn daemon_rejects_wildcard_listener_cert() {
	let worker_id = WorkerId::from_u128(49);
	let pki = TestPki::generate_with_daemon_dns("analytics", "*.cluster.local");
	expect_daemon_start_error_contains(
		tls_config(
			worker_id,
			&pki,
			"127.0.0.1:0".parse().expect("worker bind"),
			Some("0.0.0.0:0".parse().expect("sql bind")),
		),
		"tls listener cert SAN does not match node.worker_id",
	)
	.await;
}

#[tokio::test]
async fn daemon_rejects_listener_cert_for_different_worker_id() {
	let worker_id = WorkerId::from_u128(50);
	let wrong_worker_id = WorkerId::from_u128(51);
	let pki = TestPki::generate(wrong_worker_id, "analytics");
	expect_daemon_start_error_contains(
		tls_config(
			worker_id,
			&pki,
			"0.0.0.0:0".parse().expect("worker bind"),
			None,
		),
		"tls listener cert SAN does not match node.worker_id",
	)
	.await;
}

#[tokio::test]
async fn daemon_rejects_non_pkcs8_listener_key() {
	let worker_id = WorkerId::from_u128(52);
	let pki = TestPki::generate(worker_id, "analytics");
	std::fs::write(&pki.daemon_key, "not a PEM PKCS8 private key").expect("overwrite key");
	expect_daemon_start_error_contains(
		tls_config(
			worker_id,
			&pki,
			"0.0.0.0:0".parse().expect("worker bind"),
			None,
		),
		"tls listener_key must be unencrypted PEM PKCS#8",
	)
	.await;
}

#[tokio::test]
async fn daemon_rejects_encrypted_listener_key() {
	let worker_id = WorkerId::from_u128(53);
	let pki = TestPki::generate(worker_id, "analytics");
	std::fs::write(
		&pki.daemon_key,
		"-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIB\n-----END ENCRYPTED PRIVATE KEY-----\n",
	)
	.expect("overwrite key");
	expect_daemon_start_error_contains(
		tls_config(
			worker_id,
			&pki,
			"0.0.0.0:0".parse().expect("worker bind"),
			None,
		),
		"tls listener_key must be unencrypted PEM PKCS#8",
	)
	.await;
}

async fn expect_daemon_start_error_contains(config: ToronBootstrapConfig, expected: &str) {
	match ToronDaemon::start(config).await {
		Ok(daemon) => {
			daemon.shutdown().await.expect("shutdown unexpected daemon");
			panic!("daemon start unexpectedly succeeded");
		}
		Err(error) => assert!(error.to_string().contains(expected), "{error}"),
	}
}

impl TestPki {
	fn generate(worker_id: WorkerId, principal: &str) -> Self {
		let daemon_dns = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
		Self::generate_with_worker_and_daemon_dns(worker_id, principal, &daemon_dns)
	}

	fn generate_with_daemon_dns(principal: &str, daemon_dns: &str) -> Self {
		Self::generate_with_worker_and_daemon_dns(WorkerId::from_u128(1), principal, daemon_dns)
	}

	fn generate_with_worker_and_daemon_dns(
		worker_id: WorkerId,
		principal: &str,
		daemon_dns: &str,
	) -> Self {
		let dir = fresh_dir("tls-listener");
		let ca = ca_cert();
		let unknown_ca = ca_cert();
		let daemon = leaf_cert(
			daemon_dns,
			&[
				ExtendedKeyUsagePurpose::ServerAuth,
				ExtendedKeyUsagePurpose::ClientAuth,
			],
		);
		let client_dns = canonical_client_dns_name(principal, DEFAULT_CLUSTER_DOMAIN);
		let client = leaf_cert(&client_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
		let peer_worker_id = WorkerId::from_u128(worker_id.get().as_u128().wrapping_add(1));
		let peer_daemon_dns = canonical_daemon_dns_name(&peer_worker_id, DEFAULT_CLUSTER_DOMAIN);
		let peer_daemon = leaf_cert(&peer_daemon_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
		let wrong_prefix = leaf_cert(
			"foo-1.cluster.local",
			&[ExtendedKeyUsagePurpose::ClientAuth],
		);
		let subdomain = leaf_cert(
			"client-analytics.evil.cluster.local",
			&[ExtendedKeyUsagePurpose::ClientAuth],
		);
		let subdomain_daemon = leaf_cert(
			"toron-foo.evil.cluster.local",
			&[ExtendedKeyUsagePurpose::ClientAuth],
		);
		let unknown_ca_client = leaf_cert(&client_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
		let pki = Self {
			ca_cert: dir.join("ca.pem"),
			daemon_cert: dir.join("daemon.pem"),
			daemon_key: dir.join("daemon.key"),
			client_cert: dir.join("client.pem"),
			client_key: dir.join("client.key"),
			peer_daemon_cert: dir.join("peer-daemon.pem"),
			peer_daemon_key: dir.join("peer-daemon.key"),
			wrong_prefix_client_cert: dir.join("wrong-prefix-client.pem"),
			wrong_prefix_client_key: dir.join("wrong-prefix-client.key"),
			subdomain_client_cert: dir.join("subdomain-client.pem"),
			subdomain_client_key: dir.join("subdomain-client.key"),
			subdomain_daemon_cert: dir.join("subdomain-daemon.pem"),
			subdomain_daemon_key: dir.join("subdomain-daemon.key"),
			unknown_ca_client_cert: dir.join("unknown-ca-client.pem"),
			unknown_ca_client_key: dir.join("unknown-ca-client.key"),
		};
		std::fs::write(&pki.ca_cert, ca.serialize_pem().expect("CA PEM")).expect("write CA");
		write_signed_cert(&pki.daemon_cert, &pki.daemon_key, &daemon, &ca);
		write_signed_cert(&pki.client_cert, &pki.client_key, &client, &ca);
		write_signed_cert(
			&pki.peer_daemon_cert,
			&pki.peer_daemon_key,
			&peer_daemon,
			&ca,
		);
		write_signed_cert(
			&pki.wrong_prefix_client_cert,
			&pki.wrong_prefix_client_key,
			&wrong_prefix,
			&ca,
		);
		write_signed_cert(
			&pki.subdomain_client_cert,
			&pki.subdomain_client_key,
			&subdomain,
			&ca,
		);
		write_signed_cert(
			&pki.subdomain_daemon_cert,
			&pki.subdomain_daemon_key,
			&subdomain_daemon,
			&ca,
		);
		write_signed_cert(
			&pki.unknown_ca_client_cert,
			&pki.unknown_ca_client_key,
			&unknown_ca_client,
			&unknown_ca,
		);
		pki
	}
}

fn tls_config(
	worker_id: WorkerId,
	pki: &TestPki,
	worker_bind: SocketAddr,
	sql_bind: Option<SocketAddr>,
) -> ToronBootstrapConfig {
	ToronBootstrapConfig {
		node: ToronNodeConfig {
			worker_id,
			tenant_id: TenantId(1),
			drain_timeout: Duration::from_secs(1),
		},
		worker: ToronWorkerConfig {
			task_listener_bind: worker_bind,
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
		sql: sql_bind.map(|listener_bind| SqlConfig {
			transport: SqlTransport::Quic,
			listener_bind,
			statement_timeout: Duration::from_secs(1),
		}),
		coordinator: CoordinatorConfig {
			include_self_worker: true,
			static_workers: Vec::new(),
		},
		tls: Some(TlsConfig {
			listener_cert: pki.daemon_cert.clone(),
			listener_key: pki.daemon_key.clone(),
			peer_ca: pki.ca_cert.clone(),
			cluster_domain: DEFAULT_CLUSTER_DOMAIN.to_string(),
		}),
		observability: ToronObservabilityConfig {
			level: "info".to_string(),
			format: ToronTracingFormat::Pretty,
		},
	}
}

fn ca_cert() -> Certificate {
	let mut params = CertificateParams::new(Vec::new());
	params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
	params.key_usages = vec![
		KeyUsagePurpose::DigitalSignature,
		KeyUsagePurpose::KeyCertSign,
	];
	params
		.distinguished_name
		.push(DnType::CommonName, "toron-test-ca");
	Certificate::from_params(params).expect("CA cert")
}

fn leaf_cert(dns_name: &str, extended_key_usages: &[ExtendedKeyUsagePurpose]) -> Certificate {
	let mut params = CertificateParams::new(vec![dns_name.to_string()]);
	params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
	params.extended_key_usages = extended_key_usages.to_vec();
	params.distinguished_name.push(DnType::CommonName, dns_name);
	Certificate::from_params(params).expect("leaf cert")
}

fn write_signed_cert(cert_path: &Path, key_path: &Path, cert: &Certificate, ca: &Certificate) {
	std::fs::write(
		cert_path,
		cert.serialize_pem_with_signer(ca).expect("signed cert PEM"),
	)
	.expect("write cert");
	std::fs::write(key_path, cert.serialize_private_key_pem()).expect("write key");
}

fn authenticated_client(cert: &Path, key: &Path, ca: &Path) -> QuicEndpoint {
	let client_config = client_config_from_paths(cert, key, ca).expect("client TLS config");
	QuicEndpoint::client_authenticated(client_config).expect("authenticated QUIC client")
}

async fn execute_sql_with_tls(
	endpoint: SocketAddr,
	server_name: &str,
	cert: &Path,
	key: &Path,
	ca: &Path,
) -> std::io::Result<SqlMessage> {
	let client = authenticated_client(cert, key, ca);
	let mut stream = client
		.connect_stream_with_server_name(endpoint, server_name)
		.await?;
	let query_id = QueryId::new();
	write_sql_frame(
		&mut stream,
		&SqlFrame::execute(query_id, "SELEC * FROM nope".to_string(), None),
	)
	.await?;
	stream.finish().await?;
	let frame = read_sql_frame(&mut stream).await?;
	Ok(frame.message)
}

fn task_identity() -> TaskIdentity {
	TaskIdentity::new(
		QueryId::from_u128(100),
		StageId::new(1),
		TaskId::new(2),
		AttemptId::first(),
	)
}

async fn try_cancel_task_with_tls(
	endpoint: SocketAddr,
	server_name: &str,
	cert: &Path,
	key: &Path,
	ca: &Path,
) -> std::io::Result<WireEnvelope> {
	let client = authenticated_client(cert, key, ca);
	let mut stream = client
		.connect_stream_with_server_name(endpoint, server_name)
		.await?;
	let identity = task_identity();
	let payload = CancelTaskPayload { identity };
	let envelope = WireEnvelope::try_new(
		MessageKind::CancelTask,
		EnvelopeIdentity::task(identity),
		None,
		None,
		encode_payload(&payload),
	)
	.map_err(|error| Error::new(ErrorKind::InvalidData, format!("{error}")))?;

	try_write_envelope(&mut stream, &envelope).await?;
	try_read_envelope(&mut stream).await
}

async fn try_write_envelope<W>(stream: &mut W, envelope: &WireEnvelope) -> std::io::Result<()>
where
	W: AsyncWrite + Unpin,
{
	let bytes = envelope.encode();
	stream
		.write_all(
			&u32::try_from(bytes.len())
				.expect("frame length fits u32")
				.to_le_bytes(),
		)
		.await?;
	stream.write_all(&bytes).await?;
	stream.flush().await
}

async fn try_read_envelope<R>(stream: &mut R) -> std::io::Result<WireEnvelope>
where
	R: AsyncRead + Unpin,
{
	let mut len = [0u8; 4];
	stream.read_exact(&mut len).await?;
	let len = u32::from_le_bytes(len) as usize;
	let mut bytes = vec![0u8; len];
	stream.read_exact(&mut bytes).await?;
	WireEnvelope::decode(&bytes)
		.map_err(|error| Error::new(ErrorKind::InvalidData, format!("{error}")))
}

fn connectable_loopback_addr(addr: SocketAddr) -> SocketAddr {
	SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
}

fn fresh_dir(tag: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("system time after epoch")
		.as_nanos();
	let dir = std::env::temp_dir().join(format!("{tag}-{}-{nanos}", std::process::id()));
	std::fs::create_dir_all(&dir).expect("create temp dir");
	dir
}
