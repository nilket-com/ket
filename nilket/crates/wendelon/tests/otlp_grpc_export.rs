#![cfg(feature = "otlp-grpc")]

#[path = "support/tls_pki.rs"]
mod tls_pki;

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;
use wendelon::metrics::telemetry::{
	self, TRACE_SAMPLING_RATE_DENOMINATOR, TraceExportConfig, TraceExportProtocol,
};
use wendelon::network::{TlsMaterial, rustls_server_config_with_peer_roots};

fn fresh_dir(tag: &str) -> std::path::PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-otlp-grpc-{tag}-{}-{}",
		std::process::id(),
		Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

struct CapturedGrpcRequest {
	path: String,
	content_type: String,
	te: String,
	payload: Vec<u8>,
}

#[tokio::test]
async fn grpc_trace_export_posts_mtls_h2_batch() {
	let root = fresh_dir("export");
	let pki = tls_pki::write_test_pki_with_server_dns(&root.join("pki"), "localhost");
	let server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let local_addr = listener.local_addr().unwrap();
	let acceptor = TlsAcceptor::from(Arc::new(
		rustls_server_config_with_peer_roots(
			&server_material,
			server_material.ca_roots.clone(),
			vec![b"h2".to_vec()],
		)
		.unwrap(),
	));
	let (captured_tx, captured_rx) = oneshot::channel();
	let server = tokio::spawn(async move {
		let (stream, _) = listener.accept().await.unwrap();
		let tls = acceptor.accept(stream).await.unwrap();
		let mut h2 = h2::server::handshake(tls).await.unwrap();
		let (request, mut respond) = h2.accept().await.unwrap().unwrap();
		let captured = read_grpc_request(request).await;
		let response = http::Response::builder()
			.status(200)
			.header("content-type", "application/grpc")
			.body(())
			.unwrap();
		let mut send = respond.send_response(response, false).unwrap();
		send.send_data(Bytes::from_static(&[0, 0, 0, 0, 0]), false)
			.unwrap();
		let mut trailers = http::HeaderMap::new();
		trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
		send.send_trailers(trailers).unwrap();
		let _ = captured_tx.send(captured);
	});

	let trace_cfg = TraceExportConfig {
		endpoint: format!("https://localhost:{}", local_addr.port()),
		protocol: TraceExportProtocol::Grpc,
		sampling_rate_per_million: TRACE_SAMPLING_RATE_DENOMINATOR,
		client_cert_path: pki.data_client_cert.clone(),
		client_key_path: pki.data_client_key.clone(),
		ca_bundle_path: pki.ca_cert.clone(),
		max_queue_size: 32,
		export_timeout: Duration::from_secs(5),
	};
	let (dispatch, telemetry) = telemetry::build_dispatch(Some(&trace_cfg), 8).unwrap();
	tracing::dispatcher::with_default(&dispatch, || {
		let span = tracing::info_span!("grpc.test.span", tenant = 18_u64, durable = true);
		let _entered = span.enter();
		tracing::info!(message = "grpc exported event", attempts = 4_u64);
	});
	telemetry.shutdown().await.unwrap();

	let captured = tokio::time::timeout(Duration::from_secs(5), captured_rx)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(
		captured.path,
		"/opentelemetry.proto.collector.trace.v1.TraceService/Export"
	);
	assert_eq!(captured.content_type, "application/grpc");
	assert_eq!(captured.te, "trailers");
	let body = String::from_utf8_lossy(&captured.payload);
	assert!(
		body.contains("service.name"),
		"unexpected gRPC payload: {body:?}"
	);
	assert!(
		body.contains("wendelon"),
		"unexpected gRPC payload: {body:?}"
	);
	assert!(
		body.contains("grpc.test.span"),
		"unexpected gRPC payload: {body:?}"
	);
	assert!(
		body.contains("grpc exported event"),
		"unexpected gRPC payload: {body:?}"
	);
	assert!(
		body.contains("wendelon.traces"),
		"unexpected gRPC payload: {body:?}"
	);

	server.await.unwrap();
}

#[tokio::test]
async fn grpc_trace_export_reuses_h2_connection_across_batches() {
	let root = fresh_dir("reuse");
	let pki = tls_pki::write_test_pki_with_server_dns(&root.join("pki"), "localhost");
	let server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let local_addr = listener.local_addr().unwrap();
	let acceptor = TlsAcceptor::from(Arc::new(
		rustls_server_config_with_peer_roots(
			&server_material,
			server_material.ca_roots.clone(),
			vec![b"h2".to_vec()],
		)
		.unwrap(),
	));
	let (captured_tx, captured_rx) = oneshot::channel();
	let server = tokio::spawn(async move {
		let (stream, _) = listener.accept().await.unwrap();
		let tls = acceptor.accept(stream).await.unwrap();
		let mut h2 = h2::server::handshake(tls).await.unwrap();
		let mut captured = Vec::new();
		for _ in 0..2 {
			let (request, mut respond) = h2.accept().await.unwrap().unwrap();
			captured.push(read_grpc_request(request).await);
			let response = http::Response::builder()
				.status(200)
				.header("content-type", "application/grpc")
				.body(())
				.unwrap();
			let mut send = respond.send_response(response, false).unwrap();
			send.send_data(Bytes::from_static(&[0, 0, 0, 0, 0]), false)
				.unwrap();
			let mut trailers = http::HeaderMap::new();
			trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
			send.send_trailers(trailers).unwrap();
		}
		let _ = captured_tx.send(captured);
	});

	let trace_cfg = TraceExportConfig {
		endpoint: format!("https://localhost:{}", local_addr.port()),
		protocol: TraceExportProtocol::Grpc,
		sampling_rate_per_million: TRACE_SAMPLING_RATE_DENOMINATOR,
		client_cert_path: pki.data_client_cert.clone(),
		client_key_path: pki.data_client_key.clone(),
		ca_bundle_path: pki.ca_cert.clone(),
		max_queue_size: 32,
		export_timeout: Duration::from_secs(5),
	};
	let (dispatch, telemetry) = telemetry::build_dispatch(Some(&trace_cfg), 9).unwrap();
	tracing::dispatcher::with_default(&dispatch, || {
		let span = tracing::info_span!("grpc.reuse.first");
		let _entered = span.enter();
		tracing::info!(message = "first batch");
	});
	tokio::time::sleep(Duration::from_millis(80)).await;
	tracing::dispatcher::with_default(&dispatch, || {
		let span = tracing::info_span!("grpc.reuse.second");
		let _entered = span.enter();
		tracing::info!(message = "second batch");
	});
	tokio::time::sleep(Duration::from_millis(80)).await;
	telemetry.shutdown().await.unwrap();

	let captured = tokio::time::timeout(Duration::from_secs(5), captured_rx)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(captured.len(), 2);
	let first = String::from_utf8_lossy(&captured[0].payload);
	let second = String::from_utf8_lossy(&captured[1].payload);
	assert!(
		first.contains("grpc.reuse.first"),
		"unexpected first payload: {first:?}"
	);
	assert!(
		second.contains("grpc.reuse.second"),
		"unexpected second payload: {second:?}"
	);

	server.await.unwrap();
}

async fn read_grpc_request(request: http::Request<h2::RecvStream>) -> CapturedGrpcRequest {
	let (parts, mut body) = request.into_parts();
	let mut raw = Vec::new();
	while let Some(chunk) = body.data().await {
		let chunk = chunk.unwrap();
		body.flow_control().release_capacity(chunk.len()).unwrap();
		raw.extend_from_slice(&chunk);
	}
	let payload = decode_grpc_message(&raw);
	CapturedGrpcRequest {
		path: parts.uri.path().to_string(),
		content_type: parts
			.headers
			.get("content-type")
			.unwrap()
			.to_str()
			.unwrap()
			.to_string(),
		te: parts
			.headers
			.get("te")
			.unwrap()
			.to_str()
			.unwrap()
			.to_string(),
		payload,
	}
}

fn decode_grpc_message(raw: &[u8]) -> Vec<u8> {
	assert!(raw.len() >= 5, "gRPC message too short: {}", raw.len());
	assert_eq!(raw[0], 0, "gRPC compressed flag should be zero");
	let len = u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]) as usize;
	assert_eq!(raw.len() - 5, len, "gRPC payload length mismatch");
	raw[5..].to_vec()
}
