#![cfg(feature = "otlp-http")]

#[path = "support/tls_pki.rs"]
mod tls_pki;

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
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
		"wendelon-otlp-http-{tag}-{}-{}",
		std::process::id(),
		Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

struct CapturedRequest {
	headers: String,
	body: Vec<u8>,
}

#[tokio::test]
async fn http_protobuf_trace_export_posts_mtls_batch() {
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
			Vec::new(),
		)
		.unwrap(),
	));
	let (captured_tx, captured_rx) = oneshot::channel();
	let server = tokio::spawn(async move {
		let (stream, _) = listener.accept().await.unwrap();
		let mut tls = acceptor.accept(stream).await.unwrap();
		let captured = read_http_request(&mut tls).await;
		tls.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
			.await
			.unwrap();
		tls.flush().await.unwrap();
		let _ = captured_tx.send(captured);
	});

	let trace_cfg = TraceExportConfig {
		endpoint: format!("https://localhost:{}", local_addr.port()),
		protocol: TraceExportProtocol::HttpProtobuf,
		sampling_rate_per_million: TRACE_SAMPLING_RATE_DENOMINATOR,
		client_cert_path: pki.data_client_cert.clone(),
		client_key_path: pki.data_client_key.clone(),
		ca_bundle_path: pki.ca_cert.clone(),
		max_queue_size: 32,
		export_timeout: Duration::from_secs(5),
	};
	let (dispatch, telemetry) = telemetry::build_dispatch(Some(&trace_cfg), 7).unwrap();
	tracing::dispatcher::with_default(&dispatch, || {
		let span = tracing::info_span!("otlp.test.span", tenant = 17_u64, ready = true);
		let _entered = span.enter();
		tracing::info!(message = "exported event", attempts = 3_u64);
	});
	telemetry.shutdown().await.unwrap();

	let captured = tokio::time::timeout(Duration::from_secs(5), captured_rx)
		.await
		.unwrap()
		.unwrap();
	let headers = captured.headers.to_ascii_lowercase();
	assert!(
		headers.starts_with("post /v1/traces http/1.1"),
		"unexpected request headers: {}",
		captured.headers
	);
	assert!(
		headers.contains("content-type: application/x-protobuf"),
		"unexpected request headers: {}",
		captured.headers
	);
	let body = String::from_utf8_lossy(&captured.body);
	assert!(body.contains("service.name"), "unexpected body: {body:?}");
	assert!(body.contains("wendelon"), "unexpected body: {body:?}");
	assert!(body.contains("otlp.test.span"), "unexpected body: {body:?}");
	assert!(body.contains("exported event"), "unexpected body: {body:?}");
	assert!(
		body.contains("wendelon.traces"),
		"unexpected body: {body:?}"
	);

	server.await.unwrap();
}

async fn read_http_request<S>(stream: &mut S) -> CapturedRequest
where
	S: AsyncRead + Unpin,
{
	let mut buf = Vec::new();
	let header_end;
	loop {
		let mut chunk = [0u8; 1024];
		let n = stream.read(&mut chunk).await.unwrap();
		assert!(n > 0, "unexpected EOF while reading request headers");
		buf.extend_from_slice(&chunk[..n]);
		if let Some(idx) = find_header_end(&buf) {
			header_end = idx;
			break;
		}
	}

	let headers = String::from_utf8(buf[..header_end].to_vec()).unwrap();
	let content_length = headers
		.lines()
		.find_map(|line| {
			let (name, value) = line.split_once(':')?;
			if name.eq_ignore_ascii_case("content-length") {
				return value.trim().parse::<usize>().ok();
			}
			None
		})
		.unwrap_or(0);
	let mut body = buf[header_end..].to_vec();
	while body.len() < content_length {
		let mut chunk = vec![0u8; content_length - body.len()];
		let n = stream.read(&mut chunk).await.unwrap();
		assert!(n > 0, "unexpected EOF while reading request body");
		body.extend_from_slice(&chunk[..n]);
	}
	body.truncate(content_length);
	CapturedRequest { headers, body }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
	buf.windows(4)
		.position(|window| window == b"\r\n\r\n")
		.map(|idx| idx + 4)
}
