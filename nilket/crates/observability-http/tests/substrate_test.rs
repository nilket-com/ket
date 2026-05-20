use tokio::io::AsyncWriteExt;

use observability_http::{
	HttpResponse, parse_request_line, read_request, read_response, write_request, write_response,
};

#[tokio::test]
async fn observability_http_reads_request_until_header_terminator() {
	let (mut client, mut server) = tokio::io::duplex(64);
	let writer = tokio::spawn(async move {
		server.write_all(b"GET /ready").await?;
		server
			.write_all(b" HTTP/1.1\r\nHost: local\r\n\r\n")
			.await?;
		Ok::<_, std::io::Error>(())
	});

	let request = read_request(&mut client).await.expect("read request");
	writer.await.expect("join writer").expect("write request");

	assert_eq!(request, b"GET /ready HTTP/1.1\r\nHost: local\r\n\r\n");
}

#[tokio::test]
async fn observability_http_rejects_request_headers_over_limit() {
	let bytes = vec![b'a'; 9 * 1024];
	let mut stream = bytes.as_slice();
	let error = read_request(&mut stream)
		.await
		.expect_err("oversized request must fail");

	assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
	assert_eq!(error.to_string(), "HTTP request headers exceed 8 KiB");
}

#[test]
fn observability_http_parses_request_line_with_version_field() {
	let line = parse_request_line(b"GET /ready HTTP/1.1\r\nHost: local\r\n\r\n")
		.expect("parse request line");

	assert_eq!(line.method, "GET");
	assert_eq!(line.path, "/ready");
	assert_eq!(line.version, "HTTP/1.1");
}

#[test]
fn observability_http_rejects_missing_request_line_parts() {
	for (raw, expected) in [
		(
			b"\r\nHost: local\r\n\r\n".as_slice(),
			"missing HTTP request line",
		),
		(
			b" /ready HTTP/1.1\r\nHost: local\r\n\r\n".as_slice(),
			"missing HTTP method",
		),
		(
			b"GET  HTTP/1.1\r\nHost: local\r\n\r\n".as_slice(),
			"missing HTTP path",
		),
		(
			b"GET /ready\r\nHost: local\r\n\r\n".as_slice(),
			"missing HTTP protocol version",
		),
	] {
		let error = parse_request_line(raw).expect_err("malformed line must fail");
		assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
		assert_eq!(error.to_string(), expected);
	}
}

#[tokio::test]
async fn observability_http_writes_titlecase_response_headers() {
	let mut buf = Vec::new();
	write_response(&mut buf, "200 OK", "application/json", "{}")
		.await
		.expect("write response");

	assert_eq!(
		buf,
		b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
	);
}

#[tokio::test]
async fn observability_http_reads_content_length_response() {
	let response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\nhello";
	let mut stream = response.as_slice();

	assert_eq!(
		read_response(&mut stream).await.expect("read response"),
		HttpResponse {
			status_code: 200,
			content_type: Some("text/plain".to_string()),
			body: b"hello".to_vec(),
		}
	);
}

#[tokio::test]
async fn observability_http_rejects_chunked_response() {
	let response =
		b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\nhello";
	let mut stream = response.as_slice();
	let error = read_response(&mut stream)
		.await
		.expect_err("chunked response must fail");

	assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
	assert_eq!(
		error.to_string(),
		"chunked HTTP response bodies are not supported"
	);
}

#[tokio::test]
async fn observability_http_rejects_response_body_over_limit() {
	let response = b"HTTP/1.1 200 OK\r\nContent-Length: 65537\r\n\r\n";
	let mut stream = response.as_slice();
	let error = read_response(&mut stream)
		.await
		.expect_err("oversized response must fail");

	assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
	assert_eq!(error.to_string(), "HTTP response body exceeds 64 KiB");
}

#[tokio::test]
async fn observability_http_writes_get_request_with_host_and_close() {
	let mut buf = Vec::new();
	write_request(&mut buf, "GET", "/ready", "toron-x.cluster.local:9443")
		.await
		.expect("write request");

	assert_eq!(
		buf,
		b"GET /ready HTTP/1.1\r\nHost: toron-x.cluster.local:9443\r\nConnection: close\r\n\r\n"
	);
}
