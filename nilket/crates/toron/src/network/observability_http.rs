use std::io::{Error, ErrorKind};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REQUEST_HEADER_BYTES: usize = 8 * 1024;
const MAX_RESPONSE_HEADER_BYTES: usize = 8 * 1024;
const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
	pub status_code: u16,
	pub content_type: Option<String>,
	pub body: Vec<u8>,
}

pub async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Vec<u8>> {
	let mut request = Vec::new();
	let mut byte = [0u8; 1];
	while request.len() < MAX_REQUEST_HEADER_BYTES {
		let read = stream.read(&mut byte).await?;
		if read == 0 {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"connection closed before HTTP headers completed",
			));
		}
		request.push(byte[0]);
		if request.ends_with(b"\r\n\r\n") {
			return Ok(request);
		}
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		"HTTP request headers exceed 8 KiB",
	))
}

pub fn parse_request_line(raw: &[u8]) -> std::io::Result<(&str, &str)> {
	let request =
		std::str::from_utf8(raw).map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	let line = request
		.split("\r\n")
		.next()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP request line"))?;
	let mut parts = line.split_whitespace();
	let method = parts
		.next()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP method"))?;
	let path = parts
		.next()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP path"))?;
	if parts.next().is_none() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"missing HTTP protocol version",
		));
	}
	Ok((method, path))
}

pub async fn write_response<S: AsyncWrite + Unpin>(
	stream: &mut S,
	status: &str,
	content_type: &str,
	body: &str,
) -> std::io::Result<()> {
	let response = format!(
		"HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
		body.len()
	);
	stream.write_all(response.as_bytes()).await?;
	stream.flush().await
}

pub async fn write_request<S: AsyncWrite + Unpin>(
	stream: &mut S,
	method: &str,
	path: &str,
	host: &str,
) -> std::io::Result<()> {
	let request = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
	stream.write_all(request.as_bytes()).await?;
	stream.flush().await
}

pub async fn read_response<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<HttpResponse> {
	let mut response_head = Vec::new();
	let mut byte = [0u8; 1];
	while response_head.len() < MAX_RESPONSE_HEADER_BYTES {
		let read = stream.read(&mut byte).await?;
		if read == 0 {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"connection closed before HTTP response headers completed",
			));
		}
		response_head.push(byte[0]);
		if response_head.ends_with(b"\r\n\r\n") {
			return read_response_body(stream, &response_head).await;
		}
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		"HTTP response headers exceed 8 KiB",
	))
}

async fn read_response_body<S: AsyncRead + Unpin>(
	stream: &mut S,
	response_head: &[u8],
) -> std::io::Result<HttpResponse> {
	let headers = std::str::from_utf8(response_head)
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	let mut lines = headers.split("\r\n");
	let status_line = lines
		.next()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP response status line"))?;
	let mut status_parts = status_line.split_whitespace();
	let version = status_parts
		.next()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP response version"))?;
	if version != "HTTP/1.1" {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"unsupported HTTP response version",
		));
	}
	let status_code = status_parts
		.next()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP response status code"))?;
	if status_code.len() != 3 || !status_code.chars().all(|ch| ch.is_ascii_digit()) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"malformed HTTP response status code",
		));
	}
	let status_code = status_code
		.parse::<u16>()
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	let mut content_length = None;
	let mut content_type = None;
	let mut chunked = false;
	for line in lines {
		if line.is_empty() {
			break;
		}
		let Some((name, value)) = line.split_once(':') else {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"malformed HTTP response header",
			));
		};
		let value = value.trim();
		if name.eq_ignore_ascii_case("Content-Length") {
			content_length = Some(
				value
					.parse::<usize>()
					.map_err(|error| Error::new(ErrorKind::InvalidData, error))?,
			);
		} else if name.eq_ignore_ascii_case("Content-Type") {
			content_type = Some(value.to_string());
		} else if name.eq_ignore_ascii_case("Transfer-Encoding")
			&& value
				.split(',')
				.any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
		{
			chunked = true;
		}
	}
	if chunked {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"chunked HTTP response bodies are not supported",
		));
	}
	let content_length = content_length
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing Content-Length header"))?;
	if content_length > MAX_RESPONSE_BODY_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"HTTP response body exceeds 64 KiB",
		));
	}
	let mut body = vec![0u8; content_length];
	stream.read_exact(&mut body).await?;
	Ok(HttpResponse {
		status_code,
		content_type,
		body,
	})
}
