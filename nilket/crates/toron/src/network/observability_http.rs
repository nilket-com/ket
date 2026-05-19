use std::io::{Error, ErrorKind};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REQUEST_HEADER_BYTES: usize = 8 * 1024;

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
