use std::io::{Error, ErrorKind};

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{MAX_RESPONSE_BODY_BYTES, MAX_RESPONSE_HEADER_BYTES};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
	pub status_code: u16,
	pub content_type: Option<String>,
	pub body: Vec<u8>,
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
