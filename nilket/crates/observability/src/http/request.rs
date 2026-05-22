use std::io::{Error, ErrorKind};

use tokio::io::{AsyncRead, AsyncReadExt};

use super::MAX_REQUEST_HEADER_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLine<'a> {
	pub method: &'a str,
	pub path: &'a str,
	pub version: &'a str,
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

pub fn parse_request_line(raw: &[u8]) -> std::io::Result<RequestLine<'_>> {
	let request =
		std::str::from_utf8(raw).map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	let line = request
		.split("\r\n")
		.next()
		.filter(|line| !line.is_empty())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP request line"))?;
	let mut parts = line.split(' ');
	let method = parts
		.next()
		.filter(|method| !method.is_empty())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP method"))?;
	let path = parts
		.next()
		.filter(|path| !path.is_empty())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP path"))?;
	let version = parts
		.next()
		.filter(|version| !version.is_empty())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing HTTP protocol version"))?;
	Ok(RequestLine {
		method,
		path,
		version,
	})
}
