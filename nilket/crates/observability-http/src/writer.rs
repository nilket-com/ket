use tokio::io::{AsyncWrite, AsyncWriteExt};

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
